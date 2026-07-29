"""Funcoes chamadas pelas tarefas das DAGs.

Por que a logica NAO mora no arquivo da DAG
-------------------------------------------
Um arquivo de DAG e reavaliado pelo processador de DAGs a cada poucos segundos. Tudo que
estiver no nivel do modulo roda nessa varredura -- inclusive conexoes de rede, leituras do
MinIO e importacoes pesadas. Uma DAG que abre o catalogo Iceberg durante o parse deixa o
scheduler lento e, pior, pode falhar a varredura inteira quando o MinIO esta fora do ar.

Aqui as funcoes so executam quando a TAREFA roda. Os imports pesados (`dlt`, `dbt`,
`great_expectations`) ficam dentro das funcoes pelo mesmo motivo.

Este modulo tambem e a fronteira que mantem as DAGs finas: elas declaram ordem e politica de
retry; a regra de negocio continua no pacote `pharma_pipeline`, testavel sem Airflow.
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


def _settings():
    from pharma_pipeline.config import Settings

    return Settings.from_env()


def ingerir(source: str, **contexto: Any) -> dict[str, Any]:
    """Extrai uma fonte e sincroniza a bronze Iceberg.

    Idempotente por construcao: o watermark do dlt evita reler o que ja foi lido, e o UPSERT
    por chave garante que uma reexecucao apos falha parcial nao duplique linhas. E por isso que
    esta tarefa pode ter `retries` sem risco.
    """
    from pharma_pipeline.iceberg import sync_bronze_to_iceberg
    from pharma_pipeline.ingestion import ingest_source

    settings = _settings()
    ingestao = ingest_source(settings, source)
    sync = sync_bronze_to_iceberg(settings, source)

    LOGGER.info(
        "%s: %s linhas ingeridas; Iceberg +%d inseridas, %d atualizadas.",
        source,
        ingestao.rows_loaded if ingestao.rows_loaded is not None else "?",
        sync.rows_inserted,
        sync.rows_updated,
    )
    return {
        "source": source,
        "load_ids": list(ingestao.load_ids),
        "rows_loaded": ingestao.rows_loaded,
        "rows_inserted": sync.rows_inserted,
        "rows_updated": sync.rows_updated,
        "snapshot_created": sync.snapshot_created,
    }


def transformar(select: str | None = None, **contexto: Any) -> dict[str, Any]:
    """Roda `dbt build`: materializa os modelos e executa os testes na ordem do grafo.

    `build` -- e nao `run` seguido de `test` -- porque ele testa cada modelo logo apos
    materializa-lo. Um modelo que falha no teste impede que os modelos seguintes sejam
    construidos sobre dado ruim, em vez de propagar o defeito por toda a gold.
    """
    from pharma_pipeline.transform import run_dbt

    resultado = run_dbt(_settings(), "build", select=select)
    if not resultado.success:
        raise RuntimeError(
            "dbt build falhou: " + ("; ".join(resultado.failures) or "sem detalhe do no")
        )
    return {"nodes_executed": resultado.nodes_executed}


def publicar(layer: str, **contexto: Any) -> list[dict[str, Any]]:
    """Publica uma camada do DuckDB para tabelas Iceberg no MinIO."""
    from pharma_pipeline.publish import publish_layer

    resultados = publish_layer(_settings(), layer)
    return [
        {
            "table": item.table,
            "rows_read": item.rows_read,
            "rows_inserted": item.upsert.rows_inserted,
            "rows_updated": item.upsert.rows_updated,
            "unchanged": item.unchanged,
        }
        for item in resultados
    ]


def validar_contratos(**contexto: Any) -> list[dict[str, Any]]:
    """Valida o contrato das tabelas publicadas com Great Expectations."""
    from pharma_pipeline.quality import validar_todas

    validacoes = validar_todas(_settings())
    reprovadas = [item for item in validacoes if not item.success]
    if reprovadas:
        detalhes = "; ".join(
            f"{item.table}: " + ", ".join(f"{f.expectation}({f.column})" for f in item.failures)
            for item in reprovadas
        )
        raise RuntimeError(f"Contrato violado nas tabelas publicadas -- {detalhes}")
    return [
        {"table": item.table, "rows": item.rows, "success": item.success} for item in validacoes
    ]


def verificar_frescor(**contexto: Any) -> dict[str, Any]:
    """Avalia o staleness gap e decide se alguem precisa ser acordado.

    Falha a tarefa APENAS quando o SLO do pipeline e violado -- a parte que esta sob nosso
    controle. Atraso da fonte vira aviso no log: nao ha correcao possivel do nosso lado, e um
    alerta que fica permanentemente vermelho e um alerta que sera ignorado no dia em que
    realmente importar.
    """
    from pharma_pipeline.freshness import avaliar_frescor

    relatorio = avaliar_frescor(_settings())
    LOGGER.info("Relatorio de frescor:\n%s", relatorio.resumo())

    for item in relatorio.violacoes_fonte:
        LOGGER.warning("SLO DA FONTE violado -- %s", item.mensagem())

    if relatorio.violacoes_pipeline:
        detalhes = " | ".join(item.mensagem() for item in relatorio.violacoes_pipeline)
        raise RuntimeError(f"SLO DO PIPELINE violado -- {detalhes}")

    return relatorio.to_dict()


def notificar(**contexto: Any) -> str:
    """Fecha a execucao com um resumo legivel no log da tarefa.

    Em producao, este e o ponto de integracao com Slack, e-mail ou PagerDuty. Aqui ele escreve
    no log de proposito: um webhook fixo no codigo seria um segredo versionado, e a Fase 4 nao
    precisa de um canal externo para ensinar o conceito de notificacao.
    """
    ti = contexto.get("ti")
    frescor = ti.xcom_pull(task_ids="verificar_frescor") if ti is not None else None

    linhas = ["Execucao concluida."]
    if frescor:
        linhas.append(f"Severidade do frescor: {frescor.get('severidade')}")
        for fonte in frescor.get("fontes", []):
            linhas.append(f"  {fonte['mensagem']}")

    resumo = "\n".join(linhas)
    LOGGER.info(resumo)
    return resumo
