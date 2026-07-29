"""Avaliacao do frescor e decisao de alerta.

O que este modulo decide
------------------------
`gold.metricas_frescor` MEDE. Este modulo JULGA: le a medicao mais recente de cada fonte e
responde a unica pergunta que interessa a quem esta de plantao -- alguem precisa agir agora?

A separacao entre medir e julgar e proposital. O limiar muda com o tempo e com o contrato de
uso; a medicao, nao. Guardar o julgamento no modelo SQL obrigaria a reprocessar historico toda
vez que um limiar fosse revisto.

As duas severidades, e por que nao podem ser a mesma
----------------------------------------------------
    ATRASO DO PIPELINE  -> a culpa e nossa. O extrator nao rodou, falhou ou travou.
                           Acao: investigar o pipeline. Isso DEVE falhar a DAG.

    ATRASO DA FONTE     -> a origem nao publicou. Nenhum deploy nosso muda isso.
                           Acao: avisar quem consome que o dado esta velho. Isso NAO deve
                           falhar a DAG, senao a equipe aprende a ignorar o alarme.

Tratar os dois como o mesmo alerta produz o pior resultado possivel em operacao: ou o time
investiga codigo por horas para descobrir que a FDA e que nao publicou, ou passa a ignorar um
alerta que vive vermelho -- e nao percebe o dia em que o pipeline realmente quebrou.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pharma_pipeline.config import Settings
from pharma_pipeline.iceberg import read_table, table_exists

LOGGER = logging.getLogger(__name__)

TABELA_METRICAS = "gold.metricas_frescor"

# Severidades, da mais grave para a menos grave.
SEVERIDADE_PIPELINE = "pipeline_atrasado"
SEVERIDADE_FONTE = "fonte_atrasada"
SEVERIDADE_OK = "ok"


@dataclass(frozen=True, slots=True)
class AvaliacaoFonte:
    """Julgamento da medicao mais recente de uma fonte."""

    fonte: str
    medicao_em: datetime
    event_time_mais_recente: datetime
    ultimo_ingest_time: datetime
    atraso_da_fonte_horas: int
    atraso_do_pipeline_horas: int
    idade_do_dado_horas: int
    sla_ingestao_horas: int
    sla_frescor_fonte_horas: int
    situacao: str

    @property
    def violou_sla_pipeline(self) -> bool:
        return self.atraso_do_pipeline_horas > self.sla_ingestao_horas

    @property
    def violou_sla_fonte(self) -> bool:
        return self.atraso_da_fonte_horas > self.sla_frescor_fonte_horas

    def mensagem(self) -> str:
        if self.violou_sla_pipeline:
            return (
                f"{self.fonte}: pipeline atrasado -- {self.atraso_do_pipeline_horas} h sem "
                f"ingestao (limite {self.sla_ingestao_horas} h). "
                f"Ultima captura em {self.ultimo_ingest_time:%Y-%m-%d %H:%M} UTC."
            )
        if self.violou_sla_fonte:
            return (
                f"{self.fonte}: fonte atrasada -- o dado mais novo ja tinha "
                f"{self.atraso_da_fonte_horas} h quando foi capturado "
                f"(limite {self.sla_frescor_fonte_horas} h). O pipeline esta saudavel."
            )
        return (
            f"{self.fonte}: ok -- dado com {self.idade_do_dado_horas} h de idade; "
            f"fonte {self.atraso_da_fonte_horas} h, pipeline {self.atraso_do_pipeline_horas} h."
        )


@dataclass(frozen=True, slots=True)
class RelatorioFrescor:
    """Resultado consolidado da avaliacao, pronto para virar log, alerta ou saida de CLI."""

    avaliado_em: datetime
    fontes: tuple[AvaliacaoFonte, ...]

    @property
    def violacoes_pipeline(self) -> tuple[AvaliacaoFonte, ...]:
        return tuple(item for item in self.fontes if item.violou_sla_pipeline)

    @property
    def violacoes_fonte(self) -> tuple[AvaliacaoFonte, ...]:
        # Uma fonte ja contada como atraso do pipeline nao entra aqui: a causa raiz e nossa, e
        # reportar as duas coisas duplicaria o mesmo incidente.
        return tuple(
            item for item in self.fontes if item.violou_sla_fonte and not item.violou_sla_pipeline
        )

    @property
    def severidade(self) -> str:
        if self.violacoes_pipeline:
            return SEVERIDADE_PIPELINE
        if self.violacoes_fonte:
            return SEVERIDADE_FONTE
        return SEVERIDADE_OK

    @property
    def saudavel(self) -> bool:
        """Verdadeiro quando NADA que esteja sob nosso controle esta fora do SLO."""
        return not self.violacoes_pipeline

    def resumo(self) -> str:
        linhas = [f"Frescor avaliado em {self.avaliado_em:%Y-%m-%d %H:%M} UTC"]
        linhas += [f"  {item.mensagem()}" for item in self.fontes]
        return "\n".join(linhas)

    def to_dict(self) -> dict[str, Any]:
        return {
            "avaliado_em": self.avaliado_em.isoformat(),
            "severidade": self.severidade,
            "saudavel": self.saudavel,
            "fontes": [
                {
                    "fonte": item.fonte,
                    "medicao_em": item.medicao_em.isoformat(),
                    "event_time_mais_recente": item.event_time_mais_recente.isoformat(),
                    "ultimo_ingest_time": item.ultimo_ingest_time.isoformat(),
                    "atraso_da_fonte_horas": item.atraso_da_fonte_horas,
                    "atraso_do_pipeline_horas": item.atraso_do_pipeline_horas,
                    "idade_do_dado_horas": item.idade_do_dado_horas,
                    "sla_ingestao_horas": item.sla_ingestao_horas,
                    "sla_frescor_fonte_horas": item.sla_frescor_fonte_horas,
                    "situacao": item.situacao,
                    "violou_sla_pipeline": item.violou_sla_pipeline,
                    "violou_sla_fonte": item.violou_sla_fonte,
                    "mensagem": item.mensagem(),
                }
                for item in self.fontes
            ],
        }


class MetricasIndisponiveis(RuntimeError):
    """A tabela de metricas ainda nao existe; o pipeline de transformacao nao rodou."""


def ultimas_medicoes(linhas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mantem apenas a medicao mais recente de cada fonte.

    A tabela e uma serie temporal: julgar sobre TODAS as medicoes reprovaria por causa de um
    atraso que ja foi resolvido dias atras.
    """
    recentes: dict[str, dict[str, Any]] = {}
    for linha in linhas:
        fonte = linha["fonte"]
        atual = recentes.get(fonte)
        if atual is None or linha["medicao_em"] > atual["medicao_em"]:
            recentes[fonte] = linha
    return [recentes[fonte] for fonte in sorted(recentes)]


def avaliar_linhas(linhas: list[dict[str, Any]], *, avaliado_em: datetime) -> RelatorioFrescor:
    """Constroi o relatorio a partir de linhas ja lidas da tabela de metricas."""
    fontes = tuple(
        AvaliacaoFonte(
            fonte=linha["fonte"],
            medicao_em=linha["medicao_em"],
            event_time_mais_recente=linha["event_time_mais_recente"],
            ultimo_ingest_time=linha["ultimo_ingest_time"],
            atraso_da_fonte_horas=int(linha["atraso_da_fonte_horas"]),
            atraso_do_pipeline_horas=int(linha["atraso_do_pipeline_horas"]),
            idade_do_dado_horas=int(linha["idade_do_dado_horas"]),
            sla_ingestao_horas=int(linha["sla_ingestao_horas"]),
            sla_frescor_fonte_horas=int(linha["sla_frescor_fonte_horas"]),
            situacao=linha["situacao"],
        )
        for linha in ultimas_medicoes(linhas)
    )
    return RelatorioFrescor(avaliado_em=avaliado_em, fontes=fontes)


def avaliar_frescor(settings: Settings) -> RelatorioFrescor:
    """Le `gold.metricas_frescor` no Iceberg e julga a medicao mais recente de cada fonte.

    A leitura e da tabela PUBLICADA, e nao do DuckDB: e ela que os consumidores enxergam, e um
    alerta deve descrever o mundo que os outros veem, nao um estado intermediario da nossa
    maquina de transformacao.
    """
    if not table_exists(settings, TABELA_METRICAS):
        raise MetricasIndisponiveis(
            f"{TABELA_METRICAS} nao existe no catalogo Iceberg. Rode "
            "`pharma-pipeline transform build` e `pharma-pipeline publish gold` antes."
        )

    linhas = read_table(settings, TABELA_METRICAS).to_pylist()
    if not linhas:
        raise MetricasIndisponiveis(f"{TABELA_METRICAS} existe, mas esta vazia.")

    return avaliar_linhas(linhas, avaliado_em=datetime.now(UTC))
