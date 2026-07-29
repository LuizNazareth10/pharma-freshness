"""Validacao de contrato com Great Expectations sobre as tabelas Iceberg PUBLICADAS.

Por que existem dois mecanismos de teste no projeto
--------------------------------------------------
Os testes do dbt e as expectativas do Great Expectations parecem redundantes, mas guardam
fronteiras diferentes:

    dbt test    -> valida a TRANSFORMACAO, dentro do motor, antes de publicar.
                   Falhou? O modelo esta errado. O dado ruim ainda nao saiu do DuckDB.

    Great Exp.  -> valida o CONTRATO, na tabela Iceberg ja gravada no MinIO.
                   Falhou? O que os consumidores enxergam esta errado, mesmo que a
                   transformacao estivesse certa.

A segunda barreira pega o que a primeira nao ve: falha de conversao de tipo na escrita, UPSERT
em chave errada, publicacao parcial por interrupcao, ou uma tabela que ficou para tras porque
o `publish` daquela camada nao foi executado. E a reconciliacao pos-carga do Volume 6.

As expectativas aqui sao deliberadamente as REGRAS DE DOMINIO do projeto, nao uma copia dos
testes do dbt. A principal delas: toda linha precisa citar fonte e data, porque toda resposta
futura da LLM tera de citar fonte e data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import lake_table
from pharma_pipeline.iceberg import read_table, table_exists

LOGGER = logging.getLogger(__name__)

# Tabelas publicadas que carregam o contrato de procedencia do projeto.
TABELAS_VALIDADAS = ("gold.fato_evento_adverso", "gold.fato_recall")


@dataclass(frozen=True, slots=True)
class ExpectationOutcome:
    expectation: str
    column: str | None
    success: bool
    observed: Any = None
    details: str | None = None


@dataclass(frozen=True, slots=True)
class TableValidation:
    table: str
    rows: int
    success: bool
    outcomes: list[ExpectationOutcome] = field(default_factory=list)

    @property
    def failures(self) -> list[ExpectationOutcome]:
        return [outcome for outcome in self.outcomes if not outcome.success]


def _suite_para(identifier: str, contexto: dict[str, Any]) -> list[dict[str, Any]]:
    """Expectativas por tabela, expressas na API de suites do Great Expectations."""
    tabela = lake_table(identifier)
    chave = tabela.join_cols[0]

    comuns: list[dict[str, Any]] = [
        # --- procedencia: o requisito de dominio do projeto -----------------------------
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "fonte"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_time"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_time"}},
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "fonte", "value_set": list(contexto["fontes_validas"])},
        },
        # --- identidade ------------------------------------------------------------------
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": chave}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": chave}},
        {"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        # --- chaves estrangeiras obrigatorias ---------------------------------------------
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id_farmaco"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id_fonte"}},
    ]

    datas = {
        "gold.fato_evento_adverso": "receivedate",
        "gold.fato_recall": "report_date",
    }[identifier]

    comuns.append(
        {
            # Uma data de evento fora da janela plausivel denuncia parsing errado ou fuso
            # trocado -- os dois defeitos que mais corrompem uma metrica de frescor.
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": datas,
                "min_value": contexto["data_minima"],
                "max_value": contexto["data_maxima"],
            },
        }
    )

    if identifier == "gold.fato_evento_adverso":
        comuns += [
            {
                # Dominio do padrao ICH E2B. `mostly` tolera erro de preenchimento pontual na
                # origem sem cegar a suite: em 2026-07-29 um unico relato trouxe o codigo 4,
                # que o padrao nao define. Exigir 100% faria a validacao do contrato reprovar
                # por causa de uma linha em dezenas de milhares; nao exigir nada esconderia uma
                # mudanca real de dominio da fonte. O limiar de 99% separa os dois casos.
                "type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "caracterizacao_codigo",
                    "value_set": [1, 2, 3, None],
                    "mostly": 0.99,
                },
            },
            {
                # Latencia negativa significaria capturar o dado antes de ele existir.
                "type": "expect_column_values_to_be_between",
                "kwargs": {"column": "latencia_atualizacao_horas", "min_value": -24},
            },
        ]
    else:
        comuns.append(
            {
                "type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "classificacao_nivel", "value_set": [1, 2, 3, None]},
            }
        )
    return comuns


def validar_tabela(settings: Settings, identifier: str) -> TableValidation:
    """Roda a suite de expectativas contra a tabela Iceberg publicada."""
    if not table_exists(settings, identifier):
        raise FileNotFoundError(
            f"Tabela {identifier} nao existe no catalogo Iceberg. "
            "Rode `pharma-pipeline publish gold` antes de validar."
        )

    frame = read_table(settings, identifier).to_pandas()
    return validar_dataframe(identifier, frame)


def validar_dataframe(identifier: str, frame) -> TableValidation:
    """Aplica a suite de um identificador a um DataFrame ja carregado.

    Separado de `validar_tabela` para que a suite possa ser exercitada em teste -- inclusive
    com dados propositalmente invalidos, provando que ela realmente reprova o que deve.
    """
    import great_expectations as gx

    contexto = {
        "fontes_validas": ("dailymed", "faers", "res"),
        "data_minima": datetime(1990, 1, 1).date(),
        # Folga de 2 dias: as fontes publicam com precisao de dia e fusos diferentes.
        "data_maxima": (datetime.now(UTC) + timedelta(days=2)).date(),
    }

    # O silenciamento precisa vir ANTES de criar o contexto: e na criacao que o GX registra
    # o diretorio temporario do site de documentacao.
    _silenciar_logs_do_gx()
    gx_context = gx.get_context(mode="ephemeral")
    _silenciar_barras_de_progresso(gx_context)

    fonte_dados = gx_context.data_sources.add_pandas(name=f"pandas_{identifier}")
    ativo = fonte_dados.add_dataframe_asset(name=identifier)
    definicao = ativo.add_batch_definition_whole_dataframe(name=f"lote_{identifier}")

    suite = gx_context.suites.add(gx.ExpectationSuite(name=f"contrato_{identifier}"))
    for spec in _suite_para(identifier, contexto):
        suite.add_expectation(_build_expectation(gx, spec))

    validacao = gx_context.validation_definitions.add(
        gx.ValidationDefinition(name=f"validacao_{identifier}", data=definicao, suite=suite)
    )
    resultado = validacao.run(batch_parameters={"dataframe": frame})

    outcomes = [
        ExpectationOutcome(
            expectation=item["expectation_config"]["type"],
            column=item["expectation_config"]["kwargs"].get("column"),
            success=bool(item["success"]),
            observed=item.get("result", {}).get("observed_value")
            or item.get("result", {}).get("unexpected_count"),
            details=None
            if item["success"]
            else str(item.get("result", {}).get("partial_unexpected_list", ""))[:200],
        )
        for item in resultado.results
    ]
    return TableValidation(
        table=identifier,
        rows=len(frame),
        success=bool(resultado.success),
        outcomes=outcomes,
    )


def _silenciar_logs_do_gx() -> None:
    """Rebaixa para WARNING os loggers informativos do Great Expectations.

    Nao basta ajustar o logger raiz `great_expectations`: alguns modulos do GX definem o
    proprio nivel em INFO no momento do import, e um nivel explicito no filho ignora o do pai.
    Por isso a varredura passa por todos os loggers ja registrados.

    O motivo nao e estetico. Essas mensagens vao para o stderr e, quando um script PowerShell
    roda com `$ErrorActionPreference = "Stop"`, qualquer linha de stderr de um executavel vira
    um registro de erro -- e o passo de qualidade "falha" tendo passado.
    """
    logging.getLogger("great_expectations").setLevel(logging.WARNING)
    for nome, logger in logging.root.manager.loggerDict.items():
        if nome.startswith("great_expectations") and isinstance(logger, logging.Logger):
            if 0 < logger.level < logging.WARNING:
                logger.setLevel(logging.WARNING)


def _silenciar_barras_de_progresso(gx_context) -> None:
    """Desliga as barras de progresso, que iriam para o mesmo console do JSON do CLI."""
    try:
        from great_expectations.data_context.types.base import ProgressBarsConfig

        gx_context.variables.progress_bars = ProgressBarsConfig(
            globally=False, metric_calculations=False
        )
    except Exception:  # noqa: BLE001 - ruido de console nao pode derrubar a validacao
        LOGGER.debug("Nao foi possivel desligar as barras de progresso do GX.", exc_info=True)


def _build_expectation(gx, spec: dict[str, Any]):
    """Instancia a classe de expectativa correspondente ao tipo declarado."""
    nome_classe = "".join(parte.capitalize() for parte in spec["type"].split("_"))
    # `expect_column_values_to_not_be_null` -> `ExpectColumnValuesToNotBeNull`
    classe = getattr(gx.expectations, nome_classe, None)
    if classe is None:
        raise ValueError(f"Expectativa desconhecida: {spec['type']} (classe {nome_classe}).")
    return classe(**spec["kwargs"])


def validar_todas(settings: Settings, tabelas: tuple[str, ...] = TABELAS_VALIDADAS):
    return [validar_tabela(settings, identifier) for identifier in tabelas]
