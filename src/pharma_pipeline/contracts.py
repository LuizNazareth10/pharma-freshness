from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceContract:
    """Contrato de extracao de uma fonte: identidade, watermark e limites da API."""

    name: str
    table_name: str
    primary_key: str
    cursor_field: str
    event_time_field: str
    default_page_size: int
    max_page_size: int


CONTRACTS = {
    "dailymed": SourceContract(
        name="dailymed",
        table_name="dailymed_spls",
        primary_key="setid",
        cursor_field="published_date",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=100,
    ),
    "faers": SourceContract(
        name="faers",
        table_name="faers_events",
        primary_key="safetyreportid",
        cursor_field="receiptdate",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=1000,
    ),
    "res": SourceContract(
        name="res",
        table_name="res_recalls",
        primary_key="recall_number",
        cursor_field="report_date",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=1000,
    ),
}


def contract_for(source: str) -> SourceContract:
    try:
        return CONTRACTS[source]
    except KeyError as exc:
        raise ValueError(f"Fonte desconhecida: {source}. Use dailymed, faers ou res.") from exc


@dataclass(frozen=True, slots=True)
class LakeTable:
    """Contrato de uma tabela publicada no lakehouse Iceberg.

    `join_cols` define a identidade usada no UPSERT. Uma tabela sem chave declarada nao
    pode ser publicada de forma idempotente, por isso a chave e obrigatoria aqui.
    """

    layer: str
    name: str
    join_cols: tuple[str, ...]
    grain: str

    @property
    def identifier(self) -> str:
        return f"{self.layer}.{self.name}"

    @property
    def dbt_relation(self) -> str:
        """Relacao correspondente no DuckDB, materializada pelo dbt."""
        return f"{self.layer}.{self.name}"


_LAKE_TABLES: tuple[LakeTable, ...] = (
    # --- bronze: escrita pelo sincronizador da Fase 2 -------------------------------
    LakeTable(
        layer="bronze",
        name="dailymed_spls",
        join_cols=("setid",),
        grain="Estado mais recente conhecido de um conjunto SPL identificado por setid.",
    ),
    LakeTable(
        layer="bronze",
        name="faers_events",
        join_cols=("safetyreportid",),
        grain="Versao mais recente exposta pelo openFDA de um relato de seguranca.",
    ),
    LakeTable(
        layer="bronze",
        name="res_recalls",
        join_cols=("recall_number",),
        grain="Registro de recall identificado por recall_number.",
    ),
    # --- silver: limpeza, tipagem, explosao e normalizacao --------------------------
    LakeTable(
        layer="silver",
        name="stg_dailymed",
        join_cols=("setid",),
        grain="Uma bula SPL limpa e tipada, deduplicada por setid.",
    ),
    LakeTable(
        layer="silver",
        name="stg_faers",
        join_cols=("safetyreportid",),
        grain="Um relato de evento adverso limpo e tipado, deduplicado por safetyreportid.",
    ),
    LakeTable(
        layer="silver",
        name="stg_faers_drugs",
        join_cols=("safetyreportid", "drug_seq"),
        grain="Uma entrada de medicamento dentro de um relato, na ordem original do array.",
    ),
    LakeTable(
        layer="silver",
        name="stg_faers_reactions",
        join_cols=("safetyreportid", "reaction_seq"),
        grain="Uma reacao relatada dentro de um relato, na ordem original do array.",
    ),
    LakeTable(
        layer="silver",
        name="stg_res",
        join_cols=("recall_number",),
        grain="Um recall limpo e tipado, deduplicado por recall_number.",
    ),
    LakeTable(
        layer="silver",
        name="farmaco_nomes",
        join_cols=("nome_normalizado",),
        grain="Um nome de farmaco distinto observado nas fontes, aguardando normalizacao.",
    ),
    LakeTable(
        layer="silver",
        name="rxnorm_mapping",
        join_cols=("nome_normalizado",),
        grain="Resultado da consulta ao RxNorm para um nome de farmaco distinto.",
    ),
    # --- gold: modelo dimensional ---------------------------------------------------
    LakeTable(
        layer="gold",
        name="dim_farmaco",
        join_cols=("id_farmaco",),
        grain="Uma identidade de farmaco: ingrediente RxNorm resolvido ou nome nao mapeado.",
    ),
    LakeTable(
        layer="gold",
        name="dim_reacao",
        join_cols=("id_reacao",),
        grain="Um termo preferencial MedDRA distinto observado nos relatos.",
    ),
    LakeTable(
        layer="gold",
        name="dim_data",
        join_cols=("id_data",),
        grain="Um dia do calendario.",
    ),
    LakeTable(
        layer="gold",
        name="dim_fonte",
        join_cols=("id_fonte",),
        grain="Um sistema de origem do pipeline.",
    ),
    LakeTable(
        layer="gold",
        name="dim_bula",
        join_cols=("id_bula",),
        grain="A versao corrente de uma bula SPL identificada por setid.",
    ),
    LakeTable(
        layer="gold",
        name="fato_evento_adverso",
        join_cols=("id_evento",),
        grain="Um par farmaco-reacao distinto dentro de um relato FAERS.",
    ),
    LakeTable(
        layer="gold",
        name="fato_recall",
        join_cols=("id_recall",),
        grain="Uma acao de recolhimento identificada por recall_number.",
    ),
)

LAKE_TABLES: dict[str, LakeTable] = {table.identifier: table for table in _LAKE_TABLES}

PUBLISHABLE_LAYERS = ("silver", "gold")


def lake_table(identifier: str) -> LakeTable:
    """Resolve `camada.tabela`, aceitando o nome curto de uma fonte bronze."""
    if identifier in CONTRACTS:
        identifier = f"bronze.{CONTRACTS[identifier].table_name}"
    try:
        return LAKE_TABLES[identifier]
    except KeyError as exc:
        known = ", ".join(sorted(LAKE_TABLES))
        raise ValueError(f"Tabela desconhecida: {identifier}. Conhecidas: {known}.") from exc


def tables_in_layer(layer: str) -> tuple[LakeTable, ...]:
    """Tabelas de uma camada, na ordem de dependencia declarada em `_LAKE_TABLES`."""
    if layer not in {table.layer for table in _LAKE_TABLES}:
        raise ValueError(f"Camada desconhecida: {layer}. Use bronze, silver ou gold.")
    return tuple(table for table in _LAKE_TABLES if table.layer == layer)
