"""Publicacao das camadas silver e gold do DuckDB para tabelas Iceberg no MinIO.

Por que a publicacao e um passo separado do dbt
----------------------------------------------
O dbt-duckdb transforma; ele nao escreve Iceberg. Poderiamos deixar os modelos apenas no
arquivo DuckDB local, mas esse arquivo nao tem snapshot, nao tem time travel e nao e legivel
por outro motor. A camada de armazenamento do projeto e o Iceberg sobre o MinIO -- e e la que
silver e gold precisam existir para que a Fase 4 possa consultar o estado de ontem.

A divisao de papeis fica igual a da Fase 2:
    motor de transformacao  -> DuckDB (efemero, reconstruivel)
    armazenamento de estado -> Iceberg (transacional, versionado)

O UPSERT reutiliza exatamente a mesma funcao que publica a bronze. Assim, a garantia de
idempotencia e a mesma nas tres camadas, e nao tres implementacoes parecidas.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import LakeTable, lake_table, tables_in_layer
from pharma_pipeline.iceberg import UpsertResult, upsert_arrow

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PublishResult:
    table: str
    rows_read: int
    upsert: UpsertResult

    @property
    def unchanged(self) -> bool:
        return self.upsert.unchanged


class TabelaAusenteNoDuckDB(RuntimeError):
    """Modelo esperado nao existe no DuckDB; o dbt provavelmente ainda nao rodou."""


def _connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    if not settings.duckdb_path.exists():
        raise TabelaAusenteNoDuckDB(
            f"Banco DuckDB nao encontrado em {settings.duckdb_path}. "
            "Rode `pharma-pipeline transform` antes de publicar."
        )
    connection = duckdb.connect(str(settings.duckdb_path), read_only=True)
    # A sessao le e exporta timestamps com fuso usando o fuso local da maquina. Fixar UTC faz
    # a publicacao produzir o mesmo resultado em qualquer maquina, independente do fuso.
    connection.execute("SET TimeZone = 'UTC'")
    return connection


def _read_model(connection: duckdb.DuckDBPyConnection, table: LakeTable):
    existe = connection.execute(
        """
        select count(*) from information_schema.tables
        where table_schema = ? and table_name = ?
        """,
        [table.layer, table.name],
    ).fetchone()[0]
    if not existe:
        raise TabelaAusenteNoDuckDB(
            f"{table.dbt_relation} nao existe no DuckDB. "
            "Rode `pharma-pipeline transform` para materializar os modelos."
        )
    # `fetch_arrow_table` materializa a tabela; `arrow()` devolveria um RecordBatchReader,
    # que e consumido uma unica vez e nao expoe o schema completo antecipadamente.
    return connection.table(f"{table.layer}.{table.name}").fetch_arrow_table()


def publish_table(
    settings: Settings,
    identifier: str,
    *,
    recreate: bool = False,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> PublishResult:
    """Publica um unico modelo do DuckDB em sua tabela Iceberg."""
    table = lake_table(identifier)
    if table.layer == "bronze":
        raise ValueError(
            "A bronze e publicada por `pharma-pipeline sync`, a partir dos Parquet imutaveis. "
            "Republica-la a partir do DuckDB inverteria a direcao do fluxo."
        )

    own_connection = connection is None
    connection = connection or _connect(settings)
    try:
        arrow = _read_model(connection, table)
    finally:
        if own_connection:
            connection.close()

    result = upsert_arrow(
        settings,
        table.identifier,
        arrow,
        join_cols=table.join_cols,
        snapshot_properties={
            "layer": table.layer,
            "model": table.name,
            "grain": table.grain,
            "published-at-utc": datetime.now(UTC).isoformat(),
        },
        recreate=recreate,
    )
    LOGGER.info(
        "%s: %d linhas lidas, %d inseridas, %d atualizadas.",
        table.identifier,
        arrow.num_rows,
        result.rows_inserted,
        result.rows_updated,
    )
    return PublishResult(table=table.identifier, rows_read=arrow.num_rows, upsert=result)


def publish_layer(settings: Settings, layer: str, *, recreate: bool = False) -> list[PublishResult]:
    """Publica todos os modelos de uma camada, reaproveitando uma unica conexao DuckDB."""
    connection = _connect(settings)
    try:
        return [
            publish_table(settings, table.identifier, recreate=recreate, connection=connection)
            for table in tables_in_layer(layer)
        ]
    finally:
        connection.close()
