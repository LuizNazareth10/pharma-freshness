from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import LakeTable, contract_for, lake_table

# Colunas tecnicas que mudam a cada extracao e, por isso, nao indicam mudanca real do dado.
_VOLATILE_BRONZE_COLUMNS = ("ingest_time", "extraction_id")


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Resultado de um UPSERT em uma tabela Iceberg."""

    table_name: str
    input_rows: int
    candidate_rows: int
    rows_inserted: int
    rows_updated: int
    snapshot_before: int | None
    snapshot_after: int | None
    created_table: bool = False

    @property
    def snapshot_created(self) -> bool:
        return self.snapshot_after is not None and self.snapshot_after != self.snapshot_before

    @property
    def unchanged(self) -> bool:
        return not self.snapshot_created and self.rows_inserted == 0 and self.rows_updated == 0


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Resultado da sincronizacao bronze Parquet -> tabela Iceberg."""

    source: str
    table_name: str
    parquet_files: int
    input_rows: int
    deduplicated_rows: int
    rows_inserted: int
    rows_updated: int
    snapshot_before: int | None
    snapshot_after: int | None

    @property
    def snapshot_created(self) -> bool:
        return self.snapshot_after is not None and self.snapshot_after != self.snapshot_before


def catalog_properties(settings: Settings) -> dict[str, str]:
    """Propriedades do catalogo Iceberg.

    Fonte unica de verdade: o CLI usa este dicionario e tambem o exporta como variaveis de
    ambiente para o `profiles.yml` do dbt, evitando que as duas configuracoes divirjam.
    """
    catalog_path = Path(settings.iceberg_catalog_path).resolve().as_posix()
    return {
        "uri": f"sqlite:///{catalog_path}",
        "warehouse": f"s3://{settings.minio_bucket}/{settings.iceberg_warehouse_prefix}",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": settings.minio_endpoint,
        "s3.access-key-id": settings.minio_user,
        "s3.secret-access-key": settings.minio_password,
        "s3.region": settings.aws_region,
        "s3.force-virtual-addressing": "false",
    }


def _catalog(settings: Settings) -> SqlCatalog:
    return SqlCatalog("local", **catalog_properties(settings))


def _s3(settings: Settings) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_user,
        secret=settings.minio_password,
        endpoint_url=settings.minio_endpoint,
        client_kwargs={"region_name": settings.aws_region},
        use_ssl=settings.minio_endpoint.startswith("https://"),
    )


def _identifier(source: str) -> str:
    return f"bronze.{contract_for(source).table_name}"


def _read_bronze(settings: Settings, source: str) -> tuple[pa.Table, list[str]]:
    contract = contract_for(source)
    fs = _s3(settings)
    pattern = f"{settings.minio_bucket}/bronze/{source}/**/{contract.table_name}/**/*.parquet"
    files = sorted(fs.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado para {source} em bronze/{source}. Rode a ingestao primeiro."
        )

    tables: list[pa.Table] = []
    for path in files:
        with fs.open(path, "rb") as parquet_file:
            tables.append(pq.read_table(parquet_file))
    return pa.concat_tables(tables, promote_options="default"), files


def _without_dlt_internal_columns(data: pa.Table) -> pa.Table:
    keep = [name for name in data.column_names if not name.startswith("_dlt_")]
    return data.select(keep)


def normalize_arrow(data: pa.Table) -> pa.Table:
    """Alinha tipos do Arrow aos que o Iceberg aceita.

    Tres ajustes, todos motivados por diferencas reais entre o que o DuckDB exporta e o que o
    Iceberg aceita:

    - `large_string`/`large_binary` viram `string`/`binary`;
    - timestamps passam para microssegundos, a precisao do tipo Iceberg;
    - timestamps com fuso passam para UTC.

    O ultimo e o mais importante. O DuckDB exporta `timestamp with time zone` marcado com o
    fuso da sessao (por exemplo `America/Sao_Paulo`), e o Iceberg so aceita `timestamptz` em
    UTC. A conversao e apenas de rotulo: o Arrow guarda o instante como epoch, entao nenhum
    horario e deslocado -- o mesmo instante passa a ser lido em UTC, que e como o pipeline
    inteiro raciocina sobre tempo.
    """
    fields = []
    changed = False
    for field in data.schema:
        target = field.type
        if pa.types.is_large_string(target):
            target = pa.string()
        elif pa.types.is_large_binary(target):
            target = pa.binary()
        elif pa.types.is_timestamp(target):
            tz = "UTC" if target.tz is not None else None
            if target.unit != "us" or target.tz != tz:
                target = pa.timestamp("us", tz=tz)
        if target != field.type:
            changed = True
        fields.append(field.with_type(target))
    return data.cast(pa.schema(fields)) if changed else data


def _deduplicate(data: pa.Table, primary_key: str) -> pa.Table:
    """Mantem a versao de maior ingest_time por chave; adequado ao volume do laboratorio."""
    latest: dict[Any, dict[str, Any]] = {}
    for row in data.to_pylist():
        key = row[primary_key]
        previous = latest.get(key)
        if previous is None or row["ingest_time"] > previous["ingest_time"]:
            latest[key] = row
    return pa.Table.from_pylist(list(latest.values()), schema=data.schema)


def _compare_columns(data: pa.Table, join_cols: tuple[str, ...]) -> tuple[str, ...]:
    """Colunas que definem 'a linha mudou de verdade'.

    Na bronze o `raw_payload` e o registro original completo: se ele nao mudou, a fonte nao
    mudou, e reler o mesmo dado nao deve gerar snapshot. Nas camadas derivadas os modelos sao
    deterministicos, entao comparamos todas as colunas fora da chave.
    """
    if "raw_payload" in data.column_names:
        return ("raw_payload",)
    return tuple(
        name
        for name in data.column_names
        if name not in join_cols and name not in _VOLATILE_BRONZE_COLUMNS
    )


def _changed_rows(
    incoming: pa.Table,
    current: pa.Table,
    join_cols: tuple[str, ...],
    compare_cols: tuple[str, ...],
) -> pa.Table:
    """Retem chaves novas ou linhas realmente alteradas, ignorando releitura sem mudanca."""

    def key(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in join_cols)

    def fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in compare_cols)

    current_index = {key(row): fingerprint(row) for row in current.to_pylist()}
    changed = [
        row for row in incoming.to_pylist() if current_index.get(key(row)) != fingerprint(row)
    ]
    return pa.Table.from_pylist(changed, schema=incoming.schema)


def upsert_arrow(
    settings: Settings,
    identifier: str,
    incoming: pa.Table,
    *,
    join_cols: tuple[str, ...],
    snapshot_properties: dict[str, str] | None = None,
    recreate: bool = False,
) -> UpsertResult:
    """Faz UPSERT idempotente de uma tabela Arrow em uma tabela Iceberg.

    Cria namespace e tabela quando ausentes. Linhas byte-identicas as ja publicadas nao geram
    commit, de modo que reexecutar o pipeline nao cria snapshots vazios.
    """
    incoming = normalize_arrow(incoming)
    missing = [column for column in join_cols if column not in incoming.column_names]
    if missing:
        raise ValueError(
            f"{identifier}: colunas de chave ausentes no resultado: {', '.join(missing)}."
        )

    catalog = _catalog(settings)
    namespace = identifier.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)

    created = False
    if recreate and catalog.table_exists(identifier):
        catalog.drop_table(identifier)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=identifier,
            schema=incoming.schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.delete-after-commit.enabled": "false",
            },
        )
        created = True

    before = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    compare_cols = _compare_columns(incoming, join_cols)
    candidates = incoming
    if before is not None:
        current = table.scan(selected_fields=join_cols + compare_cols).to_arrow()
        candidates = _changed_rows(incoming, current, join_cols, compare_cols)

    rows_inserted = 0
    rows_updated = 0
    if candidates.num_rows:
        result = table.upsert(
            candidates,
            join_cols=list(join_cols),
            snapshot_properties={
                "pipeline": "pharma-freshness",
                "committed-at-utc": datetime.now(UTC).isoformat(),
                **(snapshot_properties or {}),
            },
        )
        rows_inserted = result.rows_inserted
        rows_updated = result.rows_updated
        table.refresh()

    after = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    return UpsertResult(
        table_name=identifier,
        input_rows=incoming.num_rows,
        candidate_rows=candidates.num_rows,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        snapshot_before=before,
        snapshot_after=after,
        created_table=created,
    )


def sync_bronze_to_iceberg(settings: Settings, source: str) -> SyncResult:
    """Le os Parquet imutaveis da bronze e publica o estado atual na tabela Iceberg."""
    contract = contract_for(source)
    raw, files = _read_bronze(settings, source)
    incoming = _deduplicate(_without_dlt_internal_columns(raw), contract.primary_key)
    result = upsert_arrow(
        settings,
        _identifier(source),
        incoming,
        join_cols=(contract.primary_key,),
        snapshot_properties={"source": source, "layer": "bronze"},
    )
    return SyncResult(
        source=source,
        table_name=result.table_name,
        parquet_files=len(files),
        input_rows=raw.num_rows,
        deduplicated_rows=incoming.num_rows,
        rows_inserted=result.rows_inserted,
        rows_updated=result.rows_updated,
        snapshot_before=result.snapshot_before,
        snapshot_after=result.snapshot_after,
    )


def resolve(identifier: str) -> LakeTable:
    return lake_table(identifier)


def list_snapshots(settings: Settings, identifier: str) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    rows = table.inspect.snapshots().to_pylist()
    for row in rows:
        row["committed_at"] = row["committed_at"].isoformat()
        row["summary"] = dict(row["summary"] or [])
    return rows


def query_table(
    settings: Settings,
    identifier: str,
    *,
    snapshot_id: int | None = None,
    as_of: datetime | None = None,
    limit: int = 10,
    columns: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    if as_of is not None:
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=UTC)
        snapshot = table.snapshot_as_of_timestamp(int(as_of.timestamp() * 1000))
        if snapshot is None:
            raise ValueError(f"Nao existe snapshot em ou antes de {as_of.isoformat()}.")
        snapshot_id = snapshot.snapshot_id
    selected_fields = columns or ("*",)
    return (
        table.scan(snapshot_id=snapshot_id, selected_fields=selected_fields, limit=limit)
        .to_arrow()
        .to_pylist()
    )


def read_table(settings: Settings, identifier: str) -> pa.Table:
    """Le a tabela publicada inteira; usado pelas validacoes de contrato."""
    return _catalog(settings).load_table(resolve(identifier).identifier).scan().to_arrow()


def row_count(settings: Settings, identifier: str, snapshot_id: int | None = None) -> int:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    return table.scan(snapshot_id=snapshot_id).count()


def table_exists(settings: Settings, identifier: str) -> bool:
    return _catalog(settings).table_exists(resolve(identifier).identifier)
