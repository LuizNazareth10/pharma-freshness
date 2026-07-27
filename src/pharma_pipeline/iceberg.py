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
from pharma_pipeline.contracts import contract_for


@dataclass(frozen=True, slots=True)
class SyncResult:
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


def _catalog(settings: Settings) -> SqlCatalog:
    catalog_path = Path(settings.iceberg_catalog_path).resolve().as_posix()
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{catalog_path}",
        warehouse=f"s3://{settings.minio_bucket}/{settings.iceberg_warehouse_prefix}",
        **{
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_user,
            "s3.secret-access-key": settings.minio_password,
            "s3.region": settings.aws_region,
            "s3.force-virtual-addressing": "false",
        },
    )


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


def _deduplicate(data: pa.Table, primary_key: str) -> pa.Table:
    """Mantem a versao de maior ingest_time por chave; adequado ao volume do laboratorio."""
    latest: dict[Any, dict[str, Any]] = {}
    for row in data.to_pylist():
        key = row[primary_key]
        previous = latest.get(key)
        if previous is None or row["ingest_time"] > previous["ingest_time"]:
            latest[key] = row
    return pa.Table.from_pylist(list(latest.values()), schema=data.schema)


def _changed_rows(incoming: pa.Table, current: pa.Table, primary_key: str) -> pa.Table:
    """Retem chaves novas ou payloads realmente alterados, ignorando novo horario de releitura."""
    current_payloads = {row[primary_key]: row["raw_payload"] for row in current.to_pylist()}
    changed = [
        row
        for row in incoming.to_pylist()
        if current_payloads.get(row[primary_key]) != row["raw_payload"]
    ]
    return pa.Table.from_pylist(changed, schema=incoming.schema)


def sync_bronze_to_iceberg(settings: Settings, source: str) -> SyncResult:
    contract = contract_for(source)
    raw, files = _read_bronze(settings, source)
    incoming = _deduplicate(_without_dlt_internal_columns(raw), contract.primary_key)
    catalog = _catalog(settings)
    catalog.create_namespace_if_not_exists("bronze")
    identifier = _identifier(source)

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

    before = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    candidates = incoming
    if before is not None:
        current = table.scan(selected_fields=(contract.primary_key, "raw_payload")).to_arrow()
        candidates = _changed_rows(incoming, current, contract.primary_key)

    rows_inserted = 0
    rows_updated = 0
    if candidates.num_rows:
        result = table.upsert(
            candidates,
            join_cols=[contract.primary_key],
            snapshot_properties={
                "source": source,
                "pipeline": "pharma-freshness",
                "committed-at-utc": datetime.now(UTC).isoformat(),
            },
        )
        rows_inserted = result.rows_inserted
        rows_updated = result.rows_updated
        table.refresh()
    after = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    return SyncResult(
        source=source,
        table_name=identifier,
        parquet_files=len(files),
        input_rows=raw.num_rows,
        deduplicated_rows=incoming.num_rows,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        snapshot_before=before,
        snapshot_after=after,
    )


def list_snapshots(settings: Settings, source: str) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(_identifier(source))
    rows = table.inspect.snapshots().to_pylist()
    for row in rows:
        row["committed_at"] = row["committed_at"].isoformat()
        row["summary"] = dict(row["summary"] or [])
    return rows


def query_table(
    settings: Settings,
    source: str,
    *,
    snapshot_id: int | None = None,
    as_of: datetime | None = None,
    limit: int = 10,
    columns: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(_identifier(source))
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


def row_count(settings: Settings, source: str, snapshot_id: int | None = None) -> int:
    table = _catalog(settings).load_table(_identifier(source))
    return table.scan(snapshot_id=snapshot_id).count()
