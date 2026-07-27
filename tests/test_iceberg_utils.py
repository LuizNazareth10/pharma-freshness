from datetime import UTC, datetime

import pyarrow as pa

from pharma_pipeline.iceberg import _changed_rows, _deduplicate, _without_dlt_internal_columns


def test_removes_only_dlt_internal_columns() -> None:
    table = pa.Table.from_pylist([{"setid": "a", "_dlt_id": "technical", "raw_payload": "{}"}])
    result = _without_dlt_internal_columns(table)
    assert result.column_names == ["setid", "raw_payload"]


def test_deduplicate_keeps_latest_ingestion() -> None:
    older = datetime(2026, 7, 27, 10, tzinfo=UTC)
    newer = datetime(2026, 7, 27, 11, tzinfo=UTC)
    table = pa.Table.from_pylist(
        [
            {"setid": "a", "spl_version": 1, "ingest_time": older},
            {"setid": "a", "spl_version": 2, "ingest_time": newer},
            {"setid": "b", "spl_version": 1, "ingest_time": older},
        ]
    )

    result = _deduplicate(table, "setid")
    rows = {row["setid"]: row for row in result.to_pylist()}

    assert result.num_rows == 2
    assert rows["a"]["spl_version"] == 2


def test_changed_rows_ignores_reingestion_time_but_keeps_payload_change() -> None:
    old_time = datetime(2026, 7, 27, 10, tzinfo=UTC)
    new_time = datetime(2026, 7, 27, 11, tzinfo=UTC)
    current = pa.Table.from_pylist(
        [
            {"id": "same", "ingest_time": old_time, "raw_payload": '{"v":1}'},
            {"id": "changed", "ingest_time": old_time, "raw_payload": '{"v":1}'},
        ]
    )
    incoming = pa.Table.from_pylist(
        [
            {"id": "same", "ingest_time": new_time, "raw_payload": '{"v":1}'},
            {"id": "changed", "ingest_time": new_time, "raw_payload": '{"v":2}'},
            {"id": "new", "ingest_time": new_time, "raw_payload": '{"v":1}'},
        ]
    )

    result = _changed_rows(incoming, current, "id")

    assert {row["id"] for row in result.to_pylist()} == {"changed", "new"}
