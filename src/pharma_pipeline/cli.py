from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import CONTRACTS
from pharma_pipeline.iceberg import list_snapshots, query_table, row_count, sync_bronze_to_iceberg
from pharma_pipeline.ingestion import ingest_source


def _date(value: str):
    from datetime import date

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use uma data YYYY-MM-DD.") from exc


def _datetime(value: str):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use um timestamp ISO 8601.") from exc


def _json_default(value: Any):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, default=_json_default))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pharma-pipeline",
        description="Ingestao batch didatica: APIs -> Parquet bronze -> Iceberg no MinIO.",
    )
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Extrai uma fonte com dlt e grava Parquet na bronze.")
    ingest.add_argument("source", choices=CONTRACTS)
    ingest.add_argument("--initial-date", type=_date)
    ingest.add_argument("--end-date", type=_date)
    ingest.add_argument("--page-size", type=int)
    ingest.add_argument("--start-page", type=int, default=1)
    ingest.add_argument("--max-pages", type=int)
    ingest.add_argument("--pipeline-suffix", default="")

    sync = sub.add_parser("sync", help="Faz UPSERT dos Parquets bronze em uma tabela Iceberg.")
    sync.add_argument("source", choices=CONTRACTS)

    run = sub.add_parser("run", help="Executa ingest e sync na sequencia.")
    run.add_argument("source", choices=CONTRACTS)
    run.add_argument("--initial-date", type=_date)
    run.add_argument("--end-date", type=_date)
    run.add_argument("--page-size", type=int)
    run.add_argument("--start-page", type=int, default=1)
    run.add_argument("--max-pages", type=int)
    run.add_argument("--pipeline-suffix", default="")

    snapshots = sub.add_parser("snapshots", help="Lista o historico de snapshots Iceberg.")
    snapshots.add_argument("source", choices=CONTRACTS)

    query = sub.add_parser("query", help="Consulta o snapshot atual ou faz time travel.")
    query.add_argument("source", choices=CONTRACTS)
    group = query.add_mutually_exclusive_group()
    group.add_argument("--snapshot-id", type=int)
    group.add_argument("--as-of", type=_datetime)
    query.add_argument("--limit", type=int, default=10)
    query.add_argument(
        "--columns",
        help="Colunas separadas por virgula; omita para retornar o payload completo.",
    )

    verify = sub.add_parser(
        "verify-idempotency", help="Confirma que um sync repetido nao muda a tabela."
    )
    verify.add_argument("source", choices=CONTRACTS)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    settings = Settings.from_env()

    if args.command in {"ingest", "run"}:
        result = ingest_source(
            settings,
            args.source,
            initial_date=args.initial_date,
            end_date=args.end_date,
            page_size=args.page_size,
            start_page=args.start_page,
            max_pages=args.max_pages,
            pipeline_suffix=args.pipeline_suffix,
        )
        _print(
            {
                "stage": "ingestion",
                "source": result.source,
                "pipeline_name": result.pipeline_name,
                "load_ids": result.load_ids,
                "rows_loaded": result.rows_loaded,
            }
        )
        if args.command == "ingest":
            return

    if args.command in {"sync", "run"}:
        _print({"stage": "iceberg_sync", **asdict(sync_bronze_to_iceberg(settings, args.source))})
    elif args.command == "snapshots":
        _print(list_snapshots(settings, args.source))
    elif args.command == "query":
        _print(
            query_table(
                settings,
                args.source,
                snapshot_id=args.snapshot_id,
                as_of=args.as_of,
                limit=args.limit,
                columns=(
                    tuple(column.strip() for column in args.columns.split(","))
                    if args.columns
                    else None
                ),
            )
        )
    elif args.command == "verify-idempotency":
        before = list_snapshots(settings, args.source)
        count_before = row_count(settings, args.source)
        sync = sync_bronze_to_iceberg(settings, args.source)
        after = list_snapshots(settings, args.source)
        count_after = row_count(settings, args.source)
        passed = (
            count_before == count_after
            and len(before) == len(after)
            and not sync.snapshot_created
            and sync.rows_inserted == 0
            and sync.rows_updated == 0
        )
        _print(
            {
                "passed": passed,
                "rows_before": count_before,
                "rows_after": count_after,
                "snapshots_before": len(before),
                "snapshots_after": len(after),
                "sync": asdict(sync),
            }
        )
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
