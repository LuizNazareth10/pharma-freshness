from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime
from typing import Any

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import CONTRACTS, LAKE_TABLES, PUBLISHABLE_LAYERS
from pharma_pipeline.iceberg import list_snapshots, query_table, row_count, sync_bronze_to_iceberg
from pharma_pipeline.ingestion import ingest_source
from pharma_pipeline.publish import publish_layer, publish_table
from pharma_pipeline.transform import run_dbt


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
        description=(
            "Pipeline de farmacovigilancia: APIs -> Parquet bronze -> Iceberg -> "
            "modelos dbt (silver/gold) -> Iceberg publicado."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- Fase 2: ingestao ---------------------------------------------------------------
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

    # ---- Fase 3: transformacao, publicacao e qualidade ----------------------------------
    transform = sub.add_parser(
        "transform",
        help="Roda o dbt (silver e gold) no DuckDB, lendo a bronze Iceberg.",
    )
    transform.add_argument(
        "dbt_command",
        nargs="?",
        default="build",
        choices=["build", "run", "test", "seed", "compile", "docs", "parse"],
        help="Comando do dbt. `build` roda modelos e testes na ordem do grafo.",
    )
    transform.add_argument("--select", help="Seletor de nos do dbt, por exemplo `gold`.")
    transform.add_argument("--exclude", help="Nos a excluir, por exemplo `rxnorm_mapping`.")
    transform.add_argument(
        "--full-refresh",
        action="store_true",
        help="Recria os modelos incrementais do zero em vez de acrescentar.",
    )
    transform.add_argument(
        "--serve",
        action="store_true",
        help="Com `docs`: sobe o site local do lineage apos gerar o catalogo.",
    )

    publish = sub.add_parser(
        "publish", help="Publica os modelos do DuckDB como tabelas Iceberg no MinIO."
    )
    publish.add_argument(
        "alvo",
        help="Camada (`silver`, `gold`) ou tabela especifica (`gold.fato_evento_adverso`).",
    )
    publish.add_argument(
        "--recreate",
        action="store_true",
        help="Descarta e recria a tabela Iceberg. Use quando o schema do modelo mudar.",
    )

    expectations = sub.add_parser(
        "expectations",
        help="Valida o contrato das tabelas Iceberg publicadas com Great Expectations.",
    )
    expectations.add_argument(
        "--table",
        action="append",
        help="Tabela a validar; repetivel. Omita para validar os fatos da gold.",
    )

    # ---- Fase 4: orquestracao e observabilidade -----------------------------------------
    freshness = sub.add_parser(
        "freshness",
        help="Avalia o staleness gap por fonte contra os SLOs declarados.",
    )
    freshness.add_argument(
        "--fail-on-breach",
        action="store_true",
        help=(
            "Encerra com codigo 1 se o SLO do PIPELINE for violado. O atraso da FONTE nunca "
            "falha: nao esta sob nosso controle e um alerta que vive vermelho e ignorado."
        ),
    )
    freshness.add_argument(
        "--formato",
        choices=["json", "texto"],
        default="json",
        help="`texto` produz um resumo legivel, util no log de uma tarefa do Airflow.",
    )

    sub.add_parser(
        "compact",
        help=(
            "Compacta o banco DuckDB. O arquivo cresce a cada --full-refresh e nunca "
            "devolve espaco ao disco sozinho."
        ),
    )

    # ---- inspecao do lakehouse -----------------------------------------------------------
    snapshots = sub.add_parser("snapshots", help="Lista o historico de snapshots Iceberg.")
    snapshots.add_argument("table", help="Fonte bronze (`faers`) ou tabela (`gold.dim_farmaco`).")

    query = sub.add_parser("query", help="Consulta o snapshot atual ou faz time travel.")
    query.add_argument("table", help="Fonte bronze (`faers`) ou tabela (`gold.dim_farmaco`).")
    group = query.add_mutually_exclusive_group()
    group.add_argument("--snapshot-id", type=int)
    group.add_argument("--as-of", type=_datetime)
    query.add_argument("--limit", type=int, default=10)
    query.add_argument(
        "--columns",
        help="Colunas separadas por virgula; omita para retornar o payload completo.",
    )

    sub.add_parser("tables", help="Lista as tabelas conhecidas do lakehouse e seu grao.")

    verify = sub.add_parser(
        "verify-idempotency", help="Confirma que um sync repetido nao muda a tabela."
    )
    verify.add_argument("source", choices=CONTRACTS)
    return parser


def _cmd_transform(settings: Settings, args) -> None:
    # `docs` sozinho nao e um comando do dbt; o util aqui e gerar o catalogo de linhagem.
    command = "docs generate" if args.dbt_command == "docs" else args.dbt_command
    if args.serve and args.dbt_command != "docs":
        raise SystemExit("--serve so se aplica ao comando `docs`.")

    result = run_dbt(
        settings,
        command,
        select=args.select,
        exclude=args.exclude,
        full_refresh=args.full_refresh,
    )
    _print(
        {
            "stage": "transform",
            "success": result.success,
            "nodes_executed": result.nodes_executed,
            "failures": list(result.failures),
        }
    )
    if not result.success:
        raise SystemExit(1)

    if args.serve:
        # 8082: a 8080 costuma estar ocupada (Docker Desktop / outros labs); a 8081 e o Airflow.
        print("Servindo dbt docs em http://localhost:8082 (Ctrl+C para parar).", flush=True)
        run_dbt(settings, "docs serve", extra_args=("--port", "8082"))



def _cmd_publish(settings: Settings, args) -> None:
    if args.alvo in PUBLISHABLE_LAYERS:
        results = publish_layer(settings, args.alvo, recreate=args.recreate)
    else:
        results = [publish_table(settings, args.alvo, recreate=args.recreate)]

    _print(
        {
            "stage": "publish",
            "target": args.alvo,
            "tables": [
                {
                    "table": item.table,
                    "rows_read": item.rows_read,
                    "rows_inserted": item.upsert.rows_inserted,
                    "rows_updated": item.upsert.rows_updated,
                    "created_table": item.upsert.created_table,
                    "snapshot_created": item.upsert.snapshot_created,
                    "unchanged": item.unchanged,
                }
                for item in results
            ],
        }
    )


def _cmd_expectations(settings: Settings, args) -> None:
    from pharma_pipeline.quality import TABELAS_VALIDADAS, validar_todas

    alvos = tuple(args.table) if args.table else TABELAS_VALIDADAS
    validations = validar_todas(settings, alvos)
    _print(
        {
            "stage": "expectations",
            "passed": all(item.success for item in validations),
            "tables": [
                {
                    "table": item.table,
                    "rows": item.rows,
                    "success": item.success,
                    "expectations": len(item.outcomes),
                    "failures": [asdict(failure) for failure in item.failures],
                }
                for item in validations
            ],
        }
    )
    if not all(item.success for item in validations):
        raise SystemExit(1)


def _cmd_freshness(settings: Settings, args) -> None:
    from pharma_pipeline.freshness import avaliar_frescor

    relatorio = avaliar_frescor(settings)

    if args.formato == "texto":
        print(relatorio.resumo())
    else:
        _print({"stage": "freshness", **relatorio.to_dict()})

    # O atraso da fonte e reportado, mas nao derruba a execucao: veja a docstring de
    # `pharma_pipeline.freshness` para o porque dessa assimetria.
    if args.fail_on_breach and not relatorio.saudavel:
        raise SystemExit(1)


def _cmd_verify_idempotency(settings: Settings, args) -> None:
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


def main() -> None:
    # O console do Windows usa cp1252 por padrao e quebraria ao imprimir caracteres presentes
    # em descricoes de produto vindas das APIs.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

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
    elif args.command == "transform":
        _cmd_transform(settings, args)
    elif args.command == "publish":
        _cmd_publish(settings, args)
    elif args.command == "expectations":
        _cmd_expectations(settings, args)
    elif args.command == "freshness":
        _cmd_freshness(settings, args)
    elif args.command == "compact":
        from pharma_pipeline.maintenance import compactar_duckdb

        resultado = compactar_duckdb(settings)
        _print(
            {
                "stage": "compact",
                "path": resultado.caminho,
                "mb_antes": round(resultado.bytes_antes / 1048576, 1),
                "mb_depois": round(resultado.bytes_depois / 1048576, 1),
                "reducao_percentual": resultado.reducao_percentual,
            }
        )
    elif args.command == "snapshots":
        _print(list_snapshots(settings, args.table))
    elif args.command == "tables":
        _print(
            [
                {
                    "table": table.identifier,
                    "chave": list(table.join_cols),
                    "grao": table.grain,
                }
                for table in LAKE_TABLES.values()
            ]
        )
    elif args.command == "query":
        _print(
            query_table(
                settings,
                args.table,
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
        _cmd_verify_idempotency(settings, args)


if __name__ == "__main__":
    main()
