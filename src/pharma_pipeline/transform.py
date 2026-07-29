"""Execucao do dbt a partir do mesmo `Settings` usado pelo restante do pipeline.

O dbt precisa saber onde fica o banco DuckDB, o catalogo Iceberg e as credenciais do MinIO.
Essas informacoes ja existem em `Settings`. Duplica-las no `profiles.yml` criaria duas fontes
de verdade que divergem no primeiro dia em que alguem muda uma porta.

Aqui a direcao e unica: `Settings` -> variaveis de ambiente -> `profiles.yml`. O arquivo de
perfil so contem `env_var(...)`, portanto nenhum segredo e versionado.
"""

from __future__ import annotations

import gc
import logging
import os
from dataclasses import dataclass
from typing import Any

from pharma_pipeline.config import Settings
from pharma_pipeline.iceberg import catalog_properties

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DbtResult:
    command: tuple[str, ...]
    success: bool
    nodes_executed: int
    failures: tuple[str, ...]


def dbt_environment(settings: Settings) -> dict[str, str]:
    """Variaveis de ambiente consumidas por `transform/profiles.yml`."""
    properties = catalog_properties(settings)
    return {
        "PHARMA_DUCKDB_PATH": str(settings.duckdb_path),
        "PHARMA_ICEBERG_URI": properties["uri"],
        "PHARMA_ICEBERG_WAREHOUSE": properties["warehouse"],
        "PHARMA_S3_ENDPOINT": properties["s3.endpoint"],
        "PHARMA_S3_ACCESS_KEY_ID": properties["s3.access-key-id"],
        "PHARMA_S3_SECRET_ACCESS_KEY": properties["s3.secret-access-key"],
        "PHARMA_S3_REGION": properties["s3.region"],
        # Artefatos do dbt (manifest, logs, catalogo de docs) ficam fora do repositorio.
        "DBT_TARGET_PATH": str(settings.dbt_target_dir),
        "DBT_LOG_PATH": str(settings.dbt_target_dir / "logs"),
    }


def build_dbt_args(
    settings: Settings,
    command: str,
    *,
    select: str | None = None,
    exclude: str | None = None,
    full_refresh: bool = False,
    extra_args: tuple[str, ...] = (),
) -> list[str]:
    """Monta a linha de comando do dbt.

    Os tokens do comando vem primeiro e juntos. `docs` e um GRUPO de comandos: em
    `dbt docs generate`, o `generate` precisa vir imediatamente depois de `docs`, antes de
    qualquer flag. Colocar as flags no meio faz o dbt recusar com
    `No such option '--project-dir'`.
    """
    args: list[str] = [*command.split(), *extra_args]
    args += [
        "--project-dir",
        str(settings.dbt_project_dir),
        "--profiles-dir",
        str(settings.dbt_project_dir),
    ]
    if select:
        args += ["--select", select]
    if exclude:
        args += ["--exclude", exclude]
    # `--full-refresh` so existe nos comandos que materializam algo.
    #
    # `seed` precisa estar nesta lista: quando as COLUNAS de um seed mudam, o dbt nao altera a
    # tabela existente -- ele tenta carregar o CSV novo na tabela antiga e falha com um erro de
    # dialeto CSV que nao menciona a causa real. Descartar a flag aqui deixava
    # `transform seed --full-refresh` sem efeito nenhum, silenciosamente.
    if full_refresh and args[0] in {"run", "build", "seed"}:
        args.append("--full-refresh")
    return args


def liberar_conexao_duckdb() -> None:
    """Fecha a conexao DuckDB que o dbt-duckdb mantem em cache no processo.

    Por que isso e necessario
    -------------------------
    O `dbtRunner` roda no processo atual e o dbt-duckdb guarda o ambiente de conexao num
    singleton. Terminado o `dbt build`, essa conexao continua ABERTA e em modo de ESCRITA.

    O passo seguinte do pipeline, `publish`, abre o mesmo arquivo em modo somente leitura. O
    DuckDB recusa duas conexoes com configuracoes diferentes para o mesmo arquivo:

        Can't open a connection to same database file with a different configuration
        than existing connections

    Rodando pelo CLI o problema nao aparece, porque cada comando e um processo novo. Ele surge
    quando transformacao e publicacao compartilham processo -- exatamente o que acontece em
    `airflow dags test`. Foi assim que ele foi descoberto.

    `close_all_connections()` do dbt-duckdb apenas descarta a referencia ao ambiente; nao fecha
    o arquivo. Por isso fechamos o ambiente antes, quando ele existe.
    """
    try:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    except ImportError:  # pragma: no cover - dbt sempre presente no ambiente do projeto
        return

    # `_ENV` e interno, mas e o unico caminho para fechar sem CRIAR um ambiente novo:
    # o acessor publico `env()` instancia um quando nao existe, que e o oposto do desejado.
    ambiente = getattr(DuckDBConnectionManager, "_ENV", None)
    if ambiente is not None:
        try:
            ambiente.close()
        except Exception:  # noqa: BLE001 - falha ao fechar nao pode derrubar a transformacao
            LOGGER.debug("Nao foi possivel fechar o ambiente DuckDB do dbt.", exc_info=True)

    try:
        DuckDBConnectionManager.close_all_connections()
    except Exception:  # noqa: BLE001
        LOGGER.debug("close_all_connections falhou.", exc_info=True)

    # O DuckDB so libera o arquivo quando o objeto de conexao e finalizado.
    gc.collect()


def run_dbt(
    settings: Settings,
    command: str,
    *,
    select: str | None = None,
    exclude: str | None = None,
    full_refresh: bool = False,
    extra_args: tuple[str, ...] = (),
) -> DbtResult:
    """Invoca o dbt no processo atual.

    Usar o `dbtRunner` em vez de um subprocesso mantem o mesmo interpretador Python, o que
    importa porque o modelo `rxnorm_mapping` importa `pharma_pipeline` e o plugin Iceberg usa
    o PyIceberg instalado neste ambiente.
    """
    from dbt.cli.main import dbtRunner

    os.environ.update(dbt_environment(settings))
    args = build_dbt_args(
        settings,
        command,
        select=select,
        exclude=exclude,
        full_refresh=full_refresh,
        extra_args=extra_args,
    )

    LOGGER.info("Executando dbt: %s", " ".join(args))
    try:
        result = dbtRunner().invoke(args)
    finally:
        liberar_conexao_duckdb()

    if result.exception is not None:
        raise RuntimeError(f"dbt falhou ao iniciar: {result.exception}") from result.exception

    return DbtResult(
        command=tuple(args),
        success=bool(result.success),
        nodes_executed=_count_nodes(result.result),
        failures=_failures(result.result),
    )


def _count_nodes(payload: Any) -> int:
    results = getattr(payload, "results", None)
    return len(results) if results is not None else 0


def _failures(payload: Any) -> tuple[str, ...]:
    results = getattr(payload, "results", None) or []
    falhas = []
    for item in results:
        status = str(getattr(item, "status", ""))
        if status in {"error", "fail", "runtime error"}:
            node = getattr(item, "node", None)
            nome = getattr(node, "name", "<desconhecido>")
            mensagem = (getattr(item, "message", "") or "").splitlines()
            falhas.append(f"{nome}: {status}{' - ' + mensagem[0] if mensagem else ''}")
    return tuple(falhas)
