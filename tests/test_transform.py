"""Testes da montagem da linha de comando do dbt.

A ordem dos argumentos ja causou uma falha real: `dbt docs` e um grupo de comandos, e colocar
`--project-dir` antes de `generate` faz o dbt recusar com `No such option`. Estes testes fixam
o contrato para que a regressao nao volte silenciosamente.
"""

from __future__ import annotations

from pharma_pipeline.config import Settings
from pharma_pipeline.transform import build_dbt_args, dbt_environment, liberar_conexao_duckdb


def _settings() -> Settings:
    return Settings.from_env()


def test_subcomando_vem_antes_das_flags() -> None:
    args = build_dbt_args(_settings(), "docs generate")

    assert args[0] == "docs"
    assert args[1] == "generate"
    assert args.index("--project-dir") > 1


def test_comando_simples_mantem_um_unico_token() -> None:
    args = build_dbt_args(_settings(), "build")

    assert args[0] == "build"
    assert "--project-dir" in args
    assert "--profiles-dir" in args


def test_seletores_sao_repassados() -> None:
    args = build_dbt_args(_settings(), "run", select="gold", exclude="rxnorm_mapping")

    assert args[args.index("--select") + 1] == "gold"
    assert args[args.index("--exclude") + 1] == "rxnorm_mapping"


def test_full_refresh_apenas_em_comandos_que_materializam() -> None:
    assert "--full-refresh" in build_dbt_args(_settings(), "build", full_refresh=True)
    assert "--full-refresh" in build_dbt_args(_settings(), "run", full_refresh=True)
    # `test` e `docs` nao aceitam a flag; passa-la faria o dbt falhar na partida.
    assert "--full-refresh" not in build_dbt_args(_settings(), "test", full_refresh=True)
    assert "--full-refresh" not in build_dbt_args(_settings(), "docs generate", full_refresh=True)


def test_seed_aceita_full_refresh() -> None:
    """Regressao: a flag era descartada em `seed`, e silenciosamente.

    Quando as colunas de um seed mudam, o dbt nao altera a tabela existente -- ele tenta
    carregar o CSV novo na estrutura antiga e falha com um erro de dialeto CSV que nao cita a
    causa. `--full-refresh` e a unica saida, e ela precisa chegar ao dbt.
    """
    assert "--full-refresh" in build_dbt_args(_settings(), "seed", full_refresh=True)


def test_liberar_conexao_e_seguro_sem_dbt_em_execucao() -> None:
    """Chamar a liberacao quando nao ha conexao aberta nao pode explodir.

    Ela roda num `finally`, inclusive quando o dbt falhou antes de abrir qualquer conexao.
    Se levantasse excecao ali, mascararia o erro real do dbt.
    """
    liberar_conexao_duckdb()
    liberar_conexao_duckdb()  # idempotente


def test_liberacao_nao_cria_ambiente_novo() -> None:
    """Regressao: a liberacao nao pode INSTANCIAR uma conexao para depois fecha-la.

    O acessor publico `DuckDBConnectionManager.env()` cria um ambiente quando nao existe --
    exatamente o oposto do desejado. A liberacao precisa inspecionar o cache sem popula-lo,
    senao ela abriria o arquivo DuckDB que veio justamente liberar.
    """
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager

    liberar_conexao_duckdb()

    assert getattr(DuckDBConnectionManager, "_ENV", None) is None


def test_ambiente_do_dbt_deriva_do_settings() -> None:
    """O profiles.yml so le env_var; nenhuma credencial e versionada."""
    settings = _settings()
    ambiente = dbt_environment(settings)

    assert ambiente["PHARMA_DUCKDB_PATH"] == str(settings.duckdb_path)
    assert ambiente["PHARMA_S3_ACCESS_KEY_ID"] == settings.minio_user
    assert ambiente["PHARMA_ICEBERG_WAREHOUSE"].startswith("s3://")
    # Artefatos do dbt ficam fora do repositorio.
    assert ambiente["DBT_TARGET_PATH"] == str(settings.dbt_target_dir)
