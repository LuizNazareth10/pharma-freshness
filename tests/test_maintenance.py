"""Testes da compactacao do banco DuckDB.

Contexto real: apos algumas dezenas de reconstrucoes com `--full-refresh`, o arquivo do motor
de transformacao chegou a 1,46 GB neste laboratorio e a execucao da DAG morreu com "espaco
insuficiente no disco" -- num ponto que nao tinha nada a ver com a causa. A compactacao reduziu
o arquivo para 818 MB sem perder uma linha.

Estes testes provam as duas propriedades que importam: o espaco morto some e o CONTEUDO
sobrevive. Uma compactacao que encolhe o arquivo perdendo dados seria muito pior do que
nenhuma compactacao.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from pharma_pipeline.maintenance import CompactacaoResult, compactar_duckdb


class _SettingsFake:
    """Substitui `Settings` expondo apenas o que a compactacao consome."""

    def __init__(self, caminho: Path) -> None:
        self.duckdb_path = caminho


def _banco_com_espaco_morto(caminho: Path) -> None:
    """Cria um banco e gera espaco morto reescrevendo a mesma tabela varias vezes."""
    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute(
            "create table dados as select range as n, repeat('x', 500) as texto from range(60000)"
        )
        for _ in range(6):
            # Cada reescrita abandona a versao anterior dentro do arquivo.
            conexao.execute("create or replace table dados as select * from dados")


def test_compactacao_reduz_o_arquivo(tmp_path: Path) -> None:
    caminho = tmp_path / "pharma.duckdb"
    _banco_com_espaco_morto(caminho)

    resultado = compactar_duckdb(_SettingsFake(caminho))

    assert resultado.bytes_depois < resultado.bytes_antes
    assert resultado.bytes_liberados > 0
    assert caminho.stat().st_size == resultado.bytes_depois


def test_compactacao_preserva_os_dados(tmp_path: Path) -> None:
    """A propriedade que realmente importa: nenhuma linha pode sumir."""
    caminho = tmp_path / "pharma.duckdb"
    _banco_com_espaco_morto(caminho)

    with duckdb.connect(str(caminho)) as conexao:
        antes = conexao.execute("select count(*), sum(n) from dados").fetchone()

    compactar_duckdb(_SettingsFake(caminho))

    with duckdb.connect(str(caminho)) as conexao:
        depois = conexao.execute("select count(*), sum(n) from dados").fetchone()

    assert depois == antes


def test_compactacao_preserva_schemas_nomeados(tmp_path: Path) -> None:
    """silver e gold sao schemas proprios; a copia precisa trazer os dois."""
    caminho = tmp_path / "pharma.duckdb"
    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute("create schema silver")
        conexao.execute("create schema gold")
        conexao.execute("create table silver.stg as select 1 as a")
        conexao.execute("create table gold.fato as select 2 as b")

    compactar_duckdb(_SettingsFake(caminho))

    with duckdb.connect(str(caminho)) as conexao:
        assert conexao.execute("select a from silver.stg").fetchone() == (1,)
        assert conexao.execute("select b from gold.fato").fetchone() == (2,)


def test_nao_deixa_arquivos_temporarios(tmp_path: Path) -> None:
    caminho = tmp_path / "pharma.duckdb"
    _banco_com_espaco_morto(caminho)

    compactar_duckdb(_SettingsFake(caminho))

    residuos = [item.name for item in tmp_path.iterdir() if item.name != "pharma.duckdb"]
    assert residuos == [], f"sobraram arquivos de manutencao: {residuos}"


def test_banco_inexistente_falha_com_mensagem_clara(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="nao encontrado"):
        compactar_duckdb(_SettingsFake(tmp_path / "nao_existe.duckdb"))


def test_reducao_percentual() -> None:
    resultado = CompactacaoResult(caminho="x", bytes_antes=1000, bytes_depois=250)
    assert resultado.bytes_liberados == 750
    assert resultado.reducao_percentual == 75.0

    # Banco vazio nao deve provocar divisao por zero.
    assert replace(resultado, bytes_antes=0, bytes_depois=0).reducao_percentual == 0.0
