"""Manutencao do motor de transformacao.

O problema que este modulo resolve
----------------------------------
O DuckDB reaproveita blocos livres DENTRO do arquivo, mas nunca devolve espaco ao sistema
operacional. Cada `dbt build --full-refresh` reescreve todas as tabelas, e as versoes antigas
viram espaco livre interno que o arquivo continua ocupando em disco.

Na pratica: nesta base, com cerca de 38 mil linhas de fato, o arquivo chegou a **1,53 GB** apos
algumas dezenas de reconstrucoes. O conteudo real ocupa poucas dezenas de megabytes.

Isso nao e um defeito do DuckDB -- e o comportamento esperado de um formato que privilegia
escrita rapida. Mas num laboratorio local ele consome o disco em silencio, ate a execucao
falhar com "espaco insuficiente" num ponto que nao tem nada a ver com a causa.

A compactacao usa `COPY FROM DATABASE`, que copia o conteudo logico para um arquivo novo e
descarta o espaco morto. E muito mais rapido do que reconstruir tudo com o dbt, e nao depende
de rede: nenhuma consulta ao RxNav e refeita.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from pharma_pipeline.config import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CompactacaoResult:
    caminho: str
    bytes_antes: int
    bytes_depois: int

    @property
    def bytes_liberados(self) -> int:
        return self.bytes_antes - self.bytes_depois

    @property
    def reducao_percentual(self) -> float:
        if not self.bytes_antes:
            return 0.0
        return round(100 * self.bytes_liberados / self.bytes_antes, 1)


def _mb(valor: int) -> float:
    return round(valor / (1024 * 1024), 1)


def compactar_duckdb(settings: Settings) -> CompactacaoResult:
    """Reescreve o banco DuckDB em um arquivo novo, descartando o espaco morto.

    A troca so acontece depois que a copia termina com sucesso. Se a copia falhar no meio, o
    arquivo original permanece intacto -- perder o motor de transformacao no meio de uma
    manutencao seria pior do que continuar com ele grande.
    """
    origem = Path(settings.duckdb_path)
    if not origem.exists():
        raise FileNotFoundError(
            f"Banco DuckDB nao encontrado em {origem}. Nao ha nada para compactar."
        )

    bytes_antes = origem.stat().st_size
    destino = origem.with_suffix(".compactado")
    anterior = origem.with_suffix(".duckdb.anterior")

    for residuo in (destino, anterior):
        residuo.unlink(missing_ok=True)

    LOGGER.info("Compactando %s (%.1f MB)...", origem, _mb(bytes_antes))
    with duckdb.connect(str(origem)) as conexao:
        # O alias do banco vem do nome do arquivo; perguntar evita depender desse detalhe.
        atual = conexao.execute("select current_database()").fetchone()[0]
        conexao.execute(f"ATTACH '{destino.as_posix()}' AS compactado")
        conexao.execute(f'COPY FROM DATABASE "{atual}" TO compactado')
        conexao.execute("DETACH compactado")

    bytes_depois = destino.stat().st_size

    # Troca em dois passos: preserva o original ate a substituicao dar certo.
    origem.replace(anterior)
    destino.replace(origem)
    anterior.unlink(missing_ok=True)
    Path(str(origem) + ".wal").unlink(missing_ok=True)

    LOGGER.info("Compactacao concluida: %.1f MB -> %.1f MB.", _mb(bytes_antes), _mb(bytes_depois))
    return CompactacaoResult(
        caminho=str(origem), bytes_antes=bytes_antes, bytes_depois=bytes_depois
    )
