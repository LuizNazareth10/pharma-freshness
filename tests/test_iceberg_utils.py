from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pyarrow as pa

from pharma_pipeline.iceberg import (
    _changed_rows,
    _chaves,
    _compare_columns,
    _deduplicate,
    _em_lotes,
    _separar_novos_de_alterados,
    _without_dlt_internal_columns,
    normalize_arrow,
)


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


def test_bronze_compares_apenas_o_payload_original() -> None:
    """Na bronze, `raw_payload` representa a fonte; o resto e ruido de extracao."""
    table = pa.Table.from_pylist(
        [{"setid": "a", "raw_payload": "{}", "ingest_time": 1, "extraction_id": "x"}]
    )
    assert _compare_columns(table, ("setid",)) == ("raw_payload",)


def test_camadas_derivadas_comparam_todas_as_colunas_de_negocio() -> None:
    """Sem `raw_payload`, a comparacao cobre tudo fora da chave e dos campos volateis."""
    table = pa.Table.from_pylist(
        [{"id_farmaco": "a", "nome_farmaco": "X", "rxcui": "1", "ingest_time": 1}]
    )
    assert _compare_columns(table, ("id_farmaco",)) == ("nome_farmaco", "rxcui")


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

    result = _changed_rows(incoming, current, ("id",), ("raw_payload",))

    assert {row["id"] for row in result.to_pylist()} == {"changed", "new"}


def test_changed_rows_com_chave_composta() -> None:
    """O grao de `stg_faers_drugs` e composto; a comparacao precisa respeitar isso."""
    current = pa.Table.from_pylist(
        [
            {"relato": "r1", "seq": 1, "produto": "A"},
            {"relato": "r1", "seq": 2, "produto": "B"},
        ]
    )
    incoming = pa.Table.from_pylist(
        [
            {"relato": "r1", "seq": 1, "produto": "A"},
            {"relato": "r1", "seq": 2, "produto": "B ALTERADO"},
            {"relato": "r2", "seq": 1, "produto": "C"},
        ]
    )

    result = _changed_rows(incoming, current, ("relato", "seq"), ("produto",))

    assert {(row["relato"], row["seq"]) for row in result.to_pylist()} == {("r1", 2), ("r2", 1)}


def test_normalize_arrow_converte_fuso_local_para_utc_sem_deslocar_o_instante() -> None:
    """O DuckDB exporta timestamps no fuso da sessao; o Iceberg exige UTC."""
    instante = datetime(2026, 7, 28, 12, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    table = pa.table(
        {"ingest_time": pa.array([instante], type=pa.timestamp("us", tz="America/Sao_Paulo"))}
    )

    result = normalize_arrow(table)

    assert result.schema.field("ingest_time").type == pa.timestamp("us", tz="UTC")
    assert result.column("ingest_time")[0].as_py() == instante.astimezone(UTC)


def test_normalize_arrow_ajusta_precisao_e_tipos_largos() -> None:
    table = pa.table(
        {
            "titulo": pa.array(["bula"], type=pa.large_string()),
            "event_time": pa.array(
                [datetime(2026, 7, 28, tzinfo=UTC)], type=pa.timestamp("ns", tz="UTC")
            ),
        }
    )

    result = normalize_arrow(table)

    assert result.schema.field("titulo").type == pa.string()
    assert result.schema.field("event_time").type == pa.timestamp("us", tz="UTC")


# --- separacao entre insercao pura e atualizacao ---------------------------------------------
#
# Estes testes guardam a correcao de uma falha real: publicar `silver.stg_faers_drugs` com
# 38.639 linhas matava o processo com STATUS_STACK_OVERFLOW (0xC00000FD), sem excecao e sem
# mensagem. O `upsert` do PyIceberg monta uma expressao booleana com uma comparacao POR LINHA
# para localizar o que sera substituido, e a arvore ficava profunda demais.
#
# A saida foi nao pedir ao `upsert` o que ele nao precisa fazer: chave que ainda nao existe e
# `append`, sem expressao nenhuma. Depois da correcao, a mesma publicacao leva 4,8 s.


def _tabela(linhas: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(linhas)


def test_chaves_extrai_o_conjunto_de_chaves_compostas() -> None:
    tabela = _tabela([{"a": "x", "b": 1}, {"a": "y", "b": 2}])

    assert _chaves(tabela, ("a", "b")) == {("x", 1), ("y", 2)}


def test_tabela_vazia_manda_tudo_para_insercao() -> None:
    """Primeira carga: nao ha o que localizar, entao nada deve passar pelo upsert."""
    candidatos = _tabela([{"id": "a", "v": 1}, {"id": "b", "v": 2}])

    novos, alterados = _separar_novos_de_alterados(candidatos, set(), ("id",))

    assert novos.num_rows == 2
    assert alterados.num_rows == 0


def test_separa_chave_nova_de_chave_existente() -> None:
    candidatos = _tabela([{"id": "a", "v": 9}, {"id": "novo", "v": 1}])

    novos, alterados = _separar_novos_de_alterados(candidatos, {("a",)}, ("id",))

    assert [linha["id"] for linha in novos.to_pylist()] == ["novo"]
    assert [linha["id"] for linha in alterados.to_pylist()] == ["a"]


def test_separacao_funciona_com_chave_composta() -> None:
    candidatos = _tabela(
        [
            {"relato": "r1", "seq": 1, "v": 9},
            {"relato": "r1", "seq": 2, "v": 9},
        ]
    )

    novos, alterados = _separar_novos_de_alterados(candidatos, {("r1", 1)}, ("relato", "seq"))

    assert [(r["relato"], r["seq"]) for r in novos.to_pylist()] == [("r1", 2)]
    assert [(r["relato"], r["seq"]) for r in alterados.to_pylist()] == [("r1", 1)]


def test_particao_vazia_preserva_o_schema() -> None:
    """Regressao: `take([])` gerava indice de tipo `null` e o Arrow recusava a operacao."""
    candidatos = _tabela([{"id": "a", "v": 1}])

    novos, alterados = _separar_novos_de_alterados(candidatos, {("a",)}, ("id",))

    assert novos.num_rows == 0
    assert novos.schema == candidatos.schema, "particao vazia deve manter o schema"
    assert alterados.num_rows == 1


def test_nenhuma_linha_se_perde_na_separacao() -> None:
    candidatos = _tabela([{"id": str(indice), "v": indice} for indice in range(50)])
    existentes = {(str(indice),) for indice in range(0, 50, 3)}

    novos, alterados = _separar_novos_de_alterados(candidatos, existentes, ("id",))

    assert novos.num_rows + alterados.num_rows == candidatos.num_rows
    ids_separados = {r["id"] for r in novos.to_pylist()} | {r["id"] for r in alterados.to_pylist()}
    assert ids_separados == {str(indice) for indice in range(50)}


def test_lotes_cobrem_a_tabela_inteira() -> None:
    tabela = _tabela([{"id": str(indice)} for indice in range(2500)])

    lotes = list(_em_lotes(tabela, 1000))

    assert [lote.num_rows for lote in lotes] == [1000, 1000, 500]
    assert sum(lote.num_rows for lote in lotes) == tabela.num_rows


def test_lotes_de_tabela_vazia_nao_produzem_nada() -> None:
    vazia = _tabela([{"id": "a"}]).slice(0, 0)

    assert list(_em_lotes(vazia, 1000)) == []
