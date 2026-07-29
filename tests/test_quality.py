"""Testes da barreira de contrato (Great Expectations).

Uma suite de qualidade que so foi vista passar nao prova nada: ela poderia estar aprovando
tudo. Estes testes exercitam os DOIS lados -- dado bom aprovado, dado ruim reprovado -- e
apontam qual expectativa falhou.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from pharma_pipeline.quality import _build_expectation, validar_dataframe


def _fato_valido(**overrides) -> pd.DataFrame:
    linha = {
        "id_evento": ["a1", "b2"],
        "id_farmaco": ["f1", "f2"],
        "id_reacao": ["r1", "r2"],
        "id_fonte": ["s1", "s1"],
        "safetyreportid": ["1", "2"],
        "caracterizacao_codigo": [1, 2],
        "latencia_atualizacao_horas": [10, 20],
        "receivedate": [date(2026, 3, 1), date(2026, 3, 2)],
        "event_time": [datetime(2026, 3, 1, tzinfo=UTC), datetime(2026, 3, 2, tzinfo=UTC)],
        "ingest_time": [datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC)],
        "fonte": ["faers", "faers"],
    }
    linha.update(overrides)
    return pd.DataFrame(linha)


def _falhas(resultado) -> set[tuple[str, str | None]]:
    return {(item.expectation, item.column) for item in resultado.failures}


def test_fato_valido_passa_no_contrato() -> None:
    resultado = validar_dataframe("gold.fato_evento_adverso", _fato_valido())
    assert resultado.success, _falhas(resultado)
    assert resultado.rows == 2


def test_codigo_fora_do_dominio_e_tolerado_quando_e_pontual() -> None:
    """Um erro de preenchimento na origem nao pode derrubar o contrato inteiro.

    A FDA enviou um unico relato com `drugcharacterization = 4`, valor que o padrao ICH E2B
    nao define. Reprovar a publicacao por causa de uma linha em dezenas de milhares seria
    desproporcional -- e ensinaria a equipe a rodar a validacao com os olhos fechados.
    """
    codigos = [1] * 199 + [4]
    frame = pd.DataFrame(
        {
            "id_evento": [f"e{i}" for i in range(200)],
            "id_farmaco": ["f1"] * 200,
            "id_reacao": ["r1"] * 200,
            "id_fonte": ["s1"] * 200,
            "safetyreportid": [str(i) for i in range(200)],
            "caracterizacao_codigo": codigos,
            "latencia_atualizacao_horas": [10] * 200,
            "receivedate": [date(2026, 3, 1)] * 200,
            "event_time": [datetime(2026, 3, 1, tzinfo=UTC)] * 200,
            "ingest_time": [datetime(2026, 7, 1, tzinfo=UTC)] * 200,
            "fonte": ["faers"] * 200,
        }
    )

    resultado = validar_dataframe("gold.fato_evento_adverso", frame)

    assert resultado.success, _falhas(resultado)


def test_codigo_fora_do_dominio_reprova_quando_vira_sistemico() -> None:
    """A tolerancia tem limite: um dominio novo na fonte precisa aparecer.

    Se metade das linhas passa a usar um codigo desconhecido, nao e mais erro de digitacao --
    e a fonte mudou de vocabulario, e o modelo precisa ser revisto antes de seguir publicando.
    """
    codigos = [1] * 100 + [4] * 100
    frame = pd.DataFrame(
        {
            "id_evento": [f"e{i}" for i in range(200)],
            "id_farmaco": ["f1"] * 200,
            "id_reacao": ["r1"] * 200,
            "id_fonte": ["s1"] * 200,
            "safetyreportid": [str(i) for i in range(200)],
            "caracterizacao_codigo": codigos,
            "latencia_atualizacao_horas": [10] * 200,
            "receivedate": [date(2026, 3, 1)] * 200,
            "event_time": [datetime(2026, 3, 1, tzinfo=UTC)] * 200,
            "ingest_time": [datetime(2026, 7, 1, tzinfo=UTC)] * 200,
            "fonte": ["faers"] * 200,
        }
    )

    resultado = validar_dataframe("gold.fato_evento_adverso", frame)

    assert not resultado.success
    assert ("expect_column_values_to_be_in_set", "caracterizacao_codigo") in _falhas(resultado)


def test_fonte_nula_reprova() -> None:
    """A regra central do projeto: toda linha precisa citar sua fonte."""
    resultado = validar_dataframe("gold.fato_evento_adverso", _fato_valido(fonte=["faers", None]))

    assert not resultado.success
    assert ("expect_column_values_to_not_be_null", "fonte") in _falhas(resultado)


def test_ingest_time_nulo_reprova() -> None:
    resultado = validar_dataframe(
        "gold.fato_evento_adverso",
        _fato_valido(ingest_time=[datetime(2026, 7, 1, tzinfo=UTC), None]),
    )

    assert not resultado.success
    assert ("expect_column_values_to_not_be_null", "ingest_time") in _falhas(resultado)


def test_chave_duplicada_reprova() -> None:
    resultado = validar_dataframe("gold.fato_evento_adverso", _fato_valido(id_evento=["a1", "a1"]))

    assert not resultado.success
    assert ("expect_column_values_to_be_unique", "id_evento") in _falhas(resultado)


def test_data_de_evento_implausivel_reprova() -> None:
    """Data de 1900 denuncia parsing errado -- o defeito que mais corrompe metrica de frescor."""
    resultado = validar_dataframe(
        "gold.fato_evento_adverso",
        _fato_valido(receivedate=[date(2026, 3, 1), date(1900, 1, 1)]),
    )

    assert not resultado.success
    assert ("expect_column_values_to_be_between", "receivedate") in _falhas(resultado)


def test_latencia_negativa_reprova() -> None:
    """Capturar o dado antes de ele existir e impossivel; indica fuso trocado."""
    resultado = validar_dataframe(
        "gold.fato_evento_adverso", _fato_valido(latencia_atualizacao_horas=[10, -500])
    )

    assert not resultado.success
    assert ("expect_column_values_to_be_between", "latencia_atualizacao_horas") in _falhas(
        resultado
    )


def test_fonte_fora_do_dominio_reprova() -> None:
    resultado = validar_dataframe(
        "gold.fato_evento_adverso", _fato_valido(fonte=["faers", "inventada"])
    )

    assert not resultado.success
    assert ("expect_column_values_to_be_in_set", "fonte") in _falhas(resultado)


def test_nome_da_expectativa_vira_classe_do_gx() -> None:
    import great_expectations as gx

    expectativa = _build_expectation(
        gx, {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "fonte"}}
    )
    assert type(expectativa).__name__ == "ExpectColumnValuesToNotBeNull"


def test_expectativa_desconhecida_falha_com_contexto() -> None:
    import great_expectations as gx

    with pytest.raises(ValueError, match="Expectativa desconhecida"):
        _build_expectation(gx, {"type": "expect_nada_disso", "kwargs": {}})
