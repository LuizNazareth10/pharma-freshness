"""Testes da avaliacao de frescor e da decisao de alerta.

O que estes testes protegem
---------------------------
A regra mais importante da Fase 4 nao e calcular horas: e decidir QUEM e culpado pelo atraso.
Um alerta que confunde "a FDA nao publicou" com "o nosso pipeline quebrou" causa dois danos
opostos e igualmente graves -- investigacao inutil de codigo, ou um alarme cronicamente
vermelho que a equipe aprende a ignorar.

Por isso os testes exercitam a fronteira exata dos limiares e, principalmente, os casos em que
os dois tipos de atraso acontecem juntos.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pharma_pipeline.freshness import (
    SEVERIDADE_FONTE,
    SEVERIDADE_OK,
    SEVERIDADE_PIPELINE,
    avaliar_linhas,
    ultimas_medicoes,
)

AGORA = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)


def _linha(
    fonte: str = "faers",
    *,
    atraso_fonte: int = 10,
    atraso_pipeline: int = 5,
    sla_ingestao: int = 36,
    sla_fonte: int = 72,
    medicao_em: datetime | None = None,
) -> dict[str, Any]:
    medicao = medicao_em or AGORA
    return {
        "fonte": fonte,
        "medicao_em": medicao,
        "event_time_mais_recente": medicao - timedelta(hours=atraso_fonte + atraso_pipeline),
        "ultimo_ingest_time": medicao - timedelta(hours=atraso_pipeline),
        "atraso_da_fonte_horas": atraso_fonte,
        "atraso_do_pipeline_horas": atraso_pipeline,
        "idade_do_dado_horas": atraso_fonte + atraso_pipeline,
        "sla_ingestao_horas": sla_ingestao,
        "sla_frescor_fonte_horas": sla_fonte,
        "situacao": "ok",
    }


def _avaliar(linhas: list[dict[str, Any]]):
    return avaliar_linhas(linhas, avaliado_em=AGORA)


# --- selecao da medicao mais recente ---------------------------------------------------------


def test_usa_apenas_a_medicao_mais_recente_de_cada_fonte() -> None:
    """A tabela e uma serie temporal: um atraso ja resolvido nao pode reprovar hoje."""
    antiga = _linha("faers", atraso_pipeline=999, medicao_em=AGORA - timedelta(days=3))
    nova = _linha("faers", atraso_pipeline=2, medicao_em=AGORA)

    recentes = ultimas_medicoes([antiga, nova])

    assert len(recentes) == 1
    assert recentes[0]["atraso_do_pipeline_horas"] == 2


def test_mantem_uma_medicao_por_fonte() -> None:
    linhas = [_linha("faers"), _linha("dailymed"), _linha("res")]

    assert {item["fonte"] for item in ultimas_medicoes(linhas)} == {"faers", "dailymed", "res"}


# --- classificacao de severidade -------------------------------------------------------------


def test_tudo_dentro_do_slo_e_saudavel() -> None:
    relatorio = _avaliar([_linha(atraso_fonte=10, atraso_pipeline=5)])

    assert relatorio.severidade == SEVERIDADE_OK
    assert relatorio.saudavel


def test_atraso_do_pipeline_reprova() -> None:
    relatorio = _avaliar([_linha(atraso_pipeline=48, sla_ingestao=36)])

    assert relatorio.severidade == SEVERIDADE_PIPELINE
    assert not relatorio.saudavel
    assert len(relatorio.violacoes_pipeline) == 1


def test_atraso_da_fonte_nao_reprova_a_execucao() -> None:
    """A distincao central da Fase 4.

    O FAERS chega com ~120 dias de atraso e nao ha nada que o nosso codigo possa fazer a
    respeito. Reprovar a DAG por isso ensinaria a equipe a ignorar o alerta.
    """
    relatorio = _avaliar([_linha(atraso_fonte=200, sla_fonte=72)])

    assert relatorio.severidade == SEVERIDADE_FONTE
    assert relatorio.saudavel, "atraso da fonte deve avisar, nunca falhar a execucao"
    assert len(relatorio.violacoes_fonte) == 1


def test_atraso_do_pipeline_tem_precedencia_sobre_o_da_fonte() -> None:
    """Quando os dois estouram, a causa que controlamos vem primeiro e nao e contada duas vezes."""
    relatorio = _avaliar(
        [_linha(atraso_fonte=200, sla_fonte=72, atraso_pipeline=48, sla_ingestao=36)]
    )

    assert relatorio.severidade == SEVERIDADE_PIPELINE
    assert len(relatorio.violacoes_pipeline) == 1
    assert relatorio.violacoes_fonte == (), "a mesma fonte nao deve gerar dois incidentes"


def test_uma_fonte_ruim_entre_varias_define_a_severidade() -> None:
    relatorio = _avaliar(
        [
            _linha("dailymed", atraso_pipeline=2),
            _linha("res", atraso_pipeline=2),
            _linha("faers", atraso_pipeline=100, sla_ingestao=36),
        ]
    )

    assert not relatorio.saudavel
    assert {item.fonte for item in relatorio.violacoes_pipeline} == {"faers"}


# --- fronteira exata dos limiares ------------------------------------------------------------


def test_limiar_e_exclusivo_nas_duas_pontas() -> None:
    """Exatamente no limite ainda esta dentro do SLO; um passo alem, nao."""
    no_limite = _avaliar([_linha(atraso_pipeline=36, sla_ingestao=36)])
    assert no_limite.saudavel

    acima = _avaliar([_linha(atraso_pipeline=37, sla_ingestao=36)])
    assert not acima.saudavel


# --- saida para humanos e para maquinas ------------------------------------------------------


def test_mensagem_de_pipeline_atrasado_diz_o_que_fazer() -> None:
    relatorio = _avaliar([_linha("faers", atraso_pipeline=48, sla_ingestao=36)])
    mensagem = relatorio.fontes[0].mensagem()

    assert "pipeline atrasado" in mensagem
    assert "48" in mensagem and "36" in mensagem


def test_mensagem_de_fonte_atrasada_isenta_o_pipeline() -> None:
    relatorio = _avaliar([_linha("faers", atraso_fonte=200, sla_fonte=72)])
    mensagem = relatorio.fontes[0].mensagem()

    assert "fonte atrasada" in mensagem
    assert "pipeline esta saudavel" in mensagem


def test_serializacao_preserva_a_decisao() -> None:
    payload = _avaliar([_linha(atraso_pipeline=48, sla_ingestao=36)]).to_dict()

    assert payload["severidade"] == SEVERIDADE_PIPELINE
    assert payload["saudavel"] is False
    assert payload["fontes"][0]["violou_sla_pipeline"] is True
