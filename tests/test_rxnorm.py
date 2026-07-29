"""Testes da normalizacao RxNorm.

Nenhum destes testes toca a rede: o cliente e substituido por um duplo. Um teste que depende
do RxNav falharia quando a NLM estivesse fora do ar, o que nada diz sobre o nosso codigo.
"""

from __future__ import annotations

import json

from pharma_pipeline.rxnorm import RxNormCache, RxNormMatch, normalizar_nomes


class ClienteFalso:
    """Cliente de teste que registra o que foi consultado."""

    def __init__(self, respostas: dict[str, RxNormMatch] | None = None) -> None:
        self.respostas = respostas or {}
        self.consultados: list[str] = []

    def lookup(self, nome: str) -> RxNormMatch:
        self.consultados.append(nome)
        return self.respostas.get(
            nome,
            RxNormMatch(
                nome_normalizado=nome,
                rxcui=None,
                rxnorm_nome=None,
                rxnorm_tty=None,
                tipo_correspondencia="nao_mapeado",
                score=None,
                consultado_em="2026-07-28T00:00:00+00:00",
            ),
        )


def _match(nome: str, rxcui: str) -> RxNormMatch:
    return RxNormMatch(
        nome_normalizado=nome,
        rxcui=rxcui,
        rxnorm_nome=nome.lower(),
        rxnorm_tty="IN",
        tipo_correspondencia="exata",
        score=None,
        consultado_em="2026-07-28T00:00:00+00:00",
    )


def test_cache_evita_reconsultar_o_mesmo_nome(tmp_path) -> None:
    cache = tmp_path / "cache.json"
    cliente = ClienteFalso({"TACROLIMUS": _match("TACROLIMUS", "42316")})

    primeira = normalizar_nomes(["TACROLIMUS"], cache_path=cache, client=cliente)
    segunda = normalizar_nomes(["TACROLIMUS"], cache_path=cache, client=cliente)

    assert cliente.consultados == ["TACROLIMUS"]  # a segunda execucao nao chamou a API
    assert primeira[0].rxcui == segunda[0].rxcui == "42316"


def test_nomes_sem_correspondencia_permanecem_no_resultado(tmp_path) -> None:
    """Um farmaco que o RxNorm nao conhece nao pode sumir do modelo."""
    resultados = normalizar_nomes(
        ["CREME DESCONHECIDO"], cache_path=tmp_path / "cache.json", client=ClienteFalso()
    )

    assert len(resultados) == 1
    assert resultados[0].rxcui is None
    assert resultados[0].tipo_correspondencia == "nao_mapeado"
    assert not resultados[0].mapeado


def test_falha_sem_correspondencia_tambem_entra_no_cache(tmp_path) -> None:
    """Sem isso, todo nome desconhecido seria reconsultado em toda execucao."""
    cache = tmp_path / "cache.json"
    cliente = ClienteFalso()

    normalizar_nomes(["NAO EXISTE"], cache_path=cache, client=cliente)
    normalizar_nomes(["NAO EXISTE"], cache_path=cache, client=cliente)

    assert cliente.consultados == ["NAO EXISTE"]


def test_modo_offline_nao_consulta_a_api(tmp_path) -> None:
    cliente = ClienteFalso({"TACROLIMUS": _match("TACROLIMUS", "42316")})

    resultados = normalizar_nomes(
        ["TACROLIMUS"], cache_path=tmp_path / "cache.json", offline=True, client=cliente
    )

    assert cliente.consultados == []
    assert resultados[0].rxcui is None
    # Sem consulta, `consultado_em` fica nulo em vez de "agora": o modelo precisa ser
    # deterministico para nao gerar um snapshot Iceberg novo a cada execucao.
    assert resultados[0].consultado_em is None


def test_limite_de_consultas_freia_execucoes_inesperadamente_grandes(tmp_path) -> None:
    cliente = ClienteFalso()

    resultados = normalizar_nomes(
        ["A", "B", "C"], cache_path=tmp_path / "cache.json", max_lookups=2, client=cliente
    )

    assert cliente.consultados == ["A", "B"]
    assert len(resultados) == 3
    assert resultados[2].consultado_em is None


def test_cache_corrompido_nao_derruba_a_execucao(tmp_path) -> None:
    cache = tmp_path / "cache.json"
    cache.write_text("{ isto nao e json", encoding="utf-8")

    assert len(RxNormCache(cache)) == 0


def test_cache_e_gravado_de_forma_legivel(tmp_path) -> None:
    cache = tmp_path / "cache.json"
    normalizar_nomes(
        ["TACROLIMUS"],
        cache_path=cache,
        client=ClienteFalso({"TACROLIMUS": _match("TACROLIMUS", "42316")}),
    )

    conteudo = json.loads(cache.read_text(encoding="utf-8"))
    assert conteudo["TACROLIMUS"]["rxcui"] == "42316"
