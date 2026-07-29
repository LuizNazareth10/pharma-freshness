"""Normalizacao de nomes de farmaco via RxNorm (RxNav / NLM).

Por que este modulo existe
--------------------------
O mesmo farmaco aparece escrito de formas diferentes em cada fonte: `TACROLIMUS` no FAERS,
`Tacrolimus` numa bula, `PROGRAF` como marca. Sem uma identidade comum, contar eventos por
farmaco produz numeros errados -- o mesmo principio ativo vira varias linhas distintas.

O RxNorm e o vocabulario da NLM que resolve isso. Levamos cada nome ate ele e guardamos o
`rxcui` do INGREDIENTE, que e o nivel certo para agrupar sinais de seguranca.

Estrategia de resolucao, em ordem
---------------------------------
1. busca normalizada (`search=2`): tolera maiusculas, ordem de palavras e pontuacao;
2. busca aproximada (`approximateTerm`): usada quando a normalizada nao encontra nada;
3. nenhuma correspondencia: a linha e marcada como `nao_mapeado` e segue viva.

O passo 3 e uma decisao de projeto: um nome que o RxNorm nao conhece nao pode desaparecer do
modelo. Perder o evento adverso seria pior do que nao saber o ingrediente dele.

Cache
-----
O resultado de cada nome e gravado em disco. Isso mantem a execucao repetivel, evita punir a
API publica com consultas identicas e permite rodar offline (`RXNORM_OFFLINE=1`). O cache
guarda tambem as buscas sem resultado -- caso contrario, todo nome desconhecido seria
reconsultado em toda execucao.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from pharma_pipeline.http import build_session

LOGGER = logging.getLogger(__name__)

RXNAV_BASE = "https://rxnav.nlm.nih.gov/REST"

# Tipos de termo RxNorm aceitos como identidade de ingrediente.
#   IN  = ingrediente
#   MIN = ingrediente multiplo (associacoes)
#   PIN = ingrediente preciso (sal/ester especifico)
TTY_INGREDIENTE = frozenset({"IN", "MIN", "PIN"})

# Abaixo deste score a sugestao aproximada e ruido: nomes como "CREME PARA PSORIASE"
# encontram qualquer coisa com pontuacao baixa.
SCORE_MINIMO_APROXIMADO = 50.0


@dataclass(frozen=True, slots=True)
class RxNormMatch:
    """Resultado da normalizacao de um nome."""

    nome_normalizado: str
    rxcui: str | None
    rxnorm_nome: str | None
    rxnorm_tty: str | None
    tipo_correspondencia: str  # exata | aproximada | nao_mapeado
    score: float | None
    # None quando o nome ainda nao chegou a ser consultado (modo offline ou limite atingido).
    # Manter None, em vez de "agora", preserva o determinismo: um modelo que muda de valor a
    # cada execucao geraria um snapshot Iceberg novo mesmo sem nada ter mudado.
    consultado_em: str | None

    @property
    def mapeado(self) -> bool:
        return self.rxcui is not None


class RxNormCache:
    """Cache em disco, gravado de forma atomica para nao corromper em caso de interrupcao."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._entries: dict[str, dict[str, Any]] = {}
        if path.exists():
            try:
                self._entries = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                LOGGER.warning("Cache RxNorm ilegivel em %s; recomecando vazio.", path)
                self._entries = {}

    def __contains__(self, name: str) -> bool:
        return name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, name: str) -> RxNormMatch | None:
        entry = self._entries.get(name)
        return RxNormMatch(**entry) if entry else None

    def put(self, match: RxNormMatch) -> None:
        self._entries[match.nome_normalizado] = asdict(match)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self._entries, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self.path)


class RxNormClient:
    """Cliente do RxNav com intervalo entre chamadas e degradacao controlada."""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
        request_interval_seconds: float = 0.06,
    ) -> None:
        self.session = session or build_session()
        self.timeout_seconds = timeout_seconds
        self.request_interval_seconds = request_interval_seconds

    def _get(self, path: str, **params: Any) -> dict[str, Any] | None:
        try:
            response = self.session.get(
                f"{RXNAV_BASE}/{path}", params=params, timeout=self.timeout_seconds
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            # Uma falha de rede nao pode derrubar a transformacao inteira. O nome fica sem
            # mapeamento nesta execucao e sera reconsultado na proxima.
            LOGGER.warning("RxNav falhou em %s (%s): %s", path, params, exc)
            return None
        finally:
            if self.request_interval_seconds:
                time.sleep(self.request_interval_seconds)

    def _properties(self, rxcui: str) -> dict[str, Any]:
        body = self._get(f"rxcui/{rxcui}/properties.json") or {}
        return body.get("properties") or {}

    def lookup(self, nome: str) -> RxNormMatch:
        agora = datetime.now(UTC).isoformat()

        exata = self._get("rxcui.json", name=nome, search=2) or {}
        ids = (exata.get("idGroup") or {}).get("rxnormId") or []
        if ids:
            rxcui = str(ids[0])
            props = self._properties(rxcui)
            return RxNormMatch(
                nome_normalizado=nome,
                rxcui=rxcui,
                rxnorm_nome=props.get("name"),
                rxnorm_tty=props.get("tty"),
                tipo_correspondencia="exata",
                score=None,
                consultado_em=agora,
            )

        aproximada = self._get("approximateTerm.json", term=nome, maxEntries=1, option=1) or {}
        candidatos = (aproximada.get("approximateGroup") or {}).get("candidate") or []
        if candidatos:
            candidato = candidatos[0]
            score = float(candidato.get("score") or 0.0)
            rxcui = candidato.get("rxcui")
            if rxcui and score >= SCORE_MINIMO_APROXIMADO:
                props = self._properties(str(rxcui))
                return RxNormMatch(
                    nome_normalizado=nome,
                    rxcui=str(rxcui),
                    rxnorm_nome=props.get("name") or candidato.get("name"),
                    rxnorm_tty=props.get("tty"),
                    tipo_correspondencia="aproximada",
                    score=score,
                    consultado_em=agora,
                )

        return RxNormMatch(
            nome_normalizado=nome,
            rxcui=None,
            rxnorm_nome=None,
            rxnorm_tty=None,
            tipo_correspondencia="nao_mapeado",
            score=None,
            consultado_em=agora,
        )


def normalizar_nomes(
    nomes: list[str],
    *,
    cache_path: Path,
    offline: bool = False,
    max_lookups: int = 500,
    client: RxNormClient | None = None,
) -> list[RxNormMatch]:
    """Resolve uma lista de nomes, consultando o RxNav apenas para os que faltam no cache.

    `max_lookups` limita quantas consultas novas uma execucao pode disparar. Serve de freio:
    uma ingestao muito maior que o esperado nao deve virar milhares de chamadas sem que
    alguem tenha decidido isso. Os nomes excedentes ficam sem mapeamento e entram no cache na
    execucao seguinte.
    """
    cache = RxNormCache(cache_path)
    pendentes = [nome for nome in nomes if nome not in cache]

    if pendentes and not offline:
        client = client or RxNormClient()
        limite = pendentes[:max_lookups]
        if len(pendentes) > max_lookups:
            LOGGER.warning(
                "%d nomes pendentes excedem RXNORM_MAX_LOOKUPS=%d; "
                "%d ficarao sem mapeamento nesta execucao.",
                len(pendentes),
                max_lookups,
                len(pendentes) - max_lookups,
            )
        for indice, nome in enumerate(limite, start=1):
            cache.put(client.lookup(nome))
            if indice % 50 == 0:
                LOGGER.info("RxNorm: %d/%d nomes consultados.", indice, len(limite))
                cache.save()
        cache.save()
    elif pendentes and offline:
        LOGGER.warning(
            "RXNORM_OFFLINE ativo: %d nomes sem cache ficarao como nao_mapeado.", len(pendentes)
        )

    resultados: list[RxNormMatch] = []
    for nome in nomes:
        match = cache.get(nome)
        resultados.append(
            match
            or RxNormMatch(
                nome_normalizado=nome,
                rxcui=None,
                rxnorm_nome=None,
                rxnorm_tty=None,
                tipo_correspondencia="nao_mapeado",
                score=None,
                consultado_em=None,
            )
        )
    return resultados
