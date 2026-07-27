from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import date
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


class ApiError(RuntimeError):
    """Falha contextualizada de uma API fonte."""


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "pharma-freshness-pipeline/0.2 (educational project)",
        }
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class DailyMedClient:
    base_url = "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json"

    def __init__(
        self, session: requests.Session | None = None, timeout_seconds: float = 30
    ) -> None:
        self.session = session or build_session()
        self.timeout_seconds = timeout_seconds
        self.last_metadata: dict[str, Any] = {}

    def iter_spls(
        self,
        *,
        published_since: date,
        published_until: date,
        page_size: int,
        start_page: int = 1,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        page = start_page
        pages_read = 0
        while True:
            params = {
                "published_date": published_since.isoformat(),
                "published_date_comparison": "gte",
                "pagesize": page_size,
                "page": page,
            }
            response = self.session.get(self.base_url, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            body = response.json()
            self.last_metadata = body.get("metadata", {})
            data = body.get("data", [])

            # A API aceita apenas um comparador. O limite superior e aplicado aqui.
            for record in data:
                published = _parse_dailymed_date(record["published_date"])
                if published <= published_until:
                    yield record

            pages_read += 1
            total_pages = int(self.last_metadata.get("total_pages", page))
            if page >= total_pages or not data or (max_pages and pages_read >= max_pages):
                return
            page += 1


class OpenFdaClient:
    endpoints = {
        "faers": "https://api.fda.gov/drug/event.json",
        "res": "https://api.fda.gov/drug/enforcement.json",
    }
    date_fields = {"faers": "receiptdate", "res": "report_date"}

    def __init__(
        self,
        api_key: str | None = None,
        session: requests.Session | None = None,
        timeout_seconds: float = 30,
        request_interval_seconds: float = 0.26,
    ) -> None:
        self.api_key = api_key
        self.session = session or build_session()
        self.timeout_seconds = timeout_seconds
        self.request_interval_seconds = request_interval_seconds
        self.last_metadata: dict[str, Any] = {}

    def iter_records(
        self,
        source: str,
        *,
        since: date,
        until: date,
        page_size: int,
        start_page: int = 1,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        endpoint = self.endpoints[source]
        date_field = self.date_fields[source]
        page = start_page
        pages_read = 0
        while True:
            skip = (page - 1) * page_size
            params: dict[str, Any] = {
                "search": f"{date_field}:[{since:%Y%m%d} TO {until:%Y%m%d}]",
                "sort": f"{date_field}:asc",
                "limit": page_size,
                "skip": skip,
            }
            if self.api_key:
                params["api_key"] = self.api_key

            response = self.session.get(endpoint, params=params, timeout=self.timeout_seconds)
            if response.status_code == 404:
                # openFDA usa 404 para uma busca valida sem resultados.
                return
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise ApiError(
                    f"openFDA falhou em {response.url}: HTTP {response.status_code}"
                ) from exc

            body = response.json()
            self.last_metadata = body.get("meta", {})
            records = body.get("results", [])
            yield from records

            pages_read += 1
            total = int(self.last_metadata.get("results", {}).get("total", 0))
            finished = not records or skip + len(records) >= total
            if finished or (max_pages and pages_read >= max_pages):
                return

            page += 1
            if (page - 1) * page_size > 25_000:
                raise ApiError(
                    "A consulta ultrapassou o limite de paginacao por skip do openFDA. "
                    "Divida o backfill em janelas menores de data ou use os arquivos bulk oficiais."
                )
            if self.request_interval_seconds:
                time.sleep(self.request_interval_seconds)


def _parse_dailymed_date(value: str) -> date:
    from datetime import datetime

    return datetime.strptime(value, "%b %d, %Y").date()
