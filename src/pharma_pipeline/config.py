from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(
            f"Variavel {name} ausente. Copie .env.example para .env e configure o ambiente."
        )
    return value


def _date_env(name: str, default: str) -> date:
    raw = os.getenv(name, default)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} deve usar o formato YYYY-MM-DD; recebido: {raw!r}") from exc


def _non_negative_int_env(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} deve ser um inteiro; recebido: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} nao pode ser negativo; recebido: {value}.")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    minio_user: str
    minio_password: str
    minio_bucket: str
    minio_endpoint: str
    aws_region: str
    openfda_api_key: str | None
    dailymed_initial_date: date
    faers_initial_date: date
    res_initial_date: date
    res_lookback_days: int
    local_dir: Path
    iceberg_warehouse_prefix: str

    @classmethod
    def from_env(cls) -> Settings:
        local_dir = PROJECT_ROOT / ".local"
        local_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            minio_user=_required("MINIO_ROOT_USER"),
            minio_password=_required("MINIO_ROOT_PASSWORD"),
            minio_bucket=os.getenv("MINIO_BUCKET", "farmacovigilancia"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000").rstrip("/"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            openfda_api_key=os.getenv("OPENFDA_API_KEY") or None,
            dailymed_initial_date=_date_env("DAILYMED_INITIAL_DATE", "2026-07-20"),
            faers_initial_date=_date_env("FAERS_INITIAL_DATE", "2026-03-31"),
            res_initial_date=_date_env("RES_INITIAL_DATE", "2026-07-01"),
            res_lookback_days=_non_negative_int_env("RES_LOOKBACK_DAYS", "90"),
            local_dir=local_dir,
            iceberg_warehouse_prefix=os.getenv("ICEBERG_WAREHOUSE_PREFIX", "iceberg").strip("/"),
        )

    @property
    def iceberg_catalog_path(self) -> Path:
        path = self.local_dir / "iceberg" / "catalog.db"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def dlt_pipelines_dir(self) -> Path:
        path = self.local_dir / "dlt"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def initial_date_for(self, source: str) -> date:
        return {
            "dailymed": self.dailymed_initial_date,
            "faers": self.faers_initial_date,
            "res": self.res_initial_date,
        }[source]
