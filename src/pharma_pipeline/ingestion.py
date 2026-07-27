from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import dlt
from dlt.common.configuration.specs import AwsCredentials
from dlt.destinations import filesystem

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import contract_for
from pharma_pipeline.http import DailyMedClient, OpenFdaClient
from pharma_pipeline.resources import dailymed_resource, faers_resource, res_resource


@dataclass(frozen=True, slots=True)
class IngestionResult:
    source: str
    pipeline_name: str
    load_ids: tuple[str, ...]
    rows_loaded: int | None
    info: Any


def _destination(settings: Settings, source: str):
    credentials = AwsCredentials(
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
        endpoint_url=settings.minio_endpoint,
        region_name=settings.aws_region,
    )
    return filesystem(
        bucket_url=f"s3://{settings.minio_bucket}/bronze/{source}",
        credentials=credentials,
        layout="{table_name}/load_id={load_id}/{file_id}.{ext}",
    )


def ingest_source(
    settings: Settings,
    source: str,
    *,
    initial_date: date | None = None,
    end_date: date | None = None,
    page_size: int | None = None,
    start_page: int = 1,
    max_pages: int | None = None,
    pipeline_suffix: str = "",
) -> IngestionResult:
    contract = contract_for(source)
    initial_date = initial_date or settings.initial_date_for(source)
    end_date = end_date or date.today()
    page_size = page_size or contract.default_page_size
    if not 1 <= page_size <= contract.max_page_size:
        raise ValueError(
            f"page_size de {source} deve estar entre 1 e {contract.max_page_size}; "
            f"recebido {page_size}."
        )
    if initial_date > end_date:
        raise ValueError("initial_date nao pode ser posterior a end_date.")
    if start_page < 1:
        raise ValueError("start_page deve ser maior ou igual a 1.")
    if max_pages is not None and not pipeline_suffix:
        raise ValueError(
            "--max-pages cria uma amostra incompleta e exige --pipeline-suffix "
            "(por exemplo, _lab) para nao contaminar o watermark de producao."
        )

    pipeline_name = f"pharma_{source}{pipeline_suffix}"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=_destination(settings, source),
        dataset_name=f"bronze_{source}",
        pipelines_dir=str(settings.dlt_pipelines_dir),
        progress="log",
    )

    if source == "dailymed":
        resource = dailymed_resource(
            DailyMedClient(),
            initial_date=initial_date,
            end_date=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
        )
    elif source == "faers":
        resource = faers_resource(
            OpenFdaClient(api_key=settings.openfda_api_key),
            initial_date=initial_date,
            end_date=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
        )
    else:
        resource = res_resource(
            OpenFdaClient(api_key=settings.openfda_api_key),
            initial_date=initial_date,
            end_date=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
            lookback_days=settings.res_lookback_days,
        )

    info = pipeline.run(resource, loader_file_format="parquet")
    load_ids = tuple(package.load_id for package in info.load_packages)
    rows_loaded = _extract_row_count(info, contract.table_name)
    return IngestionResult(source, pipeline_name, load_ids, rows_loaded, info)


def _extract_row_count(info: Any, table_name: str) -> int | None:
    """Extrai contagem quando o destino a fornece; versoes do dlt variam nesse detalhe."""
    total = 0
    found = False
    for package in info.load_packages:
        for job in package.jobs.get("completed_jobs", []):
            if getattr(job, "job_file_info", None) is None:
                continue
            if job.job_file_info.table_name == table_name:
                found = True
                metrics = getattr(job, "metrics", None)
                count = getattr(metrics, "items_count", None) if metrics else None
                if count is not None:
                    total += int(count)
                else:
                    return None
    return total if found else 0
