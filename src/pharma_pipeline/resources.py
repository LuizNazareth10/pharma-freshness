from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from typing import Any
from uuid import uuid4

import dlt

from pharma_pipeline.contracts import contract_for
from pharma_pipeline.http import DailyMedClient, OpenFdaClient, _parse_dailymed_date


def _utc_midnight(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=UTC)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def dailymed_resource(
    client: DailyMedClient,
    *,
    initial_date: date,
    end_date: date,
    page_size: int,
    start_page: int = 1,
    max_pages: int | None = None,
):
    contract = contract_for("dailymed")
    extraction_id = str(uuid4())
    incremental_cursor = dlt.sources.incremental(
        "published_date",
        initial_value=initial_date,
        primary_key=contract.primary_key,
    )

    @dlt.resource(
        name=contract.table_name,
        primary_key=contract.primary_key,
        write_disposition="append",
    )
    def resource(published_date=incremental_cursor):
        start = published_date.start_value
        if isinstance(start, datetime):
            start = start.date()
        for raw in client.iter_spls(
            published_since=start,
            published_until=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
        ):
            published = _parse_dailymed_date(raw["published_date"])
            yield {
                "setid": raw["setid"],
                "spl_version": int(raw["spl_version"]),
                "title": raw["title"],
                "published_date": published,
                "event_time": _utc_midnight(published),
                "ingest_time": datetime.now(UTC),
                "fonte": "dailymed",
                "source_url": f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={raw['setid']}",
                "extraction_id": extraction_id,
                "raw_payload": _json(raw),
            }

    return resource


def faers_resource(
    client: OpenFdaClient,
    *,
    initial_date: date,
    end_date: date,
    page_size: int,
    start_page: int = 1,
    max_pages: int | None = None,
):
    contract = contract_for("faers")
    extraction_id = str(uuid4())
    incremental_cursor = dlt.sources.incremental(
        "receiptdate",
        initial_value=initial_date,
        primary_key=contract.primary_key,
    )

    @dlt.resource(
        name=contract.table_name,
        primary_key=contract.primary_key,
        write_disposition="append",
    )
    def resource(receiptdate=incremental_cursor):
        start = receiptdate.start_value
        if isinstance(start, datetime):
            start = start.date()
        for raw in client.iter_records(
            "faers",
            since=start,
            until=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
        ):
            received = datetime.strptime(raw["receivedate"], "%Y%m%d").date()
            receipt = raw.get("receiptdate")
            yield {
                "safetyreportid": raw["safetyreportid"],
                "safetyreportversion": int(raw.get("safetyreportversion", 1)),
                "receivedate": received,
                "receiptdate": datetime.strptime(receipt, "%Y%m%d").date() if receipt else None,
                "serious": raw.get("serious") == "1",
                "occurcountry": raw.get("occurcountry"),
                "event_time": _utc_midnight(received),
                "ingest_time": datetime.now(UTC),
                "fonte": "faers",
                "source_url": "https://api.fda.gov/drug/event.json",
                "extraction_id": extraction_id,
                "patient_payload": _json(raw.get("patient", {})),
                "raw_payload": _json(raw),
            }

    return resource


def res_resource(
    client: OpenFdaClient,
    *,
    initial_date: date,
    end_date: date,
    page_size: int,
    start_page: int = 1,
    max_pages: int | None = None,
    lookback_days: int = 90,
):
    contract = contract_for("res")
    extraction_id = str(uuid4())
    incremental_cursor = dlt.sources.incremental(
        "report_date",
        initial_value=initial_date,
        primary_key=contract.primary_key,
        lag=lookback_days,
    )

    @dlt.resource(
        name=contract.table_name,
        primary_key=contract.primary_key,
        write_disposition="append",
    )
    def resource(report_date=incremental_cursor):
        start = report_date.start_value
        if isinstance(start, datetime):
            start = start.date()
        for raw in client.iter_records(
            "res",
            since=start,
            until=end_date,
            page_size=page_size,
            start_page=start_page,
            max_pages=max_pages,
        ):
            reported = datetime.strptime(raw["report_date"], "%Y%m%d").date()
            initiated = raw.get("recall_initiation_date")
            yield {
                "recall_number": raw["recall_number"],
                "event_id": raw.get("event_id"),
                "report_date": reported,
                "recall_initiation_date": (
                    datetime.strptime(initiated, "%Y%m%d").date() if initiated else None
                ),
                "classification": raw.get("classification"),
                "status": raw.get("status"),
                "recalling_firm": raw.get("recalling_firm"),
                "product_description": raw.get("product_description"),
                "reason_for_recall": raw.get("reason_for_recall"),
                "event_time": _utc_midnight(reported),
                "ingest_time": datetime.now(UTC),
                "fonte": "res",
                "source_url": "https://api.fda.gov/drug/enforcement.json",
                "extraction_id": extraction_id,
                "openfda_payload": _json(raw.get("openfda", {})),
                "raw_payload": _json(raw),
            }

    return resource
