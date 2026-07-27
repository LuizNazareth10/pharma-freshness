from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceContract:
    name: str
    table_name: str
    primary_key: str
    cursor_field: str
    event_time_field: str
    default_page_size: int
    max_page_size: int


CONTRACTS = {
    "dailymed": SourceContract(
        name="dailymed",
        table_name="dailymed_spls",
        primary_key="setid",
        cursor_field="published_date",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=100,
    ),
    "faers": SourceContract(
        name="faers",
        table_name="faers_events",
        primary_key="safetyreportid",
        cursor_field="receiptdate",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=1000,
    ),
    "res": SourceContract(
        name="res",
        table_name="res_recalls",
        primary_key="recall_number",
        cursor_field="report_date",
        event_time_field="event_time",
        default_page_size=100,
        max_page_size=1000,
    ),
}


def contract_for(source: str) -> SourceContract:
    try:
        return CONTRACTS[source]
    except KeyError as exc:
        raise ValueError(f"Fonte desconhecida: {source}. Use dailymed, faers ou res.") from exc
