from datetime import date
from unittest.mock import Mock

from pharma_pipeline.http import DailyMedClient, OpenFdaClient, _parse_dailymed_date


def response(status: int, payload: dict) -> Mock:
    item = Mock(status_code=status)
    item.json.return_value = payload
    item.raise_for_status.side_effect = None
    return item


def test_parse_dailymed_date() -> None:
    assert _parse_dailymed_date("Jul 24, 2026") == date(2026, 7, 24)


def test_dailymed_paginates_and_applies_upper_bound() -> None:
    session = Mock()
    session.get.side_effect = [
        response(
            200,
            {
                "metadata": {"total_pages": 2},
                "data": [
                    {"setid": "a", "published_date": "Jul 24, 2026"},
                    {"setid": "future", "published_date": "Jul 29, 2026"},
                ],
            },
        ),
        response(
            200,
            {
                "metadata": {"total_pages": 2},
                "data": [{"setid": "b", "published_date": "Jul 25, 2026"}],
            },
        ),
    ]
    client = DailyMedClient(session=session)

    records = list(
        client.iter_spls(
            published_since=date(2026, 7, 20),
            published_until=date(2026, 7, 27),
            page_size=2,
        )
    )

    assert [record["setid"] for record in records] == ["a", "b"]
    assert session.get.call_count == 2


def test_openfda_treats_404_as_empty_result() -> None:
    session = Mock()
    session.get.return_value = response(404, {})
    client = OpenFdaClient(session=session, request_interval_seconds=0)

    records = list(
        client.iter_records(
            "faers",
            since=date(2026, 7, 1),
            until=date(2026, 7, 2),
            page_size=10,
        )
    )

    assert records == []


def test_openfda_builds_inclusive_date_window() -> None:
    session = Mock()
    session.get.return_value = response(
        200,
        {
            "meta": {"results": {"total": 1}},
            "results": [{"safetyreportid": "1", "receivedate": "20260331"}],
        },
    )
    client = OpenFdaClient(session=session, request_interval_seconds=0)

    records = list(
        client.iter_records(
            "faers",
            since=date(2026, 3, 31),
            until=date(2026, 3, 31),
            page_size=10,
        )
    )

    assert len(records) == 1
    params = session.get.call_args.kwargs["params"]
    assert params["search"] == "receiptdate:[20260331 TO 20260331]"
    assert params["sort"] == "receiptdate:asc"
