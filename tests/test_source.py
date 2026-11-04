from datetime import UTC, date, datetime

import httpx
import pytest

from urbanlake.contracts import Observation
from urbanlake.source import IncompleteWindow, Socrata, WindowTooLarge, validate_window


def source(handler):
    return Socrata(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)


def test_keyset_pagination_and_cap():
    calls = []

    def handler(request):
        calls.append(dict(request.url.params))
        rows = (
            []
            if len(calls) > 1
            else [{"unique_key": "1", "created_date": "2026-01-01T10:00:00.000"}]
        )
        return httpx.Response(200, json=rows)

    rows = list(source(handler).requests(date(2026, 1, 1), date(2026, 1, 2), page_size=1))
    assert len(rows) == 1
    assert "unique_key > '1'" in calls[1]["$where"]
    assert "$offset" not in calls[1]


def test_truncation_is_failure():
    def handler(request):
        return httpx.Response(
            200,
            json=[{"unique_key": str(n), "created_date": "2026-01-01T10:00:00"} for n in (1, 2)],
        )

    with pytest.raises(IncompleteWindow):
        list(source(handler).requests(date(2026, 1, 1), date(2026, 1, 2), max_records=1))


def test_rate_limit_retry_and_schema_validation():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429 if len(calls) == 1 else 200, json=[])

    assert source(handler).query({}) == []
    assert len(calls) == 2
    with pytest.raises(ValueError):
        source(lambda _: httpx.Response(200, json={"error": "schema"})).query({})


def test_bad_window_and_missing_days():
    with pytest.raises(WindowTooLarge):
        validate_window(date(2020, 1, 1), date(2026, 1, 1))
    with pytest.raises(IncompleteWindow):
        source(lambda _: httpx.Response(200, json=[])).daily_demand(
            date(2026, 1, 1), date(2026, 1, 2)
        )


def test_observation_fingerprints_and_replay_identity():
    row = {"unique_key": "42", "created_date": "2026-01-01T00:00:00", "status": "Open"}
    args = {"kind": "request", "run_id": "run1", "observed_at": datetime.now(UTC)}
    first = Observation.wrap(row, **args)
    replay = Observation.wrap(dict(reversed(list(row.items()))), **args)
    assert first.observation_id == replay.observation_id
    changed = Observation.wrap({**row, "status": "Closed"}, **args)
    assert first.fingerprint != changed.fingerprint
    second_poll = Observation.wrap(row, **{**args, "run_id": "run2"})
    assert first.fingerprint == second_poll.fingerprint
    assert first.observation_id != second_poll.observation_id


def test_source_latest_partial_day_is_rejected():
    def handler(request):
        if request.url.params["$select"] == "created_date":
            return httpx.Response(200, json=[{"created_date": "2026-01-01T02:00:00"}])
        return httpx.Response(200, json=[{"day": "2026-01-01", "requests": "12"}])

    with pytest.raises(IncompleteWindow, match="latest source day"):
        source(handler).daily_demand(date(2026, 1, 1), date(2026, 1, 2))


def test_complete_aggregate_window_is_accepted():
    def handler(request):
        if request.url.params["$select"] == "created_date":
            return httpx.Response(200, json=[{"created_date": "2026-01-02T02:00:00"}])
        return httpx.Response(200, json=[{"day": "2026-01-01", "requests": "12"}])

    assert source(handler).daily_demand(date(2026, 1, 1), date(2026, 1, 2))[0]["requests"] == "12"


def test_permanent_http_failure_is_not_retried():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(400, json={"error": "bad query"})

    with pytest.raises(httpx.HTTPStatusError):
        source(handler).query({})
    assert len(calls) == 1


def test_invalid_cursor_identifier_cannot_be_interpolated():
    with pytest.raises(ValueError):
        list(
            source(
                lambda _: httpx.Response(
                    200,
                    json=[
                        {
                            "unique_key": "1' OR true",
                            "created_date": "2026-01-01T00:00:00",
                        }
                    ],
                )
            ).requests(date(2026, 1, 1), date(2026, 1, 2))
        )
