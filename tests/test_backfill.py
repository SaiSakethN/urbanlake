import gzip
import json

import pytest

from urbanlake.backfill import (
    _base_where,
    create_plan,
    local_datetime,
    run,
    source_timestamp_ceiling,
    summary,
    validate_page,
)
from urbanlake.contracts import Observation


class FakeSource:
    def __init__(self, pages, count=3, fail_after=None, final_count=None):
        self.pages = iter(pages)
        self.count = count
        self.final_count = final_count if final_count is not None else count
        self.count_calls = 0
        self.query_calls = 0
        self.fail_after = fail_after
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def query(self, params):
        self.queries.append(params)
        if "$select" in params:
            self.count_calls += 1
            value = self.count if self.count_calls == 1 else self.final_count
            return [{"rows": str(value)}]
        self.query_calls += 1
        if self.fail_after is not None and self.query_calls > self.fail_after:
            raise RuntimeError("interrupted API")
        return next(self.pages, [])


def row(key, stamp="2020-01-01T00:00:00.123456"):
    return {"unique_key": str(key), "created_date": stamp, "status": "Closed"}


def state(root, partition="2020-01"):
    return json.loads((root / "partitions" / partition / "state.json").read_text())


def test_plan_month_bounds_are_frozen_and_exclusive(tmp_path):
    first = create_plan(tmp_path, "2020-01-15", "2020-03-02T01:02:03.000001", "test")
    assert [item["partition_id"] for item in first["partitions"]] == [
        "2020-01",
        "2020-02",
        "2020-03",
    ]
    assert first["partitions"][-1]["end"] == "2020-03-02T01:02:03.000001"
    assert create_plan(tmp_path, "2020-01-15", "2020-03-02T01:02:03.000001") == first
    with pytest.raises(ValueError, match="different bounds"):
        create_plan(tmp_path, "2020-01-01", "2020-03-02")
    with pytest.raises(ValueError):
        local_datetime("2020-01-01T00:00:00' OR true")


def test_bounded_pilot_resumes_exact_cursor_and_observation_identity(tmp_path):
    create_plan(tmp_path, "2020-01-01", "2020-02-01", "test")
    first_source = FakeSource([[row(1), row(2)]])
    pilot = run(
        tmp_path, page_size=2, max_pages=1, min_free_gb=0, source_factory=lambda: first_source
    )
    assert pilot["records"] == 2
    assert not pilot["complete"]
    initial = state(tmp_path)
    first_page = tmp_path / initial["files"][0]
    original_bytes = first_page.read_bytes()
    observations = [
        Observation.model_validate_json(line)
        for line in gzip.decompress(original_bytes).splitlines()
    ]
    assert len(observations) == 2
    resume_source = FakeSource([[row(3, "2020-01-02T00:00:00")], []])
    result = run(tmp_path, page_size=2, min_free_gb=0, source_factory=lambda: resume_source)
    completed = state(tmp_path)
    assert result["complete"] and result["records"] == 3
    assert completed["complete"]
    assert completed["observed_at"] == initial["observed_at"]
    assert first_page.read_bytes() == original_bytes
    assert "2020-01-01T00:00:00.123456" in resume_source.queries[0]["$where"]
    assert "unique_key > '2'" in resume_source.queries[0]["$where"]
    assert summary(tmp_path)["pages"] == 2


def test_source_count_mismatch_is_never_complete(tmp_path):
    create_plan(tmp_path, "2020-01-01", "2020-02-01")
    fake = FakeSource([[row(1)], []], count=2)
    with pytest.raises(RuntimeError, match="incomplete"):
        run(tmp_path, min_free_gb=0, source_factory=lambda: fake)
    checkpoint = state(tmp_path)
    assert not checkpoint["complete"]
    assert checkpoint["status"] == "needs_reconciliation"
    assert summary(tmp_path)["failed_partitions"] == ["2020-01"]


def test_bad_page_does_not_advance_checkpoint(tmp_path):
    create_plan(tmp_path, "2020-01-01", "2020-02-01")
    fake = FakeSource([[row(2), row(1)]], count=2)
    with pytest.raises(RuntimeError, match="strictly increasing"):
        run(tmp_path, min_free_gb=0, source_factory=lambda: fake)
    assert state(tmp_path)["records"] == 0
    assert state(tmp_path)["cursor"] is None


def test_only_selected_partition_queries_source(tmp_path):
    create_plan(tmp_path, "2020-01-01", "2020-03-01")
    fake = FakeSource([[]], count=0)
    result = run(tmp_path, min_free_gb=0, partition="2020-02", source_factory=lambda: fake)
    assert result["completed_partitions"] == 1 and not result["complete"]
    assert state(tmp_path, "2020-02")["complete"]
    assert not (tmp_path / "partitions/2020-01").exists()
    with pytest.raises(ValueError, match="not included"):
        run(tmp_path, partition="2030-01", min_free_gb=0)


def test_disk_safeguard_stops_before_downloading(tmp_path, monkeypatch):
    from types import SimpleNamespace

    create_plan(tmp_path, "2020-01-01", "2020-02-01")
    monkeypatch.setattr("urbanlake.backfill.shutil.disk_usage", lambda _: SimpleNamespace(free=1))
    fake = FakeSource([], count=2)
    with pytest.raises(RuntimeError, match="disk safeguard"):
        run(tmp_path, min_free_gb=30, source_factory=lambda: fake)
    assert fake.query_calls == 0


def test_interruption_keeps_committed_pages_and_resume_rejects_corruption(tmp_path):
    create_plan(tmp_path, "2020-01-01", "2020-02-01")
    fake = FakeSource([[row(1)]], count=2, fail_after=1)
    with pytest.raises(RuntimeError, match="interrupted API"):
        run(tmp_path, min_free_gb=0, source_factory=lambda: fake)
    checkpoint = state(tmp_path)
    assert checkpoint["records"] == 1
    page = tmp_path / checkpoint["files"][0]
    page.write_bytes(b"corrupted")
    with pytest.raises(RuntimeError, match="missing or corrupted"):
        run(tmp_path, min_free_gb=0, source_factory=lambda: FakeSource([]))
    assert not state(tmp_path)["complete"]


@pytest.mark.parametrize(
    ("logical", "source"),
    [
        ("2026-11-01T02:06:29.000001", "2026-11-01T02:06:29.001"),
        ("2026-11-01T02:06:29.123000", "2026-11-01T02:06:29.123"),
        ("2026-11-01T02:06:29.999999", "2026-11-01T02:06:30.000"),
        ("2026-12-31T23:59:59.999999", "2027-01-01T00:00:00.000"),
        ("2020-01-01", "2020-01-01T00:00:00.000"),
    ],
)
def test_source_bounds_round_up_without_losing_latest_row(logical, source):
    assert source_timestamp_ceiling(logical) == source


def test_generated_source_predicate_ceilings_both_bounds_without_mutating_plan():
    partition = {"start": "2026-11-01T02:06:29.000001", "end": "2026-11-01T02:06:29.002001"}
    original = partition.copy()
    assert _base_where(partition) == (
        "created_date >= '2026-11-01T02:06:29.001' AND created_date < '2026-11-01T02:06:29.003'"
    )
    assert partition == original
    # Both logical and serialized windows include exactly .001 and .002.
    logical_start, logical_end = map(local_datetime, (partition["start"], partition["end"]))
    source_start, source_end = map(
        local_datetime,
        (
            source_timestamp_ceiling(partition["start"]),
            source_timestamp_ceiling(partition["end"]),
        ),
    )
    for millisecond in range(5):
        stamp = local_datetime(f"2026-11-01T02:06:29.{millisecond:03d}")
        assert (logical_start <= stamp < logical_end) == (source_start <= stamp < source_end)


def test_final_source_row_below_logical_microsecond_end_is_valid():
    partition = {"start": "2026-11-01T00:00:00", "end": "2026-11-01T02:06:29.000001"}
    latest = row("12345678", "2026-11-01T02:06:29.000")
    assert validate_page([latest], partition, None) == [latest["created_date"], "12345678"]
    assert "created_date < '2026-11-01T02:06:29.001'" in _base_where(partition)
    with pytest.raises(ValueError, match="outside"):
        validate_page([row("2", "2026-11-01T02:06:29.001")], partition, None)
