import gzip
import hashlib
import json
from datetime import UTC, datetime

import pytest

from urbanlake.bulk_replay import relay_partition
from urbanlake.contracts import Observation


def archive(tmp_path):
    partition = tmp_path / "partitions/2020-01"
    partition.mkdir(parents=True)
    page = partition / "page-000000.jsonl.gz"
    row = Observation.wrap(
        {"unique_key": "1", "created_date": "2020-01-01T00:00:00"},
        kind="request",
        run_id="test-full",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with gzip.open(page, "wt", encoding="utf-8") as output:
        output.write(row.model_dump_json() + "\n")
    checksum = hashlib.sha256(page.read_bytes()).hexdigest()
    manifest = {
        "complete": True,
        "records": 1,
        "files": [
            {
                "path": "partitions/2020-01/page-000000.jsonl.gz",
                "records": 1,
                "sha256": checksum,
            }
        ],
    }
    (partition / "state.json").write_text(json.dumps(manifest))
    return page


def test_acknowledged_pages_are_not_republished(monkeypatch, tmp_path):
    archive(tmp_path)
    sent = []

    class Publisher:
        def __init__(self, **kwargs):
            pass

        def send(self, item):
            sent.append(item.observation_id)

        def flush(self):
            pass

    monkeypatch.setattr("urbanlake.bulk_replay.Publisher", Publisher)
    result = relay_partition(tmp_path, "2020-01", topic="test")
    assert result["complete"] and result["records"] == 1
    relay_partition(tmp_path, "2020-01", topic="test")
    assert len(sent) == 1


def test_archive_tampering_fails_before_publication(monkeypatch, tmp_path):
    page = archive(tmp_path)
    page.write_bytes(b"tampered")
    monkeypatch.setattr("urbanlake.bulk_replay.Publisher", lambda **kwargs: object())
    with pytest.raises(ValueError, match="checksum"):
        relay_partition(tmp_path, "2020-01", topic="test")
