"""Durable per-page relay from the full source archive through Kafka.

A crash after acknowledgment but before state publication may duplicate messages.
Observation identity is preserved, so the Delta sink can reconcile that replay.
"""

import argparse
import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from urbanlake.contracts import Observation
from urbanlake.messaging import Publisher


def atomic_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def relay_partition(root: Path, partition: str, *, topic: str, bootstrap=None):
    state_path = root / "partitions" / partition / "state.json"
    manifest = json.loads(state_path.read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise ValueError(f"source partition {partition} is incomplete")
    progress_path = state_path.with_name("kafka.json")
    progress = (
        json.loads(progress_path.read_text())
        if progress_path.exists()
        else {
            "topic": topic,
            "pages": {},
            "records": 0,
        }
    )
    if progress["topic"] != topic:
        raise ValueError("Kafka relay topic changed; use a separate relay state")
    publisher = Publisher(bootstrap=bootstrap, topic=topic)
    files = manifest.get("pages") or manifest.get("files", [])
    expected_records = int(manifest.get("records", 0))
    for item in files:
        item = {"path": item} if isinstance(item, str) else item
        relative = item.get("path") or item.get("file")
        if not relative:
            raise ValueError("manifest page has no path")
        candidate = root / relative
        if not candidate.is_file():
            candidate = state_path.parent / relative
        candidate = candidate.resolve()
        if not candidate.is_relative_to(root.resolve()):
            raise ValueError("archive path escaped source root")
        with candidate.open("rb") as handle:
            checksum = hashlib.file_digest(handle, "sha256").hexdigest()
        if item.get("sha256") and checksum != item["sha256"]:
            raise ValueError(f"archive checksum mismatch: {relative}")
        page_key = str(candidate.relative_to(root.resolve()))
        if page_key in progress["pages"]:
            if progress["pages"][page_key]["sha256"] != checksum:
                raise ValueError("previously published source page was modified")
            continue
        count = 0
        with gzip.open(candidate, "rt", encoding="utf-8") as source:
            for line in source:
                publisher.send(Observation.model_validate_json(line))
                count += 1
        if item.get("records") is not None and count != int(item["records"]):
            raise ValueError("archive page record count mismatch")
        publisher.flush()
        progress["pages"][page_key] = {"sha256": checksum, "records": count}
        progress["records"] += count
        progress["updated_at"] = datetime.now(UTC).isoformat()
        atomic_json(progress_path, progress)
        print(
            json.dumps({"stage": "kafka", "partition": partition, "records": progress["records"]}),
            flush=True,
        )
    if progress["records"] != expected_records:
        raise ValueError(f"relay count {progress['records']} != source {expected_records}")
    progress["complete"] = True
    atomic_json(progress_path, progress)
    return progress


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data/full/backfill"))
    parser.add_argument("--partition", required=True)
    parser.add_argument("--topic", default="urbanlake.full.observations.v1")
    parser.add_argument("--bootstrap")
    args = parser.parse_args()
    relay_partition(args.root, args.partition, topic=args.topic, bootstrap=args.bootstrap)


if __name__ == "__main__":
    main()
