"""Spark regression for partition-aware history, replay and source-date corrections.

Run in the existing Spark image with mounted current source/sql/scripts. It uses
only synthetic observations, no NYC API traffic and no Kafka broker operations.
The parent validates the live Kafka path separately with its orchestration smoke.
"""

import gzip
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from urbanlake.bulk_processing import bronze_batch, finalize_partition
from urbanlake.contracts import Observation
from urbanlake.processing import session

root = Path("data/full-fixtures") / uuid4().hex[:12]
source_root = root / "backfill"
lake = str(root / "lake")
source_root.mkdir(parents=True)
(source_root / "plan.json").write_text(json.dumps({"end": "2020-03-01T00:00:00"}))
observed = datetime(2026, 11, 4, tzinfo=UTC)


def source_month(partition, rows, run_id, instant):
    folder = source_root / "partitions" / partition
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"page-{run_id}.jsonl.gz"
    observations = [
        Observation.wrap(row, kind="request", run_id=run_id, observed_at=instant) for row in rows
    ]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for observation in observations:
            handle.write(observation.model_dump_json() + "\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (folder / "state.json").write_text(
        json.dumps(
            {
                "complete": True,
                "status": "complete",
                "partition_id": partition,
                "run_id": run_id,
                "records": len(rows),
                "source_count_initial": len(rows),
                "source_count_final": len(rows),
                "pages": [
                    {
                        "path": path.relative_to(source_root).as_posix(),
                        "records": len(rows),
                        "sha256": digest,
                    }
                ],
            }
        )
    )
    return path


first = source_month(
    "2020-01",
    [
        {"unique_key": "1", "created_date": "2020-01-01T00:00:00", "status": "Open"},
        {
            "unique_key": "2",
            "created_date": "2020-01-02T00:00:00",
            "status": "Closed",
            "closed_date": "2020-01-03T00:00:00",
        },
    ],
    "january",
    observed,
)
second = source_month(
    "2020-02",
    [
        # Source-date correction must move key1 out of January Silver and Gold.
        {
            "unique_key": "1",
            "created_date": "2020-02-01T00:00:00",
            "status": "Closed",
            "closed_date": "2020-02-02T00:00:00",
        },
    ],
    "february",
    observed + timedelta(hours=1),
)
spark = session()
try:
    frame = spark.read.text(str(first))
    bronze_batch(frame, 0, lake)
    finalize_partition(spark, lake, str(source_root), "2020-01")
    # Explicitly replay the same archive before introducing a source change.
    bronze_batch(frame, 1, lake)
    finalize_partition(spark, lake, str(source_root), "2020-01")
    assert spark.read.format("delta").load(f"{lake}/bronze/observations").count() == 2
    bronze_batch(spark.read.text(str(second)), 2, lake)
    finalize_partition(spark, lake, str(source_root), "2020-02")
    silver = spark.read.format("delta").load(f"{lake}/silver/requests")
    assert silver.count() == 2
    assert silver.filter("created_month = '2020-01'").count() == 1
    assert silver.filter("source_key = '1'").first().created_month == "2020-02"
    assert spark.read.format("delta").load(f"{lake}/silver/observed_changes").count() == 1
    demand = spark.read.format("delta").load(f"{lake}/gold/daily_demand")
    assert demand.agg({"requests": "sum"}).first()[0] == 2
    assert demand.filter("day = DATE '2020-01-01'").count() == 0
    assert spark.read.format("delta").load(f"{lake}/bronze/observations").count() == 3
    report = {
        "status": "passed",
        "lake": lake,
        "replay": "idempotent",
        "source_creation_month_move": "Silver/Gold repaired",
        "bronze": 3,
        "silver": 2,
    }
    Path("artifacts/bulk-fixture.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
finally:
    spark.stop()
