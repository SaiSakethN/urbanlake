"""Deterministic Docker integration: Kafka -> Spark -> Delta + replay + correction.

Uses a new topic and isolated lake/checkpoint for each run. No source API access.
Synthetic observations are explicitly separate from live source data.
"""

import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from confluent_kafka.admin import AdminClient, NewTopic

from urbanlake.contracts import Observation
from urbanlake.messaging import Publisher
from urbanlake.processing import process_batch, session

token = uuid4().hex[:12]
topic = f"urbanlake.integration.{token}"
lake = f"data/integration/{token}"
admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
admin.create_topics(
    [
        NewTopic(
            topic,
            2,
            1,
            config={
                "retention.ms": "3600000",
                "segment.ms": "60000",
            },
        )
    ]
)[topic].result()
publisher = Publisher(topic=topic)
observed = datetime(2026, 1, 1, tzinfo=UTC)
first = Observation.wrap(
    {
        "unique_key": "42",
        "created_date": "2025-01-01T00:00:00",
        "status": "Open",
        "borough": "Queens",
        "complaint_type": "Test",
    },
    kind="request",
    run_id="fixture-first",
    observed_at=observed,
)
correction = Observation.wrap(
    {
        "unique_key": "42",
        "created_date": "2025-01-01T00:00:00",
        "status": "Closed",
        "closed_date": "2025-01-02T00:00:00",
        "borough": "Queens",
        "complaint_type": "Test",
    },
    kind="request",
    run_id="fixture-correction",
    observed_at=observed + timedelta(hours=1),
)
publisher.send(first)
publisher.send(first)
publisher.send(correction)
publisher.producer.produce(topic, key="invalid", value="{not-json", on_delivery=publisher.delivered)
for n in range(65):
    publisher.send(
        Observation.wrap(
            {"day": str(date(2025, 1, 1) + timedelta(days=n)), "requests": str(1000 + n)},
            kind="daily_demand",
            run_id="fixture-demand",
            observed_at=observed,
        )
    )
publisher.flush()
spark = session(kafka=True)
try:

    def consume(checkpoint):
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        (
            stream.writeStream.option("checkpointLocation", checkpoint)
            .foreachBatch(lambda frame, batch: process_batch(frame, batch, lake))
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

    consume(f"{lake}/checkpoint")
    assert spark.read.format("delta").load(f"{lake}/bronze/observations").count() == 67
    silver = spark.read.format("delta").load(f"{lake}/silver/requests")
    assert silver.count() == 1
    assert silver.first().status == "CLOSED"
    assert silver.first().resolution_hours == 24
    assert spark.read.format("delta").load(f"{lake}/silver/observed_changes").count() == 1
    # Rewind using a new checkpoint; replay every Kafka offset against the same Delta lake.
    consume(f"{lake}/checkpoint-replay")
    assert spark.read.format("delta").load(f"{lake}/bronze/observations").count() == 67
    assert spark.read.format("delta").load(f"{lake}/silver/requests").count() == 1
    assert spark.read.format("delta").load(f"{lake}/gold/daily_demand").count() == 65
    assert spark.read.format("delta").load(f"{lake}/bronze/quarantine").count() == 1
    assert spark.read.format("delta").load(f"{lake}/gold/daily_demand_features").count() == 51
    report = {
        "status": "passed",
        "topic": topic,
        "bronze": 67,
        "silver": 1,
        "daily_demand": 65,
        "observed_changes": 1,
        "full_kafka_replay": "idempotent",
    }
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/integration-result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
finally:
    spark.stop()
    # The isolated short-retention test topic and files are retained for failure diagnosis.
