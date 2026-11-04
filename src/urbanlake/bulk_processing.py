"""Partition-aware full-history medallion processing.

Kafka consumption commits Bronze only. A completed, count-reconciled source-month
manifest gates Silver/Gold finalization, so derived tables are not rebuilt for
every ingestion page. All commands require a single writer per lake.
"""

import argparse
import hashlib
import json
import os
import re
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

from urbanlake.processing import envelope_schema, session

MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def month_predicate(months, column="created_month"):
    values = sorted(set(months))
    if not values or any(not MONTH.fullmatch(value) for value in values):
        raise ValueError("valid YYYY-MM partitions are required")
    return f"{column} IN ({','.join(repr(value) for value in values)})"


def atomic_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(path)


@contextmanager
def writer_lock(lake):
    """OS-released advisory lock; process/container death does not strand a PID lock."""
    root = Path(lake)
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".bulk-writer.lock"
    with lock.open("a+b") as handle:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError(f"another bulk writer owns {lake}") from error
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def completed_manifest(root, partition):
    """Only an atomically committed complete month may enter derived layers."""
    if not MONTH.fullmatch(partition):
        raise ValueError("partition must be YYYY-MM")
    root = Path(root).resolve()
    path = root / "partitions" / partition / "state.json"
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("complete") is not True or state.get("status") != "complete":
        raise ValueError(f"source month {partition} is not count-reconciled and complete")
    if state.get("partition_id", partition) != partition:
        raise ValueError("manifest partition does not match its directory")
    if not isinstance(state.get("run_id"), str) or not state["run_id"]:
        raise ValueError("manifest requires a stable run_id")
    records = state.get("records")
    if not isinstance(records, int) or records < 0:
        raise ValueError("manifest records must be a nonnegative integer")
    pages = state.get("pages", [])
    if sum(page["records"] for page in pages) != records:
        raise ValueError("manifest page counts do not match the month total")
    for name in ("source_count_initial", "source_count_final"):
        if name in state and state[name] != records:
            raise ValueError("source count changed; this month requires reconciliation")
    paths = []
    for page in pages:
        candidate = (root / page["path"]).resolve()
        if not candidate.is_relative_to(root) or not candidate.name.endswith(".jsonl.gz"):
            raise ValueError("manifest page must be a committed gzip JSONL under the input root")
        if not candidate.is_file() or not re.fullmatch(r"[a-f0-9]{64}", page["sha256"]):
            raise ValueError("manifest page is missing or has no SHA-256")
        paths.append(str(candidate))
    if len(paths) != len(set(paths)):
        raise ValueError("manifest lists duplicate page paths")
    return state, paths


def verify_page_hashes(state, paths):
    for page, path in zip(state["pages"], paths, strict=True):
        with open(path, "rb") as handle:
            actual = hashlib.file_digest(handle, "sha256").hexdigest()
        if actual != page["sha256"]:
            raise ValueError(f"committed observation page failed SHA-256: {path}")


def merge_rows(frame, path, key, partitions=None, partition_column="created_month"):
    from delta.tables import DeltaTable

    frame = frame.dropDuplicates([key])
    if DeltaTable.isDeltaTable(frame.sparkSession, path):
        condition = f"t.{key} = s.{key}"
        if partitions:
            condition += " AND " + month_predicate(partitions, f"t.{partition_column}")
        (
            DeltaTable.forPath(frame.sparkSession, path)
            .alias("t")
            .merge(frame.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = frame.write.format("delta").mode("overwrite")
        if partitions:
            writer = writer.partitionBy(partition_column)
        writer.save(path)


def bronze_batch(frame, batch_id, lake):
    """Idempotent partition-pruned sink; no global Silver/Gold work per batch."""
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    # The parsed branch below handles an empty frame without writing tables.
    # Avoid an extra Kafka action just to check emptiness before materialization.
    parsed = frame.select(F.col("value").cast("string").alias("value")).withColumn(
        "p", F.from_json("value", envelope_schema())
    )
    created = F.to_timestamp(F.get_json_object("p.raw_json", "$.created_date"))
    valid = (
        (F.col("p.schema_version") == 1)
        & (F.col("p.kind") == "request")
        & F.col("p.source_key").rlike("^[0-9]+$")
        & F.col("p.source_key").cast("long").isNotNull()
        & (F.get_json_object("p.raw_json", "$.unique_key") == F.col("p.source_key"))
        & created.isNotNull()
        & F.to_timestamp("p.observed_at").isNotNull()
        & (F.sha2("p.raw_json", 256) == F.col("p.fingerprint"))
        & (
            F.col("p.observation_id")
            == F.sha2(F.concat_ws(":", "p.run_id", "p.kind", "p.source_key", "p.fingerprint"), 256)
        )
    )
    # Quarantine and valid-row writes share decoding/fingerprint work. Persisting
    # this branch point also avoids fetching the same fixed Kafka offsets twice.
    parsed = parsed.withColumn("valid", F.coalesce(valid, F.lit(False))).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    invalid = parsed.filter("NOT valid").select(
        F.sha2("value", 256).alias("observation_id"), "value"
    )
    rejected = invalid.count()
    if rejected:
        merge_rows(invalid, f"{lake}/bronze/quarantine", "observation_id")
    observations = (
        parsed.filter("valid")
        .select("p.*")
        .withColumn("observed_at", F.to_timestamp("observed_at"))
        .withColumn(
            "created_month", F.substring(F.get_json_object("raw_json", "$.created_date"), 1, 7)
        )
        .dropDuplicates(["observation_id"])
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        months = [row[0] for row in observations.select("created_month").distinct().collect()]
        if months:
            path = f"{lake}/bronze/observations"
            if DeltaTable.isDeltaTable(frame.sparkSession, path):
                (
                    DeltaTable.forPath(frame.sparkSession, path)
                    .alias("t")
                    .merge(
                        observations.alias("s"),
                        "t.observation_id = s.observation_id AND "
                        + month_predicate(months, "t.created_month"),
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    observations.write.format("delta")
                    .partitionBy("created_month")
                    .mode("overwrite")
                    .save(path)
                )
        if rejected:
            raise ValueError(f"{rejected} invalid observations quarantined; batch must be repaired")
    finally:
        observations.unpersist()
        parsed.unpersist()


def normalize(latest):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    fields = ["unique_key", "created_date", "closed_date", "status", "complaint_type", "borough"]
    schema = StructType([StructField(name, StringType()) for name in fields])
    requests = latest.withColumn("raw", F.from_json("raw_json", schema)).select(
        "source_key",
        "observation_id",
        "fingerprint",
        "observed_at",
        "run_id",
        "created_month",
        "raw.*",
    )
    return (
        requests.withColumn("created_date", F.to_timestamp("created_date"))
        .withColumn("closed_date", F.to_timestamp("closed_date"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("borough", F.coalesce(F.upper(F.trim("borough")), F.lit("UNKNOWN")))
        .withColumn("complaint_type", F.coalesce(F.trim("complaint_type"), F.lit("Unknown")))
        .withColumn(
            "invalid_resolution",
            F.coalesce(F.col("closed_date") < F.col("created_date"), F.lit(False)),
        )
        .withColumn(
            "resolution_hours",
            F.when(
                ~F.col("invalid_resolution"),
                (F.col("closed_date").cast("long") - F.col("created_date").cast("long")) / 3600,
            ),
        )
    )


def replace_partitions(frame, path, months):
    from delta.tables import DeltaTable

    writer = frame.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(frame.sparkSession, path):
        writer.option("replaceWhere", month_predicate(months)).save(path)
    else:
        writer.partitionBy("created_month").save(path)


def sql_file(name):
    path = Path(__file__).resolve().parents[2] / "sql" / name
    return path if path.exists() else Path("/app/sql") / name


def finalize_partition(spark, lake, root, partition):
    """Finalize complete source months, scanning affected request partitions only."""
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    state, _ = completed_manifest(root, partition)
    if state["records"] == 0:
        report = {
            "partition": partition,
            "source_records": 0,
            "status": "complete",
            "finalized_at": datetime.now(UTC).isoformat(),
        }
        atomic_json(Path(lake) / "_processing_state" / f"{partition}.json", report)
        return report
    bronze = spark.read.format("delta").load(f"{lake}/bronze/observations")
    current = bronze.filter(month_predicate([partition]))
    run = current.filter(F.col("run_id") == state["run_id"])
    actual = run.select("source_key").distinct().count()
    if actual != state["records"]:
        raise ValueError(
            f"Bronze coverage {actual} differs from completed source count {state['records']}"
        )
    keys = (
        current.select("source_key")
        .distinct()
        .withColumn("source_id", F.col("source_key").cast("long"))
    )
    bounds = keys.agg(F.min("source_id"), F.max("source_id")).first()
    index_path = f"{lake}/_index/source_keys"
    old_index = None
    affected_months = {partition}
    if DeltaTable.isDeltaTable(spark, index_path):
        old_index = (
            spark.read.format("delta")
            .load(index_path)
            .filter(F.col("source_id").between(bounds[0], bounds[1]))
            .join(keys.select("source_key"), "source_key")
        )
        affected_months.update(
            row[0] for row in old_index.select(F.explode("months_seen")).distinct().collect()
        )
    history = (
        bronze.filter(month_predicate(affected_months))
        .join(keys.select("source_key"), "source_key")
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        order = Window.partitionBy("source_key").orderBy(
            F.col("observed_at").desc(), F.col("observation_id").desc()
        )
        latest = (
            history.withColumn("rank", F.row_number().over(order)).filter("rank = 1").drop("rank")
        )
        requests = normalize(latest).persist(StorageLevel.DISK_ONLY)
        try:
            merge_rows(requests, f"{lake}/silver/requests", "source_key", affected_months)
            seen = history.groupBy("source_key").agg(
                F.collect_set("created_month").alias("months_seen")
            )
            index = (
                requests.select("source_key", "created_month", "observed_at", "observation_id")
                .join(seen, "source_key")
                .withColumn("source_id", F.col("source_key").cast("long"))
                .withColumn("bucket", F.pmod(F.xxhash64("source_key"), F.lit(32)))
            )
            if DeltaTable.isDeltaTable(spark, index_path):
                (
                    DeltaTable.forPath(spark, index_path)
                    .alias("t")
                    .merge(
                        index.alias("s"),
                        f"t.source_id BETWEEN {bounds[0]} AND {bounds[1]} "
                        "AND t.source_key = s.source_key",
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                index.write.format("delta").partitionBy("bucket").mode("overwrite").save(index_path)
            ascending = Window.partitionBy("source_key").orderBy("observed_at", "observation_id")
            states = history.withColumn(
                "previous_fingerprint", F.lag("fingerprint").over(ascending)
            )
            states = states.withColumn(
                "is_change",
                F.coalesce(
                    F.col("previous_fingerprint").isNotNull()
                    & (F.col("previous_fingerprint") != F.col("fingerprint")),
                    F.lit(False),
                ),
            )
            change_path = f"{lake}/silver/observed_changes"
            changes = states.filter("is_change").drop("is_change")
            if DeltaTable.isDeltaTable(spark, change_path):
                columns = {name: f"s.`{name}`" for name in changes.columns}
                (
                    DeltaTable.forPath(spark, change_path)
                    .alias("t")
                    .merge(
                        states.alias("s"),
                        "t.observation_id = s.observation_id AND "
                        + month_predicate(affected_months, "t.created_month"),
                    )
                    .whenMatchedDelete(condition="NOT s.is_change")
                    .whenMatchedUpdate(condition="s.is_change", set=columns)
                    .whenNotMatchedInsert(condition="s.is_change", values=columns)
                    .execute()
                )
            else:
                changes.write.format("delta").partitionBy("created_month").mode("overwrite").save(
                    change_path
                )
        finally:
            requests.unpersist()
    finally:
        history.unpersist()
    silver = (
        spark.read.format("delta")
        .load(f"{lake}/silver/requests")
        .filter(month_predicate(affected_months))
    )
    silver.createOrReplaceTempView("silver_requests")
    analytics = spark.sql(sql_file("daily_requests.sql").read_text()).withColumn(
        "created_month", F.date_format("day", "yyyy-MM")
    )
    replace_partitions(analytics, f"{lake}/gold/daily_requests", affected_months)
    demand = silver.groupBy(F.to_date("created_date").alias("day"), "created_month").agg(
        F.count("source_key").alias("requests")
    )
    replace_partitions(demand, f"{lake}/gold/daily_demand", affected_months)
    all_demand = spark.read.format("delta").load(f"{lake}/gold/daily_demand")
    plan_path = Path(root) / "plan.json"
    if plan_path.exists():
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        cutoff = date.fromisoformat(plan["end"][:10])
        all_demand = all_demand.filter(F.col("day") < F.lit(str(cutoff)))
    all_demand.createOrReplaceTempView("daily_demand")
    (
        spark.sql(sql_file("demand_features.sql").read_text())
        .write.format("delta")
        .mode("overwrite")
        .save(f"{lake}/gold/daily_demand_features")
    )
    (
        all_demand.select("day", "requests")
        .orderBy("day")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(f"{lake}/gold/demand_export")
    )
    report = {
        "partition": partition,
        "source_records": actual,
        "affected_months": sorted(affected_months),
        "status": "complete",
        "finalized_at": datetime.now(UTC).isoformat(),
        "source_manifest_sha256": hashlib.sha256(
            json.dumps(state, sort_keys=True).encode()
        ).hexdigest(),
    }
    atomic_json(Path(lake) / "_processing_state" / f"{partition}.json", report)
    print(json.dumps(report), flush=True)
    return report


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["consume", "consume-finalize", "finalize", "load-files"])
    parser.add_argument("--lake", default="data/full/lake")
    parser.add_argument("--input-root", default="data/full/backfill")
    parser.add_argument("--partition", help="Completed source month YYYY-MM")
    parser.add_argument("--topic", default="urbanlake.observations.full.v1")
    parser.add_argument("--checkpoint", default="data/full/checkpoints/kafka-bronze-v1")
    parser.add_argument("--max-offsets", type=int, default=100000)
    args = parser.parse_args(argv)
    if args.action != "consume" and not args.partition:
        parser.error("--partition is required for consume-finalize/finalize/load-files")
    if args.partition and not MONTH.fullmatch(args.partition):
        parser.error("--partition must be YYYY-MM")
    if not 1 <= args.max_offsets <= 1000000:
        parser.error("--max-offsets must be 1..1000000")
    return args


def main():
    args = parse_arguments()
    consumes_kafka = args.action in ("consume", "consume-finalize")
    with writer_lock(args.lake):
        spark = session(kafka=consumes_kafka)
        spark.conf.set("spark.sql.shuffle.partitions", "8")
        try:
            if consumes_kafka:
                stream = (
                    spark.readStream.format("kafka")
                    .option(
                        "kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
                    )
                    .option("subscribe", args.topic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true")
                    .option("maxOffsetsPerTrigger", args.max_offsets)
                    .load()
                )
                (
                    stream.writeStream.option("checkpointLocation", args.checkpoint)
                    .foreachBatch(lambda frame, batch: bronze_batch(frame, batch, args.lake))
                    .trigger(availableNow=True)
                    .start()
                    .awaitTermination()
                )
                if args.action == "consume-finalize":
                    finalize_partition(spark, args.lake, args.input_root, args.partition)
            else:
                if args.action == "load-files":
                    state, paths = completed_manifest(args.input_root, args.partition)
                    verify_page_hashes(state, paths)
                    if paths:
                        bronze_batch(spark.read.text(paths), 0, args.lake)
                finalize_partition(spark, args.lake, args.input_root, args.partition)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
