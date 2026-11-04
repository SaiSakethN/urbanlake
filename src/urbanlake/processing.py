"""Recoverable medallion writes from a replayable Bronze observation log.

One writer only. Derived layers are rebuilt from Bronze after each trigger;
cross-table commits are not atomic. A retry repairs partially updated layers.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path


def session(kafka: bool = False):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("UrbanLake")
        .master("local[2]")
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.jars", ",".join(str(p) for p in Path("/opt/spark-jars").glob("*.jar")))
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
    )
    if kafka:
        builder = builder.config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def envelope_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    names = [
        "observation_id",
        "run_id",
        "kind",
        "source_key",
        "observed_at",
        "fingerprint",
        "raw_json",
    ]
    return StructType(
        [StructField("schema_version", IntegerType())]
        + [StructField(name, StringType()) for name in names]
    )


def merge_bronze(frame, path: str):
    from delta.tables import DeltaTable

    frame = frame.dropDuplicates(["observation_id"])
    if DeltaTable.isDeltaTable(frame.sparkSession, path):
        DeltaTable.forPath(frame.sparkSession, path).alias("t").merge(
            frame.alias("s"), "t.observation_id = s.observation_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        frame.write.format("delta").mode("overwrite").save(path)


def derive(spark, lake: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    bronze = spark.read.format("delta").load(f"{lake}/bronze/observations")
    order = Window.partitionBy("kind", "source_key").orderBy(
        F.col("observed_at").desc(), F.col("observation_id").desc()
    )
    latest = (
        bronze.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")
    )
    fields = ["unique_key", "created_date", "closed_date", "status", "complaint_type", "borough"]
    schema = StructType([StructField(name, StringType()) for name in fields])
    requests = (
        latest.filter("kind = 'request'")
        .withColumn("raw", F.from_json("raw_json", schema))
        .select("source_key", "fingerprint", "observed_at", "run_id", "raw.*")
        .withColumn("created_date", F.to_timestamp("created_date"))
        .withColumn("closed_date", F.to_timestamp("closed_date"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("borough", F.coalesce(F.upper(F.trim("borough")), F.lit("UNKNOWN")))
        .withColumn("complaint_type", F.coalesce(F.trim("complaint_type"), F.lit("Unknown")))
    )
    requests = requests.withColumn(
        "invalid_resolution", F.coalesce(F.col("closed_date") < F.col("created_date"), F.lit(False))
    ).withColumn(
        "resolution_hours",
        F.when(
            ~F.col("invalid_resolution"),
            (F.col("closed_date").cast("long") - F.col("created_date").cast("long")) / 3600,
        ),
    )
    invalid = requests.filter(
        "created_date IS NULL OR unique_key IS NULL OR unique_key <> source_key"
    )
    invalid.write.format("delta").mode("overwrite").save(f"{lake}/silver/invalid_requests")
    requests = requests.filter("created_date IS NOT NULL AND unique_key = source_key")
    requests.write.format("delta").mode("overwrite").save(f"{lake}/silver/requests")

    # These are observed differences, never inferred historical transition timestamps.
    history = Window.partitionBy("source_key").orderBy("observed_at", "observation_id")
    changes = (
        bronze.filter("kind = 'request'")
        .withColumn("previous_fingerprint", F.lag("fingerprint").over(history))
        .filter("previous_fingerprint IS NOT NULL AND previous_fingerprint <> fingerprint")
    )
    changes.write.format("delta").mode("overwrite").save(f"{lake}/silver/observed_changes")
    requests.createOrReplaceTempView("silver_requests")
    sql_path = Path(__file__).resolve().parents[2] / "sql/daily_requests.sql"
    if not sql_path.exists():
        sql_path = Path("/app/sql/daily_requests.sql")
    spark.sql(sql_path.read_text()).write.format("delta").mode("overwrite").save(
        f"{lake}/gold/daily_requests"
    )
    demand = latest.filter("kind = 'daily_demand'").select(
        F.to_date("source_key").alias("day"),
        F.get_json_object("raw_json", "$.requests").cast("long").alias("requests"),
        "observed_at",
        "run_id",
    )
    if demand.filter("day IS NULL OR requests IS NULL OR requests < 0").count():
        raise ValueError("invalid daily demand observation; refusing to publish Gold")
    demand.write.format("delta").mode("overwrite").save(f"{lake}/gold/daily_demand")
    demand.createOrReplaceTempView("daily_demand")
    feature_sql = sql_path.with_name("demand_features.sql")
    demand_features = spark.sql(feature_sql.read_text())
    demand_features.write.format("delta").mode("overwrite").save(
        f"{lake}/gold/daily_demand_features"
    )
    demand.orderBy("day").coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{lake}/gold/demand_export"
    )
    counts = {
        "bronze_observations": bronze.count(),
        "silver_requests": requests.count(),
        "observed_changes": changes.count(),
        "daily_demand_days": demand.count(),
        "daily_demand_feature_rows": demand_features.count(),
        "invalid_requests": invalid.count(),
        "last_success_unixtime": datetime.now(UTC).timestamp(),
    }
    # Isolated integration lakes must not replace the live pipeline's metrics.
    metrics_dir = Path("artifacts") if lake.rstrip("/") == "data" else Path(lake)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    destination = metrics_dir / "pipeline_metrics.json"
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(counts, indent=2))
    temporary.replace(destination)
    print(json.dumps(counts))


def process_batch(frame, batch_id: int, lake: str):
    from pyspark.sql import functions as F

    if frame.isEmpty():
        return
    parsed = frame.select(F.col("value").cast("string").alias("value")).withColumn(
        "parsed", F.from_json("value", envelope_schema())
    )
    valid = (
        (F.col("parsed.schema_version") == 1)
        & F.col("parsed.kind").isin("request", "daily_demand")
        & F.col("parsed.observation_id").rlike("^[a-f0-9]{64}$")
        & F.col("parsed.fingerprint").rlike("^[a-f0-9]{64}$")
        & F.col("parsed.source_key").isNotNull()
        & F.col("parsed.run_id").isNotNull()
        & F.to_timestamp("parsed.observed_at").isNotNull()
        & (F.sha2(F.col("parsed.raw_json"), 256) == F.col("parsed.fingerprint"))
        & F.get_json_object("parsed.raw_json", "$").isNotNull()
        & (
            F.col("parsed.observation_id")
            == F.sha2(
                F.concat_ws(
                    ":", "parsed.run_id", "parsed.kind", "parsed.source_key", "parsed.fingerprint"
                ),
                256,
            )
        )
    )
    invalid = parsed.filter(~F.coalesce(valid, F.lit(False))).select(
        F.sha2("value", 256).alias("observation_id"), "value"
    )
    if not invalid.isEmpty():
        merge_bronze(invalid, f"{lake}/bronze/quarantine")
    observations = (
        parsed.filter(valid)
        .select("parsed.*")
        .withColumn("observed_at", F.to_timestamp("observed_at"))
    )
    if not observations.isEmpty():
        merge_bronze(observations, f"{lake}/bronze/observations")
        derive(frame.sparkSession, lake)


def run(args):
    spark = session(kafka=not args.input)
    try:
        if args.input:
            process_batch(spark.read.text(args.input), 0, args.lake)
        else:
            stream = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
                .option("subscribe", args.topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", "2000")
                .load()
            )
            writer = stream.writeStream.option("checkpointLocation", args.checkpoint).foreachBatch(
                lambda frame, batch: process_batch(frame, batch, args.lake)
            )
            trigger = (
                {"processingTime": "60 seconds"} if args.continuous else {"availableNow": True}
            )
            writer.trigger(**trigger).start().awaitTermination()
    finally:
        spark.stop()
