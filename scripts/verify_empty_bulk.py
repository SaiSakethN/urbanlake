"""An empty Kafka-shaped batch must not create or alter a Bronze table."""

from uuid import uuid4

from delta.tables import DeltaTable

from urbanlake.bulk_processing import bronze_batch
from urbanlake.processing import session

spark = session()
lake = f"data/full-fixtures/empty-{uuid4().hex[:12]}"
try:
    bronze_batch(spark.createDataFrame([], "value STRING"), 0, lake)
    assert not DeltaTable.isDeltaTable(spark, f"{lake}/bronze/observations")
    print("Empty-batch assertion passed; no Bronze table created")
finally:
    spark.stop()
