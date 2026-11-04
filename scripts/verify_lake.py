"""Read-only assertions against the live-source bounded integration tables."""

import json

import pandas as pd

from urbanlake.ml import FEATURES, features, read_daily
from urbanlake.processing import session

spark = session()
try:
    counts = {}
    for name in (
        "bronze/observations",
        "silver/requests",
        "gold/daily_demand",
        "gold/daily_requests",
    ):
        frame = spark.read.format("delta").load(f"data/{name}")
        counts[name] = frame.count()
        if name == "bronze/observations":
            assert frame.select("observation_id").distinct().count() == counts[name]
        if name == "silver/requests":
            assert frame.select("unique_key").distinct().count() == counts[name]
        if name == "gold/daily_requests":
            assert frame.agg({"requests": "sum"}).first()[0] == 200
    assert counts["bronze/observations"] == 320, counts
    assert counts["silver/requests"] == 200, counts
    assert counts["gold/daily_demand"] == 120, counts
    gold_features = spark.read.format("delta").load("data/gold/daily_demand_features").toPandas()
    expected = features(read_daily("data/gold/demand_export"))
    actual = gold_features.sort_values("day").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        actual[FEATURES].astype(float), expected[FEATURES].astype(float), check_dtype=False
    )
    assert len(actual) == 106
    print(json.dumps({"assertions": "passed", "counts": counts}, indent=2))
finally:
    spark.stop()
