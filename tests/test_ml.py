from datetime import date

import pandas as pd
import pytest

from urbanlake.ml import FEATURES, features, next_features, predict, read_daily


def test_lags_never_use_current_or_future_target():
    frame = pd.DataFrame({"day": pd.date_range("2026-01-01", periods=65), "requests": range(65)})
    original = features(frame)
    frame.loc[64, "requests"] = 999999
    changed = features(frame)
    pd.testing.assert_frame_equal(original[FEATURES], changed[FEATURES])
    assert original.iloc[0].lag1 == 13
    assert original.iloc[0].lag7 == 7
    assert original.iloc[0].lag14 == 0
    assert original.iloc[0].mean7 == 10


def test_serving_features_match_training():
    history = list(range(14))
    target = date(2026, 1, 15)
    batch = features(
        pd.DataFrame({"day": pd.date_range("2026-01-01", periods=15), "requests": range(15)})
    )
    pd.testing.assert_frame_equal(
        batch[FEATURES].astype(float), next_features(history, target).astype(float)
    )


def test_missing_days_refused(tmp_path):
    file = tmp_path / "daily.csv"
    frame = pd.DataFrame({"day": pd.date_range("2026-01-01", periods=70), "requests": range(70)})
    frame.drop(index=20).to_csv(file, index=False)
    with pytest.raises(ValueError, match="contiguous"):
        read_daily(str(file))


def test_snapshot_only_predicts_next_day():
    bundle = {
        "as_of_date": "2026-01-14",
        "history": list(range(14)),
        "selected_model": "seasonal_naive",
    }
    assert predict(bundle, date(2026, 1, 15)) == 7
    with pytest.raises(ValueError, match="2026-01-15"):
        predict(bundle, date(2026, 2, 1))
