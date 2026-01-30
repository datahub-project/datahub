from __future__ import annotations

import pandas as pd
import pytest

from datahub_executor.common.monitor.inference_v2 import inference_utils as iu


def test_get_inference_v2_prediction_horizon_days_defaults_to_7(monkeypatch) -> None:
    monkeypatch.delenv(
        "DATAHUB_EXECUTOR_INFERENCE_V2_PREDICTION_HORIZON_DAYS", raising=False
    )
    assert iu.get_inference_v2_prediction_horizon_days() == 7


def test_get_inference_v2_prediction_horizon_days_env_override(monkeypatch) -> None:
    monkeypatch.setenv("DATAHUB_EXECUTOR_INFERENCE_V2_PREDICTION_HORIZON_DAYS", "14")
    assert iu.get_inference_v2_prediction_horizon_days() == 14


def test_get_default_prediction_num_intervals_7d(monkeypatch) -> None:
    monkeypatch.delenv(
        "DATAHUB_EXECUTOR_INFERENCE_V2_PREDICTION_HORIZON_DAYS", raising=False
    )
    assert iu.get_default_prediction_num_intervals(interval_hours=1) == 168
    assert iu.get_default_prediction_num_intervals(interval_hours=24) == 7


def test_split_time_series_df_sorts_and_splits() -> None:
    df = pd.DataFrame(
        {
            "ds": pd.to_datetime(
                ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-05", "2024-01-04"]
            ),
            "y": [3, 1, 2, 5, 4],
        }
    )

    train_df, eval_df = iu.split_time_series_df(df, train_ratio=0.6)

    assert len(train_df) > 0
    assert len(eval_df) > 0
    assert train_df["ds"].is_monotonic_increasing
    assert eval_df["ds"].is_monotonic_increasing
    assert train_df["ds"].max() < eval_df["ds"].min()


def test_split_time_series_df_raises_on_empty_or_invalid_ratio() -> None:
    with pytest.raises(ValueError, match="empty"):
        iu.split_time_series_df(pd.DataFrame(), train_ratio=0.7)

    df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=5), "y": range(5)})
    with pytest.raises(ValueError, match="train_ratio must be in"):
        iu.split_time_series_df(df, train_ratio=1.0)

    with pytest.raises(ValueError, match="empty partition"):
        iu.split_time_series_df(df, train_ratio=0.01)


def test_prepare_predictions_df_for_persistence_adds_timestamp_and_renames() -> None:
    df = pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=2, freq="h"),
            "detection_lower": [1.0, 2.0],
            "detection_upper": [3.0, 4.0],
        }
    )

    out = iu.prepare_predictions_df_for_persistence(df)
    assert "timestamp_ms" in out.columns
    assert "detection_band_lower" in out.columns
    assert "detection_band_upper" in out.columns
    assert "detection_lower" not in out.columns
    assert "detection_upper" not in out.columns


def test_timestamp_ms_to_ds_adds_ds_column() -> None:
    df = pd.DataFrame({"timestamp_ms": [1704067200000, 1704070800000], "y": [1.0, 2.0]})
    out = iu.timestamp_ms_to_ds(df)
    assert "ds" in out.columns
    assert pd.api.types.is_datetime64_any_dtype(out["ds"])


def test_get_force_retune_anomaly_only_parses_env(monkeypatch) -> None:
    monkeypatch.setenv("DATAHUB_EXECUTOR_FORCE_RETUNE_ANOMALY_ONLY", "true")
    assert iu.get_force_retune_anomaly_only() is True
    monkeypatch.setenv("DATAHUB_EXECUTOR_FORCE_RETUNE_ANOMALY_ONLY", "false")
    assert iu.get_force_retune_anomaly_only() is False


def test_get_model_pairings_env(monkeypatch) -> None:
    monkeypatch.delenv("DATAHUB_EXECUTOR_MODEL_PAIRINGS", raising=False)
    assert iu.get_model_pairings_env() is None
    monkeypatch.setenv("DATAHUB_EXECUTOR_MODEL_PAIRINGS", "  ")
    assert iu.get_model_pairings_env() is None
    monkeypatch.setenv("DATAHUB_EXECUTOR_MODEL_PAIRINGS", "prophet_adaptive_band")
    assert iu.get_model_pairings_env() == "prophet_adaptive_band"
