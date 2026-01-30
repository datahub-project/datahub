from __future__ import annotations

import pytest


def test_inference_v2_training_pipeline_executes_and_serializes(monkeypatch) -> None:
    """
    End-to-end-ish smoke test for inference_v2 on small synthetic data.

    This validates that ObserveAdapter can run and produce ModelConfig fields that
    are round-trip deserializable via the inference_v2 serializers.
    """
    pytest.importorskip("datahub_observe")

    import numpy as np
    import pandas as pd

    import datahub_executor.common.monitor.inference_v2.observe_adapter.adapter as adapter_mod
    from datahub_executor.common.monitor.inference_v2.observe_adapter.adapter import (
        ObserveAdapter,
    )
    from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
        InputDataContext,
    )
    from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
        AnomalyConfigSerializer,
        ForecastConfigSerializer,
        PreprocessingConfigSerializer,
    )

    # Be robust to evaluation/quality thresholds during a lightweight smoke run.
    monkeypatch.setattr(
        adapter_mod, "DEFAULT_ANOMALY_SCORE_THRESHOLD", 0.0, raising=False
    )
    monkeypatch.setattr(
        adapter_mod, "DEFAULT_FORECAST_SCORE_THRESHOLD", 0.0, raising=False
    )

    df = pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=60, freq="h"),
            "y": (np.sin(np.linspace(0, 6.0, 60)) * 10.0 + 100.0),
        }
    )

    ctx = InputDataContext(
        assertion_category="volume",
        is_dataframe_cumulative=False,
        is_delta=None,
    )

    adapter = ObserveAdapter()
    try:
        training_result = adapter.run_training_pipeline(
            df=df,
            input_data_context=ctx,
            num_intervals=4,
            interval_hours=1,
            sensitivity_level=5,
            model_combinations=[],  # uses default pairings; empty => get_pairings_from_env_or_default()
        )
    except Exception as e:
        err_msg = str(e).lower()
        if "all transform candidates failed" in err_msg:
            pytest.skip(
                "Prophet hyperparameter tuning failed on small synthetic data "
                "(known limitation in observe-models with minimal data)"
            )
        raise

    assert training_result is not None
    assert training_result.model_config is not None

    mc = training_result.model_config
    assert isinstance(mc.preprocessing_config_json, str)
    assert isinstance(mc.forecast_config_json, str)
    # Some inference_v2 runs may fall back to forecast-only behavior; allow anomaly
    # config to be absent in lightweight smoke tests.
    assert mc.anomaly_config_json is None or isinstance(mc.anomaly_config_json, str)

    # Round-trip deserialize using inference_v2 serializers.
    assert (
        PreprocessingConfigSerializer.deserialize(mc.preprocessing_config_json)
        is not None
    )
    assert ForecastConfigSerializer.deserialize(mc.forecast_config_json) is not None
    if mc.anomaly_config_json is not None:
        assert AnomalyConfigSerializer.deserialize(mc.anomaly_config_json) is not None
