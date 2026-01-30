import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.algorithms.preprocessing.preprocessor import PreprocessingConfig

from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    AnomalyConfigSerializer,
    AnomalyEvalsSerializer,
    ForecastConfigSerializer,
    ForecastEvalsSerializer,
    PreprocessingConfigSerializer,
    build_anomaly_training_evals,
    build_forecast_training_evals,
    build_model_config,
)


class _FakeForecastModel:
    def __init__(self) -> None:
        self.darts_model_config = {"interval_width": 0.9}
        self.best_params = {"changepoint_prior_scale": 0.05}

    def get_hyperparameters(self):
        # Simulate base API used by serialization to extract tuned params.
        return {"seasonality_mode": "additive"}


def test_build_model_config_uses_model_hyperparams_when_config_empty() -> None:
    """
    When a ForecastModelConfig object is provided but has empty hyperparameters,
    serialization should fall back to model-derived tuned params, and disable
    tuning for downstream retrains by forcing param_grid to {}.
    """
    fake_registry = SimpleNamespace(
        get=lambda _key: SimpleNamespace(name="prophet", version="0.1.0")
    )
    forecast_model = _FakeForecastModel()
    empty_cfg = ForecastModelConfig(hyperparameters={}, param_grid={"x": [1]})

    with patch(
        "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
        return_value=fake_registry,
    ):
        mc = build_model_config(
            forecast_model=forecast_model,
            forecast_config=empty_cfg,
            anomaly_model=None,
            anomaly_config=None,
            preprocessing_config=None,
            has_detection_bands=False,
            forecast_registry_key="prophet",
            anomaly_registry_key="datahub_forecast_anomaly",
        )

    assert mc.forecast_config_json is not None
    restored = ForecastConfigSerializer.deserialize(mc.forecast_config_json)
    assert isinstance(restored, ForecastModelConfig)
    # Includes merged model-derived params
    assert restored.hyperparameters.get("interval_width") == 0.9
    assert restored.hyperparameters.get("changepoint_prior_scale") == 0.05
    assert restored.hyperparameters.get("seasonality_mode") == "additive"
    # Ensures downstream retrains do not re-tune
    assert restored.param_grid == {}


def _make_test_df(n: int = 50) -> pd.DataFrame:
    """Create a test DataFrame with ds and y columns."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="h"),
            "y": range(n),
        }
    )


class TestPreprocessingConfigSerializer:
    """Tests for PreprocessingConfigSerializer."""

    def test_serialize_preprocessing_config(self) -> None:
        """PreprocessingConfigSerializer.serialize serializes config."""
        config = PreprocessingConfig()

        result = PreprocessingConfigSerializer.serialize(config)

        assert result is not None
        assert isinstance(result, str)
        # Should be valid JSON
        json.loads(result)

    def test_serialize_preprocessing_config_none(self) -> None:
        """PreprocessingConfigSerializer.serialize returns None for None."""
        result = PreprocessingConfigSerializer.serialize(None)  # type: ignore[arg-type]

        assert result is None

    def test_deserialize_preprocessing_config(self) -> None:
        """PreprocessingConfigSerializer.deserialize deserializes config."""
        config = PreprocessingConfig()
        json_str = PreprocessingConfigSerializer.serialize(config)

        result = PreprocessingConfigSerializer.deserialize(json_str)  # type: ignore[arg-type]

        assert result is not None

    def test_deserialize_preprocessing_config_invalid_json(self) -> None:
        """PreprocessingConfigSerializer.deserialize handles invalid JSON."""
        result = PreprocessingConfigSerializer.deserialize("invalid json")

        assert result is None


class TestForecastConfigSerializer:
    """Tests for ForecastConfigSerializer."""

    def test_serialize_forecast_config(self) -> None:
        """ForecastConfigSerializer.serialize serializes config."""
        config = ForecastModelConfig(hyperparameters={"param": "value"})

        result = ForecastConfigSerializer.serialize(config)

        assert result is not None
        assert isinstance(result, str)

    def test_serialize_forecast_config_none(self) -> None:
        """ForecastConfigSerializer.serialize returns None for None."""
        result = ForecastConfigSerializer.serialize(None)  # type: ignore[arg-type]

        assert result is None

    def test_deserialize_forecast_config(self) -> None:
        """ForecastConfigSerializer.deserialize deserializes config."""
        config = ForecastModelConfig(hyperparameters={"param": "value"})
        json_str = ForecastConfigSerializer.serialize(config)
        assert json_str is not None

        result = ForecastConfigSerializer.deserialize(json_str)

        assert result is not None

    def test_deserialize_forecast_config_invalid_json(self) -> None:
        """ForecastConfigSerializer.deserialize handles invalid JSON."""
        result = ForecastConfigSerializer.deserialize("invalid json")

        assert result is None


class TestAnomalyConfigSerializer:
    """Tests for AnomalyConfigSerializer."""

    def test_serialize_anomaly_config(self) -> None:
        """AnomalyConfigSerializer.serialize serializes config."""
        config = AnomalyModelConfig(hyperparameters={"param": "value"})

        result = AnomalyConfigSerializer.serialize(config)

        assert result is not None
        assert isinstance(result, str)

    def test_serialize_anomaly_config_none(self) -> None:
        """AnomalyConfigSerializer.serialize returns None for None."""
        result = AnomalyConfigSerializer.serialize(None)  # type: ignore[arg-type]

        assert result is None

    def test_deserialize_anomaly_config(self) -> None:
        """AnomalyConfigSerializer.deserialize deserializes config."""
        config = AnomalyModelConfig(hyperparameters={"param": "value"})
        json_str = AnomalyConfigSerializer.serialize(config)
        assert json_str is not None

        result = AnomalyConfigSerializer.deserialize(json_str)

        assert result is not None

    def test_deserialize_anomaly_config_invalid_json(self) -> None:
        """AnomalyConfigSerializer.deserialize handles invalid JSON."""
        result = AnomalyConfigSerializer.deserialize("invalid json")

        assert result is None


class TestForecastEvalsSerializer:
    """Tests for ForecastEvalsSerializer."""

    def test_serialize_forecast_evals(self) -> None:
        """ForecastEvalsSerializer.serialize serializes evals."""
        from datahub_observe.algorithms.training.forecast_evals import (
            ForecastTrainingEvals,
            ForecastTrainingRun,
        )

        run = ForecastTrainingRun(
            mae=10.0,
            rmse=12.0,
            mape=5.0,
            coverage=90.0,
        )
        evals = ForecastTrainingEvals(runs=[run])

        result = ForecastEvalsSerializer.serialize(evals)

        assert result is not None
        assert isinstance(result, str)

    def test_serialize_forecast_evals_none(self) -> None:
        """ForecastEvalsSerializer.serialize returns None for None."""
        result = ForecastEvalsSerializer.serialize(None)  # type: ignore[arg-type]

        assert result is None

    def test_deserialize_forecast_evals(self) -> None:
        """ForecastEvalsSerializer.deserialize deserializes evals."""
        from datahub_observe.algorithms.training.forecast_evals import (
            ForecastTrainingEvals,
            ForecastTrainingRun,
        )

        run = ForecastTrainingRun(
            mae=10.0,
            rmse=12.0,
            mape=5.0,
            coverage=90.0,
        )
        evals = ForecastTrainingEvals(runs=[run])
        json_str = ForecastEvalsSerializer.serialize(evals)
        assert json_str is not None

        result = ForecastEvalsSerializer.deserialize(json_str)

        assert result is not None

    def test_deserialize_forecast_evals_invalid_json(self) -> None:
        """ForecastEvalsSerializer.deserialize handles invalid JSON."""
        result = ForecastEvalsSerializer.deserialize("invalid json")

        assert result is None


class TestAnomalyEvalsSerializer:
    """Tests for AnomalyEvalsSerializer."""

    def test_serialize_anomaly_evals(self) -> None:
        """AnomalyEvalsSerializer.serialize serializes evals."""
        from datahub_observe.algorithms.training.anomaly_evals import (
            AnomalyTrainingEvals,
            AnomalyTrainingRun,
        )

        run = AnomalyTrainingRun(
            precision=0.9,
            recall=0.8,
            f1_score=0.85,
        )
        evals = AnomalyTrainingEvals(runs=[run])

        result = AnomalyEvalsSerializer.serialize(evals)

        assert result is not None
        assert isinstance(result, str)

    def test_serialize_anomaly_evals_none(self) -> None:
        """AnomalyEvalsSerializer.serialize returns None for None."""
        result = AnomalyEvalsSerializer.serialize(None)  # type: ignore[arg-type]

        assert result is None

    def test_deserialize_anomaly_evals(self) -> None:
        """AnomalyEvalsSerializer.deserialize deserializes evals."""
        from datahub_observe.algorithms.training.anomaly_evals import (
            AnomalyTrainingEvals,
            AnomalyTrainingRun,
        )

        run = AnomalyTrainingRun(
            precision=0.9,
            recall=0.8,
            f1_score=0.85,
        )
        evals = AnomalyTrainingEvals(runs=[run])
        json_str = AnomalyEvalsSerializer.serialize(evals)
        assert json_str is not None

        result = AnomalyEvalsSerializer.deserialize(json_str)

        assert result is not None

    def test_deserialize_anomaly_evals_invalid_json(self) -> None:
        """AnomalyEvalsSerializer.deserialize handles invalid JSON."""
        result = AnomalyEvalsSerializer.deserialize("invalid json")

        assert result is None


class TestBuildForecastTrainingEvals:
    """Tests for build_forecast_training_evals function."""

    def test_build_forecast_training_evals_with_dataframes(self) -> None:
        """build_forecast_training_evals builds evals with timestamp ranges."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        metrics = {"mae": 10.0, "rmse": 12.0, "mape": 5.0, "coverage": 90.0}

        result = build_forecast_training_evals(
            metrics=metrics,
            train_df=train_df,
            eval_df=eval_df,
        )

        assert result is not None
        assert len(result.runs) == 1
        assert result.runs[0].mae == 10.0
        assert result.runs[0].rmse == 12.0
        assert result.runs[0].train_samples == 40
        assert result.runs[0].test_samples == 10

    def test_build_forecast_training_evals_without_dataframes(self) -> None:
        """build_forecast_training_evals builds evals without dataframes."""
        metrics = {"mae": 10.0, "rmse": 12.0, "mape": 5.0}

        result = build_forecast_training_evals(metrics=metrics)

        assert result is not None
        assert result.runs[0].train_samples is None
        assert result.runs[0].test_samples is None

    def test_build_forecast_training_evals_handles_timestamp_extraction_failure(
        self,
    ) -> None:
        """build_forecast_training_evals handles timestamp extraction failure."""
        train_df = pd.DataFrame({"invalid": [1, 2, 3]})
        eval_df = _make_test_df(10)
        metrics = {"mae": 10.0}

        result = build_forecast_training_evals(
            metrics=metrics,
            train_df=train_df,
            eval_df=eval_df,
        )

        assert result is not None
        assert result.runs[0].train_start_millis is None

    def test_build_forecast_training_evals_with_custom_metrics(self) -> None:
        """build_forecast_training_evals includes custom metrics."""
        metrics = {
            "mae": 10.0,
            "rmse": 12.0,
            "mape": 5.0,
            "smoothness_ratio": 0.95,
        }

        result = build_forecast_training_evals(metrics=metrics)

        assert result is not None
        assert "smoothness_ratio" in result.runs[0].custom_metrics


class TestBuildAnomalyTrainingEvals:
    """Tests for build_anomaly_training_evals function."""

    def test_build_anomaly_training_evals_with_ground_truth_metrics(self) -> None:
        """build_anomaly_training_evals builds evals when ground truth metrics present."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        metrics = {"precision": 0.9, "recall": 0.8, "f1_score": 0.85}

        result = build_anomaly_training_evals(
            metrics=metrics,
            train_df=train_df,
            eval_df=eval_df,
        )

        assert result is not None
        assert len(result.runs) == 1
        assert result.runs[0].precision == 0.9
        assert result.runs[0].recall == 0.8
        assert result.runs[0].f1_score == 0.85

    def test_build_anomaly_training_evals_without_ground_truth_metrics(self) -> None:
        """build_anomaly_training_evals returns None when ground truth metrics missing."""
        metrics = {"anomaly_rate": 0.1, "mean_score": 0.5}

        result = build_anomaly_training_evals(metrics=metrics)

        assert result is None

    def test_build_anomaly_training_evals_missing_required_keys(self) -> None:
        """build_anomaly_training_evals returns None when required keys missing."""
        metrics = {"precision": 0.9}  # Missing recall and f1_score

        result = build_anomaly_training_evals(metrics=metrics)

        assert result is None

    def test_build_anomaly_training_evals_handles_timestamp_extraction_failure(
        self,
    ) -> None:
        """build_anomaly_training_evals handles timestamp extraction failure."""
        train_df = pd.DataFrame({"invalid": [1, 2, 3]})
        eval_df = _make_test_df(10)
        metrics = {"precision": 0.9, "recall": 0.8, "f1_score": 0.85}

        result = build_anomaly_training_evals(
            metrics=metrics,
            train_df=train_df,
            eval_df=eval_df,
        )

        assert result is not None
        assert result.runs[0].train_start_millis is None


class TestBuildModelConfig:
    """Tests for build_model_config function."""

    def test_build_model_config_with_all_fields(self) -> None:
        """build_model_config builds config with all fields provided."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_anomaly_entry = MagicMock()
        mock_anomaly_entry.name = "test_anomaly"
        mock_anomaly_entry.version = "0.2.0"
        mock_registry.get.side_effect = [mock_forecast_entry, mock_anomaly_entry]

        forecast_config = ForecastModelConfig(hyperparameters={"param": "value"})
        anomaly_config = AnomalyModelConfig(hyperparameters={"param": "value"})
        preprocessing_config = PreprocessingConfig()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
            return_value=mock_registry,
        ):
            result = build_model_config(
                forecast_model=MagicMock(),
                forecast_config=forecast_config,
                anomaly_model=MagicMock(),
                anomaly_config=anomaly_config,
                preprocessing_config=preprocessing_config,
                has_detection_bands=True,
                forecast_evals={"mae": 10.0},
                anomaly_evals={"precision": 0.9, "recall": 0.8, "f1_score": 0.85},
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
                forecast_score=0.8,
                anomaly_score=0.9,
            )

            assert result.forecast_model_name == "test_forecast"
            assert result.forecast_model_version == "0.1.0"
            assert result.anomaly_model_name == "test_anomaly"
            assert result.anomaly_model_version == "0.2.0"
            assert result.forecast_score == 0.8
            assert result.anomaly_score == 0.9

    def test_build_model_config_without_anomaly_model(self) -> None:
        """build_model_config handles missing anomaly model."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_registry.get.return_value = mock_forecast_entry

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
            return_value=mock_registry,
        ):
            result = build_model_config(
                forecast_model=MagicMock(),
                forecast_config=ForecastModelConfig(),
                anomaly_model=None,
                anomaly_config=None,
                preprocessing_config=PreprocessingConfig(),
                has_detection_bands=False,
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
            )

            assert result.anomaly_model_name is None
            assert result.anomaly_model_version is None

    def test_build_model_config_extracts_hyperparameters_from_model(self) -> None:
        """build_model_config extracts hyperparameters from model when config missing."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.darts_model_config = {"param": "value"}
        mock_forecast.get_hyperparameters.return_value = {"param2": "value2"}
        mock_forecast.best_params = {"param3": "value3"}

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_registry.get.return_value = mock_forecast_entry

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
            return_value=mock_registry,
        ):
            result = build_model_config(
                forecast_model=mock_forecast,
                forecast_config=ForecastModelConfig(hyperparameters={}),
                anomaly_model=None,
                anomaly_config=None,
                preprocessing_config=PreprocessingConfig(),
                has_detection_bands=False,
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
            )

            assert result.forecast_config_json is not None

    def test_build_model_config_computes_scores_from_evals(self) -> None:
        """build_model_config computes scores from evals when not provided."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_registry.get.return_value = mock_forecast_entry

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.compute_forecast_score"
            ) as mock_compute_forecast,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.compute_anomaly_score"
            ) as mock_compute_anomaly,
        ):
            mock_compute_forecast.return_value = 0.75
            mock_compute_anomaly.return_value = 0.85

            result = build_model_config(
                forecast_model=MagicMock(),
                forecast_config=ForecastModelConfig(),
                anomaly_model=None,
                anomaly_config=None,
                preprocessing_config=PreprocessingConfig(),
                has_detection_bands=False,
                forecast_evals={"mae": 10.0},
                anomaly_evals={"precision": 0.9},
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
            )

            assert result.forecast_score == 0.75
            assert result.anomaly_score == 0.85

    def test_build_model_config_handles_score_computation_failure(self) -> None:
        """build_model_config handles score computation failure gracefully."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_registry.get.return_value = mock_forecast_entry

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute_forecast.side_effect = Exception("Computation failed")

            result = build_model_config(
                forecast_model=MagicMock(),
                forecast_config=ForecastModelConfig(),
                anomaly_model=None,
                anomaly_config=None,
                preprocessing_config=PreprocessingConfig(),
                has_detection_bands=False,
                forecast_evals={"mae": 10.0},
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
            )

            assert result.forecast_score is None

    def test_build_model_config_uses_preprocessing_from_model(self) -> None:
        """build_model_config extracts preprocessing config from model when not provided."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.preprocessing_config = PreprocessingConfig()

        mock_registry = MagicMock()
        mock_forecast_entry = MagicMock()
        mock_forecast_entry.name = "test_forecast"
        mock_forecast_entry.version = "0.1.0"
        mock_registry.get.return_value = mock_forecast_entry

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.serialization.get_model_registry",
            return_value=mock_registry,
        ):
            result = build_model_config(
                forecast_model=mock_forecast,
                forecast_config=ForecastModelConfig(),
                anomaly_model=None,
                anomaly_config=None,
                preprocessing_config=None,
                has_detection_bands=False,
                forecast_registry_key="test_forecast",
                anomaly_registry_key="test_anomaly",
                train_df=train_df,
                eval_df=eval_df,
            )

            assert result.preprocessing_config_json is not None
            assert result.preprocessing_config_json != "{}"
