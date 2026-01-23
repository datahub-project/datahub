"""Tests for the pages/model_training.py module."""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from scripts.streamlit_explorer.model_explorer.model_training import (
    ModelConfig,
    TrainingGroup,
    TrainingRun,
    _compute_metrics,
    _generate_run_id,
    _split_train_test,
    check_preprocessing_model_compatibility,
)


class TestNaNHandling:
    """Tests for NaN value handling in training data."""

    def test_dropna_removes_nan_rows(self):
        """Test that NaN values in y column are dropped before training."""
        # Simulate preprocessed data with NaN gaps (from frequency alignment)
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
                "y": [1.0, np.nan, np.nan, 4.0, 5.0, np.nan, 7.0, 8.0, 9.0, 10.0],
            }
        )

        # This is the logic used in model_training.py
        cleaned_df = df.dropna(subset=["y"]).copy()

        assert len(cleaned_df) == 7  # 3 NaN rows removed
        assert cleaned_df["y"].isna().sum() == 0

    def test_dropna_preserves_non_nan_rows(self):
        """Test that non-NaN rows are preserved after dropna."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
                "y": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

        cleaned_df = df.dropna(subset=["y"]).copy()

        assert len(cleaned_df) == 5  # No rows removed
        assert list(cleaned_df["y"]) == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_dropna_handles_all_nan(self):
        """Test handling when all values are NaN."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
                "y": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )

        cleaned_df = df.dropna(subset=["y"]).copy()

        assert len(cleaned_df) == 0  # All rows removed

    def test_dropna_maintains_column_structure(self):
        """Test that dropna maintains all columns."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
                "y": [1.0, np.nan, 3.0, np.nan, 5.0],
                "type": ["INIT", "INIT", "DATA", "DATA", "DATA"],
            }
        )

        cleaned_df = df.dropna(subset=["y"]).copy()

        assert list(cleaned_df.columns) == ["ds", "y", "type"]
        assert len(cleaned_df) == 3


class TestSplitTrainTest:
    """Tests for the _split_train_test function."""

    def test_splits_data_correctly(self):
        """Test that data is split according to the ratio."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=100, freq="D"),
                "y": range(100),
            }
        )
        train_df, test_df = _split_train_test(df, 0.8)

        assert len(train_df) == 80
        assert len(test_df) == 20

    def test_maintains_time_order(self):
        """Test that train data comes before test data temporally."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
                "y": range(10),
            }
        )
        train_df, test_df = _split_train_test(df, 0.5)

        assert train_df["ds"].max() < test_df["ds"].min()

    def test_handles_small_dataset(self):
        """Test splitting a small dataset."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
                "y": [1, 2, 3, 4, 5],
            }
        )
        train_df, test_df = _split_train_test(df, 0.8)

        assert len(train_df) == 4
        assert len(test_df) == 1

    def test_handles_different_ratios(self):
        """Test various split ratios."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=100, freq="D"),
                "y": range(100),
            }
        )

        for ratio in [0.5, 0.7, 0.9]:
            train_df, test_df = _split_train_test(df, ratio)
            assert len(train_df) == int(100 * ratio)
            assert len(test_df) == 100 - int(100 * ratio)


class TestComputeMetrics:
    """Tests for the _compute_metrics function."""

    def test_computes_mae_correctly(self):
        """Test MAE computation."""
        actual = pd.Series([10, 20, 30, 40])
        predicted = pd.Series([12, 18, 32, 38])
        metrics = _compute_metrics(actual, predicted)

        # MAE = mean(|2, 2, 2, 2|) = 2
        assert metrics["MAE"] == 2.0

    def test_computes_rmse_correctly(self):
        """Test RMSE computation."""
        actual = pd.Series([10, 20, 30, 40])
        predicted = pd.Series([10, 20, 30, 40])  # Perfect predictions
        metrics = _compute_metrics(actual, predicted)

        assert metrics["RMSE"] == 0.0

    def test_computes_mape_correctly(self):
        """Test MAPE computation."""
        actual = pd.Series([100, 200, 300, 400])
        predicted = pd.Series([110, 220, 330, 440])  # 10% error
        metrics = _compute_metrics(actual, predicted)

        assert abs(metrics["MAPE"] - 10.0) < 0.1  # 10% MAPE

    def test_handles_zeros_in_mape(self):
        """Test that zeros in actual values are handled for MAPE."""
        actual = pd.Series([0, 100, 200])
        predicted = pd.Series([10, 110, 220])
        metrics = _compute_metrics(actual, predicted)

        # MAPE should only consider non-zero actuals
        assert not np.isnan(metrics["MAPE"])

    def test_returns_nan_mape_when_all_zeros(self):
        """Test that MAPE is NaN when all actual values are zero."""
        actual = pd.Series([0, 0, 0])
        predicted = pd.Series([1, 2, 3])
        metrics = _compute_metrics(actual, predicted)

        assert np.isnan(metrics["MAPE"])

    def test_returns_all_metric_keys(self):
        """Test that all expected metric keys are returned."""
        actual = pd.Series([10, 20, 30])
        predicted = pd.Series([11, 21, 31])
        metrics = _compute_metrics(actual, predicted)

        assert "MAE" in metrics
        assert "RMSE" in metrics
        assert "MAPE" in metrics


class TestGenerateRunId:
    """Tests for the _generate_run_id function."""

    def test_generates_expected_format(self):
        """Test that run ID has expected format."""
        run_id = _generate_run_id("model_a", "preproc_1")
        assert run_id == "model_a__preproc_1"

    def test_handles_special_characters(self):
        """Test handling of special characters in inputs."""
        run_id = _generate_run_id("obs_prophet", "daily_clean")
        assert run_id == "obs_prophet__daily_clean"


class TestModelConfig:
    """Tests for the ModelConfig dataclass."""

    def test_creates_basic_config(self):
        """Test creating a basic ModelConfig."""
        config = ModelConfig(
            name="Test Model",
            description="A test model",
            train_fn=lambda df: None,
            predict_fn=lambda m, df: df,
            color="#ff0000",
        )

        assert config.name == "Test Model"
        assert config.description == "A test model"
        assert config.color == "#ff0000"
        assert config.dash is None
        assert config.is_observe_model is False
        assert config.registry_key is None

    def test_creates_observe_model_config(self):
        """Test creating a config for observe-models."""
        config = ModelConfig(
            name="Observe Model",
            description="An observe model",
            train_fn=lambda df: None,
            predict_fn=lambda m, df: df,
            color="#00ff00",
            dash="dot",
            is_observe_model=True,
        )

        assert config.is_observe_model is True
        assert config.dash == "dot"
        assert config.registry_key is None

    def test_creates_observe_model_config_with_registry_key(self):
        """Test creating an observe-models config with registry_key."""
        config = ModelConfig(
            name="NBEATS Model",
            description="N-BEATS neural network",
            train_fn=lambda df: None,
            predict_fn=lambda m, df: df,
            color="#1f77b4",
            dash="dot",
            is_observe_model=True,
            registry_key="nbeats",
        )

        assert config.is_observe_model is True
        assert config.registry_key == "nbeats"


class TestTrainingRun:
    """Tests for the TrainingRun dataclass."""

    def test_display_name_property(self):
        """Test the display_name property."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
            assertion_urn="urn:li:assertion:test",
        )

        assert run.display_name == "DataHub Base + daily_clean"

    def test_assertion_urn_field(self):
        """Test that assertion_urn is stored correctly."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
            assertion_urn="urn:li:assertion:volume_123",
        )

        assert run.assertion_urn == "urn:li:assertion:volume_123"

    def test_assertion_urn_default_none(self):
        """Test that assertion_urn defaults to None."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
        )

        assert run.assertion_urn is None

    def test_is_observe_model_default_false(self):
        """Test that is_observe_model defaults to False."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
        )

        assert run.is_observe_model is False
        assert run.registry_key is None

    def test_observe_model_fields(self):
        """Test that observe-model fields are stored correctly."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="obs_nbeats__volume",
            model_key="obs_nbeats",
            model_name="nbeats (0.1.0)",
            preprocessing_id="volume",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#1f77b4",
            dash="dot",
            timestamp=datetime.now(),
            is_observe_model=True,
            registry_key="nbeats",
        )

        assert run.is_observe_model is True
        assert run.registry_key == "nbeats"

    def test_filtering_runs_by_is_observe_model(self):
        """Test filtering runs by is_observe_model field instead of string prefix."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        runs = {
            "datahub_base__volume": TrainingRun(
                run_id="datahub_base__volume",
                model_key="datahub_base",
                model_name="DataHub Base",
                preprocessing_id="volume",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 1.0},
                color="#ff0000",
                dash=None,
                timestamp=datetime.now(),
                is_observe_model=False,
            ),
            "obs_nbeats__volume": TrainingRun(
                run_id="obs_nbeats__volume",
                model_key="obs_nbeats",
                model_name="nbeats (0.1.0)",
                preprocessing_id="volume",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 0.5},
                color="#1f77b4",
                dash="dot",
                timestamp=datetime.now(),
                is_observe_model=True,
                registry_key="nbeats",
            ),
            "obs_prophet__volume": TrainingRun(
                run_id="obs_prophet__volume",
                model_key="obs_prophet",
                model_name="prophet (0.1.0)",
                preprocessing_id="volume",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 0.8},
                color="#ff7f0e",
                dash="dot",
                timestamp=datetime.now(),
                is_observe_model=True,
                registry_key="prophet",
            ),
        }

        # Filter by is_observe_model (the correct way)
        observe_runs = {k: v for k, v in runs.items() if v.is_observe_model}

        assert len(observe_runs) == 2
        assert "obs_nbeats__volume" in observe_runs
        assert "obs_prophet__volume" in observe_runs
        assert "datahub_base__volume" not in observe_runs

    def test_sorting_runs_by_mae(self):
        """Test that runs can be sorted by MAE."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        runs = [
            TrainingRun(
                run_id="run1",
                model_key="m1",
                model_name="Model 1",
                preprocessing_id="p1",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 5.0, "RMSE": 6.0, "MAPE": 10.0},
                color="#ff0000",
                dash=None,
                timestamp=datetime.now(),
            ),
            TrainingRun(
                run_id="run2",
                model_key="m2",
                model_name="Model 2",
                preprocessing_id="p1",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 2.0, "RMSE": 3.0, "MAPE": 5.0},
                color="#00ff00",
                dash=None,
                timestamp=datetime.now(),
            ),
            TrainingRun(
                run_id="run3",
                model_key="m3",
                model_name="Model 3",
                preprocessing_id="p1",
                train_df=train_df,
                test_df=test_df,
                forecast=forecast,
                model=MagicMock(),
                metrics={"MAE": 3.5, "RMSE": 4.0, "MAPE": 7.0},
                color="#0000ff",
                dash=None,
                timestamp=datetime.now(),
            ),
        ]

        # Sort by MAE (best/lowest first)
        sorted_runs = sorted(
            runs, key=lambda r: float(r.metrics.get("MAE", float("inf")))
        )

        assert sorted_runs[0].run_id == "run2"  # MAE 2.0
        assert sorted_runs[1].run_id == "run3"  # MAE 3.5
        assert sorted_runs[2].run_id == "run1"  # MAE 5.0


class TestTrainingGroup:
    """Tests for the TrainingGroup dataclass."""

    def test_creates_training_group(self):
        """Test creating a TrainingGroup."""
        group = TrainingGroup(
            preprocessing_id="daily_clean",
            model_keys=["datahub_base", "obs_prophet"],
        )

        assert group.preprocessing_id == "daily_clean"
        assert len(group.model_keys) == 2
        assert "datahub_base" in group.model_keys
        assert "obs_prophet" in group.model_keys

    def test_empty_model_keys(self):
        """Test TrainingGroup with empty model keys."""
        group = TrainingGroup(
            preprocessing_id="test_preproc",
            model_keys=[],
        )

        assert group.preprocessing_id == "test_preproc"
        assert group.model_keys == []


class TestCheckPreprocessingModelCompatibility:
    """Tests for the check_preprocessing_model_compatibility function."""

    def test_returns_empty_list_for_none_config(self):
        """Test that function returns empty list when config is None."""
        result = check_preprocessing_model_compatibility(None, "prophet")
        assert result == []

    def test_returns_empty_list_for_none_registry_key(self):
        """Test that function returns empty list when registry_key is None."""
        result = check_preprocessing_model_compatibility(MagicMock(), None)
        assert result == []

    def test_returns_empty_list_for_unknown_model(self):
        """Test that function returns empty list for unknown models."""
        result = check_preprocessing_model_compatibility(MagicMock(), "unknown_model")
        assert isinstance(result, list)


class TestTrainingRunPreprocessingConfig:
    """Tests for TrainingRun preprocessing config tracking."""

    def test_preprocessing_config_dict_field(self):
        """Test that preprocessing_config_dict field can be set."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        config_dict = {
            "type_col": "type",
            "pandas_transformers": ["InitDataFilterConfig", "DataFilterConfig"],
            "darts_transformers": ["DifferenceConfig", "ResamplingConfig"],
        }

        run = TrainingRun(
            run_id="test_run",
            model_key="obs_prophet",
            model_name="prophet (0.1.0)",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash="dot",
            timestamp=datetime.now(),
            is_observe_model=True,
            registry_key="prophet",
            preprocessing_config_dict=config_dict,
        )

        assert run.preprocessing_config_dict == config_dict
        assert run.preprocessing_config_dict["type_col"] == "type"

    def test_preprocessing_config_dict_default_none(self):
        """Test that preprocessing_config_dict defaults to None."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
        )

        assert run.preprocessing_config_dict is None

    def test_get_preprocessing_warnings_returns_list(self):
        """Test that get_preprocessing_warnings returns a list."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="obs_prophet",
            model_name="prophet (0.1.0)",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash="dot",
            timestamp=datetime.now(),
            is_observe_model=True,
            registry_key="prophet",
        )

        warnings = run.get_preprocessing_warnings()
        assert isinstance(warnings, list)

    def test_get_preprocessing_warnings_empty_for_non_observe_model(self):
        """Test that get_preprocessing_warnings returns empty for non-observe models."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run = TrainingRun(
            run_id="test_run",
            model_key="datahub_base",
            model_name="DataHub Base",
            preprocessing_id="daily_clean",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 1.0, "RMSE": 1.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
            is_observe_model=False,  # Not an observe model
        )

        warnings = run.get_preprocessing_warnings()
        assert warnings == []
