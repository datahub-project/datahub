"""Tests for the model_explorer/model_override.py module.

These tests focus on data handling functions that don't require Streamlit context.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from scripts.streamlit_explorer.model_explorer.model_override import (
    _compute_anomaly_metrics_from_detection,
    _generate_future_predictions,
    _infer_frequency_hours,
    _is_forecast_based_model,
    _parse_model_name_version,
    _prepare_predictions_for_save,
)


class TestInferFrequencyHours:
    """Tests for the _infer_frequency_hours function."""

    def test_infers_daily_frequency(self):
        """Test inferring daily frequency from data."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
                "y": range(10),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 24.0

    def test_infers_hourly_frequency(self):
        """Test inferring hourly frequency from data."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=24, freq="h"),
                "y": range(24),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 1.0

    def test_infers_weekly_frequency(self):
        """Test inferring weekly frequency from data."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="W"),
                "y": range(5),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 7 * 24  # 168 hours

    def test_returns_default_for_none(self):
        """Test that None DataFrame returns default 24 hours."""
        freq = _infer_frequency_hours(None)  # type: ignore[arg-type]
        assert freq == 24.0

    def test_returns_default_for_empty_dataframe(self):
        """Test that empty DataFrame returns default 24 hours."""
        df = pd.DataFrame({"ds": [], "y": []})
        freq = _infer_frequency_hours(df)
        assert freq == 24.0

    def test_returns_default_for_single_row(self):
        """Test that single row DataFrame returns default 24 hours."""
        df = pd.DataFrame(
            {
                "ds": [pd.Timestamp("2024-01-01")],
                "y": [100],
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 24.0

    def test_returns_default_for_missing_ds_column(self):
        """Test that DataFrame without 'ds' column returns default."""
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=10, freq="D"),
                "y": range(10),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 24.0

    def test_minimum_frequency_is_one_hour(self):
        """Test that frequency never goes below 1 hour."""
        # Create data with 30-minute intervals
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="30min"),
                "y": range(10),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 1.0  # Minimum is 1 hour

    def test_handles_unsorted_data(self):
        """Test that unsorted timestamps are handled correctly."""
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        # Shuffle the dates
        df = pd.DataFrame(
            {
                "ds": [dates[2], dates[0], dates[4], dates[1], dates[3]],
                "y": range(5),
            }
        )
        freq = _infer_frequency_hours(df)
        assert freq == 24.0

    def test_uses_median_for_irregular_data(self):
        """Test that median is used for irregular time intervals."""
        # Create irregular intervals: 1h, 1h, 1h, 24h
        df = pd.DataFrame(
            {
                "ds": [
                    pd.Timestamp("2024-01-01 00:00"),
                    pd.Timestamp("2024-01-01 01:00"),
                    pd.Timestamp("2024-01-01 02:00"),
                    pd.Timestamp("2024-01-01 03:00"),
                    pd.Timestamp("2024-01-02 03:00"),  # 24h gap
                ],
                "y": range(5),
            }
        )
        freq = _infer_frequency_hours(df)
        # Median of [1, 1, 1, 24] = 1 (the middle values)
        assert freq == 1.0


class TestIsForecastBasedModel:
    """Tests for the _is_forecast_based_model function."""

    def test_returns_false_for_none(self):
        """Test that None model returns False."""
        assert _is_forecast_based_model(None) is False

    def test_returns_true_for_model_with_forecast_model_attr(self):
        """Test model with forecast_model attribute returns True."""
        model = MagicMock()
        model.forecast_model = MagicMock()  # Non-None forecast model
        assert _is_forecast_based_model(model) is True

    def test_returns_false_for_model_with_none_forecast_model(self):
        """Test model with None forecast_model returns False."""
        model = MagicMock()
        model.forecast_model = None
        # Remove the class name check by using a generic class name
        model.__class__.__name__ = "GenericModel"
        assert _is_forecast_based_model(model) is False

    def test_returns_true_for_adaptive_band_class(self):
        """Test model with 'adaptive_band' in class name returns True."""
        model = MagicMock(spec=[])  # No forecast_model attr
        model.__class__ = type("AdaptiveBandForecastAnomaly", (), {})
        assert _is_forecast_based_model(model) is True

    def test_returns_true_for_forecast_class(self):
        """Test model with 'forecast' in class name returns True."""
        model = MagicMock(spec=[])
        model.__class__ = type("DataHubForecastAnomaly", (), {})
        assert _is_forecast_based_model(model) is True

    def test_returns_true_for_iforest_forecast_class(self):
        """Test model with 'iforest_forecast' in class name returns True."""
        model = MagicMock(spec=[])
        model.__class__ = type("IForestForecastAnomaly", (), {})
        assert _is_forecast_based_model(model) is True

    def test_returns_false_for_deepsvdd_class(self):
        """Test DeepSVDD model returns False (standalone model)."""
        model = MagicMock(spec=[])  # No forecast_model attr
        model.__class__ = type("DeepSVDDAnomaly", (), {})
        assert _is_forecast_based_model(model) is False

    def test_returns_false_for_generic_model(self):
        """Test generic model without forecast patterns returns False."""
        model = MagicMock(spec=[])
        model.__class__ = type("SomeOtherModel", (), {})
        assert _is_forecast_based_model(model) is False

    def test_class_name_check_is_case_insensitive(self):
        """Test that class name check is case insensitive."""
        model = MagicMock(spec=[])
        # Use a name that matches pattern when lowercased
        model.__class__ = type("FORECAST_ANOMALY", (), {})
        assert _is_forecast_based_model(model) is True


class TestPreparePredictionsForSave:
    """Tests for the _prepare_predictions_for_save function."""

    def test_returns_none_for_none_input(self):
        """Test that None input returns None."""
        result = _prepare_predictions_for_save(None)  # type: ignore[arg-type]
        assert result is None

    def test_returns_empty_for_empty_dataframe(self):
        """Test that empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame()
        result = _prepare_predictions_for_save(df)
        assert result is not None
        assert result.empty

    def test_converts_ds_to_timestamp_ms(self):
        """Test that 'ds' column is converted to 'timestamp_ms'."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 00:00:00", "2024-01-02 00:00:00"]),
                "y": [100, 200],
            }
        )
        result = _prepare_predictions_for_save(df)

        assert "timestamp_ms" in result.columns
        # Jan 1, 2024 00:00:00 UTC = 1704067200000 ms
        expected_ts = int(pd.Timestamp("2024-01-01").timestamp() * 1000)
        assert result["timestamp_ms"].iloc[0] == expected_ts

    def test_renames_detection_columns(self):
        """Test that detection columns are renamed correctly."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]),
                "detection_lower": [50.0],
                "detection_upper": [150.0],
            }
        )
        result = _prepare_predictions_for_save(df)

        assert "detection_band_lower" in result.columns
        assert "detection_band_upper" in result.columns
        assert "detection_lower" not in result.columns
        assert "detection_upper" not in result.columns
        assert result["detection_band_lower"].iloc[0] == 50.0
        assert result["detection_band_upper"].iloc[0] == 150.0

    def test_preserves_other_columns(self):
        """Test that other columns are preserved."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]),
                "y": [100],
                "yhat": [95],
                "is_anomaly": [False],
                "anomaly_score": [0.1],
            }
        )
        result = _prepare_predictions_for_save(df)

        assert "y" in result.columns
        assert "yhat" in result.columns
        assert "is_anomaly" in result.columns
        assert "anomaly_score" in result.columns

    def test_does_not_modify_original_dataframe(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]),
                "detection_lower": [50.0],
            }
        )
        original_columns = list(df.columns)
        _prepare_predictions_for_save(df)

        assert list(df.columns) == original_columns
        assert "detection_lower" in df.columns

    def test_handles_dataframe_without_ds(self):
        """Test DataFrame without 'ds' column."""
        df = pd.DataFrame(
            {
                "timestamp": [1704067200000],
                "y": [100],
            }
        )
        result = _prepare_predictions_for_save(df)

        # Should not add timestamp_ms if ds is missing
        assert "timestamp_ms" not in result.columns

    def test_handles_dataframe_without_detection_columns(self):
        """Test DataFrame without detection columns."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]),
                "y": [100],
            }
        )
        result = _prepare_predictions_for_save(df)

        assert "detection_band_lower" not in result.columns
        assert "detection_band_upper" not in result.columns

    def test_handles_nan_values(self):
        """Test that NaN values are preserved."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "y": [float("nan"), 100],
                "detection_lower": [50.0, float("nan")],
            }
        )
        result = _prepare_predictions_for_save(df)

        assert pd.isna(result["y"].iloc[0])
        assert pd.isna(result["detection_band_lower"].iloc[1])


class TestParseModelNameVersion:
    """Tests for the _parse_model_name_version function."""

    def test_parses_standard_format(self):
        """Test parsing 'model_name (version)' format."""
        name, version = _parse_model_name_version("prophet (0.1.0)", None)
        assert name == "prophet"
        assert version == "0.1.0"

    def test_parses_with_spaces(self):
        """Test parsing with spaces in model name."""
        name, version = _parse_model_name_version("my model (1.2.3)", None)
        assert name == "my model"
        assert version == "1.2.3"

    def test_handles_nested_parentheses(self):
        """Test handling of nested parentheses (uses rightmost)."""
        name, version = _parse_model_name_version("model (a) (0.1.0)", None)
        assert name == "model (a)"
        assert version == "0.1.0"

    def test_returns_model_name_when_no_version(self):
        """Test returning model_name when registry_key has no version."""
        name, version = _parse_model_name_version("simple_model", "fallback_name")
        assert name == "fallback_name"
        assert version is None

    def test_returns_none_when_both_none(self):
        """Test returning None when both inputs are None."""
        name, version = _parse_model_name_version(None, None)
        assert name is None
        assert version is None

    def test_uses_fallback_name(self):
        """Test using fallback model_name when registry_key is None."""
        name, version = _parse_model_name_version(None, "fallback")
        assert name == "fallback"
        assert version is None

    def test_handles_empty_version(self):
        """Test handling of empty version string."""
        name, version = _parse_model_name_version("model ()", None)
        assert name == "model"
        assert version == ""


class TestComputeAnomalyMetricsFromDetection:
    """Tests for the _compute_anomaly_metrics_from_detection function."""

    def test_computes_basic_metrics(self):
        """Test computing basic anomaly metrics."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=100, freq="D"),
                "y": range(100),
                "is_anomaly": [True] * 5 + [False] * 95,
            }
        )
        metrics = _compute_anomaly_metrics_from_detection(df)

        assert metrics["anomaly_count"] == 5
        assert metrics["total_points"] == 100
        assert metrics["anomaly_rate"] == 5.0

    def test_returns_empty_for_none(self):
        """Test returning empty dict for None input."""
        metrics = _compute_anomaly_metrics_from_detection(None)  # type: ignore[arg-type]
        assert metrics == {}

    def test_returns_empty_for_empty_dataframe(self):
        """Test returning empty dict for empty DataFrame."""
        df = pd.DataFrame()
        metrics = _compute_anomaly_metrics_from_detection(df)
        assert metrics == {}

    def test_handles_missing_is_anomaly_column(self):
        """Test handling DataFrame without is_anomaly column."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
                "y": range(10),
            }
        )
        metrics = _compute_anomaly_metrics_from_detection(df)

        assert metrics["anomaly_count"] == 0
        assert metrics["total_points"] == 10
        assert metrics["anomaly_rate"] == 0.0

    def test_handles_all_anomalies(self):
        """Test handling when all points are anomalies."""
        df = pd.DataFrame(
            {
                "is_anomaly": [True, True, True],
            }
        )
        metrics = _compute_anomaly_metrics_from_detection(df)

        assert metrics["anomaly_count"] == 3
        assert metrics["anomaly_rate"] == 100.0

    def test_handles_no_anomalies(self):
        """Test handling when no points are anomalies."""
        df = pd.DataFrame(
            {
                "is_anomaly": [False, False, False],
            }
        )
        metrics = _compute_anomaly_metrics_from_detection(df)

        assert metrics["anomaly_count"] == 0
        assert metrics["anomaly_rate"] == 0.0

    def test_handles_nan_in_is_anomaly(self):
        """Test handling NaN values in is_anomaly column."""
        df = pd.DataFrame(
            {
                "is_anomaly": [True, False, np.nan, True, np.nan],
            }
        )
        metrics = _compute_anomaly_metrics_from_detection(df)

        # NaN should not be counted as anomalies (sum treats NaN as 0)
        assert metrics["anomaly_count"] == 2
        assert metrics["total_points"] == 5


class TestGenerateFuturePredictions:
    """Tests for the _generate_future_predictions function."""

    def test_returns_none_for_none_model(self):
        """Test returning None when model is None."""
        run = MagicMock()
        run.model = None
        result = _generate_future_predictions(run)
        assert result is None

    def test_returns_none_for_model_without_detect(self):
        """Test returning None when model has no detect method."""
        run = MagicMock()
        run.model = MagicMock(spec=[])  # No detect method
        result = _generate_future_predictions(run)
        assert result is None

    def test_returns_none_for_standalone_model(self):
        """Test returning None for standalone models (like DeepSVDD)."""
        run = MagicMock()
        model = MagicMock()
        model.__class__ = type("DeepSVDDAnomaly", (), {})
        model.detect = MagicMock()
        delattr(model, "forecast_model")  # Ensure no forecast_model attr
        run.model = model
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=10, freq="D"), "y": range(10)}
        )
        run.test_df = None

        result = _generate_future_predictions(run)
        assert result is None

    def test_returns_none_for_empty_data(self):
        """Test returning None when no reference data available."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()  # Has forecast model
        model.detect = MagicMock()
        run.model = model
        run.train_df = pd.DataFrame()
        run.test_df = pd.DataFrame()

        result = _generate_future_predictions(run)
        assert result is None

    def test_generates_correct_number_of_periods(self):
        """Test generating the correct number of prediction periods."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()  # Has forecast model
        model.detect = MagicMock(
            side_effect=lambda df: df.assign(
                yhat=100, detection_lower=90, detection_upper=110
            )
        )
        run.model = model
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=30, freq="D"), "y": range(30)}
        )
        run.test_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-31", periods=7, freq="D"), "y": range(7)}
        )

        result = _generate_future_predictions(run, horizon_periods=14)

        assert result is not None
        assert len(result) == 14

    def test_uses_test_df_end_as_start(self):
        """Test that predictions start after test_df end date."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()
        model.detect = MagicMock(side_effect=lambda df: df)
        run.model = model
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=30, freq="D"), "y": range(30)}
        )
        test_end = pd.Timestamp("2024-02-15")
        run.test_df = pd.DataFrame(
            {"ds": pd.date_range("2024-02-01", end=test_end, freq="D"), "y": range(15)}
        )

        result = _generate_future_predictions(run, horizon_periods=5)

        assert result is not None
        # First prediction should be one day after test_df end
        first_pred_date = result["ds"].iloc[0]
        assert first_pred_date > test_end

    def test_falls_back_to_train_df_when_no_test(self):
        """Test using train_df when test_df is empty."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()
        model.detect = MagicMock(side_effect=lambda df: df)
        run.model = model
        train_end = pd.Timestamp("2024-01-30")
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", end=train_end, freq="D"), "y": range(30)}
        )
        run.test_df = pd.DataFrame()  # Empty test_df

        result = _generate_future_predictions(run, horizon_periods=5)

        assert result is not None
        first_pred_date = result["ds"].iloc[0]
        assert first_pred_date > train_end

    def test_future_df_has_nan_y_values(self):
        """Test that future predictions have NaN y values."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()
        # Capture the input DataFrame
        captured_df: list[pd.DataFrame] = []

        def capture_and_return(df: pd.DataFrame) -> pd.DataFrame:
            captured_df.append(df.copy())
            return df

        model.detect = MagicMock(side_effect=capture_and_return)
        run.model = model
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=30, freq="D"), "y": range(30)}
        )
        run.test_df = None

        _generate_future_predictions(run, horizon_periods=5)

        # Check the DataFrame passed to detect()
        assert len(captured_df) == 1
        assert all(pd.isna(captured_df[0]["y"]))

    def test_handles_detect_exception(self):
        """Test graceful handling of detect() exception."""
        run = MagicMock()
        model = MagicMock()
        model.forecast_model = MagicMock()
        model.detect = MagicMock(side_effect=ValueError("Model error"))
        run.model = model
        run.train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=30, freq="D"), "y": range(30)}
        )
        run.test_df = None

        result = _generate_future_predictions(run)
        assert result is None


class TestExtractConfigsFromAnomalyRun:
    """Tests for the _extract_configs_from_anomaly_run function.

    Note: This function uses Streamlit session state, so we test it with mocking.
    """

    @patch("scripts.streamlit_explorer.model_explorer.model_override.st")
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override._generate_future_predictions"
    )
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override.get_model_hyperparameters"
    )
    def test_extracts_basic_info(self, mock_hyperparams, mock_future_preds, mock_st):
        """Test extracting basic model information."""
        from scripts.streamlit_explorer.model_explorer.model_override import (
            _extract_configs_from_anomaly_run,
        )

        mock_st.session_state.get.return_value = "test-host"
        mock_future_preds.return_value = None
        mock_hyperparams.return_value = {"param1": 1.0}

        # Create mock anomaly run
        anomaly_run = MagicMock()
        anomaly_run.anomaly_model_key = "adaptive_band (0.1.0)"
        anomaly_run.anomaly_model_name = "adaptive_band"
        anomaly_run.model = MagicMock()
        anomaly_run.forecast_run_id = None
        anomaly_run.preprocessing_id = "daily"
        anomaly_run.detection_results = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=5, freq="D"), "y": range(5)}
        )

        result = _extract_configs_from_anomaly_run(anomaly_run, {})

        assert result["anomaly_model_name"] == "adaptive_band"
        assert result["anomaly_model_version"] == "0.1.0"

    @patch("scripts.streamlit_explorer.model_explorer.model_override.st")
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override._generate_future_predictions"
    )
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override.get_model_hyperparameters"
    )
    def test_uses_detection_results_for_standalone_model(
        self, mock_hyperparams, mock_future_preds, mock_st
    ):
        """Test that detection_results are used for standalone models."""
        from scripts.streamlit_explorer.model_explorer.model_override import (
            _extract_configs_from_anomaly_run,
        )

        mock_st.session_state.get.return_value = "test-host"
        mock_future_preds.return_value = None  # Standalone model can't generate future
        mock_hyperparams.return_value = {"note": "No hyperparameters"}

        anomaly_run = MagicMock()
        anomaly_run.anomaly_model_key = "deepsvdd (0.1.0)"
        anomaly_run.anomaly_model_name = "deepsvdd"
        anomaly_run.model = MagicMock()
        anomaly_run.forecast_run_id = None
        anomaly_run.preprocessing_id = "daily"
        anomaly_run.detection_results = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
                "y": range(5),
                "detection_lower": [50.0] * 5,
                "detection_upper": [150.0] * 5,
            }
        )

        result = _extract_configs_from_anomaly_run(anomaly_run, {})

        assert result["predictions_df"] is not None
        assert result["predictions_type"] == "historical"
        assert len(result["predictions_df"]) == 5

    @patch("scripts.streamlit_explorer.model_explorer.model_override.st")
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override._generate_future_predictions"
    )
    @patch(
        "scripts.streamlit_explorer.model_explorer.model_override.get_model_hyperparameters"
    )
    def test_uses_future_predictions_for_forecast_model(
        self, mock_hyperparams, mock_future_preds, mock_st
    ):
        """Test that future predictions are used for forecast-based models."""
        from scripts.streamlit_explorer.model_explorer.model_override import (
            _extract_configs_from_anomaly_run,
        )

        mock_st.session_state.get.return_value = "test-host"
        mock_future_preds.return_value = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-15", periods=7, freq="D"),
                "y": [float("nan")] * 7,
                "yhat": [100.0] * 7,
            }
        )
        mock_hyperparams.return_value = {"param1": 1.0}

        anomaly_run = MagicMock()
        anomaly_run.anomaly_model_key = "adaptive_band (0.1.0)"
        anomaly_run.anomaly_model_name = "adaptive_band"
        anomaly_run.model = MagicMock()
        anomaly_run.forecast_run_id = None
        anomaly_run.preprocessing_id = "daily"
        anomaly_run.detection_results = pd.DataFrame()

        result = _extract_configs_from_anomaly_run(anomaly_run, {})

        assert result["predictions_df"] is not None
        assert result["predictions_type"] == "future"
        assert len(result["predictions_df"]) == 7


class TestDataConsistency:
    """Tests for data consistency across transformations."""

    def test_timestamp_conversion_roundtrip(self):
        """Test that timestamp conversion is accurate."""
        original_ts = pd.Timestamp("2024-06-15 12:30:00")
        df = pd.DataFrame({"ds": [original_ts], "y": [100]})

        result = _prepare_predictions_for_save(df)

        # Convert back
        recovered_ts = pd.to_datetime(result["timestamp_ms"].iloc[0], unit="ms")
        # Should be within millisecond precision
        assert abs((recovered_ts - original_ts).total_seconds()) < 0.001

    def test_frequency_inference_matches_period(self):
        """Test that inferred frequency matches actual period for common frequencies."""
        test_cases = [
            ("h", 1.0),
            ("D", 24.0),
            ("W", 168.0),
        ]

        for freq, expected_hours in test_cases:
            df = pd.DataFrame(
                {
                    "ds": pd.date_range("2024-01-01", periods=10, freq=freq),
                    "y": range(10),
                }
            )
            inferred = _infer_frequency_hours(df)
            assert inferred == expected_hours, (
                f"Failed for {freq}: expected {expected_hours}, got {inferred}"
            )

    def test_column_rename_idempotent(self):
        """Test that applying prepare twice doesn't break columns."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]),
                "detection_lower": [50.0],
                "detection_upper": [150.0],
            }
        )

        result1 = _prepare_predictions_for_save(df)
        result2 = _prepare_predictions_for_save(result1)

        # Should have detection_band columns after both
        assert "detection_band_lower" in result2.columns
        assert "detection_band_upper" in result2.columns
        # Values should be preserved
        assert result2["detection_band_lower"].iloc[0] == 50.0


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_handles_timezone_aware_timestamps(self):
        """Test handling of timezone-aware timestamps."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01"]).tz_localize("UTC"),
                "y": [100],
            }
        )
        result = _prepare_predictions_for_save(df)
        assert "timestamp_ms" in result.columns

    def test_handles_very_old_timestamps(self):
        """Test handling of very old timestamps."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["1970-01-02"]),  # Just after epoch
                "y": [100],
            }
        )
        result = _prepare_predictions_for_save(df)
        assert result["timestamp_ms"].iloc[0] > 0

    def test_handles_future_timestamps(self):
        """Test handling of far future timestamps."""
        df = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2050-01-01"]),
                "y": [100],
            }
        )
        result = _prepare_predictions_for_save(df)
        # Should be at or after year 2050 in milliseconds
        # 2050-01-01 00:00:00 UTC = 2524608000000 ms
        assert result["timestamp_ms"].iloc[0] >= 2524608000000

    def test_large_dataset_performance(self):
        """Test that functions handle large datasets efficiently."""
        import time

        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=100000, freq="h"),
                "y": range(100000),
                "detection_lower": [50.0] * 100000,
                "detection_upper": [150.0] * 100000,
            }
        )

        start = time.time()
        result = _prepare_predictions_for_save(df)
        elapsed = time.time() - start

        assert len(result) == 100000
        assert elapsed < 5.0  # Should complete in under 5 seconds
