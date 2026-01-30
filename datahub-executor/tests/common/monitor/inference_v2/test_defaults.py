"""Comprehensive tests for defaults module."""

import pytest

pytest.importorskip("datahub_observe")

from datahub_observe.algorithms.preprocessing.preprocessor import PreprocessingConfig
from datahub_observe.assertions.config import AssertionPreprocessingConfig

from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    CATEGORY_STRING_TO_ENUM,
    AdjustmentSettings,
    ExclusionWindow,
    InputDataContext,
    ObserveDefaultsBuilder,
    build_adjustment_settings_from_dict,
    get_defaults_for_context,
)


class TestObserveDefaultsBuilder:
    """Tests for ObserveDefaultsBuilder class."""

    def test_resolve_category_volume(self) -> None:
        """_resolve_category correctly resolves volume category."""
        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)

        assert builder._category_enum.value == "volume"

    def test_resolve_category_rate(self) -> None:
        """_resolve_category correctly resolves rate category."""
        context = InputDataContext(assertion_category="rate")
        builder = ObserveDefaultsBuilder(context)

        assert builder._category_enum.value == "rate"

    def test_resolve_category_unknown_defaults_to_volume(self) -> None:
        """_resolve_category defaults to volume for unknown categories."""
        context = InputDataContext(assertion_category="unknown_category")
        builder = ObserveDefaultsBuilder(context)

        assert builder._category_enum.value == "volume"

    def test_resolve_category_case_insensitive(self) -> None:
        """_resolve_category handles case-insensitive category names."""
        context = InputDataContext(assertion_category="VOLUME")
        builder = ObserveDefaultsBuilder(context)

        assert builder._category_enum.value == "volume"

    def test_preprocessing_config_volume_returns_assertion_config(self) -> None:
        """preprocessing_config returns AssertionPreprocessingConfig for volume so observe-models uses VolumeTimeSeriesPreprocessor."""
        from datahub_observe.assertions.types import AssertionCategory

        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)
        config = builder.preprocessing_config()
        assert isinstance(config, AssertionPreprocessingConfig)
        assert config.assertion_type == AssertionCategory.VOLUME
        assert config.base_preprocessing_config is not None

    def test_preprocessing_config_volume_cumulative(self) -> None:
        """preprocessing_config enables differencing for cumulative volume."""
        context = InputDataContext(
            assertion_category="volume", is_dataframe_cumulative=True
        )
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )
        assert isinstance(config, PreprocessingConfig)

        # Check that differencing is enabled
        from datahub_observe.algorithms.preprocessing.transformers import (
            DifferenceConfig,
        )

        has_differencing = any(
            isinstance(t, DifferenceConfig) and getattr(t, "enabled", False)
            for t in config.darts_transformers
        )
        assert has_differencing

    def test_preprocessing_config_volume_delta(self) -> None:
        """preprocessing_config uses sum aggregation for delta volume."""
        context = InputDataContext(
            assertion_category="volume", is_dataframe_cumulative=False, is_delta=True
        )
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )

        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
        )

        resampling = next(
            t for t in config.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "sum"

    def test_preprocessing_config_volume_non_delta(self) -> None:
        """preprocessing_config uses last aggregation and filters negatives for non-delta volume."""
        context = InputDataContext(
            assertion_category="volume", is_dataframe_cumulative=False, is_delta=False
        )
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )

        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
            ValueFilterConfig,
        )

        resampling = next(
            t for t in config.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "last"

        has_value_filter = any(
            isinstance(t, ValueFilterConfig) and t.min_value == 0
            for t in config.pandas_transformers
        )
        assert has_value_filter

    def test_preprocessing_config_rate_category(self) -> None:
        """preprocessing_config uses mean aggregation for rate category."""
        context = InputDataContext(assertion_category="rate")
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )
        assert isinstance(config, PreprocessingConfig)

        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
        )

        resampling = next(
            t for t in config.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "mean"

    def test_preprocessing_config_statistic_category(self) -> None:
        """preprocessing_config uses mean aggregation for statistic category."""
        context = InputDataContext(assertion_category="statistic")
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )
        assert isinstance(config, PreprocessingConfig)

        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
        )

        resampling = next(
            t for t in config.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "mean"

    def test_preprocessing_config_freshness_category(self) -> None:
        """preprocessing_config uses mean aggregation for freshness category."""
        context = InputDataContext(assertion_category="freshness")
        builder = ObserveDefaultsBuilder(context)

        raw = builder.preprocessing_config()
        config = (
            raw.to_preprocessing_config()
            if isinstance(raw, AssertionPreprocessingConfig)
            else raw
        )
        assert isinstance(config, PreprocessingConfig)

        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
        )

        resampling = next(
            t for t in config.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "mean"

    def test_forecast_config_with_sensitivity(self) -> None:
        """forecast_config includes sensitivity when provided."""
        adjustment_settings = AdjustmentSettings(sensitivity_level=7)
        context = InputDataContext(
            assertion_category="volume", adjustment_settings=adjustment_settings
        )
        builder = ObserveDefaultsBuilder(context)

        config = builder.forecast_config()

        assert config.sensitivity == 7

    def test_forecast_config_without_sensitivity(self) -> None:
        """forecast_config has no sensitivity when not provided."""
        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)

        config = builder.forecast_config()

        assert config.sensitivity is None

    def test_anomaly_config_with_sensitivity(self) -> None:
        """anomaly_config includes sensitivity when provided."""
        adjustment_settings = AdjustmentSettings(sensitivity_level=8)
        context = InputDataContext(
            assertion_category="volume", adjustment_settings=adjustment_settings
        )
        builder = ObserveDefaultsBuilder(context)

        config = builder.anomaly_config()

        assert config.sensitivity == 8

    def test_anomaly_config_without_sensitivity(self) -> None:
        """anomaly_config has no sensitivity when not provided."""
        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)

        config = builder.anomaly_config()

        assert config.sensitivity is None

    def test_forecast_model_registry_key(self) -> None:
        """forecast_model_registry_key returns default key."""
        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)

        key = builder.forecast_model_registry_key()

        assert key is not None
        assert isinstance(key, str)

    def test_anomaly_model_registry_key(self) -> None:
        """anomaly_model_registry_key returns default key."""
        context = InputDataContext(assertion_category="volume")
        builder = ObserveDefaultsBuilder(context)

        key = builder.anomaly_model_registry_key()

        assert key is not None
        assert isinstance(key, str)


class TestGetDefaultsForContext:
    """Tests for get_defaults_for_context function."""

    def test_get_defaults_for_context_returns_builder(self) -> None:
        """get_defaults_for_context returns ObserveDefaultsBuilder."""
        context = InputDataContext(assertion_category="volume")
        builder = get_defaults_for_context(context)

        assert isinstance(builder, ObserveDefaultsBuilder)
        assert builder.context == context


class TestBuildAdjustmentSettingsFromDict:
    """Tests for build_adjustment_settings_from_dict function."""

    def test_build_adjustment_settings_from_dict_none(self) -> None:
        """build_adjustment_settings_from_dict returns None for None input."""
        result = build_adjustment_settings_from_dict(None)

        assert result is None

    def test_build_adjustment_settings_from_dict_empty(self) -> None:
        """build_adjustment_settings_from_dict returns None for empty dict."""
        result = build_adjustment_settings_from_dict({})

        assert result is None

    def test_build_adjustment_settings_from_dict_with_sensitivity(self) -> None:
        """build_adjustment_settings_from_dict extracts sensitivity level."""
        settings_dict = {"sensitivity": {"level": 5}}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.sensitivity_level == 5

    def test_build_adjustment_settings_from_dict_with_exclusion_windows(self) -> None:
        """build_adjustment_settings_from_dict extracts exclusion windows."""
        settings_dict = {
            "exclusionWindows": [
                {
                    "fixedRange": {
                        "startTimeMillis": 1000,
                        "endTimeMillis": 2000,
                    },
                    "displayName": "Test Window",
                }
            ]
        }

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert len(result.exclusion_windows) == 1
        assert result.exclusion_windows[0].start_time_ms == 1000
        assert result.exclusion_windows[0].end_time_ms == 2000
        assert result.exclusion_windows[0].display_name == "Test Window"

    def test_build_adjustment_settings_from_dict_with_lookback_days(self) -> None:
        """build_adjustment_settings_from_dict extracts lookback days."""
        settings_dict = {"trainingDataLookbackWindowDays": 30}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.training_lookback_days == 30

    def test_build_adjustment_settings_from_dict_with_context(self) -> None:
        """build_adjustment_settings_from_dict extracts context."""
        settings_dict = {"context": {"key": "value"}}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.context == {"key": "value"}

    def test_build_adjustment_settings_from_dict_complete(self) -> None:
        """build_adjustment_settings_from_dict extracts all fields."""
        settings_dict = {
            "sensitivity": {"level": 6},
            "exclusionWindows": [
                {
                    "fixedRange": {
                        "startTimeMillis": 1000,
                        "endTimeMillis": 2000,
                    }
                }
            ],
            "trainingDataLookbackWindowDays": 45,
            "context": {"env": "prod"},
        }

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.sensitivity_level == 6
        assert len(result.exclusion_windows) == 1
        assert result.training_lookback_days == 45
        assert result.context == {"env": "prod"}

    def test_build_adjustment_settings_from_dict_invalid_sensitivity(self) -> None:
        """build_adjustment_settings_from_dict handles invalid sensitivity."""
        settings_dict = {"sensitivity": "not_a_dict"}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.sensitivity_level is None

    def test_build_adjustment_settings_from_dict_invalid_exclusion_windows(
        self,
    ) -> None:
        """build_adjustment_settings_from_dict handles invalid exclusion windows."""
        settings_dict = {"exclusionWindows": "not_a_list"}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert len(result.exclusion_windows) == 0

    def test_build_adjustment_settings_from_dict_invalid_lookback_days(self) -> None:
        """build_adjustment_settings_from_dict handles invalid lookback days."""
        settings_dict = {"trainingDataLookbackWindowDays": "not_an_int"}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.training_lookback_days is None

    def test_build_adjustment_settings_from_dict_invalid_context(self) -> None:
        """build_adjustment_settings_from_dict handles invalid context."""
        settings_dict = {"context": "not_a_dict"}

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert result.context is None

    def test_build_adjustment_settings_from_dict_exclusion_window_missing_fields(
        self,
    ) -> None:
        """build_adjustment_settings_from_dict skips invalid exclusion windows."""
        settings_dict = {
            "exclusionWindows": [
                {
                    "fixedRange": {
                        "startTimeMillis": 1000,
                        # Missing endTimeMillis
                    }
                },
                {
                    # Missing fixedRange
                },
            ]
        }

        result = build_adjustment_settings_from_dict(settings_dict)  # type: ignore[arg-type]

        assert result is not None
        assert len(result.exclusion_windows) == 0


class TestExclusionWindow:
    """Tests for ExclusionWindow dataclass."""

    def test_exclusion_window_creation(self) -> None:
        """ExclusionWindow can be created with required fields."""
        window = ExclusionWindow(
            start_time_ms=1000, end_time_ms=2000, display_name="Test"
        )

        assert window.start_time_ms == 1000
        assert window.end_time_ms == 2000
        assert window.display_name == "Test"

    def test_exclusion_window_optional_display_name(self) -> None:
        """ExclusionWindow display_name is optional."""
        window = ExclusionWindow(start_time_ms=1000, end_time_ms=2000)

        assert window.display_name is None


class TestAdjustmentSettings:
    """Tests for AdjustmentSettings dataclass."""

    def test_adjustment_settings_creation(self) -> None:
        """AdjustmentSettings can be created with all fields."""
        settings = AdjustmentSettings(
            sensitivity_level=5,
            exclusion_windows=[ExclusionWindow(1000, 2000)],
            training_lookback_days=30,
            context={"key": "value"},
        )

        assert settings.sensitivity_level == 5
        assert len(settings.exclusion_windows) == 1
        assert settings.training_lookback_days == 30
        assert settings.context == {"key": "value"}

    def test_adjustment_settings_defaults(self) -> None:
        """AdjustmentSettings has sensible defaults."""
        settings = AdjustmentSettings()

        assert settings.sensitivity_level is None
        assert len(settings.exclusion_windows) == 0
        assert settings.training_lookback_days is None
        assert settings.context is None


class TestCategoryStringToEnum:
    """Tests for CATEGORY_STRING_TO_ENUM mapping."""

    def test_category_mapping_contains_expected_categories(self) -> None:
        """CATEGORY_STRING_TO_ENUM contains all expected categories."""
        expected_categories = {"volume", "freshness", "rate", "statistic", "length"}

        assert set(CATEGORY_STRING_TO_ENUM.keys()) == expected_categories
