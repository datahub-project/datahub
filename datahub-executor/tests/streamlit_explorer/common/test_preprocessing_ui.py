"""Tests for the preprocessing_ui module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.streamlit_explorer.common.preprocessing_ui import (
    PreprocessingState,
    _apply_inference_config,
    _apply_predefined_pipeline,
    build_config_for_display,
    get_available_darts_transformers,
    get_available_pandas_transformers,
    get_available_pipelines,
    get_pipeline_options,
    get_transformer_config_class,
    instantiate_pipeline,
    mark_anomalies_in_type_column,
)

# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=100, freq="h"),
            "y": range(100),
        }
    )


@pytest.fixture
def mock_registry_entry():
    """Create a mock registry entry."""
    entry = MagicMock()
    entry.name = "volume"
    entry.description = "Volume-specific preprocessing pipeline"
    entry.version = "0.1.0"
    entry.tags = frozenset({"volume", "assertion"})
    entry.metadata = {"key": "value"}
    return entry


@pytest.fixture
def mock_preprocessor():
    """Create a mock preprocessor that returns a TimeSeries."""
    preprocessor = MagicMock()
    mock_ts = MagicMock()
    mock_df = pd.DataFrame(
        {
            "y": range(50),
        },
        index=pd.date_range("2024-01-01", periods=50, freq="h"),
    )
    mock_ts.to_dataframe.return_value = mock_df
    preprocessor.process.return_value = mock_ts
    return preprocessor


# =============================================================================
# PreprocessingState Tests
# =============================================================================


class TestPreprocessingState:
    """Tests for the PreprocessingState dataclass."""

    def test_default_pipeline_mode_is_custom(self):
        """Verify default pipeline_mode is 'custom'."""
        state = PreprocessingState()
        assert state.pipeline_mode == "custom"

    def test_pipeline_mode_can_be_set(self):
        """Verify pipeline_mode can be set to a pipeline name."""
        state = PreprocessingState(pipeline_mode="volume")
        assert state.pipeline_mode == "volume"

    def test_preserves_all_default_values(self):
        """Verify all default values are preserved."""
        state = PreprocessingState()
        assert state.filtering_enabled is False
        assert state.resampling_enabled is True
        assert state.differencing_enabled is False
        assert state.frequency == "auto"
        assert state.aggregation_method == "sum"
        assert state.missing_data_strategy == "propagate"

    def test_exclude_init_results_default_is_false(self):
        """Verify exclude_init_results defaults to False (include INIT by default)."""
        state = PreprocessingState()
        assert state.exclude_init_results is False

    def test_exclude_init_results_can_be_set(self):
        """Verify exclude_init_results can be set to True."""
        state = PreprocessingState(exclude_init_results=True)
        assert state.exclude_init_results is True

    def test_type_aware_fields_have_defaults(self):
        """Verify type-aware processing fields have correct defaults."""
        state = PreprocessingState()
        assert state.type_aware_enabled is True
        assert state.type_col == "type"
        # init_filter_enabled defaults to False (opt-in filtering)
        assert state.init_filter_enabled is False
        assert state.init_trim_count == 1

    def test_init_filter_settings_can_be_configured(self):
        """Verify init filter settings can be customized."""
        state = PreprocessingState(
            init_filter_enabled=True,
            init_trim_count=5,
        )
        assert state.init_filter_enabled is True
        assert state.init_trim_count == 5

    def test_type_aware_can_be_disabled(self):
        """Verify type-aware processing can be disabled."""
        state = PreprocessingState(type_aware_enabled=False)
        assert state.type_aware_enabled is False


# =============================================================================
# Transformer Discovery Tests
# =============================================================================


class TestTransformerDiscovery:
    """Tests for transformer discovery functions."""

    def test_get_available_pandas_transformers_returns_list(self):
        """Verify get_available_pandas_transformers returns a list."""
        result = get_available_pandas_transformers()
        assert isinstance(result, list)

    def test_get_available_darts_transformers_returns_list(self):
        """Verify get_available_darts_transformers returns a list."""
        result = get_available_darts_transformers()
        assert isinstance(result, list)

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    def test_returns_empty_when_registry_unavailable(self):
        """Verify empty list returned when registry not available."""
        assert get_available_pandas_transformers() == []
        assert get_available_darts_transformers() == []

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.PreprocessorType")
    def test_fetches_pandas_transformers_from_registry(
        self, mock_preprocessor_type, mock_get_registry
    ):
        """Verify pandas transformers are fetched from registry."""
        mock_entry = MagicMock()
        mock_entry.name = "init_data_filter"
        mock_registry = MagicMock()
        mock_registry.list.return_value = [mock_entry]
        mock_get_registry.return_value = mock_registry

        result = get_available_pandas_transformers()

        assert len(result) == 1
        assert result[0].name == "init_data_filter"

    def test_get_transformer_config_class_returns_none_when_unavailable(self):
        """Verify None returned when registry unavailable."""
        with patch(
            "scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False
        ):
            result = get_transformer_config_class("unknown")
            assert result is None


# =============================================================================
# Registry Integration Tests
# =============================================================================


class TestGetAvailablePipelines:
    """Tests for get_available_pipelines function."""

    def test_returns_list(self):
        """Verify function returns a list."""
        result = get_available_pipelines()
        assert isinstance(result, list)

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    def test_returns_empty_list_when_registry_unavailable(self):
        """Verify empty list returned when registry not available."""
        result = get_available_pipelines()
        assert result == []

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_fetches_pipelines_from_registry(
        self, mock_get_registry, mock_registry_entry
    ):
        """Verify pipelines are fetched from registry."""
        mock_registry = MagicMock()
        mock_registry.list.return_value = [mock_registry_entry]
        mock_get_registry.return_value = mock_registry

        result = get_available_pipelines()

        assert len(result) == 1
        assert result[0].name == "volume"


class TestGetPipelineOptions:
    """Tests for get_pipeline_options function."""

    def test_always_includes_custom_first(self):
        """Verify 'custom' is always the first option."""
        options = get_pipeline_options()
        assert options[0][0] == "custom"
        assert "Custom" in options[0][1]

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.get_available_pipelines")
    def test_includes_all_registry_pipelines(
        self, mock_get_pipelines, mock_registry_entry
    ):
        """Verify all registry pipelines are included."""
        mock_get_pipelines.return_value = [mock_registry_entry]

        options = get_pipeline_options()

        assert len(options) == 2  # custom + volume
        assert options[1][0] == "volume"
        assert "Volume" in options[1][1]

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.get_available_pipelines")
    def test_truncates_long_descriptions(self, mock_get_pipelines):
        """Verify long descriptions are truncated."""
        entry = MagicMock()
        entry.name = "test"
        entry.description = "A" * 100  # Very long description
        mock_get_pipelines.return_value = [entry]

        options = get_pipeline_options()

        # Should be truncated with ellipsis
        assert len(options[1][1]) < 100


class TestInstantiatePipeline:
    """Tests for instantiate_pipeline function."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    def test_raises_import_error_when_registry_unavailable(self):
        """Verify ImportError raised when registry not available."""
        with pytest.raises(ImportError, match="Registry not available"):
            instantiate_pipeline("volume")

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_instantiates_pipeline_from_registry(self, mock_get_registry):
        """Verify pipeline is instantiated from registry."""
        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_class = MagicMock()
        mock_entry.cls.return_value = mock_class
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        result = instantiate_pipeline("volume")

        mock_registry.get.assert_called_once_with("volume")
        mock_entry.cls.assert_called_once()
        assert result == mock_class


# =============================================================================
# Apply Preprocessing Tests
# =============================================================================


class TestApplyPredefinedPipeline:
    """Tests for _apply_predefined_pipeline function."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_returns_none_when_registry_unavailable(self, mock_st, sample_df):
        """Verify None returned when registry not available."""
        state = PreprocessingState()
        result = _apply_predefined_pipeline(sample_df, "volume", state)
        assert result is None
        mock_st.error.assert_called()

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_applies_predefined_pipeline(
        self, mock_instantiate, sample_df, mock_preprocessor
    ):
        """Verify predefined pipeline is applied correctly."""
        mock_instantiate.return_value = mock_preprocessor
        state = PreprocessingState()

        result = _apply_predefined_pipeline(sample_df, "volume", state)

        assert result is not None
        assert "ds" in result.columns
        assert "y" in result.columns
        # Second argument is config_overrides (None when not provided)
        mock_instantiate.assert_called_once_with("volume", None)
        mock_preprocessor.process.assert_called_once()

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_handles_pipeline_not_found(self, mock_st, mock_instantiate, sample_df):
        """Verify error handling when pipeline not found."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            EntryNotFoundError,
        )

        mock_instantiate.side_effect = EntryNotFoundError("not found")
        state = PreprocessingState()

        result = _apply_predefined_pipeline(sample_df, "unknown", state)

        assert result is None
        mock_st.error.assert_called()

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_output_dataframe_has_correct_columns(
        self, mock_instantiate, sample_df, mock_preprocessor
    ):
        """Verify output DataFrame has exactly ['ds', 'y'] columns."""
        mock_instantiate.return_value = mock_preprocessor
        state = PreprocessingState()

        result = _apply_predefined_pipeline(sample_df, "volume", state)

        assert result is not None
        assert list(result.columns) == ["ds", "y"]


# =============================================================================
# Logical Consistency Tests
# =============================================================================


class TestLogicalConsistency:
    """Tests for logical consistency of the preprocessing system."""

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_all_registry_pipelines_have_process_method(self, mock_get_registry):
        """Verify all pipelines in registry implement process method."""
        # This test documents the expected interface
        mock_entry = MagicMock()
        mock_class = MagicMock()
        mock_class.return_value.process = MagicMock()
        mock_entry.cls = mock_class
        mock_registry = MagicMock()
        mock_registry.list.return_value = [mock_entry]
        mock_get_registry.return_value = mock_registry

        pipelines = get_available_pipelines()
        for entry in pipelines:
            instance = entry.cls()
            assert hasattr(instance, "process"), (
                f"Pipeline {entry.name} missing process method"
            )

    def test_custom_mode_is_always_available(self):
        """Verify custom mode is always available regardless of registry state."""
        options = get_pipeline_options()
        custom_options = [opt for opt in options if opt[0] == "custom"]
        assert len(custom_options) == 1

    def test_pipeline_mode_values_match_option_values(self):
        """Verify pipeline_mode field accepts values from get_pipeline_options."""
        options = get_pipeline_options()
        valid_values = [opt[0] for opt in options]

        # Custom should work
        state = PreprocessingState(pipeline_mode="custom")
        assert state.pipeline_mode in valid_values


# =============================================================================
# Anomaly Type Marking Tests
# =============================================================================


class TestMarkAnomaliesInTypeColumn:
    """Tests for mark_anomalies_in_type_column function."""

    def test_empty_timestamps_returns_original(self):
        """Verify empty timestamp list returns original DataFrame."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "y": range(10),
                "type": ["SUCCESS"] * 10,
            }
        )
        result = mark_anomalies_in_type_column(df, [])
        assert (result["type"] == "SUCCESS").all()

    def test_marks_matching_timestamps_as_anomaly(self):
        """Verify matching timestamps are marked as ANOMALY."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "y": range(10),
                "type": ["SUCCESS"] * 10,
            }
        )
        # Mark first and third rows (timestamps at 0 and 2 hours)
        timestamps_ms = [
            int(pd.Timestamp("2024-01-01 00:00:00").timestamp() * 1000),
            int(pd.Timestamp("2024-01-01 02:00:00").timestamp() * 1000),
        ]
        result = mark_anomalies_in_type_column(df, timestamps_ms)

        assert result["type"].iloc[0] == "ANOMALY"
        assert result["type"].iloc[1] == "SUCCESS"
        assert result["type"].iloc[2] == "ANOMALY"
        assert (result["type"].iloc[3:] == "SUCCESS").all()

    def test_returns_original_without_type_column(self):
        """Verify returns original if type column missing."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "y": range(10),
            }
        )
        timestamps_ms = [
            int(pd.Timestamp("2024-01-01 00:00:00").timestamp() * 1000),
        ]
        result = mark_anomalies_in_type_column(df, timestamps_ms)
        assert "type" not in result.columns

    def test_empty_dataframe_returns_empty(self):
        """Verify empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame({"ds": [], "y": [], "type": []})
        timestamps_ms = [int(pd.Timestamp("2024-01-01").timestamp() * 1000)]
        result = mark_anomalies_in_type_column(df, timestamps_ms)
        assert len(result) == 0

    def test_does_not_modify_original(self):
        """Verify original DataFrame is not modified."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="h"),
                "y": range(5),
                "type": ["SUCCESS"] * 5,
            }
        )
        timestamps_ms = [
            int(pd.Timestamp("2024-01-01 00:00:00").timestamp() * 1000),
        ]
        _ = mark_anomalies_in_type_column(df, timestamps_ms)

        # Original should be unchanged
        assert (df["type"] == "SUCCESS").all()

    def test_custom_type_column_name(self):
        """Verify custom type column name is respected."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="h"),
                "y": range(5),
                "result_type": ["SUCCESS"] * 5,
            }
        )
        timestamps_ms = [
            int(pd.Timestamp("2024-01-01 00:00:00").timestamp() * 1000),
        ]
        result = mark_anomalies_in_type_column(
            df, timestamps_ms, type_col="result_type"
        )
        assert result["result_type"].iloc[0] == "ANOMALY"


class TestApplyPredefinedPipelineWithAnomalyFilter:
    """Tests for _apply_predefined_pipeline with type-based anomaly filtering."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.AnomalyDataFilterConfig")
    def test_applies_anomaly_filter_when_enabled(
        self, mock_filter_config, mock_instantiate, mock_preprocessor
    ):
        """Verify AnomalyDataFilter is applied when anomaly exclusion is enabled."""
        mock_instantiate.return_value = mock_preprocessor
        mock_transformer = MagicMock()
        mock_transformer.transform.return_value = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=90, freq="h"),
                "y": range(90),
                "type": ["SUCCESS"] * 90,
            }
        )
        mock_filter_config.return_value.create_transformer.return_value = (
            mock_transformer
        )

        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=100, freq="h"),
                "y": range(100),
                "type": ["SUCCESS"] * 90 + ["ANOMALY"] * 10,
            }
        )
        state = PreprocessingState(use_anomalies_as_exclusions=True)

        _apply_predefined_pipeline(df, "volume", state)

        # Verify the filter was created with correct params
        mock_filter_config.assert_called_once_with(
            enabled=True, type_values=["ANOMALY"]
        )

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_no_filter_when_anomaly_exclusion_disabled(
        self, mock_instantiate, mock_preprocessor
    ):
        """Verify no filtering when anomaly exclusion is disabled."""
        mock_instantiate.return_value = mock_preprocessor

        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=100, freq="h"),
                "y": range(100),
                "type": ["SUCCESS"] * 90 + ["ANOMALY"] * 10,
            }
        )
        state = PreprocessingState(use_anomalies_as_exclusions=False)

        _apply_predefined_pipeline(df, "volume", state)

        # Verify pipeline received full data (all 100 rows)
        call_args = mock_preprocessor.process.call_args
        filtered_df = call_args[0][0]
        assert len(filtered_df) == 100


# =============================================================================
# Pipeline Config Override Tests
# =============================================================================


class TestPipelineConfigOverrides:
    """Tests for dynamic pipeline configuration overrides."""

    def test_preprocessing_state_has_pipeline_configs(self):
        """Verify PreprocessingState has pipeline_configs field."""
        state = PreprocessingState()
        assert hasattr(state, "pipeline_configs")
        assert isinstance(state.pipeline_configs, dict)
        assert state.pipeline_configs == {}

    def test_pipeline_configs_can_store_values(self):
        """Verify pipeline configs can store override values."""
        state = PreprocessingState()
        state.pipeline_configs["volume"] = {"convert_cumulative": True}
        state.pipeline_configs["field"] = {"convert_to_delta": False}

        assert state.pipeline_configs["volume"]["convert_cumulative"] is True
        assert state.pipeline_configs["field"]["convert_to_delta"] is False

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.instantiate_pipeline")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_config_overrides_passed_to_instantiate_pipeline(
        self, mock_instantiate, sample_df, mock_preprocessor
    ):
        """Verify config overrides are passed to instantiate_pipeline."""
        mock_instantiate.return_value = mock_preprocessor
        config_overrides = {"convert_cumulative": True}
        state = PreprocessingState()

        _apply_predefined_pipeline(sample_df, "volume", state, config_overrides)

        mock_instantiate.assert_called_once_with("volume", config_overrides)

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_pipeline_config_class"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_instantiate_pipeline_with_config_overrides(
        self, mock_get_config_cls, mock_get_registry
    ):
        """Verify instantiate_pipeline creates config from overrides."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            instantiate_pipeline,
        )

        # Setup mock config class
        mock_config_cls = MagicMock()
        mock_config_instance = MagicMock()
        mock_config_cls.return_value = mock_config_instance
        mock_get_config_cls.return_value = mock_config_cls

        # Setup mock pipeline class with from_config
        mock_pipeline_cls = MagicMock()
        mock_pipeline_instance = MagicMock()
        mock_pipeline_cls.from_config.return_value = mock_pipeline_instance

        mock_entry = MagicMock()
        mock_entry.cls = mock_pipeline_cls
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        # Call with config overrides
        result = instantiate_pipeline("volume", {"convert_cumulative": True})

        # Verify config class was instantiated with filtered overrides
        mock_config_cls.assert_called_once_with(convert_cumulative=True)

        # Verify from_config was called with the config instance
        mock_pipeline_cls.from_config.assert_called_once_with(mock_config_instance)

        assert result == mock_pipeline_instance

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_instantiate_pipeline_without_config_overrides_uses_defaults(
        self, mock_get_registry
    ):
        """Verify instantiate_pipeline uses defaults when no overrides."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            instantiate_pipeline,
        )

        mock_pipeline_cls = MagicMock()
        mock_pipeline_instance = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline_instance

        mock_entry = MagicMock()
        mock_entry.cls = mock_pipeline_cls
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        result = instantiate_pipeline("volume", None)

        # Verify cls() was called without arguments (using defaults)
        mock_pipeline_cls.assert_called_once_with()
        assert result == mock_pipeline_instance

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_pipeline_config_class"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_instantiate_pipeline_filters_none_values_from_overrides(
        self, mock_get_config_cls, mock_get_registry
    ):
        """Verify None values are filtered from config overrides."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            instantiate_pipeline,
        )

        mock_config_cls = MagicMock()
        mock_config_instance = MagicMock()
        mock_config_cls.return_value = mock_config_instance
        mock_get_config_cls.return_value = mock_config_cls

        mock_pipeline_cls = MagicMock()
        mock_pipeline_cls.from_config.return_value = MagicMock()

        mock_entry = MagicMock()
        mock_entry.cls = mock_pipeline_cls
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        # Call with overrides including None values
        instantiate_pipeline(
            "volume", {"convert_cumulative": True, "allow_negative": None}
        )

        # Verify only non-None values passed to config class
        mock_config_cls.assert_called_once_with(convert_cumulative=True)

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_pipeline_config_class"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_instantiate_pipeline_filters_special_fields_from_overrides(
        self, mock_get_config_cls, mock_get_registry
    ):
        """Verify 'type' and 'preprocessing_config' are filtered from overrides."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            instantiate_pipeline,
        )

        mock_config_cls = MagicMock()
        mock_config_instance = MagicMock()
        mock_config_cls.return_value = mock_config_instance
        mock_get_config_cls.return_value = mock_config_cls

        mock_pipeline_cls = MagicMock()
        mock_pipeline_cls.from_config.return_value = MagicMock()

        mock_entry = MagicMock()
        mock_entry.cls = mock_pipeline_cls
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        # Call with overrides including special fields
        instantiate_pipeline(
            "volume",
            {
                "convert_cumulative": True,
                "type": "volume",
                "preprocessing_config": {},
            },
        )

        # Verify special fields are filtered out
        mock_config_cls.assert_called_once_with(convert_cumulative=True)


# =============================================================================
# Pipeline Config Discovery Tests
# =============================================================================


class TestPipelineConfigDiscovery:
    """Tests for pipeline configuration discovery functions."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    def test_get_pipeline_config_class_returns_none_when_no_registry(self):
        """Verify returns None when registry unavailable."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_class,
        )

        result = get_pipeline_config_class("volume")
        assert result is None

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_get_pipeline_config_class_returns_class_from_registry(
        self, mock_get_registry
    ):
        """Verify returns config class from registry."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_class,
        )

        mock_config_cls = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_config_class.return_value = mock_config_cls
        mock_get_registry.return_value = mock_registry

        result = get_pipeline_config_class("volume")

        assert result == mock_config_cls
        mock_registry.get_config_class.assert_called_once_with("volume")

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_get_pipeline_config_class_returns_none_on_exception(
        self, mock_get_registry
    ):
        """Verify returns None when exception occurs."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_class,
        )

        mock_registry = MagicMock()
        mock_registry.get_config_class.side_effect = Exception("Test error")
        mock_get_registry.return_value = mock_registry

        result = get_pipeline_config_class("volume")

        assert result is None

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_pipeline_config_class"
    )
    def test_get_pipeline_config_defaults_returns_empty_dict_when_no_class(
        self, mock_get_config_cls
    ):
        """Verify returns empty dict when config class is None."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_defaults,
        )

        mock_get_config_cls.return_value = None

        result = get_pipeline_config_defaults("volume")

        assert result == {}

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_pipeline_config_class"
    )
    def test_get_pipeline_config_defaults_uses_get_defaults_method(
        self, mock_get_config_cls
    ):
        """Verify uses get_defaults() if available on config class."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_defaults,
        )

        mock_config_cls = MagicMock()
        mock_config_cls.get_defaults.return_value = {"convert_cumulative": False}
        mock_get_config_cls.return_value = mock_config_cls

        result = get_pipeline_config_defaults("volume")

        assert result == {"convert_cumulative": False}
        mock_config_cls.get_defaults.assert_called_once()

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", False)
    def test_get_pipeline_config_metadata_returns_empty_dict_when_no_registry(self):
        """Verify returns empty dict when registry unavailable."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_metadata,
        )

        result = get_pipeline_config_metadata("volume")
        assert result == {}

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.get_preprocessing_registry"
    )
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_REGISTRY", True)
    def test_get_pipeline_config_metadata_returns_entry_metadata(
        self, mock_get_registry
    ):
        """Verify returns metadata from registry entry."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_pipeline_config_metadata,
        )

        mock_entry = MagicMock()
        mock_entry.metadata = {"supported_metrics": ["NULL_COUNT"]}
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_entry
        mock_get_registry.return_value = mock_registry

        result = get_pipeline_config_metadata("field")

        assert result == {"supported_metrics": ["NULL_COUNT"]}


# =============================================================================
# Assertion Context Tests
# =============================================================================


class TestAssertionContext:
    """Tests for assertion context retrieval."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_get_assertion_context_returns_values_from_session_state(self, mock_st):
        """Verify returns assertion type and metric from session state."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_assertion_context,
        )

        mock_st.session_state = {
            "current_assertion_type": "VOLUME",
            "current_assertion_metric": "NULL_COUNT",
        }

        assertion_type, metric = get_assertion_context()

        assert assertion_type == "VOLUME"
        assert metric == "NULL_COUNT"

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_get_assertion_context_returns_none_when_not_set(self, mock_st):
        """Verify returns None when session state values not set."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            get_assertion_context,
        )

        mock_st.session_state = {}

        assertion_type, metric = get_assertion_context()

        assert assertion_type is None
        assert metric is None


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_format_field_label_converts_snake_case(self):
        """Verify snake_case is converted to Title Case."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            _format_field_label,
        )

        assert _format_field_label("convert_cumulative") == "Convert Cumulative"
        assert _format_field_label("allow_negative") == "Allow Negative"
        assert _format_field_label("metric_type") == "Metric Type"

    def test_get_field_help_returns_help_text(self):
        """Verify returns help text for known fields."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            _get_field_help,
        )

        help_text = _get_field_help("convert_cumulative", "volume", {})
        assert "cumulative" in help_text.lower()

        help_text = _get_field_help("convert_to_delta", "field", {})
        assert "absolute values" in help_text.lower()  # Matches actual help text

    def test_get_field_help_returns_empty_for_unknown_fields(self):
        """Verify returns empty string for unknown fields."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            _get_field_help,
        )

        help_text = _get_field_help("unknown_field", "volume", {})
        assert help_text == ""


# =============================================================================
# Serialization Tests
# =============================================================================


class TestSerializePreprocessingState:
    """Tests for preprocessing state serialization."""

    def test_serialize_custom_mode_includes_all_settings(self):
        """Verify custom mode serializes all relevant settings."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_preprocessing_state,
        )

        state = PreprocessingState(
            pipeline_mode="custom",
            init_filter_enabled=True,
            init_trim_count=2,
            use_anomalies_as_exclusions=True,
            anomaly_window_minutes=30,
            resampling_enabled=True,
            frequency="daily",
            aggregation_method="sum",
            differencing_enabled=True,
            difference_order=1,
            missing_data_strategy="interpolate",
        )

        result = serialize_preprocessing_state(state)

        assert result["pipeline_mode"] == "custom"
        assert result["init_filter"]["enabled"] is True
        assert result["init_filter"]["trim_count"] == 2
        assert result["anomaly_exclusion"]["enabled"] is True
        assert result["anomaly_exclusion"]["window_minutes"] == 30
        assert result["resampling"]["enabled"] is True
        assert result["resampling"]["frequency"] == "daily"
        assert result["differencing"]["enabled"] is True
        assert result["missing_data"]["strategy"] == "interpolate"

    def test_serialize_predefined_pipeline_includes_pipeline_options(self):
        """Verify predefined pipeline mode includes pipeline options."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_preprocessing_state,
        )

        state = PreprocessingState(pipeline_mode="volume")
        state.pipeline_configs["volume"] = {"convert_cumulative": True}

        result = serialize_preprocessing_state(state)

        assert result["pipeline_mode"] == "volume"
        assert result["pipeline_options"]["convert_cumulative"] is True
        assert "resampling" not in result  # Custom-only settings not included

    def test_serialize_includes_init_filter_settings(self):
        """Verify init filter settings are always included."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_preprocessing_state,
        )

        state = PreprocessingState(
            pipeline_mode="volume",
            init_filter_enabled=True,
            init_trim_count=3,
        )

        result = serialize_preprocessing_state(state)

        assert result["init_filter"]["enabled"] is True
        assert result["init_filter"]["trim_count"] == 3

    def test_serialize_includes_anomaly_exclusion_settings(self):
        """Verify anomaly exclusion settings are always included."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_preprocessing_state,
        )

        state = PreprocessingState(
            pipeline_mode="field",
            use_anomalies_as_exclusions=True,
            anomaly_window_minutes=60,
        )

        result = serialize_preprocessing_state(state)

        assert result["anomaly_exclusion"]["enabled"] is True
        assert result["anomaly_exclusion"]["window_minutes"] == 60


class TestSerializeAppliedConfig:
    """Tests for serializing the actual observe-models config."""

    def test_serialize_applied_config_returns_none_for_none_input(self):
        """Verify returns None when config is None."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_applied_config,
        )

        result = serialize_applied_config(None)
        assert result is None

    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.HAS_PREPROCESSING", False
    )
    def test_serialize_applied_config_returns_none_without_preprocessing(self):
        """Verify returns None when preprocessing not available."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            serialize_applied_config,
        )

        result = serialize_applied_config({"some": "config"})
        assert result is None

    def test_serialize_applied_config_with_real_config(self):
        """Verify serializes actual observe-models config when available."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            HAS_PREPROCESSING,
            serialize_applied_config,
        )

        if not HAS_PREPROCESSING:
            pytest.skip("observe-models not available")

        # Import actual config class
        from datahub_observe.algorithms.preprocessing import (  # type: ignore[import-untyped]
            PreprocessingConfig,
        )

        config = PreprocessingConfig()
        result = serialize_applied_config(config)

        # Should return a dictionary with preprocessing config structure
        assert result is not None
        assert isinstance(result, dict)
        # The config should have pandas_transformers and darts_transformers keys
        assert "pandas_transformers" in result or "darts_transformers" in result


# =============================================================================
# From Inference Mode Tests
# =============================================================================


class TestFromInferencePipelineMode:
    """Tests for the 'from_inference' pipeline mode functionality."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.get_available_pipelines")
    def test_get_pipeline_options_includes_from_inference_when_config_loaded(
        self, mock_get_pipelines, mock_st
    ):
        """Verify 'from_inference' option appears when inference config is loaded."""
        mock_get_pipelines.return_value = []

        # Setup mock config in session state
        mock_config = MagicMock()
        mock_config.type = "volume"
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": mock_config,
            "_loaded_inference_source_urn": "urn:li:assertion:test123",
        }

        options = get_pipeline_options()

        # Should have custom + from_inference
        option_values = [opt[0] for opt in options]
        assert "custom" in option_values
        assert "from_inference" in option_values

        # Verify label includes config type
        from_inference_option = next(
            opt for opt in options if opt[0] == "from_inference"
        )
        assert "volume" in from_inference_option[1]

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.get_available_pipelines")
    def test_get_pipeline_options_excludes_from_inference_when_no_config(
        self, mock_get_pipelines, mock_st
    ):
        """Verify 'from_inference' option is not present when no config loaded."""
        mock_get_pipelines.return_value = []
        mock_st.session_state = {}  # No loaded config

        options = get_pipeline_options()

        option_values = [opt[0] for opt in options]
        assert "custom" in option_values
        assert "from_inference" not in option_values

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.get_available_pipelines")
    def test_get_pipeline_options_handles_dict_config(
        self, mock_get_pipelines, mock_st
    ):
        """Verify handles dict config type correctly."""
        mock_get_pipelines.return_value = []

        # Setup dict config in session state
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": {
                "type": "field",
                "metric_type": "NULL_COUNT",
            },
            "_loaded_inference_source_urn": "urn:li:assertion:test456",
        }

        options = get_pipeline_options()

        option_values = [opt[0] for opt in options]
        assert "from_inference" in option_values

        # Verify label includes config type from dict
        from_inference_option = next(
            opt for opt in options if opt[0] == "from_inference"
        )
        assert "field" in from_inference_option[1]

    def test_preprocessing_state_accepts_from_inference_mode(self):
        """Verify PreprocessingState accepts 'from_inference' as pipeline_mode."""
        state = PreprocessingState(pipeline_mode="from_inference")
        assert state.pipeline_mode == "from_inference"


class TestApplyInferenceConfig:
    """Tests for _apply_inference_config function."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_returns_none_when_no_config_loaded(self, mock_st):
        """Verify returns None and shows error when no config loaded."""
        mock_st.session_state = {}  # No loaded config
        state = PreprocessingState()

        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "y": range(10),
            }
        )

        result = _apply_inference_config(df, state)

        assert result is None
        mock_st.error.assert_called()

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch(
        "scripts.streamlit_explorer.common.preprocessing_ui.VolumeTimeSeriesPreprocessor",
        create=True,
    )
    def test_applies_volume_config(self, mock_preprocessor_cls, mock_st):
        """Verify volume config is applied using VolumeTimeSeriesPreprocessor."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.type = "volume"
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": mock_config,
        }

        # Setup mock preprocessor
        mock_preprocessor = MagicMock()
        mock_ts = MagicMock()
        mock_df = pd.DataFrame(
            {"y": range(10)},
            index=pd.date_range("2024-01-01", periods=10, freq="h"),
        )
        mock_ts.to_dataframe.return_value = mock_df
        mock_preprocessor.process.return_value = mock_ts
        mock_preprocessor_cls.from_config.return_value = mock_preprocessor

        # Patch the import
        with patch.dict(
            "sys.modules",
            {
                "datahub_observe.algorithms.preprocessing": MagicMock(
                    VolumeTimeSeriesPreprocessor=mock_preprocessor_cls
                )
            },
        ):
            state = PreprocessingState()
            df = pd.DataFrame(
                {
                    "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                    "y": range(10),
                }
            )

            # Import fresh to get patched version
            from scripts.streamlit_explorer.common.preprocessing_ui import (
                _apply_inference_config,
            )

            _apply_inference_config(df, state)

            # Verify correct preprocessor was used
            mock_preprocessor_cls.from_config.assert_called_once_with(mock_config)

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.AnomalyDataFilterConfig")
    def test_applies_anomaly_filter_when_enabled(self, mock_filter_config, mock_st):
        """Verify anomaly filter is applied before inference config."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.type = "volume"
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": mock_config,
        }

        # Setup mock filter
        mock_transformer = MagicMock()
        mock_transformer.transform.return_value = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=8, freq="h"),
                "y": range(8),
                "type": ["SUCCESS"] * 8,
            }
        )
        mock_filter_config.return_value.create_transformer.return_value = (
            mock_transformer
        )

        state = PreprocessingState(use_anomalies_as_exclusions=True)
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "y": range(10),
                "type": ["SUCCESS"] * 8 + ["ANOMALY"] * 2,
            }
        )

        # The function will fail after filter because we haven't mocked the preprocessor
        # but we can verify the filter was called
        try:
            _apply_inference_config(df, state)
        except Exception:
            pass  # Expected to fail after filtering

        mock_filter_config.assert_called_once_with(
            enabled=True, type_values=["ANOMALY"]
        )


class TestBuildConfigForDisplayFromInference:
    """Tests for build_config_for_display with from_inference mode."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_PREPROCESSING", True)
    def test_returns_error_when_no_config_loaded(self, mock_st):
        """Verify returns error dict when no inference config loaded."""
        mock_st.session_state = {}
        state = PreprocessingState(pipeline_mode="from_inference")

        with patch(
            "datahub_observe.algorithms.preprocessing.serialization.pipeline_config_to_dict"
        ):
            result = build_config_for_display(state)

        assert result is not None
        assert "error" in result

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_PREPROCESSING", True)
    def test_serializes_volume_config(self, mock_st):
        """Verify volume config is serialized with correct type."""
        mock_config = MagicMock()
        mock_config.type = "volume"
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": mock_config,
            "_loaded_inference_source_urn": "urn:li:assertion:test",
        }

        state = PreprocessingState(pipeline_mode="from_inference")

        with patch(
            "datahub_observe.algorithms.preprocessing.serialization.pipeline_config_to_dict"
        ) as mock_to_dict:
            mock_to_dict.return_value = {"type": "volume", "convert_cumulative": False}
            result = build_config_for_display(state)

        assert result is not None
        assert result.get("_source") == "inference"
        assert result.get("_source_urn") == "urn:li:assertion:test"

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_PREPROCESSING", True)
    def test_handles_dict_config(self, mock_st):
        """Verify dict config is returned as-is with metadata."""
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": {
                "type": "custom",
                "settings": {},
            },
            "_loaded_inference_source_urn": "urn:li:assertion:test",
        }

        state = PreprocessingState(pipeline_mode="from_inference")

        with patch(
            "datahub_observe.algorithms.preprocessing.serialization.pipeline_config_to_dict"
        ):
            result = build_config_for_display(state)

        assert result is not None
        assert result.get("_source") == "inference"


class TestRenderInferenceConfigInfo:
    """Tests for render_inference_config_info function."""

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_shows_warning_when_no_config_loaded(self, mock_st):
        """Verify warning shown when no inference config loaded."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            render_inference_config_info,
        )

        mock_st.session_state = {}

        render_inference_config_info()

        mock_st.warning.assert_called_with("No inference config loaded")

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    @patch("scripts.streamlit_explorer.common.preprocessing_ui.HAS_PREPROCESSING", True)
    def test_shows_config_type_info(self, mock_st):
        """Verify config type info is displayed."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            render_inference_config_info,
        )

        mock_config = MagicMock()
        mock_config.type = "volume"
        mock_config.__class__.__name__ = "VolumePreprocessorConfig"
        mock_st.session_state = {
            "_loaded_inference_preprocessing_config": mock_config,
            "_loaded_inference_source_urn": "urn:li:assertion:test123",
        }

        render_inference_config_info()

        # Verify info was displayed
        mock_st.info.assert_called()
        call_args = mock_st.info.call_args[0][0]
        assert "volume" in call_args

    @patch("scripts.streamlit_explorer.common.preprocessing_ui.st")
    def test_clear_config_button_clears_session_state(self, mock_st):
        """Verify clear button removes config from session state."""
        from scripts.streamlit_explorer.common.preprocessing_ui import (
            render_inference_config_info,
        )

        mock_config = MagicMock()
        mock_config.type = "volume"

        # Create a MagicMock that supports both dict-style and attribute-style access
        mock_session_state = MagicMock()
        mock_session_state.get.side_effect = lambda k, default=None: {
            "_loaded_inference_preprocessing_config": mock_config,
            "_loaded_inference_source_urn": "urn:li:assertion:test",
            "preprocessing_state": PreprocessingState(pipeline_mode="from_inference"),
        }.get(k, default)
        mock_session_state.preprocessing_state = PreprocessingState(
            pipeline_mode="from_inference"
        )
        mock_st.session_state = mock_session_state

        # Simulate button click
        mock_st.button.return_value = True

        render_inference_config_info()

        # Verify config was cleared
        mock_st.session_state.pop.assert_any_call(
            "_loaded_inference_preprocessing_config", None
        )
        mock_st.session_state.pop.assert_any_call("_loaded_inference_source_urn", None)
        mock_st.rerun.assert_called()
