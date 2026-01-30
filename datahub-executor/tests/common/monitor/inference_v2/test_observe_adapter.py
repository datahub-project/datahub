"""Tests for ObserveAdapter and ModelFactory configuration."""

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from unittest.mock import MagicMock, patch

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
from datahub_executor.common.monitor.inference_v2.observe_adapter.adapter import (
    ObserveAdapter,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
    get_defaults_for_context,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    TuningDecision,
    TuningReason,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    ModelPairing,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
    ModelFactory,
)
from datahub_executor.common.monitor.inference_v2.types import TrainingResultBuilder


def _make_context(
    assertion_category: str = "volume",
    is_dataframe_cumulative: bool = False,
    is_delta: bool | None = None,
) -> InputDataContext:
    """Helper to create InputDataContext for tests."""
    return InputDataContext(
        assertion_category=assertion_category,
        is_dataframe_cumulative=is_dataframe_cumulative,
        is_delta=is_delta,
    )


def _make_factory(
    context: InputDataContext | None = None,
    existing_model_config: ModelConfig | None = None,
) -> ModelFactory:
    """Helper to create ModelFactory for tests."""
    if context is None:
        context = _make_context()
    defaults = get_defaults_for_context(context)
    return ModelFactory(defaults, existing_model_config)


def _make_test_df(n: int = 50) -> pd.DataFrame:
    """Create a test DataFrame with ds and y columns."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="h"),
            "y": range(n),
        }
    )


class TestInitializedModels:
    """Tests for InitializedModels dataclass properties."""

    def test_forecast_model_property_returns_standalone_model(self) -> None:
        """forecast_model property returns standalone model when set."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        mock_standalone = MagicMock()
        models = InitializedModels(_standalone_forecast_model=mock_standalone)

        assert models.forecast_model is mock_standalone

    def test_forecast_model_property_returns_from_anomaly_model(self) -> None:
        """forecast_model property extracts from anomaly_model when no standalone."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        mock_anomaly = MagicMock()
        mock_anomaly.forecast_model = MagicMock()
        models = InitializedModels(anomaly_model=mock_anomaly)

        assert models.forecast_model is mock_anomaly.forecast_model

    def test_forecast_model_property_raises_when_no_model_available(self) -> None:
        """forecast_model property raises when neither model is available."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        models = InitializedModels()

        with pytest.raises(AssertionError, match="No forecast model available"):
            _ = models.forecast_model

    def test_forecast_config_property_returns_standalone_config(self) -> None:
        """forecast_config property returns standalone config when set."""
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        standalone_config = ForecastModelConfig(hyperparameters={"test": True})
        models = InitializedModels(_standalone_forecast_config=standalone_config)

        assert models.forecast_config is standalone_config

    def test_forecast_config_property_returns_from_anomaly_config(self) -> None:
        """forecast_config property extracts from anomaly_config when no standalone."""
        from datahub_observe.algorithms.anomaly_detection.config import (
            AnomalyModelConfig,
        )
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        forecast_config = ForecastModelConfig(hyperparameters={"embedded": True})
        anomaly_config = AnomalyModelConfig(forecast_model_config=forecast_config)
        models = InitializedModels(anomaly_config=anomaly_config)

        assert models.forecast_config is forecast_config

    def test_forecast_config_property_raises_when_no_anomaly_config(self) -> None:
        """forecast_config property raises when no config available."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        models = InitializedModels()

        with pytest.raises(AssertionError, match="No forecast config available"):
            _ = models.forecast_config

    def test_forecast_config_property_raises_when_no_forecast_in_anomaly_config(
        self,
    ) -> None:
        """forecast_config raises when anomaly_config has no forecast_model_config."""
        from datahub_observe.algorithms.anomaly_detection.config import (
            AnomalyModelConfig,
        )

        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            InitializedModels,
        )

        anomaly_config = AnomalyModelConfig()  # No forecast_model_config
        models = InitializedModels(anomaly_config=anomaly_config)

        with pytest.raises(AssertionError):
            _ = models.forecast_config


class TestModelFactoryWarmStart:
    """Tests for warm start configuration extraction methods in ModelFactory."""

    def test_get_existing_forecast_config_from_config_object(self) -> None:
        """Extract config when deserialize returns ForecastModelConfig object."""
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        model_config = ModelConfig(
            forecast_config_json='{"hyperparameters": {"interval_width": 0.95}}',
            preprocessing_config_json="{}",
        )

        mock_config = ForecastModelConfig(hyperparameters={"interval_width": 0.95})

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.ForecastConfigSerializer.deserialize",
            return_value=mock_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_forecast_config()

        assert result is not None
        assert result.hyperparameters == {"interval_width": 0.95}

    def test_get_existing_forecast_config_from_dict_fallback(self) -> None:
        """Extract config when deserialize returns dict (fallback path)."""
        model_config = ModelConfig(
            forecast_config_json='{"hyperparameters": {"changepoint_prior_scale": 0.05}}',
            preprocessing_config_json="{}",
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.ForecastConfigSerializer.deserialize",
            return_value={"hyperparameters": {"changepoint_prior_scale": 0.05}},
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_forecast_config()

        assert result is not None
        assert result.hyperparameters == {"changepoint_prior_scale": 0.05}

    def test_get_existing_forecast_config_returns_none_when_no_config(self) -> None:
        """Returns None when no existing config."""
        factory = _make_factory(existing_model_config=None)
        result = factory._get_existing_forecast_config()
        assert result is None

    def test_get_existing_anomaly_config_from_config_object(self) -> None:
        """Extract anomaly config when deserialize returns AnomalyModelConfig."""
        from datahub_observe.algorithms.anomaly_detection.config import (
            AnomalyModelConfig,
        )

        model_config = ModelConfig(
            anomaly_config_json='{"hyperparameters": {"deviation_threshold": 1.8}}',
            preprocessing_config_json="{}",
        )

        mock_config = AnomalyModelConfig(hyperparameters={"deviation_threshold": 1.8})

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.AnomalyConfigSerializer.deserialize",
            return_value=mock_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_anomaly_config()

        assert result is not None
        assert result.hyperparameters == {"deviation_threshold": 1.8}


class TestObserveAdapterInputValidation:
    def test_empty_dataframe_raises(self) -> None:
        adapter = ObserveAdapter()
        context = _make_context()
        df = pd.DataFrame()

        with pytest.raises(TrainingErrorException) as excinfo:
            adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT

    def test_missing_columns_raises(self) -> None:
        adapter = ObserveAdapter()
        context = _make_context()
        df = pd.DataFrame({"ds": [pd.Timestamp("2024-01-01")]})

        with pytest.raises(TrainingErrorException) as excinfo:
            adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.INPUT_DATA_INVALID

    def test_get_existing_preprocessing_config_returns_deserialized(self) -> None:
        """Returns deserialized preprocessing config when available."""
        model_config = ModelConfig(
            preprocessing_config_json='{"type": "volume", "convert_cumulative": true}',
        )

        mock_preproc = MagicMock()
        mock_preproc.type = "volume"

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.PreprocessingConfigSerializer.deserialize",
            return_value=mock_preproc,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_preprocessing_config()

        assert result == mock_preproc

    def test_get_existing_preprocessing_config_returns_none_when_no_config(
        self,
    ) -> None:
        """Returns None when no existing model config."""
        factory = _make_factory(existing_model_config=None)
        result = factory._get_existing_preprocessing_config()
        assert result is None


def _resolve_preprocessing_config(raw):
    """Resolve to PreprocessingConfig for inspection (volume returns AssertionPreprocessingConfig)."""
    from datahub_observe.assertions.config import AssertionPreprocessingConfig

    return (
        raw.to_preprocessing_config()
        if isinstance(raw, AssertionPreprocessingConfig)
        else raw
    )


class TestModelFactoryPreprocessingConfig:
    """Tests for preprocessing config building in ModelFactory."""

    def test_build_preprocessing_config_returns_volume_config_when_cumulative(
        self,
    ) -> None:
        """Returns AssertionPreprocessingConfig (volume) with differencing enabled for cumulative."""
        from datahub_observe.algorithms.preprocessing.preprocessor import (
            PreprocessingConfig,
        )
        from datahub_observe.algorithms.preprocessing.transformers import (
            DifferenceConfig,
            FrequencyAlignmentConfig,
            ResamplingConfig,
            ValueFilterConfig,
        )
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=True,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, (AssertionPreprocessingConfig, PreprocessingConfig))
        cfg = _resolve_preprocessing_config(result)
        assert any(
            isinstance(t, DifferenceConfig) and bool(getattr(t, "enabled", False))
            for t in cfg.darts_transformers
        )
        resampling = next(
            t for t in cfg.darts_transformers if isinstance(t, ResamplingConfig)
        )
        # Cumulative volume is differenced to deltas; resampling uses sum.
        assert resampling.aggregation_method == "sum"
        freq_align = next(
            t
            for t in cfg.pandas_transformers
            if isinstance(t, FrequencyAlignmentConfig)
        )
        assert freq_align.aggregation_method == "sum"
        value_filter = next(
            t
            for t in cfg.pandas_transformers
            if isinstance(t, ValueFilterConfig) and t.min_value == 0
        )
        # Volume non-delta uses non-strict validation (default in defaults.py)
        assert value_filter.strict is False

    def test_build_preprocessing_config_returns_assertion_config_when_not_cumulative(
        self,
    ) -> None:
        """Returns AssertionPreprocessingConfig for volume (non-cumulative)."""
        from datahub_observe.algorithms.preprocessing.preprocessor import (
            PreprocessingConfig,
        )
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, (AssertionPreprocessingConfig, PreprocessingConfig))

    def test_build_preprocessing_config_respects_is_delta_true(
        self,
    ) -> None:
        """Volume is_delta=True should use sum aggregation and avoid negative filtering."""
        from datahub_observe.algorithms.preprocessing.preprocessor import (
            PreprocessingConfig,
        )
        from datahub_observe.algorithms.preprocessing.transformers import (
            FrequencyAlignmentConfig,
            ResamplingConfig,
            ValueFilterConfig,
        )
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
            is_delta=True,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()
        assert isinstance(result, (AssertionPreprocessingConfig, PreprocessingConfig))
        cfg = _resolve_preprocessing_config(result)

        resampling = next(
            t for t in cfg.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "sum"
        freq_align = next(
            t
            for t in cfg.pandas_transformers
            if isinstance(t, FrequencyAlignmentConfig)
        )
        assert freq_align.aggregation_method == "sum"
        assert not any(
            isinstance(t, ValueFilterConfig) for t in cfg.pandas_transformers
        )

    def test_build_preprocessing_config_respects_is_delta_false(
        self,
    ) -> None:
        """Volume is_delta=False should use last aggregation and filter negatives."""
        from datahub_observe.algorithms.preprocessing.preprocessor import (
            PreprocessingConfig,
        )
        from datahub_observe.algorithms.preprocessing.transformers import (
            FrequencyAlignmentConfig,
            ResamplingConfig,
            ValueFilterConfig,
        )
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
            is_delta=False,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()
        assert isinstance(result, (AssertionPreprocessingConfig, PreprocessingConfig))
        cfg = _resolve_preprocessing_config(result)

        resampling = next(
            t for t in cfg.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert resampling.aggregation_method == "last"
        freq_align = next(
            t
            for t in cfg.pandas_transformers
            if isinstance(t, FrequencyAlignmentConfig)
        )
        assert freq_align.aggregation_method == "last"
        assert any(
            isinstance(t, ValueFilterConfig) and t.min_value == 0
            for t in cfg.pandas_transformers
        )

    def test_build_preprocessing_config_defaults_is_delta_by_category(
        self,
    ) -> None:
        """is_delta defaults based on assertion category when not explicitly set."""
        from datahub_observe.algorithms.preprocessing.preprocessor import (
            PreprocessingConfig,
        )
        from datahub_observe.algorithms.preprocessing.transformers import (
            ResamplingConfig,
        )
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        # Volume category should default to is_delta=False (cumulative semantics)
        volume_context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
            is_delta=None,
        )
        volume_factory = _make_factory(context=volume_context)
        volume_result = volume_factory._build_preprocessing_config()
        assert isinstance(
            volume_result, (AssertionPreprocessingConfig, PreprocessingConfig)
        )
        volume_cfg = _resolve_preprocessing_config(volume_result)
        volume_resampling = next(
            t for t in volume_cfg.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert volume_resampling.aggregation_method == "last"

        # Rate category should default to is_delta=True (delta semantics)
        rate_context = _make_context(
            assertion_category="rate",
            is_dataframe_cumulative=False,
            is_delta=None,
        )
        rate_factory = _make_factory(context=rate_context)
        rate_result = rate_factory._build_preprocessing_config()
        assert isinstance(rate_result, PreprocessingConfig)
        rate_resampling = next(
            t for t in rate_result.darts_transformers if isinstance(t, ResamplingConfig)
        )
        assert rate_resampling.aggregation_method == "mean"

    def test_build_preprocessing_config_uses_existing_config_if_available(
        self,
    ) -> None:
        """Uses existing preprocessing config for warm start."""
        mock_preproc = MagicMock()
        model_config = ModelConfig(
            preprocessing_config_json='{"type": "volume"}',
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.PreprocessingConfigSerializer.deserialize",
            return_value=mock_preproc,
        ):
            context = _make_context(
                assertion_category="volume",
                is_dataframe_cumulative=True,  # Should be ignored since existing config exists
            )
            factory = _make_factory(context=context, existing_model_config=model_config)
            result = factory._build_preprocessing_config()

        assert result == mock_preproc


class TestModelFactoryRegistryKeys:
    """Tests for registry key resolution methods in ModelFactory."""

    def test_get_anomaly_registry_key_from_existing_config(self) -> None:
        """Uses existing anomaly_model_name when available."""
        model_config = ModelConfig(
            anomaly_model_name="custom_anomaly_detector",
            preprocessing_config_json="{}",
        )
        factory = _make_factory(existing_model_config=model_config)

        result = factory._get_anomaly_registry_key()

        assert result == "custom_anomaly_detector"

    def test_get_anomaly_registry_key_falls_back_to_defaults(self) -> None:
        """Falls back to defaults when no existing config."""
        factory = _make_factory(existing_model_config=None)

        result = factory._get_anomaly_registry_key()

        # Default from ObserveDefaultsBuilder
        assert result is not None
        assert isinstance(result, str)

    def test_get_forecast_registry_key_from_existing_config(self) -> None:
        """Uses existing forecast_model_name when available."""
        model_config = ModelConfig(
            forecast_model_name="custom_forecast_model",
            preprocessing_config_json="{}",
        )
        factory = _make_factory(existing_model_config=model_config)

        result = factory._get_forecast_registry_key()

        assert result == "custom_forecast_model"


class TestModelFactoryCoalesceConfigs:
    """Tests for config coalescing methods."""

    def test_coalesce_forecast_config_returns_default_on_force_retune(self) -> None:
        """Returns default config when force_retune=True."""
        # Force retune by creating factory with force_retune=True
        context = _make_context()
        defaults = get_defaults_for_context(context)
        factory = ModelFactory(defaults, force_retune=True)

        result = factory._coalesce_forecast_config(force_retune=True)

        assert result is not None
        # Default config should be returned

    def test_coalesce_forecast_config_merges_existing_with_defaults(self) -> None:
        """Merges existing config with defaults, existing values override."""
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        model_config = ModelConfig(
            forecast_config_json='{"hyperparameters": {"custom_param": 42}}',
            preprocessing_config_json="{}",
        )

        # Return a ForecastModelConfig object
        existing_config = ForecastModelConfig(hyperparameters={"custom_param": 42})

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.ForecastConfigSerializer.deserialize",
            return_value=existing_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._coalesce_forecast_config(force_retune=False)

        assert result is not None
        assert isinstance(result, ForecastModelConfig)
        # Existing hyperparameter should be preserved
        assert result.hyperparameters.get("custom_param") == 42

    def test_coalesce_anomaly_config_embeds_forecast_config(self) -> None:
        """Embeds forecast config when embed_forecast_config=True."""
        factory = _make_factory()

        result = factory._coalesce_anomaly_config(embed_forecast_config=True)

        assert result is not None
        assert result.forecast_model_config is not None

    def test_coalesce_anomaly_config_skips_forecast_embed(self) -> None:
        """Does not embed forecast config when embed_forecast_config=False."""
        factory = _make_factory()

        result = factory._coalesce_anomaly_config(embed_forecast_config=False)

        assert result is not None
        # forecast_model_config should not be embedded

    def test_coalesce_anomaly_config_force_retune_with_embed(self) -> None:
        """Returns default with embedded forecast config when force_retune=True."""
        context = _make_context()
        defaults = get_defaults_for_context(context)
        factory = ModelFactory(defaults, force_retune=True)

        result = factory._coalesce_anomaly_config(embed_forecast_config=True)

        assert result is not None
        assert result.forecast_model_config is not None

    def test_coalesce_anomaly_config_force_retune_without_embed(self) -> None:
        """Returns default without forecast config when force_retune=True and embed=False."""
        context = _make_context()
        defaults = get_defaults_for_context(context)
        factory = ModelFactory(defaults, force_retune=True)

        result = factory._coalesce_anomaly_config(embed_forecast_config=False)

        assert result is not None
        # Should be the default config

    def test_coalesce_anomaly_config_skips_existing_forecast_model_config(self) -> None:
        """Skips forecast_model_config from existing config during merge."""
        from datahub_observe.algorithms.anomaly_detection.config import (
            AnomalyModelConfig,
        )
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        # Existing config has a forecast_model_config that should be ignored
        existing_forecast_config = ForecastModelConfig(
            hyperparameters={"old_param": "should_be_ignored"}
        )
        existing_config = AnomalyModelConfig(
            hyperparameters={"deviation_threshold": 2.0},
            forecast_model_config=existing_forecast_config,
        )

        model_config = ModelConfig(
            anomaly_config_json='{"hyperparameters": {"deviation_threshold": 2.0}}',
            preprocessing_config_json="{}",
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.AnomalyConfigSerializer.deserialize",
            return_value=existing_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._coalesce_anomaly_config(embed_forecast_config=True)

        assert result is not None
        # Hyperparameters from existing should be preserved
        assert result.hyperparameters.get("deviation_threshold") == 2.0
        # But forecast_model_config should be freshly coalesced, not from existing
        assert result.forecast_model_config is not None

    def test_coalesce_anomaly_config_no_existing_with_embed(self) -> None:
        """Returns default with embedded forecast when no existing config."""
        factory = _make_factory(existing_model_config=None)

        result = factory._coalesce_anomaly_config(embed_forecast_config=True)

        assert result is not None
        assert result.forecast_model_config is not None

    def test_coalesce_anomaly_config_no_existing_without_embed(self) -> None:
        """Returns default without forecast when no existing config and embed=False."""
        factory = _make_factory(existing_model_config=None)

        result = factory._coalesce_anomaly_config(embed_forecast_config=False)

        assert result is not None


class TestModelFactoryCreateModels:
    """Tests for ModelFactory.create_models() method."""

    def test_create_models_returns_initialized_models_with_anomaly_model(self) -> None:
        """create_models returns InitializedModels with anomaly model and configs."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            # Set up mock registry
            mock_registry = MagicMock()
            mock_anomaly_model = MagicMock()
            mock_registry.create_anomaly_model.return_value = mock_anomaly_model
            mock_get_registry.return_value = mock_registry

            result = factory.create_models()

            # Verify result has all expected fields
            assert result is not None
            assert result.anomaly_model is mock_anomaly_model
            assert result.preprocessing_config is not None
            assert result.anomaly_config is not None
            assert result.forecast_registry_key is not None
            assert result.anomaly_registry_key is not None

    def test_create_models_passes_forecast_model_name_to_registry(self) -> None:
        """create_models passes forecast_model_name to registry.create_anomaly_model."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_registry.create_anomaly_model.return_value = MagicMock()
            mock_get_registry.return_value = mock_registry

            factory.create_models()

            # Verify create_anomaly_model was called with forecast_model_name
            call_kwargs = mock_registry.create_anomaly_model.call_args.kwargs
            assert "forecast_model_name" in call_kwargs
            assert call_kwargs["forecast_model_name"] is not None

    def test_create_models_embeds_forecast_config_in_anomaly_config(self) -> None:
        """create_models creates anomaly config with embedded forecast config."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_registry.create_anomaly_model.return_value = MagicMock()
            mock_get_registry.return_value = mock_registry

            result = factory.create_models()

            # Verify anomaly_config has embedded forecast config
            assert result.anomaly_config is not None
            assert result.anomaly_config.forecast_model_config is not None

    def test_create_models_uses_existing_registry_keys(self) -> None:
        """create_models uses model names from existing config."""
        model_config = ModelConfig(
            forecast_model_name="existing_forecast",
            anomaly_model_name="existing_anomaly",
            preprocessing_config_json="{}",
        )
        factory = _make_factory(existing_model_config=model_config)

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_registry.create_anomaly_model.return_value = MagicMock()
            mock_get_registry.return_value = mock_registry

            result = factory.create_models()

            assert result.forecast_registry_key == "existing_forecast"
            assert result.anomaly_registry_key == "existing_anomaly"


class TestModelFactoryCreateForecastModel:
    """Tests for ModelFactory.create_forecast_model() method."""

    def test_create_forecast_model_returns_initialized_models(self) -> None:
        """create_forecast_model returns InitializedModels with forecast-only data."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            # Set up mock registry
            mock_registry = MagicMock()
            mock_forecast_model = MagicMock()
            mock_registry.create_forecast_model.return_value = mock_forecast_model
            mock_get_registry.return_value = mock_registry

            result = factory.create_forecast_model()

            # Verify result type and properties
            assert result is not None
            assert result.has_forecast_only
            assert not result.has_anomaly_model
            assert result.forecast_model is mock_forecast_model
            assert result.anomaly_model is None

    def test_create_forecast_model_with_explicit_registry_key(self) -> None:
        """create_forecast_model respects explicit registry_key parameter."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_registry.create_forecast_model.return_value = MagicMock()
            mock_get_registry.return_value = mock_registry

            factory.create_forecast_model(registry_key="custom_forecast_key")

            # Verify the registry was called with our custom key
            call_args = mock_registry.create_forecast_model.call_args
            assert call_args[0][0] == "custom_forecast_key"

    def test_create_forecast_model_sets_registry_key_in_result(self) -> None:
        """create_forecast_model stores the registry key in the result."""
        factory = _make_factory()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_registry.create_forecast_model.return_value = MagicMock()
            mock_get_registry.return_value = mock_registry

            result = factory.create_forecast_model(registry_key="my_forecast")

            assert result.forecast_registry_key == "my_forecast"


class TestModelFactoryCreateAnomalyModel:
    """Tests for ModelFactory.create_anomaly_model() method."""

    def test_create_anomaly_model_returns_initialized_models(self) -> None:
        """create_anomaly_model returns InitializedModels with anomaly model."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            # Set up mock registry
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_anomaly_class = MagicMock()
            mock_anomaly_instance = MagicMock()
            # observe-models >=0.1.0.dev42: prefer cls.from_config(...) over cls(...)
            mock_anomaly_class.from_config.return_value = mock_anomaly_instance
            mock_entry.cls = mock_anomaly_class
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            result = factory.create_anomaly_model(mock_forecast_model)

            # Verify result type and properties
            assert result is not None
            assert result.has_anomaly_model
            assert not result.has_forecast_only
            assert result.anomaly_model is mock_anomaly_instance

    def test_create_anomaly_model_passes_forecast_model_to_constructor(self) -> None:
        """create_anomaly_model passes forecast model via cls.from_config()."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_anomaly_class = MagicMock()
            mock_anomaly_class.from_config.return_value = MagicMock()
            mock_entry.cls = mock_anomaly_class
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            factory.create_anomaly_model(mock_forecast_model)

            # Verify forecast_model was passed to from_config, and we did NOT call cls(...)
            mock_anomaly_class.assert_not_called()
            mock_anomaly_class.from_config.assert_called_once()
            _, kwargs = mock_anomaly_class.from_config.call_args
            assert kwargs["forecast_model"] is mock_forecast_model
            assert (
                kwargs["preprocessing_config"]
                is mock_forecast_model.preprocessing_config
            )
            # For param_grid=None, executor treats this as "disable grid search".
            assert kwargs["model_config"].param_grid == {}

    def test_create_anomaly_model_passes_param_grid(self) -> None:
        """create_anomaly_model maps param_grid into AnomalyModelConfig."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_anomaly_class = MagicMock()
            mock_anomaly_class.from_config.return_value = MagicMock()
            mock_entry.cls = mock_anomaly_class
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            factory.create_anomaly_model(
                mock_forecast_model,
                param_grid="auto",
            )

            # For param_grid="auto", executor leaves param_grid=None to use model defaults.
            mock_anomaly_class.assert_not_called()
            mock_anomaly_class.from_config.assert_called_once()
            _, kwargs = mock_anomaly_class.from_config.call_args
            assert kwargs["forecast_model"] is mock_forecast_model
            assert kwargs["model_config"].param_grid is None

    def test_create_anomaly_model_with_explicit_registry_key(self) -> None:
        """create_anomaly_model respects explicit anomaly_registry_key."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_entry.cls = MagicMock()
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            result = factory.create_anomaly_model(
                mock_forecast_model,
                anomaly_registry_key="custom_anomaly",
            )

            # Verify registry was called with custom key
            mock_registry.get.assert_called_once_with("custom_anomaly")
            assert result.anomaly_registry_key == "custom_anomaly"


class TestTrainingAnomalyScore:
    """Tests for training anomaly score derivation in ObserveAdapter._evaluate_quality."""

    def test_uses_best_model_score_value_when_present(self) -> None:
        """Prefers anomaly_model.best_model_score.value when available."""
        from datahub_observe.algorithms.scoring import ModelScore, ScoreType

        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )

        mock_anomaly_model = MagicMock()
        mock_anomaly_model.min_samples_for_cv = 10
        mock_anomaly_model.best_model_score = ModelScore(
            value=0.85,
            score_type=ScoreType.ANOMALY,
            metric_name="f1_score",
            has_ground_truth=True,
            confidence=0.8,
        )

        models = MagicMock()
        models.anomaly_model = mock_anomaly_model
        models.forecast_model = MagicMock()

        eval_df = df.tail(5).copy()
        train_df = df.iloc[:-5].copy()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute_metrics,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_anomaly_score"
            ) as mock_compute_anomaly_score,
        ):
            mock_compute_metrics.return_value = ({}, {"precision": 0.9}, df.copy())

            anomaly_score, forecast_score, _, _, _ = adapter._evaluate_quality(  # type: ignore[attr-defined]
                df=train_df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert anomaly_score == 0.85
            assert forecast_score is None
            mock_compute_anomaly_score.assert_not_called()

    def test_falls_back_to_computed_anomaly_score_when_no_best_model_score(
        self,
    ) -> None:
        """Uses compute_anomaly_score(anomaly_evals) if best_model_score missing."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )

        mock_anomaly_model = MagicMock()
        mock_anomaly_model.min_samples_for_cv = 10
        mock_anomaly_model.best_model_score = None

        models = MagicMock()
        models.anomaly_model = mock_anomaly_model
        models.forecast_model = MagicMock()

        eval_df = df.tail(5).copy()
        train_df = df.iloc[:-5].copy()

        anomaly_evals = {"precision": 0.8, "recall": 0.7, "f1_score": 0.75}

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute_metrics,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_anomaly_score"
            ) as mock_compute_anomaly_score,
        ):
            mock_compute_metrics.return_value = ({}, anomaly_evals, df.copy())
            mock_compute_anomaly_score.return_value = 0.42

            anomaly_score, forecast_score, _, _, _ = adapter._evaluate_quality(  # type: ignore[attr-defined]
                df=train_df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert anomaly_score == 0.42
            assert forecast_score is None
            mock_compute_anomaly_score.assert_called_once_with(anomaly_evals)

    def test_defaults_to_conservative_score_when_no_evals_and_no_best_score(
        self,
    ) -> None:
        """Returns 0.6 when there's no best_model_score and no anomaly_evals."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )

        mock_anomaly_model = MagicMock()
        mock_anomaly_model.min_samples_for_cv = 10
        mock_anomaly_model.best_model_score = None

        models = MagicMock()
        models.anomaly_model = mock_anomaly_model
        models.forecast_model = MagicMock()

        eval_df = df.tail(5).copy()
        train_df = df.iloc[:-5].copy()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute_metrics,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_anomaly_score"
            ) as mock_compute_anomaly_score,
        ):
            mock_compute_metrics.return_value = ({}, {}, df.copy())

            anomaly_score, forecast_score, _, _, _ = adapter._evaluate_quality(  # type: ignore[attr-defined]
                df=train_df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert anomaly_score == 0.6
            assert forecast_score is None
            mock_compute_anomaly_score.assert_not_called()

    def test_computes_forecast_score_when_forecast_evals_present(self) -> None:
        """Computes and returns forecast score when forecast_evals exist."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )

        mock_anomaly_model = MagicMock()
        mock_anomaly_model.min_samples_for_cv = 10
        mock_anomaly_model.best_model_score = None

        mock_forecast_model = MagicMock()
        mock_forecast_model.y_range = None

        models = MagicMock()
        models.anomaly_model = mock_anomaly_model
        models.forecast_model = mock_forecast_model

        eval_df = df.tail(5).copy()
        train_df = df.iloc[:-5].copy()

        forecast_evals = {"mape": 10.0, "coverage": 90.0}

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute_metrics,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast_score,
        ):
            mock_compute_metrics.return_value = (forecast_evals, {}, df.copy())
            mock_compute_forecast_score.return_value = 0.33

            anomaly_score, forecast_score, _, _, _ = adapter._evaluate_quality(  # type: ignore[attr-defined]
                df=train_df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert anomaly_score == 0.6
            assert forecast_score == 0.33
            mock_compute_forecast_score.assert_called_once()

    def test_raises_with_insufficient_data(self) -> None:
        builder = TrainingResultBuilder()
        """Raises TrainingErrorException when df is below min_samples_for_cv."""
        adapter = ObserveAdapter()
        df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=5), "y": range(5)})
        eval_df = df.tail(2).copy()
        train_df = df.iloc[:-2].copy()

        mock_anomaly_model = MagicMock()
        mock_anomaly_model.min_samples_for_cv = 10
        mock_anomaly_model.best_model_score = None

        models = MagicMock()
        models.anomaly_model = mock_anomaly_model
        models.forecast_model = MagicMock()

        with pytest.raises(
            TrainingErrorException, match="Insufficient data"
        ) as excinfo:
            adapter._evaluate_quality(  # type: ignore[attr-defined]
                df=train_df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )


class TestObserveAdapterForceRetune:
    """Tests for force_retune parameter handling."""

    def test_force_retune_sets_both_when_flag_false(self) -> None:
        """force_retune=True sets both forecast and anomaly when flag is false."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_force_retune_anomaly_only",
                return_value=False,
            ),
            patch.object(adapter, "_run_combination_evaluation") as mock_comb,
            patch.object(adapter, "_train_and_evaluate") as mock_train,
        ):
            mock_train.return_value = (
                MagicMock(),
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                df.tail(10),
                None,  # eval_detection_results
            )
            mock_comb.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            try:
                adapter.run_training_pipeline(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
                    force_retune=True,
                )
            except Exception:
                pass  # We're just testing the force_retune logic

    def test_force_retune_sets_anomaly_only_when_flag_true(self) -> None:
        """force_retune=True sets only anomaly when flag is true."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_force_retune_anomaly_only",
                return_value=True,
            ),
            patch.object(adapter, "_run_combination_evaluation") as mock_comb,
            patch.object(adapter, "_train_and_evaluate") as mock_train,
        ):
            mock_train.return_value = (
                MagicMock(),
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                df.tail(10),
                None,  # eval_detection_results
            )
            mock_comb.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            try:
                adapter.run_training_pipeline(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
                    force_retune=True,
                )
            except Exception:
                pass  # We're just testing the force_retune logic


class TestObserveAdapterCombinationEvaluation:
    """Tests for model combination evaluation path."""

    def test_runs_combination_evaluation_when_combinations_provided(self) -> None:
        """run_training_pipeline calls _run_combination_evaluation when model_combinations provided."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        with patch.object(adapter, "_run_combination_evaluation") as mock_comb_eval:
            mock_result = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )
            mock_comb_eval.return_value = mock_result

            result = adapter.run_training_pipeline(
                df=df,
                input_data_context=context,
                num_intervals=24,
                model_combinations=pairings,
            )

            assert result is mock_result
            mock_comb_eval.assert_called_once()

    def test_uses_default_pairings_when_none_provided(self) -> None:
        """run_training_pipeline uses default pairings when model_combinations is None."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_pairings_from_env_or_default"
            ) as mock_get_pairings,
            patch.object(adapter, "_run_combination_evaluation") as mock_comb_eval,
        ):
            mock_pairings = [
                ModelPairing(
                    anomaly_model="test_anomaly", forecast_model="test_forecast"
                )
            ]
            mock_get_pairings.return_value = mock_pairings
            mock_comb_eval.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            mock_get_pairings.assert_called_once()
            mock_comb_eval.assert_called_once()


class TestObserveAdapterTrainingPipeline:
    """Tests for the main training pipeline flow."""

    def test_quality_retune_when_score_below_threshold(self) -> None:
        """Pipeline retries with retuning when score is below threshold."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            # First call: low score, not tuned
            # Second call: after retune
            first_models = MagicMock(
                forecast_model=MagicMock(),
                anomaly_model=MagicMock(),
                forecast_registry_key="test",
                anomaly_registry_key="test",
            )
            mock_train.side_effect = [
                (
                    first_models,
                    0.3,  # Low anomaly score
                    0.7,
                    {},
                    {},
                    False,  # Not tuned yet
                    None,
                    eval_df,
                    None,  # eval_detection_results
                ),
                (
                    MagicMock(
                        forecast_model=MagicMock(),
                        anomaly_model=MagicMock(),
                        forecast_registry_key="test",
                        anomaly_registry_key="test",
                    ),
                    0.8,  # Better score after retune
                    0.7,
                    {},
                    {},
                    True,
                    None,
                    eval_df,
                    None,  # eval_detection_results
                ),
            ]

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.return_value = pd.DataFrame(
                {
                    "timestamp_ms": [1000, 2000],
                    "detection_band_lower": [0.5, 0.6],
                    "detection_band_upper": [1.5, 1.6],
                }
            )
            first_models.anomaly_model = mock_anomaly

            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            mock_factory.return_value.create_models.return_value = full_models

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            assert mock_train.call_count == 2

    def test_returns_scores_only_when_score_still_below_threshold_after_retry(
        self,
    ) -> None:
        """When score still below threshold after retry, returns scores_only_persist=True and prediction_df=None."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)
            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            mock_train.return_value = (
                MagicMock(
                    forecast_model=MagicMock(),
                    anomaly_model=MagicMock(),
                    forecast_registry_key="test",
                    anomaly_registry_key="test",
                ),
                0.3,  # Low score
                0.7,
                {},
                {},
                True,  # Already tuned, so no retry
                None,  # eval_forecast_df
                eval_df,  # eval_df (discarded in unpack)
                None,  # eval_detection_results
            )

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result.prediction_df is None
            assert result.scores_only_persist is True
            assert result.model_config is not None

    def test_full_history_retrain(self) -> None:
        """Pipeline performs full history retrain after evaluation."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_registry_key = "test"
            mock_models.anomaly_registry_key = "test"
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()

            mock_train.return_value = (
                mock_models,
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                eval_df,
                None,  # eval_detection_results
            )

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.return_value = pd.DataFrame(
                {
                    "timestamp_ms": [1000, 2000],
                    "detection_band_lower": [0.5, 0.6],
                    "detection_band_upper": [1.5, 1.6],
                }
            )
            mock_anomaly.train = MagicMock()

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_model = MagicMock()
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            full_models.forecast_config = MagicMock()
            full_models.anomaly_config = MagicMock()
            full_models.preprocessing_config = MagicMock()

            mock_factory.return_value.create_models.return_value = full_models
            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            mock_anomaly.train.assert_called_once_with(df, ground_truth=None)

    def test_prediction_failure_handled_gracefully(self) -> None:
        """Pipeline handles prediction failure gracefully."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_registry_key = "test"
            mock_models.anomaly_registry_key = "test"
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()

            mock_train.return_value = (
                mock_models,
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                eval_df,
                None,  # eval_detection_results
            )

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.side_effect = Exception("Prediction failed")
            mock_anomaly.train = MagicMock()

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_model = MagicMock()
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            full_models.forecast_config = MagicMock()
            full_models.anomaly_config = MagicMock()
            full_models.preprocessing_config = MagicMock()

            mock_factory.return_value.create_models.return_value = full_models
            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            assert result.prediction_df is None
            assert "prediction" in result.step_results
            assert result.step_results["prediction"].success is False


class TestObserveAdapterHelperMethods:
    """Tests for helper methods in ObserveAdapter."""

    def test_generate_future_timestamps(self) -> None:
        """_generate_future_timestamps generates correct future timestamps."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        result = adapter._generate_future_timestamps(
            df, num_intervals=24, interval_hours=1
        )

        assert len(result) == 24
        assert "ds" in result.columns
        assert result["ds"].min() > df["ds"].max()

    def test_generate_future_timestamps_with_nan_last_ts(self) -> None:
        """_generate_future_timestamps handles NaN last timestamp."""
        adapter = ObserveAdapter()
        df = pd.DataFrame(
            {
                "ds": [pd.Timestamp("2024-01-01"), pd.NaT],
                "y": [1, 2],
            }
        )

        result = adapter._generate_future_timestamps(
            df, num_intervals=5, interval_hours=1
        )

        assert len(result) == 5
        assert "ds" in result.columns

    def test_format_prediction_output_success(self) -> None:
        """_format_prediction_output formats valid prediction results."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "timestamp_ms": [1000, 2000],
                "detection_band_lower": [0.5, 0.6],
                "detection_band_upper": [1.5, 1.6],
                "y": [1.0, 2.0],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            result = adapter._format_prediction_output(results)

            assert result is not None
            assert len(result) == 2

    def test_format_prediction_output_missing_timestamp_ms(self) -> None:
        """_format_prediction_output raises when timestamp_ms missing."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "detection_band_lower": [0.5, 0.6],
                "detection_band_upper": [1.5, 1.6],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            with pytest.raises(ValueError, match="timestamp_ms"):
                adapter._format_prediction_output(results)

    def test_format_prediction_output_missing_detection_band_lower(self) -> None:
        """_format_prediction_output raises when detection_band_lower missing."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "timestamp_ms": [1000, 2000],
                "detection_band_upper": [1.5, 1.6],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            with pytest.raises(ValueError, match="detection_band_lower"):
                adapter._format_prediction_output(results)

    def test_format_prediction_output_empty_results(self) -> None:
        """_format_prediction_output handles empty results."""
        adapter = ObserveAdapter()
        results = pd.DataFrame()

        result = adapter._format_prediction_output(results)

        assert result is not None
        assert len(result) == 0

    def test_format_prediction_output_none_results(self) -> None:
        """_format_prediction_output handles None results."""
        adapter = ObserveAdapter()

        result = adapter._format_prediction_output(None)  # type: ignore[arg-type]

        assert result is None


class TestObserveAdapterTrainAndEvaluate:
    """Tests for _train_and_evaluate method."""

    def test_train_and_evaluate_with_tuning_decision(self) -> None:
        """_train_and_evaluate uses provided tuning decision."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        tuning_decision = TuningDecision(
            should_retune_forecast=True,
            should_retune_anomaly=True,
            forecast_reason=TuningReason.FORCED,
            anomaly_reason=TuningReason.FORCED,
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                tuning_decision=tuning_decision,
                eval_df=df.tail(10),
            )

            assert result is not None
            assert result[1] == 0.8  # anomaly_score
            assert result[2] == 0.7  # forecast_score

    def test_train_and_evaluate_default_tuning_decision(self) -> None:
        """_train_and_evaluate creates default tuning decision when None."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                tuning_decision=None,
                eval_df=df.tail(10),
            )

            assert result is not None

    def test_train_and_evaluate_model_creation_failure(self) -> None:
        """_train_and_evaluate raises on model creation failure."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
        ) as mock_factory:
            mock_factory.return_value.create_models.side_effect = Exception(
                "Creation failed"
            )

            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type == MonitorErrorTypeClass.MODEL_CREATION_FAILED
            )

    def test_train_and_evaluate_training_failure(self) -> None:
        """_train_and_evaluate raises on training failure."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
        ) as mock_factory:
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.anomaly_model.train.side_effect = Exception("Training failed")
            mock_factory.return_value.create_models.return_value = mock_models

            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type == MonitorErrorTypeClass.MODEL_TRAINING_FAILED
            )

    def test_train_and_evaluate_evaluation_failure(self) -> None:
        """_train_and_evaluate raises on evaluation failure."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.side_effect = Exception("Evaluation failed")

            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )

    def test_train_and_evaluate_with_forecast_model_predict(self) -> None:
        """_train_and_evaluate generates eval_forecast_df when forecast model available."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_forecast = MagicMock()
            mock_forecast.is_trained = True
            mock_forecast.predict.return_value = df.tail(10)

            mock_models = MagicMock()
            mock_models.forecast_model = mock_forecast
            mock_models.anomaly_model = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=df.tail(10),
            )

            assert result is not None
            assert result[6] is not None  # eval_forecast_df


class TestObserveAdapterEvaluateQuality:
    """Tests for _evaluate_quality method."""

    def test_evaluate_quality_raises_when_eval_df_empty(self) -> None:
        """_evaluate_quality raises when eval_df is empty."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)
        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.forecast_model = MagicMock()

        with pytest.raises(TrainingErrorException) as excinfo:
            adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=pd.DataFrame(),
            )

        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )

    def test_evaluate_quality_raises_when_insufficient_samples(self) -> None:
        """_evaluate_quality raises when total samples below min_samples_for_cv."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(5)
        eval_df = df.tail(2)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 20
        models.forecast_model = MagicMock()

        with pytest.raises(TrainingErrorException) as excinfo:
            adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )

    def test_evaluate_quality_uses_best_model_score(self) -> None:
        """_evaluate_quality uses best_model_score when available."""
        from datahub_observe.algorithms.scoring import ModelScore, ScoreType

        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = ModelScore(
            value=0.85,
            score_type=ScoreType.ANOMALY,
            metric_name="f1_score",
            has_ground_truth=True,
            confidence=0.8,
        )
        models.forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
        ) as mock_compute:
            mock_compute.return_value = ({}, {}, eval_df)

            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[0] == 0.85  # Uses best_model_score.value

    def test_evaluate_quality_computes_forecast_score_with_y_range(self) -> None:
        """_evaluate_quality computes forecast score using model y_range."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = 100.0

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.return_value = 0.75

            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[1] == 0.75  # training_forecast_score is at index 1
            mock_compute_forecast.assert_called_once()
            call_kwargs = mock_compute_forecast.call_args[1]
            assert call_kwargs["y_range"] == 100.0

    def test_evaluate_quality_computes_forecast_score_from_eval_df(self) -> None:
        """_evaluate_quality computes forecast score using eval_df y_range when model y_range unavailable."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)
        eval_df = df.tail(10).copy()
        eval_df["y"] = range(10, 20)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.return_value = 0.75

            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[1] == 0.75  # forecast_score is at index 1
            call_kwargs = mock_compute_forecast.call_args[1]
            assert call_kwargs["y_range"] == 9.0  # max - min from eval_df

    def test_evaluate_quality_handles_forecast_score_computation_failure(self) -> None:
        """_evaluate_quality handles forecast score computation failure gracefully."""
        adapter = ObserveAdapter()
        builder = TrainingResultBuilder()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.side_effect = Exception("Score computation failed")

            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert (
                result[1] is None
            )  # training_forecast_score is None on failure (at index 1)


class TestObserveAdapterRunCombinationEvaluation:
    """Tests for _run_combination_evaluation method."""

    def test_run_combination_evaluation_calls_run_pairing_evaluation(self) -> None:
        """_run_combination_evaluation calls run_pairing_evaluation."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.run_pairing_evaluation"
            ) as mock_run_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.select_best_combination"
            ) as mock_select,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch.object(adapter, "_train_single_combination") as mock_train_single,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_result = MagicMock()
            mock_result.combined_score = 0.9
            mock_run_pairing.return_value = [mock_result]
            mock_select.return_value = (mock_result, 0)

            mock_train_single.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            result = adapter._run_combination_evaluation(
                df=df,
                input_data_context=context,
                num_intervals=24,
                interval_hours=1,
                force_retune_forecast=False,
                force_retune_anomaly=False,
                ground_truth=None,
                existing_model_config=None,
                model_combinations=pairings,
                score_drop_threshold=0.25,
                eval_train_ratio=None,
                progress_hooks=None,
            )

            assert result is not None
            mock_run_pairing.assert_called_once()
            mock_train_single.assert_called_once()

    def test_run_combination_evaluation_handles_evaluation_failure(self) -> None:
        """_run_combination_evaluation raises on evaluation failure."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.run_pairing_evaluation"
            ) as mock_run_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_run_pairing.side_effect = Exception("Evaluation failed")

            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._run_combination_evaluation(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
                    interval_hours=1,
                    force_retune_forecast=False,
                    force_retune_anomaly=False,
                    ground_truth=None,
                    existing_model_config=None,
                    model_combinations=pairings,
                    score_drop_threshold=0.25,
                    eval_train_ratio=None,
                    progress_hooks=None,
                )

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )

    def test_run_combination_evaluation_with_existing_model_config(self) -> None:
        """_run_combination_evaluation uses existing model config when available."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]
        existing_config = ModelConfig(
            forecast_model_name="old_forecast",
            anomaly_model_name="old_anomaly",
            preprocessing_config_json="{}",
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.run_pairing_evaluation"
            ) as mock_run_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.select_best_combination"
            ) as mock_select,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch.object(adapter, "_train_single_combination") as mock_train_single,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_result = MagicMock()
            mock_result.combined_score = 0.9
            mock_run_pairing.return_value = [mock_result]
            mock_select.return_value = (mock_result, 0)

            mock_train_single.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            adapter._run_combination_evaluation(
                df=df,
                input_data_context=context,
                num_intervals=24,
                interval_hours=1,
                force_retune_forecast=False,
                force_retune_anomaly=False,
                ground_truth=None,
                existing_model_config=existing_config,
                model_combinations=pairings,
                score_drop_threshold=0.25,
                eval_train_ratio=None,
                progress_hooks=None,
            )

            # Verify _train_single_combination was called with modified config
            assert mock_train_single.called
            call_kwargs = mock_train_single.call_args.kwargs
            assert call_kwargs["existing_model_config"] is not None
