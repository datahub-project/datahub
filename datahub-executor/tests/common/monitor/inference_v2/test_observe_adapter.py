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
    extract_quality_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
    ModelFactory,
)


def _make_context(
    assertion_category: str = "volume",
    is_dataframe_cumulative: bool = False,
    allow_negative: bool | None = None,
) -> InputDataContext:
    """Helper to create InputDataContext for tests."""
    return InputDataContext(
        assertion_category=assertion_category,
        is_dataframe_cumulative=is_dataframe_cumulative,
        allow_negative=allow_negative,
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
            adapter.run_training_pipeline(df=df, context=context, num_intervals=24)

        assert excinfo.value.error_type == MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT

    def test_missing_columns_raises(self) -> None:
        adapter = ObserveAdapter()
        context = _make_context()
        df = pd.DataFrame({"ds": [pd.Timestamp("2024-01-01")]})

        with pytest.raises(TrainingErrorException) as excinfo:
            adapter.run_training_pipeline(df=df, context=context, num_intervals=24)

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


class TestModelFactoryPreprocessingConfig:
    """Tests for preprocessing config building in ModelFactory."""

    def test_build_preprocessing_config_returns_volume_config_when_cumulative(
        self,
    ) -> None:
        """Returns VolumePreprocessorConfig when is_dataframe_cumulative=True for volume assertions."""
        from datahub_observe.algorithms.preprocessing.volume_preprocessor import (
            VolumePreprocessorConfig,
        )

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=True,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, VolumePreprocessorConfig)
        assert result.convert_cumulative is True
        assert result.allow_negative is False

    def test_build_preprocessing_config_returns_assertion_config_when_not_cumulative(
        self,
    ) -> None:
        """Returns AssertionPreprocessingConfig when is_dataframe_cumulative=False."""
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, AssertionPreprocessingConfig)

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
            mock_anomaly_class.return_value = mock_anomaly_instance
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
        """create_anomaly_model passes the forecast model to the anomaly class."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_anomaly_class = MagicMock()
            mock_entry.cls = mock_anomaly_class
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            factory.create_anomaly_model(mock_forecast_model)

            # Verify forecast_model was passed to the constructor
            mock_anomaly_class.assert_called_once_with(
                forecast_model=mock_forecast_model,
                param_grid=None,
            )

    def test_create_anomaly_model_passes_param_grid(self) -> None:
        """create_anomaly_model passes param_grid to the anomaly model."""
        factory = _make_factory()
        mock_forecast_model = MagicMock()

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.get_model_registry"
        ) as mock_get_registry:
            mock_registry = MagicMock()
            mock_entry = MagicMock()
            mock_anomaly_class = MagicMock()
            mock_entry.cls = mock_anomaly_class
            mock_registry.get.return_value = mock_entry
            mock_get_registry.return_value = mock_registry

            factory.create_anomaly_model(
                mock_forecast_model,
                param_grid="auto",
            )

            # Verify param_grid was passed
            mock_anomaly_class.assert_called_once_with(
                forecast_model=mock_forecast_model,
                param_grid="auto",
            )

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


class TestExtractQualityScore:
    """Tests for extract_quality_score function.

    The extract_quality_score function uses the anomaly model's best_score
    from grid search, which represents true held-out validation performance.

    NOTE: Discrepancies with streamlit_explorer/model_explorer approach:
    - Streamlit computes metrics manually from detection results
    - This function uses the package's built-in grid search metrics
    See evaluator.py docstring for full details.
    """

    def test_extract_quality_score_with_f1_score(self) -> None:
        """Returns F1 score when best_score >= 0 (ground_truth was provided)."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = 0.85  # F1 score from grid search
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        assert result == 0.85

    def test_extract_quality_score_with_negative_score(self) -> None:
        """Normalizes negative score (without ground_truth) to 0.5-1.0 range."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = -10  # Negative anomaly count
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        # Normalized: 1.0 + (-10) / 40.0 = 0.75
        assert result == 0.75

    def test_extract_quality_score_with_very_negative_score(self) -> None:
        """Clamps very negative scores to 0.5 minimum."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = -100  # Very many anomalies detected
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        # Normalized: max(0.5, 1.0 + (-100) / 40.0) = max(0.5, -1.5) = 0.5
        assert result == 0.5

    def test_extract_quality_score_with_none_best_score(self) -> None:
        """Returns 1.0 when best_score is None (cached/default params)."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = None  # Using cached or default params
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        assert result == 1.0

    def test_extract_quality_score_with_none_model(self) -> None:
        """Returns 1.0 when no anomaly model available."""
        import pandas as pd

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, None)

        assert result == 1.0

    def test_extract_quality_score_fails_with_insufficient_data(self) -> None:
        """Raises TrainingErrorException when data is below min_samples_for_cv."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = 0.85
        mock_model.min_samples_for_cv = 10

        # Only 5 samples, below threshold of 10
        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=5), "y": range(5)}
        )

        with pytest.raises(
            TrainingErrorException, match="Insufficient data"
        ) as excinfo:
            extract_quality_score(train_df, mock_model)
        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )
