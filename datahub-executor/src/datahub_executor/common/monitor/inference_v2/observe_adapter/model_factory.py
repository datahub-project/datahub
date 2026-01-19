"""
Factory for creating and configuring observe models.

This module handles model initialization with proper config coalescing:
- Deserializes existing configs from JSON (warm start)
- Coalesces defaults with existing configs
- Creates forecast and anomaly models with proper configuration
"""

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.registry import get_model_registry

if TYPE_CHECKING:
    from datahub_observe.algorithms.anomaly_detection.anomaly_base import (
        BaseAnomalyModel,
    )
    from datahub_observe.algorithms.anomaly_detection.forecast_anomaly_base import (
        BaseForecastAnomalyModel,
    )
    from datahub_observe.algorithms.forecasting.forecast_base import (
        DartsBaseForecastModel,
    )

from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    ObserveDefaultsBuilder,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    AnomalyConfigSerializer,
    ForecastConfigSerializer,
    PreprocessingConfigSerializer,
)


@dataclass
class InitializedModels:
    """Result of model initialization."""

    forecast_model: "DartsBaseForecastModel"
    anomaly_model: Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]
    preprocessing_config: Any
    forecast_config: ForecastModelConfig
    anomaly_config: AnomalyModelConfig
    # Registry keys used for model instantiation (for serialization)
    forecast_registry_key: str
    anomaly_registry_key: str


class ModelFactory:
    """
    Factory for creating and configuring observe models.

    Handles:
    - Deserializing existing configs from JSON (warm start)
    - Coalescing defaults with existing configs
    - Creating forecast and anomaly models with proper configuration

    Usage:
        factory = ModelFactory(defaults, existing_model_config)
        models = factory.create_models()
        # Access models.forecast_model, models.anomaly_model, etc.
    """

    def __init__(
        self,
        defaults: ObserveDefaultsBuilder,
        existing_model_config: Optional[ModelConfig] = None,
        force_retune: bool = False,
    ):
        """
        Initialize the factory.

        Args:
            defaults: ObserveDefaultsBuilder providing context-aware default configs.
            existing_model_config: Previously trained model config for warm start.
            force_retune: If True, ignore existing hyperparameters and use defaults
                only. This forces fresh tuning of the model. Preprocessing config
                is still preserved since it describes the data shape.
        """
        self._defaults = defaults
        self._existing_model_config = existing_model_config
        self._force_retune = force_retune

    def create_models(self) -> InitializedModels:
        """
        Create forecast and anomaly models with coalesced configs.

        Returns:
            InitializedModels containing the created models and their configs.
        """
        registry = get_model_registry()

        # Determine model registry keys: use existing config names if available,
        # otherwise use defaults
        forecast_registry_key = self._get_forecast_registry_key()
        anomaly_registry_key = self._get_anomaly_registry_key()

        # Build preprocessing config (coalesced with existing if available)
        preprocessing_config = self._build_preprocessing_config()

        # Coalesce forecast config
        forecast_config = self._coalesce_forecast_config()

        # Create forecast model with hyperparameters from coalesced config
        forecast_entry = registry.get(forecast_registry_key)
        forecast_model = forecast_entry.cls(
            preprocessing_config=preprocessing_config,
            darts_model_config=forecast_config.hyperparameters,
        )

        # Coalesce anomaly config
        anomaly_config = self._coalesce_anomaly_config()

        # Create anomaly model with forecast model (composition)
        anomaly_entry = registry.get(anomaly_registry_key)
        anomaly_model = anomaly_entry.cls(
            forecast_model=forecast_model,
            scorer_config=anomaly_config.hyperparameters or None,
        )

        return InitializedModels(
            forecast_model=forecast_model,
            anomaly_model=anomaly_model,
            preprocessing_config=preprocessing_config,
            forecast_config=forecast_config,
            anomaly_config=anomaly_config,
            forecast_registry_key=forecast_registry_key,
            anomaly_registry_key=anomaly_registry_key,
        )

    def _build_preprocessing_config(self) -> Any:
        """Build preprocessing config by coalescing existing with defaults.

        Returns existing config if available, otherwise uses defaults from
        ObserveDefaultsBuilder based on the input data context.

        Returns:
            Preprocessing config (VolumePreprocessorConfig or AssertionPreprocessingConfig).
        """
        # Try existing config first (warm start)
        existing = self._get_existing_preprocessing_config()
        if existing is not None:
            return existing

        # Use defaults builder for context-aware defaults
        return self._defaults.preprocessing_config()

    def _get_existing_forecast_config(self) -> Optional[ForecastModelConfig]:
        """Get full forecast config from existing model config.

        Returns the complete ForecastModelConfig object, preserving all fields
        (not just hyperparameters) for future-proofing.

        Returns:
            ForecastModelConfig if available, None otherwise.
        """
        if not (
            self._existing_model_config
            and self._existing_model_config.forecast_config_json
        ):
            return None

        config = ForecastConfigSerializer.deserialize(
            self._existing_model_config.forecast_config_json
        )
        if isinstance(config, ForecastModelConfig):
            return config  # Full object preserved
        elif isinstance(config, dict):
            # Reconstruct from dict - pass all fields, not just hyperparameters
            config_dict = {k: v for k, v in config.items() if k != "_schemaVersion"}
            return ForecastModelConfig(**config_dict)
        return None

    def _get_existing_anomaly_config(self) -> Optional[AnomalyModelConfig]:
        """Get full anomaly config from existing model config.

        Returns the complete AnomalyModelConfig object, preserving all fields
        (not just hyperparameters) for future-proofing.

        Returns:
            AnomalyModelConfig if available, None otherwise.
        """
        if not (
            self._existing_model_config
            and self._existing_model_config.anomaly_config_json
        ):
            return None

        config = AnomalyConfigSerializer.deserialize(
            self._existing_model_config.anomaly_config_json
        )
        if isinstance(config, AnomalyModelConfig):
            return config  # Full object preserved
        elif isinstance(config, dict):
            # Reconstruct from dict - pass all fields, not just hyperparameters
            config_dict = {k: v for k, v in config.items() if k != "_schemaVersion"}
            return AnomalyModelConfig(**config_dict)
        return None

    def _get_existing_preprocessing_config(self) -> Optional[Any]:
        """Get existing preprocessing config if available."""
        if (
            self._existing_model_config
            and self._existing_model_config.preprocessing_config_json
        ):
            return PreprocessingConfigSerializer.deserialize(
                self._existing_model_config.preprocessing_config_json
            )
        return None

    def _get_forecast_registry_key(self) -> str:
        """Get the forecast model registry key.

        Uses the model name from existing config if available (warm start),
        otherwise uses the default from ObserveDefaultsBuilder.

        Returns:
            Registry key for the forecast model (e.g., "prophet").
        """
        if (
            self._existing_model_config
            and self._existing_model_config.forecast_model_name
        ):
            return self._existing_model_config.forecast_model_name
        return self._defaults.forecast_model_registry_key()

    def _get_anomaly_registry_key(self) -> str:
        """Get the anomaly model registry key.

        Uses the model name from existing config if available (warm start),
        otherwise uses the default from ObserveDefaultsBuilder.

        Returns:
            Registry key for the anomaly model (e.g., "datahub_forecast_anomaly").
        """
        if (
            self._existing_model_config
            and self._existing_model_config.anomaly_model_name
        ):
            return self._existing_model_config.anomaly_model_name
        return self._defaults.anomaly_model_registry_key()

    def _coalesce_forecast_config(self) -> ForecastModelConfig:
        """Coalesce defaults with existing forecast config.

        Pattern: defaults first, then existing values override.
        This ensures new default fields are picked up while preserving
        previously trained hyperparameters.

        If force_retune is True, existing hyperparameters are ignored and
        defaults are used to trigger fresh tuning.

        Returns:
            Merged ForecastModelConfig with existing values overriding defaults.
        """
        default = self._defaults.forecast_config()

        # If force_retune is True, skip existing config to trigger fresh tuning
        if self._force_retune:
            return default

        existing = self._get_existing_forecast_config()

        if existing is None:
            return default

        # Merge: defaults as base, existing overrides
        default_dict = dataclasses.asdict(default)
        existing_dict = dataclasses.asdict(existing)

        merged = {**default_dict}
        for key, value in existing_dict.items():
            if key == "hyperparameters":
                # Deep merge hyperparameters: default first, then existing
                merged["hyperparameters"] = {
                    **default_dict.get("hyperparameters", {}),
                    **existing_dict.get("hyperparameters", {}),
                }
            elif value is not None:
                merged[key] = value

        return ForecastModelConfig(**merged)

    def _coalesce_anomaly_config(self) -> AnomalyModelConfig:
        """Coalesce defaults with existing anomaly config.

        Pattern: defaults first, then existing values override.
        This ensures new default fields are picked up while preserving
        previously trained hyperparameters.

        If force_retune is True, existing hyperparameters are ignored and
        defaults are used to trigger fresh tuning.

        Returns:
            Merged AnomalyModelConfig with existing values overriding defaults.
        """
        default = self._defaults.anomaly_config()

        # If force_retune is True, skip existing config to trigger fresh tuning
        if self._force_retune:
            return default

        existing = self._get_existing_anomaly_config()

        if existing is None:
            return default

        # Merge: defaults as base, existing overrides
        default_dict = dataclasses.asdict(default)
        existing_dict = dataclasses.asdict(existing)

        merged = {**default_dict}
        for key, value in existing_dict.items():
            if key == "hyperparameters":
                # Deep merge hyperparameters: default first, then existing
                merged["hyperparameters"] = {
                    **default_dict.get("hyperparameters", {}),
                    **existing_dict.get("hyperparameters", {}),
                }
            elif value is not None:
                merged[key] = value

        return AnomalyModelConfig(**merged)
