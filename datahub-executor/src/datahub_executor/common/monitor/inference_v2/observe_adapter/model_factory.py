"""
Factory for creating and configuring observe models.

This module handles model initialization with proper config coalescing:
- Deserializes existing configs from JSON (warm start)
- Coalesces defaults with existing configs
- Creates forecast and anomaly models with proper configuration
"""

import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, cast

from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.anomaly_detection.forecast_anomaly_base import (
    BaseForecastAnomalyModel,
)
from datahub_observe.algorithms.config import PreprocessingConfigType
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.algorithms.forecasting.forecast_base import DartsBaseForecastModel
from datahub_observe.registry import get_model_registry

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
    """Result of model initialization.

    This class wraps created models and their configurations. Supports three modes:
    1. Full anomaly detection: Both forecast and anomaly models created together
    2. Forecast-only: Standalone forecast model without anomaly detection
    3. Anomaly-only: Anomaly model wrapping a pre-trained forecast model

    Fields are optional to support all modes. Use the properties to access
    models safely with appropriate assertions.
    """

    # Models - at least one will be set
    anomaly_model: Optional[BaseForecastAnomalyModel] = None
    _standalone_forecast_model: Optional[DartsBaseForecastModel] = None

    # Configs - set based on what was created
    preprocessing_config: Optional[PreprocessingConfigType] = None
    anomaly_config: Optional[AnomalyModelConfig] = None
    _standalone_forecast_config: Optional[ForecastModelConfig] = None

    # Registry keys - set based on what was created
    forecast_registry_key: Optional[str] = None
    anomaly_registry_key: Optional[str] = None

    @property
    def forecast_model(self) -> DartsBaseForecastModel:
        """Access the forecast model.

        Returns the standalone forecast model if set, otherwise extracts
        from the anomaly model (composition).
        """
        if self._standalone_forecast_model is not None:
            return self._standalone_forecast_model
        assert self.anomaly_model is not None, "No forecast model available"
        return self.anomaly_model.forecast_model

    @property
    def forecast_config(self) -> ForecastModelConfig:
        """Access the forecast config.

        Returns the standalone forecast config if set, otherwise extracts
        from the anomaly config (embedded).
        """
        if self._standalone_forecast_config is not None:
            return self._standalone_forecast_config
        assert self.anomaly_config is not None, "No forecast config available"
        assert self.anomaly_config.forecast_model_config is not None
        return self.anomaly_config.forecast_model_config

    @property
    def has_anomaly_model(self) -> bool:
        """Check if an anomaly model is available."""
        return self.anomaly_model is not None

    @property
    def has_forecast_only(self) -> bool:
        """Check if this is a forecast-only result (no anomaly model)."""
        return (
            self._standalone_forecast_model is not None and self.anomaly_model is None
        )


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
        ground_truth: Optional[Any] = None,
    ):
        """
        Initialize the factory.

        Args:
            defaults: ObserveDefaultsBuilder providing context-aware default configs.
            existing_model_config: Previously trained model config for warm start.
            force_retune: If True, ignore existing hyperparameters and use defaults
                only. This forces fresh tuning of the model. Preprocessing config
                is still preserved since it describes the data shape.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
                Used to configure preprocessing to exclude anomalies from training.
        """
        self._defaults = defaults
        self._existing_model_config = existing_model_config
        self._force_retune = force_retune
        self._ground_truth = ground_truth

    def create_models(self) -> InitializedModels:
        """
        Create anomaly model with embedded forecast model using registry.

        The registry's `create_anomaly_model()` method handles creating both
        the forecast model (from forecast_model_config) and the anomaly model,
        automatically wiring them together via composition.

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

        # Build anomaly config with embedded forecast config
        anomaly_config = self._coalesce_anomaly_config()

        # Get parallelization kwargs for consistent defaults across all models
        parallelization_kwargs = self._defaults.parallelization_kwargs()

        # Use typed registry factory - handles forecast model creation automatically
        # Cast is safe: we always provide forecast_model_name, so it returns
        # BaseForecastAnomalyModel (not BaseAnomalyModel)
        anomaly_model = cast(
            BaseForecastAnomalyModel,
            registry.create_anomaly_model(
                anomaly_registry_key,
                preprocessing_config,
                anomaly_config,
                forecast_model_name=forecast_registry_key,
                **parallelization_kwargs,
            ),
        )

        return InitializedModels(
            anomaly_model=anomaly_model,
            preprocessing_config=preprocessing_config,
            anomaly_config=anomaly_config,
            forecast_registry_key=forecast_registry_key,
            anomaly_registry_key=anomaly_registry_key,
        )

    def create_forecast_model(
        self, registry_key: Optional[str] = None
    ) -> InitializedModels:
        """
        Create a standalone forecast model without anomaly detection.

        This method creates just the forecast model, useful for:
        - Training forecast models independently before anomaly detection
        - Streamlit explorer where users train forecast models separately
        - Testing and comparison of different forecasting approaches

        Args:
            registry_key: Optional override for the forecast model registry key.
                If not provided, uses the key from existing config or defaults.

        Returns:
            InitializedModels with forecast model only (has_forecast_only=True).
        """
        registry = get_model_registry()

        # Determine model registry key: explicit override > existing config > default
        if registry_key is not None:
            forecast_registry_key = registry_key
        else:
            forecast_registry_key = self._get_forecast_registry_key()

        # Build preprocessing config (coalesced with existing if available)
        preprocessing_config = self._build_preprocessing_config()

        # Build forecast config (coalesced with existing if available)
        forecast_config = self._coalesce_forecast_config(self._force_retune)

        # Get parallelization kwargs for consistent defaults across all models
        parallelization_kwargs = self._defaults.parallelization_kwargs()

        # Create the forecast model via registry
        forecast_model = registry.create_forecast_model(
            forecast_registry_key,
            preprocessing_config,
            forecast_config,
            **parallelization_kwargs,
        )

        return InitializedModels(
            _standalone_forecast_model=forecast_model,
            preprocessing_config=preprocessing_config,
            _standalone_forecast_config=forecast_config,
            forecast_registry_key=forecast_registry_key,
        )

    def create_anomaly_model(
        self,
        forecast_model: DartsBaseForecastModel,
        anomaly_registry_key: Optional[str] = None,
        param_grid: Optional[Union[str, Dict[str, List[Any]], bool]] = None,
    ) -> InitializedModels:
        """
        Create an anomaly model using a pre-trained forecast model.

        This method wraps an existing forecast model in an anomaly detector,
        useful for:
        - Streamlit explorer where users train forecast models separately
        - Testing different anomaly detectors on the same forecast model
        - Reusing a trained forecast model with multiple anomaly approaches

        The forecast model is NOT re-trained - only the anomaly scorer/detector
        is created fresh and will be trained by the caller.

        Args:
            forecast_model: A pre-trained DartsBaseForecastModel to use for
                anomaly detection. This model will be wrapped by the anomaly
                model via composition.
            anomaly_registry_key: Optional override for the anomaly model registry
                key. If not provided, uses the key from existing config or defaults.
            param_grid: Controls anomaly-model hyperparameter tuning.
                - "auto"/True/None: enable default grid search (model decides defaults)
                - {} / False: disable grid search
                - dict: explicit parameter grid

        Returns:
            InitializedModels with anomaly model wrapping the pre-trained forecast.
        """
        registry = get_model_registry()

        # Determine anomaly model registry key
        if anomaly_registry_key is not None:
            registry_key = anomaly_registry_key
        else:
            registry_key = self._get_anomaly_registry_key()

        # Build anomaly config (coalesced with existing if available).
        # Note: We don't embed forecast_model_config since we're using a pre-trained
        # forecast model directly.
        anomaly_config = self._coalesce_anomaly_config(embed_forecast_config=False)

        # Apply grid-search override from caller (Streamlit explorer).
        # In this API, param_grid=None historically meant "no grid search".
        if param_grid is None or param_grid is False or param_grid == {}:
            anomaly_config = dataclasses.replace(anomaly_config, param_grid={})
        elif param_grid == "auto" or param_grid is True:
            # Leave param_grid as None to use model defaults.
            pass
        elif isinstance(param_grid, dict):
            anomaly_config = dataclasses.replace(anomaly_config, param_grid=param_grid)

        # Get the anomaly model class from registry and instantiate with
        # the pre-trained forecast model
        entry = registry.get(registry_key)
        model_class = entry.cls

        # Get parallelization kwargs for consistent defaults across all models
        parallelization_kwargs = self._defaults.parallelization_kwargs()

        # Create anomaly model with the pre-trained forecast model.
        # Prefer using observe-models' from_config() so sensitivity and other config
        # fields are consistently applied.
        if hasattr(model_class, "from_config"):
            anomaly_model = model_class.from_config(  # type: ignore[attr-defined]
                preprocessing_config=forecast_model.preprocessing_config,
                model_config=anomaly_config,
                forecast_model=forecast_model,
                **parallelization_kwargs,
            )
        else:
            anomaly_model = model_class(
                forecast_model=forecast_model,
                param_grid=param_grid,
                **parallelization_kwargs,
            )

        return InitializedModels(
            anomaly_model=anomaly_model,
            anomaly_config=anomaly_config,
            anomaly_registry_key=registry_key,
        )

    def _build_preprocessing_config(self) -> PreprocessingConfigType:
        """Build preprocessing config by coalescing existing with defaults.

        Returns existing config if available, otherwise uses defaults from
        ObserveDefaultsBuilder based on the input data context.

        Returns:
            Concrete observe-models `PreprocessingConfig` (base + patches).
        """
        # Try existing config first (warm start)
        existing = self._get_existing_preprocessing_config()
        if existing is not None:
            # Even with existing config, check if we need to add AnomalyDataFilterConfig
            # when ground_truth has anomalies
            if self._ground_truth is not None:
                import pandas as pd
                from datahub_observe.algorithms.preprocessing.transformers import (
                    AnomalyDataFilterConfig,
                )

                # Check if ground_truth is a DataFrame and has anomalies
                if isinstance(self._ground_truth, pd.DataFrame):
                    if (
                        not self._ground_truth.empty
                        and "is_anomaly_gt" in self._ground_truth.columns
                    ):
                        # Use .eq(True) to handle boolean Series correctly
                        anomalies = self._ground_truth[
                            self._ground_truth["is_anomaly_gt"].eq(True)
                        ]
                        if not anomalies.empty:
                            # Check if AnomalyDataFilterConfig already exists
                            has_anomaly_filter = any(
                                isinstance(t, AnomalyDataFilterConfig)
                                for t in existing.pandas_transformers
                            )
                            if not has_anomaly_filter:
                                # Add AnomalyDataFilterConfig to existing config
                                anomaly_filter = AnomalyDataFilterConfig(
                                    enabled=True, type_values=["ANOMALY"]
                                )
                                return dataclasses.replace(
                                    existing,
                                    pandas_transformers=[
                                        anomaly_filter,
                                        *existing.pandas_transformers,
                                    ],
                                )
            return existing

        # Use defaults builder for context-aware defaults, passing ground_truth
        # to enable anomaly filtering when anomalies are present
        return self._defaults.preprocessing_config(ground_truth=self._ground_truth)

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
            name = self._existing_model_config.forecast_model_name
            # If already version-qualified (e.g. from pairing evaluation), keep as-is.
            if "@" in name:
                return name
            version = self._existing_model_config.forecast_model_version
            if version:
                return f"{name}@{version}"
            return name
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
            name = self._existing_model_config.anomaly_model_name
            if "@" in name:
                return name
            version = self._existing_model_config.anomaly_model_version
            if version:
                return f"{name}@{version}"
            return name
        return self._defaults.anomaly_model_registry_key()

    def _coalesce_forecast_config(self, force_retune: bool) -> ForecastModelConfig:
        """Coalesce defaults with existing forecast config.

        Pattern: defaults first, then existing values override.
        This ensures new default fields are picked up while preserving
        previously trained hyperparameters.

        Args:
            force_retune: If True, existing hyperparameters are ignored and
                defaults are used to trigger fresh tuning.

        Returns:
            Merged ForecastModelConfig with existing values overriding defaults.
        """
        default = self._defaults.forecast_config()

        # If force_retune is True, skip existing config to trigger fresh tuning
        if force_retune:
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

    def _coalesce_anomaly_config(
        self, embed_forecast_config: bool = True
    ) -> AnomalyModelConfig:
        """Coalesce defaults with existing anomaly config.

        Pattern: defaults first, then existing values override.
        This ensures new default fields are picked up while preserving
        previously trained hyperparameters.

        Args:
            embed_forecast_config: If True, embeds the coalesced forecast_model_config
                in the returned config. Set to False when using a pre-trained forecast
                model directly (the forecast config is not needed in that case).

        If force_retune is True, existing hyperparameters are ignored and
        defaults are used to trigger fresh tuning.

        Returns:
            Merged AnomalyModelConfig, optionally with forecast_model_config embedded.
        """
        default = self._defaults.anomaly_config()

        # If force_retune is True, skip existing config to trigger fresh tuning
        if self._force_retune:
            if embed_forecast_config:
                forecast_config = self._coalesce_forecast_config(self._force_retune)
                return dataclasses.replace(
                    default, forecast_model_config=forecast_config
                )
            return default

        existing = self._get_existing_anomaly_config()

        if existing is None:
            if embed_forecast_config:
                forecast_config = self._coalesce_forecast_config(self._force_retune)
                return dataclasses.replace(
                    default, forecast_model_config=forecast_config
                )
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
            elif key == "forecast_model_config":
                # Skip - handled separately based on embed_forecast_config
                pass
            elif value is not None:
                merged[key] = value

        # Optionally embed the coalesced forecast config
        if embed_forecast_config:
            merged["forecast_model_config"] = self._coalesce_forecast_config(
                self._force_retune
            )

        return AnomalyModelConfig(**merged)
