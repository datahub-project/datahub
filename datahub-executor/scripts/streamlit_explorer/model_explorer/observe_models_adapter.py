# ruff: noqa: INP001
"""
Adapter for observe-models registry integration.

This module bridges the observe-models forecasting registry with the Streamlit
time series explorer's model training infrastructure.

Uses ModelFactory from datahub_executor.common.monitor.inference_v2 for
consistent model instantiation with proper config coalescing.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional, cast

import pandas as pd
import streamlit as st

# Type alias for the preprocessing config (avoid import at module level)
_PreprocessingConfig = Any


def is_observe_models_available() -> bool:
    """
    Check if the observe-models package is available.

    Returns:
        True if observe-models can be imported, False otherwise.
    """
    try:
        from datahub_observe.registry import get_model_registry  # type: ignore[import-untyped]  # noqa: F401, I001

        return True
    except ImportError:
        return False


def get_assertion_type_from_context() -> Any:
    """
    Extract assertion type from session state context.

    The assertion type is determined by the current assertion/monitor being analyzed.
    Falls back to VOLUME as the default for volume assertions (most common case).

    Returns:
        AssertionCategory enum value
    """
    try:
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        # Check session state for assertion type context
        # This could be set by the assertion browser or monitor detail pages
        assertion_type = st.session_state.get("current_assertion_type")

        if assertion_type is not None:
            # If it's already an AssertionCategory, return it
            if isinstance(assertion_type, AssertionCategory):
                return assertion_type
            # If it's a string, try to convert it
            if isinstance(assertion_type, str):
                try:
                    return AssertionCategory(assertion_type.lower())
                except ValueError:
                    pass

        # Default to VOLUME (most common use case in this app)
        return AssertionCategory.VOLUME
    except ImportError:
        return None


def build_preprocessing_config(
    assertion_type: Any,
    frequency: str = "auto",
) -> Optional[_PreprocessingConfig]:
    """
    Build an AssertionPreprocessingConfig for observe-models.

    Args:
        assertion_type: AssertionCategory enum value
        frequency: Resampling frequency ('auto', 'hourly', 'daily', etc.)

    Returns:
        AssertionPreprocessingConfig instance, or None if observe-models unavailable
    """
    try:
        from datahub_observe.assertions.config import AssertionPreprocessingConfig  # type: ignore[import-untyped]  # noqa: I001

        return AssertionPreprocessingConfig(
            assertion_type=assertion_type,
            frequency=frequency,
        )
    except ImportError:
        return None


def _build_input_context_from_session() -> Any:
    """
    Build InputDataContext from Streamlit session state.

    Maps the assertion type from session state to an InputDataContext
    that can be used with ObserveDefaultsBuilder.

    Returns:
        InputDataContext instance, or None if imports fail.
    """
    try:
        from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
            InputDataContext,
        )

        # Get assertion type from session state
        assertion_type = get_assertion_type_from_context()
        assertion_category = "volume"  # Default
        if assertion_type is not None:
            assertion_category = str(assertion_type.value).lower()

        # Check session state for cumulative data flag (if set by preprocessing UI)
        is_cumulative = st.session_state.get("is_dataframe_cumulative", False)

        return InputDataContext(
            assertion_category=assertion_category,
            is_dataframe_cumulative=is_cumulative,
            allow_negative=None,  # Let defaults determine based on category
        )
    except ImportError:
        return None


def _get_model_factory(registry_key: Optional[str] = None) -> Any:
    """
    Create a ModelFactory with defaults from session state context.

    Args:
        registry_key: Optional registry key override (not used for factory creation,
            but could be used for future warm-start support).

    Returns:
        ModelFactory instance, or None if imports fail.
    """
    try:
        from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
            get_defaults_for_context,
        )
        from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
            ModelFactory,
        )

        context = _build_input_context_from_session()
        if context is None:
            return None

        defaults = get_defaults_for_context(context)
        return ModelFactory(defaults)
    except ImportError:
        return None


# =============================================================================
# Model Configuration
# =============================================================================

# Color palette for dynamic color cycling (used when models don't have metadata colors)
# These colors are distinct from local model colors (which use blues/greens)
DYNAMIC_COLOR_PALETTE = [
    "#9467bd",  # Purple
    "#8c564b",  # Brown
    "#e377c2",  # Pink
    "#7f7f7f",  # Gray
    "#bcbd22",  # Olive
    "#17becf",  # Cyan
    "#d62728",  # Red
    "#ff7f0e",  # Orange
]


def _get_model_color(registry_key: str, index: int) -> str:
    """
    Get a color for a model from registry metadata or cycle through palette.

    Args:
        registry_key: The model's registry key
        index: Index for cycling through the palette if no metadata color

    Returns:
        Hex color string
    """
    try:
        from datahub_observe.registry import get_model_registry  # type: ignore[import-untyped]  # noqa: I001

        registry = get_model_registry()
        entry = registry.get(registry_key)
        if hasattr(entry, "metadata") and entry.metadata:
            color = entry.metadata.get("color")
            if color:
                return color
    except Exception:
        pass

    # Fall back to cycling through the palette
    return DYNAMIC_COLOR_PALETTE[index % len(DYNAMIC_COLOR_PALETTE)]


@dataclass
class ObserveModelConfig:
    """Configuration for an observe-models forecaster."""

    name: str
    description: str
    registry_key: str  # Key in observe-models registry
    color: str
    dash: Optional[str] = None
    is_observe_model: bool = True

    # These are set dynamically during training
    train_fn: Optional[Callable[[pd.DataFrame], object]] = None
    predict_fn: Optional[Callable[[object, pd.DataFrame], pd.DataFrame]] = None


def _create_train_fn(
    registry_key: str,
) -> Callable[[pd.DataFrame], object]:
    """
    Create a training function for an observe-models forecaster.

    Uses ModelFactory.create_forecast_model() for consistent model instantiation
    with proper config coalescing and context-aware defaults.

    Args:
        registry_key: Key in the observe-models registry

    Returns:
        A function that trains the model and returns it.
    """

    def train_fn(train_df: pd.DataFrame) -> object:
        # Use ModelFactory for consistent instantiation
        factory = _get_model_factory(registry_key)
        if factory is None:
            raise RuntimeError(
                f"ModelFactory not available for {registry_key}. "
                "Ensure datahub_executor is properly installed."
            )

        # Use ModelFactory.create_forecast_model() with explicit registry key
        result = factory.create_forecast_model(registry_key=registry_key)
        model = result.forecast_model

        # Train model
        model.train(train_df)
        return model

    return train_fn


def _create_predict_fn() -> Callable[[object, pd.DataFrame], pd.DataFrame]:
    """
    Create a prediction function for an observe-models forecaster.

    Returns:
        A function that generates predictions from a trained model
    """

    def predict_fn(model: object, future_df: pd.DataFrame) -> pd.DataFrame:
        # Call the model's predict method using getattr to satisfy mypy
        predict_method = getattr(model, "predict")  # noqa: B009
        predictions: pd.DataFrame = predict_method(future_df)

        # Ensure output has expected columns for compatibility
        # observe-models returns: ds, yhat, yhat_lower, yhat_upper
        return predictions

    return predict_fn


def get_observe_model_configs() -> dict[str, ObserveModelConfig]:
    """
    Get model configurations for all available observe-models forecasters.

    Dynamically discovers all forecasting models from the registry
    without any hardcoded filtering.

    Returns:
        Dictionary mapping model keys to ObserveModelConfig instances.
        Returns empty dict if observe-models is not available.
    """
    if not is_observe_models_available():
        return {}

    try:
        from datahub_observe.registry import ModelType, get_model_registry  # type: ignore[import-untyped]  # noqa: I001

        registry = get_model_registry()
        forecasters = registry.list(model_type=ModelType.FORECAST)

        configs: dict[str, ObserveModelConfig] = {}

        for idx, entry in enumerate(forecasters):
            registry_key = entry.name

            # Create prefixed key to avoid collision with local models
            model_key = f"obs_{registry_key}"

            # Use registry name and version for display: "prophet (0.1.0)"
            display_name = f"{entry.name} ({entry.version})"

            configs[model_key] = ObserveModelConfig(
                name=display_name,
                description=entry.description,
                registry_key=registry_key,
                color=_get_model_color(registry_key, idx),
                dash="dot",  # Use dotted line to distinguish from local models
                is_observe_model=True,
                train_fn=_create_train_fn(registry_key),
                predict_fn=_create_predict_fn(),
            )

        return configs

    except Exception:
        # If anything goes wrong, return empty dict
        return {}


# =============================================================================
# Anomaly Detection Model Configuration
# =============================================================================


@dataclass
class AnomalyModelConfig:
    """Configuration for an observe-models anomaly detector."""

    name: str
    description: str
    registry_key: str  # Key in observe-models registry
    requires_forecast_model: bool = True  # Most anomaly models need a forecast model
    requires_global_forecast_model: bool = (
        False  # IForest requires GlobalForecastingModel
    )
    is_observe_model: bool = True

    # These are set dynamically
    # train_fn takes: train_df, forecast_model, ground_truth_df (optional)
    train_fn: Optional[
        Callable[[pd.DataFrame, object, Optional[pd.DataFrame]], object]
    ] = None
    detect_fn: Optional[Callable[[object, pd.DataFrame], pd.DataFrame]] = None


def _create_anomaly_train_fn(
    registry_key: str,
    requires_forecast: bool = True,
    enable_grid_search: bool = False,
) -> Callable[[pd.DataFrame, object, Optional[pd.DataFrame]], object]:
    """
    Create a training function for an observe-models anomaly detector.

    The returned function uses an already-trained forecast model (passed in)
    rather than creating a new one. The forecast model is NOT re-trained.

    Uses ModelFactory.create_anomaly_model() for forecast-based anomaly models,
    ensuring consistent instantiation with proper config coalescing.

    For standalone anomaly models (requires_forecast=False), uses
    ObserveDefaultsBuilder for context-aware preprocessing configuration.

    Args:
        registry_key: Key in the observe-models registry
        requires_forecast: Whether this model requires a forecast model
        enable_grid_search: Whether to enable hyperparameter grid search

    Returns:
        A function that trains the anomaly model and returns it
    """

    def train_fn(
        train_df: pd.DataFrame,
        forecast_model: object,
        ground_truth: Optional[pd.DataFrame] = None,
    ) -> object:
        # Set param_grid for grid search
        param_grid = "auto" if enable_grid_search else None

        if requires_forecast:
            # Use ModelFactory.create_anomaly_model() for consistent instantiation
            factory = _get_model_factory(registry_key)
            if factory is None:
                raise RuntimeError(
                    f"ModelFactory not available for {registry_key}. "
                    "Ensure datahub_executor is properly installed."
                )

            # Cast forecast_model to correct type for ModelFactory
            from datahub_observe.algorithms.forecasting.forecast_base import (
                DartsBaseForecastModel,
            )

            result = factory.create_anomaly_model(
                forecast_model=cast(DartsBaseForecastModel, forecast_model),
                anomaly_registry_key=registry_key,
                param_grid=param_grid,
            )
            anomaly_model = result.anomaly_model
        else:
            # For standalone models (e.g., DeepSVDD) that don't need a forecast
            # Try to use ObserveDefaultsBuilder for context-aware preprocessing
            preprocessing_config = None
            try:
                from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
                    get_defaults_for_context,
                )

                context = _build_input_context_from_session()
                if context is not None:
                    defaults = get_defaults_for_context(context)
                    preprocessing_config = defaults.preprocessing_config()
            except ImportError:
                pass

            # Fallback if ModelFactory approach didn't work
            if preprocessing_config is None:
                assertion_type = get_assertion_type_from_context()
                preprocessing_config = build_preprocessing_config(
                    assertion_type=assertion_type,
                    frequency="auto",
                )

            from datahub_observe.registry import get_model_registry

            registry = get_model_registry()
            entry = registry.get(registry_key)
            model_class = entry.cls

            # Pass empty model_config to use defaults
            anomaly_model = model_class(
                preprocessing_config=preprocessing_config,
                model_config={},
                param_grid=param_grid,
            )

        # Train the anomaly model (trains scorer, not forecast model)
        # Pass ground_truth for grid search scoring
        anomaly_model.train(train_df, ground_truth=ground_truth)
        return anomaly_model

    return train_fn


def _create_anomaly_detect_fn() -> Callable[[object, pd.DataFrame], pd.DataFrame]:
    """
    Create a detection function for an observe-models anomaly detector.

    Returns:
        A function that detects anomalies and returns results
    """

    def detect_fn(model: object, data_df: pd.DataFrame) -> pd.DataFrame:
        # Call the model's detect method
        detect_method = getattr(model, "detect")  # noqa: B009
        results: pd.DataFrame = detect_method(data_df)

        # Expected output columns: ds, y, is_anomaly, anomaly_score,
        # detection_band_upper, detection_band_lower
        return results

    return detect_fn


def get_anomaly_model_configs(
    enable_grid_search: bool = False,
) -> dict[str, AnomalyModelConfig]:
    """
    Get model configurations for all available observe-models anomaly detectors.

    Dynamically discovers all anomaly detection models from the registry
    without any hardcoded filtering.

    Args:
        enable_grid_search: Whether to enable hyperparameter grid search for models

    Returns:
        Dictionary mapping model keys to AnomalyModelConfig instances.
        Returns empty dict if observe-models is not available.
    """
    if not is_observe_models_available():
        return {}

    try:
        from datahub_observe.registry import ModelType, get_model_registry  # type: ignore[import-untyped]  # noqa: I001

        registry = get_model_registry()
        anomaly_detectors = registry.list(model_type=ModelType.ANOMALY)

        configs: dict[str, AnomalyModelConfig] = {}

        for entry in anomaly_detectors:
            registry_key = entry.name

            # Create prefixed key to avoid collision
            model_key = f"anomaly_{registry_key}"

            # Use registry name and version for display
            display_name = f"{entry.name} ({entry.version})"

            # Check metadata for whether this model requires a forecast model
            # Default to True since most anomaly models need a forecast
            requires_forecast = True
            if hasattr(entry, "metadata") and entry.metadata:
                requires_forecast = entry.metadata.get("requires_forecast_model", True)

            # Also check base class - BaseAnomalyModel doesn't require forecast,
            # only BaseForecastAnomalyModel does
            base_class = getattr(entry, "base_class_name", None)
            if base_class == "BaseAnomalyModel":
                requires_forecast = False

            # Check if this model requires a GlobalForecastingModel (Darts models)
            # e.g., IForest only works with GlobalForecastingModels (nbeats, chronos2)
            # not with Prophet or DataHub forecasters
            # This is defined in the registry metadata - no hardcoding needed
            requires_global = False
            if hasattr(entry, "metadata") and entry.metadata:
                requires_global = entry.metadata.get(
                    "requires_global_forecast_model", False
                )

            configs[model_key] = AnomalyModelConfig(
                name=display_name,
                description=entry.description,
                registry_key=registry_key,
                requires_forecast_model=requires_forecast,
                requires_global_forecast_model=requires_global,
                is_observe_model=True,
                train_fn=_create_anomaly_train_fn(
                    registry_key, requires_forecast, enable_grid_search
                ),
                detect_fn=_create_anomaly_detect_fn(),
            )

        return configs

    except Exception:
        # If anything goes wrong, return empty dict
        return {}


def is_global_forecasting_model(registry_key: Optional[str]) -> bool:
    """
    Check if a forecast model is a GlobalForecastingModel.

    This is used to filter compatible forecast models for anomaly detection
    models that require GlobalForecastingModels (e.g., IForest).

    Args:
        registry_key: The registry key from TrainingRun.registry_key (e.g., "nbeats")
                      Use TrainingRun.registry_key directly - no string parsing needed.

    Returns:
        True if the model is a GlobalForecastingModel, False otherwise.
        Returns False if observe-models is not available or model not found.
    """
    if registry_key is None:
        return False

    if not is_observe_models_available():
        return False

    try:
        from datahub_observe.registry import ModelType, get_model_registry  # type: ignore[import-untyped]  # noqa: I001

        registry = get_model_registry()
        forecasters = registry.list(model_type=ModelType.FORECAST)

        for entry in forecasters:
            if entry.name == registry_key:
                # Check metadata for is_global_forecasting_model
                if hasattr(entry, "metadata") and entry.metadata:
                    return entry.metadata.get("is_global_forecasting_model", False)
                return False

        return False

    except Exception:
        return False


def get_model_preprocessing_defaults(registry_key: str) -> Optional[Any]:
    """
    Get preprocessing defaults for a model from the registry.

    Uses ModelRegistry.get_preprocessing_defaults() to retrieve the model's
    preferred preprocessing configuration. This enables compatibility checking
    between user-selected preprocessing and model requirements.

    Args:
        registry_key: The model's registry key (e.g., "prophet", "nbeats")

    Returns:
        PreprocessingDefaults instance if model has defaults, None otherwise.
        Returns None if observe-models is not available or model not found.
    """
    if not is_observe_models_available():
        return None

    try:
        from datahub_observe.registry import get_model_registry  # type: ignore[import-untyped]  # noqa: I001

        registry = get_model_registry()
        return registry.get_preprocessing_defaults(registry_key)

    except Exception:
        return None


__all__ = [
    "is_observe_models_available",
    "get_assertion_type_from_context",
    "build_preprocessing_config",
    "get_observe_model_configs",
    "get_anomaly_model_configs",
    "is_global_forecasting_model",
    "get_model_preprocessing_defaults",
    "ObserveModelConfig",
    "AnomalyModelConfig",
    "DYNAMIC_COLOR_PALETTE",
]
