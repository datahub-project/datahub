"""Default configurations for observe-models integration.

Provides context-aware defaults that vary based on input data characteristics.
This module centralizes all default configuration values for the inference_v2 pipeline.
"""

from dataclasses import dataclass
from typing import Optional, Union

from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.algorithms.preprocessing.volume_preprocessor import (
    VolumePreprocessorConfig,
)
from datahub_observe.assertions.config import AssertionPreprocessingConfig
from datahub_observe.assertions.types import AssertionCategory

# =============================================================================
# Default Model Registry Keys
# =============================================================================
# These are the default registry keys for the models used in the inference pipeline.
# They match the keys registered in datahub_observe's model registry.
# When warm-starting from stored inference details, the stored model names override
# these defaults.

DEFAULT_FORECAST_MODEL_REGISTRY_KEY = "datahub"
DEFAULT_ANOMALY_MODEL_REGISTRY_KEY = "datahub_forecast_anomaly"

# Category string to enum mapping
CATEGORY_STRING_TO_ENUM: dict[str, AssertionCategory] = {
    "volume": AssertionCategory.VOLUME,
    "freshness": AssertionCategory.FRESHNESS,
    "rate": AssertionCategory.RATE,
    "statistic": AssertionCategory.STATISTIC,
    "length": AssertionCategory.LENGTH,
}


@dataclass
class InputDataContext:
    """Context describing the input data's characteristics.

    Callers provide metadata about the shape and nature of the input data.
    The adapter uses this to determine appropriate preprocessing and model
    configuration - callers don't need to know the implementation details.

    Attributes:
        assertion_category: The assertion category (volume, rate, freshness, etc.).
            Used to determine appropriate preprocessing defaults.
        is_dataframe_cumulative: Whether the input dataframe contains cumulative
            values (e.g., ROW_COUNT_TOTAL). If True, differencing will be applied
            to convert to deltas.
        is_delta: Whether data represents deltas/changes (True) or cumulative
            values (False). If True (default), negative values are allowed.
            If False, negative values will be filtered during preprocessing.
            If None, the default is determined by assertion_category.
    """

    assertion_category: str = "volume"
    is_dataframe_cumulative: bool = False
    is_delta: Optional[bool] = None


class ObserveDefaultsBuilder:
    """Builds default configs based on input data context.

    This class provides context-aware default configurations for preprocessing,
    forecasting, and anomaly detection. Defaults vary based on the assertion
    category and data characteristics.

    Example:
        >>> context = InputDataContext(
        ...     assertion_category="volume",
        ...     is_dataframe_cumulative=True,
        ...     is_delta=False,
        ... )
        >>> builder = ObserveDefaultsBuilder(context)
        >>> preprocessing = builder.preprocessing_config()
        >>> forecast = builder.forecast_config()
    """

    def __init__(self, context: InputDataContext):
        """Initialize the defaults builder with input data context.

        Args:
            context: The input data context describing data characteristics.
        """
        self.context = context
        self._category_enum = self._resolve_category()

    def _resolve_category(self) -> AssertionCategory:
        """Convert string category to AssertionCategory enum.

        Returns:
            The AssertionCategory enum value, defaulting to VOLUME if not found.
        """
        return CATEGORY_STRING_TO_ENUM.get(
            self.context.assertion_category.lower(), AssertionCategory.VOLUME
        )

    def preprocessing_config(
        self,
    ) -> Union[VolumePreprocessorConfig, AssertionPreprocessingConfig]:
        """Build default preprocessing config for the input data context.

        For volume assertions with cumulative data, returns VolumePreprocessorConfig
        with differencing enabled. For other cases, returns AssertionPreprocessingConfig
        with appropriate defaults for the category.

        Returns:
            The preprocessing config appropriate for the input data.
        """
        # Volume with cumulative data needs special handling
        if (
            self._category_enum == AssertionCategory.VOLUME
            and self.context.is_dataframe_cumulative
        ):
            return VolumePreprocessorConfig(
                convert_cumulative=True,
                # Validates input dataframe before differencing - raw cumulative
                # row counts should never be negative. Should almost always be False.
                is_delta=False,
                strict_validation=True,
            )

        # Default: use AssertionPreprocessingConfig with category-specific defaults
        # Determine is_delta: explicit override > category-based default
        if self.context.is_delta is not None:
            is_delta = self.context.is_delta
        else:
            # Volume should not allow negative values (is_delta=False),
            # other categories may (is_delta=True)
            is_delta = self._category_enum != AssertionCategory.VOLUME

        return AssertionPreprocessingConfig(
            assertion_type=self._category_enum,
            frequency="auto",
            is_delta=is_delta,
        )

    def forecast_config(self) -> ForecastModelConfig:
        """Build default forecast model config.

        Returns an empty hyperparameters config, allowing the model to use
        its own defaults. Future fields added to ForecastModelConfig will
        be handled here.

        Returns:
            Default ForecastModelConfig.
        """
        return ForecastModelConfig(hyperparameters={})

    def anomaly_config(self) -> AnomalyModelConfig:
        """Build default anomaly model config.

        Returns an empty hyperparameters config, allowing the model to use
        its own defaults. Future fields added to AnomalyModelConfig will
        be handled here.

        Returns:
            Default AnomalyModelConfig.
        """
        return AnomalyModelConfig(hyperparameters={})

    def forecast_model_registry_key(self) -> str:
        """Get the default forecast model registry key.

        Returns:
            Registry key for the default forecast model (e.g., "prophet").
        """
        return DEFAULT_FORECAST_MODEL_REGISTRY_KEY

    def anomaly_model_registry_key(self) -> str:
        """Get the default anomaly model registry key.

        Returns:
            Registry key for the default anomaly model (e.g., "datahub_forecast_anomaly").
        """
        return DEFAULT_ANOMALY_MODEL_REGISTRY_KEY


def get_defaults_for_context(context: InputDataContext) -> ObserveDefaultsBuilder:
    """Factory function to get defaults builder for an input data context.

    Args:
        context: The input data context describing data characteristics.

    Returns:
        An ObserveDefaultsBuilder configured for the given context.
    """
    return ObserveDefaultsBuilder(context)
