"""Default configurations for observe-models integration.

Provides context-aware defaults that vary based on input data characteristics.
This module centralizes all default configuration values for the inference_v2 pipeline.

Preprocessing Configuration Hierarchy:
1. Base defaults from assertion category
2. Data characteristics (cumulative, delta)
3. Monitor-level overrides from AssertionAdjustmentSettings
"""

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeAlias, Union

if TYPE_CHECKING:
    import pandas as pd

from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.algorithms.preprocessing.preprocessor import PreprocessingConfig
from datahub_observe.algorithms.preprocessing.transformers import (
    AnomalyDataFilterConfig,
    DataFilterConfig,
    DifferenceConfig,
    FrequencyAlignmentConfig,
    FrequencyAnalysisConfig,
    FrequencyTruncationConfig,
    InitDataFilterConfig,
    MissingDataConfig,
    ResamplingConfig,
    ValueFilterConfig,
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

DartsTransformer: TypeAlias = DifferenceConfig | ResamplingConfig
PandasTransformer: TypeAlias = (
    InitDataFilterConfig
    | DataFilterConfig
    | AnomalyDataFilterConfig
    | FrequencyAlignmentConfig
    | FrequencyAnalysisConfig
    | FrequencyTruncationConfig
    | ValueFilterConfig
    | MissingDataConfig
)


def _enable_differencing(cfg: PreprocessingConfig) -> PreprocessingConfig:
    """Enable differencing in the darts transformer list."""
    found = False
    new_darts: list[DartsTransformer] = []
    for t in cfg.darts_transformers:
        if isinstance(t, DifferenceConfig):
            found = True
            if t.enabled:
                new_darts.append(t)
            else:
                new_darts.append(DifferenceConfig(enabled=True, order=t.order))
        else:
            new_darts.append(t)

    if not found:
        new_darts = [DifferenceConfig(enabled=True), *cfg.darts_transformers]

    return replace(cfg, darts_transformers=new_darts)


def _set_resampling_aggregation(
    cfg: PreprocessingConfig, method: str
) -> PreprocessingConfig:
    """Set resampling aggregation method when a ResamplingConfig is present."""
    new_darts: list[DartsTransformer] = []
    changed = False
    for t in cfg.darts_transformers:
        if isinstance(t, ResamplingConfig):
            if t.aggregation_method != method:
                new_darts.append(replace(t, aggregation_method=method))
                changed = True
            else:
                new_darts.append(t)
        else:
            new_darts.append(t)
    return replace(cfg, darts_transformers=new_darts) if changed else cfg


def _set_resampling_frequency(
    cfg: PreprocessingConfig, frequency: str
) -> PreprocessingConfig:
    """Set resampling frequency when a ResamplingConfig is present (e.g. 'auto' for heuristics)."""
    new_darts: list[DartsTransformer] = []
    changed = False
    for t in cfg.darts_transformers:
        if isinstance(t, ResamplingConfig):
            if t.frequency != frequency:
                new_darts.append(replace(t, frequency=frequency))
                changed = True
            else:
                new_darts.append(t)
        else:
            new_darts.append(t)
    return replace(cfg, darts_transformers=new_darts) if changed else cfg


def _set_frequency_alignment_aggregation(
    cfg: PreprocessingConfig, method: str
) -> PreprocessingConfig:
    """Set frequency-alignment aggregation method when a FrequencyAlignmentConfig is present."""
    new_pandas: list[PandasTransformer] = []
    changed = False
    for t in cfg.pandas_transformers:
        if isinstance(t, FrequencyAlignmentConfig):
            if t.aggregation_method != method:
                new_pandas.append(replace(t, aggregation_method=method))
                changed = True
            else:
                new_pandas.append(t)
        else:
            new_pandas.append(t)
    return replace(cfg, pandas_transformers=new_pandas) if changed else cfg


def _ensure_value_filter_min0(
    cfg: PreprocessingConfig, *, strict: bool
) -> PreprocessingConfig:
    """Ensure a ValueFilter(min_value=0) exists to filter negative values."""
    for t in cfg.pandas_transformers:
        if isinstance(t, ValueFilterConfig) and t.min_value == 0:
            # Preserve user's strictness if already configured; otherwise align.
            if t.strict != strict:
                new_pandas: list[PandasTransformer] = []
                for x in cfg.pandas_transformers:
                    if x is t:
                        new_pandas.append(replace(t, strict=strict))
                    else:
                        new_pandas.append(x)
                return replace(cfg, pandas_transformers=new_pandas)
            return cfg

    new_pandas = [
        *cfg.pandas_transformers,
        ValueFilterConfig(enabled=True, min_value=0, strict=strict),
    ]
    return replace(cfg, pandas_transformers=new_pandas)


@dataclass
class ExclusionWindow:
    """Time window to exclude from training data.

    Attributes:
        start_time_ms: Start timestamp in milliseconds
        end_time_ms: End timestamp in milliseconds
        display_name: Optional display name for the window
    """

    start_time_ms: int
    end_time_ms: int
    display_name: Optional[str] = None


@dataclass
class AdjustmentSettings:
    """Monitor-level settings for preprocessing and model configuration.

    These settings come from AssertionAdjustmentSettings and allow per-monitor
    overrides of preprocessing and model behavior.

    Attributes:
        sensitivity_level: Sensitivity level (1-10) for model behavior.
            Higher (7-10): more aggressive (tighter bands / more anomalies).
            Lower (1-3): more conservative (wider bands / fewer anomalies).
            Medium (4-6): balanced. In observe-models this maps to
            contamination/coverage-like parameters for anomaly models.
        exclusion_windows: Time windows to exclude from training data.
        training_lookback_days: Number of days to look back for training data.
        context: Adjustment context (optional dict from assertion settings) for
            preprocessing. Distinct from input data context (InputDataContext).
    """

    sensitivity_level: Optional[int] = None
    exclusion_windows: List[ExclusionWindow] = field(default_factory=list)
    training_lookback_days: Optional[int] = None
    context: Optional[Dict[str, str]] = None


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
        adjustment_settings: Optional monitor-level settings for preprocessing
            and model configuration overrides.
        metric_type: Optional field metric type (e.g., "MEAN", "NULL_COUNT").
            Affects aggregation method selection for field assertions.
    """

    assertion_category: str = "volume"
    is_dataframe_cumulative: bool = False
    is_delta: Optional[bool] = None
    adjustment_settings: Optional[AdjustmentSettings] = None
    metric_type: Optional[str] = None


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
        ground_truth: Optional["pd.DataFrame"] = None,
    ) -> Union[AssertionPreprocessingConfig, PreprocessingConfig]:
        """
        Build default preprocessing config for the input data context.

        For VOLUME assertions returns AssertionPreprocessingConfig so observe-models
        creates VolumeTimeSeriesPreprocessor (and sets is_delta_series for resampling).
        For other categories returns a concrete PreprocessingConfig.

        Args:
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
                If provided and contains anomalies, AnomalyDataFilterConfig will be
                added to exclude anomalies from training data.
        """

        cfg = PreprocessingConfig()

        # Add anomaly filter if ground truth contains anomalies
        if ground_truth is not None and not ground_truth.empty:
            if "is_anomaly_gt" in ground_truth.columns:
                # Check if there are any anomalies in ground truth
                # Use .eq(True) to handle boolean Series correctly, including NaN values
                anomalies = ground_truth[ground_truth["is_anomaly_gt"].eq(True)]
                if not anomalies.empty:
                    # Add AnomalyDataFilterConfig early in the pipeline
                    # Insert at the beginning of pandas_transformers to filter before other operations
                    anomaly_filter = AnomalyDataFilterConfig(
                        enabled=True, type_values=["ANOMALY"]
                    )
                    # Check if AnomalyDataFilterConfig already exists to avoid duplicates
                    has_anomaly_filter = any(
                        isinstance(t, AnomalyDataFilterConfig)
                        for t in cfg.pandas_transformers
                    )
                    if not has_anomaly_filter:
                        cfg = replace(
                            cfg,
                            pandas_transformers=[
                                anomaly_filter,
                                *cfg.pandas_transformers,
                            ],
                        )
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.info(
                            f"Added AnomalyDataFilterConfig to preprocessing pipeline "
                            f"(found {len(anomalies)} anomalies in ground truth)"
                        )

        # Volume-specific defaults
        if self._category_enum == AssertionCategory.VOLUME:
            # Determine delta semantics: explicit override wins, otherwise default volume => not delta.
            is_delta = (
                bool(self.context.is_delta)
                if self.context.is_delta is not None
                else False
            )

            # If dataframe is cumulative totals, we must difference to deltas.
            if self.context.is_dataframe_cumulative:
                is_delta = False
                cfg = _enable_differencing(cfg)
                # Use frequency='auto' so ResamplingTransformer runs heuristics (oversampling:
                # delta + hourly -> daily, max). We pass AssertionPreprocessingConfig(VOLUME)
                # so observe-models creates VolumeTimeSeriesPreprocessor and sets is_delta_series.
                cfg = _set_resampling_frequency(cfg, "auto")

            # Volume aggregation: deltas -> sum, snapshots -> last.
            # After differencing (is_dataframe_cumulative) the series is delta, so use sum.
            aggregation = (
                "sum" if (is_delta or self.context.is_dataframe_cumulative) else "last"
            )
            cfg = _set_resampling_aggregation(cfg, aggregation)
            cfg = _set_frequency_alignment_aggregation(cfg, aggregation)

            # For non-delta volume, filter negatives; default to non-strict validation.
            strict_validation = False
            if not is_delta:
                cfg = _ensure_value_filter_min0(cfg, strict=strict_validation)

            return AssertionPreprocessingConfig(
                assertion_type=AssertionCategory.VOLUME,
                is_delta=is_delta,
                strict_validation=strict_validation,
                base_preprocessing_config=cfg,
            )

        # Non-volume defaults: use mean aggregation (rates/statistics/length/freshness).
        if self._category_enum in {
            AssertionCategory.RATE,
            AssertionCategory.STATISTIC,
            AssertionCategory.LENGTH,
            AssertionCategory.FRESHNESS,
        }:
            cfg = _set_resampling_aggregation(cfg, "mean")
            cfg = _set_frequency_alignment_aggregation(cfg, "mean")
            return cfg

        return cfg

    def forecast_config(self) -> ForecastModelConfig:
        """Build default forecast model config.

        Returns an empty hyperparameters config, allowing the model to use
        its own defaults. If sensitivity_level is set in adjustment_settings,
        it is passed to the config for model interpretation.

        Returns:
            Default ForecastModelConfig with optional sensitivity.
        """
        sensitivity = None
        if (
            self.context.adjustment_settings
            and self.context.adjustment_settings.sensitivity_level is not None
        ):
            sensitivity = self.context.adjustment_settings.sensitivity_level

        return ForecastModelConfig(hyperparameters={}, sensitivity=sensitivity)  # type: ignore[call-arg]

    def anomaly_config(self) -> AnomalyModelConfig:
        """Build default anomaly model config.

        Returns hyperparameters config. If sensitivity_level is set in
        adjustment_settings, it is passed to the config for model interpretation.
        Models will interpret sensitivity to set contamination or other parameters.

        Returns:
            Default AnomalyModelConfig with optional sensitivity.
        """
        sensitivity = None
        if (
            self.context.adjustment_settings
            and self.context.adjustment_settings.sensitivity_level is not None
        ):
            sensitivity = self.context.adjustment_settings.sensitivity_level

        return AnomalyModelConfig(hyperparameters={}, sensitivity=sensitivity)  # type: ignore[call-arg]

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

    def parallelization_kwargs(self) -> Dict[str, Any]:
        """Return parallelization kwargs for model training.

        Provides consistent parallelization settings across all models that support it.
        Defaults to 80% of available CPU cores, with override via environment variable.

        Returns:
            Dictionary with 'use_parallelization' and 'n_jobs' keys.
        """
        import os

        from datahub_executor.common.monitor.inference_v2.inference_utils import (
            get_inference_v2_n_jobs,
        )

        n_jobs = get_inference_v2_n_jobs()
        if n_jobs is None:
            # Default to 80% of cores (rounded down)
            cpu_count = os.cpu_count() or 1
            n_jobs = max(1, int(cpu_count * 0.8))

        return {
            "use_parallelization": True,
            "n_jobs": n_jobs,
        }


def get_defaults_for_context(context: InputDataContext) -> ObserveDefaultsBuilder:
    """Factory function to get defaults builder for an input data context.

    Args:
        context: The input data context describing data characteristics.

    Returns:
        An ObserveDefaultsBuilder configured for the given context.
    """
    return ObserveDefaultsBuilder(context)


def build_adjustment_settings_from_dict(
    settings_dict: Optional[Dict[str, Any]],
) -> Optional[AdjustmentSettings]:
    """Build AdjustmentSettings from AssertionAdjustmentSettings-like dict.

    Args:
        settings_dict: Dictionary with AssertionAdjustmentSettings fields.
            Expected keys:
            - sensitivity: {level: int}
            - exclusionWindows: [{type, fixedRange: {startTimeMillis, endTimeMillis}, ...}]
            - trainingDataLookbackWindowDays: int
            - context: adjustment context (optional dict)

    Returns:
        AdjustmentSettings or None if input is None/empty.
    """
    if not settings_dict:
        return None

    # Extract sensitivity level
    sensitivity_level = None
    sensitivity = settings_dict.get("sensitivity")
    if sensitivity and isinstance(sensitivity, dict):
        sensitivity_level = sensitivity.get("level")

    # Extract exclusion windows
    exclusion_windows: List[ExclusionWindow] = []
    raw_windows = settings_dict.get("exclusionWindows") or []
    if not isinstance(raw_windows, list):
        raw_windows = []
    for window in raw_windows:
        if isinstance(window, dict) and window.get("fixedRange"):
            fixed_range = window["fixedRange"]
            if (
                fixed_range.get("startTimeMillis") is not None
                and fixed_range.get("endTimeMillis") is not None
            ):
                exclusion_windows.append(
                    ExclusionWindow(
                        start_time_ms=int(fixed_range["startTimeMillis"]),
                        end_time_ms=int(fixed_range["endTimeMillis"]),
                        display_name=window.get("displayName"),
                    )
                )

    # Extract lookback days
    training_lookback_days = settings_dict.get("trainingDataLookbackWindowDays")
    if training_lookback_days is not None:
        try:
            training_lookback_days = int(training_lookback_days)  # type: ignore[call-overload, arg-type]
        except (ValueError, TypeError):
            training_lookback_days = None

    # Extract context
    context = settings_dict.get("context")
    if context is not None and not isinstance(context, dict):
        context = None

    return AdjustmentSettings(
        sensitivity_level=sensitivity_level,
        exclusion_windows=exclusion_windows,
        training_lookback_days=training_lookback_days,
        context=context,  # type: ignore
    )
