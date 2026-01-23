"""
Types for the V2 inference pipeline.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd

from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig


@dataclass
class StepResult:
    """Result of a single pipeline step."""

    success: bool
    error: Optional[str] = None


@dataclass
class TrainingResult:
    """
    Result of the observe-models training pipeline.

    This contains all outputs from the pipeline, designed for use with
    the persistence utilities (AnomalyAssertions).

    Attributes:
        prediction_df: DataFrame with predictions and detection bands, or None if
            prediction failed. Check step_results["prediction"] for failure details.
            Required columns: timestamp_ms, detection_band_lower, detection_band_upper
            Optional columns: y, yhat, yhat_lower, yhat_upper, anomaly_score, is_anomaly
        model_config: ModelConfig for persistence (contains all serialized configs).
        step_results: Dict tracking success/failure of each pipeline step.
            Keys: "preprocessing", "training", "prediction", etc.
    """

    prediction_df: Optional[pd.DataFrame]
    model_config: ModelConfig
    step_results: Dict[str, StepResult] = field(default_factory=dict)


@dataclass
class AssertionTrainingContext:
    """
    Context for training an assertion, used by v2 trainers.

    Attributes:
        entity_urn: URN of the entity being monitored.
        num_intervals: Number of future intervals to predict.
        interval_hours: Size of each interval in hours.
        sensitivity_level: Sensitivity level (1-10).
        floor_value: Optional minimum bound for predictions.
        ceiling_value: Optional maximum bound for predictions.
        assertion_category: Type of assertion ("volume", "field", "freshness").
        metric_type: For field assertions, the metric type (e.g., "NULL_COUNT").
        existing_model_config: Previously trained model config for warm start.
        is_delta: Whether data represents deltas/changes (True) or cumulative values
            (False). If True (default), negative values are allowed. If False,
            negative values will be filtered during preprocessing.
        is_dataframe_cumulative: Whether the input dataframe contains cumulative
            values (e.g., ROW_COUNT_TOTAL). If True, differencing will be applied.
    """

    entity_urn: str
    num_intervals: int = 48
    interval_hours: int = 1
    sensitivity_level: int = 3
    floor_value: Optional[float] = None
    ceiling_value: Optional[float] = None
    assertion_category: str = "volume"
    metric_type: Optional[str] = None
    existing_model_config: Optional[ModelConfig] = None
    is_delta: Optional[bool] = None
    is_dataframe_cumulative: bool = False
