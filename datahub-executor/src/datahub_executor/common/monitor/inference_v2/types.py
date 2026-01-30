"""
Types for the V2 inference pipeline.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import pandas as pd

from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig

# Progress hook: (message, progress, step_name, step_result). step_name/step_result
# are set when a pipeline step completes; otherwise they are None.
ProgressHook = Callable[
    [str, Optional[float], Optional[str], Optional["StepResult"]], None
]


def normalize_progress_hooks(
    hooks: Optional[Union[ProgressHook, Sequence[ProgressHook]]],
) -> tuple[ProgressHook, ...]:
    """Normalize progress_hooks to a tuple of callables. None or single hook allowed."""
    if hooks is None:
        return ()
    if callable(hooks) and not isinstance(hooks, (list, tuple)):
        return (hooks,)
    return tuple(hooks)


@dataclass
class StepResult:
    """Result of a single pipeline step."""

    success: bool
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


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
        scores_only_persist: When True, caller should persist model_config (inference_details)
            so scores are stored for the next run, but should not persist predictions
            (e.g. pass embedded_assertions=[]). Used when quality is below threshold.
    """

    prediction_df: Optional[pd.DataFrame]
    model_config: ModelConfig
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    scores_only_persist: bool = False

    # Strict holdout evaluation artifacts (best-effort; may be omitted by callers).
    evaluation_df: Optional[pd.DataFrame] = None
    evaluation_forecast_df: Optional[pd.DataFrame] = None
    evaluation_detection_results: Optional[pd.DataFrame] = None
    forecast_evals: Optional[Dict[str, float]] = None
    anomaly_evals: Optional[Dict[str, float]] = None

    # Combination evaluation results (for displaying alternatives tab in UI).
    # List of CombinationEvaluationResult objects serialized as dicts.
    combination_results: Optional[List[Dict[str, Any]]] = None


class TrainingResultBuilder:
    """
    Default progress hook that accumulates step_results and builds TrainingResult.

    Implements ProgressHook so the pipeline can use it as the first hook; when
    step_name/step_result are provided, records them. At the end the pipeline
    calls set_final() and build() to produce the TrainingResult.
    """

    def __init__(self) -> None:
        self.step_results: Dict[str, StepResult] = {}
        self._prediction_df: Optional[pd.DataFrame] = None
        self._model_config: Optional[ModelConfig] = None
        self._scores_only_persist: bool = False
        self._evaluation_df: Optional[pd.DataFrame] = None
        self._evaluation_forecast_df: Optional[pd.DataFrame] = None
        self._evaluation_detection_results: Optional[pd.DataFrame] = None
        self._forecast_evals: Optional[Dict[str, float]] = None
        self._anomaly_evals: Optional[Dict[str, float]] = None
        self._combination_results: Optional[List[Dict[str, Any]]] = None

    def __call__(
        self,
        message: str,
        progress: Optional[float],
        step_name: Optional[str] = None,
        step_result: Optional[StepResult] = None,
    ) -> None:
        if step_name is not None and step_result is not None:
            self.step_results[step_name] = step_result

    def set_final(
        self,
        prediction_df: Optional[pd.DataFrame] = None,
        model_config: Optional[ModelConfig] = None,
        scores_only_persist: bool = False,
        evaluation_df: Optional[pd.DataFrame] = None,
        evaluation_forecast_df: Optional[pd.DataFrame] = None,
        evaluation_detection_results: Optional[pd.DataFrame] = None,
        forecast_evals: Optional[Dict[str, float]] = None,
        anomaly_evals: Optional[Dict[str, float]] = None,
        combination_results: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if prediction_df is not None:
            self._prediction_df = prediction_df
        if model_config is not None:
            self._model_config = model_config
        self._scores_only_persist = scores_only_persist
        if evaluation_df is not None:
            self._evaluation_df = evaluation_df
        if evaluation_forecast_df is not None:
            self._evaluation_forecast_df = evaluation_forecast_df
        if evaluation_detection_results is not None:
            self._evaluation_detection_results = evaluation_detection_results
        if forecast_evals is not None:
            self._forecast_evals = forecast_evals
        if anomaly_evals is not None:
            self._anomaly_evals = anomaly_evals
        if combination_results is not None:
            self._combination_results = combination_results

    def build(self) -> TrainingResult:
        if self._model_config is None:
            raise ValueError("model_config must be set via set_final() before build()")
        return TrainingResult(
            prediction_df=self._prediction_df,
            model_config=self._model_config,
            step_results=dict(self.step_results),
            scores_only_persist=self._scores_only_persist,
            evaluation_df=self._evaluation_df,
            evaluation_forecast_df=self._evaluation_forecast_df,
            evaluation_detection_results=self._evaluation_detection_results,
            forecast_evals=self._forecast_evals,
            anomaly_evals=self._anomaly_evals,
            combination_results=self._combination_results,
        )


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
    # Default prediction horizon is ~7 days at 1h frequency (168 intervals).
    num_intervals: int = 168
    interval_hours: int = 1
    sensitivity_level: int = 3
    floor_value: Optional[float] = None
    ceiling_value: Optional[float] = None
    assertion_category: str = "volume"
    metric_type: Optional[str] = None
    existing_model_config: Optional[ModelConfig] = None
    is_delta: Optional[bool] = None
    is_dataframe_cumulative: bool = False
    min_training_samples: int = 0
