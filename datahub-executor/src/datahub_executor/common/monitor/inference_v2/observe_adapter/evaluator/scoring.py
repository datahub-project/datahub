"""
Scoring utilities for model evaluation.

Provides functions to compute normalized scores from model evaluation metrics.
Adapts the observe-models scoring module for use in the evaluator.

IMPORTANT: Policy decisions (thresholds, retuning decisions) are defined HERE
in the datahub-executor layer, not in observe-models. The observe-models
scoring module only computes confidence scores.
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Default minimum samples - matches anomaly_base.py min_samples_for_cv default
DEFAULT_MIN_SAMPLES_FOR_CV = 10

# =============================================================================
# Policy Thresholds (datahub-executor responsibility)
# =============================================================================
# These thresholds control when models are considered acceptable and when
# retuning is triggered. They are business logic, not ML evaluation metrics.
# All thresholds are configurable via environment variables.


def _env_to_float(varname: str, default_val: float) -> float:
    """Convert environment variable to float, returning default if not set or invalid."""
    val = os.environ.get(varname)
    if val is None:
        return default_val
    try:
        return float(val)
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid value for {varname}: {val}, using default {default_val}"
        )
        return default_val


# Minimum acceptable scores for models (0-1 scale)
# Below these thresholds, models should be retuned
# Configurable via DATAHUB_EXECUTOR_FORECAST_SCORE_THRESHOLD (default: 0.3)
DEFAULT_FORECAST_SCORE_THRESHOLD = _env_to_float(
    "DATAHUB_EXECUTOR_FORECAST_SCORE_THRESHOLD", 0.3
)
# Configurable via DATAHUB_EXECUTOR_ANOMALY_SCORE_THRESHOLD (default: 0.5)
DEFAULT_ANOMALY_SCORE_THRESHOLD = _env_to_float(
    "DATAHUB_EXECUTOR_ANOMALY_SCORE_THRESHOLD", 0.5
)

# Maximum relative drop from previous score before triggering retune
# 25% drop from previous score is considered significant degradation
# Configurable via DATAHUB_EXECUTOR_SCORE_DROP_THRESHOLD (default: 0.25)
DEFAULT_DROP_THRESHOLD = _env_to_float("DATAHUB_EXECUTOR_SCORE_DROP_THRESHOLD", 0.25)


def get_primary_anomaly_score(anomaly_model: Any) -> Optional[float]:
    """
    Extract a single comparable anomaly score from an anomaly model.

    observe-models 0.1.0.dev48+ distinguishes score types via AnomalyScoreKind
    (GROUND_TRUTH vs CLEAN_TEST). For inference_v2 comparison and persistence we
    use one "primary" score: prefer GROUND_TRUTH when available (labels-based,
    comparable across runs), else CLEAN_TEST (FPR/sensitivity). If the model only
    exposes best_model_score from grid search, that value is used.

    Args:
        anomaly_model: Trained anomaly model (BaseForecastAnomalyModel or BaseAnomalyModel).

    Returns:
        Primary score in [0, 1], or None if no score is available.
    """
    try:
        from datahub_observe.algorithms.scoring import (  # type: ignore[import-not-found]
            AnomalyScoreKind,
        )

        scores = getattr(anomaly_model, "scores", None)
        if isinstance(scores, dict):
            gt = scores.get(AnomalyScoreKind.GROUND_TRUTH)
            if gt is not None and hasattr(gt, "value"):
                return float(gt.value)
            clean = scores.get(AnomalyScoreKind.CLEAN_TEST)
            if clean is not None and hasattr(clean, "value"):
                return float(clean.value)
    except ImportError:
        pass

    best = getattr(anomaly_model, "best_model_score", None)
    if best is not None and hasattr(best, "value"):
        return float(best.value)
    return None


def compute_forecast_score(
    metrics: Dict[str, float],
    y_range: Optional[float] = None,
) -> float:
    """
    Compute normalized score from forecast model metrics.

    Args:
        metrics: Dictionary with forecast metrics.
            Expected: {'mae', 'rmse', 'mape', 'coverage'}
        y_range: Range of y values (max - min) for MAE/RMSE normalization.

    Returns:
        Normalized score between 0.0 and 1.0 (higher is better)
    """
    try:
        from datahub_observe.algorithms.scoring import (
            ForecastScorer,  # type: ignore[import-not-found]
        )

        scorer = ForecastScorer()
        model_score = scorer.compute_score(metrics, y_range=y_range)
        return model_score.value
    except ImportError:
        raise TrainingErrorException(
            message="observe-models scoring is unavailable (missing dependency)",
            error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            properties={"step": "scoring_forecast"},
        )
    except Exception as e:
        raise TrainingErrorException(
            message=f"Forecast scoring failed: {e}",
            error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            properties={
                "step": "scoring_forecast",
                "y_range": str(y_range),
                "metrics_keys_present": ",".join(sorted(metrics.keys())),
            },
        ) from e


def compute_anomaly_score(
    metrics: Dict[str, float],
) -> float:
    """
    Compute normalized score from anomaly model metrics.

    Args:
        metrics: Dictionary with anomaly metrics.
            With ground truth: {'precision', 'recall', 'f1_score', 'accuracy'}
            Without ground truth: {'anomaly_rate', 'mean_score', 'max_score', 'min_score'}

    Returns:
        Normalized score between 0.0 and 1.0 (higher is better)
    """
    try:
        from datahub_observe.algorithms.scoring import AnomalyScorer

        scorer = AnomalyScorer()
        model_score = scorer.compute_score(metrics)
        return model_score.value
    except ImportError:
        raise TrainingErrorException(
            message="observe-models scoring is unavailable (missing dependency)",
            error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            properties={"step": "scoring_anomaly"},
        )
    except Exception as e:
        raise TrainingErrorException(
            message=f"Anomaly scoring failed: {e}",
            error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            properties={
                "step": "scoring_anomaly",
                "metrics_keys_present": ",".join(sorted(metrics.keys())),
            },
        ) from e


def score_needs_retuning(
    current_score: float,
    previous_score: Optional[float] = None,
    threshold: float = DEFAULT_ANOMALY_SCORE_THRESHOLD,
    drop_threshold: float = DEFAULT_DROP_THRESHOLD,
) -> bool:
    """
    Check if model needs retuning based on score.

    Args:
        current_score: Current model score (0-1)
        previous_score: Previous model score (if available)
        threshold: Minimum acceptable score (default: 0.5)
        drop_threshold: Relative drop threshold (default: 0.25 = 25%)

    Returns:
        True if retuning is recommended
    """
    # Check threshold
    if current_score < threshold:
        return True

    # Check drop from previous
    if previous_score is not None and previous_score > 0:
        relative_drop = (previous_score - current_score) / previous_score
        if relative_drop > drop_threshold:
            return True

    return False
