"""
Tuning policy for hyperparameter retuning decisions.

Provides logic to decide when models need hyperparameter retuning based on
scores, ground truth availability, and other factors.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    DEFAULT_ANOMALY_SCORE_THRESHOLD,
    DEFAULT_DROP_THRESHOLD,
    DEFAULT_FORECAST_SCORE_THRESHOLD,
    score_needs_retuning,
)

logger = logging.getLogger(__name__)


class TuningReason(Enum):
    """Reason for retuning decision."""

    NO_PREVIOUS_CONFIG = "no_previous_config"
    SCORE_BELOW_THRESHOLD = "score_below_threshold"
    SCORE_DROPPED = "score_dropped"
    GROUND_TRUTH_AVAILABLE = "ground_truth_available"
    FORCED = "forced"
    RECENT_FAILURE = "recent_failure"  # Recent training failure detected
    NOT_NEEDED = "not_needed"


@dataclass
class TuningDecision:
    """
    Decision about whether to retune forecast and/or anomaly models.

    Attributes:
        should_retune_forecast: Whether forecast model needs retuning
        should_retune_anomaly: Whether anomaly model needs retuning
        forecast_reason: Reason for forecast tuning decision
        anomaly_reason: Reason for anomaly tuning decision
        previous_forecast_score: Previous forecast score (if available)
        previous_anomaly_score: Previous anomaly score (if available)
        persist_even_if_not_improved: Whether to persist results even if
            retuning didn't improve scores (prevents infinite loops)
    """

    should_retune_forecast: bool
    should_retune_anomaly: bool
    forecast_reason: TuningReason
    anomaly_reason: TuningReason
    previous_forecast_score: Optional[float] = None
    previous_anomaly_score: Optional[float] = None
    persist_even_if_not_improved: bool = True  # Default True to prevent loops

    def any_retuning_needed(self) -> bool:
        """Check if any retuning is needed."""
        return self.should_retune_forecast or self.should_retune_anomaly


def make_tuning_decision(
    current_forecast_score: Optional[float] = None,
    current_anomaly_score: Optional[float] = None,
    previous_forecast_score: Optional[float] = None,
    previous_anomaly_score: Optional[float] = None,
    ground_truth: Optional[pd.DataFrame] = None,
    force_retune_forecast: bool = False,
    force_retune_anomaly: bool = False,
    has_previous_config: bool = True,
    forecast_score_threshold: float = DEFAULT_FORECAST_SCORE_THRESHOLD,
    anomaly_score_threshold: float = DEFAULT_ANOMALY_SCORE_THRESHOLD,
    drop_threshold: float = DEFAULT_DROP_THRESHOLD,
) -> TuningDecision:
    """
    Make a decision about whether to retune forecast and/or anomaly models.

    Decision logic:
    1. Force retune if explicitly requested
    2. Retune if no previous config exists
    3. Retune forecast if score below threshold or dropped significantly
    4. ALWAYS retune anomaly if ground truth with anomalies is available
    5. Otherwise, retune anomaly if score below threshold or dropped

    IMPORTANT: Results should be persisted even if retuning doesn't improve
    scores, to prevent infinite retuning loops.

    Args:
        current_forecast_score: Current forecast model score (if evaluated)
        current_anomaly_score: Current anomaly model score (if evaluated)
        previous_forecast_score: Previous forecast score from last persisted run's
            stored config (inference_details).
        previous_anomaly_score: Previous anomaly score from last persisted run's
            stored config (inference_details).
        ground_truth: DataFrame with 'ds' and 'is_anomaly_gt' columns
        force_retune_forecast: Explicitly force forecast retuning
        force_retune_anomaly: Explicitly force anomaly retuning
        has_previous_config: Whether a previous model config exists
        forecast_score_threshold: Minimum acceptable forecast score (default: 0.3)
        anomaly_score_threshold: Minimum acceptable anomaly score (default: 0.5)
        drop_threshold: Relative drop threshold for triggering retune

    Returns:
        TuningDecision with recommendations for both models
    """
    # Initialize decisions
    should_retune_forecast = False
    should_retune_anomaly = False
    forecast_reason = TuningReason.NOT_NEEDED
    anomaly_reason = TuningReason.NOT_NEEDED

    # Check if ground truth has anomalies
    has_gt_anomalies = _has_ground_truth_anomalies(ground_truth)

    # Check for recent failures
    has_recent_failure = _has_recent_failure(
        previous_forecast_score, previous_anomaly_score
    )

    # ---- Forecast Model Decision ----

    if force_retune_forecast:
        should_retune_forecast = True
        forecast_reason = TuningReason.FORCED
    elif not has_previous_config:
        should_retune_forecast = True
        forecast_reason = TuningReason.NO_PREVIOUS_CONFIG
    elif has_recent_failure and previous_forecast_score == 0.0:
        # Recent failure detected - still allow retuning but mark the reason
        # Note: We don't skip retuning here because data might have changed,
        # but we log it for visibility
        should_retune_forecast = True
        forecast_reason = TuningReason.RECENT_FAILURE
        logger.info(
            "Recent forecast model failure detected (score=0.0). "
            "Allowing retuning in case data characteristics have changed."
        )
    elif current_forecast_score is not None:
        # Check score-based triggers
        if current_forecast_score < forecast_score_threshold:
            should_retune_forecast = True
            forecast_reason = TuningReason.SCORE_BELOW_THRESHOLD
        elif score_needs_retuning(
            current_forecast_score,
            previous_forecast_score,
            forecast_score_threshold,
            drop_threshold,
        ):
            should_retune_forecast = True
            forecast_reason = TuningReason.SCORE_DROPPED

    # ---- Anomaly Model Decision ----

    if force_retune_anomaly:
        should_retune_anomaly = True
        anomaly_reason = TuningReason.FORCED
    elif has_gt_anomalies:
        # ALWAYS retune anomaly model when ground truth anomalies are available
        # This ensures the model learns from the latest feedback
        should_retune_anomaly = True
        anomaly_reason = TuningReason.GROUND_TRUTH_AVAILABLE
        logger.info("Ground truth anomalies detected - forcing anomaly model retuning")
    elif not has_previous_config:
        should_retune_anomaly = True
        anomaly_reason = TuningReason.NO_PREVIOUS_CONFIG
    elif has_recent_failure and previous_anomaly_score == 0.0:
        # Recent failure detected - still allow retuning but mark the reason
        # Note: We don't skip retuning here because data might have changed,
        # but we log it for visibility
        should_retune_anomaly = True
        anomaly_reason = TuningReason.RECENT_FAILURE
        logger.info(
            "Recent anomaly model failure detected (score=0.0). "
            "Allowing retuning in case data characteristics have changed."
        )
    elif current_anomaly_score is not None:
        # Check score-based triggers
        if current_anomaly_score < anomaly_score_threshold:
            should_retune_anomaly = True
            anomaly_reason = TuningReason.SCORE_BELOW_THRESHOLD
        elif score_needs_retuning(
            current_anomaly_score,
            previous_anomaly_score,
            anomaly_score_threshold,
            drop_threshold,
        ):
            should_retune_anomaly = True
            anomaly_reason = TuningReason.SCORE_DROPPED

    return TuningDecision(
        should_retune_forecast=should_retune_forecast,
        should_retune_anomaly=should_retune_anomaly,
        forecast_reason=forecast_reason,
        anomaly_reason=anomaly_reason,
        previous_forecast_score=previous_forecast_score,
        previous_anomaly_score=previous_anomaly_score,
        persist_even_if_not_improved=True,  # Always persist to prevent loops
    )


def _has_recent_failure(
    previous_forecast_score: Optional[float],
    previous_anomaly_score: Optional[float],
) -> bool:
    """
    Check if there was a recent training failure based on scores.

    A failure is indicated by scores of 0.0, which is set by build_failed_model_config()
    when training fails and hyperparameters are persisted.

    Args:
        previous_forecast_score: Previous forecast score from stored config.
        previous_anomaly_score: Previous anomaly score from stored config.

    Returns:
        True if either score indicates a recent failure (score = 0.0).
    """
    # Check if scores indicate failure (0.0 is used by build_failed_model_config)
    forecast_failed = (
        previous_forecast_score is not None and previous_forecast_score == 0.0
    )
    anomaly_failed = (
        previous_anomaly_score is not None and previous_anomaly_score == 0.0
    )

    return forecast_failed or anomaly_failed


def _has_ground_truth_anomalies(ground_truth: Optional[pd.DataFrame]) -> bool:
    """
    Check if ground truth DataFrame contains any anomalies.

    Args:
        ground_truth: DataFrame with 'ds' and 'is_anomaly_gt' columns, or None

    Returns:
        True if ground truth contains at least one anomaly
    """
    if ground_truth is None:
        return False

    if "is_anomaly_gt" not in ground_truth.columns:
        return False

    # Check if any rows have is_anomaly_gt = True
    anomaly_count = ground_truth["is_anomaly_gt"].sum()
    return anomaly_count > 0
