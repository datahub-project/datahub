"""
Model quality evaluation utilities for observe-models integration.

This module provides functions to evaluate trained forecast and anomaly models
using the evaluate() methods from datahub_observe package.

Quality Score:
- Extracted from anomaly model's grid search `best_score` attribute
- F1 score (0-1) when ground_truth was provided during training
- Normalized -anomaly_count (0.5-1) when no ground_truth

Evaluation Metrics:
- Forecast model: {'mae', 'rmse', 'mape', 'coverage'}
- Anomaly model (without ground_truth): {'anomaly_rate', 'mean_score', 'max_score', 'min_score'}
- Anomaly model (with ground_truth): {'precision', 'recall', 'f1_score', 'accuracy'}

DISCREPANCIES WITH STREAMLIT EXPLORER:
======================================
This module differs from scripts/streamlit_explorer/model_explorer in several ways:

1. Split Timing:
   - Streamlit: Splits BEFORE training (true holdout), user-configurable 50-95%
   - Here: Splits AFTER training on full data, fixed 70% (cv_split_ratio)
   - Impact: Our forecast metrics are optimistic since model saw the data

2. Forecast Metrics:
   - Streamlit: Computes MAE/RMSE/MAPE manually, no coverage
   - Here: Uses forecast_model.evaluate() which includes coverage

3. Anomaly Matching:
   - Streamlit: Uses 1-hour time tolerance for matching detections to ground_truth
     (see anomaly_comparison.py:_compute_classification_metrics)
   - Here: Uses package's evaluate() which requires exact timestamp matching
   - Impact: Precision/recall may differ from streamlit explorer results

See: scripts/streamlit_explorer/model_explorer/model_training.py
     scripts/streamlit_explorer/model_explorer/anomaly_comparison.py
"""

import logging
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException

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

logger = logging.getLogger(__name__)

# Default minimum samples - matches anomaly_base.py min_samples_for_cv default
DEFAULT_MIN_SAMPLES_FOR_CV = 10


def extract_quality_score(
    train_df: pd.DataFrame,
    anomaly_model: Optional[Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]],
) -> float:
    """
    Extract quality score from anomaly model's grid search results.

    Uses best_score from grid search which represents actual held-out
    validation performance (F1 with ground_truth, or -anomaly_count without).

    This is the TRUE validation metric - computed during hyperparameter tuning
    on data that was held out from training at that time.

    Args:
        train_df: Training DataFrame (used to check minimum data requirement).
        anomaly_model: Trained anomaly model instance.

    Returns:
        Quality score between 0.0 and 1.0

    Raises:
        TrainingErrorException: If insufficient data to validate model quality
    """
    # Get min_samples from model or use default
    min_samples = getattr(
        anomaly_model, "min_samples_for_cv", DEFAULT_MIN_SAMPLES_FOR_CV
    )

    # FAIL FAST: Check data size before anything else
    if len(train_df) < min_samples:
        raise TrainingErrorException(
            message=(
                "Insufficient data for quality evaluation "
                f"({len(train_df)} < {min_samples} samples). "
                "Cannot validate model quality."
            ),
            error_type=MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT,
            properties={
                "sample_count": str(len(train_df)),
                "min_samples": str(min_samples),
            },
        )

    if anomaly_model is None:
        logger.warning("No anomaly model available for quality scoring.")
        return 1.0

    best_score = getattr(anomaly_model, "best_score", None)

    if best_score is not None:
        if best_score >= 0:
            # F1 score (with ground_truth) - already 0 to 1
            # This means ground_truth was provided during training and
            # grid search used F1 as the optimization target
            return float(best_score)
        else:
            # Negative anomaly count (without ground_truth)
            # Grid search minimizes detected anomalies when no ground truth
            # Normalize: 0 anomalies → 1.0, -20 or worse → 0.5
            return max(0.5, 1.0 + best_score / 40.0)

    # best_score is None - likely using cached params or no param_grid
    # This is acceptable behavior when reusing proven hyperparameters
    logger.info(
        "No grid search score available (using cached or default params). "
        "Proceeding with default confidence."
    )
    return 1.0


def compute_evaluation_metrics(
    train_df: pd.DataFrame,
    forecast_model: Optional["DartsBaseForecastModel"],
    anomaly_model: Optional[Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]],
    ground_truth: Optional[pd.DataFrame] = None,
) -> tuple[dict[str, float], dict[str, float], pd.DataFrame]:
    """
    Compute evaluation metrics by calling evaluate() on validation portion.

    Replicates the same split that grid search uses internally (cv_split_ratio).
    Properly passes ground_truth to anomaly model evaluation.

    NOTE: Discrepancies with streamlit_explorer/model_explorer approach:

    1. Split timing: Streamlit splits BEFORE training (true holdout), we split
       AFTER training on full data (optimistic for forecast metrics). We use
       the grid search split ratio for consistency with best_score computation.

    2. Forecast metrics: Streamlit computes MAE/RMSE/MAPE manually without
       coverage. We use forecast_model.evaluate() which includes coverage.

    3. Anomaly matching: Streamlit uses 1-hour time tolerance for matching
       detections to ground_truth (_compute_classification_metrics). We use
       package's evaluate() which requires exact timestamp matching. This may
       cause precision/recall to differ from streamlit explorer results.

    See: scripts/streamlit_explorer/model_explorer/model_training.py
         scripts/streamlit_explorer/model_explorer/anomaly_comparison.py

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        forecast_model: Trained forecast model instance (optional).
        anomaly_model: Trained anomaly model instance (optional).
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
            If provided, anomaly evaluation returns precision/recall/f1/accuracy.
            If not provided, returns anomaly_rate/mean_score/max_score/min_score.

    Returns:
        Tuple of (forecast_evals, anomaly_evals, eval_df):
        - forecast_evals: Dict with {mae, rmse, mape, coverage} or empty
        - anomaly_evals: Dict with classification metrics or score statistics
        - eval_df: The validation DataFrame used for evaluation
    """
    # Replicate grid search split (default cv_split_ratio=0.7)
    # Note: Streamlit uses user-configurable split (50-95%), we use fixed ratio
    # matching what grid search used to compute best_score
    cv_split_ratio = (
        getattr(anomaly_model, "cv_split_ratio", 0.7) if anomaly_model else 0.7
    )
    train_df_sorted = train_df.sort_values("ds").reset_index(drop=True)
    split_idx = int(len(train_df_sorted) * cv_split_ratio)
    eval_df = train_df_sorted.iloc[split_idx:].copy()

    # Filter ground_truth to validation window timestamps
    # This ensures we only evaluate on the portion that would have been
    # held out during grid search
    eval_ground_truth = None
    if ground_truth is not None:
        gt_df = ground_truth.copy()
        gt_df["ds"] = pd.to_datetime(gt_df["ds"])
        eval_dates = pd.to_datetime(eval_df["ds"])
        eval_ground_truth = gt_df[gt_df["ds"].isin(eval_dates)]
        if len(eval_ground_truth) == 0:
            logger.warning(
                "No ground_truth overlap with evaluation window. "
                "Anomaly evals will return score statistics instead of "
                "precision/recall/f1."
            )
            eval_ground_truth = None

    # Evaluate forecast model
    # Note: These are "optimistic" metrics since model was trained on full data
    # including this eval portion. The true holdout metric is best_score.
    forecast_evals: dict[str, float] = {}
    if forecast_model is not None and forecast_model.is_trained:
        try:
            forecast_evals = forecast_model.evaluate(eval_df)
            logger.info(f"Forecast evaluation: {forecast_evals}")
        except Exception as e:
            logger.warning(f"Forecast evaluation failed: {e}")

    # Evaluate anomaly model
    # Note: Uses exact timestamp matching, unlike streamlit's 1-hour tolerance
    # (see anomaly_comparison.py:_compute_classification_metrics tolerance_seconds=3600)
    anomaly_evals: dict[str, float] = {}
    if anomaly_model is not None and getattr(anomaly_model, "is_trained", False):
        try:
            # Pass ground_truth to get precision/recall/f1/accuracy
            # Without ground_truth, returns anomaly_rate/mean_score/max_score/min_score
            anomaly_evals = anomaly_model.evaluate(
                eval_df, ground_truth=eval_ground_truth
            )
            logger.info(f"Anomaly evaluation: {anomaly_evals}")
        except Exception as e:
            logger.warning(f"Anomaly evaluation failed: {e}")

    return forecast_evals, anomaly_evals, eval_df
