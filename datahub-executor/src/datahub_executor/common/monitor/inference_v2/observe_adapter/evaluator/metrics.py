"""
Metrics computation for model evaluation.

Provides functions to compute evaluation metrics for forecast and anomaly models.
"""

import logging
from typing import TYPE_CHECKING, Dict, Optional, Tuple, Union

import pandas as pd

from datahub_executor.common.monitor.inference_v2.inference_utils import (
    split_time_series_df,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    compute_anomaly_score,
    compute_forecast_score,
    get_primary_anomaly_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.types import (
    AnomalyEvaluationResult,
    ForecastEvaluationResult,
)

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


def compute_evaluation_metrics(
    train_df: pd.DataFrame,
    forecast_model: Optional["DartsBaseForecastModel"],
    anomaly_model: Optional[Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]],
    ground_truth: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
) -> Tuple[Dict[str, float], Dict[str, float], pd.DataFrame]:
    """
    Compute evaluation metrics by calling evaluate() on validation portion.

    Replicates the same split that grid search uses internally (cv_split_ratio).
    Properly passes ground_truth to anomaly model evaluation.

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        forecast_model: Trained forecast model instance (optional).
        anomaly_model: Trained anomaly model instance (optional).
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.

    Returns:
        Tuple of (forecast_evals, anomaly_evals, eval_df):
        - forecast_evals: Dict with {mae, rmse, mape, coverage} or empty
        - anomaly_evals: Dict with classification metrics or score statistics
        - eval_df: The validation DataFrame used for evaluation
    """
    # NOTE:
    # For strict holdout evaluation, callers should pass an explicit eval_df that was
    # split from the full history. This avoids leakage: models are trained on the
    # train split only, and evaluated on eval_df only.
    if eval_df is None:
        cv_split_ratio = (
            getattr(anomaly_model, "cv_split_ratio", 0.7) if anomaly_model else 0.7
        )
        try:
            _, eval_df = split_time_series_df(train_df, train_ratio=cv_split_ratio)
        except ValueError as e:
            logger.warning(f"Failed to split data for evaluation: {e}")
            eval_df = train_df.sort_values("ds").reset_index(drop=True).iloc[0:0].copy()
    else:
        eval_df = eval_df.sort_values("ds").reset_index(drop=True)

    # Filter ground_truth to validation window timestamps
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
    forecast_evals: Dict[str, float] = {}
    if forecast_model is not None and forecast_model.is_trained:
        try:
            forecast_evals = forecast_model.evaluate(eval_df)
            logger.info(f"Forecast evaluation: {forecast_evals}")
        except Exception as e:
            logger.warning(f"Forecast evaluation failed: {e}")

    # Evaluate anomaly model
    anomaly_evals: Dict[str, float] = {}
    if anomaly_model is not None and getattr(anomaly_model, "is_trained", False):
        try:
            anomaly_evals = anomaly_model.evaluate(
                eval_df, ground_truth=eval_ground_truth
            )
            logger.info(f"Anomaly evaluation: {anomaly_evals}")
        except Exception as e:
            logger.warning(f"Anomaly evaluation failed: {e}")

    return forecast_evals, anomaly_evals, eval_df


def evaluate_forecast_model(
    train_df: pd.DataFrame,
    forecast_model: Optional["DartsBaseForecastModel"],
    eval_df: Optional[pd.DataFrame] = None,
    cv_split_ratio: float = 0.7,
) -> ForecastEvaluationResult:
    """
    Evaluate a trained forecast model.

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        forecast_model: Trained forecast model instance.
        cv_split_ratio: Ratio for train/eval split (default: 0.7).

    Returns:
        ForecastEvaluationResult with score and metrics.

    Raises:
        TrainingErrorException: If evaluation fails critically.
    """
    if forecast_model is None:
        return ForecastEvaluationResult.failed("No forecast model provided")

    if not forecast_model.is_trained:
        return ForecastEvaluationResult.failed("Forecast model is not trained")

    try:
        # Split data unless an explicit eval_df is provided.
        if eval_df is None:
            try:
                _, eval_df = split_time_series_df(train_df, train_ratio=cv_split_ratio)
            except ValueError:
                eval_df = pd.DataFrame()
        else:
            eval_df = eval_df.sort_values("ds").reset_index(drop=True)

        if len(eval_df) == 0:
            return ForecastEvaluationResult.failed(
                "No data available for evaluation after split"
            )

        # Evaluate
        metrics = forecast_model.evaluate(eval_df)

        # Get y_range for normalization - prefer model.y_range from training data
        # (guaranteed from raw training data, not test/eval data)
        y_range = None
        if hasattr(forecast_model, "y_range") and forecast_model.y_range is not None:
            y_range = forecast_model.y_range
        elif "y" in eval_df.columns:
            # Fallback: calculate from eval_df if model.y_range not available
            y_range = eval_df["y"].max() - eval_df["y"].min()

        # Compute normalized score
        score = compute_forecast_score(metrics, y_range=y_range)

        return ForecastEvaluationResult.from_metrics(
            metrics=metrics,
            score=score,
            eval_df=eval_df,
            forecast_model=forecast_model,  # Pass model for y_range access
        )

    except Exception as e:
        logger.warning(f"Forecast evaluation failed: {e}")
        return ForecastEvaluationResult.failed(str(e))


def evaluate_anomaly_model(
    train_df: pd.DataFrame,
    anomaly_model: Optional[Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]],
    ground_truth: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
    cv_split_ratio: float = 0.7,
) -> AnomalyEvaluationResult:
    """
    Evaluate a trained anomaly detection model.

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        anomaly_model: Trained anomaly model instance.
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
        cv_split_ratio: Ratio for train/eval split (default: 0.7).

    Returns:
        AnomalyEvaluationResult with score and metrics.

    Raises:
        TrainingErrorException: If evaluation fails critically.
    """
    if anomaly_model is None:
        return AnomalyEvaluationResult.failed("No anomaly model provided")

    if not getattr(anomaly_model, "is_trained", False):
        return AnomalyEvaluationResult.failed("Anomaly model is not trained")

    try:
        # Split data unless an explicit eval_df is provided.
        if eval_df is None:
            try:
                _, eval_df = split_time_series_df(train_df, train_ratio=cv_split_ratio)
            except ValueError:
                eval_df = pd.DataFrame()
        else:
            eval_df = eval_df.sort_values("ds").reset_index(drop=True)

        if len(eval_df) == 0:
            return AnomalyEvaluationResult.failed(
                "No data available for evaluation after split"
            )

        # Filter ground_truth to validation window
        eval_ground_truth = None
        has_ground_truth = False
        if ground_truth is not None:
            gt_df = ground_truth.copy()
            gt_df["ds"] = pd.to_datetime(gt_df["ds"])
            eval_dates = pd.to_datetime(eval_df["ds"])
            eval_ground_truth = gt_df[gt_df["ds"].isin(eval_dates)]
            if len(eval_ground_truth) == 0:
                eval_ground_truth = None
            else:
                has_ground_truth = True

        # Evaluate
        metrics = anomaly_model.evaluate(eval_df, ground_truth=eval_ground_truth)

        # Primary anomaly score: prefer GROUND_TRUTH then CLEAN_TEST when observe-models
        # exposes score types; else best_model_score from grid search.
        best_score = get_primary_anomaly_score(anomaly_model)

        # Compute normalized score
        score = compute_anomaly_score(metrics)

        return AnomalyEvaluationResult.from_metrics(
            metrics=metrics,
            score=score,
            has_ground_truth=has_ground_truth,
            best_score=best_score,
            eval_df=eval_df,
        )

    except Exception as e:
        logger.warning(f"Anomaly evaluation failed: {e}")
        return AnomalyEvaluationResult.failed(str(e))
