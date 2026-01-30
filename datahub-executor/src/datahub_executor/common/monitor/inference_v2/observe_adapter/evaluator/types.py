"""
Evaluation types and data structures.

Provides type definitions for evaluation results.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class EvaluationResult:
    """
    Base evaluation result container.

    Attributes:
        score: Normalized score (0.0 to 1.0, higher is better)
        raw_metrics: Original metrics dictionary from evaluate()
        success: Whether evaluation completed successfully
        error: Error message if evaluation failed
    """

    score: float
    raw_metrics: Dict[str, float] = field(default_factory=dict)
    success: bool = True
    error: Optional[str] = None


@dataclass
class ForecastEvaluationResult(EvaluationResult):
    """
    Forecast model evaluation result.

    Includes forecast-specific metrics and training artifacts.

    Attributes:
        mae: Mean Absolute Error
        rmse: Root Mean Squared Error
        mape: Mean Absolute Percentage Error
        coverage: Percentage of actuals within confidence intervals
        eval_df: DataFrame used for evaluation
        y_range: Range of y values (used for normalization)
    """

    mae: Optional[float] = None
    rmse: Optional[float] = None
    mape: Optional[float] = None
    coverage: Optional[float] = None
    eval_df: Optional[pd.DataFrame] = None
    y_range: Optional[float] = None

    @classmethod
    def from_metrics(
        cls,
        metrics: Dict[str, float],
        score: float,
        eval_df: Optional[pd.DataFrame] = None,
        forecast_model: Optional[object] = None,
    ) -> "ForecastEvaluationResult":
        """Create from raw metrics dictionary.

        Args:
            metrics: Raw evaluation metrics
            score: Computed normalized score
            eval_df: DataFrame used for evaluation
            forecast_model: Optional forecast model instance (for y_range from training)
        """
        # Prefer model.y_range from training data (guaranteed from raw training data)
        y_range = None
        if forecast_model is not None and hasattr(forecast_model, "y_range"):
            y_range = forecast_model.y_range
        elif eval_df is not None and "y" in eval_df.columns:
            # Fallback: calculate from eval_df if model.y_range not available
            y_range = eval_df["y"].max() - eval_df["y"].min()

        return cls(
            score=score,
            raw_metrics=metrics,
            mae=metrics.get("mae"),
            rmse=metrics.get("rmse"),
            mape=metrics.get("mape"),
            coverage=metrics.get("coverage"),
            eval_df=eval_df,
            y_range=y_range,
        )

    @classmethod
    def failed(cls, error: str) -> "ForecastEvaluationResult":
        """Create a failed result."""
        return cls(
            score=0.0,
            success=False,
            error=error,
        )


@dataclass
class AnomalyEvaluationResult(EvaluationResult):
    """
    Anomaly model evaluation result.

    Includes anomaly-specific metrics based on whether ground truth was available.

    Attributes:
        # With ground truth:
        precision: True positives / (True positives + False positives)
        recall: True positives / (True positives + False negatives)
        f1_score: Harmonic mean of precision and recall
        accuracy: (True positives + True negatives) / Total

        # Without ground truth:
        anomaly_rate: Percentage of points flagged as anomalies
        mean_score: Mean anomaly score
        max_score: Maximum anomaly score
        min_score: Minimum anomaly score

        has_ground_truth: Whether ground truth was used for evaluation
        best_score: Grid search best score (if available)
        eval_df: DataFrame used for evaluation
    """

    # With ground truth
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    accuracy: Optional[float] = None

    # Without ground truth
    anomaly_rate: Optional[float] = None
    mean_score: Optional[float] = None
    max_score: Optional[float] = None
    min_score: Optional[float] = None

    has_ground_truth: bool = False
    best_score: Optional[float] = None
    eval_df: Optional[pd.DataFrame] = None

    @classmethod
    def from_metrics(
        cls,
        metrics: Dict[str, float],
        score: float,
        has_ground_truth: bool,
        best_score: Optional[float] = None,
        eval_df: Optional[pd.DataFrame] = None,
    ) -> "AnomalyEvaluationResult":
        """Create from raw metrics dictionary."""
        result = cls(
            score=score,
            raw_metrics=metrics,
            has_ground_truth=has_ground_truth,
            best_score=best_score,
            eval_df=eval_df,
        )

        if has_ground_truth:
            result.precision = metrics.get("precision")
            result.recall = metrics.get("recall")
            result.f1_score = metrics.get("f1_score")
            result.accuracy = metrics.get("accuracy")
        else:
            result.anomaly_rate = metrics.get("anomaly_rate")
            result.mean_score = metrics.get("mean_score")
            result.max_score = metrics.get("max_score")
            result.min_score = metrics.get("min_score")

        return result

    @classmethod
    def failed(cls, error: str) -> "AnomalyEvaluationResult":
        """Create a failed result."""
        return cls(
            score=0.0,
            success=False,
            error=error,
        )


@dataclass
class CombinationEvaluationResult:
    """
    Result of evaluating a model pairing.

    Supports both forecast-based anomaly models (with forecast_model_key) and
    direct anomaly models (forecast_model_key is None).

    Attributes:
        combination_name: Name of the model pairing
        anomaly_model_key: Registry key for anomaly model (required)
        forecast_model_key: Registry key for forecast model (optional)
        forecast_result: Forecast evaluation result
        anomaly_result: Anomaly evaluation result
        combined_score: Combined score (weighted average or anomaly-only)
        success: Whether evaluation completed successfully
        errors: List of errors encountered
    """

    combination_name: str
    anomaly_model_key: str
    forecast_result: ForecastEvaluationResult
    anomaly_result: AnomalyEvaluationResult
    combined_score: float
    forecast_model_key: Optional[str] = None
    success: bool = True
    errors: List[str] = field(default_factory=list)

    @classmethod
    def compute_combined_score(
        cls,
        forecast_score: Optional[float],
        anomaly_score: float,
        forecast_weight: float = 0.4,
        anomaly_weight: float = 0.6,
    ) -> float:
        """
        Compute combined score from forecast and anomaly scores.

        For forecast-based models: weighted average (anomaly weighted higher).
        For direct anomaly models: anomaly score only.

        Args:
            forecast_score: Normalized forecast score (0-1), or None for direct anomaly models
            anomaly_score: Normalized anomaly score (0-1)
            forecast_weight: Weight for forecast score (default: 0.4)
            anomaly_weight: Weight for anomaly score (default: 0.6)

        Returns:
            Combined score (0-1)
        """
        if forecast_score is None:
            # Direct anomaly model - use anomaly score only
            return anomaly_score
        return forecast_weight * forecast_score + anomaly_weight * anomaly_score

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "combination_name": self.combination_name,
            "forecast_model_key": self.forecast_model_key,
            "anomaly_model_key": self.anomaly_model_key,
            "forecast_score": self.forecast_result.score,
            "anomaly_score": self.anomaly_result.score,
            "combined_score": self.combined_score,
            "success": self.success,
            "errors": self.errors,
            "forecast_metrics": self.forecast_result.raw_metrics,
            "anomaly_metrics": self.anomaly_result.raw_metrics,
        }
