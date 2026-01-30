"""
Model quality evaluation utilities for observe-models integration.

DEPRECATED: This module is maintained for backwards compatibility.
Use the new evaluator package instead:
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
        compute_evaluation_metrics,
        TuningDecision,
        make_tuning_decision,
    )

Quality Score:
- Prefer using anomaly_score (see compute_anomaly_score / compute_evaluation_metrics)

Evaluation Metrics:
- Forecast model: {'mae', 'rmse', 'mape', 'coverage'}
- Anomaly model (without ground_truth): {'anomaly_rate', 'mean_score', 'max_score', 'min_score'}
- Anomaly model (with ground_truth): {'precision', 'recall', 'f1_score', 'accuracy'}
"""

# Re-export from new module for backwards compatibility
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics import (
    compute_evaluation_metrics,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    compute_anomaly_score,
    compute_forecast_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.tuning_policy import (
    TuningDecision,
    TuningReason,
    make_tuning_decision,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.types import (
    AnomalyEvaluationResult,
    CombinationEvaluationResult,
    EvaluationResult,
    ForecastEvaluationResult,
)

# Export all public API
__all__ = [
    # Legacy functions (maintained for backwards compatibility)
    "compute_evaluation_metrics",
    # Scoring functions
    "compute_forecast_score",
    "compute_anomaly_score",
    # Tuning policy
    "TuningDecision",
    "TuningReason",
    "make_tuning_decision",
    # Types
    "EvaluationResult",
    "ForecastEvaluationResult",
    "AnomalyEvaluationResult",
    "CombinationEvaluationResult",
]
