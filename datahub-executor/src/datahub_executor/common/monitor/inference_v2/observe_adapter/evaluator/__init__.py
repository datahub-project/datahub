"""
Evaluator Module for observe-models integration.

This module provides comprehensive evaluation utilities for trained forecast and
anomaly models, including:

- Normalized scoring interface (ModelScore)
- Tuning decision logic (when to retune hyperparameters)
- Multi-model pairing evaluation
- Quality assessment and threshold checking

Public API:
===========

Scoring:
- compute_forecast_score() - Compute normalized forecast model score
- compute_anomaly_score() - Compute normalized anomaly model score

Evaluation:
- compute_evaluation_metrics() - Compute metrics for forecast and anomaly models
- evaluate_model_pairing() - Evaluate a single model pairing

Tuning Decisions:
- TuningDecision - Container for retuning decisions
- make_tuning_decision() - Decide if models need retuning

Model Pairings:
- ModelPairing - Simple forecast + anomaly model pairing (name + version)
- get_default_pairings() - Get default model pairings
- run_pairing_evaluation() - Evaluate multiple model pairings
- select_best_combination() - Select best pairing based on scores
"""

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
    evaluate_model_pairing,
    run_pairing_evaluation,
    select_best_combination,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    DEFAULT_MODEL_PAIRINGS,
    ModelPairing,
    get_all_pairings,
    get_default_pairings,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics import (
    compute_evaluation_metrics,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    DEFAULT_ANOMALY_SCORE_THRESHOLD,
    DEFAULT_DROP_THRESHOLD,
    DEFAULT_FORECAST_SCORE_THRESHOLD,
    compute_anomaly_score,
    compute_forecast_score,
    get_primary_anomaly_score,
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

__all__ = [
    # Types
    "EvaluationResult",
    "ForecastEvaluationResult",
    "AnomalyEvaluationResult",
    "CombinationEvaluationResult",
    # Scoring
    "compute_forecast_score",
    "compute_anomaly_score",
    "get_primary_anomaly_score",
    "DEFAULT_FORECAST_SCORE_THRESHOLD",
    "DEFAULT_ANOMALY_SCORE_THRESHOLD",
    "DEFAULT_DROP_THRESHOLD",
    # Metrics
    "compute_evaluation_metrics",
    # Tuning decisions
    "TuningDecision",
    "TuningReason",
    "make_tuning_decision",
    # Model pairings
    "ModelPairing",
    "DEFAULT_MODEL_PAIRINGS",
    "get_default_pairings",
    "get_all_pairings",
    # Pairing evaluation
    "run_pairing_evaluation",
    "select_best_combination",
    "evaluate_model_pairing",
]
