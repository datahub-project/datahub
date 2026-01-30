"""Tests for evaluator module re-exports."""

import pytest

pytest.importorskip("datahub_observe")

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    AnomalyEvaluationResult,
    CombinationEvaluationResult,
    EvaluationResult,
    ForecastEvaluationResult,
    TuningDecision,
    TuningReason,
    compute_anomaly_score,
    compute_evaluation_metrics,
    compute_forecast_score,
    make_tuning_decision,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics import (
    compute_evaluation_metrics as metrics_compute_evaluation_metrics,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    compute_anomaly_score as scoring_compute_anomaly_score,
    compute_forecast_score as scoring_compute_forecast_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.tuning_policy import (
    TuningDecision as policy_TuningDecision,
    TuningReason as policy_TuningReason,
    make_tuning_decision as policy_make_tuning_decision,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.types import (
    AnomalyEvaluationResult as types_AnomalyEvaluationResult,
    CombinationEvaluationResult as types_CombinationEvaluationResult,
    EvaluationResult as types_EvaluationResult,
    ForecastEvaluationResult as types_ForecastEvaluationResult,
)


class TestEvaluatorReExports:
    """Tests that evaluator module correctly re-exports from submodules."""

    def test_compute_evaluation_metrics_exported(self) -> None:
        """compute_evaluation_metrics is exported and points to correct function."""
        assert compute_evaluation_metrics is not None
        assert callable(compute_evaluation_metrics)
        # Verify it's the same function from metrics module
        assert compute_evaluation_metrics is metrics_compute_evaluation_metrics

    def test_compute_forecast_score_exported(self) -> None:
        """compute_forecast_score is exported and points to correct function."""
        assert compute_forecast_score is not None
        assert callable(compute_forecast_score)
        # Verify it's the same function from scoring module
        assert compute_forecast_score is scoring_compute_forecast_score

    def test_compute_anomaly_score_exported(self) -> None:
        """compute_anomaly_score is exported and points to correct function."""
        assert compute_anomaly_score is not None
        assert callable(compute_anomaly_score)
        # Verify it's the same function from scoring module
        assert compute_anomaly_score is scoring_compute_anomaly_score

    def test_tuning_decision_exported(self) -> None:
        """TuningDecision is exported and points to correct class."""
        assert TuningDecision is not None
        # Verify it's the same class from tuning_policy module
        assert TuningDecision is policy_TuningDecision
        # Verify it can be instantiated
        decision = TuningDecision(
            should_retune_forecast=False,
            should_retune_anomaly=False,
            forecast_reason=TuningReason.NOT_NEEDED,
            anomaly_reason=TuningReason.NOT_NEEDED,
        )
        assert decision.should_retune_forecast is False
        assert decision.should_retune_anomaly is False

    def test_tuning_reason_exported(self) -> None:
        """TuningReason is exported and points to correct enum."""
        assert TuningReason is not None
        # Verify it's the same enum from tuning_policy module
        assert TuningReason is policy_TuningReason
        assert hasattr(TuningReason, "NOT_NEEDED")
        assert hasattr(TuningReason, "FORCED")
        # Verify enum values work
        assert TuningReason.NOT_NEEDED is not None
        assert TuningReason.FORCED is not None

    def test_make_tuning_decision_exported(self) -> None:
        """make_tuning_decision is exported and points to correct function."""
        assert make_tuning_decision is not None
        assert callable(make_tuning_decision)
        # Verify it's the same function from tuning_policy module
        assert make_tuning_decision is policy_make_tuning_decision

    def test_evaluation_result_types_exported(self) -> None:
        """Evaluation result types are exported and point to correct classes."""
        assert EvaluationResult is not None
        assert ForecastEvaluationResult is not None
        assert AnomalyEvaluationResult is not None
        assert CombinationEvaluationResult is not None
        # Verify they point to correct source classes
        assert EvaluationResult is types_EvaluationResult
        assert ForecastEvaluationResult is types_ForecastEvaluationResult
        assert AnomalyEvaluationResult is types_AnomalyEvaluationResult
        assert CombinationEvaluationResult is types_CombinationEvaluationResult
