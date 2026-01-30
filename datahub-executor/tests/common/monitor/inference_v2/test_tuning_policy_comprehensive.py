"""Comprehensive tests for tuning_policy module."""

import pandas as pd

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.tuning_policy import (
    TuningDecision,
    TuningReason,
    _has_ground_truth_anomalies,
    make_tuning_decision,
)


class TestTuningDecision:
    """Tests for TuningDecision dataclass."""

    def test_tuning_decision_any_retuning_needed_true(self) -> None:
        """any_retuning_needed returns True when forecast retuning needed."""
        decision = TuningDecision(
            should_retune_forecast=True,
            should_retune_anomaly=False,
            forecast_reason=TuningReason.FORCED,
            anomaly_reason=TuningReason.NOT_NEEDED,
        )

        assert decision.any_retuning_needed() is True

    def test_tuning_decision_any_retuning_needed_anomaly(self) -> None:
        """any_retuning_needed returns True when anomaly retuning needed."""
        decision = TuningDecision(
            should_retune_forecast=False,
            should_retune_anomaly=True,
            forecast_reason=TuningReason.NOT_NEEDED,
            anomaly_reason=TuningReason.FORCED,
        )

        assert decision.any_retuning_needed() is True

    def test_tuning_decision_any_retuning_needed_false(self) -> None:
        """any_retuning_needed returns False when no retuning needed."""
        decision = TuningDecision(
            should_retune_forecast=False,
            should_retune_anomaly=False,
            forecast_reason=TuningReason.NOT_NEEDED,
            anomaly_reason=TuningReason.NOT_NEEDED,
        )

        assert decision.any_retuning_needed() is False


class TestMakeTuningDecision:
    """Tests for make_tuning_decision function."""

    def test_make_tuning_decision_force_retune_forecast(self) -> None:
        """make_tuning_decision retunes forecast when force_retune_forecast=True."""
        decision = make_tuning_decision(
            force_retune_forecast=True,
            has_previous_config=True,
        )

        assert decision.should_retune_forecast is True
        assert decision.forecast_reason == TuningReason.FORCED

    def test_make_tuning_decision_force_retune_anomaly(self) -> None:
        """make_tuning_decision retunes anomaly when force_retune_anomaly=True."""
        decision = make_tuning_decision(
            force_retune_anomaly=True,
            has_previous_config=True,
        )

        assert decision.should_retune_anomaly is True
        assert decision.anomaly_reason == TuningReason.FORCED

    def test_make_tuning_decision_no_previous_config(self) -> None:
        """make_tuning_decision retunes when no previous config exists."""
        decision = make_tuning_decision(has_previous_config=False)

        assert decision.should_retune_forecast is True
        assert decision.should_retune_anomaly is True
        assert decision.forecast_reason == TuningReason.NO_PREVIOUS_CONFIG
        assert decision.anomaly_reason == TuningReason.NO_PREVIOUS_CONFIG

    def test_make_tuning_decision_score_below_threshold_forecast(self) -> None:
        """make_tuning_decision retunes forecast when score below threshold."""
        decision = make_tuning_decision(
            current_forecast_score=0.2,  # Below default threshold of 0.3
            has_previous_config=True,
        )

        assert decision.should_retune_forecast is True
        assert decision.forecast_reason == TuningReason.SCORE_BELOW_THRESHOLD

    def test_make_tuning_decision_score_below_threshold_anomaly(self) -> None:
        """make_tuning_decision retunes anomaly when score below threshold."""
        decision = make_tuning_decision(
            current_anomaly_score=0.4,  # Below default threshold of 0.5
            has_previous_config=True,
        )

        assert decision.should_retune_anomaly is True
        assert decision.anomaly_reason == TuningReason.SCORE_BELOW_THRESHOLD

    def test_make_tuning_decision_score_dropped_forecast(self) -> None:
        """make_tuning_decision retunes forecast when score dropped significantly."""
        decision = make_tuning_decision(
            current_forecast_score=0.5,
            previous_forecast_score=0.8,  # Significant drop
            has_previous_config=True,
            drop_threshold=0.25,
        )

        assert decision.should_retune_forecast is True
        assert decision.forecast_reason == TuningReason.SCORE_DROPPED

    def test_make_tuning_decision_score_dropped_anomaly(self) -> None:
        """make_tuning_decision retunes anomaly when score dropped significantly."""
        # Use current above threshold (0.6) so reason is SCORE_DROPPED not SCORE_BELOW_THRESHOLD
        decision = make_tuning_decision(
            current_anomaly_score=0.6,
            previous_anomaly_score=0.9,  # Significant drop (0.3 > 0.25)
            has_previous_config=True,
            drop_threshold=0.25,
        )

        assert decision.should_retune_anomaly is True
        assert decision.anomaly_reason == TuningReason.SCORE_DROPPED

    def test_make_tuning_decision_ground_truth_anomalies(self) -> None:
        """make_tuning_decision always retunes anomaly when ground truth has anomalies."""
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [False] * 9 + [True],
            }
        )

        decision = make_tuning_decision(
            ground_truth=ground_truth,
            has_previous_config=True,
        )

        assert decision.should_retune_anomaly is True
        assert decision.anomaly_reason == TuningReason.GROUND_TRUTH_AVAILABLE

    def test_make_tuning_decision_no_retuning_needed(self) -> None:
        """make_tuning_decision does not retune when scores are good."""
        decision = make_tuning_decision(
            current_forecast_score=0.8,
            current_anomaly_score=0.9,
            previous_forecast_score=0.8,
            previous_anomaly_score=0.9,
            has_previous_config=True,
        )

        assert decision.should_retune_forecast is False
        assert decision.should_retune_anomaly is False
        assert decision.forecast_reason == TuningReason.NOT_NEEDED
        assert decision.anomaly_reason == TuningReason.NOT_NEEDED

    def test_make_tuning_decision_custom_thresholds(self) -> None:
        """make_tuning_decision respects custom score thresholds."""
        decision = make_tuning_decision(
            current_forecast_score=0.4,
            current_anomaly_score=0.6,
            has_previous_config=True,
            forecast_score_threshold=0.5,
            anomaly_score_threshold=0.7,
        )

        assert decision.should_retune_forecast is True
        assert decision.should_retune_anomaly is True

    def test_make_tuning_decision_persist_even_if_not_improved(self) -> None:
        """make_tuning_decision always sets persist_even_if_not_improved=True."""
        decision = make_tuning_decision(has_previous_config=False)

        assert decision.persist_even_if_not_improved is True

    def test_make_tuning_decision_stores_previous_scores(self) -> None:
        """make_tuning_decision stores previous scores in decision."""
        decision = make_tuning_decision(
            previous_forecast_score=0.7,
            previous_anomaly_score=0.8,
            has_previous_config=True,
        )

        assert decision.previous_forecast_score == 0.7
        assert decision.previous_anomaly_score == 0.8

    def test_make_tuning_decision_recent_failure_forecast(self) -> None:
        """make_tuning_decision detects recent forecast failure (score=0.0)."""
        decision = make_tuning_decision(
            previous_forecast_score=0.0,  # Indicates failure
            previous_anomaly_score=0.7,
            has_previous_config=True,
        )

        assert decision.should_retune_forecast is True
        assert decision.forecast_reason == TuningReason.RECENT_FAILURE

    def test_make_tuning_decision_recent_failure_anomaly(self) -> None:
        """make_tuning_decision detects recent anomaly failure (score=0.0)."""
        decision = make_tuning_decision(
            previous_forecast_score=0.8,
            previous_anomaly_score=0.0,  # Indicates failure
            has_previous_config=True,
        )

        assert decision.should_retune_anomaly is True
        assert decision.anomaly_reason == TuningReason.RECENT_FAILURE

    def test_make_tuning_decision_recent_failure_both(self) -> None:
        """make_tuning_decision detects recent failures for both models."""
        decision = make_tuning_decision(
            previous_forecast_score=0.0,  # Indicates failure
            previous_anomaly_score=0.0,  # Indicates failure
            has_previous_config=True,
        )

        assert decision.should_retune_forecast is True
        assert decision.should_retune_anomaly is True
        assert decision.forecast_reason == TuningReason.RECENT_FAILURE
        assert decision.anomaly_reason == TuningReason.RECENT_FAILURE


class TestHasGroundTruthAnomalies:
    """Tests for _has_ground_truth_anomalies function."""

    def test_has_ground_truth_anomalies_none(self) -> None:
        """_has_ground_truth_anomalies returns False for None."""
        result = _has_ground_truth_anomalies(None)

        assert result == False  # noqa: E712  # use == for numpy/pandas bool compatibility

    def test_has_ground_truth_anomalies_no_column(self) -> None:
        """_has_ground_truth_anomalies returns False when column missing."""
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
            }
        )

        result = _has_ground_truth_anomalies(ground_truth)

        assert result == False  # noqa: E712

    def test_has_ground_truth_anomalies_with_anomalies(self) -> None:
        """_has_ground_truth_anomalies returns True when anomalies present."""
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [False] * 9 + [True],
            }
        )

        result = _has_ground_truth_anomalies(ground_truth)

        assert result == True  # noqa: E712

    def test_has_ground_truth_anomalies_no_anomalies(self) -> None:
        """_has_ground_truth_anomalies returns False when no anomalies."""
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [False] * 10,
            }
        )

        result = _has_ground_truth_anomalies(ground_truth)

        assert result == False  # noqa: E712

    def test_has_ground_truth_anomalies_multiple_anomalies(self) -> None:
        """_has_ground_truth_anomalies returns True with multiple anomalies."""
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [True, False, True, False] * 2 + [False, False],
            }
        )

        result = _has_ground_truth_anomalies(ground_truth)

        assert result == True  # noqa: E712
