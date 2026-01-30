"""Tests for scoring module."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("datahub_observe")

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    DEFAULT_ANOMALY_SCORE_THRESHOLD,
    DEFAULT_DROP_THRESHOLD,
    compute_anomaly_score,
    compute_forecast_score,
    score_needs_retuning,
)


class TestComputeForecastScore:
    """Tests for compute_forecast_score function."""

    def test_compute_forecast_score_success(self) -> None:
        """compute_forecast_score successfully computes score from metrics."""
        metrics = {"mae": 10.0, "rmse": 12.0, "mape": 5.0, "coverage": 90.0}

        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.75
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_forecast_score(metrics, y_range=100.0)

            assert result == 0.75
            mock_scorer.compute_score.assert_called_once_with(metrics, y_range=100.0)

    def test_compute_forecast_score_without_y_range(self) -> None:
        """compute_forecast_score works without y_range."""
        metrics = {"mae": 10.0, "rmse": 12.0}

        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.8
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_forecast_score(metrics)

            assert result == 0.8
            mock_scorer.compute_score.assert_called_once_with(metrics, y_range=None)

    def test_compute_forecast_score_import_error(self) -> None:
        """compute_forecast_score raises TrainingErrorException on ImportError."""
        metrics = {"mae": 10.0}

        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer",
            side_effect=ImportError("Module not found"),
        ):
            with pytest.raises(TrainingErrorException) as excinfo:
                compute_forecast_score(metrics)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )
            assert "unavailable" in str(excinfo.value).lower()

    def test_compute_forecast_score_general_exception(self) -> None:
        """compute_forecast_score raises TrainingErrorException on general exception."""
        metrics = {"mae": 10.0}

        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_scorer.compute_score.side_effect = ValueError("Invalid metrics")
            mock_scorer_class.return_value = mock_scorer

            with pytest.raises(TrainingErrorException) as excinfo:
                compute_forecast_score(metrics, y_range=100.0)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )
            assert "Forecast scoring failed" in str(excinfo.value)
            assert "y_range" in excinfo.value.properties


class TestComputeAnomalyScore:
    """Tests for compute_anomaly_score function."""

    def test_compute_anomaly_score_success(self) -> None:
        """compute_anomaly_score successfully computes score from metrics."""
        metrics = {"precision": 0.9, "recall": 0.8, "f1_score": 0.85}

        with patch(
            "datahub_observe.algorithms.scoring.AnomalyScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.85
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_anomaly_score(metrics)

            assert result == 0.85
            mock_scorer.compute_score.assert_called_once_with(metrics)

    def test_compute_anomaly_score_without_ground_truth(self) -> None:
        """compute_anomaly_score works with metrics without ground truth."""
        metrics = {"anomaly_rate": 0.1, "mean_score": 0.5}

        with patch(
            "datahub_observe.algorithms.scoring.AnomalyScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.7
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_anomaly_score(metrics)

            assert result == 0.7

    def test_compute_anomaly_score_import_error(self) -> None:
        """compute_anomaly_score raises TrainingErrorException on ImportError."""
        metrics = {"precision": 0.9}

        with patch(
            "datahub_observe.algorithms.scoring.AnomalyScorer",
            side_effect=ImportError("Module not found"),
        ):
            with pytest.raises(TrainingErrorException) as excinfo:
                compute_anomaly_score(metrics)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )
            assert "unavailable" in str(excinfo.value).lower()

    def test_compute_anomaly_score_general_exception(self) -> None:
        """compute_anomaly_score raises TrainingErrorException on general exception."""
        metrics = {"precision": 0.9}

        with patch(
            "datahub_observe.algorithms.scoring.AnomalyScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_scorer.compute_score.side_effect = ValueError("Invalid metrics")
            mock_scorer_class.return_value = mock_scorer

            with pytest.raises(TrainingErrorException) as excinfo:
                compute_anomaly_score(metrics)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )
            assert "Anomaly scoring failed" in str(excinfo.value)
            assert "metrics_keys_present" in excinfo.value.properties


class TestScoreNeedsRetuning:
    """Tests for score_needs_retuning function."""

    def test_needs_retuning_below_threshold(self) -> None:
        """score_needs_retuning returns True when score below threshold."""
        result = score_needs_retuning(
            current_score=0.2, threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD
        )
        assert result is True

    def test_needs_retuning_above_threshold_no_previous(self) -> None:
        """score_needs_retuning returns False when above threshold and no previous score."""
        result = score_needs_retuning(
            current_score=0.8, threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD
        )
        assert result is False

    def test_needs_retuning_significant_drop(self) -> None:
        """score_needs_retuning returns True when score drops significantly."""
        result = score_needs_retuning(
            current_score=0.5,
            previous_score=0.8,
            threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=DEFAULT_DROP_THRESHOLD,
        )
        assert result is True  # 0.3 drop / 0.8 = 37.5% drop > 25% threshold

    def test_needs_retuning_small_drop(self) -> None:
        """score_needs_retuning returns False when score drops slightly."""
        result = score_needs_retuning(
            current_score=0.75,
            previous_score=0.8,
            threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=DEFAULT_DROP_THRESHOLD,
        )
        assert result is False  # 0.05 drop / 0.8 = 6.25% drop < 25% threshold

    def test_needs_retuning_improved_score(self) -> None:
        """score_needs_retuning returns False when score improves."""
        result = score_needs_retuning(
            current_score=0.9,
            previous_score=0.7,
            threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=DEFAULT_DROP_THRESHOLD,
        )
        assert result is False

    def test_needs_retuning_custom_threshold(self) -> None:
        """score_needs_retuning respects custom threshold."""
        result = score_needs_retuning(current_score=0.4, threshold=0.5)
        assert result is True

    def test_needs_retuning_custom_drop_threshold(self) -> None:
        """score_needs_retuning respects custom drop threshold."""
        result = score_needs_retuning(
            current_score=0.6,
            previous_score=0.8,
            threshold=0.3,
            drop_threshold=0.1,  # 10% drop threshold
        )
        assert result is True  # 0.2 drop / 0.8 = 25% drop > 10% threshold

    def test_needs_retuning_previous_score_zero(self) -> None:
        """score_needs_retuning handles previous score of zero."""
        result = score_needs_retuning(
            current_score=0.5,
            previous_score=0.0,
            threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=DEFAULT_DROP_THRESHOLD,
        )
        # Should only check threshold, not drop (division by zero avoided)
        assert result is False  # 0.5 > 0.5 threshold


class TestScoreNeedsRetuningBoundary:
    """Tests for score_needs_retuning boundary conditions."""

    def test_exactly_at_threshold(self) -> None:
        """score_needs_retuning returns False when exactly at threshold (not below)."""
        result = score_needs_retuning(
            current_score=0.5,
            threshold=0.5,  # Exactly at threshold
        )
        assert result is False  # current_score < threshold is False when equal

    def test_just_below_threshold(self) -> None:
        """score_needs_retuning returns True when just below threshold."""
        result = score_needs_retuning(current_score=0.499, threshold=0.5)
        assert result is True

    def test_just_above_threshold(self) -> None:
        """score_needs_retuning returns False when just above threshold."""
        result = score_needs_retuning(current_score=0.501, threshold=0.5)
        assert result is False

    def test_exactly_at_drop_threshold(self) -> None:
        """score_needs_retuning returns True when exactly at drop threshold."""
        result = score_needs_retuning(
            current_score=0.6,  # 0.2 drop from 0.8 = 25% drop
            previous_score=0.8,
            threshold=0.3,
            drop_threshold=0.25,  # Exactly 25% drop
        )
        assert result is True

    def test_just_below_drop_threshold(self) -> None:
        """score_needs_retuning returns True when just below drop threshold."""
        result = score_needs_retuning(
            current_score=0.59,  # 0.21 drop from 0.8 = 26.25% drop
            previous_score=0.8,
            threshold=0.3,
            drop_threshold=0.25,
        )
        assert result is True

    def test_just_above_drop_threshold(self) -> None:
        """score_needs_retuning returns False when just above drop threshold."""
        result = score_needs_retuning(
            current_score=0.61,  # 0.19 drop from 0.8 = 23.75% drop
            previous_score=0.8,
            threshold=0.3,
            drop_threshold=0.25,
        )
        assert result is False

    def test_negative_previous_score(self) -> None:
        """score_needs_retuning handles negative previous_score gracefully."""
        # Should only check threshold, not drop (previous_score <= 0)
        result = score_needs_retuning(
            current_score=0.6,
            previous_score=-0.1,
            threshold=0.5,
            drop_threshold=0.25,
        )
        assert result is False  # 0.6 > 0.5 threshold

    def test_current_score_greater_than_previous(self) -> None:
        """score_needs_retuning returns False when score improves."""
        result = score_needs_retuning(
            current_score=0.9,
            previous_score=0.7,
            threshold=0.5,
            drop_threshold=0.25,
        )
        assert result is False  # Improvement should not trigger retune


class TestComputeForecastScoreEdgeCases:
    """Tests for compute_forecast_score edge cases."""

    def test_empty_metrics_dict(self) -> None:
        """compute_forecast_score handles empty metrics dict."""
        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.0
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_forecast_score({}, y_range=100.0)

            assert result == 0.0
            mock_scorer.compute_score.assert_called_once_with({}, y_range=100.0)

    def test_zero_y_range(self) -> None:
        """compute_forecast_score handles zero y_range."""
        metrics = {"mae": 10.0, "rmse": 12.0}

        with patch(
            "datahub_observe.algorithms.scoring.ForecastScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.5
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_forecast_score(metrics, y_range=0.0)

            assert result == 0.5
            mock_scorer.compute_score.assert_called_once_with(metrics, y_range=0.0)


class TestComputeAnomalyScoreEdgeCases:
    """Tests for compute_anomaly_score edge cases."""

    def test_empty_metrics_dict(self) -> None:
        """compute_anomaly_score handles empty metrics dict."""
        with patch(
            "datahub_observe.algorithms.scoring.AnomalyScorer"
        ) as mock_scorer_class:
            mock_scorer = MagicMock()
            mock_score = MagicMock()
            mock_score.value = 0.0
            mock_scorer.compute_score.return_value = mock_score
            mock_scorer_class.return_value = mock_scorer

            result = compute_anomaly_score({})

            assert result == 0.0
            mock_scorer.compute_score.assert_called_once_with({})
