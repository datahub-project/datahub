from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics import (
    compute_evaluation_metrics,
    evaluate_anomaly_model,
    evaluate_forecast_model,
)


def test_evaluate_forecast_model_uses_explicit_eval_df() -> None:
    df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)})
    eval_df = df.tail(3).copy()

    forecast_model = MagicMock()
    forecast_model.is_trained = True
    forecast_model.evaluate.return_value = {
        "mape": 1.0,
        "rmse": 1.0,
        "mae": 1.0,
        "coverage": 90.0,
    }
    forecast_model.y_range = 9.0

    res = evaluate_forecast_model(
        train_df=df, forecast_model=forecast_model, eval_df=eval_df
    )
    assert res.success is True
    forecast_model.evaluate.assert_called_once()

    called_eval_df = forecast_model.evaluate.call_args.args[0]
    assert len(called_eval_df) == len(eval_df)
    assert called_eval_df["ds"].is_monotonic_increasing


def test_evaluate_anomaly_model_uses_explicit_eval_df() -> None:
    df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)})
    eval_df = df.tail(3).copy()

    anomaly_model = MagicMock()
    anomaly_model.is_trained = True
    anomaly_model.evaluate.return_value = {
        "precision": 1.0,
        "recall": 1.0,
        "f1_score": 1.0,
    }
    anomaly_model.best_model_score = None

    res = evaluate_anomaly_model(
        train_df=df, anomaly_model=anomaly_model, eval_df=eval_df
    )
    assert res.success is True
    anomaly_model.evaluate.assert_called_once()

    called_eval_df = anomaly_model.evaluate.call_args.args[0]
    assert len(called_eval_df) == len(eval_df)
    assert called_eval_df["ds"].is_monotonic_increasing


def _make_test_df(n: int = 50) -> pd.DataFrame:
    """Create a test DataFrame with ds and y columns."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="h"),
            "y": range(n),
        }
    )


class TestComputeEvaluationMetrics:
    """Tests for compute_evaluation_metrics function."""

    def test_compute_evaluation_metrics_with_explicit_eval_df(self) -> None:
        """compute_evaluation_metrics uses explicit eval_df when provided."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {"mae": 10.0, "rmse": 12.0}

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9, "recall": 0.8}

        forecast_evals, anomaly_evals, result_eval_df = compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert forecast_evals == {"mae": 10.0, "rmse": 12.0}
        assert anomaly_evals == {"precision": 0.9, "recall": 0.8}
        assert len(result_eval_df) == 10
        mock_forecast.evaluate.assert_called_once()
        mock_anomaly.evaluate.assert_called_once()

    def test_compute_evaluation_metrics_splits_when_no_eval_df(self) -> None:
        """compute_evaluation_metrics splits data when eval_df not provided."""
        train_df = _make_test_df(50)
        mock_anomaly = MagicMock()
        mock_anomaly.cv_split_ratio = 0.7

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
        ) as mock_split:
            eval_df = train_df.tail(15)
            mock_split.return_value = (train_df.iloc[:-15], eval_df)

            mock_forecast = MagicMock()
            mock_forecast.is_trained = True
            mock_forecast.evaluate.return_value = {}

            mock_anomaly.is_trained = True
            mock_anomaly.evaluate.return_value = {}

            compute_evaluation_metrics(
                train_df=train_df,
                forecast_model=mock_forecast,
                anomaly_model=mock_anomaly,
            )

            mock_split.assert_called_once()

    def test_compute_evaluation_metrics_handles_split_failure(self) -> None:
        """compute_evaluation_metrics handles split failure gracefully."""
        train_df = _make_test_df(50)
        mock_anomaly = MagicMock()
        mock_anomaly.cv_split_ratio = 0.7

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
        ) as mock_split:
            mock_split.side_effect = ValueError("Split failed")

            mock_forecast = MagicMock()
            mock_forecast.is_trained = True
            # When eval_df is empty, evaluate returns empty dict
            mock_forecast.evaluate.return_value = {}

            mock_anomaly.is_trained = True
            mock_anomaly.evaluate.return_value = {}

            forecast_evals, anomaly_evals, eval_df = compute_evaluation_metrics(
                train_df=train_df,
                forecast_model=mock_forecast,
                anomaly_model=mock_anomaly,
            )

            assert len(eval_df) == 0
            assert forecast_evals == {}
            assert anomaly_evals == {}

    def test_compute_evaluation_metrics_filters_ground_truth(self) -> None:
        """compute_evaluation_metrics filters ground_truth to eval window."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=50, freq="h"),
                "is_anomaly_gt": [False] * 45 + [True] * 5,
            }
        )

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {}

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {}

        compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            ground_truth=ground_truth,
            eval_df=eval_df,
        )

        # Verify evaluate was called with filtered ground_truth
        call_kwargs = mock_anomaly.evaluate.call_args[1]
        assert "ground_truth" in call_kwargs
        eval_gt = call_kwargs["ground_truth"]
        assert len(eval_gt) <= len(eval_df)

    def test_compute_evaluation_metrics_no_ground_truth_overlap(self) -> None:
        """compute_evaluation_metrics handles no ground_truth overlap."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-02-01", periods=10, freq="h"),
                "is_anomaly_gt": [True] * 10,
            }
        )

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {}

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {}

        compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            ground_truth=ground_truth,
            eval_df=eval_df,
        )

        call_kwargs = mock_anomaly.evaluate.call_args[1]
        assert call_kwargs["ground_truth"] is None

    def test_compute_evaluation_metrics_forecast_not_trained(self) -> None:
        """compute_evaluation_metrics skips forecast when not trained."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = False

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {}

        forecast_evals, anomaly_evals, _ = compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert forecast_evals == {}
        mock_forecast.evaluate.assert_not_called()

    def test_compute_evaluation_metrics_anomaly_not_trained(self) -> None:
        """compute_evaluation_metrics skips anomaly when not trained."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {}

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = False

        forecast_evals, anomaly_evals, _ = compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert anomaly_evals == {}
        mock_anomaly.evaluate.assert_not_called()

    def test_compute_evaluation_metrics_handles_forecast_evaluation_failure(
        self,
    ) -> None:
        """compute_evaluation_metrics handles forecast evaluation failure."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.side_effect = Exception("Evaluation failed")

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {}

        forecast_evals, anomaly_evals, _ = compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert forecast_evals == {}
        assert anomaly_evals == {}

    def test_compute_evaluation_metrics_handles_anomaly_evaluation_failure(
        self,
    ) -> None:
        """compute_evaluation_metrics handles anomaly evaluation failure."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {}

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.side_effect = Exception("Evaluation failed")

        forecast_evals, anomaly_evals, _ = compute_evaluation_metrics(
            train_df=train_df,
            forecast_model=mock_forecast,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert forecast_evals == {}
        assert anomaly_evals == {}


class TestEvaluateForecastModel:
    """Tests for evaluate_forecast_model function."""

    def test_evaluate_forecast_model_success(self) -> None:
        """evaluate_forecast_model successfully evaluates trained model."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {"mae": 10.0, "rmse": 12.0}
        mock_forecast.y_range = 100.0

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_forecast_score"
        ) as mock_compute_score:
            mock_compute_score.return_value = 0.75

            result = evaluate_forecast_model(
                train_df=train_df,
                forecast_model=mock_forecast,
                eval_df=eval_df,
            )

            assert result.success is True
            assert result.score == 0.75
            mock_compute_score.assert_called_once()
            call_kwargs = mock_compute_score.call_args[1]
            assert call_kwargs["y_range"] == 100.0

    def test_evaluate_forecast_model_none_model(self) -> None:
        """evaluate_forecast_model returns failed result for None model."""
        train_df = _make_test_df(40)

        result = evaluate_forecast_model(
            train_df=train_df,
            forecast_model=None,
        )

        assert result.success is False
        assert result.error is not None
        assert "No forecast model" in result.error

    def test_evaluate_forecast_model_not_trained(self) -> None:
        """evaluate_forecast_model returns failed result for untrained model."""
        train_df = _make_test_df(40)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = False

        result = evaluate_forecast_model(
            train_df=train_df,
            forecast_model=mock_forecast,
        )

        assert result.success is False
        assert result.error is not None
        assert "not trained" in result.error

    def test_evaluate_forecast_model_splits_when_no_eval_df(self) -> None:
        """evaluate_forecast_model splits data when eval_df not provided."""
        train_df = _make_test_df(50)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {"mae": 10.0}
        mock_forecast.y_range = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_forecast_score"
            ) as mock_compute_score,
        ):
            eval_df = train_df.tail(15)
            mock_split.return_value = (train_df.iloc[:-15], eval_df)
            mock_compute_score.return_value = 0.75

            result = evaluate_forecast_model(
                train_df=train_df,
                forecast_model=mock_forecast,
            )

            assert result.success is True
            mock_split.assert_called_once()

    def test_evaluate_forecast_model_empty_eval_df_after_split(self) -> None:
        """evaluate_forecast_model returns failed when eval_df empty after split."""
        train_df = _make_test_df(5)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
        ) as mock_split:
            mock_split.side_effect = ValueError("Insufficient data")

            result = evaluate_forecast_model(
                train_df=train_df,
                forecast_model=mock_forecast,
            )

            assert result.success is False
            assert result.error is not None
            assert "No data available" in result.error

    def test_evaluate_forecast_model_uses_eval_df_y_range(self) -> None:
        """evaluate_forecast_model uses eval_df y_range when model y_range unavailable."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10).copy()
        eval_df["y"] = range(10, 20)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.return_value = {"mae": 10.0}
        mock_forecast.y_range = None

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_forecast_score"
        ) as mock_compute_score:
            mock_compute_score.return_value = 0.75

            result = evaluate_forecast_model(
                train_df=train_df,
                forecast_model=mock_forecast,
                eval_df=eval_df,
            )

            assert result.success is True
            call_kwargs = mock_compute_score.call_args[1]
            assert call_kwargs["y_range"] == 9.0  # max - min from eval_df

    def test_evaluate_forecast_model_handles_evaluation_failure(self) -> None:
        """evaluate_forecast_model handles evaluation failure gracefully."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_forecast = MagicMock()
        mock_forecast.is_trained = True
        mock_forecast.evaluate.side_effect = Exception("Evaluation failed")

        result = evaluate_forecast_model(
            train_df=train_df,
            forecast_model=mock_forecast,
            eval_df=eval_df,
        )

        assert result.success is False
        assert result.error is not None
        assert "Evaluation failed" in result.error


class TestEvaluateAnomalyModel:
    """Tests for evaluate_anomaly_model function."""

    def test_evaluate_anomaly_model_success(self) -> None:
        """evaluate_anomaly_model successfully evaluates trained model."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9, "recall": 0.8}
        mock_anomaly.best_model_score = None

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_anomaly_score"
        ) as mock_compute_score:
            mock_compute_score.return_value = 0.85

            result = evaluate_anomaly_model(
                train_df=train_df,
                anomaly_model=mock_anomaly,
                eval_df=eval_df,
            )

            assert result.success is True
            assert result.score == 0.85

    def test_evaluate_anomaly_model_none_model(self) -> None:
        """evaluate_anomaly_model returns failed result for None model."""
        train_df = _make_test_df(40)

        result = evaluate_anomaly_model(
            train_df=train_df,
            anomaly_model=None,
        )

        assert result.success is False
        assert result.error is not None
        assert "No anomaly model" in result.error

    def test_evaluate_anomaly_model_not_trained(self) -> None:
        """evaluate_anomaly_model returns failed result for untrained model."""
        train_df = _make_test_df(40)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = False

        result = evaluate_anomaly_model(
            train_df=train_df,
            anomaly_model=mock_anomaly,
        )

        assert result.success is False
        assert result.error is not None
        assert "not trained" in result.error

    def test_evaluate_anomaly_model_splits_when_no_eval_df(self) -> None:
        """evaluate_anomaly_model splits data when eval_df not provided."""
        train_df = _make_test_df(50)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9}
        mock_anomaly.best_model_score = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_anomaly_score"
            ) as mock_compute_score,
        ):
            eval_df = train_df.tail(15)
            mock_split.return_value = (train_df.iloc[:-15], eval_df)
            mock_compute_score.return_value = 0.85

            result = evaluate_anomaly_model(
                train_df=train_df,
                anomaly_model=mock_anomaly,
            )

            assert result.success is True
            mock_split.assert_called_once()

    def test_evaluate_anomaly_model_empty_eval_df_after_split(self) -> None:
        """evaluate_anomaly_model returns failed when eval_df empty after split."""
        train_df = _make_test_df(5)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.split_time_series_df"
        ) as mock_split:
            mock_split.side_effect = ValueError("Insufficient data")

            result = evaluate_anomaly_model(
                train_df=train_df,
                anomaly_model=mock_anomaly,
            )

            assert result.success is False
            assert result.error is not None
            assert "No data available" in result.error

    def test_evaluate_anomaly_model_filters_ground_truth(self) -> None:
        """evaluate_anomaly_model filters ground_truth to eval window."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=50, freq="h"),
                "is_anomaly_gt": [False] * 45 + [True] * 5,
            }
        )

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9, "recall": 0.8}
        mock_anomaly.best_model_score = None

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_anomaly_score"
        ) as mock_compute_score:
            mock_compute_score.return_value = 0.85

            result = evaluate_anomaly_model(
                train_df=train_df,
                anomaly_model=mock_anomaly,
                ground_truth=ground_truth,
                eval_df=eval_df,
            )

            assert result.success is True
            call_kwargs = mock_anomaly.evaluate.call_args[1]
            assert "ground_truth" in call_kwargs
            eval_gt = call_kwargs["ground_truth"]
            assert len(eval_gt) <= len(eval_df)

    def test_evaluate_anomaly_model_no_ground_truth_overlap(self) -> None:
        """evaluate_anomaly_model handles no ground_truth overlap."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-02-01", periods=10, freq="h"),
                "is_anomaly_gt": [True] * 10,
            }
        )

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9}
        mock_anomaly.best_model_score = None

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics.compute_anomaly_score"
        ) as mock_compute_score:
            mock_compute_score.return_value = 0.85

            result = evaluate_anomaly_model(
                train_df=train_df,
                anomaly_model=mock_anomaly,
                ground_truth=ground_truth,
                eval_df=eval_df,
            )

            assert result.success is True
            call_kwargs = mock_anomaly.evaluate.call_args[1]
            assert call_kwargs["ground_truth"] is None

    def test_evaluate_anomaly_model_uses_best_model_score(self) -> None:
        """evaluate_anomaly_model uses best_model_score when available."""
        from datahub_observe.algorithms.scoring import ModelScore, ScoreType

        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.return_value = {"precision": 0.9}
        mock_anomaly.best_model_score = ModelScore(
            value=0.88,
            score_type=ScoreType.ANOMALY,
            metric_name="f1_score",
            has_ground_truth=True,
            confidence=0.9,
        )

        result = evaluate_anomaly_model(
            train_df=train_df,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert result.success is True
        assert result.best_score == 0.88

    def test_evaluate_anomaly_model_handles_evaluation_failure(self) -> None:
        """evaluate_anomaly_model handles evaluation failure gracefully."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        mock_anomaly = MagicMock()
        mock_anomaly.is_trained = True
        mock_anomaly.evaluate.side_effect = Exception("Evaluation failed")

        result = evaluate_anomaly_model(
            train_df=train_df,
            anomaly_model=mock_anomaly,
            eval_df=eval_df,
        )

        assert result.success is False
        assert result.error is not None
        assert "Evaluation failed" in result.error
