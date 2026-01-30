from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
    evaluate_model_pairing,
    run_pairing_evaluation,
    select_best_combination,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    ModelPairing,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.tuning_policy import (
    TuningDecision,
    TuningReason,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.types import (
    AnomalyEvaluationResult,
    CombinationEvaluationResult,
    ForecastEvaluationResult,
)


def test_evaluate_model_pairing_passes_eval_df_to_evaluators() -> None:
    train_df = pd.DataFrame(
        {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
    )
    eval_df = pd.DataFrame(
        {"ds": pd.date_range("2024-01-11", periods=3), "y": range(3)}
    )

    forecast_model = MagicMock()
    anomaly_model = MagicMock()
    pairing = ModelPairing(
        anomaly_model="datahub_forecast_anomaly",
        forecast_model="prophet",
    )

    with (
        patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_forecast_model"
        ) as mock_eval_forecast,
        patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_anomaly_model"
        ) as mock_eval_anomaly,
    ):
        mock_eval_forecast.return_value = MagicMock(
            success=True, score=0.5, metrics={}, eval_df=eval_df
        )
        mock_eval_anomaly.return_value = MagicMock(
            success=True,
            score=0.5,
            metrics={},
            has_ground_truth=False,
            best_score=None,
            eval_df=eval_df,
        )

        _ = evaluate_model_pairing(
            train_df=train_df,
            eval_df=eval_df,
            forecast_model=forecast_model,
            anomaly_model=anomaly_model,
            pairing=pairing,
            ground_truth=None,
        )

        assert mock_eval_forecast.call_args.kwargs["eval_df"] is eval_df
        assert mock_eval_anomaly.call_args.kwargs["eval_df"] is eval_df


def _make_test_df(n: int = 50) -> pd.DataFrame:
    """Create a test DataFrame with ds and y columns."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="h"),
            "y": range(n),
        }
    )


class TestEvaluateModelPairing:
    """Tests for evaluate_model_pairing function."""

    def test_evaluate_pairing_with_forecast_model(self) -> None:
        """evaluate_model_pairing evaluates pairing that requires forecast."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairing = ModelPairing(
            anomaly_model="test_anomaly", forecast_model="test_forecast"
        )

        mock_forecast = MagicMock()
        mock_anomaly = MagicMock()

        forecast_result = ForecastEvaluationResult(
            score=0.8, raw_metrics={"mae": 10.0}, success=True, error=None
        )
        anomaly_result = AnomalyEvaluationResult(
            score=0.9, raw_metrics={"precision": 0.85}, success=True, error=None
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_forecast_model"
            ) as mock_eval_forecast,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_anomaly_model"
            ) as mock_eval_anomaly,
        ):
            mock_eval_forecast.return_value = forecast_result
            mock_eval_anomaly.return_value = anomaly_result

            result = evaluate_model_pairing(
                train_df=train_df,
                eval_df=eval_df,
                forecast_model=mock_forecast,
                anomaly_model=mock_anomaly,
                pairing=pairing,
            )

            assert result.success is True
            assert result.forecast_result.score == 0.8
            assert result.anomaly_result.score == 0.9
            assert result.combined_score > 0

    def test_evaluate_pairing_without_forecast_model(self) -> None:
        """evaluate_model_pairing handles direct anomaly models."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairing = ModelPairing(anomaly_model="deepsvdd")

        mock_anomaly = MagicMock()

        anomaly_result = AnomalyEvaluationResult(
            score=0.9, raw_metrics={"precision": 0.85}, success=True, error=None
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_anomaly_model"
        ) as mock_eval_anomaly:
            mock_eval_anomaly.return_value = anomaly_result

            result = evaluate_model_pairing(
                train_df=train_df,
                eval_df=eval_df,
                forecast_model=None,
                anomaly_model=mock_anomaly,
                pairing=pairing,
            )

            assert result.success is True
            assert result.forecast_result.score == 1.0  # Placeholder
            assert result.anomaly_result.score == 0.9
            assert result.combined_score > 0

    def test_evaluate_pairing_forecast_failure(self) -> None:
        """evaluate_model_pairing handles forecast evaluation failure."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairing = ModelPairing(
            anomaly_model="test_anomaly", forecast_model="test_forecast"
        )

        mock_forecast = MagicMock()
        mock_anomaly = MagicMock()

        forecast_result = ForecastEvaluationResult.failed("Forecast failed")
        anomaly_result = AnomalyEvaluationResult(
            score=0.9, raw_metrics={}, success=True, error=None
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_forecast_model"
            ) as mock_eval_forecast,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_anomaly_model"
            ) as mock_eval_anomaly,
        ):
            mock_eval_forecast.return_value = forecast_result
            mock_eval_anomaly.return_value = anomaly_result

            result = evaluate_model_pairing(
                train_df=train_df,
                eval_df=eval_df,
                forecast_model=mock_forecast,
                anomaly_model=mock_anomaly,
                pairing=pairing,
            )

            assert result.success is False
            assert len(result.errors) > 0
            assert "Forecast" in result.errors[0]

    def test_evaluate_pairing_anomaly_failure(self) -> None:
        """evaluate_model_pairing handles anomaly evaluation failure."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairing = ModelPairing(
            anomaly_model="test_anomaly", forecast_model="test_forecast"
        )

        mock_forecast = MagicMock()
        mock_anomaly = MagicMock()

        forecast_result = ForecastEvaluationResult(
            score=0.8, raw_metrics={}, success=True, error=None
        )
        anomaly_result = AnomalyEvaluationResult.failed("Anomaly failed")

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_forecast_model"
            ) as mock_eval_forecast,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_anomaly_model"
            ) as mock_eval_anomaly,
        ):
            mock_eval_forecast.return_value = forecast_result
            mock_eval_anomaly.return_value = anomaly_result

            result = evaluate_model_pairing(
                train_df=train_df,
                eval_df=eval_df,
                forecast_model=mock_forecast,
                anomaly_model=mock_anomaly,
                pairing=pairing,
            )

            assert result.success is False
            assert len(result.errors) > 0
            assert "Anomaly" in result.errors[0]


class TestRunPairingEvaluation:
    """Tests for run_pairing_evaluation function."""

    def test_run_pairing_evaluation_empty_pairings(self) -> None:
        """run_pairing_evaluation raises when no pairings provided."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)

        with pytest.raises(TrainingErrorException) as excinfo:
            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=[],
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.INVALID_PARAMETERS

    def test_run_pairing_evaluation_success(self) -> None:
        """run_pairing_evaluation successfully evaluates pairings."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.cls = MagicMock()
        mock_entry.cls.from_config = MagicMock(return_value=MagicMock())
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry
        mock_registry.create_forecast_model.return_value = MagicMock()
        mock_registry.create_anomaly_model.return_value = MagicMock()

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_model_pairing"
            ) as mock_eval_pairing,
        ):
            mock_result = CombinationEvaluationResult(
                combination_name="test",
                forecast_model_key="test_forecast",
                anomaly_model_key="test_anomaly",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            )
            mock_eval_pairing.return_value = mock_result

            results = run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

            assert len(results) == 1
            assert results[0].success is True
            assert results[0].combined_score == 0.85

    def test_run_pairing_evaluation_caches_forecast_models(self) -> None:
        """run_pairing_evaluation caches forecast models to avoid retraining."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="anomaly1", forecast_model="forecast1"),
            ModelPairing(
                anomaly_model="anomaly2", forecast_model="forecast1"
            ),  # Same forecast model
        ]

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.cls = MagicMock()
        mock_entry.cls.from_config = MagicMock(return_value=MagicMock())
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry
        mock_forecast = MagicMock()
        mock_registry.create_forecast_model.return_value = mock_forecast

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_model_pairing"
            ) as mock_eval_pairing,
        ):
            mock_result = CombinationEvaluationResult(
                combination_name="test",
                forecast_model_key="forecast1",
                anomaly_model_key="anomaly1",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            )
            mock_eval_pairing.return_value = mock_result

            results = run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

            # Forecast model should only be created once
            assert mock_registry.create_forecast_model.call_count == 1
            assert len(results) == 2

    def test_run_pairing_evaluation_direct_anomaly_model(self) -> None:
        """run_pairing_evaluation handles direct anomaly models without forecast."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [ModelPairing(anomaly_model="deepsvdd")]

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.metadata = {"requires_forecast_model": False}
        mock_registry.get.return_value = mock_entry
        mock_registry.create_anomaly_model.return_value = MagicMock()

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_model_pairing"
            ) as mock_eval_pairing,
        ):
            mock_result = CombinationEvaluationResult(
                combination_name="deepsvdd",
                forecast_model_key=None,
                anomaly_model_key="deepsvdd",
                forecast_result=ForecastEvaluationResult(
                    score=1.0, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.9,
                success=True,
                errors=[],
            )
            mock_eval_pairing.return_value = mock_result

            results = run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

            assert len(results) == 1
            # Should not create forecast model
            mock_registry.create_forecast_model.assert_not_called()

    def test_run_pairing_evaluation_handles_failure(self) -> None:
        """run_pairing_evaluation records failed result when a pairing fails; raises when all fail."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        mock_registry = MagicMock()
        mock_registry.create_forecast_model.side_effect = Exception("Training failed")

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            pytest.raises(TrainingErrorException) as excinfo,
        ):
            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
        assert "Training failed" in str(excinfo.value)

    def test_run_pairing_evaluation_all_failures_raises(self) -> None:
        """run_pairing_evaluation raises when all pairings fail."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        mock_registry = MagicMock()
        mock_registry.create_forecast_model.side_effect = Exception("Training failed")

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            pytest.raises(TrainingErrorException) as excinfo,
        ):
            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED

    def test_run_pairing_evaluation_with_tuning_decision(self) -> None:
        """run_pairing_evaluation respects tuning decision."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        tuning_decision = TuningDecision(
            should_retune_forecast=True,
            should_retune_anomaly=False,
            forecast_reason=TuningReason.FORCED,
            anomaly_reason=TuningReason.NOT_NEEDED,
        )

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.cls = MagicMock()
        mock_entry.cls.from_config = MagicMock(return_value=MagicMock())
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry
        mock_registry.create_forecast_model.return_value = MagicMock()

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_model_pairing"
            ) as mock_eval_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner._create_and_train_forecast_model"
            ) as mock_create_forecast,
        ):
            mock_result = CombinationEvaluationResult(
                combination_name="test",
                forecast_model_key="test_forecast",
                anomaly_model_key="test_anomaly",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            )
            mock_eval_pairing.return_value = mock_result

            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
                tuning_decision=tuning_decision,
            )

            # Verify tuning decision was passed
            assert mock_create_forecast.called
            call_kwargs = mock_create_forecast.call_args[1]
            assert call_kwargs["tune_hyperparameters"] is True

    def test_run_pairing_evaluation_with_progress_hook(self) -> None:
        """run_pairing_evaluation calls progress hook during evaluation."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        progress_calls = []

        def progress_hook(
            message: str,
            progress: float | None,
            step_name: str | None = None,
            step_result: object | None = None,
        ) -> None:
            progress_calls.append((message, progress))

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.cls = MagicMock()
        mock_entry.cls.from_config = MagicMock(return_value=MagicMock())
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry
        mock_registry.create_forecast_model.return_value = MagicMock()

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner.evaluate_model_pairing"
            ) as mock_eval_pairing,
        ):
            mock_result = CombinationEvaluationResult(
                combination_name="test",
                forecast_model_key="test_forecast",
                anomaly_model_key="test_anomaly",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            )
            mock_eval_pairing.return_value = mock_result

            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
                progress_hooks=progress_hook,
            )

            assert len(progress_calls) > 0

    def test_run_pairing_evaluation_requires_forecast_but_none_provided(self) -> None:
        """run_pairing_evaluation raises when forecast required but pairing has none."""
        train_df = _make_test_df(40)
        eval_df = _make_test_df(10)
        pairings = [ModelPairing(anomaly_model="test_anomaly")]

        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry

        with (
            patch(
                "datahub_observe.registry.get_model_registry",
                return_value=mock_registry,
            ),
            pytest.raises(TrainingErrorException) as excinfo,
        ):
            run_pairing_evaluation(
                train_df=train_df,
                eval_df=eval_df,
                pairings=pairings,
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.INVALID_PARAMETERS


class TestSelectBestCombination:
    """Tests for select_best_combination function."""

    def test_select_best_combination_success(self) -> None:
        """select_best_combination selects highest scoring result."""
        results = [
            CombinationEvaluationResult(
                combination_name="pairing1",
                forecast_model_key="f1",
                anomaly_model_key="a1",
                forecast_result=ForecastEvaluationResult(
                    score=0.7, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.75,
                success=True,
                errors=[],
            ),
            CombinationEvaluationResult(
                combination_name="pairing2",
                forecast_model_key="f2",
                anomaly_model_key="a2",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            ),
        ]

        best_result, index = select_best_combination(results)

        assert best_result.combined_score == 0.85
        assert index == 1

    def test_select_best_combination_filters_failed_results(self) -> None:
        """select_best_combination ignores failed results."""
        results = [
            CombinationEvaluationResult(
                combination_name="pairing1",
                forecast_model_key="f1",
                anomaly_model_key="a1",
                forecast_result=ForecastEvaluationResult.failed("Failed"),
                anomaly_result=AnomalyEvaluationResult.failed("Failed"),
                combined_score=0.0,
                success=False,
                errors=["Failed"],
            ),
            CombinationEvaluationResult(
                combination_name="pairing2",
                forecast_model_key="f2",
                anomaly_model_key="a2",
                forecast_result=ForecastEvaluationResult(
                    score=0.8, raw_metrics={}, success=True, error=None
                ),
                anomaly_result=AnomalyEvaluationResult(
                    score=0.9, raw_metrics={}, success=True, error=None
                ),
                combined_score=0.85,
                success=True,
                errors=[],
            ),
        ]

        best_result, index = select_best_combination(results)

        assert best_result.success is True
        assert best_result.combined_score == 0.85

    def test_select_best_combination_no_successful_results(self) -> None:
        """select_best_combination raises when no successful results."""
        results = [
            CombinationEvaluationResult(
                combination_name="pairing1",
                forecast_model_key="f1",
                anomaly_model_key="a1",
                forecast_result=ForecastEvaluationResult.failed("Failed"),
                anomaly_result=AnomalyEvaluationResult.failed("Failed"),
                combined_score=0.0,
                success=False,
                errors=["Failed"],
            ),
        ]

        with pytest.raises(ValueError, match="No successful"):
            select_best_combination(results)


class TestCreateAndTrainForecastModel:
    """Tests for _create_and_train_forecast_model function."""

    def test_create_and_train_forecast_model_success(self) -> None:
        """_create_and_train_forecast_model creates and trains forecast model."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
            _create_and_train_forecast_model,
        )

        train_df = _make_test_df(40)
        mock_registry = MagicMock()
        mock_model = MagicMock()
        mock_registry.create_forecast_model.return_value = mock_model

        result = _create_and_train_forecast_model(
            registry=mock_registry,
            forecast_model_key="test_forecast",
            train_df=train_df,
            preprocessing_config=MagicMock(),
            forecast_config=None,
            tune_hyperparameters=True,
        )

        assert result is mock_model
        mock_model.train.assert_called_once_with(train_df)

    def test_create_and_train_forecast_model_with_config(self) -> None:
        """_create_and_train_forecast_model uses provided config."""
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
            _create_and_train_forecast_model,
        )

        train_df = _make_test_df(40)
        mock_registry = MagicMock()
        mock_model = MagicMock()
        mock_registry.create_forecast_model.return_value = mock_model

        config = ForecastModelConfig(hyperparameters={"param": "value"})

        result = _create_and_train_forecast_model(
            registry=mock_registry,
            forecast_model_key="test_forecast",
            train_df=train_df,
            preprocessing_config=MagicMock(),
            forecast_config=config,
            tune_hyperparameters=False,
        )

        assert result is mock_model
        mock_registry.create_forecast_model.assert_called_once()


class TestCreateAndTrainAnomalyModel:
    """Tests for _create_and_train_anomaly_model function."""

    def test_create_and_train_anomaly_model_with_forecast(self) -> None:
        """_create_and_train_anomaly_model creates forecast-based anomaly model."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
            _create_and_train_anomaly_model,
        )

        train_df = _make_test_df(40)
        mock_forecast = MagicMock()
        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.cls = MagicMock()
        mock_anomaly = MagicMock()
        mock_entry.cls.from_config.return_value = mock_anomaly
        mock_entry.metadata = {"requires_forecast_model": True}
        mock_registry.get.return_value = mock_entry

        result = _create_and_train_anomaly_model(
            registry=mock_registry,
            anomaly_model_key="test_anomaly",
            forecast_model=mock_forecast,
            train_df=train_df,
            ground_truth=None,
            preprocessing_config=MagicMock(),
            anomaly_config=None,
            tune_hyperparameters=True,
        )

        assert result is mock_anomaly
        mock_anomaly.train.assert_called_once_with(train_df, ground_truth=None)
        mock_entry.cls.from_config.assert_called_once()

    def test_create_and_train_anomaly_model_direct(self) -> None:
        """_create_and_train_anomaly_model creates direct anomaly model."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
            _create_and_train_anomaly_model,
        )

        train_df = _make_test_df(40)
        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.metadata = {"requires_forecast_model": False}
        mock_anomaly = MagicMock()
        mock_registry.create_anomaly_model.return_value = mock_anomaly
        mock_registry.get.return_value = mock_entry

        result = _create_and_train_anomaly_model(
            registry=mock_registry,
            anomaly_model_key="deepsvdd",
            forecast_model=None,
            train_df=train_df,
            ground_truth=None,
            preprocessing_config=MagicMock(),
            anomaly_config=None,
            tune_hyperparameters=True,
        )

        assert result is mock_anomaly
        mock_anomaly.train.assert_called_once_with(train_df, ground_truth=None)
        mock_registry.create_anomaly_model.assert_called_once()

    def test_create_and_train_anomaly_model_with_ground_truth(self) -> None:
        """_create_and_train_anomaly_model passes ground_truth to train."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
            _create_and_train_anomaly_model,
        )

        train_df = _make_test_df(40)
        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [False] * 9 + [True],
            }
        )
        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.metadata = {"requires_forecast_model": False}
        mock_anomaly = MagicMock()
        mock_registry.create_anomaly_model.return_value = mock_anomaly
        mock_registry.get.return_value = mock_entry

        _create_and_train_anomaly_model(
            registry=mock_registry,
            anomaly_model_key="test_anomaly",
            forecast_model=None,
            train_df=train_df,
            ground_truth=ground_truth,
            preprocessing_config=MagicMock(),
            anomaly_config=None,
            tune_hyperparameters=True,
        )

        mock_anomaly.train.assert_called_once()
        call_args = mock_anomaly.train.call_args
        assert call_args.kwargs["ground_truth"] is ground_truth
        passed_df = call_args.args[0]
        assert "type" in passed_df.columns
        assert len(passed_df) == len(train_df)
