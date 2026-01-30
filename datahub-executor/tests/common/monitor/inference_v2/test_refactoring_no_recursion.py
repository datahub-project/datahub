"""Tests to verify refactoring: no recursion, no redundant work, hyperparameter persistence."""

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from unittest.mock import MagicMock, patch

from datahub_executor.common.monitor.inference_v2.observe_adapter.adapter import (
    ObserveAdapter,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    TuningDecision,
    TuningReason,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    ModelPairing,
)
from datahub_executor.common.monitor.inference_v2.types import TrainingResultBuilder


def _make_test_df(n: int = 50) -> pd.DataFrame:
    """Create a test DataFrame with ds and y columns."""
    return pd.DataFrame(
        {
            "ds": pd.date_range("2024-01-01", periods=n, freq="h"),
            "y": range(n),
        }
    )


class TestNoRecursion:
    """Verify that recursion has been eliminated."""

    def test_run_combination_evaluation_calls_train_single_combination_not_run_training_pipeline(
        self,
    ) -> None:
        """Verify _run_combination_evaluation calls _train_single_combination, not run_training_pipeline."""
        adapter = ObserveAdapter()
        df = _make_test_df(100)
        context = InputDataContext(assertion_category="volume")
        pairings = [
            ModelPairing(
                forecast_model="datahub",
                anomaly_model="datahub_forecast_anomaly",
            )
        ]

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.run_pairing_evaluation"
            ) as mock_run_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.select_best_combination"
            ) as mock_select,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch.object(
                adapter, "_train_single_combination", return_value=MagicMock()
            ) as mock_train_single,
            patch.object(adapter, "run_training_pipeline") as mock_run_training,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_result = MagicMock()
            mock_result.combined_score = 0.9
            mock_run_pairing.return_value = [mock_result]
            mock_select.return_value = (mock_result, 0)

            try:
                adapter._run_combination_evaluation(
                    df=df,
                    input_data_context=context,
                    num_intervals=7,
                    interval_hours=1,
                    force_retune_forecast=False,
                    force_retune_anomaly=False,
                    ground_truth=None,
                    existing_model_config=None,
                    model_combinations=pairings,
                    score_drop_threshold=0.25,
                    eval_train_ratio=None,
                    progress_hooks=None,
                )
            except Exception:
                # We expect it to fail during actual training, but we just want to verify
                # the call pattern
                pass

            # Verify _train_single_combination was called
            assert mock_train_single.called, (
                "_train_single_combination should be called"
            )
            # Verify run_training_pipeline was NOT called (no recursion)
            assert not mock_run_training.called, (
                "run_training_pipeline should NOT be called (recursion eliminated)"
            )


class TestNoRedundantWork:
    """Verify that tuning decisions are not recomputed unnecessarily."""

    def test_tuning_decision_passed_explicitly_not_recomputed(self) -> None:
        """Verify that when tuning_decision is passed, make_tuning_decision is not called again."""
        adapter = ObserveAdapter()
        df = _make_test_df(100)
        context = InputDataContext(assertion_category="volume")
        defaults = MagicMock()

        # Create a tuning decision to pass explicitly
        explicit_decision = TuningDecision(
            should_retune_forecast=False,
            should_retune_anomaly=False,
            forecast_reason=TuningReason.NOT_NEEDED,
            anomaly_reason=TuningReason.NOT_NEEDED,
        )

        with (
            patch.object(adapter, "_train_and_evaluate", return_value=MagicMock()),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.make_tuning_decision"
            ) as mock_make_decision,
        ):
            builder = TrainingResultBuilder()
            try:
                adapter._train_single_combination(
                    df=df,
                    input_data_context=context,
                    num_intervals=7,
                    interval_hours=1,
                    defaults=defaults,
                    tuning_decision=explicit_decision,  # Pass explicitly
                    existing_model_config=None,
                    ground_truth=None,
                    score_drop_threshold=0.25,
                    eval_train_ratio=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                )
            except Exception:
                # We expect it to fail during actual training, but we just want to verify
                # make_tuning_decision is not called
                pass

            # Verify make_tuning_decision was NOT called since we passed it explicitly
            assert not mock_make_decision.called, (
                "make_tuning_decision should NOT be called when tuning_decision is passed explicitly"
            )

    def test_tuning_decision_computed_when_not_provided(self) -> None:
        """Verify that when tuning_decision is None, it is computed."""
        adapter = ObserveAdapter()
        df = _make_test_df(100)
        context = InputDataContext(assertion_category="volume")
        defaults = MagicMock()
        builder = TrainingResultBuilder()

        with (
            patch.object(adapter, "_train_and_evaluate", return_value=MagicMock()),
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.make_tuning_decision"
            ) as mock_make_decision,
        ):
            mock_make_decision.return_value = TuningDecision(
                should_retune_forecast=True,
                should_retune_anomaly=True,
                forecast_reason=TuningReason.NO_PREVIOUS_CONFIG,
                anomaly_reason=TuningReason.NO_PREVIOUS_CONFIG,
            )

            try:
                adapter._train_single_combination(
                    df=df,
                    input_data_context=context,
                    num_intervals=7,
                    interval_hours=1,
                    defaults=defaults,
                    tuning_decision=None,  # Not provided - should be computed
                    existing_model_config=None,
                    ground_truth=None,
                    score_drop_threshold=0.25,
                    eval_train_ratio=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                )
            except Exception:
                # We expect it to fail during actual training
                pass

            # Verify make_tuning_decision WAS called since it was None
            assert mock_make_decision.called, (
                "make_tuning_decision should be called when tuning_decision is None"
            )


class TestHyperparameterPersistence:
    """Verify that hyperparameters are captured and persisted on failures."""

    def test_capture_attempted_hyperparameters(self) -> None:
        """Verify that _capture_attempted_hyperparameters extracts hyperparameters."""
        adapter = ObserveAdapter()

        # Create mock models with hyperparameters
        mock_forecast_config = MagicMock()
        mock_forecast_config.hyperparameters = {"param1": "value1"}

        mock_anomaly_config = MagicMock()
        mock_anomaly_config.hyperparameters = {"param2": "value2"}

        mock_models = MagicMock()
        mock_models.forecast_config = mock_forecast_config
        mock_models.anomaly_config = mock_anomaly_config
        mock_models.forecast_model = None
        mock_models.anomaly_model = None

        result = adapter._capture_attempted_hyperparameters(mock_models)

        assert result is not None
        assert "forecast" in result
        assert "anomaly" in result
        assert result["forecast"] == {"param1": "value1"}
        assert result["anomaly"] == {"param2": "value2"}

    def test_build_failed_model_config_creates_config_with_hyperparameters(
        self,
    ) -> None:
        """Verify that build_failed_model_config creates a ModelConfig with failed hyperparameters."""
        from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
            build_failed_model_config,
        )

        attempted_hyperparams = {
            "forecast": {"param1": "value1"},
            "anomaly": {"param2": "value2"},
        }
        error = "Training failed: test error"

        failed_config = build_failed_model_config(
            attempted_hyperparams=attempted_hyperparams,
            error=error,
            existing_config=None,
            forecast_registry_key="datahub",
            anomaly_registry_key="datahub_forecast_anomaly",
        )

        assert failed_config is not None
        assert failed_config.forecast_score == 0.0  # Indicates failure
        assert failed_config.anomaly_score == 0.0  # Indicates failure
        assert failed_config.forecast_config_json is not None
        assert failed_config.anomaly_config_json is not None
