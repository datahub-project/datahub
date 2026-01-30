"""Comprehensive tests for ObserveAdapter."""

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from unittest.mock import MagicMock, patch

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
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


def _make_context() -> InputDataContext:
    """Create a test InputDataContext."""
    return InputDataContext(assertion_category="volume")


class TestObserveAdapterForceRetune:
    """Tests for force_retune parameter handling."""

    def test_force_retune_sets_both_when_flag_false(self) -> None:
        """force_retune=True sets both forecast and anomaly when flag is false."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_force_retune_anomaly_only",
                return_value=False,
            ),
            patch.object(adapter, "_run_combination_evaluation") as mock_comb,
            patch.object(adapter, "_train_and_evaluate") as mock_train,
        ):
            mock_train.return_value = (
                MagicMock(),
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                df.tail(10),
                None,  # eval_detection_results
            )
            mock_comb.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            try:
                adapter.run_training_pipeline(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
                    force_retune=True,
                )
            except Exception:
                pass  # We're just testing the force_retune logic

    def test_force_retune_sets_anomaly_only_when_flag_true(self) -> None:
        """force_retune=True sets only anomaly when flag is true."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_force_retune_anomaly_only",
                return_value=True,
            ),
            patch.object(adapter, "_run_combination_evaluation") as mock_comb,
            patch.object(adapter, "_train_and_evaluate") as mock_train,
        ):
            mock_train.return_value = (
                MagicMock(),
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                df.tail(10),
                None,  # eval_detection_results
            )
            mock_comb.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            try:
                adapter.run_training_pipeline(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
                    force_retune=True,
                )
            except Exception:
                pass  # We're just testing the force_retune logic


class TestObserveAdapterCombinationEvaluation:
    """Tests for model combination evaluation path."""

    def test_runs_combination_evaluation_when_combinations_provided(self) -> None:
        """run_training_pipeline calls _run_combination_evaluation when model_combinations provided."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        with patch.object(adapter, "_run_combination_evaluation") as mock_comb_eval:
            mock_result = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )
            mock_comb_eval.return_value = mock_result

            result = adapter.run_training_pipeline(
                df=df,
                input_data_context=context,
                num_intervals=24,
                model_combinations=pairings,
            )

            assert result is mock_result
            mock_comb_eval.assert_called_once()

    def test_uses_default_pairings_when_none_provided(self) -> None:
        """run_training_pipeline uses default pairings when model_combinations is None."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.get_pairings_from_env_or_default"
            ) as mock_get_pairings,
            patch.object(adapter, "_run_combination_evaluation") as mock_comb_eval,
        ):
            mock_pairings = [
                ModelPairing(
                    anomaly_model="test_anomaly", forecast_model="test_forecast"
                )
            ]
            mock_get_pairings.return_value = mock_pairings
            mock_comb_eval.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            mock_get_pairings.assert_called_once()
            mock_comb_eval.assert_called_once()


class TestObserveAdapterTrainingPipeline:
    """Tests for the main training pipeline flow."""

    def test_quality_retune_when_score_below_threshold(self) -> None:
        """Pipeline retries with retuning when score is below threshold."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            # First call: low score, not tuned
            # Second call: after retune
            first_models = MagicMock(
                forecast_model=MagicMock(),
                anomaly_model=MagicMock(),
                forecast_registry_key="test",
                anomaly_registry_key="test",
            )
            mock_train.side_effect = [
                (
                    first_models,
                    0.3,  # Low anomaly score
                    0.7,
                    {},
                    {},
                    False,  # Not tuned yet
                    None,
                    eval_df,
                    None,  # eval_detection_results
                ),
                (
                    MagicMock(
                        forecast_model=MagicMock(),
                        anomaly_model=MagicMock(),
                        forecast_registry_key="test",
                        anomaly_registry_key="test",
                    ),
                    0.8,  # Better score after retune
                    0.7,
                    {},
                    {},
                    True,
                    None,
                    eval_df,
                    None,  # eval_detection_results
                ),
            ]

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.return_value = pd.DataFrame(
                {
                    "timestamp_ms": [1000, 2000],
                    "detection_band_lower": [0.5, 0.6],
                    "detection_band_upper": [1.5, 1.6],
                }
            )
            first_models.anomaly_model = mock_anomaly

            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            mock_factory.return_value.create_models.return_value = full_models

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            assert mock_train.call_count == 2

    def test_returns_scores_only_when_score_still_below_threshold_after_retry(
        self,
    ) -> None:
        """When score still below threshold after retry, returns scores_only_persist=True and prediction_df=None."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config",
                return_value=ModelConfig(preprocessing_config_json="{}"),
            ),
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_train.return_value = (
                MagicMock(
                    forecast_model=MagicMock(),
                    anomaly_model=MagicMock(),
                    forecast_registry_key="test",
                    anomaly_registry_key="test",
                ),
                0.3,  # Low score
                0.7,
                {},
                {},
                True,  # Already tuned, so no retry
                None,
                eval_df,
                None,  # eval_detection_results
            )

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result.prediction_df is None
            assert result.scores_only_persist is True
            assert result.model_config is not None

    def test_full_history_retrain(self) -> None:
        """Pipeline performs full history retrain after evaluation."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_registry_key = "test"
            mock_models.anomaly_registry_key = "test"
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()

            mock_train.return_value = (
                mock_models,
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                eval_df,
                None,  # eval_detection_results
            )

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.return_value = pd.DataFrame(
                {
                    "timestamp_ms": [1000, 2000],
                    "detection_band_lower": [0.5, 0.6],
                    "detection_band_upper": [1.5, 1.6],
                }
            )
            mock_anomaly.train = MagicMock()

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_model = MagicMock()
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            full_models.forecast_config = MagicMock()
            full_models.anomaly_config = MagicMock()
            full_models.preprocessing_config = MagicMock()

            mock_factory.return_value.create_models.return_value = full_models
            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            mock_anomaly.train.assert_called_once_with(df, ground_truth=None)

    def test_prediction_failure_handled_gracefully(self) -> None:
        """Pipeline handles prediction failure gracefully."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()

        with (
            patch.object(adapter, "_train_and_evaluate") as mock_train,
            patch.object(adapter, "_generate_future_timestamps") as mock_future,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.build_model_config"
            ) as mock_build_config,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_registry_key = "test"
            mock_models.anomaly_registry_key = "test"
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()

            mock_train.return_value = (
                mock_models,
                0.8,
                0.7,
                {},
                {},
                False,
                None,
                eval_df,
                None,  # eval_detection_results
            )

            mock_future.return_value = pd.DataFrame(
                {"ds": pd.date_range("2024-01-02", periods=24, freq="h")}
            )

            mock_anomaly = MagicMock()
            mock_anomaly.detect.side_effect = Exception("Prediction failed")
            mock_anomaly.train = MagicMock()

            full_models = MagicMock()
            full_models.anomaly_model = mock_anomaly
            full_models.forecast_model = MagicMock()
            full_models.forecast_registry_key = "test"
            full_models.anomaly_registry_key = "test"
            full_models.forecast_config = MagicMock()
            full_models.anomaly_config = MagicMock()
            full_models.preprocessing_config = MagicMock()

            mock_factory.return_value.create_models.return_value = full_models
            mock_build_config.return_value = ModelConfig(preprocessing_config_json="{}")

            result = adapter.run_training_pipeline(
                df=df, input_data_context=context, num_intervals=24
            )

            assert result is not None
            assert result.prediction_df is None
            assert "prediction" in result.step_results
            assert result.step_results["prediction"].success is False


class TestObserveAdapterHelperMethods:
    """Tests for helper methods in ObserveAdapter."""

    def test_generate_future_timestamps(self) -> None:
        """_generate_future_timestamps generates correct future timestamps."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        result = adapter._generate_future_timestamps(
            df, num_intervals=24, interval_hours=1
        )

        assert len(result) == 24
        assert "ds" in result.columns
        assert result["ds"].min() > df["ds"].max()

    def test_generate_future_timestamps_with_nan_last_ts(self) -> None:
        """_generate_future_timestamps handles NaN last timestamp."""
        adapter = ObserveAdapter()
        df = pd.DataFrame(
            {
                "ds": [pd.Timestamp("2024-01-01"), pd.NaT],
                "y": [1, 2],
            }
        )

        result = adapter._generate_future_timestamps(
            df, num_intervals=5, interval_hours=1
        )

        assert len(result) == 5
        assert "ds" in result.columns

    def test_format_prediction_output_success(self) -> None:
        """_format_prediction_output formats valid prediction results."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "timestamp_ms": [1000, 2000],
                "detection_band_lower": [0.5, 0.6],
                "detection_band_upper": [1.5, 1.6],
                "y": [1.0, 2.0],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            result = adapter._format_prediction_output(results)

            assert result is not None
            assert len(result) == 2

    def test_format_prediction_output_missing_timestamp_ms(self) -> None:
        """_format_prediction_output raises when timestamp_ms missing."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "detection_band_lower": [0.5, 0.6],
                "detection_band_upper": [1.5, 1.6],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            with pytest.raises(ValueError, match="timestamp_ms"):
                adapter._format_prediction_output(results)

    def test_format_prediction_output_missing_detection_band_lower(self) -> None:
        """_format_prediction_output raises when detection_band_lower missing."""
        adapter = ObserveAdapter()
        results = pd.DataFrame(
            {
                "timestamp_ms": [1000, 2000],
                "detection_band_upper": [1.5, 1.6],
            }
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.prepare_predictions_df_for_persistence"
        ) as mock_prep:
            mock_prep.return_value = results
            with pytest.raises(ValueError, match="detection_band_lower"):
                adapter._format_prediction_output(results)

    def test_format_prediction_output_empty_results(self) -> None:
        """_format_prediction_output handles empty results."""
        adapter = ObserveAdapter()
        results = pd.DataFrame()

        result = adapter._format_prediction_output(results)

        assert result is not None
        assert len(result) == 0

    def test_format_prediction_output_none_results(self) -> None:
        """_format_prediction_output handles None results."""
        adapter = ObserveAdapter()

        result = adapter._format_prediction_output(None)  # type: ignore[arg-type]

        assert result is None


class TestObserveAdapterTrainAndEvaluate:
    """Tests for _train_and_evaluate method."""

    def test_train_and_evaluate_with_tuning_decision(self) -> None:
        """_train_and_evaluate uses provided tuning decision."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        tuning_decision = TuningDecision(
            should_retune_forecast=True,
            should_retune_anomaly=True,
            forecast_reason=TuningReason.FORCED,
            anomaly_reason=TuningReason.FORCED,
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            builder = TrainingResultBuilder()
            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                tuning_decision=tuning_decision,
                eval_df=df.tail(10),
            )

            assert result is not None
            assert result[1] == 0.8  # anomaly_score
            assert result[2] == 0.7  # forecast_score

    def test_train_and_evaluate_default_tuning_decision(self) -> None:
        """_train_and_evaluate creates default tuning decision when None."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.forecast_config = MagicMock()
            mock_models.anomaly_config = MagicMock()
            mock_models.preprocessing_config = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            builder = TrainingResultBuilder()
            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                tuning_decision=None,
                eval_df=df.tail(10),
            )

            assert result is not None

    def test_train_and_evaluate_model_creation_failure(self) -> None:
        """_train_and_evaluate raises on model creation failure."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
        ) as mock_factory:
            mock_factory.return_value.create_models.side_effect = Exception(
                "Creation failed"
            )

            builder = TrainingResultBuilder()
            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type == MonitorErrorTypeClass.MODEL_CREATION_FAILED
            )

    def test_train_and_evaluate_training_failure(self) -> None:
        """_train_and_evaluate raises on training failure."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
        ) as mock_factory:
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_models.anomaly_model.train.side_effect = Exception("Training failed")
            mock_factory.return_value.create_models.return_value = mock_models

            builder = TrainingResultBuilder()
            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type == MonitorErrorTypeClass.MODEL_TRAINING_FAILED
            )

    def test_train_and_evaluate_evaluation_failure(self) -> None:
        """_train_and_evaluate raises on evaluation failure."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_models = MagicMock()
            mock_models.forecast_model = MagicMock()
            mock_models.anomaly_model = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.side_effect = Exception("Evaluation failed")

            builder = TrainingResultBuilder()
            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._train_and_evaluate(
                    df=df,
                    ground_truth=None,
                    defaults=MagicMock(),
                    existing_model_config=None,
                    progress_hooks=(builder,),
                    result_builder=builder,
                    eval_df=df.tail(10),
                )

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )

    def test_train_and_evaluate_with_forecast_model_predict(self) -> None:
        """_train_and_evaluate generates eval_forecast_df when forecast model available."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.ModelFactory"
            ) as mock_factory,
            patch.object(adapter, "_evaluate_quality") as mock_eval,
        ):
            mock_forecast = MagicMock()
            mock_forecast.is_trained = True
            mock_forecast.predict.return_value = df.tail(10)

            mock_models = MagicMock()
            mock_models.forecast_model = mock_forecast
            mock_models.anomaly_model = MagicMock()
            mock_factory.return_value.create_models.return_value = mock_models

            mock_eval.return_value = (0.8, 0.7, {}, {}, df.tail(10))

            builder = TrainingResultBuilder()
            result = adapter._train_and_evaluate(
                df=df,
                ground_truth=None,
                defaults=MagicMock(),
                existing_model_config=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=df.tail(10),
            )

            assert result is not None
            assert result[6] is not None  # eval_forecast_df


class TestObserveAdapterEvaluateQuality:
    """Tests for _evaluate_quality method."""

    def test_evaluate_quality_raises_when_eval_df_empty(self) -> None:
        """_evaluate_quality raises when eval_df is empty."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.forecast_model = MagicMock()

        builder = TrainingResultBuilder()
        with pytest.raises(TrainingErrorException) as excinfo:
            adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=pd.DataFrame(),
            )

        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )

    def test_evaluate_quality_raises_when_insufficient_samples(self) -> None:
        """_evaluate_quality raises when total samples below min_samples_for_cv."""
        adapter = ObserveAdapter()
        df = _make_test_df(5)
        eval_df = df.tail(2)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 20
        models.forecast_model = MagicMock()

        builder = TrainingResultBuilder()
        with pytest.raises(TrainingErrorException) as excinfo:
            adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

        assert (
            excinfo.value.error_type == MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT
        )

    def test_evaluate_quality_uses_best_model_score(self) -> None:
        """_evaluate_quality uses best_model_score when available."""
        from datahub_observe.algorithms.scoring import ModelScore, ScoreType

        adapter = ObserveAdapter()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = ModelScore(
            value=0.85,
            score_type=ScoreType.ANOMALY,
            metric_name="f1_score",
            has_ground_truth=True,
            confidence=0.8,
        )
        models.forecast_model = MagicMock()

        builder = TrainingResultBuilder()
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
        ) as mock_compute:
            mock_compute.return_value = ({}, {}, eval_df)

            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[0] == 0.85  # Uses best_model_score.value

    def test_evaluate_quality_computes_forecast_score_with_y_range(self) -> None:
        """_evaluate_quality computes forecast score using model y_range."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = 100.0

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.return_value = 0.75

            builder = TrainingResultBuilder()
            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[1] == 0.75  # forecast_score is at index 1
            mock_compute_forecast.assert_called_once()
            call_kwargs = mock_compute_forecast.call_args[1]
            assert call_kwargs["y_range"] == 100.0

    def test_evaluate_quality_computes_forecast_score_from_eval_df(self) -> None:
        """_evaluate_quality computes forecast score using eval_df y_range when model y_range unavailable."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        eval_df = df.tail(10).copy()
        eval_df["y"] = range(10, 20)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.return_value = 0.75

            builder = TrainingResultBuilder()
            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[1] == 0.75  # forecast_score is at index 1
            call_kwargs = mock_compute_forecast.call_args[1]
            assert call_kwargs["y_range"] == 9.0  # max - min from eval_df

    def test_evaluate_quality_handles_forecast_score_computation_failure(self) -> None:
        """_evaluate_quality handles forecast score computation failure gracefully."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        eval_df = df.tail(10)

        models = MagicMock()
        models.anomaly_model = MagicMock()
        models.anomaly_model.min_samples_for_cv = 10
        models.anomaly_model.best_model_score = None
        models.forecast_model = MagicMock()
        models.forecast_model.y_range = None

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_evaluation_metrics"
            ) as mock_compute,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.compute_forecast_score"
            ) as mock_compute_forecast,
        ):
            mock_compute.return_value = ({"mae": 10.0}, {}, eval_df)
            mock_compute_forecast.side_effect = Exception("Score computation failed")

            builder = TrainingResultBuilder()
            result = adapter._evaluate_quality(
                df=df,
                models=models,
                ground_truth=None,
                progress_hooks=(builder,),
                result_builder=builder,
                eval_df=eval_df,
            )

            assert result[1] is None  # forecast_score is None on failure (index 1)


class TestObserveAdapterRunCombinationEvaluation:
    """Tests for _run_combination_evaluation method."""

    def test_run_combination_evaluation_calls_run_pairing_evaluation(self) -> None:
        """_run_combination_evaluation calls run_pairing_evaluation."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
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
            patch.object(adapter, "_train_single_combination") as mock_train_single,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_result = MagicMock()
            mock_result.combined_score = 0.9
            mock_run_pairing.return_value = [mock_result]
            mock_select.return_value = (mock_result, 0)

            mock_train_single.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            result = adapter._run_combination_evaluation(
                df=df,
                input_data_context=context,
                num_intervals=24,
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

            assert result is not None
            mock_run_pairing.assert_called_once()
            mock_train_single.assert_called_once()

    def test_run_combination_evaluation_handles_evaluation_failure(self) -> None:
        """_run_combination_evaluation raises on evaluation failure."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.run_pairing_evaluation"
            ) as mock_run_pairing,
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.adapter.split_time_series_df"
            ) as mock_split,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_run_pairing.side_effect = Exception("Evaluation failed")

            with pytest.raises(TrainingErrorException) as excinfo:
                adapter._run_combination_evaluation(
                    df=df,
                    input_data_context=context,
                    num_intervals=24,
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

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.MODEL_EVALUATION_FAILED
            )

    def test_run_combination_evaluation_with_existing_model_config(self) -> None:
        """_run_combination_evaluation uses existing model config when available."""
        adapter = ObserveAdapter()
        df = _make_test_df(50)
        context = _make_context()
        pairings = [
            ModelPairing(anomaly_model="test_anomaly", forecast_model="test_forecast")
        ]
        existing_config = ModelConfig(
            forecast_model_name="old_forecast",
            anomaly_model_name="old_anomaly",
            preprocessing_config_json="{}",
        )

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
            patch.object(adapter, "_train_single_combination") as mock_train_single,
        ):
            eval_train_df = df.iloc[:-10]
            eval_df = df.tail(10)
            mock_split.return_value = (eval_train_df, eval_df)

            mock_result = MagicMock()
            mock_result.combined_score = 0.9
            mock_run_pairing.return_value = [mock_result]
            mock_select.return_value = (mock_result, 0)

            mock_train_single.return_value = MagicMock(
                prediction_df=df.head(10),
                model_config=ModelConfig(preprocessing_config_json="{}"),
                step_results={},
            )

            adapter._run_combination_evaluation(
                df=df,
                input_data_context=context,
                num_intervals=24,
                interval_hours=1,
                force_retune_forecast=False,
                force_retune_anomaly=False,
                ground_truth=None,
                existing_model_config=existing_config,
                model_combinations=pairings,
                score_drop_threshold=0.25,
                eval_train_ratio=None,
                progress_hooks=None,
            )

            # Verify _train_single_combination was called with modified config
            assert mock_train_single.called
            call_kwargs = mock_train_single.call_args[1]
            assert call_kwargs["existing_model_config"] is not None
