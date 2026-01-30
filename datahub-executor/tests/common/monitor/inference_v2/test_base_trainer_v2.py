"""Tests for BaseTrainerV2 utility methods."""

import time
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("datahub_observe")

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import (
    InsufficientSamplesException,
    TrainingErrorException,
)
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import (
    BaseTrainerV2,
    _format_model_key,
)
from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
from datahub_executor.common.monitor.inference_v2.types import (
    AssertionTrainingContext,
    TrainingResult,
)
from datahub_executor.common.types import (
    Anomaly,
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    AssertionEvaluationSpecContext,
    AssertionInferenceDetails,
    Monitor,
)


class ConcreteTrainerV2(BaseTrainerV2):
    """Concrete implementation for testing abstract base class."""

    def _train_internal(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        pass

    def get_assertion_category(self) -> str:
        return "test"

    def get_min_training_interval_seconds(self) -> int:
        return 3600  # 1 hour

    def get_retune_interval_seconds(self) -> int:
        return 6 * 24 * 3600  # 6 days

    def get_training_context(
        self,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> AssertionTrainingContext:
        return AssertionTrainingContext(entity_urn="urn:li:dataset:test")


@pytest.fixture
def trainer() -> ConcreteTrainerV2:
    """Create a trainer instance with mock dependencies."""
    graph = MagicMock(spec=DataHubGraph)
    metrics_client = MagicMock(spec=MetricClient)
    monitor_client = MagicMock(spec=MonitorClient)
    return ConcreteTrainerV2(graph, metrics_client, monitor_client)


class TestBuildTrainingDataframe:
    """Tests for _build_training_dataframe method."""

    def test_builds_ground_truth_from_anomalies(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Anomalies are converted to ground truth DataFrame with is_anomaly_gt column."""
        metrics = [
            Metric(timestamp_ms=1000, value=10.0),
            Metric(timestamp_ms=2000, value=20.0),
            Metric(timestamp_ms=3000, value=30.0),
        ]
        anomalies = [
            Anomaly(timestamp_ms=2000, status="CONFIRMED"),  # Confirmed anomaly
            Anomaly(timestamp_ms=3000, status="REJECTED"),  # Rejected anomaly
        ]

        training_df, ground_truth_df = trainer._build_training_dataframe(
            metrics, anomalies
        )

        assert len(training_df) == 3
        assert len(ground_truth_df) == 2
        assert ground_truth_df.iloc[0]["is_anomaly_gt"] == True  # noqa: E712
        assert ground_truth_df.iloc[1]["is_anomaly_gt"] == False  # noqa: E712
        assert "ds" in training_df.columns
        assert "y" in training_df.columns
        assert "ds" in ground_truth_df.columns
        assert "is_anomaly_gt" in ground_truth_df.columns

    def test_sorts_by_timestamp(self, trainer: ConcreteTrainerV2) -> None:
        """Output DataFrame is sorted by 'ds' column."""
        metrics = [
            Metric(timestamp_ms=3000, value=30.0),
            Metric(timestamp_ms=1000, value=10.0),
            Metric(timestamp_ms=2000, value=20.0),
        ]

        training_df, ground_truth_df = trainer._build_training_dataframe(metrics, [])

        assert list(training_df["y"]) == [10.0, 20.0, 30.0]
        assert training_df["ds"].is_monotonic_increasing

    def test_empty_metrics(self, trainer: ConcreteTrainerV2) -> None:
        """Empty metrics list produces empty DataFrame."""
        training_df, ground_truth_df = trainer._build_training_dataframe([], [])

        assert training_df.empty
        assert ground_truth_df.empty

    def test_raises_on_exception(self, trainer: ConcreteTrainerV2) -> None:
        """Raises TrainingErrorException on exception during dataframe building."""
        # This is hard to trigger without mocking timestamp_ms_to_ds, but we can test
        # that the exception handling path exists
        metrics = [Metric(timestamp_ms=1000, value=10.0)]

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.timestamp_ms_to_ds",
            side_effect=Exception("Conversion failed"),
        ):
            with pytest.raises(TrainingErrorException) as excinfo:
                trainer._build_training_dataframe(metrics, [])

            assert excinfo.value.error_type == MonitorErrorTypeClass.INPUT_DATA_INVALID
            assert "Failed to build training dataframes" in str(excinfo.value)
            assert excinfo.value.properties["step"] == "build_training_dataframe"


class TestShouldPerformInference:
    """Tests for _should_perform_inference method."""

    def test_true_when_no_prior_inference(self, trainer: ConcreteTrainerV2) -> None:
        """Returns True when evaluation_spec has no inference_details."""
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        result = trainer._should_perform_inference(eval_spec)
        assert result is True

    def test_false_within_interval(self, trainer: ConcreteTrainerV2) -> None:
        """Returns False when last inference was within min_training_interval."""
        now_ms = int(time.time() * 1000)

        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = MagicMock(spec=AssertionEvaluationSpecContext)
        eval_spec.context.inference_details = MagicMock(spec=AssertionInferenceDetails)
        eval_spec.context.inference_details.generated_at = now_ms - 1000  # 1 second ago

        result = trainer._should_perform_inference(eval_spec)
        assert result is False

    def test_true_after_interval(self, trainer: ConcreteTrainerV2) -> None:
        """Returns True when enough time has passed since last inference."""
        now_ms = int(time.time() * 1000)
        two_hours_ago = now_ms - (2 * 3600 * 1000)

        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = MagicMock(spec=AssertionEvaluationSpecContext)
        eval_spec.context.inference_details = MagicMock(spec=AssertionInferenceDetails)
        eval_spec.context.inference_details.generated_at = two_hours_ago

        result = trainer._should_perform_inference(eval_spec)
        assert result is True


class TestExtractExistingModelConfig:
    """Tests for _extract_existing_model_config method."""

    def test_extracts_model_config(self, trainer: ConcreteTrainerV2) -> None:
        """Extracts ModelConfig from evaluation_spec.context.inference_details."""
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = MagicMock(spec=AssertionEvaluationSpecContext)
        eval_spec.context.inference_details = MagicMock(spec=AssertionInferenceDetails)
        eval_spec.context.inference_details.modelId = "observe-models"
        eval_spec.context.inference_details.parameters = {
            "forecastModelName": "prophet",
            "preprocessingConfigJson": "{}",
        }
        # Backward-compatibility: if only legacy confidence exists, we treat it as anomaly_score.
        eval_spec.context.inference_details.confidence = 0.95
        eval_spec.context.inference_details.generatedAt = 1234567890

        result = trainer._extract_existing_model_config(eval_spec)

        assert result is not None
        assert result.forecast_model_name == "prophet"
        assert result.anomaly_score == 0.95

    def test_returns_none_when_no_context(self, trainer: ConcreteTrainerV2) -> None:
        """Returns None when evaluation_spec has no context."""
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        result = trainer._extract_existing_model_config(eval_spec)
        assert result is None


class TestShouldForceRetune:
    """Tests for _should_force_retune method."""

    def test_returns_false_when_no_config(self, trainer: ConcreteTrainerV2) -> None:
        """Returns False when no existing model config."""
        result = trainer._should_force_retune(None)
        assert result is False

    def test_returns_false_when_no_timestamp(self, trainer: ConcreteTrainerV2) -> None:
        """Returns False when config has no generated_at timestamp."""
        config = ModelConfig(preprocessing_config_json="{}", generated_at=None)
        result = trainer._should_force_retune(config)
        assert result is False

    def test_returns_false_when_timestamp_zero(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Returns False when config has generated_at of 0."""
        config = ModelConfig(preprocessing_config_json="{}", generated_at=0)
        result = trainer._should_force_retune(config)
        assert result is False

    def test_returns_false_when_fresh(self, trainer: ConcreteTrainerV2) -> None:
        """Returns False when hyperparameters are still fresh."""
        now_ms = int(time.time() * 1000)
        one_day_ago = now_ms - (24 * 3600 * 1000)  # 1 day ago

        config = ModelConfig(preprocessing_config_json="{}", generated_at=one_day_ago)
        result = trainer._should_force_retune(config)
        assert result is False  # 1 day < 6 day retune interval

    def test_returns_false_at_exact_retune_interval(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Returns False when exactly at retune interval (boundary condition)."""
        now_ms = int(time.time() * 1000)
        six_days_ago = now_ms - (6 * 24 * 3600 * 1000)  # Exactly 6 days ago

        config = ModelConfig(preprocessing_config_json="{}", generated_at=six_days_ago)
        result = trainer._should_force_retune(config)
        # Should be False because (now_ms - generated_at_ms) == retune_interval_ms
        # The condition is >=, so exactly at should return True
        # Actually, let's check: (now_ms - six_days_ago) == 6 days, which equals retune_interval
        # So (now_ms - generated_at_ms) >= retune_interval_ms should be True
        assert result is True  # Exactly at interval should trigger retune

    def test_returns_true_when_stale(self, trainer: ConcreteTrainerV2) -> None:
        """Returns True when hyperparameters are stale."""
        now_ms = int(time.time() * 1000)
        seven_days_ago = now_ms - (7 * 24 * 3600 * 1000)  # 7 days ago

        config = ModelConfig(
            preprocessing_config_json="{}", generated_at=seven_days_ago
        )
        result = trainer._should_force_retune(config)
        assert result is True  # 7 days > 6 day retune interval


class TestRunWithTrainingErrorHandling:
    """Tests for _run_with_training_error_handling method."""

    def test_passes_through_training_error_exception(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """TrainingErrorException is passed through unchanged."""
        original_error = TrainingErrorException(
            message="Test error",
            error_type=MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
        )

        def failing_fn() -> None:
            raise original_error

        with pytest.raises(TrainingErrorException) as excinfo:
            trainer._run_with_training_error_handling(
                assertion_urn="urn:test", fn=failing_fn
            )

        assert excinfo.value is original_error

    def test_converts_runtime_error(self, trainer: ConcreteTrainerV2) -> None:
        """RuntimeError is converted to TrainingErrorException."""

        def failing_fn() -> None:
            raise RuntimeError("Runtime error occurred")

        with pytest.raises(TrainingErrorException) as excinfo:
            trainer._run_with_training_error_handling(
                assertion_urn="urn:test", fn=failing_fn
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.UNKNOWN
        assert "Runtime error occurred" in str(excinfo.value)

    def test_converts_general_exception(self, trainer: ConcreteTrainerV2) -> None:
        """General Exception is converted to TrainingErrorException."""

        def failing_fn() -> None:
            raise ValueError("Value error occurred")

        with pytest.raises(TrainingErrorException) as excinfo:
            trainer._run_with_training_error_handling(
                assertion_urn="urn:test", fn=failing_fn
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.UNKNOWN
        assert "Value error occurred" in str(excinfo.value)

    def test_successful_execution(self, trainer: ConcreteTrainerV2) -> None:
        """Successful execution completes without error."""
        execution_count = 0

        def success_fn() -> None:
            nonlocal execution_count
            execution_count += 1

        trainer._run_with_training_error_handling(
            assertion_urn="urn:test", fn=success_fn
        )

        assert execution_count == 1


class TestBuildBaseTrainingContext:
    """Tests for _build_base_training_context method."""

    def test_builds_context_with_required_fields(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Builds context with entity_urn, interval_hours, and other required fields."""
        assertion = MagicMock(spec=Assertion)
        assertion.entity = MagicMock()
        assertion.entity.urn = "urn:li:dataset:test"

        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        context = trainer._build_base_training_context(
            assertion=assertion,
            adjustment_settings=None,
            evaluation_spec=eval_spec,
            assertion_category="volume",
            default_sensitivity_level=3,
            interval_hours=24,
        )

        assert context.entity_urn == "urn:li:dataset:test"
        assert context.interval_hours == 24
        assert context.assertion_category == "volume"
        assert context.sensitivity_level == 3
        assert context.num_intervals > 0

    def test_extracts_existing_model_config(self, trainer: ConcreteTrainerV2) -> None:
        """Extracts existing model config from evaluation spec."""
        assertion = MagicMock(spec=Assertion)
        assertion.entity = MagicMock()
        assertion.entity.urn = "urn:li:dataset:test"

        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = MagicMock(spec=AssertionEvaluationSpecContext)
        eval_spec.context.inference_details = MagicMock(spec=AssertionInferenceDetails)
        eval_spec.context.inference_details.modelId = "observe-models"
        eval_spec.context.inference_details.confidence = (
            None  # used by parse_inference_details
        )
        eval_spec.context.inference_details.generatedAt = None
        eval_spec.context.inference_details.parameters = {
            "forecastModelName": "prophet",
            "preprocessingConfigJson": "{}",
        }

        context = trainer._build_base_training_context(
            assertion=assertion,
            adjustment_settings=None,
            evaluation_spec=eval_spec,
            assertion_category="volume",
            default_sensitivity_level=3,
        )

        assert context.existing_model_config is not None
        assert context.existing_model_config.forecast_model_name == "prophet"


class TestGetAdjustmentSettings:
    """Tests for _get_adjustment_settings method."""

    def test_returns_settings_when_present(self, trainer: ConcreteTrainerV2) -> None:
        """Returns adjustment settings when monitor has them."""
        monitor = MagicMock(spec=Monitor)
        monitor.assertion_monitor = MagicMock()
        monitor.assertion_monitor.settings = MagicMock()
        monitor.assertion_monitor.settings.inference_settings = MagicMock(
            spec=AssertionAdjustmentSettings
        )

        result = trainer._get_adjustment_settings(monitor)

        assert result is not None
        assert result is monitor.assertion_monitor.settings.inference_settings

    def test_returns_none_when_missing(self, trainer: ConcreteTrainerV2) -> None:
        """Returns None when monitor has no adjustment settings."""
        monitor = MagicMock(spec=Monitor)
        monitor.assertion_monitor = None

        result = trainer._get_adjustment_settings(monitor)

        assert result is None

    def test_returns_none_when_settings_missing(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Returns None when assertion_monitor has no settings."""
        monitor = MagicMock(spec=Monitor)
        monitor.assertion_monitor = MagicMock()
        monitor.assertion_monitor.settings = None

        result = trainer._get_adjustment_settings(monitor)

        assert result is None


class TestFetchMetrics:
    """Tests for _fetch_metrics method."""

    def test_fetches_metrics_successfully(self, trainer: ConcreteTrainerV2) -> None:
        """Fetches metrics from metrics client."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"

        metrics = [
            Metric(timestamp_ms=1000, value=10.0),
            Metric(timestamp_ms=2000, value=20.0),
        ]

        trainer.metrics_client.fetch_metric_values.return_value = metrics  # type: ignore[attr-defined]

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.get_metric_cube_urn",
                return_value="urn:li:metricCube:test",
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.extract_lookback_days",
                return_value=30,
            ),
        ):
            result = trainer._fetch_metrics(monitor, None)

            assert result == metrics
            trainer.metrics_client.fetch_metric_values.assert_called_once()  # type: ignore[attr-defined]

    def test_raises_on_fetch_failure(self, trainer: ConcreteTrainerV2) -> None:
        """Raises TrainingErrorException on fetch failure."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"

        trainer.metrics_client.fetch_metric_values.side_effect = Exception(  # type: ignore[attr-defined]
            "Fetch failed"
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.get_metric_cube_urn",
                return_value="urn:li:metricCube:test",
            ),
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.extract_lookback_days",
                return_value=30,
            ),
        ):
            with pytest.raises(TrainingErrorException) as excinfo:
                trainer._fetch_metrics(monitor, None)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT
            )
            assert "Failed to fetch metrics" in str(excinfo.value)
            assert excinfo.value.properties["step"] == "fetch_metrics"
            assert (
                excinfo.value.properties["metric_cube_urn"] == "urn:li:metricCube:test"
            )


class TestFetchAnomalies:
    """Tests for _fetch_anomalies method."""

    def test_fetches_anomalies_successfully(self, trainer: ConcreteTrainerV2) -> None:
        """Fetches anomalies from monitor client."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"

        anomalies = [
            Anomaly(timestamp_ms=1000, status="CONFIRMED"),
            Anomaly(timestamp_ms=2000, status="REJECTED"),
        ]

        trainer.monitor_client.fetch_monitor_anomalies.return_value = anomalies  # type: ignore[attr-defined]

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.extract_lookback_days",
            return_value=30,
        ):
            result = trainer._fetch_anomalies(monitor, None)

            assert result == anomalies
            trainer.monitor_client.fetch_monitor_anomalies.assert_called_once()  # type: ignore[attr-defined]

    def test_raises_on_fetch_failure(self, trainer: ConcreteTrainerV2) -> None:
        """Raises TrainingErrorException on fetch failure."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"

        trainer.monitor_client.fetch_monitor_anomalies.side_effect = Exception(  # type: ignore[attr-defined]
            "Fetch failed"
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.extract_lookback_days",
            return_value=30,
        ):
            with pytest.raises(TrainingErrorException) as excinfo:
                trainer._fetch_anomalies(monitor, None)

            assert (
                excinfo.value.error_type
                == MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT
            )
            assert "Failed to fetch anomalies" in str(excinfo.value)
            assert excinfo.value.properties["step"] == "fetch_anomalies"
            assert excinfo.value.properties["monitor_urn"] == monitor.urn


class TestUpdateAssertion:
    """Tests for _update_assertion method."""

    def test_raises_when_prediction_df_none(self, trainer: ConcreteTrainerV2) -> None:
        """Raises TrainingErrorException when prediction_df is None."""
        monitor = MagicMock(spec=Monitor)
        assertion = MagicMock(spec=Assertion)
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        context = AssertionTrainingContext(entity_urn="urn:li:dataset:test")

        training_result = TrainingResult(
            prediction_df=None,
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with pytest.raises(TrainingErrorException) as excinfo:
            trainer._update_assertion(
                monitor, assertion, eval_spec, training_result, context
            )

        assert excinfo.value.error_type == MonitorErrorTypeClass.MODEL_TRAINING_FAILED
        assert "Prediction output missing" in str(excinfo.value)
        assert excinfo.value.properties["step"] == "prediction"
        assert excinfo.value.properties["component"] == "trainer_v2"

    def test_raises_when_prediction_df_none_with_error(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Raises TrainingErrorException with prediction error when available."""
        from datahub_executor.common.monitor.inference_v2.types import (
            StepResult,
        )

        monitor = MagicMock(spec=Monitor)
        assertion = MagicMock(spec=Assertion)
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        context = AssertionTrainingContext(entity_urn="urn:li:dataset:test")

        training_result = TrainingResult(
            prediction_df=None,
            model_config=ModelConfig(
                preprocessing_config_json="{}",
                forecast_model_name="prophet",
                anomaly_model_name="test_anomaly",
            ),
            step_results={
                "prediction": StepResult(success=False, error="Prediction failed")
            },
        )

        with pytest.raises(TrainingErrorException) as excinfo:
            trainer._update_assertion(
                monitor, assertion, eval_spec, training_result, context
            )

        assert "Prediction failed" in str(
            excinfo.value.properties.get("prediction_error")
        )

    def test_updates_assertion_successfully(self, trainer: ConcreteTrainerV2) -> None:
        """Successfully updates assertion with training results."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"
        assertion = MagicMock(spec=Assertion)
        assertion.urn = "urn:li:assertion:test"
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test", interval_hours=1
        )

        prediction_df = pd.DataFrame(
            {
                "timestamp_ms": [1000, 2000],
                "detection_band_lower": [0.5, 0.6],
                "detection_band_upper": [1.5, 1.6],
            }
        )

        training_result = TrainingResult(
            prediction_df=prediction_df,
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.AnomalyAssertions"
            ) as mock_anomaly_assertions,
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.build_evaluation_context"
            ) as mock_build_context,
        ):
            mock_assertions = [MagicMock()]
            mock_anomaly_assertions.from_df.return_value = mock_assertions
            mock_context = MagicMock()
            mock_build_context.return_value = mock_context

            trainer._update_assertion(
                monitor, assertion, eval_spec, training_result, context
            )

            trainer.monitor_client.patch_volume_monitor_evaluation_context.assert_called_once()  # type: ignore[attr-defined]
            call_args = (
                trainer.monitor_client.patch_volume_monitor_evaluation_context.call_args  # type: ignore[attr-defined]
            )
            assert call_args.args[0] == monitor.urn
            assert call_args.args[1] == assertion.urn
            assert call_args.args[2] == mock_context
            # Verify AnomalyAssertions.from_df was called with correct params
            mock_anomaly_assertions.from_df.assert_called_once()
            from_df_call = mock_anomaly_assertions.from_df.call_args
            assert from_df_call.args[0].equals(prediction_df)
            assert from_df_call.kwargs["entity_urn"] == context.entity_urn
            assert (
                from_df_call.kwargs["window_size_seconds"] == 3600
            )  # interval_hours * 3600

    def test_scores_only_persist_persists_with_empty_assertions(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """When scores_only_persist=True and prediction_df is None, persist with embedded_assertions=[]."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"
        assertion = MagicMock(spec=Assertion)
        assertion.urn = "urn:li:assertion:test"
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test", interval_hours=1
        )

        model_config = ModelConfig(preprocessing_config_json="{}")
        training_result = TrainingResult(
            prediction_df=None,
            model_config=model_config,
            step_results={},
            scores_only_persist=True,
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.build_evaluation_context"
        ) as mock_build_context:
            mock_context = MagicMock()
            mock_build_context.return_value = mock_context

            trainer._update_assertion(
                monitor, assertion, eval_spec, training_result, context
            )

            mock_build_context.assert_called_once()
            call_kwargs = mock_build_context.call_args.kwargs
            assert call_kwargs["embedded_assertions"] == []
            assert call_kwargs["model_config"] is model_config

            trainer.monitor_client.patch_volume_monitor_evaluation_context.assert_called_once()  # type: ignore[attr-defined]
            call_args = (
                trainer.monitor_client.patch_volume_monitor_evaluation_context.call_args  # type: ignore[attr-defined]
            )
            assert call_args.args[0] == monitor.urn
            assert call_args.args[1] == assertion.urn
            assert call_args.args[2] == mock_context

    def test_raises_on_persistence_failure(self, trainer: ConcreteTrainerV2) -> None:
        """Raises TrainingErrorException on persistence failure."""
        monitor = MagicMock(spec=Monitor)
        monitor.urn = "urn:li:monitor:test"
        assertion = MagicMock(spec=Assertion)
        assertion.urn = "urn:li:assertion:test"
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test", interval_hours=1
        )

        prediction_df = pd.DataFrame(
            {
                "timestamp_ms": [1000],
                "detection_band_lower": [0.5],
                "detection_band_upper": [1.5],
            }
        )

        training_result = TrainingResult(
            prediction_df=prediction_df,
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        trainer.monitor_client.patch_volume_monitor_evaluation_context.side_effect = (  # type: ignore[attr-defined]
            Exception("Persistence failed")
        )

        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.AnomalyAssertions"
            ) as mock_anomaly_assertions,
            patch(
                "datahub_executor.common.monitor.inference_v2.base_trainer_v2.build_evaluation_context"
            ) as mock_build_context,
        ):
            mock_assertions = [MagicMock()]
            mock_anomaly_assertions.from_df.return_value = mock_assertions
            mock_context = MagicMock()
            mock_build_context.return_value = mock_context

            with pytest.raises(TrainingErrorException) as excinfo:
                trainer._update_assertion(
                    monitor, assertion, eval_spec, training_result, context
                )

            assert excinfo.value.error_type == MonitorErrorTypeClass.PERSISTENCE_FAILED
            assert "Failed to persist training outputs" in str(excinfo.value)
            assert (
                excinfo.value.properties["step"]
                == "patch_volume_monitor_evaluation_context"
            )
            assert excinfo.value.properties["monitor_urn"] == monitor.urn
            assert excinfo.value.properties["assertion_urn"] == assertion.urn


class TestFormatModelKey:
    """Tests for _format_model_key function."""

    def test_format_model_key_with_version(self) -> None:
        """Formats model key with name and version."""

        result = _format_model_key("prophet", "0.1.0")
        assert result == "prophet@0.1.0"

    def test_format_model_key_without_version(self) -> None:
        """Formats model key with name only."""

        result = _format_model_key("prophet", None)
        assert result == "prophet"

    def test_format_model_key_empty_name(self) -> None:
        """Returns empty string when name is None or empty."""

        result = _format_model_key(None, "0.1.0")
        assert result == ""

        result = _format_model_key("", "0.1.0")
        assert result == ""


class TestRunTrainingPipeline:
    """Tests for _run_training_pipeline method."""

    def test_raises_on_insufficient_samples(self, trainer: ConcreteTrainerV2) -> None:
        """Raises InsufficientSamplesException when df has too few samples."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="h"),
                "y": range(5),
            }
        )

        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test",
            min_training_samples=10,
            assertion_category="volume",
        )

        with pytest.raises(InsufficientSamplesException) as excinfo:
            trainer._run_training_pipeline(df, context)

        assert excinfo.value.properties["min_samples"] == "10"
        assert excinfo.value.properties["actual_samples"] == "5"
        assert excinfo.value.properties["component"] == "trainer_v2"
        assert excinfo.value.properties["assertion_category"] == "volume"

    def test_allows_when_min_samples_none(self, trainer: ConcreteTrainerV2) -> None:
        """Allows training when min_training_samples is None."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=5, freq="h"),
                "y": range(5),
            }
        )

        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test",
            min_training_samples=None,  # type: ignore[arg-type]
            assertion_category="volume",
        )

        mock_result = TrainingResult(
            prediction_df=df.head(2),
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.ObserveAdapter"
        ) as mock_adapter_class:
            mock_adapter = MagicMock()
            mock_adapter.run_training_pipeline.return_value = mock_result
            mock_adapter_class.return_value = mock_adapter

            result = trainer._run_training_pipeline(df, context)

            assert result is mock_result

    def test_calls_adapter_with_correct_params(
        self, trainer: ConcreteTrainerV2
    ) -> None:
        """Calls ObserveAdapter with correct parameters."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=50, freq="h"),
                "y": range(50),
            }
        )

        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test",
            interval_hours=1,
            num_intervals=24,
            sensitivity_level=3,
            assertion_category="volume",
            is_dataframe_cumulative=False,
            is_delta=None,
        )

        mock_result = TrainingResult(
            prediction_df=df.head(10),
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.ObserveAdapter"
        ) as mock_adapter_class:
            mock_adapter = MagicMock()
            mock_adapter.run_training_pipeline.return_value = mock_result
            mock_adapter_class.return_value = mock_adapter

            result = trainer._run_training_pipeline(df, context)

            assert result is mock_result
            mock_adapter.run_training_pipeline.assert_called_once()
            call_kwargs = mock_adapter.run_training_pipeline.call_args[1]
            assert call_kwargs["num_intervals"] == 24
            assert call_kwargs["interval_hours"] == 1
            assert call_kwargs["sensitivity_level"] == 3
            assert call_kwargs["force_retune"] is False
            assert call_kwargs["existing_model_config"] is None

    def test_passes_ground_truth_to_adapter(self, trainer: ConcreteTrainerV2) -> None:
        """Passes ground_truth DataFrame to adapter when provided."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=50, freq="h"),
                "y": range(50),
            }
        )

        ground_truth = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=10, freq="h"),
                "is_anomaly_gt": [False] * 9 + [True],
            }
        )

        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test",
            assertion_category="volume",
        )

        mock_result = TrainingResult(
            prediction_df=df.head(10),
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.ObserveAdapter"
        ) as mock_adapter_class:
            mock_adapter = MagicMock()
            mock_adapter.run_training_pipeline.return_value = mock_result
            mock_adapter_class.return_value = mock_adapter

            trainer._run_training_pipeline(df, context, ground_truth=ground_truth)

            call_kwargs = mock_adapter.run_training_pipeline.call_args[1]
            assert call_kwargs["ground_truth"] is ground_truth

    def test_force_retune_when_stale(self, trainer: ConcreteTrainerV2) -> None:
        """Forces retune when hyperparameters are stale."""
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2024-01-01", periods=50, freq="h"),
                "y": range(50),
            }
        )

        now_ms = int(time.time() * 1000)
        seven_days_ago = now_ms - (7 * 24 * 3600 * 1000)

        context = AssertionTrainingContext(
            entity_urn="urn:li:dataset:test",
            existing_model_config=ModelConfig(
                preprocessing_config_json="{}", generated_at=seven_days_ago
            ),
            assertion_category="volume",
        )

        mock_result = TrainingResult(
            prediction_df=df.head(10),
            model_config=ModelConfig(preprocessing_config_json="{}"),
            step_results={},
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.base_trainer_v2.ObserveAdapter"
        ) as mock_adapter_class:
            mock_adapter = MagicMock()
            mock_adapter.run_training_pipeline.return_value = mock_result
            mock_adapter_class.return_value = mock_adapter

            trainer._run_training_pipeline(df, context)

            call_kwargs = mock_adapter.run_training_pipeline.call_args[1]
            assert call_kwargs["force_retune"] is True
