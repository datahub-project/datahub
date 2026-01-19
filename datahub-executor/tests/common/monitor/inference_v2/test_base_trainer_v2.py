"""Tests for BaseTrainerV2 utility methods."""

import pytest

pytest.importorskip("datahub_observe")

import time
from typing import Optional
from unittest.mock import MagicMock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import BaseTrainerV2
from datahub_executor.common.monitor.inference_v2.types import AssertionTrainingContext
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

    def test_sorts_by_timestamp(self, trainer: ConcreteTrainerV2) -> None:
        """Output DataFrame is sorted by 'ds' column."""
        metrics = [
            Metric(timestamp_ms=3000, value=30.0),
            Metric(timestamp_ms=1000, value=10.0),
            Metric(timestamp_ms=2000, value=20.0),
        ]

        training_df, ground_truth_df = trainer._build_training_dataframe(metrics, [])

        assert list(training_df["y"]) == [10.0, 20.0, 30.0]

    def test_empty_metrics(self, trainer: ConcreteTrainerV2) -> None:
        """Empty metrics list produces empty DataFrame."""
        training_df, ground_truth_df = trainer._build_training_dataframe([], [])

        assert training_df.empty
        assert ground_truth_df.empty


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
        eval_spec.context.inference_details.confidence = 0.95
        eval_spec.context.inference_details.generatedAt = 1234567890

        result = trainer._extract_existing_model_config(eval_spec)

        assert result is not None
        assert result.forecast_model_name == "prophet"

    def test_returns_none_when_no_context(self, trainer: ConcreteTrainerV2) -> None:
        """Returns None when evaluation_spec has no context."""
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        result = trainer._extract_existing_model_config(eval_spec)
        assert result is None
