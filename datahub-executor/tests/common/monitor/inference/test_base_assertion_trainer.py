from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    AssertionExclusionWindow,
    Monitor,
)
from datahub_executor.config import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
)


class TestableAssertionTrainer(BaseAssertionTrainer[Metric]):
    """A concrete implementation of BaseAssertionTrainer for testing purposes."""

    def __init__(
        self,
        graph: DataHubGraph,
        metrics_client: MetricClient,
        metrics_predictor: MetricPredictor,
        monitor_client: MonitorClient,
    ) -> None:
        super().__init__(graph, metrics_client, metrics_predictor, monitor_client)
        # Initialize attributes for testing
        self.should_perform_inference_flag: bool = True
        self.mock_metric_data: List[Metric] = []
        self.remove_called: bool = False
        self.train_called: bool = False

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        # Simple implementation that delegates to perform_training
        self.perform_training(monitor, assertion, evaluation_spec)

    def should_perform_inference(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> bool:
        # For testing, we'll just return a value set during test setup
        return self.should_perform_inference_flag

    def get_min_training_samples_timespan_seconds(self) -> int:
        # For testing, return a fixed amount of seconds
        return 5

    def get_min_training_samples(self) -> int:
        # For testing, return a fixed value
        return 5

    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Metric]:
        # For testing, return mock data set during test setup
        return self.mock_metric_data

    def remove_inferred_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        # For testing, just record that this was called
        self.remove_called = True

    def train_and_update_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        events: List[Metric],
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> Assertion:
        # For testing, just record that this was called
        self.train_called = True
        return assertion


@pytest.fixture
def mock_dependencies() -> Dict[str, Union[MagicMock, Mock]]:
    """Create mock dependencies for the trainer."""
    return {
        "graph": MagicMock(spec=DataHubGraph),
        "metrics_client": MagicMock(spec=MetricClient),
        "metrics_predictor": MagicMock(spec=MetricPredictor),
        "monitor_client": MagicMock(spec=MonitorClient),
    }


@pytest.fixture
def trainer(
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
) -> TestableAssertionTrainer:
    """Create a TestableAssertionTrainer with mock dependencies."""
    trainer = TestableAssertionTrainer(
        cast(DataHubGraph, mock_dependencies["graph"]),
        cast(MetricClient, mock_dependencies["metrics_client"]),
        cast(MetricPredictor, mock_dependencies["metrics_predictor"]),
        cast(MonitorClient, mock_dependencies["monitor_client"]),
    )
    # Reset state trackers
    trainer.remove_called = False
    trainer.train_called = False
    return trainer


@pytest.fixture
def mock_data() -> Dict[str, Union[Mock, MagicMock]]:
    """Create mock monitor, assertion, and evaluation spec for testing."""
    mock_monitor = Mock(spec=Monitor)
    mock_monitor.urn = "urn:li:dataHubMonitor:test-monitor"
    mock_monitor.assertion_monitor = Mock()
    mock_monitor.assertion_monitor.settings = Mock()
    mock_monitor.assertion_monitor.settings.inference_settings = None

    mock_assertion = Mock(spec=Assertion)
    mock_assertion.urn = "urn:li:assertion:test-assertion"
    mock_assertion.entity = Mock()
    mock_assertion.entity.urn = "urn:li:dataset:test-dataset"

    mock_evaluation_spec = Mock(spec=AssertionEvaluationSpec)
    mock_evaluation_spec.context = Mock()
    mock_evaluation_spec.context.inference_details = Mock()

    return {
        "monitor": mock_monitor,
        "assertion": mock_assertion,
        "evaluation_spec": mock_evaluation_spec,
    }


def test_perform_training_no_monitor_urn(
    trainer: TestableAssertionTrainer, mock_data: Dict[str, Union[Mock, MagicMock]]
) -> None:
    """Test that training exits early if monitor has no URN."""
    # Arrange
    mock_data["monitor"].urn = None

    # Act
    trainer.perform_training(
        cast(Monitor, mock_data["monitor"]),
        cast(Assertion, mock_data["assertion"]),
        cast(AssertionEvaluationSpec, mock_data["evaluation_spec"]),
    )

    # Assert
    assert not trainer.remove_called
    assert not trainer.train_called


def test_perform_training_skip_inference(
    trainer: TestableAssertionTrainer, mock_data: Dict[str, Union[Mock, MagicMock]]
) -> None:
    """Test that training exits early if inference should be skipped."""
    # Arrange
    trainer.should_perform_inference_flag = False

    # Act
    trainer.perform_training(
        cast(Monitor, mock_data["monitor"]),
        cast(Assertion, mock_data["assertion"]),
        cast(AssertionEvaluationSpec, mock_data["evaluation_spec"]),
    )

    # Assert
    assert not trainer.remove_called
    assert not trainer.train_called


def test_perform_training_not_enough_samples(
    trainer: TestableAssertionTrainer, mock_data: Dict[str, Union[Mock, MagicMock]]
) -> None:
    """Test that remove_inferred_assertion is called if there's not enough training data."""
    # Arrange
    trainer.should_perform_inference_flag = True
    # Create mock metrics with minimum data to be properly typed
    mock_metrics: List[Metric] = []
    for i in range(4):  # Less than min 5
        metric = Mock(spec=Metric)
        metric.timestamp_ms = 1000 * i  # Add required attribute
        mock_metrics.append(cast(Metric, metric))

    trainer.mock_metric_data = mock_metrics

    # Act
    trainer.perform_training(
        cast(Monitor, mock_data["monitor"]),
        cast(Assertion, mock_data["assertion"]),
        cast(AssertionEvaluationSpec, mock_data["evaluation_spec"]),
    )

    # Assert
    assert trainer.remove_called
    assert not trainer.train_called


def test_perform_training_not_enough_time_between_samples(
    trainer: TestableAssertionTrainer, mock_data: Dict[str, Union[Mock, MagicMock]]
) -> None:
    """Test that remove_inferred_assertion is called if there's not enough training data."""
    # Arrange
    trainer.should_perform_inference_flag = True
    # Create mock metrics with minimum data to be properly typed
    mock_metrics: List[Metric] = []
    for _ in range(4):  # Less than min 5
        metric = Mock(spec=Metric)
        metric.timestamp_ms = 1000  # All have the same timestamp!
        mock_metrics.append(cast(Metric, metric))

    trainer.mock_metric_data = mock_metrics

    # Act
    trainer.perform_training(
        cast(Monitor, mock_data["monitor"]),
        cast(Assertion, mock_data["assertion"]),
        cast(AssertionEvaluationSpec, mock_data["evaluation_spec"]),
    )

    # Assert
    assert trainer.remove_called
    assert not trainer.train_called


def test_perform_training_enough_samples_and_time(
    trainer: TestableAssertionTrainer, mock_data: Dict[str, Union[Mock, MagicMock]]
) -> None:
    """Test that train_and_update_assertion is called if there's enough training data."""
    # Arrange
    trainer.should_perform_inference_flag = True
    # Create mock metrics with minimum data to be properly typed
    mock_metrics: List[Metric] = []
    for i in range(10):  # More than min 5
        metric = Mock(spec=Metric)
        metric.timestamp_ms = (
            1000 * i
        )  # Add time between samples, more than min 5 minutes
        mock_metrics.append(cast(Metric, metric))

    trainer.mock_metric_data = mock_metrics

    # Act
    trainer.perform_training(
        cast(Monitor, mock_data["monitor"]),
        cast(Assertion, mock_data["assertion"]),
        cast(AssertionEvaluationSpec, mock_data["evaluation_spec"]),
    )

    # Assert
    assert not trainer.remove_called
    assert trainer.train_called


def test_extract_lookback_days_from_adjustment_settings_with_settings(
    trainer: TestableAssertionTrainer,
) -> None:
    """Test extracting lookback days from adjustment settings."""
    # Arrange
    adjustment_settings = Mock(spec=AssertionAdjustmentSettings)
    adjustment_settings.training_data_lookback_window_days = 30

    # Act
    result = trainer.extract_lookback_days_from_adjustment_settings(adjustment_settings)

    # Assert
    assert result == 30


def test_extract_lookback_days_from_adjustment_settings_no_settings(
    trainer: TestableAssertionTrainer,
) -> None:
    """Test extracting default lookback days when no adjustment settings are provided."""
    # Act
    result = trainer.extract_lookback_days_from_adjustment_settings(None)

    # Assert
    assert result == ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS


def test_filter_training_timeseries_no_exclusions(
    trainer: TestableAssertionTrainer,
) -> None:
    """Test that timeseries is not filtered when no exclusion windows are provided."""
    # Arrange
    mock_metrics: List[Metric] = []
    for _ in range(5):
        metric = Mock(spec=Metric)
        metric.timestamp_ms = 1000  # Add required attribute
        mock_metrics.append(cast(Metric, metric))

    # Act
    result = trainer.filter_training_timeseries(mock_metrics, None)

    # Assert
    assert result == mock_metrics


def test_filter_training_timeseries_with_fixed_range_exclusion(
    trainer: TestableAssertionTrainer,
) -> None:
    """Test filtering timeseries with fixed range exclusion."""
    # Arrange
    timeseries: List[Metric] = []
    for i in range(5):
        metric = Mock(spec=Metric)
        # Set consistent timestamp for each metric
        timestamp = 1000 + i * 1000
        metric.timestamp_ms = timestamp
        timeseries.append(cast(Metric, metric))

    # Create exclusion window for timestamps 2000-3000
    exclusion_window = Mock(spec=AssertionExclusionWindow)
    exclusion_window.fixed_range = Mock()
    exclusion_window.fixed_range.startTimeMillis = 2000
    exclusion_window.fixed_range.endTimeMillis = 3000
    exclusion_window.weekly = None

    adjustment_settings = Mock(spec=AssertionAdjustmentSettings)
    adjustment_settings.exclusion_windows = [exclusion_window]

    # Act
    result = trainer.filter_training_timeseries(timeseries, adjustment_settings)

    # Assert
    assert len(result) == 3  # 5 original - 2 excluded

    # These indices should be included (outside exclusion window)
    assert result[0].timestamp_ms == 1000
    assert result[1].timestamp_ms == 4000  # Explicitly set timestamp earlier
    assert result[2].timestamp_ms == 5000  # Explicitly set timestamp earlier


def test_filter_training_timeseries_with_weekly_exclusion(
    trainer: TestableAssertionTrainer,
) -> None:
    """Test filtering timeseries with weekly exclusion."""
    # Arrange
    # We'll configure each metric with a specific datetime to control the test
    saturday_datetime = datetime(2023, 1, 7, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
    sunday_datetime = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)  # Sunday

    saturday_timestamp_ms = int(saturday_datetime.timestamp() * 1000)
    sunday_timestamp_ms = int(sunday_datetime.timestamp() * 1000)

    # Create metrics with specific timestamps
    saturday_metric = Mock(spec=Metric)
    saturday_metric.timestamp_ms = saturday_timestamp_ms

    sunday_metric = Mock(spec=Metric)
    sunday_metric.timestamp_ms = sunday_timestamp_ms

    timeseries: List[Metric] = [
        cast(Metric, saturday_metric),
        cast(Metric, sunday_metric),
    ]

    # Create weekly exclusion for Saturdays
    exclusion_window = Mock(spec=AssertionExclusionWindow)
    exclusion_window.fixed_range = None
    exclusion_window.weekly = Mock()
    exclusion_window.weekly.days_of_week = ["SATURDAY"]
    exclusion_window.weekly.timezone = "UTC"
    exclusion_window.weekly.start_time = "00:00"
    exclusion_window.weekly.end_time = "23:59"

    adjustment_settings = Mock(spec=AssertionAdjustmentSettings)
    adjustment_settings.exclusion_windows = [exclusion_window]

    # Mock datetime behavior for the _is_excluded_by_weekly method
    with patch("datetime.datetime") as mock_datetime:
        # Configure datetime.fromtimestamp to return our pre-determined dates
        mock_datetime.fromtimestamp.side_effect = lambda ts, tz: (
            saturday_datetime
            if ts == saturday_timestamp_ms // 1000
            else sunday_datetime
        )
        mock_datetime.strptime.side_effect = datetime.strptime

        # Act
        result = trainer.filter_training_timeseries(timeseries, adjustment_settings)

    # Assert
    assert len(result) == 1  # Only Sunday metric remains
    assert result[0].timestamp_ms == sunday_timestamp_ms


def test_create_inference_details(trainer: TestableAssertionTrainer) -> None:
    """Test creating inference details with specific timestamp."""
    # Act
    result = trainer.create_inference_details(12345)

    # Assert
    assert result.modelId == "online-predictor-v1"
    assert result.modelVersion == "v1"
    assert result.generatedAt == 12345
    assert result.confidence is None
    assert result.parameters is None
    assert result.adjustmentSettings is None


def test_create_empty_context(trainer: TestableAssertionTrainer) -> None:
    """Test creating empty evaluation context."""
    # Act
    result = trainer.create_empty_context(12345)

    # Assert
    # Safely check the length of embeddedAssertions list with proper typing
    embedded_assertions = result.embeddedAssertions
    assert isinstance(embedded_assertions, list)
    assert len(embedded_assertions) == 0

    # Check inference details
    assert result.inferenceDetails is not None
    assert result.inferenceDetails.generatedAt == 12345


@patch(
    "datahub_executor.common.assertion.engine.evaluator.utils.shared.encode_monitor_urn"
)
def test_get_metric_cube_urn(
    mock_encode_monitor_urn: MagicMock, trainer: TestableAssertionTrainer
) -> None:
    """Test getting metric cube URN for a monitor."""
    # Arrange
    mock_encode_monitor_urn.return_value = "encoded-monitor-urn"

    # Act
    result = trainer.get_metric_cube_urn("test-monitor-urn")

    # Assert
    mock_encode_monitor_urn.assert_called_once_with("test-monitor-urn")
    assert result == "urn:li:dataHubMetricCube:encoded-monitor-urn"
