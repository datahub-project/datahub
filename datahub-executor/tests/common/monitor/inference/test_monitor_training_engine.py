from typing import Dict, Union, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.field_assertion_trainer import (
    FieldAssertionTrainer,
)
from datahub_executor.common.monitor.inference.freshness_assertion_trainer import (
    FreshnessAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.monitor_training_engine import (
    MonitorTrainingEngine,
)
from datahub_executor.common.monitor.inference.volume_assertion_trainer import (
    VolumeAssertionTrainer,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationSpec,
    AssertionType,
    Monitor,
)


@pytest.fixture
def mock_dependencies() -> Dict[str, Union[MagicMock, Mock]]:
    """Create mock dependencies for the engine."""
    return {
        "graph": MagicMock(spec=DataHubGraph),
        "metrics_client": MagicMock(spec=MetricClient),
        "metrics_predictor": MagicMock(spec=MetricPredictor),
        "monitor_client": MagicMock(spec=MonitorClient),
    }


@pytest.fixture
def engine(
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
) -> MonitorTrainingEngine:
    """Create a MonitorTrainingEngine with mock dependencies."""
    engine = MonitorTrainingEngine(
        cast(DataHubGraph, mock_dependencies["graph"]),
        cast(MetricClient, mock_dependencies["metrics_client"]),
        cast(MetricPredictor, mock_dependencies["metrics_predictor"]),
        cast(MonitorClient, mock_dependencies["monitor_client"]),
    )

    # Replace the trainers with mocks
    engine._trainers = {  # type: ignore
        AssertionType.VOLUME: MagicMock(spec=VolumeAssertionTrainer),
        AssertionType.FRESHNESS: MagicMock(spec=FreshnessAssertionTrainer),
        AssertionType.FIELD: MagicMock(spec=FieldAssertionTrainer),
    }

    return engine


@pytest.fixture
def volume_monitor() -> Mock:
    """Create a mock monitor with a volume assertion."""
    volume_assertion = Mock(spec=Assertion)
    volume_assertion.type = AssertionType.VOLUME
    volume_assertion.urn = "urn:li:assertion:volume-assertion"

    volume_eval_spec = Mock(spec=AssertionEvaluationSpec)
    volume_eval_spec.assertion = volume_assertion

    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:volume-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.assertions = [volume_eval_spec]

    return monitor


@pytest.fixture
def freshness_monitor() -> Mock:
    """Create a mock monitor with a freshness assertion."""
    freshness_assertion = Mock(spec=Assertion)
    freshness_assertion.type = AssertionType.FRESHNESS
    freshness_assertion.urn = "urn:li:assertion:freshness-assertion"

    freshness_eval_spec = Mock(spec=AssertionEvaluationSpec)
    freshness_eval_spec.assertion = freshness_assertion

    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:freshness-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.assertions = [freshness_eval_spec]

    return monitor


@pytest.fixture
def field_monitor() -> Mock:
    """Create a mock monitor with a field assertion."""
    field_assertion = Mock(spec=Assertion)
    field_assertion.type = AssertionType.FIELD
    field_assertion.urn = "urn:li:assertion:field-assertion"

    field_eval_spec = Mock(spec=AssertionEvaluationSpec)
    field_eval_spec.assertion = field_assertion

    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:field-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.assertions = [field_eval_spec]

    return monitor


@pytest.fixture
def unsupported_monitor() -> Mock:
    """Create a mock monitor with an unsupported assertion type."""
    unsupported_assertion = Mock(spec=Assertion)
    unsupported_assertion.type = "UNSUPPORTED"  # type: ignore  # For testing unsupported types
    unsupported_assertion.urn = "urn:li:assertion:unsupported-assertion"

    unsupported_eval_spec = Mock(spec=AssertionEvaluationSpec)
    unsupported_eval_spec.assertion = unsupported_assertion

    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:unsupported-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.assertions = [unsupported_eval_spec]

    return monitor


@pytest.fixture
def empty_monitor() -> Mock:
    """Create a mock monitor with no assertions."""
    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:empty-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.assertions = []

    return monitor


@pytest.fixture
def null_monitor() -> Mock:
    """Create a mock monitor with no assertion_monitor."""
    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:null-monitor"
    monitor.assertion_monitor = None

    return monitor


def test_initialize_trainers(
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
) -> None:
    """Test that trainers are properly initialized."""
    # Act
    engine = MonitorTrainingEngine(
        cast(DataHubGraph, mock_dependencies["graph"]),
        cast(MetricClient, mock_dependencies["metrics_client"]),
        cast(MetricPredictor, mock_dependencies["metrics_predictor"]),
        cast(MonitorClient, mock_dependencies["monitor_client"]),
    )

    # Assert
    assert len(engine._trainers) == 3  # Three trainer types

    assert AssertionType.VOLUME in engine._trainers
    assert isinstance(engine._trainers[AssertionType.VOLUME], VolumeAssertionTrainer)

    assert AssertionType.FRESHNESS in engine._trainers
    assert isinstance(
        engine._trainers[AssertionType.FRESHNESS], FreshnessAssertionTrainer
    )

    assert AssertionType.FIELD in engine._trainers
    assert isinstance(engine._trainers[AssertionType.FIELD], FieldAssertionTrainer)


def test_get_trainer(engine: MonitorTrainingEngine) -> None:
    """Test getting trainers by assertion type."""
    # Act & Assert
    assert engine.get_trainer(AssertionType.VOLUME) is not None
    assert engine.get_trainer(AssertionType.FRESHNESS) is not None
    assert engine.get_trainer(AssertionType.FIELD) is not None
    assert engine.get_trainer("UNSUPPORTED") is None  # type: ignore  # For testing unsupported types


def test_train_null_monitor(engine: MonitorTrainingEngine, null_monitor: Mock) -> None:
    """Test train method when monitor has no assertion_monitor."""
    # Act
    engine.train(null_monitor)

    # Assert - ensure no trainers were called
    for trainer in engine._trainers.values():
        trainer.train.assert_not_called()  # type: ignore


def test_train_empty_monitor(
    engine: MonitorTrainingEngine, empty_monitor: Mock
) -> None:
    """Test train method when monitor has empty assertions list."""
    # Act
    engine.train(empty_monitor)

    # Assert - ensure no trainers were called
    for trainer in engine._trainers.values():
        trainer.train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_volume_monitor(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    volume_monitor: Mock,
) -> None:
    """Test train method with a volume assertion monitor."""
    # Arrange
    mock_is_training_required.return_value = True

    # Act
    engine.train(volume_monitor)

    # Assert
    volume_trainer = engine._trainers[AssertionType.VOLUME]
    volume_trainer.train.assert_called_once()  # type: ignore

    # Other trainers should not be called
    engine._trainers[AssertionType.FRESHNESS].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FIELD].train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_freshness_monitor(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    freshness_monitor: Mock,
) -> None:
    """Test train method with a freshness assertion monitor."""
    # Arrange
    mock_is_training_required.return_value = True

    # Act
    engine.train(freshness_monitor)

    # Assert
    freshness_trainer = engine._trainers[AssertionType.FRESHNESS]
    freshness_trainer.train.assert_called_once()  # type: ignore

    # Other trainers should not be called
    engine._trainers[AssertionType.VOLUME].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FIELD].train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_field_monitor(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    field_monitor: Mock,
) -> None:
    """Test train method with a field assertion monitor."""
    # Arrange
    mock_is_training_required.return_value = True

    # Act
    engine.train(field_monitor)

    # Assert
    field_trainer = engine._trainers[AssertionType.FIELD]
    field_trainer.train.assert_called_once()  # type: ignore

    # Other trainers should not be called
    engine._trainers[AssertionType.VOLUME].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FRESHNESS].train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_unsupported_monitor(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    unsupported_monitor: Mock,
) -> None:
    """Test train method with an unsupported assertion type."""
    # Arrange
    mock_is_training_required.return_value = True

    # Act
    engine.train(unsupported_monitor)

    # Assert - no trainers should be called
    engine._trainers[AssertionType.VOLUME].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FRESHNESS].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FIELD].train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_skip_non_inference_monitor(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    volume_monitor: Mock,
) -> None:
    """Test train method skips assertions not marked for inference."""
    # Arrange
    mock_is_training_required.return_value = False

    # Act
    engine.train(volume_monitor)

    # Assert - no trainers should be called
    engine._trainers[AssertionType.VOLUME].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FRESHNESS].train.assert_not_called()  # type: ignore
    engine._trainers[AssertionType.FIELD].train.assert_not_called()  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_handles_trainer_exception(
    mock_is_training_required: MagicMock,
    engine: MonitorTrainingEngine,
    volume_monitor: Mock,
) -> None:
    """Test train method handles exceptions from trainers."""
    # Arrange
    mock_is_training_required.return_value = True

    # Make the volume trainer raise an exception
    engine._trainers[AssertionType.VOLUME].train.side_effect = Exception(  # type: ignore
        "Test exception"
    )

    # Act
    engine.train(volume_monitor)

    # Assert - should have tried to call the trainer despite the exception
    engine._trainers[AssertionType.VOLUME].train.assert_called_once()  # type: ignore


@patch("datahub_executor.common.monitor.inference.monitor_training_engine.logger")
@patch(
    "datahub_executor.common.monitor.inference.monitor_training_engine.is_training_required"
)
def test_train_logs_properly(
    mock_is_training_required: MagicMock,
    mock_logger: MagicMock,
    engine: MonitorTrainingEngine,
    volume_monitor: Mock,
) -> None:
    """Test train method logs properly."""
    # Arrange
    mock_is_training_required.return_value = True

    # Act
    engine.train(volume_monitor)

    # Assert
    # Check for log messages
    mock_logger.info.assert_any_call(
        f"Starting training run for monitor {volume_monitor.urn}"
    )
    mock_logger.info.assert_any_call(
        f"Completed training run for monitor {volume_monitor.urn}!"
    )
