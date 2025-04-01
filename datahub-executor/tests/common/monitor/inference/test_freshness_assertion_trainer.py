import time
from typing import Dict, List, Union, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    FixedIntervalScheduleClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
)

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Operation
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.freshness_assertion_trainer import (
    FRESHNESS_OPERATION_TYPES_TO_IGNORE,
    FreshnessAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.types import (
    Anomaly,
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    AssertionMonitorSensitivity,
    AssertionType,
    Monitor,
)
from datahub_executor.config import (
    FRESHNESS_DEFAULT_SENSITIVITY_LEVEL,
    FRESHNESS_MIN_TRAINING_INTERVAL_SECONDS,
    FRESHNESS_MIN_TRAINING_SAMPLES,
)


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
) -> FreshnessAssertionTrainer:
    """Create a FreshnessAssertionTrainer with mock dependencies."""
    return FreshnessAssertionTrainer(
        cast(DataHubGraph, mock_dependencies["graph"]),
        cast(MetricClient, mock_dependencies["metrics_client"]),
        cast(MetricPredictor, mock_dependencies["metrics_predictor"]),
        cast(MonitorClient, mock_dependencies["monitor_client"]),
    )


@pytest.fixture
def mock_monitor() -> Mock:
    """Create a mock monitor for testing."""
    monitor = Mock(spec=Monitor)
    monitor.urn = "urn:li:dataHubMonitor:test-monitor"
    monitor.assertion_monitor = Mock()
    monitor.assertion_monitor.settings = Mock()
    monitor.assertion_monitor.settings.inference_settings = None

    return monitor


@pytest.fixture
def mock_assertion() -> Mock:
    """Create a mock freshness assertion for testing."""
    assertion = Mock(spec=Assertion)
    assertion.urn = "urn:li:assertion:test-freshness-assertion"
    assertion.type = AssertionType.FRESHNESS
    assertion.entity = Mock()
    assertion.entity.urn = "urn:li:dataset:test-dataset"
    assertion.connection_urn = "urn:li:connection:test-connection"
    assertion.raw_info_aspect = Mock()

    return assertion


@pytest.fixture
def mock_evaluation_spec() -> Mock:
    """Create a mock evaluation spec for testing."""
    evaluation_spec = Mock(spec=AssertionEvaluationSpec)
    evaluation_spec.context = Mock()
    evaluation_spec.context.inference_details = Mock()
    evaluation_spec.context.inference_details.generated_at = None

    return evaluation_spec


@pytest.fixture
def mock_operations_data() -> List[Mock]:
    """Create mock operations data for testing."""
    operations: List[Mock] = []
    for i in range(10):
        operation = Mock(spec=Operation)
        operation.timestamp_ms = i * 3600 * 1000  # Each 1 hour apart
        operation.type = "INSERT"  # Not in ignore list
        operations.append(operation)

    return operations


@pytest.fixture
def mock_anomalies_data() -> List[Anomaly]:
    """Create mock anomalies data for testing."""
    anomalies: List[Anomaly] = []
    for i in range(5):
        anomaly = Mock(spec=Anomaly)
        timestamp_ms = (
            i * 3600 * 1000 - 10
        )  # Subtract to ensure the operation is marked.
        anomaly.timestamp_ms = timestamp_ms
        anomalies.append(cast(Anomaly, anomaly))

    return anomalies


@pytest.fixture
def mock_fixed_interval() -> Mock:
    """Create a mock FixedIntervalScheduleClass for testing."""
    fixed_interval = Mock(spec=FixedIntervalScheduleClass)
    fixed_interval.unit = "HOUR"
    fixed_interval.multiple = 6  # Every 6 hours

    return fixed_interval


@pytest.fixture
def mock_freshness_assertion_info() -> AssertionInfoClass:
    """Create a mock AssertionInfoClass for a freshness assertion."""
    freshness_assertion = FreshnessAssertionInfoClass(
        type=FreshnessAssertionTypeClass.DATASET_CHANGE,
        entity="urn:li:dataset:test-dataset",
        schedule=FreshnessAssertionScheduleClass(
            type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            fixedInterval=FixedIntervalScheduleClass(
                unit="HOUR",
                multiple=6,
            ),
        ),
    )

    return AssertionInfoClass(
        type="FRESHNESS",
        freshnessAssertion=freshness_assertion,
    )


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer.perform_training"
)
def test_train(
    mock_perform_training: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that train method calls perform_training."""
    # Act
    trainer.train(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    mock_perform_training.assert_called_once_with(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )


def test_should_perform_inference_no_previous_inference(
    trainer: FreshnessAssertionTrainer,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that inference should be performed when there's no previous inference."""
    # Arrange
    mock_evaluation_spec.context.inference_details = None

    # Act
    result = trainer.should_perform_inference(
        cast(AssertionEvaluationSpec, mock_evaluation_spec)
    )

    # Assert
    assert result is True


def test_should_perform_inference_recent_inference(
    trainer: FreshnessAssertionTrainer,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that inference should be skipped when recent inference exists."""
    # Arrange
    now_ms = int(time.time() * 1000)
    mock_evaluation_spec.context.inference_details.generated_at = (
        now_ms - 1000
    )  # 1 second ago

    # Act
    result = trainer.should_perform_inference(
        cast(AssertionEvaluationSpec, mock_evaluation_spec)
    )

    # Assert
    assert result is False


def test_should_perform_inference_old_inference(
    trainer: FreshnessAssertionTrainer,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that inference should be performed when previous inference is old."""
    # Arrange
    now_ms = int(time.time() * 1000)
    # Set last inference to be older than the min interval
    mock_evaluation_spec.context.inference_details.generated_at = (
        now_ms - (FRESHNESS_MIN_TRAINING_INTERVAL_SECONDS + 10) * 1000
    )

    # Act
    result = trainer.should_perform_inference(
        cast(AssertionEvaluationSpec, mock_evaluation_spec)
    )

    # Assert
    assert result is True


def test_get_min_training_samples(
    trainer: FreshnessAssertionTrainer,
) -> None:
    """Test that get_min_training_samples returns the correct constant."""
    # Act
    result = trainer.get_min_training_samples()

    # Assert
    assert result == FRESHNESS_MIN_TRAINING_SAMPLES


def test_get_metric_data(
    trainer: FreshnessAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_operations_data: List[Mock],
) -> None:
    """Test that get_metric_data calls the correct methods with appropriate parameters."""
    # Arrange
    mock_dependencies[
        "metrics_client"
    ].fetch_operations.return_value = mock_operations_data

    # Act
    result = trainer.get_metric_data(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        None,
    )

    # Assert
    mock_dependencies["metrics_client"].fetch_operations.assert_called_once()

    # Check that the entity_urn and ignore_types parameters are correct
    call_args = mock_dependencies["metrics_client"].fetch_operations.call_args[1]

    assert call_args["entity_urn"] == mock_assertion.entity.urn
    assert call_args["ignore_types"] == FRESHNESS_OPERATION_TYPES_TO_IGNORE

    assert result == mock_operations_data


def test_get_metric_data_with_anomalies(
    trainer: FreshnessAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_operations_data: List[Mock],
    mock_anomalies_data: List[Anomaly],
) -> None:
    """Test that get_metric_data correct applies anomaly status to operations."""
    # Arrange
    mock_dependencies[
        "metrics_client"
    ].fetch_operations.return_value = mock_operations_data
    mock_dependencies[
        "monitor_client"
    ].fetch_monitor_anomalies.return_value = mock_anomalies_data

    # Act
    result = trainer.get_metric_data(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        None,
    )

    # Assert
    mock_dependencies["metrics_client"].fetch_operations.assert_called_once()
    mock_dependencies["monitor_client"].fetch_monitor_anomalies.assert_called_once()

    # Check that the entity_urn and ignore_types parameters are correct
    call_args = mock_dependencies["metrics_client"].fetch_operations.call_args[1]

    assert call_args["entity_urn"] == mock_assertion.entity.urn
    assert call_args["ignore_types"] == FRESHNESS_OPERATION_TYPES_TO_IGNORE

    # For the first 5 operations, ensure they are marked as anomalous.
    # And ensure the rest is unchanged.
    for i in range(0, 5):
        assert result[i].is_anomaly is True
        assert result[i].timestamp_ms == mock_operations_data[i].timestamp_ms
        assert result[i].type == mock_operations_data[i].type

    # Assert that the second half of operations remained untouched.
    assert result[5:] == mock_operations_data[5:]


def test_remove_inferred_assertion(
    trainer: FreshnessAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test remove_inferred_assertion updates the context correctly."""
    # Act
    trainer.remove_inferred_assertion(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    mock_dependencies[
        "monitor_client"
    ].patch_freshness_monitor_evaluation_context.assert_called_once()

    # Verify the context has been reset
    context_arg = mock_dependencies[
        "monitor_client"
    ].patch_freshness_monitor_evaluation_context.call_args[0][2]
    assert isinstance(context_arg, AssertionEvaluationContextClass)
    assert context_arg.inferenceDetails.generatedAt == 0  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer._get_sensitivity_level"
)
@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer._get_assertion_info"
)
@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer._update_freshness_monitor_evaluation_context"
)
@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer._rebuild_assertion"
)
def test_train_and_update_assertion(
    mock_rebuild_assertion: MagicMock,
    mock_update_context: MagicMock,
    mock_get_assertion_info: MagicMock,
    mock_get_sensitivity_level: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_operations_data: List[Mock],
    mock_fixed_interval: Mock,
    mock_freshness_assertion_info: AssertionInfoClass,
    mock_evaluation_spec: Mock,
) -> None:
    """Test train_and_update_assertion with successful prediction."""
    # Arrange
    sensitivity = 2
    mock_get_sensitivity_level.return_value = sensitivity
    mock_dependencies[
        "metrics_predictor"
    ].predict_fixed_interval_schedule.return_value = mock_fixed_interval
    mock_get_assertion_info.return_value = mock_freshness_assertion_info
    mock_rebuild_assertion.return_value = cast(Assertion, mock_assertion)

    # Act
    result = trainer.train_and_update_assertion(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        cast(List[Operation], mock_operations_data),
        None,
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    # Check sensitivity level was retrieved
    mock_get_sensitivity_level.assert_called_once_with(None)

    # Check fixed interval was predicted
    mock_dependencies[
        "metrics_predictor"
    ].predict_fixed_interval_schedule.assert_called_once_with(
        mock_operations_data, sensitivity
    )

    # Check assertion info was retrieved
    mock_get_assertion_info.assert_called_once_with(mock_assertion)

    # Check freshness assertion was set with new schedule
    assert mock_freshness_assertion_info.freshnessAssertion is not None

    # Check evaluation context was updated
    mock_update_context.assert_called_once_with(
        mock_monitor,
        mock_assertion,
        mock_evaluation_spec,
    )

    # Check assertion info was updated
    mock_dependencies["monitor_client"].update_assertion_info.assert_called_once_with(
        mock_assertion.urn, mock_freshness_assertion_info
    )

    # Check assertion was rebuilt
    mock_rebuild_assertion.assert_called_once_with(
        mock_assertion, mock_freshness_assertion_info
    )

    # Check result
    assert result == mock_assertion


def test_get_sensitivity_level_with_settings(
    trainer: FreshnessAssertionTrainer,
) -> None:
    """Test getting sensitivity level from adjustment settings."""
    # Arrange
    adjustment_settings = Mock(spec=AssertionAdjustmentSettings)
    adjustment_settings.sensitivity = Mock(spec=AssertionMonitorSensitivity)
    adjustment_settings.sensitivity.level = 3

    # Act
    result = trainer._get_sensitivity_level(adjustment_settings)

    # Assert
    assert result == 3


def test_get_sensitivity_level_no_settings(
    trainer: FreshnessAssertionTrainer,
) -> None:
    """Test getting default sensitivity level when no settings provided."""
    # Act
    result = trainer._get_sensitivity_level(None)

    # Assert
    assert result == FRESHNESS_DEFAULT_SENSITIVITY_LEVEL


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.get_assertion_info"
)
def test_get_assertion_info_success(
    mock_get_assertion_info: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_assertion: Mock,
    mock_freshness_assertion_info: AssertionInfoClass,
) -> None:
    """Test successfully getting assertion info."""
    # Arrange
    mock_get_assertion_info.return_value = mock_freshness_assertion_info

    # Act
    result = trainer._get_assertion_info(cast(Assertion, mock_assertion))

    # Assert
    mock_get_assertion_info.assert_called_once_with(mock_assertion.raw_info_aspect)
    assert result == mock_freshness_assertion_info


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.get_assertion_info"
)
def test_get_assertion_info_missing(
    mock_get_assertion_info: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_assertion: Mock,
) -> None:
    """Test error when assertion info is missing."""
    # Arrange
    mock_get_assertion_info.return_value = None

    # Act & Assert
    with pytest.raises(
        RuntimeError,
        match=f"Missing raw assertionInfo aspect for assertion {mock_assertion.urn}",
    ):
        trainer._get_assertion_info(cast(Assertion, mock_assertion))


def test_build_fixed_interval_freshness_assertion_info(
    trainer: FreshnessAssertionTrainer,
    mock_fixed_interval: Mock,
) -> None:
    """Test building freshness assertion info with fixed interval."""
    # Arrange
    entity_urn = "urn:li:dataset:test-dataset"

    # Act
    result = trainer._build_fixed_interval_freshness_assertion_info(
        entity_urn,
        cast(FixedIntervalScheduleClass, mock_fixed_interval),
    )

    # Assert
    assert isinstance(result, FreshnessAssertionInfoClass)
    assert result.type == FreshnessAssertionTypeClass.DATASET_CHANGE
    assert result.entity == entity_urn

    assert result.schedule is not None
    assert result.schedule.type == FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL
    assert result.schedule.fixedInterval == mock_fixed_interval


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.FreshnessAssertionTrainer.create_inference_details"
)
def test_update_freshness_monitor_evaluation_context(
    mock_create_inference_details: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test updating freshness monitor evaluation context."""
    # Arrange
    inference_details = Mock()
    mock_create_inference_details.return_value = inference_details

    # Act
    trainer._update_freshness_monitor_evaluation_context(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    # Check that inference details were created with current timestamp
    mock_create_inference_details.assert_called_once()

    # Check that context was updated
    mock_dependencies[
        "monitor_client"
    ].patch_freshness_monitor_evaluation_context.assert_called_once()

    # Check arguments to patch_freshness_monitor_evaluation_context
    call_args = mock_dependencies[
        "monitor_client"
    ].patch_freshness_monitor_evaluation_context.call_args[0]
    assert call_args[0] == mock_monitor.urn
    assert call_args[1] == mock_assertion.urn
    assert isinstance(call_args[2], AssertionEvaluationContextClass)
    assert call_args[2].inferenceDetails == inference_details
    embedded_assertions = call_args[2].embeddedAssertions
    assert embedded_assertions is not None
    assert len(embedded_assertions) == 0
    assert call_args[3] == mock_evaluation_spec


@patch(
    "datahub_executor.common.monitor.inference.freshness_assertion_trainer.Assertion"
)
def test_rebuild_assertion(
    mock_assertion_class: MagicMock,
    trainer: FreshnessAssertionTrainer,
    mock_assertion: Mock,
    mock_freshness_assertion_info: AssertionInfoClass,
) -> None:
    """Test rebuilding an assertion with updated info."""
    # Arrange
    mock_assertion_class.parse_obj.return_value = cast(Assertion, mock_assertion)

    # Act
    result = trainer._rebuild_assertion(
        cast(Assertion, mock_assertion),
        mock_freshness_assertion_info,
    )

    # Assert
    mock_assertion_class.parse_obj.assert_called_once()

    # Check that the correct parameters were passed to parse_obj
    parsed_obj = mock_assertion_class.parse_obj.call_args[0][0]
    assert parsed_obj["urn"] == mock_assertion.urn
    assert parsed_obj["entity"] == mock_assertion.entity
    assert parsed_obj["connectionUrn"] == mock_assertion.connection_urn
    assert parsed_obj["raw_info_aspect"] is None

    # Check result
    assert result == mock_assertion
