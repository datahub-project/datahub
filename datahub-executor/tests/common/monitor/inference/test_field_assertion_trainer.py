import time
from datetime import timedelta
from typing import Dict, List, Union, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    EmbeddedAssertionClass,
    FieldAssertionInfoClass,
    FieldAssertionTypeClass,
    FieldMetricAssertionClass,
    FieldMetricTypeClass,
    SchemaFieldSpecClass,
)

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.field_assertion_trainer import (
    FieldAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
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
    FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL,
    FIELD_METRIC_MIN_TRAINING_INTERVAL_SECONDS,
    FIELD_METRIC_MIN_TRAINING_SAMPLES,
    FIELD_METRIC_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS,
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
) -> FieldAssertionTrainer:
    """Create a FieldAssertionTrainer with mock dependencies."""
    return FieldAssertionTrainer(
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
def mock_schema_field() -> Mock:
    """Create a mock schema field for testing."""
    field = Mock(spec=SchemaFieldSpecClass)
    field.fieldPath = "test_field"
    return field


@pytest.fixture
def mock_field_assertion() -> Mock:
    """Create a mock field assertion for testing."""
    assertion = Mock(spec=Assertion)
    assertion.urn = "urn:li:assertion:test-field-assertion"
    assertion.type = AssertionType.FIELD
    assertion.entity = Mock()
    assertion.entity.urn = "urn:li:dataset:test-dataset"
    assertion.connection_urn = "urn:li:connection:test-connection"
    assertion.raw_info_aspect = Mock()

    # Set up the raw_info_aspect to be recognized as a field metric assertion
    return assertion


@pytest.fixture
def mock_non_field_assertion() -> Mock:
    """Create a mock assertion that is not a field metric assertion."""
    assertion = Mock(spec=Assertion)
    assertion.urn = "urn:li:assertion:test-other-assertion"
    assertion.type = AssertionType.VOLUME  # Not FIELD
    assertion.entity = Mock()
    assertion.entity.urn = "urn:li:dataset:test-dataset"
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
def mock_metrics_data() -> List[Metric]:
    """Create mock metrics data for testing."""
    metrics: List[Metric] = []
    for i in range(10):
        metric = Mock(spec=Metric)
        metric.timestamp_ms = i * 3600 * 1000  # Each 1 hour apart
        metric.value = 0.8 + i * 0.01  # Values from 0.8 to 0.89
        metrics.append(cast(Metric, metric))

    return metrics


@pytest.fixture
def mock_anomalies_data() -> List[Anomaly]:
    """Create mock anomalies data for testing."""
    anomalies: List[Anomaly] = []
    for i in range(5):
        anomaly = Mock(spec=Anomaly)
        timestamp_ms = i * 3600 * 1000
        anomaly.timestamp_ms = timestamp_ms
        anomaly.metric = Metric(value=0.8 + i * 0.01, timestamp_ms=timestamp_ms)
        anomalies.append(cast(Anomaly, anomaly))

    return anomalies


@pytest.fixture
def mock_boundary() -> Mock:
    """Create a mock MetricBoundary for testing."""
    lower_bound = Mock(spec=MetricBoundary)
    lower_bound.value = 0.75

    upper_bound = Mock(spec=MetricBoundary)
    upper_bound.value = 0.95

    boundary = Mock(spec=MetricBoundary)
    boundary.lower_bound = lower_bound
    boundary.upper_bound = upper_bound
    boundary.start_time_ms = 1677600000000  # Example timestamp (March 1, 2023)

    return boundary


@pytest.fixture
def mock_boundaries(mock_boundary: Mock) -> List[Mock]:
    """Create a list of mock MetricBoundaries for testing."""
    boundaries = [mock_boundary]

    # Add additional future boundaries
    for i in range(1, 3):
        future_boundary = Mock(spec=MetricBoundary)

        lower_bound = Mock(spec=MetricBoundary)
        lower_bound.value = 0.75 - i * 0.02

        upper_bound = Mock(spec=MetricBoundary)
        upper_bound.value = 0.95 + i * 0.01

        future_boundary.lower_bound = lower_bound
        future_boundary.upper_bound = upper_bound
        future_boundary.start_time_ms = mock_boundary.start_time_ms + i * 3600 * 1000

        boundaries.append(future_boundary)

    return boundaries


@pytest.fixture
def mock_field_assertion_info() -> AssertionInfoClass:
    """Create a mock AssertionInfoClass for a field assertion."""
    field_metric_assertion = FieldMetricAssertionClass(
        field=SchemaFieldSpecClass(path="test", type="STRING", nativeType="varchar"),
        metric=FieldMetricTypeClass.EMPTY_COUNT,
        operator=AssertionStdOperatorClass.BETWEEN,
        parameters=AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(type="NUMBER", value="0.75"),
            maxValue=AssertionStdParameterClass(type="NUMBER", value="0.95"),
        ),
    )

    field_assertion = FieldAssertionInfoClass(
        type=FieldAssertionTypeClass.FIELD_METRIC,
        entity="urn:li:dataset:test-dataset",
        fieldMetricAssertion=field_metric_assertion,
    )

    return AssertionInfoClass(
        type="FIELD",
        fieldAssertion=field_assertion,
    )


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.is_field_metric_assertion"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer.perform_training"
)
def test_train_field_metric_assertion(
    mock_perform_training: MagicMock,
    mock_is_field_metric_assertion: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_monitor: Mock,
    mock_field_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test training a valid field metric assertion."""
    # Arrange
    mock_is_field_metric_assertion.return_value = True

    # Act
    trainer.train(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    mock_is_field_metric_assertion.assert_called_once_with(mock_field_assertion)
    mock_perform_training.assert_called_once_with(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.is_field_metric_assertion"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer.perform_training"
)
def test_train_non_field_metric_assertion(
    mock_perform_training: MagicMock,
    mock_is_field_metric_assertion: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_monitor: Mock,
    mock_non_field_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that training is skipped for non-field-metric assertions."""
    # Arrange
    mock_is_field_metric_assertion.return_value = False

    # Act
    trainer.train(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_non_field_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    mock_is_field_metric_assertion.assert_called_once_with(mock_non_field_assertion)
    mock_perform_training.assert_not_called()


def test_should_perform_inference_no_previous_inference(
    trainer: FieldAssertionTrainer,
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
    trainer: FieldAssertionTrainer,
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
    trainer: FieldAssertionTrainer,
    mock_evaluation_spec: Mock,
) -> None:
    """Test that inference should be performed when previous inference is old."""
    # Arrange
    now_ms = int(time.time() * 1000)
    # Set last inference to be older than the min interval
    mock_evaluation_spec.context.inference_details.generated_at = (
        now_ms - (FIELD_METRIC_MIN_TRAINING_INTERVAL_SECONDS + 10) * 1000
    )

    # Act
    result = trainer.should_perform_inference(
        cast(AssertionEvaluationSpec, mock_evaluation_spec)
    )

    # Assert
    assert result is True


def test_get_min_training_samples(
    trainer: FieldAssertionTrainer,
) -> None:
    """Test that get_min_training_samples returns the correct constant."""
    # Act
    result = trainer.get_min_training_samples()

    # Assert
    assert result == FIELD_METRIC_MIN_TRAINING_SAMPLES


def test_get_min_training_samples_timespan_seconds(
    trainer: FieldAssertionTrainer,
) -> None:
    """Test that get_min_training_samples_timespan_seconds returns the correct constant."""
    # Act
    result = trainer.get_min_training_samples_timespan_seconds()

    # Assert
    assert result == FIELD_METRIC_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS


@patch(
    "datahub_executor.common.monitor.inference.base_assertion_trainer.BaseAssertionTrainer.get_metric_cube_urn"
)
def test_get_metric_data(
    mock_get_metric_cube_urn: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_field_assertion: Mock,
    mock_metrics_data: List[Metric],
) -> None:
    """Test that get_metric_data calls the correct methods with appropriate parameters."""
    # Arrange
    mock_get_metric_cube_urn.return_value = (
        "urn:li:dataHubMetricCube:encoded-monitor-urn"
    )
    mock_dependencies[
        "metrics_client"
    ].fetch_metric_values.return_value = mock_metrics_data

    # Act
    result = trainer.get_metric_data(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        None,
    )

    # Assert
    mock_get_metric_cube_urn.assert_called_once_with(mock_monitor.urn)
    mock_dependencies["metrics_client"].fetch_metric_values.assert_called_once()
    assert result == mock_metrics_data


@patch(
    "datahub_executor.common.monitor.inference.base_assertion_trainer.BaseAssertionTrainer.get_metric_cube_urn"
)
def test_get_metric_data_with_anomalies(
    mock_get_metric_cube_urn: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_field_assertion: Mock,
    mock_metrics_data: List[Metric],
    mock_anomalies_data: List[Anomaly],
) -> None:
    """Test that get_metric_data correctly filters out anomalies"""
    # Arrange
    mock_get_metric_cube_urn.return_value = (
        "urn:li:dataHubMetricCube:encoded-monitor-urn"
    )
    mock_dependencies[
        "metrics_client"
    ].fetch_metric_values.return_value = mock_metrics_data
    mock_dependencies[
        "monitor_client"
    ].fetch_monitor_anomalies.return_value = mock_anomalies_data

    # Act
    result = trainer.get_metric_data(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        None,
    )

    # Assert
    mock_get_metric_cube_urn.assert_called_once_with(mock_monitor.urn)
    mock_dependencies["monitor_client"].fetch_monitor_anomalies.assert_called_once()

    # The first 5 will be filtered as anomalies.
    assert result == mock_metrics_data[5:]


def test_remove_inferred_assertion(
    trainer: FieldAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_field_assertion: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test remove_inferred_assertion updates the context correctly."""
    # Act
    trainer.remove_inferred_assertion(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    mock_dependencies[
        "monitor_client"
    ].patch_field_metric_monitor_evaluation_context.assert_called_once()

    # Verify the context has been reset
    context_arg = mock_dependencies[
        "monitor_client"
    ].patch_field_metric_monitor_evaluation_context.call_args[0][2]
    assert isinstance(context_arg, AssertionEvaluationContextClass)
    assert context_arg.inferenceDetails.generatedAt == 0  # type: ignore


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer._get_sensitivity_level"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer._get_field_assertion_details"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer._update_field_metric_monitor_evaluation_context"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer._rebuild_assertion"
)
def test_train_and_update_assertion(
    mock_rebuild_assertion: MagicMock,
    mock_update_context: MagicMock,
    mock_get_field_assertion_details: MagicMock,
    mock_get_sensitivity_level: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_monitor: Mock,
    mock_field_assertion: Mock,
    mock_metrics_data: List[Metric],
    mock_boundaries: List[Mock],
    mock_field_assertion_info: AssertionInfoClass,
    mock_schema_field: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test train_and_update_assertion with successful prediction."""
    # Arrange
    sensitivity = 2
    mock_get_sensitivity_level.return_value = sensitivity
    mock_dependencies[
        "metrics_predictor"
    ].predict_metric_boundaries.return_value = mock_boundaries

    # Setup field assertion details
    mock_get_field_assertion_details.return_value = (
        mock_field_assertion_info,
        cast(SchemaFieldSpecClass, mock_schema_field),
        FieldMetricTypeClass.EMPTY_COUNT,
    )

    mock_rebuild_assertion.return_value = cast(Assertion, mock_field_assertion)

    # Act
    result = trainer.train_and_update_assertion(
        cast(Monitor, mock_monitor),
        cast(Assertion, mock_field_assertion),
        mock_metrics_data,
        None,
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    # Check sensitivity level was retrieved
    mock_get_sensitivity_level.assert_called_once_with(None)

    # Check boundaries were predicted
    mock_dependencies[
        "metrics_predictor"
    ].predict_metric_boundaries.assert_called_once_with(
        mock_metrics_data, timedelta(hours=1), 24, sensitivity
    )

    # Check field assertion details were retrieved
    mock_get_field_assertion_details.assert_called_once_with(mock_field_assertion)

    # Check field assertion was set with new boundaries
    assert mock_field_assertion_info.fieldAssertion is not None

    # Check embedded assertions were updated
    mock_update_context.assert_called_once_with(
        mock_field_assertion.urn,
        mock_monitor.urn,
        mock_field_assertion.entity.urn,
        cast(SchemaFieldSpecClass, mock_schema_field),
        FieldMetricTypeClass.EMPTY_COUNT,
        mock_boundaries[1:],  # Future boundaries
        mock_evaluation_spec,
    )

    # Check assertion info was updated
    mock_dependencies["monitor_client"].update_assertion_info.assert_called_once_with(
        mock_field_assertion.urn, mock_field_assertion_info
    )

    # Check assertion was rebuilt
    mock_rebuild_assertion.assert_called_once_with(
        mock_field_assertion, mock_field_assertion_info
    )

    # Check result
    assert result == mock_field_assertion


def test_get_sensitivity_level_with_settings(
    trainer: FieldAssertionTrainer,
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
    trainer: FieldAssertionTrainer,
) -> None:
    """Test getting default sensitivity level when no settings provided."""
    # Act
    result = trainer._get_sensitivity_level(None)

    # Assert
    assert result == FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.get_assertion_info"
)
def test_get_field_assertion_details_success(
    mock_get_assertion_info: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_field_assertion: Mock,
    mock_field_assertion_info: AssertionInfoClass,
) -> None:
    """Test successfully getting field assertion details."""
    # Arrange
    mock_get_assertion_info.return_value = mock_field_assertion_info

    # Act
    assertion_info, field, metric = trainer._get_field_assertion_details(
        cast(Assertion, mock_field_assertion)
    )

    # Assert
    mock_get_assertion_info.assert_called_once_with(
        mock_field_assertion.raw_info_aspect
    )
    assert assertion_info == mock_field_assertion_info
    assert field == mock_field_assertion_info.fieldAssertion.fieldMetricAssertion.field  # type: ignore
    assert (
        metric == mock_field_assertion_info.fieldAssertion.fieldMetricAssertion.metric  # type: ignore
    )


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.get_assertion_info"
)
def test_get_field_assertion_details_missing_info(
    mock_get_assertion_info: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_field_assertion: Mock,
) -> None:
    """Test error when field assertion details are missing."""
    # Arrange
    mock_get_assertion_info.return_value = None

    # Act & Assert
    with pytest.raises(
        RuntimeError,
        match=f"Missing raw assertionInfo aspect or field assertion info for assertion {mock_field_assertion.urn}",
    ):
        trainer._get_field_assertion_details(cast(Assertion, mock_field_assertion))


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.get_assertion_info"
)
def test_get_field_assertion_details_missing_field_assertion(
    mock_get_assertion_info: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_field_assertion: Mock,
) -> None:
    """Test error when field assertion is missing."""
    # Arrange
    assertion_info = AssertionInfoClass(type="FIELD")  # Missing fieldAssertion
    mock_get_assertion_info.return_value = assertion_info

    # Act & Assert
    with pytest.raises(
        RuntimeError,
        match=f"Missing raw assertionInfo aspect or field assertion info for assertion {mock_field_assertion.urn}",
    ):
        trainer._get_field_assertion_details(cast(Assertion, mock_field_assertion))


def test_build_field_metric_assertion_info(
    trainer: FieldAssertionTrainer,
    mock_boundary: Mock,
    mock_schema_field: Mock,
) -> None:
    """Test building field metric assertion info with boundaries."""
    # Arrange
    entity_urn = "urn:li:dataset:test-dataset"
    metric = FieldMetricTypeClass.EMPTY_COUNT

    # Act
    result = trainer._build_field_metric_assertion_info(
        entity_urn,
        cast(SchemaFieldSpecClass, mock_schema_field),
        metric,
        cast(MetricBoundary, mock_boundary),
    )

    # Assert
    assert isinstance(result, FieldAssertionInfoClass)
    assert result.type == FieldAssertionTypeClass.FIELD_METRIC
    assert result.entity == entity_urn

    assert result.fieldMetricAssertion is not None
    assert result.fieldMetricAssertion.field == mock_schema_field
    assert result.fieldMetricAssertion.metric == metric
    assert result.fieldMetricAssertion.operator == AssertionStdOperatorClass.BETWEEN

    assert result.fieldMetricAssertion.parameters is not None
    assert result.fieldMetricAssertion.parameters.minValue is not None
    assert result.fieldMetricAssertion.parameters.minValue.value == str(
        mock_boundary.lower_bound.value
    )

    assert result.fieldMetricAssertion.parameters.maxValue is not None
    assert result.fieldMetricAssertion.parameters.maxValue.value == str(
        mock_boundary.upper_bound.value
    )


@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer._build_field_metric_assertion_info"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.FieldAssertionTrainer.create_inference_details"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.create_inference_source"
)
@patch(
    "datahub_executor.common.monitor.inference.field_assertion_trainer.create_embedded_assertion"
)
def test_update_field_metric_monitor_evaluation_context(
    mock_create_embedded_assertion: MagicMock,
    mock_create_inference_source: MagicMock,
    mock_create_inference_details: MagicMock,
    mock_build_field_metric_assertion: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_dependencies: Dict[str, Union[MagicMock, Mock]],
    mock_boundaries: List[Mock],
    mock_schema_field: Mock,
    mock_evaluation_spec: Mock,
) -> None:
    """Test updating field metric monitor evaluation context with embedded assertions."""
    # Arrange
    assertion_urn = "urn:li:assertion:test-assertion"
    monitor_urn = "urn:li:dataHubMonitor:test-monitor"
    entity_urn = "urn:li:dataset:test-dataset"
    metric = FieldMetricTypeClass.EMPTY_COUNT

    # Configure mocks
    mock_build_field_metric_assertion.return_value = Mock(spec=FieldAssertionInfoClass)
    mock_create_inference_source.return_value = Mock()
    mock_create_embedded_assertion.return_value = Mock(spec=EmbeddedAssertionClass)

    inference_details = Mock()
    mock_create_inference_details.return_value = inference_details

    # Act
    trainer._update_field_metric_monitor_evaluation_context(
        assertion_urn,
        monitor_urn,
        entity_urn,
        cast(SchemaFieldSpecClass, mock_schema_field),
        metric,
        cast(List[MetricBoundary], mock_boundaries),
        cast(AssertionEvaluationSpec, mock_evaluation_spec),
    )

    # Assert
    # Check that field metric assertion info was built for each boundary
    assert mock_build_field_metric_assertion.call_count == len(mock_boundaries)

    # Check that embedded assertions were created
    assert mock_create_embedded_assertion.call_count == len(mock_boundaries)

    # Check that inference details were created
    mock_create_inference_details.assert_called_once()

    # Check that context was updated
    mock_dependencies[
        "monitor_client"
    ].patch_field_metric_monitor_evaluation_context.assert_called_once()

    # Check arguments to patch_field_metric_monitor_evaluation_context
    call_args = mock_dependencies[
        "monitor_client"
    ].patch_field_metric_monitor_evaluation_context.call_args[0]
    assert call_args[0] == monitor_urn
    assert call_args[1] == assertion_urn
    assert isinstance(call_args[2], AssertionEvaluationContextClass)
    assert call_args[2].inferenceDetails == inference_details
    assert call_args[3] == mock_evaluation_spec


@patch("datahub_executor.common.monitor.inference.field_assertion_trainer.Assertion")
def test_rebuild_assertion(
    mock_assertion_class: MagicMock,
    trainer: FieldAssertionTrainer,
    mock_field_assertion: Mock,
    mock_field_assertion_info: AssertionInfoClass,
) -> None:
    """Test rebuilding an assertion with updated info."""
    # Arrange
    mock_assertion_class.parse_obj.return_value = cast(Assertion, mock_field_assertion)

    # Act
    result = trainer._rebuild_assertion(
        cast(Assertion, mock_field_assertion),
        mock_field_assertion_info,
    )

    # Assert
    mock_assertion_class.parse_obj.assert_called_once()

    # Check that the correct parameters were passed to parse_obj
    parsed_obj = mock_assertion_class.parse_obj.call_args[0][0]
    assert parsed_obj["urn"] == mock_field_assertion.urn
    assert parsed_obj["entity"] == mock_field_assertion.entity
    assert parsed_obj["connectionUrn"] == mock_field_assertion.connection_urn
    assert parsed_obj["raw_info_aspect"] is None

    # Check result
    assert result == mock_field_assertion
