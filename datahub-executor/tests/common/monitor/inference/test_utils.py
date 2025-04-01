from typing import cast
from unittest.mock import Mock

import pytest
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdParametersClass,
    EmbeddedAssertionClass,
)

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.monitor.inference.utils import (
    annotate_operations_with_anomalies,
    build_std_parameters,
    create_embedded_assertion,
    create_inference_source,
    is_metric_anomaly,
)
from datahub_executor.common.types import Anomaly


@pytest.fixture
def mock_boundary() -> MetricBoundary:
    """Create a mock MetricBoundary for testing."""
    lower_bound = Mock(spec=MetricBoundary)
    lower_bound.value = 100.0

    upper_bound = Mock(spec=MetricBoundary)
    upper_bound.value = 200.0

    boundary = Mock(spec=MetricBoundary)
    boundary.lower_bound = lower_bound
    boundary.upper_bound = upper_bound
    boundary.start_time_ms = 1677600000000  # Example timestamp (March 1, 2023)

    return cast(MetricBoundary, boundary)


@pytest.fixture
def mock_assertion_info() -> AssertionInfoClass:
    """Create a mock AssertionInfoClass for testing."""
    return AssertionInfoClass(
        type="VOLUME",
        source=AssertionSourceClass(type=AssertionSourceTypeClass.INFERRED),
    )


def test_build_std_parameters(mock_boundary: MetricBoundary) -> None:
    """Test building standard parameters from a boundary."""
    # Act
    result = build_std_parameters(mock_boundary)

    # Assert
    assert isinstance(result, AssertionStdParametersClass)

    assert result.minValue is not None
    assert result.minValue.type == "NUMBER"
    assert result.minValue.value == "100.0"

    assert result.maxValue is not None
    assert result.maxValue.type == "NUMBER"
    assert result.maxValue.value == "200.0"


def test_create_embedded_assertion(
    mock_assertion_info: AssertionInfoClass,
    mock_boundary: MetricBoundary,
) -> None:
    """Test creating an embedded assertion with time window."""
    # Arrange
    window_size_seconds = 3600  # 1 hour

    # Act
    result = create_embedded_assertion(
        mock_assertion_info, mock_boundary, window_size_seconds
    )

    # Assert
    assert isinstance(result, EmbeddedAssertionClass)
    assert result.assertion == mock_assertion_info

    assert result.evaluationTimeWindow is not None
    assert result.evaluationTimeWindow.startTimeMillis == mock_boundary.start_time_ms

    assert result.evaluationTimeWindow.length is not None
    assert result.evaluationTimeWindow.length.unit == "SECOND"
    assert result.evaluationTimeWindow.length.multiple == window_size_seconds


def test_create_inference_source() -> None:
    """Test creating an inference source."""
    # Act
    result = create_inference_source()

    # Assert
    assert isinstance(result, AssertionSourceClass)
    assert result.type == AssertionSourceTypeClass.INFERRED


def test_is_metric_anomaly() -> None:
    """Test detecting whether a metric is an anomaly"""
    # Case 1: Metric is an anomaly.
    anomalies = [
        Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1)),
        Anomaly(timestamp_ms=1, metric=Metric(value=124, timestamp_ms=2)),
    ]

    metric = Metric(value=123, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is True

    # Case 2: Metric is not an anomaly, does not match value.
    anomalies = [Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1))]

    metric = Metric(value=124, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is False

    # Case 2: Metric is not an anomaly, does not match timestamp.
    anomalies = [Anomaly(timestamp_ms=0, metric=Metric(value=123, timestamp_ms=1))]

    metric = Metric(value=123, timestamp_ms=2)

    assert is_metric_anomaly(metric, anomalies) is False

    # Case 3: Empty anomalies.
    anomalies = []
    metric = Metric(value=123, timestamp_ms=1)

    assert is_metric_anomaly(metric, anomalies) is False


def test_annotate_operations_with_anomalies() -> None:
    # Case 1: There are no anomalies.
    # Operations should remain unchanged.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies: list[Anomaly] = []

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    assert operations == annotated_operations

    # Case 2: There are anomalies at the beginning
    # First operation should be marked as an anomaly.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=-1, metric=None),
        Anomaly(timestamp_ms=-2, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    # Only the first should be marked an anomaly
    assert annotated_operations[0].is_anomaly is True
    for operation in annotated_operations[1:]:
        assert operation.is_anomaly is False

    # Case 3: There are anomalies at the end
    # Operations should remain unchanged.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=1, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=2, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=4, metric=None),
        Anomaly(timestamp_ms=5, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)

    assert operations == annotated_operations

    # Case 4: There is single anomalies in the middle - none equal to the exact times.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=1, metric=None),
        Anomaly(timestamp_ms=3, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is False
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is False

    # Case 5: There are multiple anomalies in the middle.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=1, metric=None),
        Anomaly(timestamp_ms=3, metric=None),
        Anomaly(timestamp_ms=5, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is False
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is True

    # Case 6: If the anomaly happened at the exact same time as the operation,
    # it was probably a timing fluke. Either way to be safe, we do mark as an anomaly.
    operations = [
        Operation(type="INSERT", timestamp_ms=0, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=4, is_anomaly=False),
        Operation(type="INSERT", timestamp_ms=6, is_anomaly=False),
    ]

    anomalies = [
        Anomaly(timestamp_ms=0, metric=None),
        Anomaly(timestamp_ms=4, metric=None),
        Anomaly(timestamp_ms=6, metric=None),
    ]

    annotated_operations = annotate_operations_with_anomalies(operations, anomalies)
    assert annotated_operations[0].is_anomaly is True
    assert annotated_operations[1].is_anomaly is True
    assert annotated_operations[2].is_anomaly is True
