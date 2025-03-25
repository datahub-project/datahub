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

from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.monitor.inference.utils import (
    build_std_parameters,
    create_embedded_assertion,
    create_inference_source,
)


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
