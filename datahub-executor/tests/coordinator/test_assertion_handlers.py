from unittest.mock import MagicMock, patch

import fastapi
import pytest

from datahub_executor.common.types import Monitor
from datahub_executor.coordinator.assertion_handlers import (
    handle_train_assertion_monitor,
)
from datahub_executor.coordinator.types import TrainAssertionMonitorInputSchema


@pytest.fixture
def mock_monitor_data() -> dict:
    """Mock GraphQL response data for a monitor"""
    return {
        "entity": {
            "urn": "urn:li:assertionMonitor:12345",
            "type": "ASSERTION",  # Match the enum value
            "mode": "ACTIVE",  # Add required field
            "properties": {"name": "Test Monitor", "description": "A test monitor"},
            "assertion": {"urn": "urn:li:assertion:67890"},
        }
    }


@pytest.fixture
def mock_graph() -> MagicMock:
    """Mock the DataHub graph client"""
    mock = MagicMock()
    return mock


@pytest.fixture
def mock_training_engine() -> MagicMock:
    """Mock the monitor training engine"""
    mock = MagicMock()
    return mock


@pytest.fixture
def mock_monitor() -> Monitor:
    """Create a valid Monitor instance that passes validation"""
    from enum import Enum

    # Mock the enum if needed
    class MockMonitorType(str, Enum):
        ASSERTION = "ASSERTION"

    # Update this with patch to use actual enum from your codebase if needed
    with patch("datahub_executor.common.types.MonitorType", MockMonitorType):
        # Create a valid monitor instance
        monitor = Monitor(
            urn="urn:li:assertionMonitor:12345",
            type=MockMonitorType.ASSERTION,  # Use enum value
            mode="ACTIVE",  # Add required field
            properties={"name": "Test Monitor", "description": "A test monitor"},
            assertion={"urn": "urn:li:assertion:67890"},
        )
        return monitor


def test_handle_train_assertion_monitor_success(
    mock_graph: MagicMock,
    mock_training_engine: MagicMock,
    mock_monitor_data: MagicMock,
    mock_monitor: MagicMock,
) -> None:
    """Test successful training of an assertion monitor"""

    # Setup input
    train_input = TrainAssertionMonitorInputSchema(
        monitorUrn="urn:li:assertionMonitor:12345"
    )

    # Setup mocks
    mock_graph.execute_graphql.return_value = mock_monitor_data

    # Patch globals and Monitor.parse_obj to return our prepared instance
    with (
        patch("datahub_executor.coordinator.assertion_handlers.graph", mock_graph),
        patch(
            "datahub_executor.coordinator.assertion_handlers.training_engine",
            mock_training_engine,
        ),
        patch(
            "datahub_executor.common.types.Monitor.parse_obj", return_value=mock_monitor
        ),
    ):
        # Call the function
        result = handle_train_assertion_monitor(train_input)

        # Verify the result
        assert result.success is True

        # Verify the mocks were called correctly
        mock_graph.execute_graphql.assert_called_once()
        mock_training_engine.train.assert_called_once()

        # Verify monitor was the one we created
        monitor_arg = mock_training_engine.train.call_args[0][0]
        assert monitor_arg is mock_monitor
        assert monitor_arg.urn == "urn:li:assertionMonitor:12345"


def test_handle_train_assertion_monitor_not_found(
    mock_graph: MagicMock, mock_training_engine: MagicMock
) -> None:
    """Test when a monitor isn't found"""

    # Setup input
    train_input = TrainAssertionMonitorInputSchema(
        monitorUrn="urn:li:assertionMonitor:nonexistent"
    )

    # Setup mocks - return empty/None result to simulate not found
    mock_graph.execute_graphql.return_value = {"entity": None}

    # Patch globals
    with (
        patch("datahub_executor.coordinator.assertion_handlers.graph", mock_graph),
        patch(
            "datahub_executor.coordinator.assertion_handlers.training_engine",
            mock_training_engine,
        ),
    ):
        # Call the function and expect an exception
        with pytest.raises(fastapi.HTTPException) as excinfo:
            handle_train_assertion_monitor(train_input)

        # Verify the error code is 404
        assert excinfo.value.status_code == 404

        # Verify that training engine was not called
        mock_training_engine.train.assert_not_called()


def test_handle_train_assertion_monitor_parse_error(
    mock_graph: MagicMock, mock_training_engine: MagicMock
) -> None:
    """Test when parsing the monitor fails"""

    # Setup input
    train_input = TrainAssertionMonitorInputSchema(
        monitorUrn="urn:li:assertionMonitor:12345"
    )

    # Setup a malformed monitor response
    malformed_data = {
        "entity": {"urn": "urn:li:assertionMonitor:12345", "invalid": "data"}
    }
    mock_graph.execute_graphql.return_value = malformed_data

    # Patch globals and Monitor.parse_obj to raise an exception
    with (
        patch("datahub_executor.coordinator.assertion_handlers.graph", mock_graph),
        patch(
            "datahub_executor.coordinator.assertion_handlers.training_engine",
            mock_training_engine,
        ),
        patch(
            "datahub_executor.common.types.Monitor.parse_obj",
            side_effect=ValueError("Parse error"),
        ),
    ):
        # Call the function and expect an exception
        with pytest.raises(fastapi.HTTPException) as excinfo:
            handle_train_assertion_monitor(train_input)

        # Verify the error code is 500
        assert excinfo.value.status_code == 500

        # Verify that training engine was not called
        mock_training_engine.train.assert_not_called()


def test_handle_train_assertion_monitor_training_error(
    mock_graph: MagicMock,
    mock_training_engine: MagicMock,
    mock_monitor_data: MagicMock,
    mock_monitor: MagicMock,
) -> None:
    """Test when training the monitor fails"""

    # Setup input
    train_input = TrainAssertionMonitorInputSchema(
        monitorUrn="urn:li:assertionMonitor:12345"
    )

    # Setup mocks
    mock_graph.execute_graphql.return_value = mock_monitor_data
    mock_training_engine.train.side_effect = Exception("Training error")

    # Patch globals and Monitor.parse_obj
    with (
        patch("datahub_executor.coordinator.assertion_handlers.graph", mock_graph),
        patch(
            "datahub_executor.coordinator.assertion_handlers.training_engine",
            mock_training_engine,
        ),
        patch(
            "datahub_executor.common.types.Monitor.parse_obj", return_value=mock_monitor
        ),
    ):
        # Call the function and expect an exception
        with pytest.raises(fastapi.HTTPException) as excinfo:
            handle_train_assertion_monitor(train_input)

        # Verify the error code is 500
        assert excinfo.value.status_code == 500

        # Verify the training engine was called
        mock_training_engine.train.assert_called_once()


def test_handle_train_assertion_monitor_engine_not_initialized(
    mock_graph: MagicMock,
) -> None:
    """Test when the training engine is not initialized"""

    # Setup input
    train_input = TrainAssertionMonitorInputSchema(
        monitorUrn="urn:li:assertionMonitor:12345"
    )

    # Patch globals with training_engine as None
    with (
        patch("datahub_executor.coordinator.assertion_handlers.graph", mock_graph),
        patch("datahub_executor.coordinator.assertion_handlers.training_engine", None),
    ):
        # Call the function and expect an exception
        with pytest.raises(fastapi.HTTPException) as excinfo:
            handle_train_assertion_monitor(train_input)

        # Verify the error code is 500
        assert excinfo.value.status_code == 500
        assert "Monitor training engine is not initialized" in str(excinfo.value.detail)
