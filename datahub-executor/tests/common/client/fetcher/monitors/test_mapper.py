from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.client.fetcher.monitors.mapper import (
    SkippableMonitorMappingError,
    generate_assertion_tasks,
    generate_training_tasks,
    graphql_to_monitor,
    graphql_to_monitors,
)
from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    RUN_ASSERTION_TASK_NAME,
    RUN_MONITOR_TRAINING_TASK_NAME,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationSpec,
    AssertionMonitor,
    AssertionSourceType,
    AssertionType,
    CronSchedule,
    ExecutionRequestSchedule,
    Monitor,
    MonitorMode,
    MonitorType,
)


@pytest.fixture
def valid_monitor_dict() -> Dict[str, Any]:
    """Fixture for a valid monitor dictionary representation."""
    return {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion",
                            "type": "VOLUME",
                            "info": {
                                "type": "VOLUME",
                                "source": {"type": "NATIVE"},
                                "volumeAssertion": {
                                    "type": "ROW_COUNT_TOTAL",
                                    "rowCountTotal": {
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {"value": "100", "type": "NUMBER"}
                                        },
                                    },
                                },
                            },
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_VOLUME",
                            "datasetVolumeParameters": {"sourceType": "QUERY"},
                        },
                    }
                ]
            },
        },
        "entity": {
            "urn": "urn:li:dataset:test-dataset",
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "properties": {"displayName": "Snowflake"},
            },
            "properties": {
                "name": "test_table",
                "qualifiedName": "test_db.test_schema.test_table",
            },
            "subTypes": {"typeNames": ["TABLE"]},
            "exists": True,
        },
    }


@pytest.fixture
def monitor_without_assertion_monitor() -> Dict[str, Any]:
    """Fixture for a monitor without an assertion monitor."""
    return {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            # Missing assertionMonitor
        },
        "entity": {
            "urn": "urn:li:dataset:test-dataset",
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "properties": {"displayName": "Snowflake"},
            },
            "properties": {
                "name": "test_table",
                "qualifiedName": "test_db.test_schema.test_table",
            },
            "subTypes": {"typeNames": ["TABLE"]},
            "exists": True,
        },
    }


@pytest.fixture
def monitor_with_multiple_assertions() -> Dict[str, Any]:
    """Fixture for a monitor with multiple assertions."""
    return {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion-1",
                            "type": "VOLUME",
                            "info": {
                                "type": "VOLUME",
                                "source": {"type": "NATIVE"},
                                "volumeAssertion": {
                                    "type": "ROW_COUNT_TOTAL",
                                    "rowCountTotal": {
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {"value": "100", "type": "NUMBER"}
                                        },
                                    },
                                },
                            },
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_VOLUME",
                            "datasetVolumeParameters": {"sourceType": "QUERY"},
                        },
                    },
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion-2",
                            "type": "FRESHNESS",
                            "info": {
                                "type": "FRESHNESS",
                                "source": {"type": "NATIVE"},
                                "freshnessAssertion": {"type": "DATASET_CHANGE"},
                            },
                        },
                        "schedule": {"cron": "*/30 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_FRESHNESS",
                            "datasetFreshnessParameters": {
                                "sourceType": "INFORMATION_SCHEMA"
                            },
                        },
                    },
                ]
            },
        },
        "entity": {
            "urn": "urn:li:dataset:test-dataset",
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "properties": {"displayName": "Snowflake"},
            },
            "properties": {
                "name": "test_table",
                "qualifiedName": "test_db.test_schema.test_table",
            },
            "subTypes": {"typeNames": ["TABLE"]},
            "exists": True,
        },
    }


@pytest.fixture
def monitor_with_soft_deleted_entity() -> Dict[str, Any]:
    """Fixture for a monitor with a soft deleted entity."""
    return {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion",
                            "type": "VOLUME",
                            "info": {
                                "type": "VOLUME",
                                "source": {"type": "NATIVE"},
                                "volumeAssertion": {
                                    "type": "ROW_COUNT_TOTAL",
                                    "rowCountTotal": {
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {"value": "100", "type": "NUMBER"}
                                        },
                                    },
                                },
                            },
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_VOLUME",
                            "datasetVolumeParameters": {"sourceType": "QUERY"},
                        },
                    }
                ]
            },
        },
        "entity": {
            "urn": "urn:li:dataset:test-dataset",
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "properties": {"displayName": "Snowflake"},
            },
            "properties": {
                "name": "test_table",
                "qualifiedName": "test_db.test_schema.test_table",
            },
            "subTypes": {"typeNames": ["TABLE"]},
            "exists": False,  # Entity is soft deleted
        },
    }


@pytest.fixture
def invalid_monitor_missing_entity() -> Dict[str, Any]:
    """Fixture for a monitor with missing entity information."""
    return {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion",
                            "type": "VOLUME",
                            "info": {
                                "type": "VOLUME",
                                "source": {"type": "NATIVE"},
                                "volumeAssertion": {
                                    "type": "ROW_COUNT_TOTAL",
                                    "rowCountTotal": {
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {"value": "100", "type": "NUMBER"}
                                        },
                                    },
                                },
                            },
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_VOLUME",
                            "datasetVolumeParameters": {"sourceType": "QUERY"},
                        },
                    }
                ]
            },
        },
        # Missing entity field
    }


def test_graphql_to_monitor_valid(valid_monitor_dict: Dict[str, Any]) -> None:
    """Test successful conversion of a valid GraphQL monitor to a Monitor object."""
    # When
    monitor = graphql_to_monitor(valid_monitor_dict)

    # Then
    assert isinstance(monitor, Monitor)
    assert monitor.urn == "urn:li:monitor:test-monitor"
    assert monitor.type == MonitorType.ASSERTION
    assert monitor.mode == MonitorMode.ACTIVE
    assert monitor.executor_id == "test-executor"

    # Verify entity information
    assert monitor.assertion_monitor is not None
    assert len(monitor.assertion_monitor.assertions) == 1
    assert (
        monitor.assertion_monitor.assertions[0].assertion.entity.urn
        == "urn:li:dataset:test-dataset"
    )
    assert (
        monitor.assertion_monitor.assertions[0].assertion.entity.platform_urn
        == "urn:li:dataPlatform:snowflake"
    )
    assert (
        monitor.assertion_monitor.assertions[0].assertion.entity.table_name
        == "test_table"
    )
    assert (
        monitor.assertion_monitor.assertions[0].assertion.entity.qualified_name
        == "test_db.test_schema.test_table"
    )
    assert monitor.assertion_monitor.assertions[0].assertion.entity.exists is True

    # Verify assertion details
    assertion_spec = monitor.assertion_monitor.assertions[0]
    assert assertion_spec.assertion.urn == "urn:li:assertion:test-assertion"
    assert assertion_spec.assertion.type == AssertionType.VOLUME
    assert assertion_spec.schedule.cron == "*/15 * * * *"
    assert (
        assertion_spec.parameters.type
        == AssertionEvaluationParametersType.DATASET_VOLUME
    )


def test_graphql_to_monitor_no_assertion_monitor(
    monitor_without_assertion_monitor: Dict[str, Any],
) -> None:
    """Test handling of a monitor without an assertion monitor."""
    # When/Then
    with pytest.raises(SkippableMonitorMappingError) as excinfo:
        graphql_to_monitor(monitor_without_assertion_monitor)

    # Verify error message
    assert "does not have an assertion monitor" in str(excinfo.value)


def test_graphql_to_monitor_multiple_assertions(
    monitor_with_multiple_assertions: Dict[str, Any],
) -> None:
    """Test handling of a monitor with multiple assertions."""
    # When/Then
    with pytest.raises(SkippableMonitorMappingError) as excinfo:
        graphql_to_monitor(monitor_with_multiple_assertions)

    # Verify error message
    assert "does not have exactly 1 assertion" in str(excinfo.value)


def test_graphql_to_monitor_soft_deleted_entity(
    monitor_with_soft_deleted_entity: Dict[str, Any],
) -> None:
    """Test handling of a monitor with a soft deleted entity."""
    # When/Then
    with pytest.raises(SkippableMonitorMappingError) as excinfo:
        graphql_to_monitor(monitor_with_soft_deleted_entity)

    # Verify error message
    assert "associated entity is soft deleted" in str(excinfo.value)


def test_graphql_to_monitor_missing_entity(
    invalid_monitor_missing_entity: Dict[str, Any],
) -> None:
    """Test handling of a monitor with missing entity information."""
    # When/Then
    with pytest.raises(Exception) as excinfo:
        graphql_to_monitor(invalid_monitor_missing_entity)

    # The error should not be a SkippableMonitorMappingError but a validation error
    assert not isinstance(excinfo.value, SkippableMonitorMappingError)


def test_graphql_to_monitors_all_valid(valid_monitor_dict: Dict[str, Any]) -> None:
    """Test successful conversion of a list of valid GraphQL monitors."""
    # Given
    graphql_monitors: List[Dict[str, Any]] = [valid_monitor_dict, valid_monitor_dict]

    # When
    monitors = graphql_to_monitors(graphql_monitors)

    # Then
    assert len(monitors) == 2
    for monitor in monitors:
        assert isinstance(monitor, Monitor)
        assert monitor.urn == "urn:li:monitor:test-monitor"


def test_graphql_to_monitors_some_skippable(
    valid_monitor_dict: Dict[str, Any],
    monitor_without_assertion_monitor: Dict[str, Any],
) -> None:
    """Test handling of a mix of valid and skippable monitors."""
    # Given
    graphql_monitors: List[Dict[str, Any]] = [
        valid_monitor_dict,
        monitor_without_assertion_monitor,
    ]

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.logger"
    ) as mock_logger:
        monitors = graphql_to_monitors(graphql_monitors)

    # Then
    assert len(monitors) == 1
    assert isinstance(monitors[0], Monitor)
    assert monitors[0].urn == "urn:li:monitor:test-monitor"

    # Verify warning was logged for skippable monitor
    mock_logger.warning.assert_called_once()


def test_graphql_to_monitors_with_validation_error(
    valid_monitor_dict: Dict[str, Any],
    invalid_monitor_missing_entity: Dict[str, Any],
) -> None:
    """Test handling of monitors with validation errors."""
    # Given
    graphql_monitors: List[Dict[str, Any]] = [
        valid_monitor_dict,
        invalid_monitor_missing_entity,
    ]

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.logger"
    ) as mock_logger:
        with patch(
            "datahub_executor.common.client.fetcher.monitors.mapper.METRIC"
        ) as mock_metric:
            # Setup the mock metric to return another mock for method chaining
            metric_mock = MagicMock()
            mock_metric.return_value = metric_mock

            monitors = graphql_to_monitors(graphql_monitors)

    # Then
    assert len(monitors) == 1
    assert isinstance(monitors[0], Monitor)

    # Verify error was logged and metric was incremented
    mock_logger.exception.assert_called_once()
    mock_metric.assert_any_call(
        "ASSERTION_FETCHER_ITEMS_ERRORED", exception="exception"
    )
    metric_mock.inc.assert_called()


def test_graphql_to_monitors_with_entity_validation_error() -> None:
    """Test handling of monitors with specific entity validation errors."""
    # Given
    # Creating a monitor that will trigger the specific error message check
    monitor_with_entity_error: Dict[str, Any] = {
        "urn": "urn:li:monitor:test-monitor",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": "urn:li:assertion:test-assertion",
                            "type": "VOLUME",
                            "info": {"type": "VOLUME"},
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {"type": "DATASET_VOLUME"},
                    }
                ]
            },
        },
        # Entity field is missing at monitor level
    }

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.logger"
    ) as mock_logger:
        monitors = graphql_to_monitors([monitor_with_entity_error])

    # Then
    assert len(monitors) == 0

    # Verify error was logged but with error message instead of exception
    mock_logger.exception.assert_called_once()


def test_graphql_to_monitors_all_invalid() -> None:
    """Test handling when all monitors are invalid."""
    # Given
    invalid_monitors: List[Dict[str, Any]] = [
        {"urn": "urn:li:monitor:invalid-1", "info": {}},  # Missing required fields
        {"urn": "urn:li:monitor:invalid-2"},  # Even more minimal
    ]

    # When
    with patch("datahub_executor.common.client.fetcher.monitors.mapper.logger"):
        monitors = graphql_to_monitors(invalid_monitors)

    # Then
    assert len(monitors) == 0


def test_graphql_to_monitors_empty_list() -> None:
    """Test handling of an empty list of monitors."""
    # Given
    graphql_monitors: List[Dict[str, Any]] = []

    # When
    monitors = graphql_to_monitors(graphql_monitors)

    # Then
    assert len(monitors) == 0


def test_graphql_to_monitors_metrics_tracking(
    valid_monitor_dict: Dict[str, Any],
) -> None:
    """Test that metrics are properly tracked during conversion."""
    # Given
    graphql_monitors: List[Dict[str, Any]] = [valid_monitor_dict]

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.METRIC"
    ) as mock_metric:
        # Setup the mock metric to return another mock for method chaining
        metric_mock = MagicMock()
        mock_metric.return_value = metric_mock

        monitors = graphql_to_monitors(graphql_monitors)

    # Then
    assert len(monitors) == 1
    # Verify metrics were incremented
    mock_metric.assert_any_call("ASSERTION_FETCHER_ITEMS_MAPPED")
    metric_mock.inc.assert_called()


@pytest.fixture
def mock_monitor() -> Monitor:
    """Create a mock monitor with one assertion."""
    entity = AssertionEntity(
        urn="urn:li:dataset:test-dataset",
        platform_urn="urn:li:dataPlatform:snowflake",
        exists=True,
    )

    assertion = Assertion(
        urn="urn:li:assertion:test-assertion",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.NATIVE,
        connectionUrn="urn:li:dataPlatform:snowflake",
    )

    assertion_spec = AssertionEvaluationSpec(
        assertion=assertion,
        schedule=CronSchedule(cron="*/15 * * * *", timezone="UTC"),
        parameters=AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME
        ),
        context=None,
        rawParameters=None,
    )

    assertion_monitor = AssertionMonitor(assertions=[assertion_spec])

    monitor = Monitor(
        urn="urn:li:monitor:test-monitor",
        type=MonitorType.ASSERTION,
        assertion_monitor=assertion_monitor,
        mode=MonitorMode.ACTIVE,
        executorId="test-executor",
    )

    return monitor


@pytest.fixture
def mock_inferred_monitor() -> Monitor:
    """Create a mock monitor with one inferred assertion."""
    entity = AssertionEntity(
        urn="urn:li:dataset:test-dataset",
        platform_urn="urn:li:dataPlatform:snowflake",
        exists=True,
    )

    assertion = Assertion(
        urn="urn:li:assertion:test-assertion",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.INFERRED,
        connectionUrn="urn:li:dataPlatform:snowflake",
    )

    assertion_spec = AssertionEvaluationSpec(
        assertion=assertion,
        schedule=CronSchedule(cron="*/15 * * * *", timezone="UTC"),
        parameters=AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME
        ),
        context=None,
        raw_parameters=None,
    )

    assertion_monitor = AssertionMonitor(assertions=[assertion_spec])

    monitor = Monitor(
        urn="urn:li:monitor:test-monitor",
        type=MonitorType.ASSERTION,
        assertion_monitor=assertion_monitor,
        mode=MonitorMode.ACTIVE,
        executorId="test-executor",
    )

    return monitor


def test_generate_assertion_tasks(mock_monitor: Monitor) -> None:
    """Test generation of assertion tasks."""
    # Given a monitor with one assertion

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.is_dry_run_mode",
        return_value=False,
    ):
        execution_requests = generate_assertion_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 1

    request_schedule = execution_requests[0]
    assert isinstance(request_schedule, ExecutionRequestSchedule)

    # Check schedule
    assert request_schedule.schedule.cron == "*/15 * * * *"
    assert request_schedule.schedule.timezone == "UTC"

    # Check execution request
    exec_request = request_schedule.execution_request
    assert isinstance(exec_request, ExecutionRequest)
    assert exec_request.executor_id == "test-executor"
    assert exec_request.name == RUN_ASSERTION_TASK_NAME
    assert exec_request.exec_id == f"{mock_monitor.urn}_scheduled_training"

    # Check args
    assert exec_request.args["urn"] == mock_monitor.urn
    assert "assertion_spec" in exec_request.args
    assert "context" in exec_request.args
    assert exec_request.args["context"]["dry_run"] is False
    assert exec_request.args["context"]["monitor_urn"] == mock_monitor.urn


def test_generate_assertion_tasks_dry_run(mock_monitor: Monitor) -> None:
    """Test generation of assertion tasks in dry run mode."""

    # Make sure we're patching where the function is imported, not where it's defined
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.is_dry_run_mode",
        return_value=True,
    ):
        execution_requests = generate_assertion_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 1

    request_schedule = execution_requests[0]
    exec_request = request_schedule.execution_request

    # Check that dry_run is True in context
    assert exec_request.args["context"]["dry_run"] is True


def test_generate_assertion_tasks_no_executor_id(mock_monitor: Monitor) -> None:
    """Test generation of assertion tasks when monitor has no executor_id."""
    # Given a monitor with no executor_id
    mock_monitor.executor_id = None

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.is_dry_run_mode",
        return_value=False,
    ):
        execution_requests = generate_assertion_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 1

    request_schedule = execution_requests[0]
    exec_request = request_schedule.execution_request

    # Check that default executor ID is used
    assert exec_request.executor_id == DATAHUB_EXECUTOR_EMBEDDED_POOL_ID


def test_generate_assertion_tasks_no_assertions(mock_monitor: Monitor) -> None:
    """Test generation of assertion tasks when monitor has no assertions."""
    # Given a monitor with no assertions
    if mock_monitor.assertion_monitor:
        mock_monitor.assertion_monitor.assertions = []

    # When
    execution_requests = generate_assertion_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 0


def test_generate_assertion_tasks_no_assertion_monitor(mock_monitor: Monitor) -> None:
    """Test generation of assertion tasks when monitor has no assertion_monitor."""
    # Given a monitor with no assertion_monitor
    mock_monitor.assertion_monitor = None

    # When
    execution_requests = generate_assertion_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 0


@patch(
    "datahub_executor.common.client.fetcher.monitors.mapper.ONLINE_SMART_ASSERTIONS_ENABLED",
    False,
)
def test_generate_training_tasks_not_inferred(mock_monitor: Monitor) -> None:
    """Test that training tasks are not generated for non-inferred assertions."""
    # Given a monitor with a NATIVE assertion

    # When
    execution_requests = generate_training_tasks(mock_monitor)

    # Then
    assert len(execution_requests) == 0


@patch(
    "datahub_executor.common.client.fetcher.monitors.mapper.ONLINE_SMART_ASSERTIONS_ENABLED",
    False,
)
@patch("datahub_executor.common.client.fetcher.monitors.mapper.IS_LOCAL_DEV", False)
def test_generate_training_tasks_inferred(mock_inferred_monitor: Monitor) -> None:
    """Test generation of training tasks for inferred assertions."""
    # Given a monitor with an INFERRED assertion

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.is_dry_run_mode",
        return_value=False,
    ):
        with patch(
            "datahub_executor.common.client.fetcher.monitors.mapper.get_monitor_training_schedule_with_jitter"
        ) as mock_jitter:
            mock_jitter.return_value = "30 * * * *"
            with patch(
                "datahub_executor.common.client.fetcher.monitors.mapper.MONITOR_TRAINING_EXECUTOR_ID",
                "default",
            ):
                execution_requests = generate_training_tasks(mock_inferred_monitor)

    # Then
    assert len(execution_requests) == 1

    request_schedule = execution_requests[0]
    assert isinstance(request_schedule, ExecutionRequestSchedule)

    # Check schedule
    assert request_schedule.schedule.cron == "30 * * * *"
    assert request_schedule.schedule.timezone == "UTC"

    # Check execution request
    exec_request = request_schedule.execution_request
    assert isinstance(exec_request, ExecutionRequest)
    assert exec_request.executor_id == "default"
    assert exec_request.name == RUN_MONITOR_TRAINING_TASK_NAME
    assert exec_request.exec_id == mock_inferred_monitor.urn

    # Check args
    assert "monitor" in exec_request.args
    assert "context" in exec_request.args
    assert exec_request.args["context"]["dry_run"] is False
    assert exec_request.args["context"]["monitor_urn"] == mock_inferred_monitor.urn


@patch(
    "datahub_executor.common.client.fetcher.monitors.mapper.ONLINE_SMART_ASSERTIONS_ENABLED",
    False,
)
@patch("datahub_executor.common.client.fetcher.monitors.mapper.IS_LOCAL_DEV", True)
def test_generate_training_tasks_local_dev(mock_inferred_monitor: Monitor) -> None:
    """Test generation of training tasks in local dev mode."""
    # Given a monitor with an INFERRED assertion and in local dev mode

    # When
    with patch(
        "datahub_executor.common.client.fetcher.monitors.mapper.is_dry_run_mode",
        return_value=False,
    ):
        with patch(
            "datahub_executor.common.client.fetcher.monitors.mapper.MONITOR_TRAINING_EXECUTOR_ID",
            "default",
        ):
            execution_requests = generate_training_tasks(mock_inferred_monitor)

    # Then
    assert len(execution_requests)
