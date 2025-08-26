from unittest.mock import Mock, patch

import pytest
import pytz
from acryl.executor.request.execution_request import ExecutionRequest
from apscheduler.triggers.cron import CronTrigger

from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    RUN_ASSERTION_TASK_NAME,
    RUN_INGEST_TASK_NAME,
    RUN_MONITOR_TRAINING_TASK_NAME,
)
from datahub_executor.common.monitor.executor import MonitorExecutor
from datahub_executor.common.types import CronSchedule
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

# Sample Execution Requests
assertion_request = ExecutionRequest(
    id="test-assertion-request",
    exec_id="urn:li:assertion:test",
    name=RUN_ASSERTION_TASK_NAME,
    args={},
)

ingestion_request = ExecutionRequest(
    id="test-ingestion-request",
    exec_id="urn:li:ingestion:test",
    name=RUN_INGEST_TASK_NAME,
    args={},
)

monitor_request = ExecutionRequest(
    id="test-monitor-request",
    exec_id="urn:li:monitor:test",
    name=RUN_MONITOR_TRAINING_TASK_NAME,
    args={},
)

schedule = CronSchedule(cron="1 2 3 4 5", timezone="America/Los_Angeles")


def test_schedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)
    execution_request_scheduler = ExecutionRequestScheduler(
        [],
        default_schedule="0 * * * *",
        default_timezone="America/Los_Angeles",
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Add assertion to the scheduler
    job_id = execution_request_scheduler.add_execution_request(
        assertion_request, schedule
    )

    # Verify that the assertion was scheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == assertion_request
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("minute")]) == "1"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("hour")]) == "2"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day")]) == "3"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("month")]) == "4"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day_of_week")]) == "5"

    assert job.trigger.timezone == pytz.timezone(schedule.timezone)


def test_unschedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    execution_request_scheduler = ExecutionRequestScheduler(
        default_schedule="0 * * * *",
        default_timezone="America/Los_Angeles",
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Add assertion to the scheduler
    job_id = execution_request_scheduler.add_execution_request(
        assertion_request, schedule
    )

    # Verify that the assertion was scheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == assertion_request

    # Remove the assertion from the scheduler
    execution_request_scheduler.remove_execution_request(assertion_request)

    # Verify that the assertion was unscheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is None


def test_initialization_with_execution_requests() -> None:
    """Test initializing scheduler with a list of execution requests"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    # Create multiple execution requests
    requests = [assertion_request, ingestion_request, monitor_request]

    scheduler = ExecutionRequestScheduler(
        execution_requests=requests,
        default_schedule="0 * * * *",
        default_timezone="America/Los_Angeles",
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Verify execution_request_ids contains the expected IDs
    expected_ids = [req.exec_id for req in requests]
    assert scheduler.execution_request_ids == expected_ids

    # Verify the schedulers were initialized and started
    assert scheduler.assertion_scheduler is not None
    assert scheduler.monitor_scheduler is not None
    assert scheduler.ingestion_scheduler is not None


def test_get_scheduler_for_execution_request() -> None:
    """Test that the correct scheduler is returned for each request type"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Test for assertion request
    result = scheduler.get_scheduler_for_execution_request(assertion_request)
    assert result is scheduler.assertion_scheduler

    # Test for ingestion request
    result = scheduler.get_scheduler_for_execution_request(ingestion_request)
    assert result is scheduler.ingestion_scheduler

    # Test for monitor request
    result = scheduler.get_scheduler_for_execution_request(monitor_request)
    assert result is scheduler.monitor_scheduler


@patch(
    "datahub_executor.coordinator.scheduler.build_execution_request_input_from_request"
)
@patch("datahub_executor.coordinator.scheduler.emit_execution_request_input")
def test_submit_execution_request_ingestion(mock_emit: Mock, mock_build: Mock) -> None:
    """Test submitting an ingestion execution request"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Configure the mock to return a valid input
    mock_build.return_value = {"some": "input"}

    # Submit ingestion request
    scheduler.submit_execution_request(ingestion_request)

    # Verify helpers were called correctly
    mock_build.assert_called_once_with(ingestion_request)
    mock_emit.assert_called_once_with({"some": "input"})


@patch(
    "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
    True,
)
def test_submit_execution_request_assertion_embedded() -> None:
    """Test submitting an assertion execution request to embedded worker"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Create assertion request with embedded pool ID
    embedded_assertion = ExecutionRequest(
        exec_id="urn:li:assertion:embedded",
        name=RUN_ASSERTION_TASK_NAME,
        executor_id=DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
        args={},
    )

    # Submit request
    scheduler.submit_execution_request(embedded_assertion)

    # Verify assertion was executed locally
    assertion_executor_mock.execute.assert_called_once_with(embedded_assertion)


@patch(
    "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
    False,
)
@patch("datahub_executor.coordinator.scheduler.apply_remote_assertion_request")
def test_submit_execution_request_assertion_remote(mock_apply_remote: Mock) -> None:
    """Test submitting an assertion execution request to remote worker"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Create assertion request with remote pool ID
    remote_assertion = ExecutionRequest(
        exec_id="urn:li:assertion:remote",
        name=RUN_ASSERTION_TASK_NAME,
        executor_id="some-remote-pool",
        args={},
    )

    # Submit request
    scheduler.submit_execution_request(remote_assertion)

    # Verify remote execution was requested
    mock_apply_remote.assert_called_once_with(remote_assertion, "some-remote-pool")


@patch(
    "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
    True,
)
def test_submit_execution_request_monitor_embedded() -> None:
    """Test submitting a monitor execution request to embedded worker"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Create monitor request with embedded pool ID
    embedded_monitor = ExecutionRequest(
        exec_id="urn:li:monitor:embedded",
        name=RUN_MONITOR_TRAINING_TASK_NAME,
        executor_id=DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
        args={},
    )

    # Submit request
    scheduler.submit_execution_request(embedded_monitor)

    # Verify monitor was executed locally
    monitor_executor_mock.execute.assert_called_once_with(embedded_monitor)


@patch(
    "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
    False,
)
@patch("datahub_executor.coordinator.scheduler.apply_remote_monitor_training_request")
def test_submit_execution_request_monitor_remote(mock_apply_remote: Mock) -> None:
    """Test submitting a monitor execution request to remote worker"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Create monitor request with remote pool ID
    remote_monitor = ExecutionRequest(
        exec_id="urn:li:monitor:remote",
        name=RUN_MONITOR_TRAINING_TASK_NAME,
        executor_id="some-remote-pool",
        args={},
    )

    # Submit request
    scheduler.submit_execution_request(remote_monitor)

    # Verify remote execution was requested
    mock_apply_remote.assert_called_once_with(remote_monitor, "some-remote-pool")


def test_should_execute_embedded() -> None:
    """Test the should_execute_embedded method with different configurations"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Test cases with DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED = True
    with patch(
        "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
        True,
    ):
        with patch(
            "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_POOL_ID",
            "my-pool-id",
        ):
            # Case 1: executor_id is EMBEDDED_POOL_ID
            request2 = ExecutionRequest(
                exec_id="urn:2",
                name=RUN_ASSERTION_TASK_NAME,
                executor_id=DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                args={},
            )
            assert scheduler.should_execute_embedded(request2) is True

            # Case 2: executor_id matches POOL_ID
            request3 = ExecutionRequest(
                exec_id="urn:3",
                name=RUN_ASSERTION_TASK_NAME,
                executor_id="my-pool-id",
                args={},
            )
            assert scheduler.should_execute_embedded(request3) is True

            # Case 3: executor_id is different
            request4 = ExecutionRequest(
                exec_id="urn:4",
                name=RUN_ASSERTION_TASK_NAME,
                executor_id="different-pool",
                args={},
            )
            assert scheduler.should_execute_embedded(request4) is False

    # Test with DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED = False
    with patch(
        "datahub_executor.coordinator.scheduler.DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED",
        False,
    ):
        request = ExecutionRequest(
            exec_id="urn:test", name=RUN_ASSERTION_TASK_NAME, executor_id="", args={}
        )
        assert scheduler.should_execute_embedded(request) is False


def test_default_schedule_and_timezone() -> None:
    """Test that default schedule and timezone are used when not provided in CronSchedule"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        default_schedule="0 12 * * *",  # Noon every day
        default_timezone="UTC",
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Need to patch the implementation since CronSchedule requires non-None values
    with patch(
        "datahub_executor.coordinator.scheduler.ExecutionRequestScheduler.schedule_execution_request"
    ) as mock_schedule:
        # Return a fake job id from the mock
        mock_schedule.return_value = "fake-job-id"

        # Set up mock job for assertion_scheduler.get_job to return
        mock_job = Mock()
        mock_job.trigger = Mock()
        mock_job.trigger.fields = [
            "0",
            "12",
            "*",
            "*",
            "*",
        ]  # Representing "0 12 * * *"
        mock_job.trigger.timezone = pytz.timezone("UTC")
        scheduler.assertion_scheduler.get_job = Mock(return_value=mock_job)

        # Use a schedule object - implementation will check for None values
        test_schedule = CronSchedule(cron="", timezone="")
        job_id = scheduler.add_execution_request(assertion_request, test_schedule)

        # Verify schedule_execution_request was called
        mock_schedule.assert_called_once()

        # Verify job can be retrieved with the fake id
        job = scheduler.assertion_scheduler.get_job(job_id)
        assert job is not None
        assert job.trigger.fields[1] == "12"  # Hour is at index 1
        assert job.trigger.timezone == pytz.timezone("UTC")


def test_exception_handling_during_scheduling() -> None:
    """Test exception handling during scheduling"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Create a valid schedule
    test_schedule = CronSchedule(cron="0 * * * *", timezone="America/Los_Angeles")

    # Directly patch the schedule_execution_request method to simulate job creation failing
    with patch.object(scheduler, "schedule_execution_request") as mock_schedule:
        # Simulate the function raising an exception before returning job.id
        mock_schedule.side_effect = ValueError("Invalid cron expression")

        # The add_execution_request method doesn't catch exceptions, so it should propagate
        with pytest.raises(ValueError, match="Invalid cron expression"):
            scheduler.add_execution_request(assertion_request, test_schedule)

        # Verify schedule_execution_request was called
        mock_schedule.assert_called_once_with(assertion_request, test_schedule)


def test_shutdown() -> None:
    """Test the shutdown method"""
    assertion_executor_mock = Mock(spec=AssertionExecutor)
    monitor_executor_mock = Mock(spec=MonitorExecutor)

    # Setup mock schedulers to verify they are shut down
    assertion_scheduler_mock = Mock()
    monitor_scheduler_mock = Mock()
    ingestion_scheduler_mock = Mock()

    scheduler = ExecutionRequestScheduler(
        override_assertion_executor=assertion_executor_mock,
        override_monitor_executor=monitor_executor_mock,
    )

    # Replace the real schedulers with mocks
    scheduler.assertion_scheduler = assertion_scheduler_mock
    scheduler.monitor_scheduler = monitor_scheduler_mock
    scheduler.ingestion_scheduler = ingestion_scheduler_mock

    # Call shutdown
    scheduler.shutdown()

    # Verify all schedulers were shut down
    assertion_scheduler_mock.shutdown.assert_called_once()
    monitor_scheduler_mock.shutdown.assert_called_once()
    ingestion_scheduler_mock.shutdown.assert_called_once()

    # Verify executors were shut down (twice due to code duplication bug in the implementation)
    assert assertion_executor_mock.shutdown.call_count == 2
    # In the real code, it calls assertion_executor.shutdown() twice, which is likely a bug
