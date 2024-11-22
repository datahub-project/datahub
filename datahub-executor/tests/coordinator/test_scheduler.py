from unittest.mock import Mock

import pytz
from acryl.executor.request.execution_request import ExecutionRequest
from apscheduler.triggers.cron import CronTrigger

from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.constants import RUN_ASSERTION_TASK_NAME
from datahub_executor.common.types import CronSchedule
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

# Sample Assertion and Context
execution_request = ExecutionRequest(
    id="test-execution-request",
    exec_id="urn:li:assertion:test",
    name=RUN_ASSERTION_TASK_NAME,
    args={},
)
schedule = CronSchedule(cron="1 2 3 4 5", timezone="America/Los_Angeles")


def test_schedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    executor_mock = Mock(spec=AssertionExecutor)
    execution_request_scheduler = ExecutionRequestScheduler(
        [],
        default_schedule="0 * * * *",
        default_timezone="America/Los_Angeles",
        override_assertion_executor=executor_mock,
    )

    # Add assertion to the scheduler
    job_id = execution_request_scheduler.add_execution_request(
        execution_request, schedule
    )

    # Verify that the assertion was scheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == execution_request
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("minute")]) == "1"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("hour")]) == "2"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day")]) == "3"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("month")]) == "4"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day_of_week")]) == "5"

    assert job.trigger.timezone == pytz.timezone(schedule.timezone)


def test_unschedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    executor_mock = Mock(spec=AssertionExecutor)
    execution_request_scheduler = ExecutionRequestScheduler(
        default_schedule="0 * * * *",
        default_timezone="America/Los_Angeles",
        override_assertion_executor=executor_mock,
    )

    # Add assertion to the scheduler
    job_id = execution_request_scheduler.add_execution_request(
        execution_request, schedule
    )

    # Verify that the assertion was scheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == execution_request

    # Remove the assertion from the scheduler
    execution_request_scheduler.remove_execution_request(execution_request)

    # Verify that the assertion was unscheduled correctly
    job = execution_request_scheduler.assertion_scheduler.get_job(job_id)
    assert job is None
