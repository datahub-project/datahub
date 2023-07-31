from unittest.mock import Mock

import pytz
from apscheduler.triggers.cron import CronTrigger

from datahub_monitors.assertion.scheduler.scheduler import AssertionScheduler
from datahub_monitors.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
)

# Sample Assertion and Context
entity = AssertionEntity(
    urn="urn:li:dataset:test",
    platformUrn="urn:li:dataPlatform:snowflake",
    platformInstance=None,
    subTypes=None,
)
assertion = Assertion(
    urn="urn:li:assertion:test",
    type=AssertionType.DATASET,
    entity=entity,
    connectionUrn="urn:li:dataPlatform:snowflake",
    freshnessAssertion=None,
)
parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    datasetFreshnessParameters=DatasetFreshnessAssertionParameters(
        sourceType=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
        field=None,
        auditLog=None,
    ),
)
schedule = CronSchedule(cron="1 2 3 4 5", timezone="America/Los_Angeles")
context = AssertionEvaluationContext()


def test_schedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    assertion_scheduler = AssertionScheduler(
        Mock(), [], default_schedule="0 * * * *", default_timezone="America/Los_Angeles"
    )

    # Add assertion to the scheduler
    job_id = assertion_scheduler.add_assertion(assertion, parameters, schedule, context)

    # Verify that the assertion was scheduled correctly
    job = assertion_scheduler.scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == assertion
    assert job.args[1] == parameters
    assert job.args[2] == context
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("minute")]) == "1"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("hour")]) == "2"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day")]) == "3"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("month")]) == "4"
    assert str(job.trigger.fields[CronTrigger.FIELD_NAMES.index("day_of_week")]) == "5"

    assert job.trigger.timezone == pytz.timezone(schedule.timezone)


def test_unschedule_assertion() -> None:
    # Initialize assertion scheduler with a mocked AssertionEngine
    assertion_scheduler = AssertionScheduler(
        Mock(), default_schedule="0 * * * *", default_timezone="America/Los_Angeles"
    )

    # Add assertion to the scheduler
    job_id = assertion_scheduler.add_assertion(assertion, parameters, schedule, context)

    # Verify that the assertion was scheduled correctly
    job = assertion_scheduler.scheduler.get_job(job_id)
    assert job is not None
    assert job.args[0] == assertion
    assert job.args[1] == parameters
    assert job.args[2] == context

    # Remove the assertion from the scheduler
    assertion_scheduler.remove_assertion(assertion)

    # Verify that the assertion was unscheduled correctly
    job = assertion_scheduler.scheduler.get_job(job_id)
    assert job is None
