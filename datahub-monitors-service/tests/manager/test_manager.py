from unittest.mock import ANY, Mock

from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.assertion.scheduler.scheduler import AssertionScheduler
from datahub_monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.manager.manager import MonitorManager
from datahub_monitors.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationSpec,
    AssertionMonitor,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    Monitor,
    MonitorType,
)

# Initialize sample objects
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
schedule = CronSchedule(cron="* * * * *", timezone="America/Los_Angeles")
assertion_spec = AssertionEvaluationSpec(
    assertion=assertion, schedule=schedule, parameters=parameters
)

monitor = Monitor(
    urn="urn:li:monitor:test",
    type=MonitorType.ASSERTION,
    assertion_monitor=AssertionMonitor(assertions=[assertion_spec]),
)


def test_refresh_assertions() -> None:
    # Create mock objects
    fetcher = Mock(spec=MonitorFetcher)
    scheduler = Mock(spec=AssertionScheduler)
    engine = Mock(spec=AssertionEngine)
    Mock(spec=AssertionEntity)

    fetcher.fetch_monitors.return_value = [monitor]

    manager = MonitorManager(fetcher, scheduler, engine)

    manager.refresh_monitors()

    # Verify that the fetcher's refresh_monitors method was called
    fetcher.fetch_monitors.assert_called_once()

    # Verify that the scheduler's add_assertion method was called with the correct arguments
    scheduler.add_assertion.assert_called_once_with(
        assertion, parameters, schedule, ANY
    )
