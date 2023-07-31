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
    MonitorMode,
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


class TestMonitorManager:
    def setup_method(self) -> None:
        self.fetcher = Mock(spec=MonitorFetcher)
        self.scheduler = Mock(spec=AssertionScheduler)
        self.engine = Mock(spec=AssertionEngine)
        Mock(spec=AssertionEntity)

        self.monitor = Monitor(
            urn="urn:li:monitor:test",
            type=MonitorType.ASSERTION,
            assertion_monitor=AssertionMonitor(assertions=[assertion_spec]),
            mode=MonitorMode.ACTIVE,
        )
        self.manager = MonitorManager(self.fetcher, self.scheduler, self.engine)

    def test_scheduled_assertion(self) -> None:
        self.manager.scheduled_monitors[self.monitor.urn] = self.monitor
        self.fetcher.fetch_monitors.return_value = [self.monitor]
        self.manager.refresh_monitors()

        # Verify that the fetcher's refresh_monitors method was called
        self.fetcher.fetch_monitors.assert_called_once()

        # Verify that the scheduler's add_assertion method was called with the correct arguments
        self.scheduler.add_assertion.assert_called_once_with(
            assertion, parameters, schedule, ANY
        )

    def test_inactive_assertion(self) -> None:
        self.manager.scheduled_monitors[self.monitor.urn] = self.monitor
        monitor = Monitor(
            urn="urn:li:monitor:test",
            type=MonitorType.ASSERTION,
            assertion_monitor=AssertionMonitor(assertions=[assertion_spec]),
            mode=MonitorMode.INACTIVE,
        )
        self.fetcher.fetch_monitors.return_value = [monitor]
        self.manager.refresh_monitors()

        # Verify that the fetcher's refresh_monitors method was called
        self.fetcher.fetch_monitors.assert_called_once()

        # Verify that the scheduler's add_assertion method was not called because this monitor is inactive
        self.scheduler.add_assertion.assert_not_called()

    def test_deleted_assertion(self) -> None:
        self.manager.scheduled_monitors[self.monitor.urn] = self.monitor
        monitor = Monitor(
            urn="urn:li:monitor:test",
            type=MonitorType.ASSERTION,
            assertion_monitor=AssertionMonitor(assertions=[]),
            mode=MonitorMode.INACTIVE,
        )
        self.fetcher.fetch_monitors.return_value = [monitor]
        self.manager.refresh_monitors()

        # Verify that the fetcher's refresh_monitors method was called
        self.fetcher.fetch_monitors.assert_called_once()

        # Verify that the scheduler's add_assertion method was not called because this monitor is inactive
        self.scheduler.add_assertion.assert_not_called()
        self.scheduler.remove_assertion.assert_called_once()

    def test_deleted_missing_assertion(self) -> None:
        # monitor in our list, but not returned (deleted on graphql side)
        self.manager.scheduled_monitors[self.monitor.urn] = self.monitor
        self.fetcher.fetch_monitors.return_value = []
        self.manager.refresh_monitors()

        # Verify that the fetcher's refresh_monitors method was called
        self.fetcher.fetch_monitors.assert_called_once()

        # Verify that the scheduler's add_assertion method was not called because this monitor is inactive
        self.scheduler.add_assertion.assert_not_called()
        self.scheduler.remove_assertion.assert_called_once()
