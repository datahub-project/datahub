from unittest.mock import Mock

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.client.fetcher.monitors.fetcher import MonitorFetcher
from datahub_executor.common.constants import RUN_ASSERTION_TASK_NAME
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationSpec,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    ExecutionRequestSchedule,
    FetcherConfig,
)
from datahub_executor.coordinator.manager import ExecutionRequestManager
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

# Initialize sample objects
entity = AssertionEntity(
    urn="urn:li:dataset:test",
    platform_urn="urn:li:dataPlatform:snowflake",
)
assertion = Assertion(
    urn="urn:li:assertion:test",
    type=AssertionType.DATASET,
    entity=entity,
    connection_urn="urn:li:dataPlatform:snowflake",
)
parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
        source_type=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
    ),
)
schedule = CronSchedule(cron="* * * * *", timezone="America/Los_Angeles")
assertion_spec = AssertionEvaluationSpec(
    assertion=assertion, schedule=schedule, parameters=parameters, context=None
)


class TestExecutionRequestManager:
    def setup_method(self) -> None:
        self.fetcher = Mock(spec=MonitorFetcher)
        self.fetcher.config = FetcherConfig(id="test-fetcher-id", refresh_interval=1)
        self.scheduler = Mock(spec=ExecutionRequestScheduler)
        Mock(spec=AssertionEntity)

        self.execution_request = ExecutionRequest(
            id="test-execution-request",
            exec_id="urn:li:assertion:test",
            name=RUN_ASSERTION_TASK_NAME,
            args={"assertion_spec": assertion_spec.dict(by_alias=True), "context": {}},
        )
        self.fetcher.fetch_execution_requests.return_value = []
        self.manager = ExecutionRequestManager([self.fetcher], self.scheduler)

    def test_scheduled_execution_request(self) -> None:
        self.manager.scheduled_execution_requests[self.fetcher.config.id][
            self.execution_request.exec_id
        ] = self.execution_request
        execution_request_schedule = ExecutionRequestSchedule(
            execution_request=self.execution_request, schedule=schedule
        )
        self.fetcher.fetch_execution_requests.return_value = [
            execution_request_schedule
        ]
        self.manager.refresh_execution_requests(self.fetcher.config.id)

        # Verify that the fetcher's refresh_execution_requests method was called
        self.fetcher.fetch_execution_requests.assert_called()

        # Verify that the scheduler's add_execution_request method was called with the correct arguments
        self.scheduler.add_execution_request.assert_called_once_with(
            self.execution_request, schedule
        )

    def test_deleted_missing_assertion(self) -> None:
        # monitor in our list, but not returned (deleted on graphql side)
        self.manager.scheduled_execution_requests[self.fetcher.config.id][
            self.execution_request.exec_id
        ] = self.execution_request
        self.fetcher.fetch_execution_requests.return_value = []
        self.manager.refresh_execution_requests(self.fetcher.config.id)

        # Verify that the fetcher's refresh_execution_requests method was called
        self.fetcher.fetch_execution_requests.assert_called()

        self.scheduler.add_execution_request.assert_not_called()
        self.scheduler.remove_execution_request.assert_called_once()
