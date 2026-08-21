import logging
from typing import Any, Dict
from unittest.mock import Mock

import pytest

from datahub.executor.execution import reporting_executor
from datahub.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import ExecutionRequestResultClass

MAX_LENGTH = 500


@pytest.fixture(autouse=True)
def payload_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    # get_payload_max_length() reads the env var on every call, so a small limit here
    # keeps the fixtures readable instead of needing multi-megabyte strings.
    monkeypatch.setenv("ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH", str(MAX_LENGTH))


@pytest.fixture
def executor() -> ReportingExecutor:
    return ReportingExecutor(
        ReportingExecutorConfig(
            id="test-executor",
            task_configs=[],
            secret_stores=[],
            graph_client=Mock(spec=DataHubGraph),
        )
    )


def _request() -> ExecutionRequest:
    return ExecutionRequest(exec_id="test-exec-id", name="RUN_INGEST", args={})


class TestResultAspectSizeGuard:
    """The oversized-payload guard must leave the aspect within `max_length`.

    It previously truncated `report` to exactly max_length and *then* prepended the
    "WARNING: ..." line, so every truncated report came out over the limit by the
    length of that prefix.
    """

    def test_report_over_the_limit_stays_within_the_limit(
        self, executor: ReportingExecutor
    ) -> None:
        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS",
            start_time_ms=0,
            report="r" * (MAX_LENGTH * 2),
            exec_request=_request(),
        )

        assert aspect.report is not None
        # Exactly the budget, not merely under it -- catches over-truncation.
        assert len(aspect.report) == MAX_LENGTH
        assert aspect.report.startswith("WARNING: ")

    @pytest.mark.parametrize("limit", [10, 40, 80, 200, MAX_LENGTH])
    def test_report_stays_within_even_a_limit_smaller_than_the_warning(
        self, executor: ReportingExecutor, monkeypatch: pytest.MonkeyPatch, limit: int
    ) -> None:
        # The warning prefix is ~75 chars, so for small limits reserving room for it
        # yields an empty body and the prefix alone would still exceed the budget.
        monkeypatch.setenv("ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH", str(limit))

        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS",
            start_time_ms=0,
            report="r" * (limit * 3),
            exec_request=_request(),
        )

        assert aspect.report is not None
        assert len(aspect.report) == limit

    def test_report_under_the_limit_is_passed_through_untouched(
        self, executor: ReportingExecutor
    ) -> None:
        report = "r" * 10
        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS", start_time_ms=0, report=report, exec_request=_request()
        )

        assert aspect.report == report

    def test_oversized_combination_drops_the_structured_report(
        self, executor: ReportingExecutor
    ) -> None:
        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS",
            start_time_ms=0,
            report="r" * (MAX_LENGTH - 100),
            structured_report="s" * (MAX_LENGTH - 100),
            exec_request=_request(),
        )

        assert aspect.structuredReport is None
        assert aspect.report is not None
        # Exactly the budget, not merely under it -- catches over-truncation.
        assert len(aspect.report) == MAX_LENGTH

    def test_both_over_the_limit_stays_within_the_limit(
        self, executor: ReportingExecutor
    ) -> None:
        # Both guards fire: the structured report is dropped and the report is
        # truncated, so the report is prefixed with two stacked warning lines.
        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS",
            start_time_ms=0,
            report="r" * (MAX_LENGTH * 2),
            structured_report="s" * (MAX_LENGTH * 2),
            exec_request=_request(),
        )

        assert aspect.structuredReport is None
        assert aspect.report is not None
        # Exactly the budget, not merely under it -- catches over-truncation.
        assert len(aspect.report) == MAX_LENGTH


class TestExecutorInstanceId:
    """`executorInstanceId` is only declared by some models builds.

    The OSS `ExecutionRequestResult` aspect does not declare it; forks and custom
    models packages do. Generated aspect classes take explicit keyword arguments, so
    passing the field to a build that lacks it raises TypeError -- and because this
    method builds every kickoff, progress and completion MCP, that would fail all
    reporting for the executor rather than degrading one field.
    """

    def test_configured_instance_id_never_breaks_aspect_construction(self) -> None:
        # The original failure mode, independent of the guard's internals: against the
        # OSS aspect this raised TypeError, taking out every MCP for the executor.
        executor = ReportingExecutor(
            ReportingExecutorConfig(
                id="test-executor",
                task_configs=[],
                secret_stores=[],
                graph_client=Mock(spec=DataHubGraph),
                executor_instance_id="pool-a",
            )
        )

        aspect = executor._build_execution_request_result_aspect(
            status="SUCCESS", start_time_ms=0, exec_request=_request()
        )

        assert aspect.status == "SUCCESS"

    def test_probe_agrees_with_the_installed_aspect(self) -> None:
        # Pins the guard to reality in whichever models build is installed, so it stays
        # correct for OSS (field absent) and for forks/custom packages (field present).
        # Splatted so the deliberately-unsupported kwarg is a runtime question, not a
        # static one: mypy would reject it inline against the OSS models, and a
        # `type: ignore` would go unused against models that do declare the field.
        kwargs: Dict[str, Any] = {
            "status": "SUCCESS",
            "startTimeMs": 0,
            "executorInstanceId": "pool-a",
        }
        try:
            ExecutionRequestResultClass(**kwargs)
        except TypeError:
            accepted = False
        else:
            accepted = True

        assert (
            accepted is reporting_executor._RESULT_ASPECT_SUPPORTS_EXECUTOR_INSTANCE_ID
        )

    def test_unsupported_field_is_omitted_and_warned_about_only_once(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # This method builds an MCP on every progress heartbeat, so warning per build
        # would flood the log for a whole run. The warning belongs at construction.
        monkeypatch.setattr(
            reporting_executor, "_RESULT_ASPECT_SUPPORTS_EXECUTOR_INSTANCE_ID", False
        )

        with caplog.at_level(logging.WARNING):
            executor = ReportingExecutor(
                ReportingExecutorConfig(
                    id="test-executor",
                    task_configs=[],
                    secret_stores=[],
                    graph_client=Mock(spec=DataHubGraph),
                    executor_instance_id="pool-a",
                )
            )
            for _ in range(5):
                aspect = executor._build_execution_request_result_aspect(
                    status="SUCCESS", start_time_ms=0, exec_request=_request()
                )

        assert getattr(aspect, "executorInstanceId", None) is None
        warnings = [r for r in caplog.records if "executorInstanceId" in r.getMessage()]
        assert len(warnings) == 1

    def test_supported_field_is_forwarded_to_the_aspect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict = {}

        def fake_aspect(**kwargs: object) -> object:
            captured.update(kwargs)
            return Mock()

        monkeypatch.setattr(
            reporting_executor, "_RESULT_ASPECT_SUPPORTS_EXECUTOR_INSTANCE_ID", True
        )
        monkeypatch.setattr(
            reporting_executor, "ExecutionRequestResultClass", fake_aspect
        )
        executor = ReportingExecutor(
            ReportingExecutorConfig(
                id="test-executor",
                task_configs=[],
                secret_stores=[],
                graph_client=Mock(spec=DataHubGraph),
                executor_instance_id="pool-a",
            )
        )

        executor._build_execution_request_result_aspect(
            status="SUCCESS", start_time_ms=0, exec_request=_request()
        )

        assert captured["executorInstanceId"] == "pool-a"
