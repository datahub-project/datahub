from datetime import datetime, timezone
from typing import cast
from unittest.mock import Mock

import pytest

from datahub.configuration.common import OperationalError
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.executor.request.signal_request import SignalRequest
from datahub.executor.result.execution_result import ExecutionResult, Type
from datahub.ingestion.graph.client import DataHubGraph

MAX_LENGTH = 500


@pytest.fixture(autouse=True)
def payload_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    # Read per call, so a small limit keeps fixtures readable.
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


def _context() -> ExecutionContext:
    return ExecutionContext(
        ExecutionRequest(exec_id="exec-1", name="RUN_INGEST", args={})
    )


class TestResultAspectSizeGuard:
    """The guard must leave the aspect at exactly `max_length`, not over it."""

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

    @pytest.mark.parametrize("limit", [10, 40, 80, 200])
    def test_report_stays_within_even_a_limit_smaller_than_the_warning(
        self, executor: ReportingExecutor, monkeypatch: pytest.MonkeyPatch, limit: int
    ) -> None:
        # The prefix is ~75 chars: below that, reserving room leaves an empty body.
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


class TestExecutorInstanceId:
    def test_the_field_is_never_put_on_the_result_aspect(self) -> None:
        """`executorInstanceId` is not an OSS aspect field.

        It is declared only by a custom models package, so OSS must not set it: the
        generated class takes explicit kwargs and would raise TypeError. Deployments
        whose models do declare it add it by overriding
        `_build_execution_request_result_aspect`. The config field itself stays, since
        DefaultExecutor uses it for the pool-claim log line.
        """
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
        assert getattr(aspect, "executorInstanceId", None) is None


class TestCompletionMcpEmission:
    """`_emit_completion_mcp` returning True means "stop retrying", not "succeeded"."""

    @pytest.mark.parametrize("status", [401, 404, 422])
    def test_permanent_failures_are_not_retried(
        self, executor: ReportingExecutor, status: int
    ) -> None:
        executor._datahub_graph.emit_mcp = Mock(  # type: ignore[method-assign]
            side_effect=OperationalError("nope", {"status": status})
        )

        assert executor._emit_completion_mcp("exec-1", Mock()) is True

    def test_transient_failures_are_retried(self, executor: ReportingExecutor) -> None:
        executor._datahub_graph.emit_mcp = Mock(  # type: ignore[method-assign]
            side_effect=OperationalError("boom", {"status": 503})
        )

        assert executor._emit_completion_mcp("exec-1", Mock()) is False

    def test_a_successful_emit_releases_the_stored_result(
        self, executor: ReportingExecutor
    ) -> None:
        executor.results_to_emit["exec-1"] = Mock()

        assert executor._emit_completion_mcp("exec-1", Mock()) is True
        assert "exec-1" not in executor.results_to_emit


class TestKillSignal:
    def test_kill_for_an_untracked_exec_emits_a_cancellation(
        self, executor: ReportingExecutor
    ) -> None:
        # What marks a run CANCELLED in the UI when the executor no longer holds it.
        executor.signal(SignalRequest(exec_id="exec-1", signal="KILL"))

        emitted = cast(Mock, executor._datahub_graph.emit_mcp)
        assert emitted.call_count == 1
        assert emitted.call_args[0][0].entityKeyAspect.id == "exec-1"

    def test_kill_prefers_the_stored_result_over_a_bare_cancellation(
        self, executor: ReportingExecutor
    ) -> None:
        result = ExecutionResult(_context(), Type.CANCELLED)
        result.context.request.start_time = datetime.now(timezone.utc)
        executor.results_to_emit["exec-1"] = result

        executor.signal(SignalRequest(exec_id="exec-1", signal="KILL"))

        # The stored result was emitted and consumed, so no empty cancellation follows.
        assert cast(Mock, executor._datahub_graph.emit_mcp).call_count == 1
        assert "exec-1" not in executor.results_to_emit

    def test_unknown_signals_are_ignored(self, executor: ReportingExecutor) -> None:
        executor.signal(SignalRequest(exec_id="exec-1", signal="PAUSE"))

        cast(Mock, executor._datahub_graph.emit_mcp).assert_not_called()
