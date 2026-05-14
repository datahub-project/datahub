"""Unit tests for LiveTrafficExecutor."""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.zdu.framework.context import IOWriteResult, TestContext
from tests.zdu.framework.live_traffic_executor import LiveTrafficExecutor
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="Live Traffic",
        name=f"TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="live_traffic",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="live_traffic",
        suite=Suite.F,
    )


@pytest.fixture
def executor() -> LiveTrafficExecutor:
    return LiveTrafficExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty(self, executor: LiveTrafficExecutor) -> None:
        # Suite F doesn't seed per-scenario.
        assert executor.seed(_scenario(tc=504)) == []


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(
        self, executor: LiveTrafficExecutor
    ) -> None:
        scen = _scenario(tc=501, expected_to_fail=True, skip_reason="needs load gen")
        result = executor.validate(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs load gen"


class TestTC504ConcurrentWrites:
    def test_tc504_passes_when_all_writes_passed(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = TestContext()
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4,
                expected_version=4,
                passed=True,
                timestamp=datetime.utcnow(),
            ),
        ]
        result = executor.validate(_scenario(tc=504), ctx)
        assert result.status == "PASS"
        assert result.expected_to_fail is False

    def test_tc504_fails_when_some_writes_failed(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = TestContext()
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4,
                expected_version=4,
                passed=True,
                timestamp=datetime.utcnow(),
            ),
            IOWriteResult(
                worker="writer-1",
                urn="urn:li:dashboard:(test,zdu-io-pool-1)",
                observed_version=3,
                expected_version=4,
                passed=False,
                timestamp=datetime.utcnow(),
                error="version mismatch",
            ),
        ]
        result = executor.validate(_scenario(tc=504), ctx)
        assert result.status == "FAIL"
        assert result.expected_to_fail is False
        assert "sweep clobbered" in (result.failure_reason or "")
        assert "zdu-io-pool-1" in (result.actual_result or "")

    def test_tc504_skips_when_no_io_writes(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_write_results = []
        result = executor.validate(_scenario(tc=504), ctx)
        assert result.status == "SKIP"
        assert result.expected_to_fail is False
