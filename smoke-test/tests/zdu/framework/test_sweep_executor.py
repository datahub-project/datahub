"""Unit tests for SweepExecutor."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import TestContext, UpgradeNonBlockingResult
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite
from tests.zdu.framework.sweep_executor import SweepExecutor


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="System-Level Sweep",
        name=f"TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="sweep",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="sweep",
        suite=Suite.E,
    )


@pytest.fixture
def executor() -> SweepExecutor:
    return SweepExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty(self, executor: SweepExecutor) -> None:
        assert executor.seed(_scenario(tc=404)) == []


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(
        self, executor: SweepExecutor
    ) -> None:
        scen = _scenario(
            tc=401,
            expected_to_fail=True,
            skip_reason="needs interrupt kit",
        )
        result = executor.validate(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs interrupt kit"


class TestTC404NoMutatorsNoop:
    def test_tc404_passes_when_chain_was_empty(self, executor: SweepExecutor) -> None:
        # sweep_total_migrated == 0, indices == [], dual_write_disabled_indices == []
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            indices=[],
            dual_write_disabled_indices=[],
        )
        ctx.sweep_total_migrated = 0
        result = executor.validate(_scenario(tc=404), ctx)
        assert result.status == "PASS"

    def test_tc404_skips_when_chain_had_mutators(self, executor: SweepExecutor) -> None:
        # sweep_total_migrated > 0 means mutators were registered.
        # TC-404 precondition not met → SKIP, not FAIL.
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            indices=[],
            dual_write_disabled_indices=["dashboardindex_v2_new"],
        )
        ctx.sweep_total_migrated = 5
        result = executor.validate(_scenario(tc=404), ctx)
        assert result.status == "SKIP"


class TestUnknownTC:
    def test_unknown_tc_returns_skip(self, executor: SweepExecutor) -> None:
        scen = _scenario(tc=499)  # not in the registered range
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"
