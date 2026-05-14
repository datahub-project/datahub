"""Unit tests for CatchUpScenarioExecutor."""

from __future__ import annotations

import pytest

from tests.zdu.framework.catchup_executor import CatchUpScenarioExecutor
from tests.zdu.framework.context import (
    IndexState,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
)
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
        category="ES Phase 2 Catch-Up",
        name=f"TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="catch_up",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="catch_up",
        suite=Suite.D,
    )


@pytest.fixture
def executor() -> CatchUpScenarioExecutor:
    return CatchUpScenarioExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty_list(self, executor: CatchUpScenarioExecutor) -> None:
        # Suite D doesn't seed per-scenario.
        assert executor.seed(_scenario(tc=305)) == []


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        scen = _scenario(tc=301, expected_to_fail=True, skip_reason="needs CI")
        result = executor.validate(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs CI"


class TestTC305T0GeT1Noop:
    def test_no_catch_up_windows_passes(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        # Dev stack — empty captures = implicit no-op.
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(catch_up_windows={})
        result = executor.validate(_scenario(tc=305), ctx)
        assert result.status == "PASS"

    def test_window_with_t0_lt_t1_passes_when_gap_urns_absent(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        # T0 < T1 — catch-up CAN run; the no-op assertion is vacuously true
        # when we have no gap_urns to find in either index. Returns PASS.
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            catch_up_windows={"dashboardindex_v2_new": (100, 200)}
        )
        ctx.gap_urns = []
        result = executor.validate(_scenario(tc=305), ctx)
        assert result.status == "PASS"

    def test_window_with_t0_ge_t1_returns_skip(
        self,
        executor: CatchUpScenarioExecutor,
    ) -> None:
        # T0 >= T1 means the runtime should have emitted zero MCLs for that
        # index. Without an MCL counter on the dev stack the validator can't
        # directly verify that — returning PASS would silently hide a real
        # no-op contract violation, so we report SKIP instead.
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            catch_up_windows={"dashboardindex_v2_new": (200, 100)}
        )
        result = executor.validate(_scenario(tc=305), ctx)
        assert result.status == "SKIP"
        assert "t0>=t1" in (result.actual_result or "")
        assert "MCL counter" in (result.actual_result or "")


class TestTC306NoPhase1Noop:
    def test_empty_blocking_indices_with_empty_nonblocking_passes(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(indices=[])
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(catch_up_windows={})
        result = executor.validate(_scenario(tc=306), ctx)
        assert result.status == "PASS"

    def test_nonblocking_captured_windows_when_blocking_empty_fails(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        # If Phase 1 produced no indices but Phase 2 still recorded catch-up
        # windows, the no-op invariant is violated.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(indices=[])
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            catch_up_windows={"dashboardindex_v2_new": (100, 200)}
        )
        result = executor.validate(_scenario(tc=306), ctx)
        assert result.status == "FAIL"

    def test_blocking_with_indices_skips_invariant(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        # The no-op assertion only applies when blocking.indices is empty.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices=[
                IndexState(
                    alias="dashboardindex_v2", source_doc_count=100, status="COMPLETED"
                )
            ]
        )
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            catch_up_windows={"dashboardindex_v2_new": (100, 200)}
        )
        result = executor.validate(_scenario(tc=306), ctx)
        assert result.status == "PASS"


class TestUnknownTC:
    def test_unknown_catchup_tc_returns_skip(
        self, executor: CatchUpScenarioExecutor
    ) -> None:
        scen = _scenario(tc=399)  # not in the registered range
        result = executor.validate(scen, TestContext())
        assert result.status == "SKIP"
