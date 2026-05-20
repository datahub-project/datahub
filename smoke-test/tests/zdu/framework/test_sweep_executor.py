"""Unit tests for the Suite N sweep-invariant validator dispatch."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import TestContext, UpgradeNonBlockingResult
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite
from tests.zdu.framework.sweep_executor import dispatch_sweep_scenario


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
        suite=Suite.N,
    )


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(self) -> None:
        scen = _scenario(
            tc=324, expected_to_fail=True, skip_reason="needs interrupt kit"
        )
        result = dispatch_sweep_scenario(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs interrupt kit"


class TestTC27NoMutatorsNoop:
    def test_tc27_passes_when_chain_was_empty(self) -> None:
        # sweep_total_migrated == 0, indices == [], dual_write_disabled_indices == []
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            indices=[],
            dual_write_disabled_indices=[],
        )
        ctx.sweep_total_migrated = 0
        result = dispatch_sweep_scenario(_scenario(tc=327), ctx)
        assert result.status == "PASS"

    def test_tc27_skips_when_chain_had_mutators(self) -> None:
        # sweep_total_migrated > 0 means mutators were registered.
        # TC-327 precondition not met → SKIP, not FAIL.
        ctx = TestContext()
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
            indices=[],
            dual_write_disabled_indices=["dashboardindex_v2_new"],
        )
        ctx.sweep_total_migrated = 5
        result = dispatch_sweep_scenario(_scenario(tc=327), ctx)
        assert result.status == "SKIP"


class TestUnknownTC:
    def test_unknown_tc_returns_skip(self) -> None:
        # 999 is not in the registered range (324..331).
        scen = _scenario(tc=999)
        result = dispatch_sweep_scenario(scen, TestContext())
        assert result.status == "SKIP"


class TestHonestSkipDispatch:
    """TC-324/325/326/328/329/330/331 share the generic SKIP validator."""

    @pytest.mark.parametrize("tc", [324, 325, 326, 328, 329, 330, 331])
    def test_returns_skip_with_scenario_reason(self, tc: int) -> None:
        scen = _scenario(tc=tc, skip_reason=f"why TC-{tc} skips")
        result = dispatch_sweep_scenario(scen, TestContext())
        assert result.status == "SKIP"
        assert result.expected_to_fail is False
        assert result.actual_result == f"why TC-{tc} skips"
