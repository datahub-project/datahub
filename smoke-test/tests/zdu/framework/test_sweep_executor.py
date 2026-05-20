"""Unit tests for the Suite N sweep-invariant validator dispatch."""

from __future__ import annotations

import pytest

from tests.zdu.framework.context import (
    KillSwitchCapture,
    TestContext,
    UpgradeNonBlockingResult,
)
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


def _good_capture(**overrides: object) -> KillSwitchCapture:
    """Build a KillSwitchCapture that represents a successful TC-324 run."""
    cap = KillSwitchCapture(
        seed_count=1000,
        kill_threshold=500,
        aspects_migrated_at_kill=525,
        cursor_at_kill=1700000050000,
        upgrade_state_at_kill="IN_PROGRESS",
        resume_log_observed=True,
        final_aspect_count_at_target=1000,
        final_upgrade_state="SUCCEEDED",
    )
    for k, v in overrides.items():
        setattr(cap, k, v)
    return cap


class TestTC324CursorResumability:
    def _ctx_with_capture(self, cap: KillSwitchCapture | None) -> TestContext:
        ctx = TestContext()
        ctx.kill_switch_capture = cap
        return ctx

    def test_passes_on_good_capture(self) -> None:
        ctx = self._ctx_with_capture(_good_capture())
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "PASS"
        assert "SUCCEEDED" in (result.actual_result or "")

    def test_skips_when_capture_missing(self) -> None:
        result = dispatch_sweep_scenario(
            _scenario(tc=324), self._ctx_with_capture(None)
        )
        assert result.status == "SKIP"
        assert "KillSwitchSweepPhase did not run" in (result.actual_result or "")

    def test_fails_when_killed_too_early(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(aspects_migrated_at_kill=5))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "killed too early" in (result.actual_result or "")

    def test_fails_when_killed_too_late(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(aspects_migrated_at_kill=999))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "killed too late" in (result.actual_result or "")

    def test_fails_when_state_at_kill_wrong(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(upgrade_state_at_kill="SUCCEEDED"))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "IN_PROGRESS" in (result.actual_result or "")

    def test_fails_when_cursor_missing(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(cursor_at_kill=None))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "no cursor" in (result.actual_result or "")

    def test_fails_when_resume_log_not_observed(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(resume_log_observed=False))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "cursor-load" in (result.actual_result or "")

    def test_fails_when_final_count_wrong(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(final_aspect_count_at_target=999))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "final state wrong" in (result.actual_result or "")

    def test_fails_when_final_upgrade_state_wrong(self) -> None:
        ctx = self._ctx_with_capture(_good_capture(final_upgrade_state="IN_PROGRESS"))
        result = dispatch_sweep_scenario(_scenario(tc=324), ctx)
        assert result.status == "FAIL"
        assert "SUCCEEDED" in (result.actual_result or "")


class TestHonestSkipDispatch:
    """TC-325/326/328/329/330/331 share the generic SKIP validator.

    TC-324 (cursor resumability) has its own active validator —
    ``_validate_cursor_resumability`` — and is covered separately.
    """

    @pytest.mark.parametrize("tc", [325, 326, 328, 329, 330, 331])
    def test_returns_skip_with_scenario_reason(self, tc: int) -> None:
        scen = _scenario(tc=tc, skip_reason=f"why TC-{tc} skips")
        result = dispatch_sweep_scenario(scen, TestContext())
        assert result.status == "SKIP"
        assert result.expected_to_fail is False
        assert result.actual_result == f"why TC-{tc} skips"
