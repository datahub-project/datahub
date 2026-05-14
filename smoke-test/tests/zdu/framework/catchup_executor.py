"""Suite D — ES Phase 2 catch-up scenario executor.

Implements the ``ScenarioTypeExecutor`` Protocol for ``scenario_type="catch_up"``.
Each registered TC has its own validator that reads from the captures on
``ctx`` (``gap_urns`` from Plan 5, ``upgrade_nonblocking.*`` from Plan 7) and
emits a :class:`ValidationResult`.

Most TCs in Suite D require infra not present on a single-image dev stack
(real two-image rolling restart, runtime config knobs, upgrade-job interrupt
kit). Those are flagged ``expected_to_fail=True`` in ``scenarios.py`` with
a ``skip_reason``; this executor returns ``XFAIL`` for them. TC-305 and
TC-306 codify the no-op invariants that hold even on dev.
"""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)


class CatchUpScenarioExecutor:
    """Strategy for ``scenario_type="catch_up"`` scenarios."""

    def seed(self, scenario: ZDUTestScenario) -> list[str]:
        """Suite D doesn't seed per-scenario — returns empty.

        Plan 5's ``InjectTrafficPrePhase`` already seeds 10 shared
        ``zdu-gap-*`` URNs that all Suite D scenarios validate against.
        """
        return []

    def validate(self, scenario: ZDUTestScenario, ctx: TestContext) -> ValidationResult:
        if scenario.expected_to_fail:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="XFAIL",
                expected_to_fail=True,
                actual_result="Expected failure on this stack",
                failure_reason=scenario.skip_reason,
            )
        validator = _VALIDATORS.get(scenario.tc_number)
        if validator is None:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result=(
                    f"No validator registered for catch_up TC-{scenario.tc_number}"
                ),
            )
        return validator(scenario, ctx)


def _validate_t0_ge_t1_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-305 — When ``T0 >= T1`` for any index, catch-up must skip and
    emit no MCLs.

    Two cases:

    * No catch-up windows recorded → invariant holds vacuously → PASS.
    * Every window has ``t0 < t1`` → invariant doesn't apply (no anomalous
      window to verify the no-op against) → PASS.
    * One or more windows have ``t0 >= t1`` → the invariant requires the
      runtime to have emitted zero MCLs for that index. The dev stack has
      no MCL counter, so we can't directly verify non-emission — return
      SKIP rather than a silent PASS. The prior behavior of returning PASS
      with a log.warning would mask a real regression of the no-op contract.
    """
    nb = ctx.upgrade_nonblocking
    if nb is None or not nb.catch_up_windows:
        return _pass(scenario, "no catch-up windows recorded — no-op holds")
    bad = [(idx, w) for idx, w in nb.catch_up_windows.items() if w[0] >= w[1]]
    if not bad:
        return _pass(scenario, "all windows have t0 < t1")
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=(
            f"{len(bad)} window(s) with t0>=t1 detected — {bad}. "
            f"MCL non-emission requires a counter the dev stack doesn't expose; "
            f"reporting SKIP rather than a silent PASS so a real no-op contract "
            f"violation can't slip through. Separate plan for MCL counter."
        ),
    )


def _validate_no_phase1_result_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-306 — When Phase 1 produced no indices, catch-up must be a no-op.

    If ``ctx.upgrade_blocking.indices`` is empty (Phase 1 did nothing),
    ``ctx.upgrade_nonblocking.catch_up_windows`` must also be empty.
    """
    blocking = ctx.upgrade_blocking
    if blocking is not None and blocking.indices:
        # Phase 1 produced indices — the no-op invariant doesn't apply.
        return _pass(scenario, "Phase 1 produced indices — no-op invariant N/A")
    nb = ctx.upgrade_nonblocking
    if nb is None:
        return _pass(scenario, "non-blocking phase did not run")
    if not nb.catch_up_windows:
        return _pass(scenario, "blocking empty + nonblocking captured no windows")
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"Phase 1 produced no indices but Phase 2 recorded windows: "
            f"{list(nb.catch_up_windows.keys())}"
        ),
        failure_reason="catch-up should be no-op when Phase 1 produced no indices",
    )


def _pass(scenario: ZDUTestScenario, note: str) -> ValidationResult:
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=note,
    )


def _skip_pending_reindex_capture(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-308 / TC-309 — would assert dual-write disable transitions, but the
    underlying capture (``ctx.upgrade_nonblocking.dual_write_disabled_indices``)
    stays empty because BuildIndicesIncrementalStep doesn't fire a real
    reindex (G20c — single host-built upgrade.jar means no target-vs-current
    mapping diff).

    Returns SKIP rather than XFAIL — there's no reason to "expect failure"
    here. The validators are correct; the precondition data just isn't being
    captured yet.
    """
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=scenario.skip_reason or "Pending G20c reindex capture",
    )


# Per-TC dispatch table. Scenarios with ``expected_to_fail=True`` are handled
# by the shared short-circuit in ``CatchUpScenarioExecutor.validate``; this
# table holds TCs that either have an active dev-stack assertion or a
# deliberate SKIP-returning validator (vs the legacy XFAIL classification).
_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    305: _validate_t0_ge_t1_noop,
    306: _validate_no_phase1_result_noop,
    308: _skip_pending_reindex_capture,
    309: _skip_pending_reindex_capture,
}
