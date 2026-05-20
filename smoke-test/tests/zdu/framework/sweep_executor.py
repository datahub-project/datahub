"""Suite N sweep-invariant validators (TC-324..031).

These scenarios share the non-blocking sweep phase with Suite N's per-URN
aspect-migration scenarios, so they live in Suite N. Their
``scenario_type="sweep"`` dispatches into ``dispatch_sweep_scenario`` from
``ScenarioExecutor.validate`` instead of the per-URN aspect-migration path.

Validators read from ``ctx.upgrade_nonblocking.*`` and ``ctx.sweep_total_migrated``
populated by ``UpgradeNonBlockingPhase``. TC-327 is the only active outcome
check (no-mutators no-op); the rest are honest SKIPs whose ``skip_reason``
points at the TC that already delivers the same signal.
"""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)


def _validate_no_mutators_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-327 — When ``AspectMigrationMutatorChain`` is empty, sweep is a no-op.

    Precondition: ``ctx.upgrade_nonblocking`` was captured by
    ``UpgradeNonBlockingPhase`` AND ``sweep_total_migrated == 0`` AND no
    indices were captured AND no dual-write disabled markings were made.

    When the dev stack DOES have mutators registered (the typical case),
    sweep activity will have occurred and the precondition is not met —
    the validator returns SKIP rather than FAIL.
    """
    if ctx.upgrade_nonblocking is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="upgrade_nonblocking phase did not run — no captures",
        )
    nb = ctx.upgrade_nonblocking
    is_noop_state = (
        not nb.indices
        and not nb.dual_write_disabled_indices
        and ctx.sweep_total_migrated == 0
    )
    if is_noop_state:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                "No mutators registered: sweep no-op confirmed "
                "(0 migrated, 0 indices captured, 0 disabled)"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=(
            f"Mutator chain was non-empty (migrated={ctx.sweep_total_migrated}, "
            f"indices={len(nb.indices)}, "
            f"disabled={len(nb.dual_write_disabled_indices)}) "
            f"— TC-327 precondition not met"
        ),
    )


def _skip_with_scenario_reason(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """Generic SKIP validator — returns the scenario's own ``skip_reason``.

    Used by sweep-invariant scenarios whose original XFAILs collapse to
    outcomes already asserted by TC-316 (re-run-after-success no-op),
    TC-322 (``APP_SOURCE`` stamping), or TC-403 (no lost writes), or which
    need timing instrumentation outside this branch's scope.
    """
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=scenario.skip_reason or "Skipped on dev stack",
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    # Honest SKIPs: each scenario's ``skip_reason`` documents why the
    # outcome is either duplicated by another TC or untractable here.
    # TCs 24..31 were originally numbered 401..408 in the design doc; they
    # were folded into Suite N's range because both groups exercise the
    # same non-blocking sweep phase.
    324: _skip_with_scenario_reason,  # duplicate of TC-316
    325: _skip_with_scenario_reason,  # untractable — batchDelayMs=0 on dev
    326: _skip_with_scenario_reason,  # duplicate of TC-316
    327: _validate_no_mutators_noop,
    328: _skip_with_scenario_reason,  # pending G20c reindex capture
    329: _skip_with_scenario_reason,  # redundant with TC-322
    330: _skip_with_scenario_reason,  # duplicate of TC-403
    331: _skip_with_scenario_reason,  # duplicate of TC-316
}


def dispatch_sweep_scenario(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """Run the registered validator for a ``scenario_type="sweep"`` scenario.

    ``ScenarioExecutor.validate`` calls this when the scenario type is
    ``"sweep"``. Handles ``expected_to_fail`` XFAIL and unknown-TC SKIP
    here so the per-TC validators only deal with the outcome path.
    """
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
                f"No validator registered for sweep TC-{scenario.tc_number}"
            ),
        )
    return validator(scenario, ctx)
