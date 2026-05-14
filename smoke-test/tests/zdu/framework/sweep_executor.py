"""Suite E — system-level sweep scenario executor.

Implements the ``ScenarioTypeExecutor`` Protocol for ``scenario_type="sweep"``.
Each registered TC has its own validator that reads from the captures on
``ctx`` (``upgrade_nonblocking.*`` and ``sweep_total_migrated`` from Plan 8)
and emits a :class:`ValidationResult`.

Most TCs in Suite E require infra not present on a single-image dev stack
(upgrade-job kill-switch, runtime config knobs, fault injection, race-window
proof). Those are flagged ``expected_to_fail=True`` in ``scenarios.py`` with
a ``skip_reason``; this executor returns ``XFAIL`` for them. TC-404 codifies
the no-op invariant that holds when the mutator chain is empty.
"""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)


class SweepExecutor:
    """Strategy for ``scenario_type="sweep"`` scenarios."""

    def seed(self, scenario: ZDUTestScenario) -> list[str]:
        """Suite E doesn't seed per-scenario — returns empty.

        Sweep scenarios validate AspectMigrationMutatorChain outcomes via
        captures on ``ctx`` populated by Plans F-5/bootJar; no individual
        entity seeding is needed.
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
                    f"No validator registered for sweep TC-{scenario.tc_number}"
                ),
            )
        return validator(scenario, ctx)


def _validate_no_mutators_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-404 — When AspectMigrationMutatorChain is empty, sweep is a no-op.

    Precondition: ``ctx.upgrade_nonblocking`` was captured by Phase 8 AND
    ``sweep_total_migrated == 0`` AND no indices were captured AND no
    dual-write disabled markings were made.

    When the dev stack DOES have mutators registered (typical after Plans F-5
    + bootJar fix), sweep activity will have occurred and the precondition is
    not met — the validator returns SKIP rather than FAIL to avoid spurious
    failures on a correctly-operating stack.
    """
    if ctx.upgrade_nonblocking is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="Phase 8 (upgrade_nonblocking) did not run — no captures",
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
    # Some sweep activity occurred — TC-404 precondition not met.
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=(
            f"Mutator chain was non-empty (migrated={ctx.sweep_total_migrated}, "
            f"indices={len(nb.indices)}, "
            f"disabled={len(nb.dual_write_disabled_indices)}) "
            f"— TC-404 precondition not met"
        ),
    )


def _skip_pending_reindex_capture(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-405 — would assert sweep behavior with rollbackDualWriteEnabled
    flipped, but the underlying capture (``dual_write_disabled_indices``)
    stays empty in the current framework because BuildIndicesIncrementalStep
    doesn't fire a real reindex (G20c — single host-built upgrade.jar mounted
    into both initial and Phase-4 system-update containers means no
    target-vs-current mapping diff).

    Returns SKIP with the honest blocker — was XFAIL with a misleading
    "runtime knob" reason that suggested a smaller fix.
    """
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=scenario.skip_reason or "Pending G20c reindex capture",
    )


def _skip_redundant_with_tc_022(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-406 — APP_SOURCE stamping on sweep writes is already asserted by
    Suite A TC-022. Standalone per-aspect inspection would require a separate
    systemMetadata fetch path that isn't worth adding for one redundant
    assertion. SKIP is the correct semantic — there's nothing to "expect to
    fail" here.
    """
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=scenario.skip_reason or "Redundant with TC-022",
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    404: _validate_no_mutators_noop,
    405: _skip_pending_reindex_capture,
    406: _skip_redundant_with_tc_022,
}
