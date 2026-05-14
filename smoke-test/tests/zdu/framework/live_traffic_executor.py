"""Suite F — live-traffic scenario executor.

Implements the ``ScenarioTypeExecutor`` Protocol for ``scenario_type="live_traffic"``.
Each registered TC has its own validator that reads from the captures on
``ctx`` (``io_write_results`` from Phase 8) and emits a :class:`ValidationResult`.

Most TCs in Suite F require sustained load generators or ingestion harnesses
not present on the dev stack. Those are flagged ``expected_to_fail=True`` in
``scenarios.py`` with a ``skip_reason``; this executor returns ``XFAIL`` for
them. TC-504 codifies the concurrent-write safety invariant: every IO-pool
write captured in ``ctx.io_write_results`` must have ``passed=True`` — the
sweep must not clobber concurrent client writes.
"""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)


class LiveTrafficExecutor:
    """Strategy for ``scenario_type="live_traffic"`` scenarios."""

    def seed(self, scenario: ZDUTestScenario) -> list[str]:
        """Suite F doesn't seed per-scenario — returns empty.

        Live-traffic scenarios validate concurrent write safety via captures
        on ``ctx`` populated by the IO-pool harness in Phase 8; no individual
        entity seeding is needed here.
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
                    f"No validator registered for live_traffic TC-{scenario.tc_number}"
                ),
            )
        return validator(scenario, ctx)


def _validate_no_lost_writes(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-504 — Sweep must not clobber concurrent client writes.

    Every ``IOWriteResult`` captured in ``ctx.io_write_results`` must have
    ``passed=True``. An empty ``io_write_results`` means Phase 8 was skipped
    or the IO harness produced no writes — the validator returns SKIP rather
    than FAIL in that case, since the invariant holds vacuously.
    """
    if not ctx.io_write_results:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No IO harness writes recorded — Phase 8 was skipped or"
                " harness produced no writes"
            ),
        )
    failed = [w for w in ctx.io_write_results if not w.passed]
    total = len(ctx.io_write_results)
    if failed:
        first = failed[0]
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"{len(failed)}/{total} concurrent writes lost data; "
                f"first: urn={first.urn} observed={first.observed_version} "
                f"expected={first.expected_version} error={first.error}"
            ),
            failure_reason="sweep clobbered concurrent client writes",
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=f"All {total} concurrent IO-pool writes preserved client content",
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    504: _validate_no_lost_writes,
}
