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


def _validate_batch_delay(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-325 — Sweep respects the configured ``delayMs`` between batches.

    Reads ``ctx.batch_delay_capture`` populated by ``BatchDelaySweepPhase``.
    Asserts the median inter-cursor-advance gap is at least ``delay_ms *
    0.7`` (70% tolerance for MySQL poll latency + batch processing time)
    and that the sweep completed cleanly with all seeded aspects at
    target ``schemaVersion``.
    """
    cap = ctx.batch_delay_capture
    if cap is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "ctx.batch_delay_capture is None — BatchDelaySweepPhase did not run"
            ),
        )

    advances = cap.cursor_advance_timestamps_s
    if len(advances) < 2:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"only {len(advances)} cursor advance(s) observed — "
                "need at least 2 to measure inter-batch gaps"
            ),
            failure_reason="too few cursor advances for timing assertion",
        )

    # Compute inter-batch gaps (in milliseconds), then take the median —
    # avoids being skewed by Spring startup or final-batch tail latency.
    gaps_ms = sorted(
        (advances[i + 1] - advances[i]) * 1000.0 for i in range(len(advances) - 1)
    )
    median_gap_ms = gaps_ms[len(gaps_ms) // 2]
    min_expected_ms = cap.configured_delay_ms * 0.7

    failures: list[str] = []
    if median_gap_ms < min_expected_ms:
        failures.append(
            f"median inter-batch gap {median_gap_ms:.0f}ms < expected >= "
            f"{min_expected_ms:.0f}ms (configured delay_ms={cap.configured_delay_ms} "
            f"with 70% tolerance)"
        )
    if cap.final_count_at_target != cap.seed_count:
        failures.append(
            f"final state wrong: {cap.final_count_at_target}/{cap.seed_count} "
            "at target schemaVersion"
        )
    if cap.final_upgrade_state != "SUCCEEDED":
        failures.append(
            f"final upgrade state was {cap.final_upgrade_state!r}, expected 'SUCCEEDED'"
        )

    if failures:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result="; ".join(failures),
            failure_reason="batchDelayMs not respected: " + failures[0],
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"Observed {len(advances)} cursor advances; median inter-batch "
            f"gap {median_gap_ms:.0f}ms (>= configured {cap.configured_delay_ms}ms "
            f"* 0.7 = {min_expected_ms:.0f}ms); "
            f"{cap.final_count_at_target}/{cap.seed_count} at target, "
            f"state SUCCEEDED"
        ),
    )


def _validate_cursor_resumability(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-324 — Sweep resumes from persisted cursor after a hard interrupt.

    Reads ``ctx.kill_switch_capture`` populated by
    ``KillSwitchSweepPhase``. Asserts the kill landed mid-execution, the
    MySQL state at kill was ``IN_PROGRESS`` with a cursor, the restart
    logged a cursor-load message, and the resume produced the correct
    final state (all seeded aspects at target ``schemaVersion``).
    """
    cap = ctx.kill_switch_capture
    if cap is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "ctx.kill_switch_capture is None — KillSwitchSweepPhase did not run"
            ),
        )

    failures: list[str] = []
    if cap.aspects_migrated_at_kill < 200:
        failures.append(
            f"killed too early: only {cap.aspects_migrated_at_kill}/{cap.seed_count} "
            "migrated at kill (expected >= 200 for a meaningful resume test)"
        )
    if cap.aspects_migrated_at_kill > 800:
        failures.append(
            f"killed too late: {cap.aspects_migrated_at_kill}/{cap.seed_count} "
            "migrated at kill (expected <= 800; we want resume work remaining)"
        )
    if cap.upgrade_state_at_kill != "IN_PROGRESS":
        failures.append(
            f"MySQL state at kill was {cap.upgrade_state_at_kill!r}, "
            "expected 'IN_PROGRESS'"
        )
    if cap.cursor_at_kill is None:
        failures.append("no cursor (lastCreatedOnMs) persisted in MySQL at kill time")
    if not cap.resume_log_observed:
        failures.append(
            "restart did not log a cursor-load message — "
            "the restart may not actually have read the persisted cursor"
        )
    if cap.final_aspect_count_at_target != cap.seed_count:
        failures.append(
            f"final state wrong: {cap.final_aspect_count_at_target}/{cap.seed_count} "
            "at target schemaVersion after resume"
        )
    if cap.final_upgrade_state != "SUCCEEDED":
        failures.append(
            f"final upgrade state was {cap.final_upgrade_state!r}, expected 'SUCCEEDED'"
        )

    if failures:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result="; ".join(failures),
            failure_reason="cursor resumability failed: " + failures[0],
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"Killed at {cap.aspects_migrated_at_kill}/{cap.seed_count} migrated; "
            f"MySQL state IN_PROGRESS, cursor={cap.cursor_at_kill}; "
            f"restart observed cursor-load message; "
            f"resume completed {cap.final_aspect_count_at_target}/{cap.seed_count} "
            f"at target, state SUCCEEDED"
        ),
    )


def _validate_skip_already_migrated(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-326 — Sweep skips rows already at target ``schemaVersion``.

    Reads ``ctx.skip_migrated_capture`` populated by
    ``SkipAlreadyMigratedSweepPhase``. The phase pre-seeds a mixed batch
    (half v1, half v4) and snapshots the v4 rows. After the sweep runs,
    asserts:

    - every v1-seeded URN is now at target (sweep DID work the rows that
      needed it)
    - every v4-seeded URN's ``(createdon, systemmetadata)`` is
      bit-identical to the pre-sweep snapshot — proving the SQL filter
      ``NOT LIKE '%"schemaVersion":<target>%'`` in
      ``EbeanAspectDao.streamAspectBatchesForMigration`` excluded the
      v4 rows from the stream
    - final upgrade state is ``SUCCEEDED``

    Failure mode this catches: a regression that drops the
    schemaVersion-NOT-LIKE clause would re-write the v4 rows, replacing
    their original ``createdon`` and ``appSource`` provenance — the
    bit-identical check fails.
    """
    cap = ctx.skip_migrated_capture
    if cap is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "ctx.skip_migrated_capture is None — "
                "SkipAlreadyMigratedSweepPhase did not run"
            ),
        )

    failures: list[str] = []
    if cap.post_sweep_v1_at_target_count != cap.seed_v1_count:
        failures.append(
            f"v1 not fully migrated: "
            f"{cap.post_sweep_v1_at_target_count}/{cap.seed_v1_count} at target"
        )
    if cap.post_sweep_v4_untouched_count != cap.seed_v4_count:
        failures.append(
            f"v4 rows touched by sweep: only "
            f"{cap.post_sweep_v4_untouched_count}/{cap.seed_v4_count} bit-identical "
            "post-sweep — schemaVersion-NOT-LIKE filter regressed"
        )
    if cap.final_upgrade_state != "SUCCEEDED":
        failures.append(
            f"final upgrade state was {cap.final_upgrade_state!r}, expected 'SUCCEEDED'"
        )

    if failures:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result="; ".join(failures),
            failure_reason="sweep did not skip already-migrated rows: " + failures[0],
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"v1→target {cap.post_sweep_v1_at_target_count}/{cap.seed_v1_count}; "
            f"v4 untouched {cap.post_sweep_v4_untouched_count}/{cap.seed_v4_count}; "
            f"state SUCCEEDED ({cap.total_duration_s:.1f}s)"
        ),
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    # Honest SKIPs: each scenario's ``skip_reason`` documents why the
    # outcome is either duplicated by another TC or untractable here.
    # TCs 24..31 were originally numbered 401..408 in the design doc; they
    # were folded into Suite N's range because both groups exercise the
    # same non-blocking sweep phase.
    324: _validate_cursor_resumability,
    325: _validate_batch_delay,
    326: _validate_skip_already_migrated,
    327: _validate_no_mutators_noop,
    328: _skip_with_scenario_reason,  # pending G20c reindex capture
    # Two earlier scenarios were dropped from the grid — old TC-329
    # "APP_SOURCE stamped on sweep writes" (syntactic duplicate of
    # TC-322's appSource check) and old TC-330 "IF_VERSION_MATCH header
    # prevents stomping" (duplicate of TC-403's race-forced concurrent
    # write check). Higher TCs shifted down to keep the range
    # contiguous.
    329: _skip_with_scenario_reason,  # duplicate of TC-316 (was TC-331)
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
