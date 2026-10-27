"""Suite D — ES Phase 2 reindexing scenario executor.

Implements the ``ScenarioTypeExecutor`` Protocol for ``scenario_type="catch_up"``.
Each registered TC has its own validator that reads from the captures on
``ctx`` (``gap_urns`` from Plan 5, ``upgrade_nonblocking.*`` from Plan 7) and
emits a :class:`ValidationResult`.

Most TCs in Suite D require infra not present on a single-image dev stack
(real two-image rolling restart, runtime config knobs, upgrade-job interrupt
kit). Those are flagged ``expected_to_fail=True`` in ``scenarios.py`` with
a ``skip_reason``; this executor returns ``XFAIL`` for them. TC-205 and
TC-206 codify the no-op invariants that hold even on dev.
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
    """TC-203 (was TC-205) — When ``T0 >= T1`` for any index, catch-up must
    skip and emit no MCLs.

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
    """TC-204 (was TC-206) — When Phase 1 produced no indices, catch-up must
    be a no-op.

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
    """TC-205 / TC-206 (were TC-308 / TC-309) — would assert dual-write
    disable transitions, but the
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
def _validate_entity_index_integrity(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-201 — every gap and dual URN is searchable in the entity index
    after Phase 10.

    Data-integrity reframe of the original catch-up scenario. The original
    asserted that 10 gap URNs would catch up to the NEW physical index via
    Phase 2's MCL replay. The dev stack can't exercise the specific Phase 2
    mechanism (upstream gap: missing oldBackingIndexName in MySQL state),
    but the OUTCOME — every URN written across the rolling-restart +
    catch-up window remains searchable post-upgrade — is testable directly.
    """
    snap = ctx.data_integrity_snapshot
    if snap is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="DataIntegritySnapshotPhase did not run",
        )
    missing = [u for u, present in snap.entity_index_presence.items() if not present]
    if missing:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"{len(missing)} URN(s) missing from {snap.entity_index_alias}: "
                f"{missing}"
            ),
            failure_reason=(
                "URN(s) written before or during the rolling-restart window "
                "are not searchable in the entity index post-upgrade"
            ),
        )
    n = len(snap.entity_index_presence)
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"All {n} gap+dual URN(s) searchable in {snap.entity_index_alias}"
        ),
    )


def _validate_systemmetadata_integrity(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-202 (was TC-204) — every gap and dual URN has at least one entry
    in the global
    systemMetadata index after Phase 10.

    Data-integrity reframe — the original TC-204 targeted catch-up of the
    ``systemmetadataindex_v2`` specifically. The dev stack uses
    ``system_metadata_service_v1``; same role, different name. The outcome
    we care about is: every URN whose aspect was written across the
    upgrade has corresponding systemMetadata entries (one per aspect)
    after Phase 10.
    """
    snap = ctx.data_integrity_snapshot
    if snap is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="DataIntegritySnapshotPhase did not run",
        )
    missing = [u for u, c in snap.systemmetadata_counts.items() if c == 0]
    if missing:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"{len(missing)} URN(s) missing from systemMetadata index: {missing}"
            ),
            failure_reason=(
                "No systemMetadata entries for URN(s) written across the "
                "upgrade — aspect-level metadata lost"
            ),
        )
    total = sum(snap.systemmetadata_counts.values())
    n = len(snap.systemmetadata_counts)
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"All {n} gap+dual URN(s) have systemMetadata entries "
            f"({total} aspect entries total)"
        ),
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    # Suite D — ES Phase 2 reindexing (dual-write phase). TC-201/202 assert
    # data integrity outcomes; TC-203/204 are no-op invariants; TC-205/206
    # carry the G20c-pending SKIP reason until BuildIndicesIncrementalStep
    # writes oldBackingIndexName to MySQL.
    201: _validate_entity_index_integrity,
    202: _validate_systemmetadata_integrity,
    203: _validate_t0_ge_t1_noop,
    204: _validate_no_phase1_result_noop,
    205: _skip_pending_reindex_capture,
    206: _skip_pending_reindex_capture,
}
