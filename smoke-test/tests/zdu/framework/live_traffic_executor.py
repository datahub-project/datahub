"""Suite F — live-traffic scenario executor.

Implements the ``ScenarioTypeExecutor`` Protocol for ``scenario_type="live_traffic"``.
Each registered TC has its own validator that reads from captures on ``ctx``
(``io_observations``, ``io_write_results``, ``data_integrity_snapshot``) and
emits a :class:`ValidationResult`.

Validator map (post-renumber):

* TC-401 — every gap/dual URN's ``embed`` aspect is at the target schemaVersion
  in MySQL after Phase 10's sweep (reframe of the old "writes persist" TC-402).
* TC-402 — for each URN seen across multiple ``io_observations``, the
  ``observed_version`` sequence is monotonic non-decreasing over time
  (reframe of the old "read consistency" TC-403).
* TC-403 — every IO-pool write captured in Phase 10 reports ``passed=True``
  (the sweep doesn't clobber concurrent writes; was TC-504).
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

        Live-traffic scenarios validate concurrent IO safety via captures on
        ``ctx`` populated by the IO-pool harness in Phase 10 and by
        ``DataIntegritySnapshotPhase`` in Phase 13; no per-scenario seeding.
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


def _validate_writes_persist_at_target(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-401 — writes persist at the target schemaVersion post-sweep.

    Data-integrity outcome reframe. Every URN that received an ``embed`` write
    across the rolling-restart + catch-up window (gap URNs from Phase 8,
    dual-write URNs from Phase 10) must end up at the migration's target
    schemaVersion in MySQL after the Phase 10 sweep completes.

    Source of truth: ``ctx.data_integrity_snapshot.embed_schema_versions``
    populated by ``DataIntegritySnapshotPhase`` (queries
    ``metadata_aspect_v2.systemmetadata.$.schemaVersion`` for each URN).
    """
    snap = ctx.data_integrity_snapshot
    if snap is None or not snap.embed_schema_versions:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "DataIntegritySnapshot.embed_schema_versions empty — Phase 13 "
                "skipped or no gap/dual URNs in run"
            ),
        )
    target = scenario.expected_schema_version
    if target is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="Scenario has no expected_schema_version configured",
        )
    mismatches = [
        (urn, v) for urn, v in snap.embed_schema_versions.items() if v != target
    ]
    total = len(snap.embed_schema_versions)
    if mismatches:
        first_urn, first_v = mismatches[0]
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"{len(mismatches)}/{total} URNs not at target schemaVersion={target}. "
                f"First: urn={first_urn} schemaVersion={first_v}"
            ),
            failure_reason=(
                "Concurrent writes during ZDU did not converge to target "
                "schemaVersion after Phase 10 sweep"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"All {total} gap+dual URNs converged to schemaVersion={target} "
            "in MySQL post-sweep"
        ),
    )


def _validate_reads_monotonic_per_urn(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-402 — per-URN read sequence is monotonic non-decreasing.

    Read consistency reframe. Across the Phase 10 sweep window, the IO-pool
    reader workers issue thousands of reads against the seeded URNs. For each
    URN that was read more than once, the ``observed_version`` returned by GMS
    must never decrease as time progresses — a regression (e.g. v4 → v3) would
    indicate the read path served a stale snapshot after the sweep advanced
    the underlying row.

    A read-path that fails to upgrade a still-OLD MySQL row to the target
    version on read is a separate concern (a known production behavior). What
    this validator catches is the strictly-wrong case: a single URN observed
    at version N at time T1 and then at version M < N at time T2 > T1.
    """
    if not ctx.io_observations:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No io_observations recorded — Phase 10 IO-pool reader workers "
                "did not run or produced no reads"
            ),
        )

    by_urn: dict[str, list[tuple[float, int]]] = {}
    for o in ctx.io_observations:
        by_urn.setdefault(o.urn, []).append(
            (o.timestamp.timestamp(), o.observed_version)
        )

    regressions: list[tuple[str, int, int]] = []
    multi_read_urns = 0
    for urn, samples in by_urn.items():
        if len(samples) < 2:
            continue
        multi_read_urns += 1
        samples.sort(key=lambda s: s[0])
        running_max = samples[0][1]
        for _, v in samples[1:]:
            if v < running_max:
                regressions.append((urn, running_max, v))
                break
            running_max = max(running_max, v)

    if multi_read_urns == 0:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No URN was read more than once — monotonic property is vacuous"
            ),
        )

    total_obs = len(ctx.io_observations)
    if regressions:
        urn, hi, lo = regressions[0]
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"{len(regressions)} URN(s) showed version regression. "
                f"First: urn={urn} saw v={hi} then later v={lo}"
            ),
            failure_reason=(
                "Read path returned a lower schemaVersion after a higher one — "
                "stale snapshot served post-advance"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"All {multi_read_urns} multi-read URN(s) showed monotonic "
            f"non-decreasing observed_version across {total_obs} reads"
        ),
    )


def _validate_no_lost_writes(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-403 — Sweep must not clobber concurrent client writes.

    Every ``IOWriteResult`` captured in ``ctx.io_write_results`` must have
    ``passed=True``. An empty ``io_write_results`` means Phase 10 was skipped
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
                "No IO harness writes recorded — Phase 10 was skipped or"
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
    401: _validate_writes_persist_at_target,
    402: _validate_reads_monotonic_per_urn,
    403: _validate_no_lost_writes,
}
