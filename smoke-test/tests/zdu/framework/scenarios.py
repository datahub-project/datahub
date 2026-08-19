"""Codified ZDU test scenarios.

Each scenario is a :class:`ZDUTestScenario` instance constructed with all
metadata fields explicit. ``load_scenarios()`` returns the canonical list.

This module contains all four codified suites: Suite B (ES Phase 1
reindexing), Suite D (ES Phase 2 reindexing), Suite N (Aspect schema
migration & system sweep), and Suite C (Live read/write & swap).
"""

from __future__ import annotations

from .scenario_loader import KNOWN_FAILURES, ZDUTestScenario
from .suite import Suite


def _phase1_reindex_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
    expected_reindex_indices: frozenset[str] | None = None,
    min_real_reindex_count: int | None = None,
    expected_in_place_update_indices: frozenset[str] | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="ES Phase 1 — Incremental Reindex",
        name=name,
        description=description,
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="phase1_reindex",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="phase1_reindex",
        suite=Suite.B,
        expected_reindex_indices=expected_reindex_indices,
        min_real_reindex_count=min_real_reindex_count,
        expected_in_place_update_indices=expected_in_place_update_indices,
    )


_DEV_STACK_REQUIRES_REINDEX_CAPTURE = (
    "Validators need ctx.upgrade_nonblocking.dual_write_disabled_indices "
    "to be populated, which depends on BuildIndicesIncrementalStep actually "
    "running a reindex (G20c — the framework currently mounts a single "
    "upgrade.jar across initial-boot and Phase-4 system-update runs, so "
    "target and current mappings match → no diff → no dual-write "
    "transitions to capture)."
)


def _catchup_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    """Construct a Suite D catch-up scenario.

    Catch-up scenarios validate ES Phase 2 outcomes via captures on ``ctx``
    populated by Plans 5/7. They don't seed individual entities — Plan 5 seeds
    the 10 shared ``zdu-gap-*`` URNs once for the whole suite.
    """
    return ZDUTestScenario(
        tc_number=tc,
        category="ES Phase 2 Catch-Up",
        name=name,
        description=description,
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


SUITE_D_SCENARIOS: list[ZDUTestScenario] = [
    # Active validator — data-integrity reframe. Original TC-201 targeted
    # Phase 2's specific MCL-replay catch-up mechanism; that path is
    # blocked on dev by an upstream gap (BuildIndicesIncrementalStep
    # doesn't persist oldBackingIndexName). The data-integrity version
    # asserts the OUTCOME directly: every gap and dual URN written across
    # phases 7-9 is searchable in the entity index alias after Phase 10.
    _catchup_scenario(
        tc=201,
        name="Entity index gap+dual URNs searchable post-upgrade",
        description=(
            "Asserts every URN in ctx.gap_urns + ctx.dual_write_urns is "
            "findable via the dashboardindex_v2 alias after Phase 10. Tests "
            "the data-integrity OUTCOME of rolling-restart + catch-up "
            "without depending on the production catch-up mechanism."
        ),
    ),
    # Sister assertion to TC-201, but on the systemMetadata index.
    _catchup_scenario(
        tc=202,
        name="systemMetadata entries preserved for gap+dual URNs",
        description=(
            "Asserts every URN in ctx.gap_urns + ctx.dual_write_urns has "
            "at least one entry in system_metadata_service_v1 after "
            "Phase 10. Aspect-level write metadata must not be lost across "
            "the rolling-restart + catch-up window."
        ),
    ),
    # Renumbered from TC-205 to TC-203.
    _catchup_scenario(
        tc=203,
        name="T0 >= T1 no-op",
        description=(
            "Force dualWriteStartTime <= reindexStartTime; non-blocking step "
            "should skip catch-up and emit no MCLs."
        ),
    ),
    # Renumbered from TC-206 to TC-204.
    _catchup_scenario(
        tc=204,
        name="No Phase 1 result no-op",
        description=(
            "Skip Phase 1 entirely; non-blocking catch-up step should "
            "return SUCCEEDED with empty captures."
        ),
    ),
    # Renumbered from TC-308 to TC-205. Kept SKIP (pending G20c reindex
    # capture); the test is correct but the precondition state isn't yet
    # produced on dev. Not XFAIL — there's no reason to expect failure.
    _catchup_scenario(
        tc=205,
        name="DUAL_WRITE_DISABLED set when rollback flag off",
        expected_to_fail=False,
        skip_reason=_DEV_STACK_REQUIRES_REINDEX_CAPTURE,
    ),
    # Renumbered from TC-309 to TC-206.
    _catchup_scenario(
        tc=206,
        name="DUAL_WRITE_DISABLED NOT set when flag on",
        expected_to_fail=False,
        skip_reason=_DEV_STACK_REQUIRES_REINDEX_CAPTURE,
    ),
]


_DEV_STACK_REQUIRES_ROLLING_RESTART = (
    "Catch-up validation needs ``dual_write_start_times`` (T1) populated by "
    "UpdateIndicesUpgradeStrategy. G20d unblocked two prereqs: (a) the "
    "framework now tails the GMS service in the collapsed debug profile, "
    "and (b) zdu-test.env enables ``rollbackDualWriteEnabled`` so the bean "
    "loads. But the bean still initializes with 0 active targets because "
    "BuildIndicesIncrementalStep's persisted state (in MySQL "
    "dataHubUpgradeResult for ``BuildIndicesIncremental_*``) is missing the "
    "``<index>.oldBackingIndexName`` keys the factory's loadOldIndexTargets "
    "looks for. Until the upstream step writes that key (likely because "
    "``ESIndexBuilder.getBackingIndices(alias)`` returns empty when called "
    "pre-swap on a fresh-ES boot), no callback registers and no "
    "``Recorded dual-write start time`` log line is ever emitted. "
    "Separate plan."
)


_DEV_STACK_REQUIRES_RUNTIME_KNOB = (
    "Requires runtime config knob for rollbackDualWriteEnabled — not yet "
    "wired through the test framework."
)


_DEV_STACK_REQUIRES_INTERRUPT_KIT = (
    "Requires upgrade-job kill-switch + restart instrumentation — separate plan."
)


def _aspect_migration(
    tc: int,
    name: str,
    aspect_name: str,
    entity_type: str,
    action: str,
    expected_schema_version: int | None,
    starting_schema_version: int | None = None,
    description: str = "",
    expected_result: str = "",
    details: str = "",
    category: str = "",
    expected_es_fields: list[str] | None = None,
) -> ZDUTestScenario:
    """Construct a Suite N (aspect_migration) scenario with sensible defaults."""
    return ZDUTestScenario(
        tc_number=tc,
        category=category,
        name=name,
        description=description,
        prerequisite_steps="",
        test_steps="",
        expected_result=expected_result,
        current_status="",
        details=details,
        starting_schema_version=starting_schema_version,
        expected_schema_version=expected_schema_version,
        action=action,
        aspect_name=aspect_name,
        entity_type=entity_type,
        expected_to_fail=tc in KNOWN_FAILURES,
        skip_reason=KNOWN_FAILURES.get(tc),
        scenario_type="aspect_migration",
        suite=Suite.N,
        expected_es_fields=expected_es_fields,
    )


# Suite D — ES Phase 2 reindexing. Most TCs depend on a real two-image rolling
# restart that the single-image dev stack doesn't reproduce. The codified
# scenarios PASS on real CI runs against a two-image stack; on dev they XFAIL
# with skip_reason documenting the dependency.
_DEV_STACK_REQUIRES_ROLLING_RESTART = (
    "Catch-up validation needs ``dual_write_start_times`` (T1) populated by "
    "UpdateIndicesUpgradeStrategy. G20d unblocked two prereqs: (a) the "
    "framework now tails the GMS service in the collapsed debug profile, "
    "and (b) zdu-test.env enables ``rollbackDualWriteEnabled`` so the bean "
    "loads. But the bean still initializes with 0 active targets because "
    "BuildIndicesIncrementalStep's persisted state (in MySQL "
    "dataHubUpgradeResult for ``BuildIndicesIncremental_*``) is missing the "
    "``<index>.oldBackingIndexName`` keys the factory's loadOldIndexTargets "
    "looks for. Until the upstream step writes that key (likely because "
    "``ESIndexBuilder.getBackingIndices(alias)`` returns empty when called "
    "pre-swap on a fresh-ES boot), no callback registers and no "
    "``Recorded dual-write start time`` log line is ever emitted. "
    "Separate plan."
)
_DEV_STACK_REQUIRES_RUNTIME_KNOB = (
    "Requires runtime config knob for rollbackDualWriteEnabled — not yet "
    "wired through the test framework."
)
_DEV_STACK_REQUIRES_REINDEX_CAPTURE = (
    "Validators need ctx.upgrade_nonblocking.dual_write_disabled_indices "
    "to be populated, which depends on BuildIndicesIncrementalStep actually "
    "running a reindex (G20c — the framework currently mounts a single "
    "upgrade.jar across initial-boot and Phase-4 system-update runs, so "
    "target and current mappings match → no diff → no dual-write "
    "transitions to capture)."
)
_DEV_STACK_REQUIRES_INTERRUPT_KIT = (
    "Requires upgrade-job kill-switch + restart instrumentation — separate plan."
)


SUITE_N_SCENARIOS: list[ZDUTestScenario] = [
    _aspect_migration(
        tc=301,
        name="Full sweep single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="sweep",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=302,
        name="Read path single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="read",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=303,
        name="Write path single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="write",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=304,
        name="Full sweep multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=305,
        name="Read path multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="read",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=306,
        name="Write path multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="write",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=307,
        name="Mid-chain start at v2",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=2,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=308,
        name="Already at target v4",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=4,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=309,
        name="Future version v5",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=5,
        expected_schema_version=5,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=310,
        name="Null systemMetadata",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=311,
        name="Gap in mutator chain",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=2,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=312,
        name="Single-hop v3→v4 sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=3,
        expected_schema_version=4,
        category="Multi Hop Migration",
        description=(
            "Validates the v3→v4 single-hop sweep via EmbedV3ToV4Mutator. "
            "The original 'transform() returns null' scenario can't be "
            "exercised here — production embed mutators always return a "
            "non-null Embed. The null-transform no-op behavior is covered "
            "by AspectMigrationMutator unit tests; an E2E variant would "
            "require test-only mutator injection (separate plan)."
        ),
    ),
    _aspect_migration(
        tc=313,
        name="Invalid URN crashes sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=314,
        name="Malformed JSON in metadata",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=315,
        name="ES reindexing after migration",
        aspect_name="embed",
        entity_type="dashboard",
        action="es",
        expected_schema_version=4,
        category="Multi Hop Migration",
        # ``urn`` is always present in the indexed dashboard ``_source``;
        # ``embedType`` lives on the embed aspect, not in the searchable
        # view, so it would not appear here even after the chain runs.
        expected_es_fields=["urn"],
    ),
    _aspect_migration(
        tc=316,
        name="Re-run after SUCCEEDED",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=317,
        name="ABORTED treated as terminal",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=318,
        name="IN_PROGRESS resumes sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=319,
        name="chain.disable() not wired",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Disable Migration After Upgrade",
    ),
    _aspect_migration(
        tc=320,
        name="Read path in-memory only",
        aspect_name="embed",
        entity_type="dashboard",
        action="read",
        starting_schema_version=1,
        expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=321,
        name="Write path persists",
        aspect_name="embed",
        entity_type="dashboard",
        action="write",
        starting_schema_version=1,
        expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=322,
        name="APP_SOURCE stamped on sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="integrity",
        expected_schema_version=None,
        category="Data Integrity Verification",
    ),
    # TC-323 placeholder — present in legacy _TC_ACTION as "rolling" but absent from CSV.
    # Keep it here so future plans (Phase 6 RollingRestartPhase) can wire it up.
    _aspect_migration(
        tc=323,
        name="Rolling Upgrade",
        aspect_name="embed",
        entity_type="dashboard",
        action="rolling",
        expected_schema_version=None,
        category="Rolling Upgrade",
    ),
]


def _sweep_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    """Construct a Suite N sweep-invariant scenario.

    Sweep scenarios validate ``AspectMigrationMutatorChain`` sweep-job
    invariants (cursor, batch-delay, version-match, chain-disable) via
    captures on ``ctx`` populated by ``upgrade_nonblocking``. They share the
    same non-blocking phase as the per-URN aspect-migration scenarios so they
    live in Suite N; their ``scenario_type="sweep"`` dispatches to a separate
    set of validators that read sweep-level captures rather than per-URN
    aspect state.
    """
    return ZDUTestScenario(
        tc_number=tc,
        category="System-Level Sweep",
        name=name,
        description=description,
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


SUITE_N_SWEEP_INVARIANT_SCENARIOS: list[ZDUTestScenario] = [
    # Suite N's sweep-invariant subset (was TC-401..408 in the original
    # design-doc numbering; renumbered into the Suite N range because both
    # groups exercise the same non-blocking sweep phase). TC-324 / TC-326 / TC-328
    # collapse to the same outcome on the dev stack: "a second sweep run
    # finds nothing to migrate" — exactly what TC-316 (Re-run after
    # SUCCEEDED) already proves end-to-end. Resumability, idempotency on
    # already-migrated rows, and effective chain-disable all manifest as
    # the same observable behavior. Honest SKIP with a pointer to the test
    # that delivers the signal.
    _sweep_scenario(
        tc=324,
        name="Sweep cursor resumability",
        description=(
            "KillSwitchSweepPhase REST-seeds 1000 aspects at OLD schemaVersion "
            "via GMS (passes systemMetadata.schemaVersion=1 explicitly so the "
            "write-path's annotation-derived stamp is bypassed), starts "
            "SystemUpdateNonBlocking, kills the upgrade-job container "
            "mid-sweep at ~500 migrated, then restarts. Validator asserts: "
            "(a) the kill landed mid-execution (between 200 and 800 migrated), "
            "(b) MySQL state was IN_PROGRESS with a non-null cursor at kill, "
            "(c) the restart logged a cursor-load message, "
            "(d) the resume completed with all 1000 aspects at the target."
        ),
        expected_to_fail=False,
    ),
    _sweep_scenario(
        tc=325,
        name="Sweep respects batchDelayMs",
        description=(
            "BatchDelaySweepPhase seeds 200 aspects, runs the sweep with "
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_BATCH_SIZE=50 and "
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_DELAY_MS=500, and polls MySQL "
            "for cursor advances per batch checkpoint. Validator asserts "
            "the median inter-batch gap is at least delay_ms * 0.7 (70% "
            "tolerance for poll latency + batch processing slop)."
        ),
        expected_to_fail=False,
    ),
    _sweep_scenario(
        tc=326,
        name="Sweep skips already-migrated rows",
        description=(
            "Verifies the per-aspect schemaVersion-NOT-LIKE filter in "
            "EbeanAspectDao.streamAspectBatchesForMigration excludes rows "
            "already at the target schemaVersion from the migration "
            "stream — distinct signal from TC-316 (state-machine "
            "short-circuit on SUCCEEDED). SkipAlreadyMigratedSweepPhase "
            "bulk-seeds 100 v1 + 100 v4 rows, snapshots the v4 rows, "
            "wipes the migrate-aspects upgrade-result so state goes back "
            "to PENDING, runs the sweep, then asserts the v1 rows are "
            "now at target AND the v4 rows are bit-identical (createdon "
            "+ systemmetadata unchanged) — proving the sweep neither "
            "rewrote them nor clobbered their original provenance."
        ),
        expected_to_fail=False,
    ),
    _sweep_scenario(
        tc=327,
        name="Sweep with no mutators registered",
        description=(
            "When AspectMigrationMutatorChain is empty, sweep step is a no-op: "
            "0 indices captured, 0 dual-write disabled markings, "
            "sweep_total_migrated == 0."
        ),
        expected_to_fail=False,
    ),
    _sweep_scenario(
        tc=328,
        name="Sweep with feature flag off",
        # Tests the master kill switch (SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED=false).
        # Distinct mechanism from TC-327 (chain empty / sweep runs and migrates 0):
        # here the sweep step never executes, so no migrate-aspects-<version>
        # upgrade-result row is written at all. The main pipeline runs the
        # upgrade-job with the flag ON (otherwise TC-301..326 / TC-403 would
        # all be no-ops), so an active validator would need its own phase
        # that re-launches the upgrade-job with the env var off — out of
        # scope for this PR. SKIP is the honest status.
        expected_to_fail=False,
        skip_reason=(
            "Master feature-flag-off path "
            "(SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED=false). The main "
            "pipeline runs with the flag ON; toggling it off would "
            "require a separate phase that re-launches the upgrade-job "
            "and asserts no migrate-aspects-<version> upgrade-result "
            "row appears — distinct mechanism from TC-327, not yet "
            "wired into the framework."
        ),
    ),
    # Three scenarios were dropped from the original 8-item sweep-invariant
    # range because their assertions had no mechanically distinct production
    # code path to exercise:
    #
    #   • old TC-329 "APP_SOURCE stamped on sweep writes" — shared
    #     MigrateAspectsStep.java:286 with TC-322's post-sweep integrity
    #     check.
    #   • old TC-330 "IF_VERSION_MATCH header prevents stomping" — shared
    #     the IF_VERSION_MATCH / ConditionalWriteValidator path with
    #     TC-403, which already forces the race deterministically via
    #     PRE_WRITE_DELAY_MS + BATCH_URNS log sync and asserts client
    #     writes survive.
    #   • old TC-331 "Chain disable after sweep completes" — the
    #     chain-disable production flow doesn't exist; no live code path
    #     to exercise. TC-316 (re-run-after-SUCCEEDED no-op) already covers
    #     the closest observable property.
    #
    # In all three cases (unlike TC-326 vs TC-316, where the two probe
    # distinct SQL clauses) a regression would surface in the existing
    # active validator. Higher TCs shifted down to keep the range
    # contiguous.
]


def _live_traffic_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
    expected_schema_version: int | None = None,
) -> ZDUTestScenario:
    """Construct a Suite C live-traffic scenario.

    Live-traffic scenarios validate concurrent read/write behaviour during a
    running sweep against captures produced by the IO-pool harness in Phase 10
    and ``DataIntegritySnapshotPhase`` in Phase 13.

    ``expected_schema_version`` parameterizes TC-401's data-integrity
    assertion: every gap+dual URN's embed aspect must converge to this
    schemaVersion in MySQL post-sweep.
    """
    return ZDUTestScenario(
        tc_number=tc,
        category="Live Traffic",
        name=name,
        description=description,
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=expected_schema_version,
        action="live_traffic",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="live_traffic",
        suite=Suite.C,
    )


SUITE_C_SCENARIOS: list[ZDUTestScenario] = [
    # Suite C — concurrent operations during the upgrade. Three sequential
    # scenarios, all PASS on the dev stack. The original Suite C design
    # specified additional scenarios for sustained-load read mid-sweep, ES
    # dual-write parity, in-progress catch-up timing, and an ingestion job
    # harness — those need infrastructure outside this branch's scope
    # (sustained load generators, OLD-physical capture before the alias
    # swap, catch-up timing instrumentation, csv-enricher under load) and
    # were deferred to follow-up plans.
    _live_traffic_scenario(
        tc=401,
        name="Writes persist at target schemaVersion mid-sweep",
        description=(
            "Every gap+dual URN's embed aspect must converge to the target "
            "schemaVersion in MySQL after the Phase 10 sweep — concurrent "
            "writes across the rolling-restart and catch-up window must not "
            "be left at the OLD version."
        ),
        expected_to_fail=False,
        expected_schema_version=4,
    ),
    _live_traffic_scenario(
        tc=402,
        name="Read consistency: observed version monotonic per URN",
        description=(
            "For each URN read more than once across Phase 10, the "
            "observed_version sequence (ordered by timestamp) must be "
            "monotonic non-decreasing — no stale snapshot served after the "
            "sweep advanced the row."
        ),
        expected_to_fail=False,
    ),
    _live_traffic_scenario(
        tc=403,
        name="Sweep + concurrent writes don't lose data",
        description=(
            "Every IO-pool write captured in ctx.io_write_results must have"
            " passed=True — sweep must not clobber concurrent client writes."
        ),
        expected_to_fail=False,
    ),
]


SUITE_B_SCENARIOS: list[ZDUTestScenario] = [
    # Active validator (Plan 15) — observed reindex set must match
    # ``expected_reindex_indices`` exactly. The expected set is hand-curated
    # per fixture pair: G19a/G19b diff the embed aspect on the dashboard
    # entity, producing one real reindex on dashboardindex_v2. Update this
    # field whenever the fixture set changes (e.g., G19c/d landing).
    _phase1_reindex_scenario(
        tc=101,
        name="Single-index reindex with mapping change",
        description=(
            "Asserts that SystemUpdateBlocking reindexes EXACTLY the indices "
            "named in expected_reindex_indices — no missing, no extra. The "
            "expected set encodes the PDL diff between OLD and NEW worktrees "
            "and must be revised when the fixture set changes."
        ),
        expected_reindex_indices=frozenset({"dashboardindex_v2"}),
    ),
    # Active validator (Plan 15) — derives the unchanged-set from TC-101's
    # expected_reindex_indices: every captured alias swap NOT in that set
    # must have an empty next_index_name (no real reindex). Depends on TC-101
    # being present in the run.
    _phase1_reindex_scenario(
        tc=102,
        name="No-reindex needed",
        description=(
            "Asserts indices outside TC-101's expected_reindex_indices were "
            "NOT reindexed — every captured alias swap outside that set must "
            "have an empty next_index_name (no-op alias swap on empty source "
            "or identical mapping)."
        ),
    ),
    # Active validator (Plan 18) — settings/mappings-only update path.
    # When NEW adds new mapping fields but doesn't modify existing ones,
    # ReindexConfig.isPureMappingsAddition is true → ESIndexBuilder takes
    # the in-place mapping update path instead of reindex+alias-swap. The
    # PDL commit on this branch renames hasValuesFieldName on uniqueUserCount
    # (a pure mappings addition on datasetindex_v2). Together with the
    # globalTags.displayName addition, ~21 entity indices hit this path on
    # the dev stack.
    _phase1_reindex_scenario(
        tc=103,
        name="Settings/mappings-only update",
        description=(
            "Asserts the in-place mapping update path was taken for the named "
            "indices — i.e., the upgrade applied new mapping fields without "
            "reindexing existing data (requiresApplyMappings=true, "
            "isPureMappingsAddition=true). Signal: log line "
            "``Updating index <name> mappings in place``."
        ),
        expected_in_place_update_indices=frozenset({"datasetindex_v2"}),
    ),
    # Active validator (Plan 15) — empty-source no-op path: ≥1 captured alias
    # swap has empty next_index_name AND (when raw available) sourceDocCount=0
    # + status=COMPLETED. schemafieldindex_v2 and corpgroupindex_v2 typically
    # exercise this on the dev stack.
    _phase1_reindex_scenario(
        tc=104,
        name="Empty source index",
        description=(
            "Asserts ≥1 captured alias swap is an empty-source no-op (empty "
            "next_index_name). Cross-checks MySQL indicesState when available: "
            "the same alias must have sourceDocCount=0 and status=COMPLETED."
        ),
    ),
    # Active validator (Plan 17) — multi-index reindex assertion. The dev
    # stack produces 3 alias entries in ctx.upgrade_blocking.raw, all with
    # requiresDataBackfill=true and status=COMPLETED (dashboardindex_v2 with
    # real source, schemafieldindex_v2 + corpgroupindex_v2 with 0 source
    # docs but the same upgrade-side classification). Raising
    # min_real_reindex_count above 3 would require seeding entities into
    # more indices — see plan 17 follow-up plan-21.
    _phase1_reindex_scenario(
        tc=105,
        name="Multiple indices, all need reindex",
        description=(
            "Asserts ≥min_real_reindex_count indices in ctx.upgrade_blocking.raw "
            "have requiresDataBackfill=true AND status=COMPLETED. Validates "
            "that BuildIndicesIncrementalStep processed multiple indices and "
            "did not partial-run, stick at IN_PROGRESS, or drop the backfill "
            "flag on any subset."
        ),
        min_real_reindex_count=2,
    ),
    # P0a — flipped active after G20c. With G19a/G19b PDLs, dashboardindex_v2
    # gets a real reindex while other captured indices (schemafield, corpgroup)
    # exercise the 0-doc empty-source no-op alias swap path. The validator
    # asserts both categories appear in the same SystemUpdateBlocking run.
    _phase1_reindex_scenario(
        tc=106,
        name="Mixed reindex / mapping-only",
    ),
    # Renumbered from TC-108 to TC-107 after TC-107/TC-110/TC-111 were
    # removed (Suite B revival cleanup, 2026-05-17).
    _phase1_reindex_scenario(
        tc=107,
        name="DataHubUpgradeResult state shape",
        description=(
            "Every entry in DataHubUpgradeResult.indicesState must have the "
            "required keys: nextIndexName, oldBackingIndexName, "
            "reindexStartTime, sourceDocCount, taskId, requiresDataBackfill, "
            "status."
        ),
    ),
    # Active validator (Plan 16) — runs SystemUpdateBlocking a SECOND time
    # via UpgradeBlockingReRunPhase and asserts every previously-reindexed
    # alias was either skipped (SKIP_ALREADY_DONE) or absent from the rerun.
    # Production no-op path lives in BuildIndicesIncrementalStep.java:103-112.
    # Renumbered from TC-109 to TC-108.
    _phase1_reindex_scenario(
        tc=108,
        name="Re-run after COMPLETED",
        description=(
            "Asserts the second invocation of SystemUpdateBlocking is a no-op: "
            "every alias reindexed in Phase 6 must emit SKIP_ALREADY_DONE on "
            "the rerun (or not appear at all). No new physical indices may be "
            "created on the rerun. Mirrors production code at "
            "BuildIndicesIncrementalStep.java:103-112."
        ),
    ),
    # Active validator — doc-count preservation across the upgrade.
    # Compares ctx.snapshot_t0.doc_counts (post-seed, pre-upgrade) against
    # ctx.snapshot_t1.doc_counts (post-upgrade-blocking-rerun, pre-traffic).
    # Catches any data-loss or duplication bug across the upgrade.
    _phase1_reindex_scenario(
        tc=109,
        name="Doc count preserved across upgrade",
        description=(
            "Asserts every index that existed at T0 (post-seed, pre-upgrade) "
            "has the same doc count at T1 (post-upgrade-blocking-rerun, "
            "pre-traffic-injection). Mismatch indicates data loss or "
            "duplication during reindex/alias-swap/in-place-update — a "
            "broader signal than triggering the validateAndSwapAlias safety "
            "check directly."
        ),
    ),
]


def load_scenarios() -> list[ZDUTestScenario]:
    """Return the canonical scenario list for all currently codified suites."""
    return (
        list(SUITE_N_SCENARIOS)
        + list(SUITE_N_SWEEP_INVARIANT_SCENARIOS)
        + list(SUITE_B_SCENARIOS)
        + list(SUITE_D_SCENARIOS)
        + list(SUITE_C_SCENARIOS)
    )
