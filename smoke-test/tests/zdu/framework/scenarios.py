"""Codified Suite A scenarios — replaces the legacy ``scenarios.csv`` + Google Sheet loader.

Each scenario is a :class:`ZDUTestScenario` instance constructed with all
metadata fields explicit. ``load_scenarios()`` returns the canonical list.

Scope: this module currently only contains Suite A. Future suites
(B–H) will append their own scenarios as they land.
"""

from __future__ import annotations

from .scenario_loader import KNOWN_FAILURES, ZDUTestScenario
from .suite import Suite


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
    """Construct a Suite A (aspect_migration) scenario with sensible defaults."""
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
        suite=Suite.A,
        expected_es_fields=expected_es_fields,
    )


# Suite D — ES Phase 2 catch-up. Most TCs depend on a real two-image rolling
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
_REDUNDANT_WITH_TC_022 = (
    "Coverage redundant with Suite A TC-022 (APP_SOURCE stamping is already "
    "asserted on the sweep path there). Standalone per-aspect inspection "
    "would require a separate systemMetadata fetch — out of scope."
)
_DEV_STACK_REQUIRES_INTERRUPT_KIT = (
    "Requires upgrade-job kill-switch + restart instrumentation — separate plan."
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
    _catchup_scenario(
        tc=301,
        name="Entity index gap catch-up",
        description=(
            "10 entities written before rolling restart land in old-only; "
            "after SystemUpdateNonBlocking, all 10 should appear in the "
            "next physical index with new mapping fields populated."
        ),
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_ROLLING_RESTART,
    ),
    _catchup_scenario(
        tc=302,
        name="Timeseries catch-up via filtered _reindex",
        description=(
            "Timeseries docs in [T0, T1] are caught up via a filtered "
            "_reindex task; assert task observed and timestamps preserved."
        ),
        expected_to_fail=True,
        skip_reason=(
            "Requires two-image rolling restart + timeseries seed harness — "
            "dev stack does not reproduce."
        ),
    ),
    _catchup_scenario(
        tc=303,
        name="Global graph index catch-up",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_ROLLING_RESTART,
    ),
    _catchup_scenario(
        tc=304,
        name="Global system metadata index catch-up",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_ROLLING_RESTART,
    ),
    _catchup_scenario(
        tc=305,
        name="T0 >= T1 no-op",
        description=(
            "Force dualWriteStartTime <= reindexStartTime; non-blocking step "
            "should skip catch-up and emit no MCLs."
        ),
    ),
    _catchup_scenario(
        tc=306,
        name="No Phase 1 result no-op",
        description=(
            "Skip Phase 1 entirely; non-blocking catch-up step should "
            "return SUCCEEDED with empty captures."
        ),
    ),
    _catchup_scenario(
        tc=307,
        name="Resume from lastUrn checkpoint",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_INTERRUPT_KIT,
    ),
    _catchup_scenario(
        tc=308,
        name="DUAL_WRITE_DISABLED set when rollback flag off",
        # Not XFAIL — there's no reason to "expect" failure. The validator
        # genuinely can't run until G20c lands. SKIP is the honest signal.
        expected_to_fail=False,
        skip_reason=_DEV_STACK_REQUIRES_REINDEX_CAPTURE,
    ),
    _catchup_scenario(
        tc=309,
        name="DUAL_WRITE_DISABLED NOT set when flag on",
        expected_to_fail=False,
        skip_reason=_DEV_STACK_REQUIRES_REINDEX_CAPTURE,
    ),
]


# Suite A — Aspect schema migration (TC-001..TC-022, plus TC-023 placeholder).
# Layout matches the historical CSV row order and field semantics 1:1.
SUITE_A_SCENARIOS: list[ZDUTestScenario] = [
    _aspect_migration(
        tc=1,
        name="Full sweep single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="sweep",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=2,
        name="Read path single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="read",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=3,
        name="Write path single hop",
        aspect_name="globalTags",
        entity_type="dataset",
        action="write",
        expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=4,
        name="Full sweep multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=5,
        name="Read path multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="read",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=6,
        name="Write path multi hop",
        aspect_name="embed",
        entity_type="dashboard",
        action="write",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=7,
        name="Mid-chain start at v2",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=2,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=8,
        name="Already at target v4",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=4,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=9,
        name="Future version v5",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=5,
        expected_schema_version=5,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=10,
        name="Null systemMetadata",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=11,
        name="Gap in mutator chain",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=2,
        expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=12,
        name="transform() returns null",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        starting_schema_version=3,
        expected_schema_version=3,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=13,
        name="Invalid URN crashes sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=14,
        name="Malformed JSON in metadata",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=15,
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
        tc=16,
        name="Re-run after SUCCEEDED",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=17,
        name="ABORTED treated as terminal",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=18,
        name="IN_PROGRESS resumes sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="lifecycle",
        expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=19,
        name="chain.disable() not wired",
        aspect_name="embed",
        entity_type="dashboard",
        action="sweep",
        expected_schema_version=None,
        category="Disable Migration After Upgrade",
    ),
    _aspect_migration(
        tc=20,
        name="Read path in-memory only",
        aspect_name="embed",
        entity_type="dashboard",
        action="read",
        starting_schema_version=1,
        expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=21,
        name="Write path persists",
        aspect_name="embed",
        entity_type="dashboard",
        action="write",
        starting_schema_version=1,
        expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=22,
        name="APP_SOURCE stamped on sweep",
        aspect_name="embed",
        entity_type="dashboard",
        action="integrity",
        expected_schema_version=None,
        category="Data Integrity Verification",
    ),
    # TC-23 placeholder — present in legacy _TC_ACTION as "rolling" but absent from CSV.
    # Keep it here so future plans (Phase 6 RollingRestartPhase) can wire it up.
    _aspect_migration(
        tc=23,
        name="Rolling Upgrade",
        aspect_name="embed",
        entity_type="dashboard",
        action="rolling",
        expected_schema_version=None,
        category="Rolling Upgrade",
    ),
]


_REQUIRES_INDICES_STATE_CAPTURE = (
    "Requires per-index indicesState capture (sourceDocCount, "
    "requiresDataBackfill, status). BuildIndicesIncrementalStep persists "
    "state in the new flat-key shape, which the framework doesn't read yet "
    "— alias_swaps_observed only proves the swap happened, not the per-index "
    "classification this scenario asserts. Separate plan."
)
_REQUIRES_NO_MAPPING_DIFF_CONDITION = (
    "Asserts the 'no reindex needed' path, which requires OLD and NEW images "
    "to share identical mappings. The G20c test bench is built around the "
    "opposite — OLD vs. NEW PDL diffs that DO need reindex — so this "
    "scenario can't fire here. Separate plan with a dedicated 'no-diff' "
    "fixture pair."
)
_REQUIRES_MULTI_INDEX_DIFF = (
    "Asserts ALL captured indices needed reindex. G19a/G19b only diff one "
    "index (dashboardindex_v2); other captured swaps are 0-doc no-ops. A "
    "broader PDL fixture set (e.g. G19c/d) would be needed to satisfy this. "
    "Separate plan."
)
_DEV_STACK_REQUIRES_FAULT_INJECTION = (
    "Requires fault injection (ES network error / doc deletion) — separate plan."
)
_DEBUG_PROFILE_COLLAPSES_MAE = (
    "Catch-up validation needs ``dual_write_start_times`` per index, which the "
    "framework captures by tailing MAE logs for ``Recorded dual-write start "
    "time`` lines. The dev compose ``debug`` profile collapses MAE into GMS, "
    "so the standalone datahub-mae-consumer-debug service doesn't exist and "
    "the tail returns empty. Separate plan (G20d) — either capture from GMS "
    "logs or run a real two-service profile."
)


def _phase1_reindex_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
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
    )


def _sweep_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    """Construct a Suite E system-level sweep scenario.

    Sweep scenarios validate AspectMigrationMutatorChain sweep outcomes via
    captures on ``ctx`` populated by Plans F-5/bootJar. Most TCs require infra
    not present on the dev stack; TC-404 is the only active validator.
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
        suite=Suite.E,
    )


SUITE_E_SCENARIOS: list[ZDUTestScenario] = [
    _sweep_scenario(
        tc=401,
        name="Sweep cursor resumability",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_INTERRUPT_KIT,
    ),
    _sweep_scenario(
        tc=402,
        name="Sweep respects batchDelayMs",
        expected_to_fail=True,
        skip_reason=(
            "Requires wall-clock instrumentation around the sweep — separate plan."
        ),
    ),
    _sweep_scenario(
        tc=403,
        name="Sweep skips already-migrated rows",
        expected_to_fail=True,
        skip_reason=(
            "Requires pre-migration setup + per-row migration metric capture"
            " — separate plan."
        ),
    ),
    _sweep_scenario(
        tc=404,
        name="Sweep with no mutators registered",
        description=(
            "When AspectMigrationMutatorChain is empty, sweep step is a no-op: "
            "0 indices captured, 0 dual-write disabled markings, "
            "sweep_total_migrated == 0."
        ),
        expected_to_fail=False,
    ),
    _sweep_scenario(
        tc=405,
        name="Sweep with feature flag off",
        # Same as TC-308/309 — blocked on G20c reindex-capture, not on a
        # missing config knob per se. SKIP is the honest status.
        expected_to_fail=False,
        skip_reason=_DEV_STACK_REQUIRES_REINDEX_CAPTURE,
    ),
    _sweep_scenario(
        tc=406,
        name="APP_SOURCE stamped on sweep writes",
        # Intentionally redundant with TC-022 — SKIP rather than XFAIL.
        expected_to_fail=False,
        skip_reason=_REDUNDANT_WITH_TC_022,
    ),
    _sweep_scenario(
        tc=407,
        name="IF_VERSION_MATCH header prevents stomping",
        expected_to_fail=True,
        skip_reason=(
            "Requires race-window line-by-line interleave proof — separate plan."
        ),
    ),
    _sweep_scenario(
        tc=408,
        name="Chain disable after sweep completes",
        expected_to_fail=True,
        skip_reason=(
            "Requires AspectMigrationMutatorChain disabled log-line capture"
            " — separate plan."
        ),
    ),
]


def _live_traffic_scenario(
    *,
    tc: int,
    name: str,
    description: str = "",
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
) -> ZDUTestScenario:
    """Construct a Suite F live-traffic scenario.

    Live-traffic scenarios validate concurrent read/write behaviour during a
    running sweep. Most TCs require sustained load generators or ingestion
    harnesses not present on the dev stack; they are flagged
    ``expected_to_fail=True`` with a ``skip_reason``. TC-504 is the active
    validator: every IO-pool write captured in ``ctx.io_write_results`` must
    have ``passed=True``.
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
        expected_schema_version=None,
        action="live_traffic",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="live_traffic",
        suite=Suite.F,
    )


SUITE_F_SCENARIOS: list[ZDUTestScenario] = [
    _live_traffic_scenario(
        tc=501,
        name="Reads return new format mid-sweep",
        expected_to_fail=True,
        skip_reason=(
            "Requires sustained 50 RPS read load generator + p99 latency capture"
            " — separate plan."
        ),
    ),
    _live_traffic_scenario(
        tc=502,
        name="Writes persist as new format mid-sweep",
        expected_to_fail=True,
        skip_reason=("Requires sustained 10 RPS write load generator — separate plan."),
    ),
    _live_traffic_scenario(
        tc=503,
        name="Read consistency across sweep boundary",
        expected_to_fail=True,
        skip_reason=(
            "Requires sequential-read instrumentation around the sweep boundary"
            " — separate plan."
        ),
    ),
    _live_traffic_scenario(
        tc=504,
        name="Sweep + concurrent writes don't lose data",
        description=(
            "Every IO-pool write captured in ctx.io_write_results must have"
            " passed=True — sweep must not clobber concurrent client writes."
        ),
        expected_to_fail=False,
    ),
    _live_traffic_scenario(
        tc=505,
        name="ES dual-write under live load",
        expected_to_fail=True,
        skip_reason=(
            "Requires ES doc-count parity check across both physical indices"
            " under load — separate plan."
        ),
    ),
    _live_traffic_scenario(
        tc=506,
        name="Read sees catch-up in progress",
        expected_to_fail=True,
        skip_reason=("Requires catch-up timing instrumentation — separate plan."),
    ),
    _live_traffic_scenario(
        tc=507,
        name="Ingestion run during ZDU",
        expected_to_fail=True,
        skip_reason=("Requires ingestion job harness (csv-enricher) — separate plan."),
    ),
]


SUITE_B_SCENARIOS: list[ZDUTestScenario] = [
    # P0a — flipped active after G20c. The G20c env-var indirection mounts the
    # NEW worktree's PDLs into the system-update container, so G19a/G19b
    # diffs vs. OLD (master HEAD) now produce a real mapping diff for
    # dashboardindex_v2. The validator asserts at least one alias swap had a
    # non-empty next_index_name (proof of a real reindex with mapping diff).
    _phase1_reindex_scenario(
        tc=101,
        name="Single-index reindex with mapping change",
    ),
    _phase1_reindex_scenario(
        tc=102,
        name="No-reindex needed",
        expected_to_fail=True,
        skip_reason=_REQUIRES_NO_MAPPING_DIFF_CONDITION,
    ),
    _phase1_reindex_scenario(
        tc=103,
        name="Settings/mappings-only update",
        expected_to_fail=True,
        skip_reason=_REQUIRES_INDICES_STATE_CAPTURE,
    ),
    _phase1_reindex_scenario(
        tc=104,
        name="Empty source index",
        expected_to_fail=True,
        skip_reason=("Requires explicit doc-drop step before upgrade — separate plan."),
    ),
    _phase1_reindex_scenario(
        tc=105,
        name="Multiple indices, all need reindex",
        expected_to_fail=True,
        skip_reason=_REQUIRES_MULTI_INDEX_DIFF,
    ),
    # P0a — flipped active after G20c. With G19a/G19b PDLs, dashboardindex_v2
    # gets a real reindex while other captured indices (schemafield, corpgroup)
    # exercise the 0-doc empty-source no-op alias swap path. The validator
    # asserts both categories appear in the same SystemUpdateBlocking run.
    _phase1_reindex_scenario(
        tc=106,
        name="Mixed reindex / mapping-only",
    ),
    _phase1_reindex_scenario(
        tc=107,
        name="Timeseries index reindex",
        expected_to_fail=True,
        skip_reason=("Requires timeseries entity seed harness — separate plan."),
    ),
    _phase1_reindex_scenario(
        tc=108,
        name="DataHubUpgradeResult state shape",
        description=(
            "Every entry in DataHubUpgradeResult.indicesState must have the "
            "required keys: nextIndexName, oldBackingIndexName, "
            "reindexStartTime, sourceDocCount, taskId, requiresDataBackfill, "
            "status."
        ),
    ),
    _phase1_reindex_scenario(
        tc=109,
        name="Re-run after COMPLETED",
        expected_to_fail=True,
        skip_reason=("Requires second blocking run — separate plan."),
    ),
    _phase1_reindex_scenario(
        tc=110,
        name="Resume after interruption",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
    _phase1_reindex_scenario(
        tc=111,
        name="Reindex failure → cleanup",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
    _phase1_reindex_scenario(
        tc=112,
        name="Doc count mismatch fails alias swap",
        expected_to_fail=True,
        skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
]


def load_scenarios() -> list[ZDUTestScenario]:
    """Return the canonical scenario list for all currently codified suites."""
    return (
        list(SUITE_A_SCENARIOS)
        + list(SUITE_B_SCENARIOS)
        + list(SUITE_D_SCENARIOS)
        + list(SUITE_E_SCENARIOS)
        + list(SUITE_F_SCENARIOS)
    )
