"""Codified ZDU test scenarios.

Each scenario is a :class:`ZDUTestScenario` instance constructed with all
metadata fields explicit. ``load_scenarios()`` returns the canonical list.

This module currently contains Suite B (ES Phase 1 reindexing). Subsequent
commits append Suite D (ES Phase 2 reindexing), Suite N (Aspect schema
migration & system sweep), and Suite C (Live read/write & swap).
"""

from __future__ import annotations

from .scenario_loader import ZDUTestScenario
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
    # Catches any data-loss or duplication bug — broader than the original
    # fault-injection design (kept as dead-code _validate_doc_count_mismatch
    # for a future plan).
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
    return list(SUITE_B_SCENARIOS)
