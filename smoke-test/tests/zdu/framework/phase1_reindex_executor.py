"""Suite B — ES Phase 1 incremental reindex scenario executor."""

from __future__ import annotations

import logging
import re
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)

# Required per-alias keys in the parsed indicesState (from MySQL flat-dotted
# shape, see ``upgrade_blocking._parse_flat_indices_state``). Under Search V3
# an index leaves the blocking phase on one of two paths, each with its own
# keys: a populated index is copied by ``_reindex``, while an empty one is
# planned as a fill from primary storage (``BACKFILL_PENDING``) that the
# non-blocking phase completes.
_COMMON_INDICES_STATE_KEYS = (
    "status",
    "nextIndexName",
    "sourceDocCount",
    "requiresDataBackfill",
    "reindexStartTime",
)
_REINDEX_PATH_KEYS = ("taskId", "reindexCompleteTime")
_FILL_PATH_KEYS = ("backfillTargetIndex", "backfillStartTime")
_FILL_STATUSES = frozenset({"BACKFILL_PENDING", "BACKFILL_IN_PROGRESS"})
# A V2 entity index, physical or alias: ``datasetindex_v2``,
# ``datasetindex_v2_<ts>``. Non-entity indices (``graph_service_v1``,
# ``dataset_operationaspect_v1``) do not match.
_V2_ENTITY_INDEX = re.compile(r"index_v2(_|$)")


class Phase1ReindexExecutor:
    def seed(self, scenario: ZDUTestScenario) -> list[str]:
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
        if scenario.skip_reason:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result=scenario.skip_reason,
            )
        validator = _VALIDATORS.get(scenario.tc_number)
        if validator is None:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result=(
                    f"No validator registered for phase1_reindex "
                    f"TC-{scenario.tc_number}"
                ),
            )
        return validator(scenario, ctx)


def _validate_single_index_reindex(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-101 — every index that held documents at T0 is reindexed.

    Search V3 entity indices share one root mapping, so the fixture's mapping
    diff reaches every index and every index is rebuilt. A populated index
    must be copied by ``_reindex``, and a reindex task must never run on an
    empty source.

    Expected comes from the ES doc counts captured at T0; observed comes from
    the per-alias upgrade state in MySQL. An index empty at T0 may still be
    reindexed: background GMS writers (execution requests, step state) can
    add its first docs while the blocking upgrade runs. Those are reported,
    not failed.
    """
    classified = _classify_reindexes(ctx)
    if classified is None or ctx.upgrade_blocking is None:
        return _skip_no_data(scenario)
    if ctx.snapshot_t0 is None or not ctx.snapshot_t0.alias_doc_counts:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No alias doc counts captured at T0 — snapshot_t0 skipped or "
                "its alias lookup failed"
            ),
        )
    t0_counts = ctx.snapshot_t0.alias_doc_counts
    # Every alias with docs at T0, including any the upgrade never processed.
    expected = {alias for alias, count in t0_counts.items() if count > 0}
    observed = classified[0]
    missing = sorted(expected - observed)
    empty_source_copies = sorted(
        state.alias
        for state in ctx.upgrade_blocking.indices
        if state.task_id and state.source_doc_count == 0
    )
    populated_after_t0 = sorted(observed - expected)
    if not missing and not empty_source_copies:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"All {len(expected)} index(es) populated at T0 were reindexed; "
                f"populated after T0 and also reindexed: {populated_after_t0}"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"missing={missing} (had docs at T0, not reindexed), "
            f"empty_source_copies={empty_source_copies} (reindex task on 0 docs)"
        ),
        failure_reason=(
            "A populated index was not reindexed, or a reindex task ran on an "
            "empty source"
        ),
    )


def _validate_no_v2_indices(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-102 — the retired V2 entity indices are not created or rebuilt.

    Search V3 is the default with V2 off, so the upgrade must build only
    ``_v3`` entity indices. A V2 index appearing after the upgrade, or a V2
    alias in the upgrade state, means the legacy index builder ran.
    """
    if ctx.upgrade_blocking is None or ctx.snapshot_t1 is None:
        return _skip_no_data(scenario)
    if not ctx.snapshot_t1.index_names:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="No index listing captured at T1",
        )
    v2_indices = sorted(
        name for name in ctx.snapshot_t1.index_names if _V2_ENTITY_INDEX.search(name)
    )
    processed = [state.alias for state in ctx.upgrade_blocking.indices]
    non_v3 = sorted(alias for alias in processed if not alias.endswith("index_v3"))
    if not v2_indices and not non_v3:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"No V2 entity indices after upgrade; all {len(processed)} "
                f"processed aliases are _v3"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"v2_indices_present={v2_indices}, non_v3_aliases_processed={non_v3}"
        ),
        failure_reason="The upgrade created or rebuilt a retired V2 entity index",
    )


def _validate_settings_only_update(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-103 — a dynamic settings change is applied in place, with no reindex.

    The settings-only upgrade changes only ``refresh_interval``. Every index
    populated in the main upgrade must log an in-place settings update to the
    new value, ES must report that value on it, and no reindex task may run.
    """
    run = ctx.upgrade_settings_only
    classified = _classify_reindexes(ctx)
    if run is None or classified is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="upgrade_settings_only or upgrade_blocking did not run",
        )
    desired = run.desired_refresh_interval
    populated = sorted(classified[0])
    not_updated = [
        alias for alias in populated if run.settings_updated.get(alias) != desired
    ]
    es_mismatch = [
        f"{alias}={run.es_refresh_intervals.get(alias)}"
        for alias in populated
        if run.es_refresh_intervals.get(alias) != desired
    ]
    if not populated:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result="No populated index from the main upgrade to check",
            failure_reason="Nothing to observe: the main upgrade reindexed no index",
        )
    if not not_updated and not es_mismatch and not run.reindex_tasks:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"{len(populated)} populated index(es) updated in place to "
                f"refresh_interval={desired}, no reindex task"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"not_updated_in_place={not_updated}, es_mismatch={es_mismatch}, "
            f"reindex_tasks={sorted(run.reindex_tasks)}"
        ),
        failure_reason=(
            "A dynamic settings change was not applied in place, or it "
            "triggered a reindex"
        ),
    )


def _validate_empty_source_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-104 — empty indices are planned as a fill from primary storage.

    Under Search V3 an empty index that needs a rebuild is not swapped as a
    no-op. The blocking phase plans a fill into the next index and marks it
    ``BACKFILL_PENDING``; the non-blocking phase completes it. Checks, per
    empty alias in the blocking-phase state: a fill status, no copied docs,
    and a fill target equal to the next index.
    """
    classified = _classify_reindexes(ctx)
    if classified is None:
        return _skip_no_data(scenario)
    empty_aliases = sorted(classified[1])
    if not empty_aliases:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"No empty-source index observed; real={sorted(classified[0])}"
            ),
            failure_reason=(
                "Expected at least one empty index to be planned as a fill"
            ),
        )
    raw = (ctx.upgrade_blocking.raw if ctx.upgrade_blocking else None) or {}
    violations: list[str] = []
    for alias in empty_aliases:
        entry = raw.get(alias)
        if not isinstance(entry, dict):
            violations.append(f"{alias}: no indicesState entry")
            continue
        if entry.get("status") not in _FILL_STATUSES:
            violations.append(f"{alias}: status={entry.get('status')}")
        if entry.get("sourceDocCount", 0) != 0:
            violations.append(f"{alias}: sourceDocCount={entry.get('sourceDocCount')}")
        if entry.get("backfillTargetIndex") != entry.get("nextIndexName"):
            violations.append(
                f"{alias}: backfillTargetIndex={entry.get('backfillTargetIndex')} "
                f"!= nextIndexName={entry.get('nextIndexName')}"
            )
    if violations:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=f"Fill path violations: {violations}",
            failure_reason="An empty index was not planned as a fill into its next index",
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"{len(empty_aliases)} empty index(es) planned as fills: {empty_aliases}"
        ),
    )


def _validate_mixed_reindex(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-106 — both paths observed in the same run.

    Under Search V3 every index is rebuilt: populated indices are copied by
    ``_reindex`` and empty ones are planned as fills from primary storage.
    Observing both within a single SystemUpdateBlocking invocation shows the
    upgrade job classifies each index by its source and only copies data
    when there is some.
    """
    classified = _classify_reindexes(ctx)
    if classified is None:
        return _skip_no_data(scenario)
    real = sorted(classified[0])
    noop = sorted(classified[1])
    if real and noop:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(f"Mixed observed — real reindex={real}, no-op swap={noop}"),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"Not mixed — real={real}, no-op={noop}; expected both categories to appear"
        ),
        failure_reason="Expected at least one of each: real reindex + no-op swap",
    )


def _validate_multi_index_all_reindex(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-105 — multi-index reindex assertion.

    Reads ``ctx.upgrade_blocking.raw`` (per-alias state from MySQL flat-key
    capture) and asserts that at least ``scenario.min_real_reindex_count``
    aliases have ``requiresDataBackfill=true`` AND ``status=COMPLETED``.

    The "all need reindex" intent is decoupled from whether each reindex had
    a non-empty ``nextIndexName`` — a 0-doc source still goes through the
    backfill-classification path; the upgrade just doesn't copy any docs.
    Strengthening to require non-empty ``nextIndexName`` would need source
    docs seeded into more indices; tracked as a follow-up plan.
    """
    if ctx.upgrade_blocking is None or not ctx.upgrade_blocking.raw:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No per-alias capture available — upgrade_blocking.raw is "
                "empty (Phase 4 skipped or BuildIndicesIncremental_* row "
                "missing)"
            ),
        )
    min_count = scenario.min_real_reindex_count
    if min_count is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="min_real_reindex_count not configured on this scenario",
        )

    raw = ctx.upgrade_blocking.raw
    backfill_aliases = [
        alias
        for alias, entry in raw.items()
        if isinstance(entry, dict) and entry.get("requiresDataBackfill") is True
    ]
    if len(backfill_aliases) < min_count:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Only {len(backfill_aliases)} alias(es) had "
                f"requiresDataBackfill=true (need >= {min_count}): "
                f"{sorted(backfill_aliases)}"
            ),
            failure_reason=(
                "Fewer indices flagged for reindex than the scenario's "
                "min_real_reindex_count — possible partial-run or "
                "backfill-flag regression"
            ),
        )

    incomplete = {
        alias: raw[alias].get("status")
        for alias in backfill_aliases
        if raw[alias].get("status") != "COMPLETED"
    }
    if incomplete:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"These backfill-flagged aliases did not reach COMPLETED: {incomplete}"
            ),
            failure_reason=(
                "At least one backfill-flagged alias is not COMPLETED — "
                "stuck IN_PROGRESS, FAILED, or unknown status"
            ),
        )

    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"{len(backfill_aliases)} alias(es) needed reindex and all "
            f"completed: {sorted(backfill_aliases)}"
        ),
    )


def _validate_doc_count_preservation(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-109 — no index loses docs across the upgrade.

    Asserts ``t1_count >= t0_count`` for every alias in
    ``ctx.snapshot_t0.alias_doc_counts``. Counts are by alias because under
    Search V3 the T0 physical index is retired by the reindex, and counting
    it again would never see the live index. ``t1 > t0`` is allowed because the
    snapshot phases don't force ES refresh + Kafka MCL drain before
    capturing — some seed-emitted writes may land in ES *after* t0 but
    before t1, inflating t1 counts harmlessly. ``t1 < t0`` is the real
    signal: docs that existed before the upgrade are missing afterwards.

    Catches: broken safety check, partial reindex copy, alias swap to
    half-populated index, doc deletion during in-place mapping update, etc.
    Does NOT catch: phantom duplication from a bad reindex retry (would
    show as t1 > t0 which we allow). That's a separate plan if needed —
    requires deterministic pre-upgrade state via ES refresh.
    """
    if ctx.snapshot_t0 is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="snapshot_t0 not captured",
        )
    if ctx.snapshot_t1 is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="snapshot_t1 not captured",
        )
    t0 = ctx.snapshot_t0.alias_doc_counts
    t1 = ctx.snapshot_t1.alias_doc_counts
    if not t0:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result="No alias doc counts captured at T0",
        )
    losses: dict[str, tuple[int, int]] = {}
    for name, t0_count in t0.items():
        if name not in t1:
            losses[name] = (t0_count, -1)
            continue
        if t1[name] < t0_count:
            losses[name] = (t0_count, t1[name])
    if losses:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Doc count DECREASED across upgrade for {len(losses)} "
                f"index(es) (t0, t1): {dict(sorted(losses.items()))}"
            ),
            failure_reason=(
                "Upgrade lost docs — at least one index has fewer docs at t1 than at t0"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"No doc loss across upgrade: all {len(t0)} index(es) have t1 >= t0"
        ),
    )


def _validate_rerun_completed(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-108 (was TC-109) — second invocation of SystemUpdateBlocking must
    be a no-op.

    Production code in ``BuildIndicesIncrementalStep.java:103-112`` reads each
    index's prior ``Status`` from MySQL and skips re-reindex if it's
    ``COMPLETED`` or ``DUAL_WRITE_DISABLED``, emitting
    ``Index <name> already <status> in previous run, skipping`` — captured by
    the framework's log monitor as ``Phase1State.SKIP_ALREADY_DONE``.

    PASS criteria:
      * The rerun phase shelled out cleanly (``rerun_exit_code == 0``).
      * No alias in ``rerun_alias_swaps_observed`` has a non-empty
        ``next_index_name`` (no new physical index created on the rerun).
      * For every alias Phase 6 reindexed (``alias_swaps_observed`` with
        non-empty new_index_name): it either appears in
        ``skip_already_done_aliases`` OR it doesn't reappear in
        ``rerun_alias_swaps_observed`` at all (production opt-out path —
        ``ReindexConfig`` may decide the index no longer needs reindex after
        the first run flipped state in MySQL).
    """
    if ctx.upgrade_blocking_rerun is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No upgrade_blocking_rerun captured — the rerun phase was "
                "skipped or didn't run"
            ),
        )
    rerun = ctx.upgrade_blocking_rerun

    if rerun.rerun_exit_code != 0:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Rerun exited with code {rerun.rerun_exit_code} — "
                f"skip_already_done={rerun.skip_already_done_aliases}, "
                f"rerun_swaps={rerun.rerun_alias_swaps_observed}"
            ),
            failure_reason="system-update rerun did not exit cleanly",
        )

    new_real_reindexes = [a for a, n in rerun.rerun_alias_swaps_observed if n]
    if new_real_reindexes:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Rerun produced new real reindex(es): {new_real_reindexes}"
            ),
            failure_reason=(
                "Second invocation must be a no-op; production code at "
                "BuildIndicesIncrementalStep.java:103-112 should skip "
                "already-COMPLETED indices"
            ),
        )

    if ctx.upgrade_blocking is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No Phase 6 baseline available — cannot verify which aliases "
                "should have been skipped on rerun"
            ),
        )

    phase6_real_reindexes = {
        a for a, n in ctx.upgrade_blocking.alias_swaps_observed if n
    }
    skipped = set(rerun.skip_already_done_aliases)
    rerun_aliases_seen = {a for a, _ in rerun.rerun_alias_swaps_observed}

    # An alias is "fine" on rerun if EITHER it was skipped (logged
    # SKIP_ALREADY_DONE) OR it doesn't appear at all in the rerun's swap
    # capture (production opt-out path — see docstring).
    leaked = (
        phase6_real_reindexes - skipped - (phase6_real_reindexes - rerun_aliases_seen)
    )
    if leaked:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Phase 6 reindexed {sorted(phase6_real_reindexes)}, but on "
                f"rerun these were neither skipped nor opt-out: "
                f"{sorted(leaked)} "
                f"(skip_already_done={sorted(skipped)}, "
                f"rerun_swaps={rerun.rerun_alias_swaps_observed})"
            ),
            failure_reason=("Previously-reindexed alias was not skipped on rerun"),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"Rerun no-op confirmed: "
            f"phase6_reindexed={sorted(phase6_real_reindexes)}, "
            f"skipped={sorted(skipped)}, no new physical indices"
        ),
    )


def _validate_state_shape(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-107 (was TC-108) — every per-alias entry has the keys for its path.

    A reindexed entry (copied docs or a task id) needs ``taskId`` and
    ``reindexCompleteTime``; a fill entry needs its fill target and start
    time instead, since the non-blocking phase finishes it.
    """
    if ctx.upgrade_blocking is None or not ctx.upgrade_blocking.raw:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No upgrade_blocking.raw captured — Phase 4 was skipped or "
                "the BuildIndicesIncremental_* row is missing in MySQL"
            ),
        )
    missing_per_alias: dict[str, list[str]] = {}
    for alias, entry in ctx.upgrade_blocking.raw.items():
        if not isinstance(entry, dict):
            missing_per_alias[alias] = list(_COMMON_INDICES_STATE_KEYS)
            continue
        reindexed = entry.get("sourceDocCount", 0) > 0 or bool(entry.get("taskId"))
        required = _COMMON_INDICES_STATE_KEYS + (
            _REINDEX_PATH_KEYS if reindexed else _FILL_PATH_KEYS
        )
        missing = [k for k in required if k not in entry]
        if missing:
            missing_per_alias[alias] = missing
    if missing_per_alias:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"indicesState entries missing required keys: {missing_per_alias}"
            ),
            failure_reason="DataHubUpgradeResult shape incomplete",
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"All {len(ctx.upgrade_blocking.raw)} per-alias entries have the "
            f"required keys for their path"
        ),
    )


def _classify_reindexes(ctx: TestContext) -> tuple[set[str], set[str]] | None:
    """Split the processed aliases into ``(real_reindex, empty_source)``.

    "Real" means data was actually copied; "empty source" means no
    ``_reindex`` ran because the source held no documents. Under Search V3
    those empty indices are planned as fills from primary storage (TC-104).

    Classification reads ``ctx.upgrade_blocking.indices``, i.e. the
    ``DataHubUpgradeResult.indicesState`` persisted in MySQL, and keys on
    ``source_doc_count``. That is the authoritative record of what production
    did. ``task_id`` is an equivalent signal — ``BuildIndicesIncrementalStep``
    only emits it for a non-empty source — and is used as a tiebreak.

    Deliberately NOT keyed on "``next_index_name`` is non-empty". Both paths
    create a next index and swap onto it (see the "Still need to swap the alias
    so new code reads from the next index with correct mappings, even though
    both indices have 0 docs" branch in ``BuildIndicesIncrementalStep``), so a
    non-empty next index says nothing about whether data moved. That
    discriminator only ever worked because the empty-source branch used to log
    a line that omitted the next index name; once #11215 routed every branch
    through the shared ``swapAliasOrCleanUp`` helper, the generic
    ``Alias swapped: <alias> -> <next>`` line is emitted for the empty-source
    case too. The framework then records the same swap twice — once via the
    ``had 0 docs ... alias swapped`` pattern (no next index) and once via the
    generic pattern (with one) — and a next-index-based check reads the second
    entry as a real reindex.

    Returns ``None`` when Phase 4 didn't run. Falls back to the log-derived
    swaps when MySQL state is unavailable; there, an alias counts as a no-op if
    ANY captured entry for it lacks a next index name, since that entry can
    only come from the empty-source log line.
    """
    if ctx.upgrade_blocking is None:
        return None

    if ctx.upgrade_blocking.indices:
        real: set[str] = set()
        no_op: set[str] = set()
        for state in ctx.upgrade_blocking.indices:
            if state.source_doc_count > 0 or state.task_id:
                real.add(state.alias)
            else:
                no_op.add(state.alias)
        return real, no_op

    swaps = ctx.upgrade_blocking.alias_swaps_observed
    no_op = {alias for alias, next_idx in swaps if not next_idx}
    real = {alias for alias, next_idx in swaps if next_idx} - no_op
    return real, no_op


def _skip_no_data(scenario: ZDUTestScenario) -> ValidationResult:
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="SKIP",
        expected_to_fail=False,
        actual_result=(
            "No upgrade_blocking captured — Phase 4 was skipped or produced "
            "no alias-swap evidence"
        ),
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    101: _validate_single_index_reindex,
    102: _validate_no_v2_indices,
    103: _validate_settings_only_update,
    104: _validate_empty_source_noop,
    105: _validate_multi_index_all_reindex,
    106: _validate_mixed_reindex,
    107: _validate_state_shape,
    108: _validate_rerun_completed,
    # TC-109 asserts doc-count preservation across the upgrade via the
    # snapshot_t0/t1 comparison (see ``_validate_doc_count_preservation``).
    # A prior design-doc TC-112 (fault-injection on doc-count mismatch) was
    # redesigned into this outcome assertion; the fault-injection helper
    # lives on as dead code for a future plan.
    109: _validate_doc_count_preservation,
}
