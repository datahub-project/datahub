"""Suite B — ES Phase 1 incremental reindex scenario executor."""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)

# Required per-alias keys in the parsed indicesState (from MySQL flat-dotted
# shape, see ``upgrade_blocking._parse_flat_indices_state``). ``taskId`` is
# intentionally optional — ``BuildIndicesIncrementalStep`` only emits it for
# real reindexes (non-empty source). ``oldBackingIndexName`` is intentionally
# absent — never observed in real-stack output.
_REQUIRED_INDICES_STATE_KEYS = (
    "status",
    "nextIndexName",
    "sourceDocCount",
    "requiresDataBackfill",
    "reindexStartTime",
    "reindexCompleteTime",
)


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
    """TC-101 — observed reindex set must exactly match ``expected_reindex_indices``.

    Catches both missing-reindex (production regression) and unexpected-
    reindex (over-reindexing). The expected set is hand-curated in the
    scenario; the discipline of keeping it in sync with PDL diffs is enforced
    by code review on ``scenarios.py``.
    """
    swaps = _alias_swaps(ctx)
    if swaps is None:
        return _skip_no_data(scenario)
    if scenario.expected_reindex_indices is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "TC-101 has no expected_reindex_indices configured — set it "
                "in scenarios.py to enable input-driven validation"
            ),
        )
    observed = frozenset(alias for alias, new in swaps if new)
    expected = scenario.expected_reindex_indices
    missing = expected - observed
    unexpected = observed - expected
    if not missing and not unexpected:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(f"Reindex set matches: {sorted(observed)}"),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"observed={sorted(observed)}, expected={sorted(expected)}, "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        ),
        failure_reason=("Observed reindex set diverges from expected_reindex_indices"),
    )


def _validate_unchanged_indices(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-102 — every captured alias swap NOT in TC-101's expected set must
    be a no-op (empty ``next_index_name``).

    Proves the inverse of TC-101: indices whose mappings did NOT change were
    not subjected to a real reindex. Reads the expected-reindex set from
    TC-101's scenario, looked up via ``ctx.all_scenarios``.
    """
    swaps = _alias_swaps(ctx)
    if swaps is None:
        return _skip_no_data(scenario)
    expected_reindex = _lookup_expected_reindex_indices(ctx)
    if expected_reindex is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "TC-101's expected_reindex_indices not available — "
                "TC-102 depends on it (was TC-101 filtered out of the run?)"
            ),
        )
    violations = [
        (alias, new_idx)
        for alias, new_idx in swaps
        if alias not in expected_reindex and new_idx
    ]
    if not violations:
        clean = [alias for alias, new in swaps if alias not in expected_reindex]
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"All {len(clean)} non-expected alias swap(s) were no-ops: {clean}"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=f"Unexpected real reindex(es): {violations}",
        failure_reason=(
            "Indices outside TC-101's expected_reindex_indices got a "
            "non-empty next_index_name (real reindex)"
        ),
    )


def _validate_empty_source_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-104 — at least one captured alias swap is an empty-source no-op.

    Signal: empty ``next_index_name`` on the swap. When the MySQL raw payload
    is available, cross-check ``sourceDocCount=0`` and ``status=COMPLETED``
    on the same aliases.
    """
    swaps = _alias_swaps(ctx)
    if swaps is None:
        return _skip_no_data(scenario)
    no_op_swaps = [alias for alias, new in swaps if not new]
    if not no_op_swaps:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=f"No empty-source no-op swaps observed; swaps={swaps}",
            failure_reason=(
                "Expected at least one alias swap with empty next_index_name "
                "(empty-source no-op path)"
            ),
        )
    if ctx.upgrade_blocking and ctx.upgrade_blocking.raw:
        raw_violations: list[str] = []
        for alias in no_op_swaps:
            entry = ctx.upgrade_blocking.raw.get(alias)
            if not isinstance(entry, dict):
                continue
            if entry.get("sourceDocCount", 0) != 0:
                raw_violations.append(
                    f"{alias}: sourceDocCount={entry.get('sourceDocCount')}"
                )
            if entry.get("status") != "COMPLETED":
                raw_violations.append(f"{alias}: status={entry.get('status')}")
        if raw_violations:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="FAIL",
                expected_to_fail=False,
                actual_result=f"Empty-source path violations: {raw_violations}",
                failure_reason=(
                    "MySQL indicesState contradicts the empty-source no-op log"
                ),
            )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=f"{len(no_op_swaps)} empty-source no-op swap(s): {no_op_swaps}",
    )


def _validate_mixed_reindex(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-106 — both real reindex and no-op alias swaps observed in same run.

    The blocking phase iterates over all index aliases. With G19a/G19b PDLs
    only ``dashboardindex_v2`` has a mapping diff vs. master HEAD, so it
    reindexes; the other indices that get processed (schemafield, corpgroup
    in the captured run) have 0 source docs and exercise the
    "empty-source no-op alias swap" path. Observing both within a single
    SystemUpdateBlocking invocation is the "mixed" invariant: the upgrade
    job correctly classifies each index by need and skips reindex when
    appropriate.
    """
    swaps = _alias_swaps(ctx)
    if swaps is None:
        return _skip_no_data(scenario)
    real = [a for a, n in swaps if n]
    noop = [a for a, n in swaps if not n]
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


def _validate_settings_only_update(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-103 — every expected index appears in
    ``ctx.upgrade_blocking.indices_updated_in_place``.

    Asserts that production took the in-place mapping update path
    (``ESIndexBuilder.updateMappingsInPlace``) for the named indices — i.e.,
    new mapping fields were added but no full reindex was triggered
    (``ReindexConfig.isPureMappingsAddition`` is true). Cross-checks against
    ``alias_swaps_observed``: the same indices must NOT appear there with a
    non-empty new_index_name (which would indicate a real reindex instead).
    """
    if ctx.upgrade_blocking is None:
        return _skip_no_data(scenario)
    if scenario.expected_in_place_update_indices is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "expected_in_place_update_indices not configured on this scenario"
            ),
        )
    expected = scenario.expected_in_place_update_indices
    observed = frozenset(ctx.upgrade_blocking.indices_updated_in_place)
    missing = expected - observed
    if missing:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Expected in-place update missing for: {sorted(missing)} "
                f"(observed in-place updates: {sorted(observed)})"
            ),
            failure_reason=(
                "Production did not take the in-place mapping update path "
                "for the named indices — either no diff was detected, or the "
                "diff routed through the reindex+swap path instead"
            ),
        )
    # Cross-check: none of the expected in-place indices should ALSO appear
    # in alias_swaps_observed with a non-empty new_index_name. If they do,
    # production took both paths somehow — surface as a FAIL.
    swap_violations = [
        alias
        for alias, new_idx in ctx.upgrade_blocking.alias_swaps_observed
        if alias in expected and new_idx
    ]
    if swap_violations:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Indices took BOTH paths (in-place update + reindex swap): "
                f"{sorted(swap_violations)}"
            ),
            failure_reason="Expected in-place-only, but real reindex also fired",
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"{len(expected)} index(es) took the in-place mapping update path: "
            f"{sorted(expected)} (total observed: {len(observed)})"
        ),
    )


def _validate_doc_count_preservation(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-109 — no index loses docs across the upgrade.

    Asserts ``t1_count >= t0_count`` for every alias in
    ``ctx.snapshot_t0.doc_counts``. ``t1 > t0`` is allowed because the
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
    t0 = ctx.snapshot_t0.doc_counts
    t1 = ctx.snapshot_t1.doc_counts
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


def _validate_doc_count_mismatch(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-109 (was TC-112) — production's doc-count safety check must fire
    under an engineered mismatch.

    ``Tc112FaultInjectionPhase`` stages MySQL ``IN_PROGRESS`` state pointing
    at a manually-created ES index with fewer docs than the alias's source,
    then runs ``system-update``. Production enters the resume path, calls
    ``ESIndexBuilder.validateAndSwapAlias``, observes the mismatch, and
    logs ``Doc count mismatch for alias swap X -> Y``. The phase captures
    those events.

    PASS criteria:
      * ``ctx.tc112_fault_injection.mismatch_events_observed`` contains at
        least one event for ``target_alias``.

    FAIL when the events are empty — usually because production exited via
    a different failure path (e.g., polling rejected the empty taskId). The
    actual_result names the upgrade's exit code so the operator can tell
    whether the safety check ran or a different branch fired.
    """
    if ctx.tc112_fault_injection is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "Tc112FaultInjectionPhase did not run or was skipped (no "
                "real-reindex alias / source too small / no upgrade row)"
            ),
        )
    result = ctx.tc112_fault_injection
    if not result.mismatch_events_observed:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"No 'Doc count mismatch' log captured for "
                f"{result.target_alias} -> {result.bad_target_index}. "
                f"Upgrade exit_code={result.upgrade_exit_code}, "
                f"source={result.source_doc_count}, "
                f"target={result.target_doc_count}. Production likely exited "
                f"via a different failure path (e.g., empty-taskId polling "
                f"branch)."
            ),
            failure_reason=(
                "validateAndSwapAlias mismatch event not observed — the "
                "safety check did not fire as expected"
            ),
        )
    for_target = [
        (a, n) for a, n in result.mismatch_events_observed if a == result.target_alias
    ]
    if not for_target:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="FAIL",
            expected_to_fail=False,
            actual_result=(
                f"Mismatch event observed but not for target alias "
                f"{result.target_alias}: {result.mismatch_events_observed}"
            ),
            failure_reason="Wrong alias triggered the mismatch",
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="PASS",
        expected_to_fail=False,
        actual_result=(
            f"{len(for_target)} doc-count-mismatch event(s) captured for "
            f"{result.target_alias} -> {result.bad_target_index} "
            f"(source={result.source_doc_count}, target={result.target_doc_count})"
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
    """TC-107 (was TC-108) — every per-alias entry has the required keys."""
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
            missing_per_alias[alias] = list(_REQUIRED_INDICES_STATE_KEYS)
            continue
        missing = [k for k in _REQUIRED_INDICES_STATE_KEYS if k not in entry]
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
            f"All {len(ctx.upgrade_blocking.raw)} per-alias entries have all "
            f"{len(_REQUIRED_INDICES_STATE_KEYS)} required keys"
        ),
    )


def _alias_swaps(ctx: TestContext) -> list[tuple[str, str]] | None:
    """Returns ``ctx.upgrade_blocking.alias_swaps_observed`` or None when
    Phase 4 didn't run / didn't capture.
    """
    if ctx.upgrade_blocking is None:
        return None
    return list(ctx.upgrade_blocking.alias_swaps_observed)


def _lookup_expected_reindex_indices(
    ctx: TestContext,
) -> frozenset[str] | None:
    """Read TC-101's ``expected_reindex_indices`` from the runner-cached list."""
    for s in ctx.all_scenarios:
        if getattr(s, "tc_number", None) == 101:
            return getattr(s, "expected_reindex_indices", None)
    return None


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
    102: _validate_unchanged_indices,
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
