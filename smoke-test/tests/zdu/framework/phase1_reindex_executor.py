"""Suite B — ES Phase 1 incremental reindex scenario executor."""

from __future__ import annotations

import logging
from typing import Callable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario

log = logging.getLogger(__name__)

# Required keys per design doc §8.2 TC-108.
_REQUIRED_INDICES_STATE_KEYS = (
    "nextIndexName",
    "oldBackingIndexName",
    "reindexStartTime",
    "sourceDocCount",
    "taskId",
    "requiresDataBackfill",
    "status",
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
    """TC-101 — at least one index reindexed with a non-empty new index name.

    Post-G20c the dev stack runs with distinct OLD/NEW worktrees mounted, so
    PDL diffs between OLD (master HEAD) and NEW (current branch's G19a/G19b
    additions) produce a real mapping diff for the affected index. The
    upgrade phase's ``alias_swaps_observed`` records the alias → new index
    pair as the swap completes; a non-empty ``next_index_name`` is the
    invariant proof that a fresh physical index was built, populated from
    the source, and swapped in (vs. the 0-doc no-op path where source is
    empty and the swap is cosmetic).
    """
    swaps = _alias_swaps(ctx)
    if swaps is None:
        return _skip_no_data(scenario)
    real_reindexes = [(alias, new) for alias, new in swaps if new]
    if real_reindexes:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="PASS",
            expected_to_fail=False,
            actual_result=(
                f"{len(real_reindexes)} real reindex(es) observed: "
                f"{[a for a, _ in real_reindexes]}"
            ),
        )
    return ValidationResult(
        tc_number=scenario.tc_number,
        name=scenario.name,
        status="FAIL",
        expected_to_fail=False,
        actual_result=(
            f"No real reindex observed — all {len(swaps)} alias swaps "
            f"had empty next_index_name (0-doc no-op path)"
        ),
        failure_reason=(
            "Expected at least one alias swap with a non-empty next_index_name "
            "(proves a real reindex with mapping diff fired)"
        ),
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


def _alias_swaps(ctx: TestContext) -> list[tuple[str, str]] | None:
    """Helper — returns ``ctx.upgrade_blocking.alias_swaps_observed`` or None
    when Phase 4 didn't run / didn't capture.
    """
    if ctx.upgrade_blocking is None:
        return None
    return list(ctx.upgrade_blocking.alias_swaps_observed)


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


def _validate_state_shape(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-108 — every indicesState entry has the required keys."""
    if ctx.upgrade_blocking is None or ctx.upgrade_blocking.raw is None:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "No upgrade_blocking.raw captured — Phase 4 was skipped or "
                "produced no DataHubUpgradeResult"
            ),
        )
    indices_state = ctx.upgrade_blocking.raw.get("indicesState")
    if not isinstance(indices_state, dict) or not indices_state:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=scenario.name,
            status="SKIP",
            expected_to_fail=False,
            actual_result=(
                "DataHubUpgradeResult contains no indicesState — nothing to "
                "validate the shape of"
            ),
        )
    missing_per_alias: dict[str, list[str]] = {}
    for alias, entry in indices_state.items():
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
            f"All {len(indices_state)} indicesState entries have all "
            f"{len(_REQUIRED_INDICES_STATE_KEYS)} required keys"
        ),
    )


_VALIDATORS: dict[int, Callable[[ZDUTestScenario, TestContext], ValidationResult]] = {
    101: _validate_single_index_reindex,
    106: _validate_mixed_reindex,
    108: _validate_state_shape,
}
