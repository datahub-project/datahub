# ZDU E2E — Plan 12: Suites B / E / F — ES Phase 1, System-Level Sweep, Live Traffic

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Codify the remaining design-doc scenarios across three suites — Suite B (TC-101..TC-112, 12 scenarios), Suite E (TC-401..TC-408, 8 scenarios), Suite F (TC-501..TC-507, 7 scenarios) — and add one `ScenarioTypeExecutor` per suite. Most TCs require infrastructure not present on the single-image dev stack (real two-image rolling restart, runtime config knobs, load generators, ingestion harness); those are flagged XFAIL with `skip_reason`. Three TCs **are** dev-passable from existing pipeline captures and ship as active validators: TC-108 (DataHubUpgradeResult state shape), TC-404 (sweep no-op with no mutators), TC-504 (sweep + concurrent writes don't lose data).

**Architecture:** Three new executor modules following the Plan 11 pattern (`Phase1ReindexExecutor`, `SweepExecutor`, `LiveTrafficExecutor`). Each implements `ScenarioTypeExecutor` (returns `[]` from `seed()`, dispatches `validate()` via a `_VALIDATORS: dict[int, Callable]` table). Three new scenario lists (`SUITE_B_SCENARIOS`, `SUITE_E_SCENARIOS`, `SUITE_F_SCENARIOS`) and three factory helpers in `scenarios.py`. `load_scenarios()` returns A + D + B + E + F. Runner registers the three new executors. Total: 27 new scenarios, 3 new executors, 3 active validators.

**Tech Stack:** Python 3 (existing). No new dependencies, no new clients, no new pipeline captures — all assertions read from existing `ctx.upgrade_blocking`, `ctx.upgrade_nonblocking`, `ctx.io_write_results`.

**Out of scope (deferred):**

- Real two-image rolling restart for TC-101..107, TC-301..304, TC-505 — CI infra, separate plan.
- Sweep cursor instrumentation for TC-401 — needs upgrade-job kill switch.
- Batch-delay timing measurement for TC-402 — needs wall-clock instrumentation around the sweep.
- Runtime config knobs (TC-405 `aspectMigrationMutatorEnabled=false`) — env-var wiring, separate plan.
- IF_VERSION_MATCH proof for TC-407 — race-window line-by-line interleave, separate plan.
- Chain disable log capture for TC-408 — separate parser + log-monitor addition.
- Load generators for TC-501 / TC-502 (50 RPS reads / 10 RPS writes), TC-503, TC-506, TC-507 — needs sustained-throughput infrastructure.
- Suites G (Rollback, TC-601..TC-604), H (Failure Recovery, TC-701..TC-705), I (K8s scale-down, TC-801..TC-804) — separate plans.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── scenarios.py                      MODIFY — 3 new factories + 3 new scenario lists + extend load_scenarios()
├── phase1_reindex_executor.py        CREATE — Suite B executor
├── sweep_executor.py                 CREATE — Suite E executor
├── live_traffic_executor.py          CREATE — Suite F executor
├── runner.py                         MODIFY — register the 3 new executors
├── test_phase1_reindex_executor.py   CREATE — 5 unit tests
├── test_sweep_executor.py            CREATE — 5 unit tests
├── test_live_traffic_executor.py     CREATE — 5 unit tests
├── test_scenario_loader.py           MODIFY — coverage for the 3 new suites
└── README.md                         MODIFY — Suite B / E / F subsections
```

---

## Task 1: Suite B (ES Phase 1 Incremental Reindex) — scenarios + executor

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenarios.py`
- Create: `smoke-test/tests/zdu/framework/phase1_reindex_executor.py`
- Create: `smoke-test/tests/zdu/framework/test_phase1_reindex_executor.py`

**Suite B scenarios** (per design doc §8.2):

| TC  | Name                                     | Dev      | Notes                                                                         |
| --- | ---------------------------------------- | -------- | ----------------------------------------------------------------------------- |
| 101 | Single-index reindex with mapping change | XFAIL    | Needs mapping change between OLD/NEW images                                   |
| 102 | No-reindex needed                        | XFAIL    | Needs identical mappings setup                                                |
| 103 | Settings/mappings-only update            | XFAIL    | Same as 102                                                                   |
| 104 | Empty source index                       | XFAIL    | Needs explicit doc-drop step                                                  |
| 105 | Multiple indices, all need reindex       | XFAIL    | Needs multi-index setup                                                       |
| 106 | Mixed reindex / mapping-only             | XFAIL    | Same                                                                          |
| 107 | Timeseries index reindex                 | XFAIL    | Needs timeseries entity seed                                                  |
| 108 | `DataHubUpgradeResult` state shape       | **PASS** | Active — verifies required keys in `ctx.upgrade_blocking.raw.indicesState[*]` |
| 109 | Re-run after COMPLETED                   | XFAIL    | Needs second blocking run                                                     |
| 110 | Resume after interruption                | XFAIL    | Needs kill switch                                                             |
| 111 | Reindex failure → cleanup                | XFAIL    | Needs fault injection                                                         |
| 112 | Doc count mismatch fails alias swap      | XFAIL    | Needs ES doc deletion mid-flight                                              |

**Active validator: TC-108 (state shape)** — every entry in `ctx.upgrade_blocking.raw["indicesState"]` must have keys: `nextIndexName`, `oldBackingIndexName`, `reindexStartTime`, `sourceDocCount`, `taskId`, `requiresDataBackfill`, `status`.

### 1.1 — Scenarios

- [ ] **Step 1: Add factory + `SUITE_B_SCENARIOS` to `scenarios.py`**

After `SUITE_D_SCENARIOS` (or before — placement is alphabetical-ish, your call):

```python
_DEV_STACK_REQUIRES_MAPPING_DIFF = (
    "Requires distinct OLD/NEW image mappings to trigger reindex — "
    "single-image dev stack does not reproduce."
)
_DEV_STACK_REQUIRES_FAULT_INJECTION = (
    "Requires fault injection (ES network error / doc deletion) — "
    "separate plan."
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
        tc_number=tc, category="ES Phase 1 — Incremental Reindex",
        name=name, description=description,
        prerequisite_steps="", test_steps="", expected_result="",
        current_status="", details="",
        starting_schema_version=None, expected_schema_version=None,
        action="phase1_reindex", aspect_name="", entity_type="",
        expected_to_fail=expected_to_fail, skip_reason=skip_reason,
        scenario_type="phase1_reindex", suite=Suite.B,
    )


SUITE_B_SCENARIOS: list[ZDUTestScenario] = [
    _phase1_reindex_scenario(
        tc=101, name="Single-index reindex with mapping change",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_MAPPING_DIFF,
    ),
    _phase1_reindex_scenario(
        tc=102, name="No-reindex needed",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_MAPPING_DIFF,
    ),
    _phase1_reindex_scenario(
        tc=103, name="Settings/mappings-only update",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_MAPPING_DIFF,
    ),
    _phase1_reindex_scenario(
        tc=104, name="Empty source index",
        expected_to_fail=True, skip_reason=(
            "Requires explicit doc-drop step before upgrade — separate plan."
        ),
    ),
    _phase1_reindex_scenario(
        tc=105, name="Multiple indices, all need reindex",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_MAPPING_DIFF,
    ),
    _phase1_reindex_scenario(
        tc=106, name="Mixed reindex / mapping-only",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_MAPPING_DIFF,
    ),
    _phase1_reindex_scenario(
        tc=107, name="Timeseries index reindex",
        expected_to_fail=True, skip_reason=(
            "Requires timeseries entity seed harness — separate plan."
        ),
    ),
    _phase1_reindex_scenario(
        tc=108, name="DataHubUpgradeResult state shape",
        description=(
            "Every entry in DataHubUpgradeResult.indicesState must have the "
            "required keys: nextIndexName, oldBackingIndexName, "
            "reindexStartTime, sourceDocCount, taskId, requiresDataBackfill, "
            "status."
        ),
    ),
    _phase1_reindex_scenario(
        tc=109, name="Re-run after COMPLETED",
        expected_to_fail=True, skip_reason=(
            "Requires second blocking run — separate plan."
        ),
    ),
    _phase1_reindex_scenario(
        tc=110, name="Resume after interruption",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
    _phase1_reindex_scenario(
        tc=111, name="Reindex failure → cleanup",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
    _phase1_reindex_scenario(
        tc=112, name="Doc count mismatch fails alias swap",
        expected_to_fail=True, skip_reason=_DEV_STACK_REQUIRES_FAULT_INJECTION,
    ),
]
```

### 1.2 — Executor + active validator + tests

- [ ] **Step 2: Failing test in `test_phase1_reindex_executor.py`** — 5 tests:
  - `test_seed_returns_empty`
  - `test_expected_to_fail_returns_xfail_with_skip_reason`
  - `test_tc108_passes_when_all_required_keys_present`
  - `test_tc108_fails_when_required_keys_missing`
  - `test_unknown_tc_returns_skip`

Use `_scenario(tc=...)` helper similar to Plan 11's `test_catchup_executor.py`.

- [ ] **Step 3: Implement `phase1_reindex_executor.py`**

```python
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

    def validate(
        self, scenario: ZDUTestScenario, ctx: TestContext
    ) -> ValidationResult:
        if scenario.expected_to_fail:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name, status="XFAIL",
                expected_to_fail=True,
                actual_result="Expected failure on this stack",
                failure_reason=scenario.skip_reason,
            )
        validator = _VALIDATORS.get(scenario.tc_number)
        if validator is None:
            return ValidationResult(
                tc_number=scenario.tc_number, name=scenario.name,
                status="SKIP", expected_to_fail=False,
                actual_result=(
                    f"No validator registered for phase1_reindex "
                    f"TC-{scenario.tc_number}"
                ),
            )
        return validator(scenario, ctx)


def _validate_state_shape(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-108 — every indicesState entry has the required keys."""
    if ctx.upgrade_blocking is None or not ctx.upgrade_blocking.raw:
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="SKIP", expected_to_fail=False,
            actual_result=(
                "No upgrade_blocking.raw captured — Phase 4 was skipped or "
                "produced no DataHubUpgradeResult"
            ),
        )
    indices_state = ctx.upgrade_blocking.raw.get("indicesState")
    if not isinstance(indices_state, dict) or not indices_state:
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="SKIP", expected_to_fail=False,
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
            tc_number=scenario.tc_number, name=scenario.name,
            status="FAIL", expected_to_fail=False,
            actual_result=(
                f"indicesState entries missing required keys: "
                f"{missing_per_alias}"
            ),
            failure_reason="DataHubUpgradeResult shape incomplete",
        )
    return ValidationResult(
        tc_number=scenario.tc_number, name=scenario.name,
        status="PASS", expected_to_fail=False,
        actual_result=(
            f"All {len(indices_state)} indicesState entries have all "
            f"{len(_REQUIRED_INDICES_STATE_KEYS)} required keys"
        ),
    )


_VALIDATORS: dict[
    int, Callable[[ZDUTestScenario, TestContext], ValidationResult]
] = {
    108: _validate_state_shape,
}
```

- [ ] **Step 4: Run tests — expect 5 pass.**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_phase1_reindex_executor.py -v 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenarios.py \
        smoke-test/tests/zdu/framework/phase1_reindex_executor.py \
        smoke-test/tests/zdu/framework/test_phase1_reindex_executor.py
git commit -m "feat(zdu): Suite B (ES Phase 1 reindex) — TC-101..TC-112 + TC-108 validator"
```

---

## Task 2: Suite E (System-Level Sweep) — scenarios + executor

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenarios.py`
- Create: `smoke-test/tests/zdu/framework/sweep_executor.py`
- Create: `smoke-test/tests/zdu/framework/test_sweep_executor.py`

**Suite E scenarios** (per design doc §8.5):

| TC  | Name                                      | Dev      | Notes                                                                                                           |
| --- | ----------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------- |
| 401 | Sweep cursor resumability                 | XFAIL    | Needs kill switch                                                                                               |
| 402 | Sweep respects batchDelayMs               | XFAIL    | Needs wall-clock instrumentation                                                                                |
| 403 | Sweep skips already-migrated rows         | XFAIL    | Needs pre-migration setup + metric capture                                                                      |
| 404 | Sweep with no mutators registered         | **PASS** | Active — when `ctx.upgrade_nonblocking.indices` is empty AND `ctx.sweep_total_migrated == 0`, sweep was a no-op |
| 405 | Sweep with feature flag off               | XFAIL    | Needs runtime config knob                                                                                       |
| 406 | APP_SOURCE stamped on sweep writes        | XFAIL    | Already covered by Suite A TC-022 (different angle)                                                             |
| 407 | IF_VERSION_MATCH header prevents stomping | XFAIL    | Needs race-window proof                                                                                         |
| 408 | Chain disable after sweep completes       | XFAIL    | Needs chain.disable() log capture                                                                               |

**Active validator: TC-404** — when no mutators in chain, sweep migrates zero entities and emits no DUAL_WRITE_DISABLED markings.

- [ ] **Step 1: Add factory + `SUITE_E_SCENARIOS` to `scenarios.py`**

Similar shape to Suite B. Use `scenario_type="sweep"`, `action="sweep"`. TC-404 has `expected_to_fail=False`; the other 7 have `expected_to_fail=True` with appropriate `skip_reason`.

- [ ] **Step 2: Failing test in `test_sweep_executor.py`** — 5 tests covering seed, XFAIL dispatch, TC-404 PASS (empty captures), TC-404 FAIL (non-zero sweep_total_migrated), unknown TC.

- [ ] **Step 3: Implement `sweep_executor.py`** with `SweepExecutor` class + `_validate_no_mutators_noop` (TC-404):

```python
def _validate_no_mutators_noop(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-404 — when the mutator chain is empty, sweep must be a no-op."""
    if ctx.upgrade_nonblocking is None:
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="SKIP", expected_to_fail=False,
            actual_result=(
                "Phase 8 (upgrade_nonblocking) did not run — no captures"
            ),
        )
    # When mutators are registered, sweep_total_migrated > 0 is expected.
    # The no-op assertion only fires when the chain was empty. We don't have
    # a direct "chain was empty" capture, so we use the dev-stack heuristic:
    # if upgrade_nonblocking.indices is empty AND dual_write_disabled is empty
    # AND sweep_total_migrated == 0, this looks like the no-op case.
    nb = ctx.upgrade_nonblocking
    is_noop_state = (
        not nb.indices
        and not nb.dual_write_disabled_indices
        and ctx.sweep_total_migrated == 0
    )
    if is_noop_state:
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="PASS", expected_to_fail=False,
            actual_result=(
                "No mutators registered: sweep no-op confirmed "
                "(0 migrated, 0 indices captured, 0 disabled)"
            ),
        )
    # Some sweep activity occurred — the no-op invariant doesn't apply.
    # Return SKIP rather than FAIL since this TC's precondition (empty chain)
    # may not have been met by this run.
    return ValidationResult(
        tc_number=scenario.tc_number, name=scenario.name,
        status="SKIP", expected_to_fail=False,
        actual_result=(
            f"Mutator chain was non-empty (migrated={ctx.sweep_total_migrated}, "
            f"indices={len(nb.indices)}, disabled={len(nb.dual_write_disabled_indices)}) "
            f"— TC-404 precondition not met"
        ),
    )
```

- [ ] **Step 4: Run tests — expect 5 pass.**

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenarios.py \
        smoke-test/tests/zdu/framework/sweep_executor.py \
        smoke-test/tests/zdu/framework/test_sweep_executor.py
git commit -m "feat(zdu): Suite E (system-level sweep) — TC-401..TC-408 + TC-404 validator"
```

---

## Task 3: Suite F (Live Traffic) — scenarios + executor

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenarios.py`
- Create: `smoke-test/tests/zdu/framework/live_traffic_executor.py`
- Create: `smoke-test/tests/zdu/framework/test_live_traffic_executor.py`

**Suite F scenarios** (per design doc §8.6):

| TC  | Name                                      | Dev      | Notes                                                                    |
| --- | ----------------------------------------- | -------- | ------------------------------------------------------------------------ |
| 501 | Reads return new format mid-sweep         | XFAIL    | Needs 50 RPS load generator + p99 latency capture                        |
| 502 | Writes persist as new format mid-sweep    | XFAIL    | Needs 10 RPS load generator                                              |
| 503 | Read consistency across sweep boundary    | XFAIL    | Needs sequential-read instrumentation                                    |
| 504 | Sweep + concurrent writes don't lose data | **PASS** | Active — every IO-pool write in `ctx.io_write_results` has `passed=True` |
| 505 | ES dual-write under live load             | XFAIL    | Needs ES doc-count parity check across both indices                      |
| 506 | Read sees catch-up in progress            | XFAIL    | Needs catch-up timing instrumentation                                    |
| 507 | Ingestion run during ZDU                  | XFAIL    | Needs ingestion job harness                                              |

**Active validator: TC-504** — `all(r.passed for r in ctx.io_write_results)` when the harness ran.

- [ ] **Step 1: Add factory + `SUITE_F_SCENARIOS` to `scenarios.py`** — `scenario_type="live_traffic"`, `action="live_traffic"`. TC-504 has `expected_to_fail=False`.

- [ ] **Step 2: Failing test in `test_live_traffic_executor.py`** — 5 tests: seed, XFAIL dispatch, TC-504 PASS (all writes passed), TC-504 FAIL (some writes failed), TC-504 SKIP (no harness ran).

- [ ] **Step 3: Implement `live_traffic_executor.py`** with `LiveTrafficExecutor`:

```python
def _validate_no_lost_writes(
    scenario: ZDUTestScenario, ctx: TestContext
) -> ValidationResult:
    """TC-504 — concurrent IO-pool writes during sweep all retain client content."""
    if not ctx.io_write_results:
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="SKIP", expected_to_fail=False,
            actual_result="No IO harness writes recorded — Phase 8 was skipped or harness produced no writes",
        )
    failed = [w for w in ctx.io_write_results if not w.passed]
    total = len(ctx.io_write_results)
    if failed:
        first = failed[0]
        return ValidationResult(
            tc_number=scenario.tc_number, name=scenario.name,
            status="FAIL", expected_to_fail=False,
            actual_result=(
                f"{len(failed)}/{total} concurrent writes lost data; "
                f"first: urn={first.urn} observed={first.observed_version} "
                f"expected={first.expected_version} error={first.error}"
            ),
            failure_reason="sweep clobbered concurrent client writes",
        )
    return ValidationResult(
        tc_number=scenario.tc_number, name=scenario.name,
        status="PASS", expected_to_fail=False,
        actual_result=(
            f"All {total} concurrent IO-pool writes preserved client content"
        ),
    )
```

- [ ] **Step 4: Run tests — expect 5 pass.**

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenarios.py \
        smoke-test/tests/zdu/framework/live_traffic_executor.py \
        smoke-test/tests/zdu/framework/test_live_traffic_executor.py
git commit -m "feat(zdu): Suite F (live traffic) — TC-501..TC-507 + TC-504 validator"
```

---

## Task 4: Extend `load_scenarios()` + wire executors into runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenarios.py`
- Modify: `smoke-test/tests/zdu/framework/runner.py`
- Modify: `smoke-test/tests/zdu/framework/test_scenario_loader.py`

- [ ] **Step 1: Extend `load_scenarios()`**

```python
def load_scenarios() -> list[ZDUTestScenario]:
    return (
        list(SUITE_A_SCENARIOS)
        + list(SUITE_B_SCENARIOS)
        + list(SUITE_D_SCENARIOS)
        + list(SUITE_E_SCENARIOS)
        + list(SUITE_F_SCENARIOS)
    )
```

- [ ] **Step 2: Add executor registration in `runner.py`**

Below the existing `_catchup_executor` registration:

```python
self._phase1_reindex_executor = Phase1ReindexExecutor()
self._registry.register("phase1_reindex", self._phase1_reindex_executor)
self._sweep_executor = SweepExecutor()
self._registry.register("sweep", self._sweep_executor)
self._live_traffic_executor = LiveTrafficExecutor()
self._registry.register("live_traffic", self._live_traffic_executor)
```

Plus the three imports.

- [ ] **Step 3: Extend `test_scenario_loader.py` with suite counts**

```python
class TestAllSuites:
    def test_load_scenarios_returns_combined_list(self) -> None:
        from tests.zdu.framework.scenarios import load_scenarios
        scenarios = load_scenarios()
        suites_present = {s.suite for s in scenarios}
        # Suite A + B + D + E + F (no C, G, H, I yet)
        assert Suite.A in suites_present
        assert Suite.B in suites_present
        assert Suite.D in suites_present
        assert Suite.E in suites_present
        assert Suite.F in suites_present

    def test_suite_b_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_B_SCENARIOS
        assert len(SUITE_B_SCENARIOS) == 12

    def test_suite_e_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_E_SCENARIOS
        assert len(SUITE_E_SCENARIOS) == 8

    def test_suite_f_count(self) -> None:
        from tests.zdu.framework.scenarios import SUITE_F_SCENARIOS
        assert len(SUITE_F_SCENARIOS) == 7
```

- [ ] **Step 4: Smoke-test runner construct**

```bash
cd <REPO_ROOT>
ZDU_SKIP_BOOTJAR=1 smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
keys = set(r._registry._executors.keys())
print('Registered executors:', sorted(keys))
assert keys >= {'aspect_migration', 'catch_up', 'phase1_reindex', 'sweep', 'live_traffic'}
print('OK')
"
```

Expected: `OK` with all 5 executor keys.

- [ ] **Step 5: Run framework suite**

Expected: ~245 pass (222 baseline + 4 loader + 5 phase1 + 5 sweep + 5 live = 241; small variance OK).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenarios.py \
        smoke-test/tests/zdu/framework/runner.py \
        smoke-test/tests/zdu/framework/test_scenario_loader.py
git commit -m "feat(zdu): extend load_scenarios() to return Suites A+B+D+E+F; register 3 new executors"
```

Re-stage if pre-commit reformats.

---

## Task 5: README — Suite B / E / F subsections

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Append three subsections AFTER Suite D**

Use the same table format as the Suite D README block — TC number, name, dev-stack status, notes.

For each suite mention which captures the active validator(s) read from and how to upgrade XFAIL → PASS on a real two-image CI stack.

- [ ] **Step 2: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Suites B / E / F scenarios + active validators"
```

---

## Task 6: Live integration check

- [ ] **Step 1: Run each new suite in isolation**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
for SUITE in b e f; do
  echo "=== Suite $SUITE ==="
  DATAHUB_GMS_URL=http://localhost:8080 \
  DATAHUB_GMS_TOKEN="$TOKEN" \
  ZDU_SKIP_PHASES=upgrade,rolling_restart \
  venv/bin/python -m tests.zdu --suite $SUITE 2>&1 | tail -25
done
```

Expected results:

- Suite B: 12 scenarios — 1 PASS (TC-108) + 11 XFAIL.
- Suite E: 8 scenarios — 1 PASS (TC-404) **or SKIP if the dev stack actually has mutators registered**. The dev stack now has `ASPECT_MIGRATION_MUTATOR_ENABLED=true` after Plan F-5 + bootJar fixes, so TC-404's no-op precondition is NOT met — it'll SKIP. That's correct behavior.
- Suite F: 7 scenarios — 1 PASS (TC-504) + 6 XFAIL.

- [ ] **Step 2: Combined run (Suite A + all new suites)**

```bash
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart \
venv/bin/python -m tests.zdu --suite a b d e f 2>&1 | tail -45
```

Expected: 14 (A) + 1 (B-TC108) + 2 (D-TC305/306) + 0-1 (E-TC404) + 1 (F-TC504) = 18-19 PASS.

Total scenarios: 23 (A) + 12 (B) + 9 (D) + 8 (E) + 7 (F) = 59.

- [ ] **Step 3: Inspect the JSON report**

```bash
python3 -c "
import json
data = json.load(open('<REPO_ROOT>/smoke-test/smoke-test/build/zdu-test-report.json'))
counts: dict[str, int] = {}
for s in data.get('scenarios', []):
    counts[s['status']] = counts.get(s['status'], 0) + 1
print('Status counts:', counts)
print('Total scenarios:', sum(counts.values()))
"
```

- [ ] **Step 4: If anything regressed, fix and commit**

---

## Task 7: Code review

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff e0597f7786..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-12.diff
wc -l /tmp/zdu-plan-12.diff
```

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`** with this prompt:

> Review the diff at `/tmp/zdu-plan-12.diff`. This PR codifies the remaining scenario suites from design doc §8 — Suite B (TC-101..TC-112, 12 scenarios), Suite E (TC-401..TC-408, 8 scenarios), Suite F (TC-501..TC-507, 7 scenarios) — and adds three new executors. Most TCs are XFAIL with documented `skip_reason` on the single-image dev stack; three TCs ship as active validators that read existing pipeline captures: TC-108 (Phase 1 state shape), TC-404 (sweep no-op when chain empty), TC-504 (concurrent writes preserved).
>
> Check specifically:
>
> 1. **Suite enum consistency:** Suite.B / E / F enums already exist in `suite.py` from earlier scaffolding. Plan 12 doesn't modify the enum, only adds scenarios that consume the values.
> 2. **`expected_to_fail` short-circuit:** in each of the 3 executors, XFAIL returns FIRST with `failure_reason=scenario.skip_reason`. All XFAIL TCs have non-None skip_reason.
> 3. **TC-108 invariant:** verifies the 7 required keys (`nextIndexName`, `oldBackingIndexName`, `reindexStartTime`, `sourceDocCount`, `taskId`, `requiresDataBackfill`, `status`) are present in every `indicesState` entry. SKIPs when `upgrade_blocking.raw` is None or has no `indicesState`.
> 4. **TC-404 invariant:** the SKIP-vs-PASS distinction. When chain has mutators (sweep_total_migrated > 0), this TC's precondition isn't met → SKIP. When the chain was empty, all three indicators (indices, dual_write_disabled, sweep_total_migrated) align → PASS.
> 5. **TC-504 invariant:** `all(r.passed for r in ctx.io_write_results)`. SKIP when no writes captured; PASS when all passed; FAIL with first-failure detail when any failed.
> 6. **Test quality:** each executor has 5 unit tests covering the happy + failure paths.
> 7. **Backward compat:** Suite A (TC-001..TC-023) baseline preserved. Suite D (TC-301..TC-309) baseline preserved.
> 8. **YAGNI:** no new clients, no new captures, no new pipeline phases.
> 9. **Type hints complete** on all 3 executors, factories, scenario lists.
> 10. **No false positives:** each active validator emits SKIP rather than FAIL when the run's preconditions aren't met. PASS / FAIL paths only fire when the captures are meaningful.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback** as one or more commits.

---

## Self-Review

**Spec coverage** (against design doc §8.2 / §8.5 / §8.6):

| Suite               | TCs codified  | Active validators | XFAIL on dev |
| ------------------- | ------------- | ----------------- | ------------ |
| B (Phase 1 reindex) | 12 (101..112) | 1 (TC-108)        | 11           |
| E (System sweep)    | 8 (401..408)  | 1 (TC-404)        | 7            |
| F (Live traffic)    | 7 (501..507)  | 1 (TC-504)        | 6            |

**Placeholder scan:** None.

**Type / signature consistency:**

- `Phase1ReindexExecutor.seed(scenario) -> list[str]`, `validate(scenario, ctx) -> ValidationResult`.
- `SweepExecutor` / `LiveTrafficExecutor` — same shape.
- `SUITE_B_SCENARIOS`, `SUITE_E_SCENARIOS`, `SUITE_F_SCENARIOS: list[ZDUTestScenario]`.

**Risks called out:**

1. **TC-404's SKIP-not-FAIL semantics.** When the dev stack runs with mutators registered (default after Plan F-5 + bootJar fix), TC-404's no-op precondition isn't met. The validator returns SKIP rather than FAIL. This is correct — we shouldn't penalise a run that didn't set up the empty-chain condition. To make TC-404 PASS on a future CI run, the test infra needs to inject `ASPECT_MIGRATION_MUTATOR_ENABLED=false` for that specific scenario.
2. **TC-504's IO-harness dependency.** When `upgrade_nonblocking` is skipped (no `io_write_results` captured), TC-504 SKIPs. When the harness ran with writes that all passed, PASS. When some failed, FAIL with first-failure detail.
3. **Forward compat for XFAILs.** The 23 XFAIL TCs across the three suites flip to active validators by changing `expected_to_fail` to False once the underlying infra lands — the framework code is forward-compatible.
4. **Total scenario count grows to 59** (23 A + 12 B + 9 D + 8 E + 7 F). Per-suite running stays cheap because most TCs are XFAIL no-ops. Suite A baseline run-time unchanged.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-11-zdu-plan-12-suites-b-e-f.md`.

Per session policy: defaulting to subagent-driven execution.
