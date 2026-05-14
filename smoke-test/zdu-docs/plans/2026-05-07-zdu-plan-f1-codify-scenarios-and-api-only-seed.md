# ZDU E2E — Plan F-1: Codify Scenarios + API-Only Seeding

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CSV/Google-Sheet scenario source with a Python module of `ZDUTestScenario` objects, and stop direct-MySQL seeding — all seed writes go through `DataHubClient.ingest_mcp` against the running GMS. Closes Notion review threads D1, D2, and D3.

**Architecture:** Move scenario data from `scenarios.csv` (+ CSV/Sheet loader) into a single Python module `framework/scenarios.py`. Each scenario is a `ZDUTestScenario` instance constructed with all metadata (TC#, suite, aspect, expected version, expected_to_fail, skip_reason). `ScenarioLoader` becomes a thin `load_scenarios()` function that returns the list. `phases/seed.py` drops `_seed_direct_mysql`, `_DIRECT_DB_SEED_TCS`, and the direct-MySQL IO-pool block; all seeds (including IO pool) go through the API. The running GMS keeps `ASPECT_MIGRATION_MUTATOR_ENABLED=false` at seed time (existing default), so write-path mutators don't fire and entities land at the OLD GMS's natural state — same outcome as the previous direct-DB inserts, without bypassing the API.

**Tech Stack:** Python 3, pytest, `requests` (existing). No new dependencies.

**Out of scope (deferred):**

- Per-scenario function references (`seed`/`action`/`expected` callables) — Govind committed to this in the D3 reply, but it's a bigger refactor. F-1 keeps the action-string dispatch in `ScenarioExecutor` to preserve behavior bit-for-bit. Function-ref redesign is a follow-up if/when scenarios need divergent validation logic.
- Two-image-tag architecture (D4-related, Notion Phase 6 body change) — Plan F-3.
- Test-only migrators replacing env-var hooks (D5) — separate plan, has Java implications.
- TC-019 reclassification under "chain stays enabled as no-op" — Plan F-4 (covers D4 broader scope).

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/
├── scenarios.csv                           DELETE
├── framework/
│   ├── scenarios.py                        CREATE — list[ZDUTestScenario] for Suite A
│   ├── scenario_loader.py                  REWRITE — strip CSV/Sheet code; keep dataclass + executor + helpers
│   ├── test_scenario_loader.py             REWRITE — load tests target the new Python module
│   └── phases/
│       └── seed.py                         MODIFY — drop _seed_direct_mysql + IO-pool mysql_upsert; all seeds via API
```

---

## Task 1: Create `framework/scenarios.py` with codified scenarios

**Files:**

- Create: `smoke-test/tests/zdu/framework/scenarios.py`

**Pattern:** A single module-level `SUITE_A_SCENARIOS: list[ZDUTestScenario] = [...]` plus a `load_scenarios()` function. Each entry is one constructor call with all fields explicit. The data is identical to what the four `_TC_*` dicts + CSV currently produce; this task is a 1:1 migration with no behavior change.

- [ ] **Step 1: Read the current dataclass and the CSV**

```bash
cd <REPO_ROOT>
head -1 smoke-test/tests/zdu/scenarios.csv
wc -l smoke-test/tests/zdu/scenarios.csv
```

The CSV has 22 data rows (TC-001..TC-022; TC-023 is in `_TC_ACTION` but absent from the CSV — that's existing behavior, preserve it).

- [ ] **Step 2: Create the scenarios module**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/scenarios.py`:

```python
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
    )


# Suite A — Aspect schema migration (TC-001..TC-022, plus TC-023 placeholder).
# Layout matches the historical CSV row order and field semantics 1:1.
SUITE_A_SCENARIOS: list[ZDUTestScenario] = [
    _aspect_migration(
        tc=1, name="Full sweep single hop",
        aspect_name="globalTags", entity_type="dataset",
        action="sweep", expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=2, name="Read path single hop",
        aspect_name="globalTags", entity_type="dataset",
        action="read", expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=3, name="Write path single hop",
        aspect_name="globalTags", entity_type="dataset",
        action="write", expected_schema_version=2,
        category="Single Hop Migration",
    ),
    _aspect_migration(
        tc=4, name="Full sweep multi hop",
        aspect_name="embed", entity_type="dashboard",
        action="sweep", expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=5, name="Read path multi hop",
        aspect_name="embed", entity_type="dashboard",
        action="read", expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=6, name="Write path multi hop",
        aspect_name="embed", entity_type="dashboard",
        action="write", expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=7, name="Mid-chain start at v2",
        aspect_name="embed", entity_type="dashboard",
        action="sweep",
        starting_schema_version=2, expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=8, name="Already at target v4",
        aspect_name="embed", entity_type="dashboard",
        action="sweep",
        starting_schema_version=4, expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=9, name="Future version v5",
        aspect_name="embed", entity_type="dashboard",
        action="sweep",
        starting_schema_version=5, expected_schema_version=5,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=10, name="Null systemMetadata",
        aspect_name="embed", entity_type="dashboard",
        action="sweep", expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=11, name="Gap in mutator chain",
        aspect_name="embed", entity_type="dashboard",
        action="sweep",
        starting_schema_version=2, expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=12, name="transform() returns null",
        aspect_name="embed", entity_type="dashboard",
        action="sweep",
        starting_schema_version=3, expected_schema_version=3,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=13, name="Invalid URN crashes sweep",
        aspect_name="embed", entity_type="dashboard",
        action="sweep", expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=14, name="Malformed JSON in metadata",
        aspect_name="embed", entity_type="dashboard",
        action="sweep", expected_schema_version=None,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=15, name="ES reindexing after migration",
        aspect_name="embed", entity_type="dashboard",
        action="es", expected_schema_version=4,
        category="Multi Hop Migration",
    ),
    _aspect_migration(
        tc=16, name="Re-run after SUCCEEDED",
        aspect_name="embed", entity_type="dashboard",
        action="lifecycle", expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=17, name="ABORTED treated as terminal",
        aspect_name="embed", entity_type="dashboard",
        action="lifecycle", expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=18, name="IN_PROGRESS resumes sweep",
        aspect_name="embed", entity_type="dashboard",
        action="lifecycle", expected_schema_version=None,
        category="Upgrade Lifecycle",
    ),
    _aspect_migration(
        tc=19, name="chain.disable() not wired",
        aspect_name="embed", entity_type="dashboard",
        action="sweep", expected_schema_version=None,
        category="Disable Migration After Upgrade",
    ),
    _aspect_migration(
        tc=20, name="Read path in-memory only",
        aspect_name="embed", entity_type="dashboard",
        action="read",
        starting_schema_version=1, expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=21, name="Write path persists",
        aspect_name="embed", entity_type="dashboard",
        action="write",
        starting_schema_version=1, expected_schema_version=4,
        category="Data Integrity Verification",
    ),
    _aspect_migration(
        tc=22, name="APP_SOURCE stamped on sweep",
        aspect_name="embed", entity_type="dashboard",
        action="integrity", expected_schema_version=None,
        category="Data Integrity Verification",
    ),
    # TC-23 placeholder — present in _TC_ACTION as "rolling" but absent from CSV.
    # Keep it here so future plans (Phase 6 RollingRestartPhase) can wire it up.
    _aspect_migration(
        tc=23, name="Rolling Upgrade",
        aspect_name="embed", entity_type="dashboard",
        action="rolling", expected_schema_version=None,
        category="Rolling Upgrade",
    ),
]


def load_scenarios() -> list[ZDUTestScenario]:
    """Return the canonical scenario list for all currently codified suites."""
    return list(SUITE_A_SCENARIOS)
```

- [ ] **Step 3: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.scenarios import load_scenarios, SUITE_A_SCENARIOS
ss = load_scenarios()
assert len(ss) == 23, f'expected 23, got {len(ss)}'
assert {s.tc_number for s in ss} == set(range(1, 24))
# Spot-check known fields
tc1 = next(s for s in ss if s.tc_number == 1)
assert tc1.aspect_name == 'globalTags'
assert tc1.expected_schema_version == 2
assert tc1.expected_to_fail is False
tc7 = next(s for s in ss if s.tc_number == 7)
assert tc7.starting_schema_version == 2
assert tc7.expected_to_fail is True  # in KNOWN_FAILURES
print('OK')
"
```

If the path resolution fails (`ModuleNotFoundError: No module named 'tests'`), `cd smoke-test` and use `venv/bin/python` instead.

Expected: `OK`.

- [ ] **Step 4: Run framework tests for no regression (loader changes still pending — only checks scenarios.py imports cleanly)**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 116 pass (Plan 2 baseline).

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenarios.py
git commit -m "feat(zdu): codify Suite A scenarios as Python module (replaces CSV)"
```

Re-stage if pre-commit hooks reformat.

---

## Task 2: Strip Sheets/CSV code from `scenario_loader.py`; route through `scenarios.py`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/scenario_loader.py`

**Pattern:** Keep `KNOWN_FAILURES`, `ZDUTestScenario`, `_make_urn`, `make_old_data`, `ScenarioExecutor` unchanged. Drop the `csv`, `io`, `os`, `pathlib`, `requests` imports. Drop the four `_TC_*` dicts (they live in `scenarios.py` now via `_aspect_migration` constructor calls). Replace `ScenarioLoader` with a one-method class that returns the static list.

- [ ] **Step 1: Modify imports**

Replace lines 1–15 of `scenario_loader.py` with:

```python
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .context import ValidationResult
from .datahub_client import DataHubClient
from .suite import Suite

if TYPE_CHECKING:
    from .context import TestContext
```

(Removed: `csv`, `io`, `os`, `pathlib`, `requests`, and the unused `suite_for_tc` import.)

- [ ] **Step 2: Delete the four `_TC_*` dicts**

Remove lines 32–124 of the old file: `_TC_STARTING_VERSION`, `_TC_EXPECTED_VERSION`, `_TC_ACTION`, `_TC_ASPECT_NAME`, `_TC_ENTITY_TYPE`. Keep `KNOWN_FAILURES` (the new `scenarios.py` imports it from here).

- [ ] **Step 3: Replace `ScenarioLoader` with a thin loader**

Replace the entire `ScenarioLoader` class (lines 149–219) with:

```python
class ScenarioLoader:
    """Returns the codified scenario list. Kept as a class for backward
    compatibility with callers that instantiate it (``ScenarioLoader().load()``).
    """

    def load(self, source: str | None = None) -> list[ZDUTestScenario]:
        # ``source`` is preserved as a kwarg for backward compatibility but
        # ignored — scenarios live in framework/scenarios.py now.
        from .scenarios import load_scenarios
        return load_scenarios()
```

The deferred import (inside the method) avoids a circular-import risk: `scenarios.py` imports `ZDUTestScenario` and `KNOWN_FAILURES` from this module.

- [ ] **Step 4: Verify nothing else references the deleted names**

```bash
grep -rn "_TC_STARTING_VERSION\|_TC_EXPECTED_VERSION\|_TC_ACTION\|_TC_ASPECT_NAME\|_TC_ENTITY_TYPE\|SHEET_URL\|LOCAL_FALLBACK\|_resolve_source\|_fetch_csv\|_parse_row" smoke-test/tests/zdu/ | grep -v __pycache__
```

Expected output:

```
smoke-test/tests/zdu/framework/test_scenario_loader.py:...  (test references — fixed in Task 3)
```

If any non-test caller references `_TC_*` or the CSV/Sheet helpers, fix those callers in this task.

- [ ] **Step 5: Smoke-import**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.scenario_loader import ScenarioLoader, ZDUTestScenario, KNOWN_FAILURES, ScenarioExecutor, _make_urn, make_old_data
ss = ScenarioLoader().load()
assert len(ss) == 23
assert ss[0].tc_number == 1
print('OK', len(ss), 'scenarios loaded')
"
```

Expected: `OK 23 scenarios loaded`.

- [ ] **Step 6: Run framework tests — most pass; `test_scenario_loader.py` will fail (fixed in Task 3)**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -10
```

Expected: ~105 pass, ~11 fail in `test_scenario_loader.py` (those tests reference `_parse_row`, `_TC_EXPECTED_VERSION`, etc. — all gone). That's fine — Task 3 fixes them.

- [ ] **Step 7: Commit**

```bash
git add smoke-test/tests/zdu/framework/scenario_loader.py
git commit -m "refactor(zdu): ScenarioLoader.load() returns the codified Python list

Strips Google Sheets HTTP fallback and CSV parsing. Drops the four _TC_*
metadata dicts (they're now constructor calls in scenarios.py). Closes
Notion review threads D2 + D3."
```

---

## Task 3: Rewrite `test_scenario_loader.py` for the new module

**Files:**

- Modify (rewrite): `smoke-test/tests/zdu/framework/test_scenario_loader.py`

**Pattern:** The existing tests target `_parse_row(dict) → ZDUTestScenario`. That helper is gone. New tests target `load_scenarios()` and assert the codified list's shape + completeness.

- [ ] **Step 1: Read the current test file to inventory what was being asserted**

```bash
grep -n "^def test_\|^class Test" smoke-test/tests/zdu/framework/test_scenario_loader.py
```

The original tests covered: load_parses_all_valid_rows, tc1_fields, tc4_multi_hop, tc12_known_failure, known_failures_set, empty_tc_number_skipped, tc23_is_always_skipped_by_number, plus the 4 cases I added in Plan 0 (default_scenario_type, explicit_scenario_type, unknown_tc_range_returns_none, executor_validate_takes_ctx_and_filters_by_tc).

- [ ] **Step 2: Rewrite the test file**

Replace the entire content of `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_scenario_loader.py`:

```python
"""Tests for the codified scenario list and the ScenarioExecutor.

Replaces the legacy CSV-row-parsing tests. Scenarios live in
``framework/scenarios.py`` as Python objects; the loader is a thin
list-returning shim.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.zdu.framework.context import SeededEntity, TestContext
from tests.zdu.framework.scenario_loader import (
    KNOWN_FAILURES,
    ScenarioExecutor,
    ScenarioLoader,
    ZDUTestScenario,
)
from tests.zdu.framework.scenarios import SUITE_A_SCENARIOS, load_scenarios
from tests.zdu.framework.suite import Suite


class TestLoadScenarios:
    def test_load_returns_full_suite_a(self) -> None:
        ss = load_scenarios()
        assert len(ss) == 23
        assert {s.tc_number for s in ss} == set(range(1, 24))

    def test_loader_class_returns_same_list(self) -> None:
        # Backward-compatible API: ScenarioLoader().load() == load_scenarios().
        a = [s.tc_number for s in ScenarioLoader().load()]
        b = [s.tc_number for s in load_scenarios()]
        assert a == b

    def test_loader_ignores_source_kwarg(self) -> None:
        # Existing callers may pass source=...; new loader ignores it.
        ss = ScenarioLoader().load(source="ignored")
        assert len(ss) == 23

    def test_all_scenarios_are_suite_a(self) -> None:
        # Currently only Suite A is codified. Future suites will extend this.
        for s in load_scenarios():
            assert s.suite == Suite.A
            assert s.scenario_type == "aspect_migration"


class TestScenarioFields:
    def test_tc1_globaltags_dataset(self) -> None:
        tc1 = next(s for s in load_scenarios() if s.tc_number == 1)
        assert tc1.aspect_name == "globalTags"
        assert tc1.entity_type == "dataset"
        assert tc1.action == "sweep"
        assert tc1.expected_schema_version == 2
        assert tc1.starting_schema_version is None
        assert tc1.expected_to_fail is False

    def test_tc4_embed_dashboard_multi_hop(self) -> None:
        tc4 = next(s for s in load_scenarios() if s.tc_number == 4)
        assert tc4.aspect_name == "embed"
        assert tc4.entity_type == "dashboard"
        assert tc4.expected_schema_version == 4

    def test_tc7_mid_chain_v2_is_xfail(self) -> None:
        tc7 = next(s for s in load_scenarios() if s.tc_number == 7)
        assert tc7.starting_schema_version == 2
        assert tc7.expected_schema_version == 4
        assert tc7.expected_to_fail is True
        assert tc7.skip_reason is not None
        assert "bridgeGap" in tc7.skip_reason

    def test_tc20_read_path_in_memory(self) -> None:
        tc20 = next(s for s in load_scenarios() if s.tc_number == 20)
        assert tc20.starting_schema_version == 1
        assert tc20.expected_schema_version == 4
        assert tc20.action == "read"


class TestKnownFailuresSet:
    def test_expected_to_fail_matches_known_failures_dict(self) -> None:
        for s in load_scenarios():
            expected = s.tc_number in KNOWN_FAILURES
            assert s.expected_to_fail is expected, (
                f"TC-{s.tc_number}: expected_to_fail={s.expected_to_fail} "
                f"but KNOWN_FAILURES has it={expected}"
            )

    def test_known_failure_skip_reason_matches_dict(self) -> None:
        for s in load_scenarios():
            if s.expected_to_fail:
                assert s.skip_reason == KNOWN_FAILURES[s.tc_number]


class TestExecutorValidateTakesCtxAndFiltersByTc:
    def test_validates_only_tc_specific_urns(self) -> None:
        datahub = MagicMock()
        aspect_resp = MagicMock()
        from tests.zdu.framework.scenarios import SUITE_A_SCENARIOS
        tc1 = next(s for s in SUITE_A_SCENARIOS if s.tc_number == 1)
        aspect_resp.schema_version = tc1.expected_schema_version
        datahub.get_aspect.return_value = aspect_resp

        ctx = TestContext()
        ctx.seeded_entities = [
            SeededEntity(
                urn="urn:li:dataset:tc1", aspect_name="globalTags",
                tc_number=1, seeded_data={}, expected_schema_version=2,
                validator=lambda d: True,
            ),
            SeededEntity(
                urn="urn:li:dataset:tc2", aspect_name="globalTags",
                tc_number=2, seeded_data={}, expected_schema_version=2,
                validator=lambda d: True,
            ),
        ]
        result = ScenarioExecutor(datahub).validate(tc1, ctx)
        assert result.status == "PASS"
        # Only TC-1's URN was queried, not TC-2's
        datahub.get_aspect.assert_called_once_with("urn:li:dataset:tc1", "globalTags")
```

- [ ] **Step 3: Run the rewritten tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_scenario_loader.py -v 2>&1 | tail -20
```

Expected: 11 tests pass (4 in TestLoadScenarios, 4 in TestScenarioFields, 2 in TestKnownFailuresSet, 1 in TestExecutorValidate).

- [ ] **Step 4: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 116 pass (was 116; same count — old tests gone, new tests added, net delta varies but framework total unchanged).

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/test_scenario_loader.py
git commit -m "test(zdu): rewrite scenario_loader tests for codified Python module"
```

---

## Task 4: Drop direct-MySQL seeding from `phases/seed.py`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/seed.py`

**Pattern:** Remove `_seed_direct_mysql`, `_DIRECT_DB_SEED_TCS`, and the IO-pool `mysql_upsert` block. All seeds (scenario entities + IO pool) go through `DataHubClient.ingest_mcp`. The running GMS already has `ASPECT_MIGRATION_MUTATOR_ENABLED=false` by default, so write-path mutators don't fire and entities land at their natural starting state.

- [ ] **Step 1: Read the existing file**

The file at `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/seed.py` currently contains:

- `_IO_POOL_SIZE = 5`, `_IO_POOL_ASPECT = "embed"`, `_IO_POOL_OLD_DATA`, `_IO_POOL_EXPECTED_VERSION`
- `_DIRECT_DB_SEED_TCS = {20}` (the only TC currently using direct DB)
- `_seed_direct_mysql(scenario)` method
- `SeedPhase.run(ctx)` with two direct-DB seed paths (scenario TC-20 + IO pool)

- [ ] **Step 2: Replace the file with the API-only version**

Replace `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/seed.py` with:

```python
from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import SeededEntity, TestContext
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient
from ..scenario_loader import (
    ScenarioExecutor,
    ZDUTestScenario,
)

log = logging.getLogger(__name__)

_IO_POOL_SIZE = 5
_IO_POOL_ASPECT = "embed"
_IO_POOL_OLD_DATA = {"renderUrl": "http://zdu-io-pool.example.com/embed"}
_IO_POOL_EXPECTED_VERSION = 4


def _noop_validator(data: dict) -> bool:
    return True


class SeedPhase(Phase):
    name = "seed"

    def __init__(
        self,
        executor: ScenarioExecutor,
        scenarios: list[ZDUTestScenario],
        datahub: DataHubClient,
        docker: DockerComposeClient,
    ) -> None:
        # ``docker`` is retained in the signature for backward compat with the
        # runner construction site, but no longer used (was needed for direct
        # MySQL inserts). Future plans may remove it.
        self._executor = executor
        self._scenarios = scenarios
        self._datahub = datahub
        self._docker = docker

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        seeded_count = 0
        try:
            for scenario in self._scenarios:
                urns = self._executor.seed(scenario)
                if not urns:
                    continue
                old_data = _scenario_seed_data(scenario)
                for urn in urns:
                    expected = scenario.expected_schema_version
                    ctx.seeded_entities.append(
                        SeededEntity(
                            urn=urn,
                            aspect_name=scenario.aspect_name,
                            tc_number=scenario.tc_number,
                            seeded_data=old_data,
                            expected_schema_version=expected
                            if expected is not None
                            else 1,
                            validator=_noop_validator,
                        )
                    )
                    seeded_count += 1
                log.info("TC-%03d: seeded %s", scenario.tc_number, urns[0])

            self._seed_io_pool_via_api(ctx)

            return PhaseResult(
                phase_name=self.name,
                status="passed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"seeded": seeded_count, "io_pool": _IO_POOL_SIZE},
            )
        except Exception as exc:
            log.exception("SeedPhase failed")
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=str(exc),
            )

    def _seed_io_pool_via_api(self, ctx: TestContext) -> None:
        """Seed the IO pool through the DataHub API.

        Sends ``systemMetadata={}`` (empty) so the row lands without a
        schemaVersion key — the running GMS keeps the write-path mutator
        chain disabled by default (``ASPECT_MIGRATION_MUTATOR_ENABLED=false``),
        so no mutator fires and the row stays at the OLD shape.
        """
        for i in range(_IO_POOL_SIZE):
            pool_urn = f"urn:li:dashboard:(test,zdu-io-pool-{i})"
            self._datahub.ingest_mcp(
                pool_urn, _IO_POOL_ASPECT, _IO_POOL_OLD_DATA, system_metadata={}
            )
            log.info("IO-pool[%d]: API ingest → %s schemaVersion=null", i, pool_urn)
            ctx.io_pool_entities.append(
                SeededEntity(
                    urn=pool_urn,
                    aspect_name=_IO_POOL_ASPECT,
                    tc_number=0,
                    seeded_data=_IO_POOL_OLD_DATA,
                    expected_schema_version=_IO_POOL_EXPECTED_VERSION,
                    validator=_noop_validator,
                )
            )


def _scenario_seed_data(scenario: ZDUTestScenario) -> dict:
    """Re-derive the seed payload for a scenario without re-importing helpers.

    Mirrors ``scenario_loader.make_old_data`` for use only inside
    ``SeedPhase``. Kept local to avoid growing the public surface of the
    loader module.
    """
    from ..scenario_loader import make_old_data
    return make_old_data(scenario)
```

- [ ] **Step 3: Run the existing seed-related framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 116 pass.

- [ ] **Step 4: Smoke-test the phase constructs cleanly**

```bash
smoke-test/venv/bin/python -c "
from unittest.mock import MagicMock
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.seed import SeedPhase
from tests.zdu.framework.scenario_loader import ScenarioExecutor
from tests.zdu.framework.scenarios import load_scenarios

datahub = MagicMock()
docker = MagicMock()
phase = SeedPhase(
    executor=ScenarioExecutor(datahub),
    scenarios=load_scenarios(),
    datahub=datahub,
    docker=docker,
)
ctx = TestContext()
result = phase.run(ctx)
print('phase status:', result.status)
print('seeded entities:', len(ctx.seeded_entities))
print('io pool entities:', len(ctx.io_pool_entities))
print('mysql_upsert calls (should be 0):', docker.mysql_upsert.call_count)
assert docker.mysql_upsert.call_count == 0, 'no direct-MySQL inserts should remain!'
"
```

Expected output:

```
phase status: passed
seeded entities: <some count > 0>
io pool entities: 5
mysql_upsert calls (should be 0): 0
```

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/seed.py
git commit -m "refactor(zdu): drop direct-MySQL seeding; all seeds via DataHub API

Closes Notion review thread D1: scenario entities and IO-pool entities
are now ingested through the running GMS so they reflect valid entity
shapes (incl. all required DataHub fields). The write-path mutator
chain stays disabled at seed time (default GMS env), so entities land
at their natural pre-upgrade state without the chain firing."
```

---

## Task 5: Delete `scenarios.csv`

**Files:**

- Delete: `smoke-test/tests/zdu/scenarios.csv`

- [ ] **Step 1: Confirm no references remain**

```bash
grep -rn "scenarios.csv" smoke-test/ docs/ | grep -v __pycache__ | grep -v "\.git"
```

Expected: no matches (the only references should have been in `scenario_loader.py` which Task 2 cleared).

- [ ] **Step 2: Delete the file**

```bash
git rm smoke-test/tests/zdu/scenarios.csv
```

- [ ] **Step 3: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 116 pass.

- [ ] **Step 4: Commit**

```bash
git commit -m "chore(zdu): delete scenarios.csv (replaced by codified Python module)"
```

---

## Task 6: Live integration check (Suite A end-to-end)

**Pre-requisite:** Compose stack up via `scripts/dev/datahub-dev.sh status` reporting `ready: true`.

This is the validation step that catches contract issues — same pattern as Plans 0/1/2.

- [ ] **Step 1: Run Suite A E2E**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,upgrade_blocking \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -45
```

We skip `upgrade` (legacy rebuild) and `upgrade_blocking` (would disable mutator chain) so `sweep_and_io` can exercise the full Suite A behavior on the same image.

Expected: 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL (same as Plan 1/2 baseline). The seed phase prints `IO-pool[N]: API ingest → ... schemaVersion=null` lines instead of MySQL exec lines.

- [ ] **Step 2: Verify TC-020 still has its expected behavior under API-only seeding**

TC-020 used to be direct-DB-seeded. Under API-only seeding, the running GMS with flag=false should still produce a row with no schemaVersion (the API call sends `system_metadata={"schemaVersion": 1}` per `ScenarioExecutor.seed`). Confirm via direct DB query:

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  SELECT urn, JSON_EXTRACT(systemmetadata, '\$.schemaVersion') AS sv
  FROM metadata_aspect_v2
  WHERE urn = 'urn:li:dashboard:(test,zdu-tc-20)' AND aspect = 'embed' AND version = 0;
"
```

Expected: TC-20's stored `schemaVersion` = `1` (API-honored) OR `4` (write-path mutator fired).

If it's `4`, the write-path mutator fired despite our flag-off assumption — that means the GMS is running with the flag on, and TC-020 truly cannot work without the two-image architecture (F-3). In that case, classify TC-020 as XFAIL with a clear reason and proceed.

- [ ] **Step 3: Verify IO pool seeded without direct DB**

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  SELECT urn, systemmetadata
  FROM metadata_aspect_v2
  WHERE urn LIKE 'urn:li:dashboard:(test,zdu-io-pool-%)' AND aspect = 'embed' AND version = 0
  LIMIT 5;
"
```

Expected: 5 rows; `systemmetadata` should be either `{}` or contain `runId` / similar GMS-natural fields. The KEY check is that the rows EXIST and were created via the API (the `runId` field is a GMS write-path side-effect; direct DB inserts never produce it).

- [ ] **Step 4: If TC-020 regressed, reclassify**

If Step 2 shows TC-020 fails because the running GMS auto-mutates on write, add it to `KNOWN_FAILURES`:

```python
# In framework/scenario_loader.py KNOWN_FAILURES dict, add:
20: "Direct-DB seeding removed; running GMS auto-mutates on write. Reinstated under F-3 two-image-tag plan",
```

This preserves Suite A's clean pass/xfail count.

- [ ] **Step 5: Commit any reclassification**

```bash
git add smoke-test/tests/zdu/framework/scenario_loader.py
git commit -m "fix(zdu): reclassify TC-020 as XFAIL — needs two-image plan F-3"
```

If TC-020 still passes via the API path, no commit needed.

---

## Task 7: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff fcadcdd4de..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-f1.diff
wc -l /tmp/zdu-plan-f1.diff
```

(`fcadcdd4de` is the last Plan 2 commit. Adjust the SHA if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send the reviewer this prompt:

> Review the diff at `/tmp/zdu-plan-f1.diff`. This PR implements the first three Notion review threads (D1, D2, D3):
>
> 1. **D2 + D3 — codify scenarios:** moved the Suite A scenario list from `scenarios.csv` (loaded via Google Sheets / local fallback) into a Python module `framework/scenarios.py`. Each scenario is a `ZDUTestScenario` instance constructed via a small helper `_aspect_migration(...)`. `ScenarioLoader.load()` is now a one-line shim that returns the codified list. The four `_TC_*` metadata dicts (`_TC_STARTING_VERSION`, `_TC_EXPECTED_VERSION`, `_TC_ACTION`, `_TC_ASPECT_NAME`, `_TC_ENTITY_TYPE`) are gone — their data is in the constructor calls.
> 2. **D1 — drop direct-MySQL seeding:** `phases/seed.py` no longer does any `mysql_upsert` calls. `_seed_direct_mysql` and `_DIRECT_DB_SEED_TCS` are removed. The IO pool now seeds through `DataHubClient.ingest_mcp` against the running GMS (which has `ASPECT_MIGRATION_MUTATOR_ENABLED=false` by default, so write-path mutators don't fire and entities land at their natural pre-upgrade state).
> 3. `scenarios.csv` is deleted.
>
> Check specifically:
>
> 1. **No CSV/Sheet code remains** — grep for `csv.DictReader`, `requests`, `SHEET_URL`, `LOCAL_FALLBACK`, `_resolve_source`, `_fetch_csv`, `_parse_row`. All should be gone from `scenario_loader.py`.
> 2. **`KNOWN_FAILURES` is the single source of truth for XFAIL classification.** `scenarios.py` reads from it via `tc in KNOWN_FAILURES`; no parallel "expected_to_fail" data anywhere.
> 3. **No `mysql_upsert` calls** in `phases/seed.py` (or anywhere outside the legacy `phases/sweep_and_io.py` race-window logic).
> 4. **TC count preserved:** all 23 scenarios still load (TC-1..TC-23). Suite A still has the same XFAIL set (TCs 7, 11, 12, 13, 14, 19, 21).
> 5. **`ScenarioLoader.load(source=...)` backward compat** — existing callers that pass `source=` must still work (parameter is preserved but ignored).
> 6. **Type hints complete:** `scenarios.py`'s `_aspect_migration` helper returns `ZDUTestScenario`; `load_scenarios()` returns `list[ZDUTestScenario]`.
> 7. **Test quality:** new `test_scenario_loader.py` tests assert the codified list's shape + spot-check known fields. The `KNOWN_FAILURES` consistency check makes drift between the module and the dict impossible.
> 8. **No coupling violations:** `scenarios.py` imports from `scenario_loader` (for the dataclass + KNOWN_FAILURES) and `suite` only. No reverse imports.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan F-1"
```

---

## Self-Review

**Spec coverage** (against Notion review threads D1, D2, D3):

| Thread                                        | Task                                                             |
| --------------------------------------------- | ---------------------------------------------------------------- |
| D1 — drop direct MySQL seeding                | Task 4 (`phases/seed.py` rewrite)                                |
| D2 — codify scenarios in test code            | Tasks 1 + 2 (`scenarios.py` create + `scenario_loader.py` strip) |
| D3 — code is source of truth, no Google Sheet | Tasks 1 + 2 (Sheet code removed)                                 |
| D4 — arbitrary version-to-version testing     | OUT OF SCOPE — Plan F-4                                          |
| D5 — test migrators not env hooks             | OUT OF SCOPE — separate Java-side plan                           |

**Placeholder scan:** None.

**Type / signature consistency:**

- `ZDUTestScenario` dataclass — unchanged from Plan 0; all fields populated by `_aspect_migration` helper match.
- `KNOWN_FAILURES: dict[int, str]` — single source of truth, used by both `scenarios.py` and external callers.
- `ScenarioLoader().load() -> list[ZDUTestScenario]` — signature preserved for backward compat.
- `ScenarioLoader().load(source=...)` — kwarg preserved (ignored) so existing conftest call site `loader.load(source=zdu_config.scenario_sheet_url or None)` still works without a conftest change.
- `ScenarioExecutor.validate(scenario, ctx)` — unchanged from Plan 0.
- `SeedPhase(executor, scenarios, datahub, docker)` — constructor unchanged. `docker` field retained for backward compat though no longer used.

**Risks called out:**

1. **TC-020 may flip to FAIL.** It's currently classified as expected-to-PASS but achieves that via direct-MySQL seeding. Under API-only seeding, if the running GMS has `ASPECT_MIGRATION_MUTATOR_ENABLED=true`, the write-path mutator fires and the row is stored at v=4 — which is fine for read-path tests but breaks the "DB record remains at schemaVersion=1" assertion. Task 6 step 4 documents the reclassification to XFAIL if this surfaces.

2. **`_seed_direct_mysql` was the only consumer of `make_old_data` for TC-20.** That helper is still exported from `scenario_loader.py` and used by `ScenarioExecutor.seed`. Verified.

3. **The legacy CSV-row tests are gone, replaced by list-shape tests.** This is a net coverage shift, not a loss — the new tests verify the codified list's completeness against `KNOWN_FAILURES`, which is stronger than verifying CSV row parsing.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-07-zdu-plan-f1-codify-scenarios-and-api-only-seed.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
