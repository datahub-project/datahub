# ZDU E2E — Plan 8: Phase 9 `RuntimeMigrationPhase` + Phase 10 Dual-Write State Validation

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Phase 9 `RuntimeMigrationPhase` (post-Phase-8 read/write mutator-chain probes) and extend Phase 10 `ValidationPhase` with a new validator covering Dimension 5 of the design doc — alias targets and dual-write state. Together these consume the captures laid down by Plans 2 / 4 / 7 (`ctx.upgrade_blocking.indices`, `ctx.rolling_restart.dual_write_start_times`, `ctx.upgrade_nonblocking.dual_write_disabled_indices`) and turn them into actionable assertions.

**Architecture:** Two changes. (1) New `RuntimeMigrationPhase` in `phases/runtime_migration.py` that runs after `upgrade_nonblocking` and before `validation`. It re-reads N seeded entities through GMS to capture the read-path mutator's observed `schemaVersion`, then issues a small write batch to capture the write-path mutator's persisted version. Results land in `ctx.runtime_migration` for Phase 10 to inspect. (2) `ValidationPhase` gains a `_check_dual_write_state(ctx)` helper that synthesises a single `ValidationResult` per asserted dimension-5 invariant. The check pulls from the three pre-existing captures and produces FAIL records when alias targets don't match, dual-write start-times are missing for an index that should have one, or a dual-write-disabled index isn't in the blocking-result index set.

**Tech Stack:** Python 3 (existing). Reuses `DataHubClient`, `ElasticsearchClient`, `MySQLClient`. No new dependencies.

**Out of scope (deferred):**

- TC-019 / TC-408 — `AspectMigrationMutatorChain.disable()` runtime hook capture. Needs a new log-monitor parser; scope for a follow-up plan.
- Dimension 4 (Elasticsearch field presence) — needs per-scenario ES-mapping expectations encoded into `ZDUTestScenario`. Separate plan to add the expectation field + the validator.
- TC-501 / TC-502 latency / throughput SLOs — separate plan.
- TC-403 race-window line-by-line interleave proof — `ctx.io_write_results` already records the outcome; a follow-up plan can correlate `[upgrade] sweep URN: X` and `[gms] Producing MCL ... X` log lines.
- Re-running probes AFTER a chain-disable hook fires to verify mutator bypass — depends on chain-disable capture (out of scope above).

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add RuntimeMigrationResult dataclass + ctx.runtime_migration slot
├── runner.py                                    MODIFY — wire RuntimeMigrationPhase between upgrade_nonblocking and validation
├── phases/
│   ├── runtime_migration.py                     CREATE — RuntimeMigrationPhase
│   └── validation.py                            MODIFY — _check_dual_write_state helper + invocation in run()
├── test_runtime_migration.py                    CREATE — phase unit tests (mocked DataHubClient)
├── test_validation.py                           MODIFY — add 4 dual-write-state validator tests (existing test file; if missing, create it minimally)
└── README.md                                    MODIFY — append Phase 9 subsection; expand validation dimension-5 description
```

---

## Task 1: Add `RuntimeMigrationResult` dataclass + ctx slot

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Single dataclass in pipeline-order placement.

- [ ] **Step 1: Add the dataclass**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`, locate the existing `class IOWriteResult` dataclass (line 30) — `RuntimeMigrationProbe` and `RuntimeMigrationResult` reuse the same `(urn, observed_version, expected_version)` shape but represent the post-Phase-8 mutator-chain probe rather than concurrent harness writes. Insert the two new dataclasses AFTER `UpgradeNonBlockingResult` (around line 122) and BEFORE `RollingRestartResult`:

```python
@dataclass
class RuntimeMigrationProbe:
    """One read/write probe captured by ``RuntimeMigrationPhase``.

    ``mode`` distinguishes a read-path mutator probe (``"read"``) from a
    write-path mutator probe (``"write"``). ``observed_version`` is the
    ``schemaVersion`` returned by GMS; for write probes it's the version
    persisted on the post-write read-back. ``expected_version`` is the
    target schema version the mutator chain should produce.
    """

    urn: str
    aspect_name: str
    mode: Literal["read", "write"]
    observed_version: int
    expected_version: int
    timestamp: datetime
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and self.observed_version == self.expected_version


@dataclass
class RuntimeMigrationResult:
    """Captured by ``RuntimeMigrationPhase``.

    ``read_probes`` is one entry per seeded URN re-read after the upgrade
    completes. ``write_probes`` is one entry per fresh write issued to
    a disjoint URN namespace (``zdu-rt-{i}``) and verified by read-back.
    Phase 10 consumes ``passed_read_count`` / ``passed_write_count`` to
    decide if the runtime mutator chain is operating correctly.
    """

    read_probes: list[RuntimeMigrationProbe] = field(default_factory=list)
    write_probes: list[RuntimeMigrationProbe] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def passed_read_count(self) -> int:
        return sum(1 for p in self.read_probes if p.passed)

    @property
    def passed_write_count(self) -> int:
        return sum(1 for p in self.write_probes if p.passed)
```

`Literal` is already imported at the top of `context.py`. Confirm before adding — if not, add `from typing import Literal` to the imports.

- [ ] **Step 2: Add the slot to `TestContext`**

Inside the `TestContext` dataclass, locate the `# UpgradeNonBlockingPhase writes` block. Insert AFTER it, BEFORE `# ValidationPhase writes`:

```python
    # RuntimeMigrationPhase writes
    runtime_migration: RuntimeMigrationResult | None = None
```

- [ ] **Step 3: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from datetime import datetime
from tests.zdu.framework.context import (
    RuntimeMigrationProbe,
    RuntimeMigrationResult,
    TestContext,
)
ctx = TestContext()
assert ctx.runtime_migration is None
p = RuntimeMigrationProbe(
    urn='urn:li:dashboard:(test,zdu-rt-0)', aspect_name='embed', mode='read',
    observed_version=4, expected_version=4, timestamp=datetime.utcnow(),
)
assert p.passed
r = RuntimeMigrationResult(read_probes=[p])
assert r.passed_read_count == 1
ctx.runtime_migration = r
print('OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 160 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): RuntimeMigrationResult + RuntimeMigrationProbe dataclasses for Phase 9"
```

Re-stage if pre-commit hooks reformat.

---

## Task 2: Implement `RuntimeMigrationPhase` + unit tests

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/runtime_migration.py`
- Create: `smoke-test/tests/zdu/framework/test_runtime_migration.py`

**Pattern:** Same shape as `InjectTrafficPrePhase` / `InjectTrafficDualPhase` (Plans 5 / 6) — write through GMS, verify through GMS read. Differences: this phase ALSO probes existing seeded entities (read-only) before issuing fresh writes. Failures are recorded but never raised — the phase always reports `passed`; Phase 10 makes the verdict.

### 2.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_runtime_migration.py`:

```python
"""Unit tests for RuntimeMigrationPhase — uses mocked DataHubClient."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import SeededEntity, TestContext
from tests.zdu.framework.datahub_client import AspectResponse
from tests.zdu.framework.phases.runtime_migration import RuntimeMigrationPhase


def _seeded(i: int, expected_version: int = 4) -> SeededEntity:
    return SeededEntity(
        urn=f"urn:li:dashboard:(test,zdu-tc-{i})",
        aspect_name="embed",
        seeded_data={"renderUrl": f"http://seed/{i}"},
        starting_schema_version=1,
        expected_schema_version=expected_version,
    )


def _aspect(version: int) -> AspectResponse:
    return AspectResponse(
        data={"renderUrl": "http://x/x"},
        system_metadata={"schemaVersion": version},
    )


@pytest.fixture
def datahub() -> MagicMock:
    m = MagicMock()
    m.get_aspect.return_value = _aspect(4)
    return m


@pytest.fixture
def phase(datahub: MagicMock) -> RuntimeMigrationPhase:
    return RuntimeMigrationPhase(datahub=datahub, n_write_probes=2, n_read_probes=3)


class TestRuntimeMigrationPhase:
    def test_reads_n_seeded_entities_and_records_probes(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[_seeded(i) for i in range(5)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert len(ctx.runtime_migration.read_probes) == 3
        assert ctx.runtime_migration.passed_read_count == 3
        # All read probes captured the right (urn, observed_version, expected_version)
        for probe in ctx.runtime_migration.read_probes:
            assert probe.observed_version == probe.expected_version == 4
            assert probe.mode == "read"

    def test_writes_n_fresh_entities_and_verifies_persisted_version(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[_seeded(0)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert len(ctx.runtime_migration.write_probes) == 2
        assert ctx.runtime_migration.passed_write_count == 2
        # n_write_probes calls to ingest_mcp + (n_read_probes + n_write_probes) get_aspect
        assert datahub.ingest_mcp.call_count == 2
        assert datahub.get_aspect.call_count == 1 + 2  # 1 read probe (clamped to 1 seed) + 2 write read-backs

    def test_read_probe_records_mismatch_without_failing_phase(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        # GMS returns v2 instead of expected v4 — read mutator did not upgrade.
        datahub.get_aspect.return_value = _aspect(2)
        ctx = TestContext(seeded_entities=[_seeded(0), _seeded(1), _seeded(2)])
        result = phase.run(ctx)
        assert result.status == "passed"  # phase always passes; Phase 10 is the verdict
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.passed_read_count == 0
        assert all(
            p.observed_version == 2 and p.expected_version == 4
            for p in ctx.runtime_migration.read_probes
        )

    def test_write_probe_records_error_when_ingest_fails(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        datahub.ingest_mcp.side_effect = RuntimeError("GMS unavailable")
        ctx = TestContext(seeded_entities=[_seeded(0)])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.passed_write_count == 0
        assert all(
            "GMS unavailable" in (p.error or "")
            for p in ctx.runtime_migration.write_probes
        )

    def test_no_seeded_entities_skips_read_probes(
        self, phase: RuntimeMigrationPhase, datahub: MagicMock
    ) -> None:
        ctx = TestContext(seeded_entities=[])
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.runtime_migration is not None
        assert ctx.runtime_migration.read_probes == []
        # Write probes still run — disjoint URN namespace.
        assert len(ctx.runtime_migration.write_probes) == 2
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_runtime_migration.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'RuntimeMigrationPhase'`.

### 2.2 — Implement the phase

- [ ] **Step 3: Create the phase file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/runtime_migration.py`:

```python
"""Phase 9 — RuntimeMigrationPhase.

Runs lightweight read/write probes against the live NEW GMS after Phase 8
completes. Captures the schema-versions observed via the read-path and
write-path mutator chains into ``TestContext.runtime_migration``. Phase 10
(Validation) consumes the captured probes to assert the mutator chain is
operating correctly.

The phase always reports ``passed`` — failures are recorded as probe-level
errors so Phase 10 can produce a unified verdict alongside other validator
findings. This matches the design doc's "validation does not fail fast"
principle (§5.10).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from .base import Phase, PhaseResult
from ..context import (
    RuntimeMigrationProbe,
    RuntimeMigrationResult,
    SeededEntity,
    TestContext,
)
from ..datahub_client import DataHubClient

log = logging.getLogger(__name__)

_DEFAULT_N_READ_PROBES = 5
_DEFAULT_N_WRITE_PROBES = 3
_DEFAULT_ASPECT = "embed"
_DEFAULT_EXPECTED_WRITE_VERSION = 4
_WRITE_PROBE_PAYLOAD: dict[str, Any] = {
    "renderUrl": "http://zdu-rt.example.com/embed",
}


class RuntimeMigrationPhase(Phase):
    name = "runtime_migration"

    def __init__(
        self,
        datahub: DataHubClient,
        n_read_probes: int = _DEFAULT_N_READ_PROBES,
        n_write_probes: int = _DEFAULT_N_WRITE_PROBES,
        expected_write_version: int = _DEFAULT_EXPECTED_WRITE_VERSION,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._n_read_probes = n_read_probes
        self._n_write_probes = n_write_probes
        self._expected_write_version = expected_write_version
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        read_probes = self._do_read_probes(ctx.seeded_entities)
        write_probes = self._do_write_probes()

        result = RuntimeMigrationResult(
            read_probes=read_probes,
            write_probes=write_probes,
            duration_s=time.monotonic() - t0,
        )
        ctx.runtime_migration = result

        log.info(
            "RuntimeMigration complete — reads %d/%d passed; writes %d/%d passed",
            result.passed_read_count,
            len(read_probes),
            result.passed_write_count,
            len(write_probes),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "read_probes": len(read_probes),
                "read_passed": result.passed_read_count,
                "write_probes": len(write_probes),
                "write_passed": result.passed_write_count,
            },
        )

    def _do_read_probes(
        self, seeded: list[SeededEntity]
    ) -> list[RuntimeMigrationProbe]:
        if not seeded:
            return []
        # Take the first N entities. Their expected_schema_version is the
        # post-mutator-chain target; the read-path mutator should produce it.
        sample = seeded[: self._n_read_probes]
        out: list[RuntimeMigrationProbe] = []
        for entity in sample:
            try:
                resp = self._datahub.get_aspect(entity.urn, entity.aspect_name)
                observed = resp.schema_version
                error: str | None = None
            except Exception as exc:
                log.warning(
                    "RuntimeMigration: read probe failed for %s: %s", entity.urn, exc
                )
                observed = 0
                error = str(exc)
            out.append(
                RuntimeMigrationProbe(
                    urn=entity.urn,
                    aspect_name=entity.aspect_name,
                    mode="read",
                    observed_version=observed,
                    expected_version=entity.expected_schema_version,
                    timestamp=datetime.utcnow(),
                    error=error,
                )
            )
        return out

    def _do_write_probes(self) -> list[RuntimeMigrationProbe]:
        out: list[RuntimeMigrationProbe] = []
        for i in range(self._n_write_probes):
            urn = f"urn:li:dashboard:(test,zdu-rt-{i})"
            error: str | None = None
            observed = 0
            try:
                self._datahub.ingest_mcp(
                    urn,
                    self._aspect_name,
                    dict(_WRITE_PROBE_PAYLOAD),
                    system_metadata={},
                )
                resp = self._datahub.get_aspect(urn, self._aspect_name)
                observed = resp.schema_version
            except Exception as exc:
                log.warning(
                    "RuntimeMigration: write probe failed for %s: %s", urn, exc
                )
                error = str(exc)
            out.append(
                RuntimeMigrationProbe(
                    urn=urn,
                    aspect_name=self._aspect_name,
                    mode="write",
                    observed_version=observed,
                    expected_version=self._expected_write_version,
                    timestamp=datetime.utcnow(),
                    error=error,
                )
            )
        return out
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_runtime_migration.py -v 2>&1 | tail -15
```

Expected: 5 tests pass. **Note** the second test asserts `datahub.get_aspect.call_count == 1 + 2` because the test passes 1 seeded entity but `n_read_probes=3` — `_do_read_probes` clamps to whatever is available (`seeded[:3]` returns just the 1 entity). If the test fails on this exact count, adjust the assertion to match the actual clamp behaviour rather than fudging the implementation.

- [ ] **Step 5: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 165 pass (160 baseline + 5 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/runtime_migration.py \
        smoke-test/tests/zdu/framework/test_runtime_migration.py
git commit -m "feat(zdu): RuntimeMigrationPhase — post-Phase-8 mutator-chain probes"
```

Re-stage if pre-commit hooks reformat.

---

## Task 3: Wire `RuntimeMigrationPhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert AFTER `("upgrade_nonblocking", ...)` and BEFORE `("validation", ...)`. The phase only needs `self._datahub` — no MySQL or ES client.

- [ ] **Step 1: Add import**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, alongside other phase imports:

```python
from .phases.runtime_migration import RuntimeMigrationPhase
```

- [ ] **Step 2: Insert into the phases list**

Find the `("upgrade_nonblocking", UpgradeNonBlockingPhase(...))` tuple. Insert this tuple AFTER it, BEFORE `("validation", ValidationPhase(...))`:

```python
            (
                "runtime_migration",
                RuntimeMigrationPhase(datahub=self._datahub),
            ),
```

- [ ] **Step 3: Smoke-test runner constructs**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
print('runner constructed OK')
"
```

Expected: `runner constructed OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 165 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire RuntimeMigrationPhase into runner pipeline"
```

Re-stage if pre-commit hooks reformat.

---

## Task 4: Extend `ValidationPhase` with `_check_dual_write_state`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/validation.py`
- Modify or create: `smoke-test/tests/zdu/framework/test_validation.py`

**Pattern:** New static helper that walks the captures and emits `ValidationResult` records. Each invariant violation becomes one FAIL record with an explicit reason. Invocation goes after the existing `_check_read_progression` block in `run()`.

The five dimension-5 invariants (design doc §5.10):

1. Every alias in `ctx.upgrade_blocking.indices` should have an `IndexState` row recording its `next_index_name` (the post-swap target). If `next_index_name` is None or empty, FAIL.
2. Every index name appearing in `ctx.upgrade_nonblocking.dual_write_disabled_indices` must correspond to an alias's `old_backing_index_name` in `ctx.upgrade_blocking.indices` (sanity — cannot disable an index we never tracked).
3. For every index in `ctx.upgrade_blocking.indices`, if rolling restart ran, the per-index `dualWriteStartTime` should be present in `ctx.rolling_restart.dual_write_start_times`. Missing entries are FAIL — but ONLY if `ctx.rolling_restart` itself ran (not None). If rolling restart was skipped (None), this check is skipped.
4. `ctx.upgrade_nonblocking.indices` (post-sweep) and `ctx.upgrade_blocking.indices` (pre-sweep) should describe the SAME set of aliases. Mismatched alias sets are FAIL.
5. (Smoke / non-strict) — if `ctx.upgrade_nonblocking.dual_write_disabled_indices` is empty AND `ctx.upgrade_blocking.indices` is non-empty, emit a SKIP-classified record (informational) so the JSON report shows the missing capture explicitly. This catches the dev-stack no-mutators case without producing a false failure.

### 4.1 — Write failing tests

- [ ] **Step 1: Create or extend `test_validation.py`**

Check if `smoke-test/tests/zdu/framework/test_validation.py` exists:

```bash
ls smoke-test/tests/zdu/framework/test_validation.py 2>&1
```

If it exists, append the following test class. If not, create it with the necessary minimal scaffolding (imports, etc.) plus the test class:

```python
# ---------- _check_dual_write_state tests ----------

from datetime import datetime
from tests.zdu.framework.context import (
    IndexState,
    RollingRestartResult,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
    ValidationResult,
)
from tests.zdu.framework.phases.validation import ValidationPhase


def _ctx_with_post_upgrade_state(
    *,
    rolling_restart: RollingRestartResult | None = None,
    dual_write_disabled: list[str] | None = None,
    nonblocking_indices: list[IndexState] | None = None,
) -> TestContext:
    ctx = TestContext()
    ctx.upgrade_blocking = UpgradeBlockingResult(
        indices=[
            IndexState(
                alias="dashboardindex_v2",
                old_backing_index_name="dashboardindex_v2_old",
                next_index_name="dashboardindex_v2_new",
                source_doc_count=100,
                status="COMPLETED",
            ),
            IndexState(
                alias="datasetindex_v2",
                old_backing_index_name="datasetindex_v2_old",
                next_index_name="datasetindex_v2_new",
                source_doc_count=200,
                status="COMPLETED",
            ),
        ]
    )
    ctx.upgrade_nonblocking = UpgradeNonBlockingResult(
        indices=nonblocking_indices
        if nonblocking_indices is not None
        else list(ctx.upgrade_blocking.indices),
        dual_write_disabled_indices=dual_write_disabled or [],
    )
    ctx.rolling_restart = rolling_restart
    return ctx


class TestCheckDualWriteState:
    def test_all_invariants_satisfied_returns_no_failures(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            rolling_restart=RollingRestartResult(
                services_restarted=["datahub-gms-debug"],
                dual_write_start_times={
                    "dashboardindex_v2_old": 1700000000,
                    "datasetindex_v2_old": 1700000000,
                },
            ),
            dual_write_disabled=["dashboardindex_v2_old", "datasetindex_v2_old"],
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        # No FAIL records — both aliases have next_index_name, both old indices
        # have dual-write start times, and disabled set matches blocking set.
        assert all(r.status != "FAIL" for r in results)

    def test_alias_missing_next_index_name_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state()
        ctx.upgrade_blocking.indices[0].next_index_name = None
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any(
            "dashboardindex_v2" in r.actual_result and "next_index_name" in r.actual_result
            for r in fails
        )

    def test_dual_write_disabled_for_unknown_index_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            dual_write_disabled=["foo_v2_old"],  # not in blocking.indices
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any("foo_v2_old" in r.actual_result for r in fails)

    def test_rolling_restart_missing_dual_write_start_time_fails(self) -> None:
        ctx = _ctx_with_post_upgrade_state(
            rolling_restart=RollingRestartResult(
                services_restarted=["datahub-gms-debug"],
                dual_write_start_times={"dashboardindex_v2_old": 1700000000},
                # datasetindex_v2_old missing
            ),
            dual_write_disabled=[],
        )
        results = ValidationPhase._check_dual_write_state(ctx)
        fails = [r for r in results if r.status == "FAIL"]
        assert any("datasetindex_v2_old" in r.actual_result for r in fails)

    def test_no_rolling_restart_skips_dual_write_start_time_check(self) -> None:
        # rolling_restart is None — dual-write start-time check must NOT fail.
        ctx = _ctx_with_post_upgrade_state(rolling_restart=None)
        results = ValidationPhase._check_dual_write_state(ctx)
        assert not any(
            "dual_write_start_times" in r.actual_result.lower()
            for r in results
            if r.status == "FAIL"
        )

    def test_empty_disabled_set_emits_informational_skip(self) -> None:
        ctx = _ctx_with_post_upgrade_state(dual_write_disabled=[])
        results = ValidationPhase._check_dual_write_state(ctx)
        # Informational skip exists (status=SKIP) acknowledging dev-stack case.
        assert any(r.status == "SKIP" for r in results)
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_validation.py -v 2>&1 | tail -10
```

Expected: `AttributeError: type object 'ValidationPhase' has no attribute '_check_dual_write_state'`.

### 4.2 — Implement the helper

- [ ] **Step 3: Add `_check_dual_write_state` static method to `ValidationPhase`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/validation.py`, add the following static method to the `ValidationPhase` class — place it AFTER `_check_read_progression`:

```python
    @staticmethod
    def _check_dual_write_state(ctx: TestContext) -> list[ValidationResult]:
        """Phase 10 Dimension 5 — alias targets and dual-write state.

        Walks ``ctx.upgrade_blocking.indices``, ``ctx.upgrade_nonblocking``, and
        ``ctx.rolling_restart`` and emits one ``ValidationResult`` per invariant
        violation. Returns an empty list when nothing was captured (e.g.,
        upgrade_blocking phase was skipped) — the dimension is then trivially
        satisfied.
        """
        results: list[ValidationResult] = []
        blocking = ctx.upgrade_blocking
        if blocking is None or not blocking.indices:
            return results

        nonblocking = ctx.upgrade_nonblocking
        # Invariant 1: every alias has a non-empty next_index_name.
        for idx in blocking.indices:
            if not idx.next_index_name:
                results.append(
                    ValidationResult(
                        tc_number=0,
                        name=f"DualWriteState[{idx.alias}]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"alias '{idx.alias}': missing next_index_name "
                            f"after blocking sweep"
                        ),
                        failure_reason="alias swap target absent",
                    )
                )

        # Invariant 2: every disabled index name was tracked in blocking.indices.
        if nonblocking is not None:
            tracked_old = {
                i.old_backing_index_name
                for i in blocking.indices
                if i.old_backing_index_name
            }
            for name in nonblocking.dual_write_disabled_indices:
                if name not in tracked_old:
                    results.append(
                        ValidationResult(
                            tc_number=0,
                            name=f"DualWriteState[{name}]",
                            status="FAIL",
                            expected_to_fail=False,
                            actual_result=(
                                f"index '{name}' was marked DUAL_WRITE_DISABLED "
                                f"but not present in blocking.indices.old_backing_index_name set"
                            ),
                            failure_reason="disabled-index sanity",
                        )
                    )

        # Invariant 3: rolling restart recorded a dualWriteStartTime per old index.
        if ctx.rolling_restart is not None:
            recorded = ctx.rolling_restart.dual_write_start_times
            for idx in blocking.indices:
                old = idx.old_backing_index_name
                if old and old not in recorded:
                    results.append(
                        ValidationResult(
                            tc_number=0,
                            name=f"DualWriteState[{old}]",
                            status="FAIL",
                            expected_to_fail=False,
                            actual_result=(
                                f"index '{old}' missing from "
                                f"rolling_restart.dual_write_start_times"
                            ),
                            failure_reason="rolling-restart did not record dualWriteStartTime",
                        )
                    )

        # Invariant 4: blocking and nonblocking should describe the same alias set.
        if nonblocking is not None and nonblocking.indices:
            blocking_aliases = {i.alias for i in blocking.indices}
            nb_aliases = {i.alias for i in nonblocking.indices}
            missing_in_nb = blocking_aliases - nb_aliases
            extra_in_nb = nb_aliases - blocking_aliases
            if missing_in_nb or extra_in_nb:
                results.append(
                    ValidationResult(
                        tc_number=0,
                        name="DualWriteState[alias-set]",
                        status="FAIL",
                        expected_to_fail=False,
                        actual_result=(
                            f"alias mismatch — blocking only: {sorted(missing_in_nb)} ; "
                            f"nonblocking only: {sorted(extra_in_nb)}"
                        ),
                        failure_reason="alias set divergence between phases",
                    )
                )

        # Invariant 5 (informational SKIP): empty disabled set on a non-empty stack.
        if (
            nonblocking is not None
            and not nonblocking.dual_write_disabled_indices
            and blocking.indices
        ):
            results.append(
                ValidationResult(
                    tc_number=0,
                    name="DualWriteState[disabled-empty]",
                    status="SKIP",
                    expected_to_fail=False,
                    actual_result=(
                        "no DUAL_WRITE_DISABLED markings observed — "
                        "expected on a dev stack with no migration mutators registered"
                    ),
                )
            )
        return results
```

- [ ] **Step 4: Invoke the helper from `run()`**

Locate the existing `_check_read_progression` invocation block in `run()`. Insert AFTER it, BEFORE `ctx.validation_results = results`:

```python
        # Dimension 5 — alias targets and dual-write state
        for r in self._check_dual_write_state(ctx):
            results.append(r)
            if r.status == "FAIL":
                log.warning("DualWriteState FAIL: %s", r.failure_reason)
```

- [ ] **Step 5: Run validator tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_validation.py -v 2>&1 | tail -15
```

Expected: 6 tests pass (the dual-write-state class). If pre-existing `test_validation.py` had other tests, they should also still pass.

- [ ] **Step 6: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 171 pass (165 + 6 new).

- [ ] **Step 7: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/validation.py \
        smoke-test/tests/zdu/framework/test_validation.py
git commit -m "feat(zdu): ValidationPhase Dimension 5 — alias / dual-write state checks"
```

Re-stage if pre-commit hooks reformat.

---

## Task 5: README — Phase 9 + Validation Dimension 5 description

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Insert Phase 9 subsection AFTER Phase 8**

Find the existing `### Phase 8: Upgrade Non-Blocking` subsection. Append a new subsection AFTER it:

```markdown
### Phase 9: Runtime Migration

`RuntimeMigrationPhase` runs lightweight read/write probes against the live NEW GMS after Phase 8 completes. It captures the schema versions observed via the read-path and write-path mutator chains into `ctx.runtime_migration`. The phase always reports `passed` — failures are recorded as probe-level errors so Phase 10 (Validation) can produce a unified verdict alongside other validator findings.

The phase verifies (via captured probes consumed by Phase 10):

- **Read-path mutator** — re-reads the first N seeded entities (default 5) through GMS and records their observed `schemaVersion`. The read-path mutator should produce the entity's `expected_schema_version` regardless of the underlying DB row's stored shape.
- **Write-path mutator** — issues N fresh writes (default 3) against URNs in the disjoint `urn:li:dashboard:(test,zdu-rt-{i})` namespace, then reads them back. The persisted `schemaVersion` should equal the post-mutator-chain target (default v4).

Probe results are surfaced in the JSON report under `phases[*].details` with counters: `read_probes`, `read_passed`, `write_probes`, `write_passed`. The probe-level detail (per-URN observed/expected versions) lives on `ctx.runtime_migration` and is consumed by Phase 10.
```

- [ ] **Step 2: Update Phase 10 (Validation) description for Dimension 5**

Find the "Phases" overview table or any existing Phase 10 / Validation discussion. If a `## Phase 10 / Validation` section exists, ensure it lists Dimension 5: "Alias targets and dual-write state — assertions over `ctx.upgrade_blocking.indices`, `ctx.upgrade_nonblocking.dual_write_disabled_indices`, and `ctx.rolling_restart.dual_write_start_times`. Failures are emitted as individual `ValidationResult` records."

If no explicit dimensions list exists, skip — the per-record detail in the JSON report is sufficient documentation.

- [ ] **Step 3: Verify section ordering**

```bash
grep -nE "^##|^###" smoke-test/tests/zdu/README.md | head -20
```

`### Phase 9: Runtime Migration` should appear AFTER `### Phase 8: Upgrade Non-Blocking`.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Phase 9 RuntimeMigration + Validation Dimension 5"
```

Re-stage if pre-commit hooks reformat.

---

## Task 6: Live integration check

**Pre-requisite:** Compose stack up.

- [ ] **Step 1: Spy-based wiring check**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies RuntimeMigrationPhase wiring + ValidationPhase Dimension 5."""
from datetime import datetime
from unittest.mock import MagicMock
from tests.zdu.framework.context import (
    IndexState,
    RollingRestartResult,
    SeededEntity,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
)
from tests.zdu.framework.datahub_client import AspectResponse
from tests.zdu.framework.phases.runtime_migration import RuntimeMigrationPhase
from tests.zdu.framework.phases.validation import ValidationPhase

# --- RuntimeMigration probe wiring ---
datahub = MagicMock()
datahub.get_aspect.return_value = AspectResponse(
    data={"renderUrl": "x"}, system_metadata={"schemaVersion": 4}
)
phase = RuntimeMigrationPhase(datahub=datahub, n_read_probes=2, n_write_probes=2)
ctx = TestContext(seeded_entities=[
    SeededEntity(
        urn=f"urn:li:dashboard:(test,zdu-tc-{i})", aspect_name="embed",
        seeded_data={}, starting_schema_version=1, expected_schema_version=4,
    ) for i in range(2)
])
phase.run(ctx)
assert ctx.runtime_migration is not None
assert ctx.runtime_migration.passed_read_count == 2
assert ctx.runtime_migration.passed_write_count == 2
print("RUNTIME-MIGRATION OK")

# --- Validation Dimension 5 ---
ctx2 = TestContext()
ctx2.upgrade_blocking = UpgradeBlockingResult(
    indices=[
        IndexState(
            alias="dashboardindex_v2",
            old_backing_index_name="dashboardindex_v2_old",
            next_index_name="dashboardindex_v2_new",
            source_doc_count=100, status="COMPLETED",
        )
    ]
)
ctx2.upgrade_nonblocking = UpgradeNonBlockingResult(
    indices=ctx2.upgrade_blocking.indices,
    dual_write_disabled_indices=["dashboardindex_v2_old"],
)
ctx2.rolling_restart = RollingRestartResult(
    services_restarted=["datahub-gms-debug"],
    dual_write_start_times={"dashboardindex_v2_old": 1700000000},
)
results = ValidationPhase._check_dual_write_state(ctx2)
fails = [r for r in results if r.status == "FAIL"]
print(f"validation results: {len(results)} (FAIL: {len(fails)})")
assert len(fails) == 0
print("VALIDATION-D5 OK")
PY
```

Expected: ends with `RUNTIME-MIGRATION OK` and `VALIDATION-D5 OK`.

- [ ] **Step 2: Suite A regression check**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,upgrade_nonblocking,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -35
```

Pipeline: `discovery → seed → snapshot_t0 → upgrade_blocking → inject_traffic_pre → inject_traffic_dual → runtime_migration → validation`. Both `upgrade_nonblocking` and `rolling_restart` are skipped (dev-stack constraints from Plans 6/7).

Expected: same baseline 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP. The new `runtime_migration` phase should run (~0.5s) and pass; the new validation Dimension 5 check should emit one informational SKIP record (no DUAL_WRITE_DISABLED markings since `upgrade_nonblocking` was skipped).

- [ ] **Step 3: Inspect the JSON report**

```bash
python3 -c "
import json
data = json.load(open('<REPO_ROOT>/smoke-test/smoke-test/build/zdu-test-report.json'))
for p in data.get('phases', []):
    if p.get('name') in ('runtime_migration', 'validation'):
        print(p.get('name'), ':', p.get('status'), p.get('details'))
"
```

Verify `runtime_migration` shows positive `read_passed` / `write_passed` counts and that `validation.details.failures` does NOT contain a Dimension 5 false positive (it may contain the informational SKIP, which is expected on dev).

- [ ] **Step 4: Cleanup runtime probe URNs**

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-rt-%)';
" 2>&1 | tail -3
```

- [ ] **Step 5: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 8 wiring"
```

If nothing regressed, no commit needed.

---

## Task 7: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff ca4b1d450a..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-8.diff
wc -l /tmp/zdu-plan-8.diff
```

(`ca4b1d450a` is the last Plan 7 commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-8.diff`. This PR adds Phase 9 `RuntimeMigrationPhase` and extends `ValidationPhase` with Dimension 5 (alias / dual-write state) checks.
>
> Concretely:
>
> - `RuntimeMigrationProbe` and `RuntimeMigrationResult` dataclasses on `TestContext` (placed between `UpgradeNonBlockingResult` and `RollingRestartResult` in dataclass order; ctx slot under `# RuntimeMigrationPhase writes`).
> - `RuntimeMigrationPhase` — runs N read probes against `seeded_entities[:N]` and N write probes against `urn:li:dashboard:(test,zdu-rt-{i})`; always reports phase `passed`; failures recorded at probe level.
> - 5 unit tests covering happy path, mismatch capture, ingest failure, no-seeded-entities edge case.
> - `ValidationPhase._check_dual_write_state` static method walking 5 invariants (alias→next_index_name presence, disabled-index sanity, rolling-restart dual-write start-time presence, blocking↔nonblocking alias-set equality, informational SKIP on empty disabled set). 6 unit tests.
> - Runner wires `RuntimeMigrationPhase` between `upgrade_nonblocking` and `validation`.
> - README updated.
>
> Check specifically:
>
> 1. **Failure-mode classification:** `RuntimeMigrationPhase` always returns `passed` — probe-level errors recorded but never raised. Phase 10 makes the verdict. Confirm this is documented in the docstring AND consistent with design doc §5.10 "validation does not fail fast".
> 2. **Probe sample-clamping:** `_do_read_probes` takes `seeded[:n_read_probes]` — if fewer entities are seeded than `n_read_probes`, the slice gracefully returns whatever is available. Confirm test coverage of the edge case.
> 3. **Disjoint URN namespace:** write probes use `zdu-rt-{i}` — distinct from `zdu-tc-` (scenario), `zdu-io-pool-` (seed harness), `zdu-gap-` (Plan 5), `zdu-dual-` (Plan 6). No collision risk.
> 4. **`_check_dual_write_state` invariant correctness:** Each invariant produces ONE `ValidationResult` per violation. Invariants 2 and 4 use `set` operations — confirm None / missing fields are guarded (Plan 7's `dual_write_disabled_indices` defaults to empty list; Plan 4's `dual_write_start_times` defaults to empty dict).
> 5. **Skip semantics on missing captures:** if `ctx.upgrade_blocking is None` (phase was skipped), `_check_dual_write_state` returns `[]`. If `ctx.rolling_restart is None`, the dual-write-start-time invariant is skipped. Confirm tests cover both.
> 6. **Validation phase wiring:** the new dimension-5 results are appended to `results` AFTER the read-progression check, BEFORE `ctx.validation_results = results`. Confirm the phase's `passed`/`failed` counts include the new records.
> 7. **YAGNI:** No retry on probes, no parallel probes, no per-scenario probe targeting (uses `seeded_entities[:N]` blindly). All deferrals from the plan's "Out of scope" list are honoured.
> 8. **Type hints complete** — `RuntimeMigrationProbe.mode: Literal["read", "write"]`, all helper return types annotated.
> 9. **Symmetry with sibling plans:** the phase shape mirrors Plans 5/6 (dependency-injected DataHubClient, `run(ctx) -> PhaseResult`, structured-result on ctx, details summary on PhaseResult).
> 10. **Pipeline order:** `runtime_migration` between `upgrade_nonblocking` and `validation`. README ordering matches.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 8"
```

---

## Self-Review

**Spec coverage** (against design doc §5.9 Phase 9 + §5.10 Phase 10 Dimension 5):

| Requirement                                                                 | Task                                                                                   |
| --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Read pre-ZDU rows return upgraded shape via read-path mutator               | Task 2 (`_do_read_probes` records observed_version vs expected_version)                |
| Writes from clients persist as latest schema version via write-path mutator | Task 2 (`_do_write_probes` ingests + reads back)                                       |
| After chain disables, subsequent reads bypass mutators                      | OUT OF SCOPE — needs chain-disable hook capture                                        |
| Alias for each index points at expected physical index                      | Task 4 (Invariant 1: alias→next_index_name presence)                                   |
| Per-index `dualWriteStartTime` set or absent according to expectation       | Task 4 (Invariant 3: rolling_restart.dual_write_start_times completeness)              |
| Indices marked `DUAL_WRITE_DISABLED` when rollback flag is off              | Task 4 (Invariant 2: disabled-index sanity; Invariant 5: informational SKIP for empty) |
| Validation collects ALL failures (no fail-fast)                             | EXISTING — `ValidationPhase.run()` already aggregates                                  |
| Failure block has URN, expected, got, reason                                | EXISTING — `ValidationResult` shape                                                    |
| Failure artifact bundle                                                     | OUT OF SCOPE — separate plan                                                           |

**Placeholder scan:** None.

**Type / signature consistency:**

- `TestContext.runtime_migration: RuntimeMigrationResult | None` (Task 1) — populated by Task 2's `phase.run()`.
- `RuntimeMigrationPhase(datahub, n_read_probes, n_write_probes, expected_write_version, aspect_name)` (Task 2) — runner constructs (Task 3).
- `ValidationPhase._check_dual_write_state(ctx) -> list[ValidationResult]` (Task 4) — invoked by `ValidationPhase.run()` after `_check_read_progression`.
- `RuntimeMigrationProbe.mode: Literal["read", "write"]` (Task 1) — set by `_do_read_probes` ("read") and `_do_write_probes` ("write").

**Risks called out:**

1. **Read probe clamping when `seeded_entities` is short.** If the runner has fewer than `n_read_probes` entities seeded, the slice silently returns fewer probes. Test `test_no_seeded_entities_skips_read_probes` covers the empty case; intermediate cases (e.g., 2 seeded, 3 expected) are validated by the slice semantics — but not exercised explicitly. Acceptable YAGNI.
2. **Disjoint URN namespace `zdu-rt-`.** Cleanup happens in Task 6 Step 4 but NOT automatically per-run. If Suite A runs many iterations, the table accumulates ~3 rows per run. Negligible for now; future plan can add a hook.
3. **Write probe expected version is hardcoded to v4.** The `expected_write_version` constructor arg defaults to 4 — matching Suite A's most common scenario. If a future scenario expects v5+, the runner needs to thread per-scenario expectations through. Plan 9+ can codify this.
4. **`_check_dual_write_state` Invariant 4 (alias-set equality) is strict.** If the upgrade adds a NEW alias mid-run (e.g., a hypothetical extension), Invariant 4 would FAIL. Currently no such case exists; if it ever does, the invariant should soften to "blocking ⊆ nonblocking".
5. **Informational SKIP on empty disabled set.** Invariant 5 emits a SKIP record (not FAIL) on the dev-stack no-mutators case. This is intentional but means JSON-report consumers must NOT count SKIP records as test failures. Consumers that already use the existing `passed` / `failed` / `xfail` / `skipped` tally will be unaffected.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-8-runtime-migration-and-validation-extension.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Per session policy: defaulting to subagent-driven execution.
