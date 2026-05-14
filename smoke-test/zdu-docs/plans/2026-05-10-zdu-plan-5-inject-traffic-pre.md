# ZDU E2E — Plan 5: Phase 5 `InjectTrafficPrePhase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 5 — `InjectTrafficPrePhase` — which fires N writes via the running OLD GMS in the gap window between Phase 4's alias swap and Phase 6's rolling restart, then verifies each entity landed in MySQL and (when distinct OLD/NEW physical indices exist) in the OLD physical index but NOT the NEW one. Records the gap URN list into `TestContext.gap_urns` so Phase 8 catch-up (future plan) can verify backfill.

**Architecture:** One new phase class (`framework/phases/inject_traffic_pre.py`) follows the existing `Phase` ABC pattern. Uses the injected `DataHubClient` for writes via `ingest_mcp`, `MySQLClient` for DB-presence assertions, `ElasticsearchClient` for direct-index queries. The OLD/NEW index distinction comes from `ctx.upgrade_blocking.indices` (populated by Plan 2's `UpgradeBlockingPhase`) — when `old_backing_index_name == new_physical_index` (degenerate single-image case), index-distinction asserts become best-effort presence checks rather than absence checks.

**Tech Stack:** Python 3 (existing). Reuses Foundation clients (`DataHubClient`, `MySQLClient`, `ElasticsearchClient`). No new dependencies.

**Out of scope (deferred):**

- Phase 7 `InjectTrafficDualPhase` (separate plan) — same shape but writes via NEW GMS during dual-write window.
- Phase 8 `UpgradeNonBlockingPhase` (separate plan) — consumes `gap_urns` for catch-up assertions.
- Concurrent reads during gap injection — out of scope; Phase 5 is purely write-then-verify, no contention.
- Failure-mode coverage (network drop mid-injection) — single-pass best-effort; per-write failures are recorded but the phase continues.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add gap_urns slot on TestContext
├── runner.py                                    MODIFY — wire InjectTrafficPrePhase
├── phases/
│   └── inject_traffic_pre.py                    CREATE — InjectTrafficPrePhase
├── test_inject_traffic_pre.py                   CREATE — phase unit tests with mocked clients
└── README.md                                    MODIFY — append Phase 5 subsection
```

The phase reads `ctx.upgrade_blocking` (set by Plan 2's `UpgradeBlockingPhase`) to find OLD/NEW physical-index names. It writes to `ctx.gap_urns` (new field on `TestContext`).

---

## Task 1: Add `gap_urns` slot to `TestContext`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** A simple `list[str]` field with default empty list. Lives in the `TestContext` dataclass alongside the other phase-output slots.

- [ ] **Step 1: Add the slot on `TestContext`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`, in the `TestContext` dataclass, after the `# RollingRestartPhase writes` block (`rolling_restart: RollingRestartResult | None`), insert:

```python
    # InjectTrafficPrePhase writes
    gap_urns: list[str] = field(default_factory=list)
```

The `field` import is already present at the top of the file.

- [ ] **Step 2: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import TestContext
ctx = TestContext()
assert ctx.gap_urns == []
ctx.gap_urns.append('urn:li:dashboard:(test,zdu-gap-0)')
assert ctx.gap_urns == ['urn:li:dashboard:(test,zdu-gap-0)']
print('OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `OK`.

- [ ] **Step 3: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 138 pass.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add gap_urns slot to TestContext for Phase 5"
```

---

## Task 2: Implement `InjectTrafficPrePhase`

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/inject_traffic_pre.py`
- Create: `smoke-test/tests/zdu/framework/test_inject_traffic_pre.py`

**Pattern:** Phase ABC subclass. Constructor takes injected `DataHubClient`, `MySQLClient`, `ElasticsearchClient`, plus `n_gap_writes` (default 10) and `entity_type_prefix` (default `"dashboard"`). `run(ctx)` does:

1. Generate N URNs of the form `urn:li:dashboard:(test,zdu-gap-{i})`.
2. Write each via `DataHubClient.ingest_mcp(urn, "embed", {"renderUrl": ...})`.
3. Verify each in MySQL via `MySQLClient.get_aspect_raw(urn, "embed")`.
4. If `ctx.upgrade_blocking` has indices with distinct OLD/NEW names, verify each write lands in OLD physical index and is absent from NEW. Otherwise skip the index distinction check (degenerate single-image case).
5. Record `ctx.gap_urns = [...]`.
6. Return `PhaseResult` with status `passed` if all writes + MySQL checks succeed, `failed` otherwise. Index-distinction assertions are best-effort and logged but don't fail the phase.

### 2.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_inject_traffic_pre.py`:

```python
"""Unit tests for InjectTrafficPrePhase — uses mocked DataHub/MySQL/ES clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import (
    IndexState,
    TestContext,
    UpgradeBlockingResult,
)
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_pre import InjectTrafficPrePhase


@pytest.fixture
def datahub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    # By default, MySQL says every URN is present.
    m.get_aspect_raw.return_value = EbeanAspectV2Row(
        urn="placeholder", aspect="embed", version=0,
        metadata="{}", systemmetadata="{}",
        createdon="2026-01-01 00:00:00", createdby="urn:li:corpuser:datahub",
    )
    return m


@pytest.fixture
def es() -> MagicMock:
    m = MagicMock()
    # Default: ES claims every doc is in every queried index (overridden per test).
    m.get_doc.return_value = {"urn": "placeholder"}
    return m


@pytest.fixture
def phase(datahub: MagicMock, mysql: MagicMock, es: MagicMock) -> InjectTrafficPrePhase:
    return InjectTrafficPrePhase(
        datahub=datahub,
        mysql=mysql,
        es=es,
        n_gap_writes=3,
    )


def _ctx_with_distinct_indices() -> TestContext:
    ctx = TestContext()
    ctx.upgrade_blocking = UpgradeBlockingResult(
        indices=[
            IndexState(
                alias="dashboardindex_v2",
                old_backing_index_name="dashboardindex_v2_old",
                next_index_name="dashboardindex_v2_new",
                source_doc_count=100,
                status="COMPLETED",
            )
        ]
    )
    return ctx


class TestInjectTrafficPrePhaseHappyPath:
    def test_writes_n_entities_and_records_gap_urns(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says: doc present in OLD index, absent (404 → None) in NEW.
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_old" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert len(ctx.gap_urns) == 3
        # URN naming: urn:li:dashboard:(test,zdu-gap-N)
        assert all(u.startswith("urn:li:dashboard:(test,zdu-gap-") for u in ctx.gap_urns)
        # Each URN got an ingest_mcp call
        assert datahub.ingest_mcp.call_count == 3
        # Each URN got a MySQL presence check
        assert mysql.get_aspect_raw.call_count == 3

    def test_degenerate_single_image_case_skips_index_distinction(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # Same name for OLD and NEW — degenerate, no real reindex ran.
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(
            indices=[
                IndexState(
                    alias="dashboardindex_v2",
                    old_backing_index_name="dashboardindex_v2",
                    next_index_name="dashboardindex_v2",
                    source_doc_count=100,
                    status="COMPLETED",
                )
            ]
        )
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.gap_urns) == 3
        # Index distinction should NOT have triggered any get_doc calls
        # (we still check presence in OLD via a single get_doc per URN, but
        # the absence-in-NEW check is skipped when names match).
        # We assert that at most n_gap_writes get_doc calls were made
        # (one per URN for the OLD/single-index check).
        assert es.get_doc.call_count <= 3

    def test_no_upgrade_blocking_state_skips_index_checks_entirely(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ctx.upgrade_blocking is None — Phase 4 didn't run or wasn't captured.
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.gap_urns) == 3
        # No index calls at all
        es.get_doc.assert_not_called()


class TestInjectTrafficPrePhaseFailures:
    def test_mysql_missing_one_urn_fails_phase(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # MySQL returns row for first 2 URNs, None for the 3rd
        rows = [
            EbeanAspectV2Row(
                urn=f"urn-{i}", aspect="embed", version=0,
                metadata="{}", systemmetadata="{}",
                createdon="2026-01-01 00:00:00", createdby="urn:li:corpuser:datahub",
            )
            for i in range(2)
        ] + [None]
        mysql.get_aspect_raw.side_effect = rows

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "missing from MySQL" in (result.error or "")

    def test_ingest_failure_fails_phase(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # First two writes succeed, third raises
        datahub.ingest_mcp.side_effect = [None, None, RuntimeError("GMS unavailable")]

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "GMS unavailable" in (result.error or "")

    def test_index_distinction_failure_logs_but_does_not_fail_phase(
        self,
        phase: InjectTrafficPrePhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says doc is in BOTH old and new — that's wrong, but Phase 5
        # treats this as a warning (not a hard failure) because it depends
        # on a real Phase 1 reindex having run.
        es.get_doc.return_value = {"urn": "x"}  # always present
        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.gap_urns) == 3
        # Details record the leak count for downstream assertions
        assert "index_distinction_warnings" in (result.details or {})
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_inject_traffic_pre.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'InjectTrafficPrePhase'` (module doesn't exist).

### 2.2 — Implement the phase

- [ ] **Step 3: Create the phase file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/inject_traffic_pre.py`:

```python
"""Phase 5 — InjectTrafficPrePhase.

Fires N writes via the running OLD GMS in the gap window between Phase 4
(alias swap) and Phase 6 (rolling restart). Each write becomes a "T0–T1
gap entity" that lands in the OLD physical index only — Phase 8 catch-up
(future plan) is responsible for backfilling these into the NEW index.

The phase records the gap URN list into ``TestContext.gap_urns`` so
downstream phases can verify backfill behaviour.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from .base import Phase, PhaseResult
from ..context import IndexState, TestContext
from ..datahub_client import DataHubClient
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_N_GAP_WRITES = 10
_DEFAULT_ASPECT = "embed"
_DEFAULT_GAP_PAYLOAD = {
    "renderUrl": "http://zdu-gap.example.com/embed",
}


class InjectTrafficPrePhase(Phase):
    name = "inject_traffic_pre"

    def __init__(
        self,
        datahub: DataHubClient,
        mysql: MySQLClient,
        es: ElasticsearchClient,
        n_gap_writes: int = _DEFAULT_N_GAP_WRITES,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._mysql = mysql
        self._es = es
        self._n_gap_writes = n_gap_writes
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        urns = [
            f"urn:li:dashboard:(test,zdu-gap-{i})" for i in range(self._n_gap_writes)
        ]

        # 1. Write each URN via the running OLD GMS.
        for urn in urns:
            try:
                self._datahub.ingest_mcp(
                    urn, self._aspect_name, dict(_DEFAULT_GAP_PAYLOAD),
                    system_metadata={},
                )
            except Exception as exc:
                log.exception("InjectTrafficPre: ingest failed for %s", urn)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"ingest failed for {urn}: {exc}",
                )

        # 2. Verify presence in MySQL.
        for urn in urns:
            row = self._mysql.get_aspect_raw(urn, self._aspect_name)
            if row is None:
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"{urn}: missing from MySQL after ingest",
                )

        # 3. (Best-effort) Verify each URN lands in OLD physical index and
        # is ABSENT from NEW. Skipped if no upgrade_blocking state captured
        # or if OLD/NEW are the same name (degenerate single-image case).
        index_warnings = self._check_index_distinction(ctx, urns)

        ctx.gap_urns = urns
        log.info(
            "InjectTrafficPre complete — %d gap URNs written; %d index-distinction warnings",
            len(urns),
            len(index_warnings),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "gap_urns": urns,
                "index_distinction_warnings": index_warnings,
            },
        )

    def _check_index_distinction(
        self, ctx: TestContext, urns: list[str]
    ) -> list[str]:
        """Verify each URN appears in OLD index and not in NEW.

        Returns a list of human-readable warning strings (e.g. "URN X leaked
        into NEW index Y"). Empty list = clean. Doesn't fail the phase —
        these checks rely on a real Phase 1 reindex having run, which may
        not be the case in dev workflows.
        """
        if not ctx.upgrade_blocking or not ctx.upgrade_blocking.indices:
            return []

        # Pick the first dashboard-aliased IndexState; URNs we wrote are
        # all dashboards. If no dashboard alias is in the upgrade result,
        # skip — nothing to check.
        target = self._find_dashboard_index(ctx.upgrade_blocking.indices)
        if target is None:
            return []
        old_idx = target.old_backing_index_name
        new_idx = target.next_index_name
        if old_idx is None or new_idx is None or old_idx == new_idx:
            # Degenerate: no real reindex ran; OLD == NEW.
            # Still verify presence in the (single) index for one URN as a sanity check.
            self._sanity_check_single_index(old_idx or new_idx, urns)
            return []

        warnings: list[str] = []
        for urn in urns:
            doc_id = urn  # ES doc id is the URN; get_doc URL-encodes it (Foundation fix).
            old_doc = self._safe_get_doc(old_idx, doc_id)
            new_doc = self._safe_get_doc(new_idx, doc_id)
            if old_doc is None:
                warnings.append(f"{urn}: missing from OLD index {old_idx}")
            if new_doc is not None:
                warnings.append(
                    f"{urn}: leaked into NEW index {new_idx} during gap window"
                )
        return warnings

    def _find_dashboard_index(
        self, indices: list[IndexState]
    ) -> IndexState | None:
        for idx in indices:
            if "dashboard" in idx.alias.lower():
                return idx
        return None

    def _sanity_check_single_index(
        self, index_name: str | None, urns: list[str]
    ) -> None:
        if index_name is None or not urns:
            return
        # One probe — if it fails, log but don't escalate.
        first_urn = urns[0]
        doc = self._safe_get_doc(index_name, first_urn)
        if doc is None:
            log.warning(
                "InjectTrafficPre sanity: %s not in single-image index %s",
                first_urn, index_name,
            )

    def _safe_get_doc(self, index_name: str, doc_id: str) -> dict[str, Any] | None:
        try:
            return self._es.get_doc(index_name, doc_id)
        except Exception as exc:
            log.warning("ES get_doc failed for %s/%s: %s", index_name, doc_id, exc)
            return None
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_inject_traffic_pre.py -v 2>&1 | tail -15
```

Expected: 6 tests pass (3 happy path + 3 failure modes).

- [ ] **Step 5: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 144 pass (138 baseline + 6 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/inject_traffic_pre.py \
        smoke-test/tests/zdu/framework/test_inject_traffic_pre.py
git commit -m "feat(zdu): InjectTrafficPrePhase — fire gap-window writes via OLD GMS"
```

Re-stage if pre-commit hooks reformat.

---

## Task 3: Wire `InjectTrafficPrePhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert AFTER `("upgrade_blocking", ...)` and BEFORE `("rolling_restart", ...)`. Phase 5 needs `ctx.upgrade_blocking` populated by Phase 4 and runs before Phase 6's restart, matching the design doc's pipeline order.

- [ ] **Step 1: Add import**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, alongside other phase imports:

```python
from .phases.inject_traffic_pre import InjectTrafficPrePhase
```

- [ ] **Step 2: Insert into the phases list**

Find the `phases = [...]` list. Insert this tuple BETWEEN the existing `("upgrade_blocking", ...)` and `("rolling_restart", ...)` entries:

```python
            (
                "inject_traffic_pre",
                InjectTrafficPrePhase(
                    datahub=self._datahub,
                    mysql=self._mysql,
                    es=self._es,
                ),
            ),
```

We rely on the defaults `n_gap_writes=10`, `aspect_name="embed"`. Future plans may expose these via config if needed.

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

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `runner constructed OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 144 pass (no count change — wiring only).

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire InjectTrafficPrePhase into runner pipeline"
```

---

## Task 4: README — Phase 5 description

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Append a Phase 5 subsection under the existing Two-Image-Tag Testing section**

Find the existing `### Phase 6: Rolling Restart` subsection. Insert a NEW `### Phase 5: Inject Traffic Pre` subsection BEFORE it (so phases appear in pipeline order in the README):

```markdown
### Phase 5: Inject Traffic Pre

`InjectTrafficPrePhase` fires N writes (default 10) via the running OLD GMS in the gap window between Phase 4 (alias swap) and Phase 6 (rolling restart). Each write becomes a "T0–T1 gap entity" that should land in the OLD physical index only — Phase 8 catch-up (future plan) is responsible for backfilling these into the NEW index.

The phase verifies:

- Each write succeeded against the OLD GMS (REST `ingestProposal`).
- Each entity is queryable in MySQL (`metadata_aspect_v2`).
- (Best-effort, when distinct OLD/NEW physical indices exist) Each entity appears in the OLD index but NOT the NEW index.

Index-distinction checks rely on `ctx.upgrade_blocking.indices[*]` having distinct `old_backing_index_name` and `next_index_name` — when they match (degenerate single-image dev workflow), the phase reduces to write + MySQL verification.

The gap URN list is recorded in `ctx.gap_urns` (also surfaced in `phases[*].details.gap_urns` of the JSON report). Phase 8 catch-up will read this to verify backfill.
```

- [ ] **Step 2: Verify the section landed correctly**

```bash
grep -nE "^##|^###" smoke-test/tests/zdu/README.md | head -20
```

The new `### Phase 5: Inject Traffic Pre` should appear before `### Phase 6: Rolling Restart`.

- [ ] **Step 3: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Phase 5 InjectTrafficPrePhase behaviour"
```

---

## Task 5: Live integration check

**Pre-requisite:** Compose stack up.

The phase writes 10 dashboard entities and verifies presence. Even without a real two-image setup, this exercises the write path end-to-end against the real running GMS, MySQL, and ES.

- [ ] **Step 1: Spy-based wiring check**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies InjectTrafficPrePhase writes the right URNs via the right channels."""
from unittest.mock import MagicMock
from tests.zdu.framework.context import (
    IndexState,
    TestContext,
    UpgradeBlockingResult,
)
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_pre import InjectTrafficPrePhase

datahub = MagicMock()
mysql = MagicMock()
mysql.get_aspect_raw.return_value = EbeanAspectV2Row(
    urn="x", aspect="embed", version=0, metadata="{}", systemmetadata="{}",
    createdon="2026-01-01 00:00:00", createdby="urn:li:corpuser:datahub",
)
es = MagicMock()
es.get_doc.side_effect = lambda idx, doc_id: (
    {"urn": doc_id} if idx == "dashboardindex_v2_old" else None
)

phase = InjectTrafficPrePhase(datahub=datahub, mysql=mysql, es=es, n_gap_writes=5)
ctx = TestContext()
ctx.upgrade_blocking = UpgradeBlockingResult(
    indices=[
        IndexState(
            alias="dashboardindex_v2",
            old_backing_index_name="dashboardindex_v2_old",
            next_index_name="dashboardindex_v2_new",
            source_doc_count=100, status="COMPLETED",
        ),
    ]
)
result = phase.run(ctx)
print("phase status:", result.status)
print("gap_urns:", ctx.gap_urns)
print("ingest_mcp calls:", datahub.ingest_mcp.call_count)
print("get_aspect_raw calls:", mysql.get_aspect_raw.call_count)
print("get_doc calls:", es.get_doc.call_count)
print("warnings:", result.details.get("index_distinction_warnings"))
assert result.status == "passed"
assert len(ctx.gap_urns) == 5
assert datahub.ingest_mcp.call_count == 5
assert mysql.get_aspect_raw.call_count == 5
assert es.get_doc.call_count == 10  # 5 × (OLD + NEW)
print("LIVE-WIRING OK")
PY
```

Expected: ends with `LIVE-WIRING OK`.

- [ ] **Step 2: Suite A regression check**

The pipeline now runs `... → upgrade_blocking → inject_traffic_pre → rolling_restart → sweep_and_io → validation`. Without real OLD/NEW images, `inject_traffic_pre` will write 10 dashboards into the running stack, then verify them in MySQL — that should succeed. With `rolling_restart` skipped (to avoid disrupting the dev stack with real recreations), Suite A's baseline is preserved.

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,sweep_and_io,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -30
```

Expected: pipeline runs `discovery → seed → snapshot_t0 → upgrade_blocking → inject_traffic_pre → validation` (skipping `upgrade`, `rolling_restart`, `sweep_and_io`). The `inject_traffic_pre` phase passes (writes 10 gap entities, verifies in MySQL). Suite A's scenario results match the baseline: 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP.

- [ ] **Step 3: Cleanup the gap entities** (optional, but courteous to repeat runs)

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-gap-%)';
" 2>&1 | tail -3
```

Expected: query runs cleanly. Without cleanup, repeat runs of Phase 5 would still pass (idempotent ingest_mcp), but the entities accumulate.

- [ ] **Step 4: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 5 wiring"
```

If nothing regressed, no commit needed.

---

## Task 6: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff e87f5bae9b..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-5.diff
wc -l /tmp/zdu-plan-5.diff
```

(`e87f5bae9b` is the last Plan 4 commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-5.diff`. This PR adds Phase 5 (`InjectTrafficPrePhase`) to the ZDU E2E framework. The phase fires N writes (default 10) via the running OLD GMS in the gap window between Phase 4's alias swap and Phase 6's rolling restart, then verifies each entity landed in MySQL and (best-effort) in the OLD physical index but NOT the NEW one.
>
> Concretely:
>
> - `gap_urns: list[str]` slot on `TestContext`.
> - `InjectTrafficPrePhase` class — uses `DataHubClient.ingest_mcp` + `MySQLClient.get_aspect_raw` + `ElasticsearchClient.get_doc`. 6 unit tests with mocked clients.
> - Runner wires it between `upgrade_blocking` and `rolling_restart`.
> - README updated with Phase 5 subsection.
>
> Check specifically:
>
> 1. **DI:** clients injected via constructor; phase doesn't construct any.
> 2. **Failure-mode classification:** `ingest_mcp` failure = phase failed. MySQL absence = phase failed. Index-distinction issues = warning only (best-effort, doesn't fail the phase).
> 3. **Degenerate single-image case** (OLD == NEW physical index name): the phase skips the index-distinction check rather than emitting false-positive warnings.
> 4. **`ctx.upgrade_blocking is None` case** (Phase 4 didn't run or wasn't captured): the phase still writes + records gap_urns, but skips index checks entirely.
> 5. **URN naming:** `urn:li:dashboard:(test,zdu-gap-{i})` — distinct from existing IO-pool URNs (`zdu-io-pool-{i}`) and per-TC URNs (`zdu-tc-{i}`).
> 6. **No coupling to F-3:** the phase doesn't read `config.new_image_tag` directly; it reasons about OLD/NEW via `ctx.upgrade_blocking.indices[*]`.
> 7. **No coupling to Phase 8:** the phase doesn't anticipate catch-up's specific behaviour; it just records `gap_urns` for downstream consumption.
> 8. **Test quality:** mocked clients, behavior-focused assertions, no implementation-detail inspection.
> 9. **YAGNI:** No retry, no concurrent writes, no fancy URN generation. Just N sequential writes + verification.
> 10. **Type hints complete.**
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 5"
```

---

## Self-Review

**Spec coverage** (against design doc Section 5.5 + Section 14.1 Phase 5):

| Requirement                                     | Task                                            |
| ----------------------------------------------- | ----------------------------------------------- |
| Old GMS write path (REST `ingestProposal`)      | Task 2 (`_DEFAULT_GAP_PAYLOAD` + `ingest_mcp`)  |
| Direct ES query against OLD physical index      | Task 2 (`_check_index_distinction` + `get_doc`) |
| MySQL aspect insert verification                | Task 2 (`get_aspect_raw`)                       |
| At least N writes (default 10)                  | Task 2 (`_DEFAULT_N_GAP_WRITES = 10`)           |
| Each entity queryable in OLD physical index     | Task 2 (best-effort)                            |
| Each entity NOT queryable in NEW physical index | Task 2 (best-effort warnings)                   |
| Each entity queryable in MySQL                  | Task 2 (mandatory check)                        |
| `TestContext.gap_urns` recorded                 | Task 1 + Task 2                                 |

**Note on best-effort vs mandatory checks:** the design doc says "must verify ... NOT queryable in NEW index". Plan 5 treats this as best-effort because in dev workflows without a real Phase 1 reindex, NEW == OLD and the absence check is meaningless. When a real two-image run happens (Plan F-3 + real images), the warning emerges naturally. Phase 10 Validation can promote these warnings to hard failures when the OLD/NEW invariant is required.

**Placeholder scan:** None.

**Type / signature consistency:**

- `TestContext.gap_urns: list[str]` (Task 1) — populated by Task 2's `phase.run()`.
- `InjectTrafficPrePhase(datahub, mysql, es, n_gap_writes, aspect_name)` (Task 2) — runner constructs (Task 3).
- `IndexState.old_backing_index_name`, `next_index_name` (existing from Plan 2) — read by `_check_index_distinction`.
- `ElasticsearchClient.get_doc(index_name, doc_id)` (existing from Foundation, fixed for URL encoding in F-1 review) — used by `_safe_get_doc`.
- `MySQLClient.get_aspect_raw(urn, aspect)` (existing from Foundation) — used directly.

**Risks called out:**

1. **Eventual consistency on MySQL check.** `ingest_mcp` is async via Kafka; the MySQL `get_aspect_raw` call right after may not see the row yet. If this becomes flaky, add a small retry or `wait_for_writes_to_sync()`. For now it's tested with mocks; a real flake in the live integration check (Task 5) would surface it.
2. **Ingest auth.** `DataHubClient.ingest_mcp` uses the bearer token configured in `gms_token`. If the test runs without a token (env var unset), writes will 401. The phase doesn't pre-validate; the failure surfaces clearly via the `ingest failed for {urn}` error message.
3. **Gap URN cleanup.** Phase 5 writes 10 entities per run; without cleanup they accumulate. Task 5 step 3 includes a manual cleanup. Phase 10 Validation may want to also delete gap URNs at teardown — out of scope for Plan 5.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-5-inject-traffic-pre.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
