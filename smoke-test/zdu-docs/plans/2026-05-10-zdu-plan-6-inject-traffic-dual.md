# ZDU E2E — Plan 6: Phase 7 `InjectTrafficDualPhase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 7 — `InjectTrafficDualPhase` — which fires N writes via the running NEW GMS (post-rolling-restart) during the dual-write window and verifies each entity appears in BOTH the OLD and NEW physical indices. Records the dual-write URN list into `TestContext.dual_write_urns`. Symmetric to Plan 5's `InjectTrafficPrePhase` but flips the index-presence assertion from "OLD only" to "BOTH OLD AND NEW".

**Architecture:** One new phase class (`framework/phases/inject_traffic_dual.py`) follows the same pattern as Plan 5. Constructor takes injected `DataHubClient`, `MySQLClient`, `ElasticsearchClient`, plus `n_dual_writes` (default 10). `run(ctx)` writes N URNs via `ingest_mcp`, verifies each in MySQL, then (best-effort) verifies each appears in BOTH the OLD physical index (rollback safety net) and the NEW physical index (primary destination). Index distinction comes from `ctx.upgrade_blocking.indices` (Plan 2).

**Tech Stack:** Python 3 (existing). Reuses Foundation clients. No new dependencies.

**Out of scope (deferred):**

- TC-204 (rollback flag off → NEW-only) — requires a config knob to flip rollback behavior at runtime; not in first release.
- TC-205 (dualWriteStartTime set once per index) — already captured in `ctx.rolling_restart.dual_write_start_times` by Plan 4; Phase 10 Validation will assert single-write semantics.
- TC-208 (deletes propagate to both indices) — separate test scenario; out of scope.
- Document content byte-for-byte comparison — best-effort presence check only in this plan; content equality is Phase 10 Validation's responsibility.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add dual_write_urns slot on TestContext
├── runner.py                                    MODIFY — wire InjectTrafficDualPhase
├── phases/
│   └── inject_traffic_dual.py                   CREATE — InjectTrafficDualPhase
├── test_inject_traffic_dual.py                  CREATE — phase unit tests with mocked clients
└── README.md                                    MODIFY — append Phase 7 subsection
```

The phase reads `ctx.upgrade_blocking` (set by Plan 2's `UpgradeBlockingPhase`) for OLD/NEW physical-index names. It writes to `ctx.dual_write_urns` (new field on `TestContext`).

---

## Task 1: Add `dual_write_urns` slot to `TestContext`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Same shape as Plan 5's `gap_urns` — a `list[str]` field with default empty. Pipeline-order placement: AFTER `# RollingRestartPhase writes` (Phase 6) and BEFORE `# ValidationPhase writes` (Phase 10).

- [ ] **Step 1: Add the slot**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`, find the `TestContext` dataclass. Locate the `# RollingRestartPhase writes` block (which has `rolling_restart: RollingRestartResult | None = None`). Insert AFTER it, BEFORE `# ValidationPhase writes`:

```python
    # InjectTrafficDualPhase writes
    dual_write_urns: list[str] = field(default_factory=list)
```

The `field` import is already present at the top of the file.

- [ ] **Step 2: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import TestContext
ctx = TestContext()
assert ctx.dual_write_urns == []
ctx.dual_write_urns.append('urn:li:dashboard:(test,zdu-dual-0)')
assert ctx.dual_write_urns == ['urn:li:dashboard:(test,zdu-dual-0)']
print('OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `OK`.

- [ ] **Step 3: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 146 pass.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add dual_write_urns slot to TestContext for Phase 7"
```

Re-stage if pre-commit hooks reformat.

---

## Task 2: Implement `InjectTrafficDualPhase`

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/inject_traffic_dual.py`
- Create: `smoke-test/tests/zdu/framework/test_inject_traffic_dual.py`

**Pattern:** Same shape as `InjectTrafficPrePhase`. Differences:

- URN namespace: `urn:li:dashboard:(test,zdu-dual-{i})`
- Index check: BOTH OLD and NEW must have the doc (vs Plan 5's OLD-yes, NEW-no).
- On hard failure (ingest or MySQL): record partial `dual_write_urns` list (same pattern Plan 5 v2 introduced).

### 2.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_inject_traffic_dual.py`:

```python
"""Unit tests for InjectTrafficDualPhase — uses mocked DataHub/MySQL/ES clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import (
    IndexState,
    TestContext,
    UpgradeBlockingResult,
)
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_dual import InjectTrafficDualPhase


@pytest.fixture
def datahub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.get_aspect_raw.return_value = EbeanAspectV2Row(
        urn="placeholder", aspect="embed", version=0,
        metadata="{}", systemmetadata="{}",
        createdon="2026-01-01 00:00:00", createdby="urn:li:corpuser:datahub",
    )
    return m


@pytest.fixture
def es() -> MagicMock:
    m = MagicMock()
    m.get_doc.return_value = {"urn": "placeholder"}
    return m


@pytest.fixture
def phase(datahub: MagicMock, mysql: MagicMock, es: MagicMock) -> InjectTrafficDualPhase:
    return InjectTrafficDualPhase(
        datahub=datahub,
        mysql=mysql,
        es=es,
        n_dual_writes=3,
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


class TestInjectTrafficDualPhaseHappyPath:
    def test_writes_n_entities_and_records_dual_write_urns(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES returns the doc in BOTH old and new indices (correct dual-write).
        es.get_doc.return_value = {"urn": "x"}

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert len(ctx.dual_write_urns) == 3
        assert all(u.startswith("urn:li:dashboard:(test,zdu-dual-") for u in ctx.dual_write_urns)
        assert datahub.ingest_mcp.call_count == 3
        assert mysql.get_aspect_raw.call_count == 3
        # 3 URNs × 2 indices (old + new) = 6 get_doc calls
        assert es.get_doc.call_count == 6
        # No warnings — both indices have the doc
        assert result.details.get("dual_write_warnings") == []

    def test_old_index_missing_doc_emits_warning(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says doc is in NEW only, NOT in OLD — dual-write fan-out failed.
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_new" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed"  # warnings don't fail the phase
        warnings = result.details.get("dual_write_warnings") or []
        assert len(warnings) == 3  # one per URN
        assert all("missing from OLD index" in w for w in warnings)

    def test_new_index_missing_doc_emits_warning(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # ES says doc is in OLD only, NOT in NEW — dual-write fan-out failed.
        es.get_doc.side_effect = lambda idx, doc_id: (
            {"urn": doc_id} if idx == "dashboardindex_v2_old" else None
        )

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)

        assert result.status == "passed"
        warnings = result.details.get("dual_write_warnings") or []
        assert len(warnings) == 3
        assert all("missing from NEW index" in w for w in warnings)

    def test_degenerate_single_image_case_skips_index_distinction(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
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
        assert len(ctx.dual_write_urns) == 3
        # Index distinction skipped — at most n_dual_writes get_doc calls.
        assert es.get_doc.call_count <= 3

    def test_no_upgrade_blocking_state_skips_index_checks_entirely(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert len(ctx.dual_write_urns) == 3
        es.get_doc.assert_not_called()


class TestInjectTrafficDualPhaseFailures:
    def test_mysql_missing_one_urn_fails_phase(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
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
        # Records full ingested list on MySQL failure (all 3 ingests succeeded)
        assert len(ctx.dual_write_urns) == 3

    def test_ingest_failure_fails_phase_with_partial_dual_write_urns(
        self,
        phase: InjectTrafficDualPhase,
        datahub: MagicMock,
        mysql: MagicMock,
        es: MagicMock,
    ) -> None:
        # First 2 ingests succeed, third raises
        datahub.ingest_mcp.side_effect = [None, None, RuntimeError("GMS unavailable")]

        ctx = _ctx_with_distinct_indices()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "GMS unavailable" in (result.error or "")
        # Partial list: 2 successful ingests recorded
        assert len(ctx.dual_write_urns) == 2
        assert ctx.dual_write_urns[0] == "urn:li:dashboard:(test,zdu-dual-0)"
        assert ctx.dual_write_urns[1] == "urn:li:dashboard:(test,zdu-dual-1)"
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_inject_traffic_dual.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'InjectTrafficDualPhase'`.

### 2.2 — Implement the phase

- [ ] **Step 3: Create the phase file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/inject_traffic_dual.py`:

```python
"""Phase 7 — InjectTrafficDualPhase.

Fires N writes via the running NEW GMS (post-rolling-restart) during the
dual-write window. Each write should appear in BOTH the OLD physical
index (rollback safety net) and the NEW physical index (primary).

The phase records the dual-write URN list into
``TestContext.dual_write_urns`` so Phase 8 (UpgradeNonBlockingPhase) and
Phase 10 (Validation) can verify dual-write disable behaviour.
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

_DEFAULT_N_DUAL_WRITES = 10
_DEFAULT_ASPECT = "embed"
_DEFAULT_DUAL_PAYLOAD = {
    "renderUrl": "http://zdu-dual.example.com/embed",
}


class InjectTrafficDualPhase(Phase):
    name = "inject_traffic_dual"

    def __init__(
        self,
        datahub: DataHubClient,
        mysql: MySQLClient,
        es: ElasticsearchClient,
        n_dual_writes: int = _DEFAULT_N_DUAL_WRITES,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._mysql = mysql
        self._es = es
        self._n_dual_writes = n_dual_writes
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        urns = [
            f"urn:li:dashboard:(test,zdu-dual-{i})" for i in range(self._n_dual_writes)
        ]
        ingested: list[str] = []

        for urn in urns:
            try:
                self._datahub.ingest_mcp(
                    urn, self._aspect_name, dict(_DEFAULT_DUAL_PAYLOAD),
                    system_metadata={},
                )
            except Exception as exc:
                log.exception("InjectTrafficDual: ingest failed for %s", urn)
                # Record the partial list of successfully-ingested URNs.
                ctx.dual_write_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"ingest failed for {urn}: {exc}",
                    details={"dual_write_urns": list(ingested)},
                )
            ingested.append(urn)

        for urn in urns:
            row = self._mysql.get_aspect_raw(urn, self._aspect_name)
            if row is None:
                # Record the full ingested list — all ingests succeeded
                # before MySQL came up missing this row.
                ctx.dual_write_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"{urn}: missing from MySQL after ingest",
                    details={"dual_write_urns": list(ingested)},
                )

        warnings = self._check_dual_write_fanout(ctx, urns)

        ctx.dual_write_urns = urns
        log.info(
            "InjectTrafficDual complete — %d dual-write URNs written; %d warnings",
            len(urns),
            len(warnings),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "dual_write_urns": urns,
                "dual_write_warnings": warnings,
            },
        )

    def _check_dual_write_fanout(
        self, ctx: TestContext, urns: list[str]
    ) -> list[str]:
        """Verify each URN appears in BOTH OLD and NEW physical indices.

        Best-effort — relies on a real Phase 1 reindex having run. Returns
        warning strings; doesn't fail the phase.
        """
        if not ctx.upgrade_blocking or not ctx.upgrade_blocking.indices:
            return []

        target = self._find_dashboard_index(ctx.upgrade_blocking.indices)
        if target is None:
            return []
        old_idx = target.old_backing_index_name
        new_idx = target.next_index_name
        if old_idx is None or new_idx is None or old_idx == new_idx:
            self._sanity_check_single_index(old_idx or new_idx, urns)
            return []

        warnings: list[str] = []
        for urn in urns:
            doc_id = urn  # ES doc id is the URN; get_doc URL-encodes it.
            old_doc = self._safe_get_doc(old_idx, doc_id)
            new_doc = self._safe_get_doc(new_idx, doc_id)
            if old_doc is None:
                warnings.append(f"{urn}: missing from OLD index {old_idx}")
            if new_doc is None:
                warnings.append(f"{urn}: missing from NEW index {new_idx}")
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
        first_urn = urns[0]
        doc = self._safe_get_doc(index_name, first_urn)
        if doc is None:
            log.warning(
                "InjectTrafficDual sanity: %s not in single-image index %s",
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
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_inject_traffic_dual.py -v 2>&1 | tail -15
```

Expected: 7 tests pass (5 happy + 2 failures).

- [ ] **Step 5: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 153 pass (146 baseline + 7 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/inject_traffic_dual.py \
        smoke-test/tests/zdu/framework/test_inject_traffic_dual.py
git commit -m "feat(zdu): InjectTrafficDualPhase — fire dual-write window writes via NEW GMS"
```

Re-stage if pre-commit hooks reformat.

---

## Task 3: Wire `InjectTrafficDualPhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert AFTER `("rolling_restart", ...)` and BEFORE `("sweep_and_io", ...)`. Phase 7 needs the post-restart NEW GMS state and runs before any Phase 8 catch-up.

- [ ] **Step 1: Add import**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, alongside other phase imports:

```python
from .phases.inject_traffic_dual import InjectTrafficDualPhase
```

- [ ] **Step 2: Insert into the phases list**

In the `phases = [...]` list, insert this tuple BETWEEN the existing `("rolling_restart", ...)` entry and the `("sweep_and_io", ...)` entry:

```python
            (
                "inject_traffic_dual",
                InjectTrafficDualPhase(
                    datahub=self._datahub,
                    mysql=self._mysql,
                    es=self._es,
                ),
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

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `runner constructed OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 153 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire InjectTrafficDualPhase into runner pipeline"
```

Re-stage if pre-commit hooks reformat.

---

## Task 4: README — Phase 7 description

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Insert Phase 7 subsection AFTER Phase 6**

Find the existing `### Phase 6: Rolling Restart` subsection in the Two-Image-Tag Testing section. Append a new subsection AFTER it:

```markdown
### Phase 7: Inject Traffic Dual

`InjectTrafficDualPhase` fires N writes (default 10) via the running NEW GMS (post-rolling-restart) during the dual-write window. Each write should appear in BOTH the OLD physical index (rollback safety net for the 24h post-upgrade window) and the NEW physical index (primary destination).

The phase verifies:

- Each write succeeded against the NEW GMS (REST `ingestProposal`).
- Each entity is queryable in MySQL.
- (Best-effort, when distinct OLD/NEW physical indices exist) Each entity appears in BOTH the OLD and NEW physical indices via direct ES queries.

Index-distinction checks rely on `ctx.upgrade_blocking.indices[*]` having distinct `old_backing_index_name` and `next_index_name`. When they match (degenerate single-image dev workflow), the phase reduces to write + MySQL verification.

The dual-write URN list is recorded in `ctx.dual_write_urns` (also surfaced in `phases[*].details.dual_write_urns` of the JSON report). On hard failure (ingest or MySQL), the partial list of successfully-ingested URNs is preserved.
```

- [ ] **Step 2: Verify section landed correctly**

```bash
grep -nE "^##|^###" smoke-test/tests/zdu/README.md | head -25
```

The new `### Phase 7: Inject Traffic Dual` should appear AFTER `### Phase 6: Rolling Restart`.

- [ ] **Step 3: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Phase 7 InjectTrafficDualPhase behaviour"
```

Re-stage if pre-commit hooks reformat.

---

## Task 5: Live integration check

**Pre-requisite:** Compose stack up.

- [ ] **Step 1: Spy-based wiring check**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies InjectTrafficDualPhase wiring."""
from unittest.mock import MagicMock
from tests.zdu.framework.context import (
    IndexState,
    TestContext,
    UpgradeBlockingResult,
)
from tests.zdu.framework.mysql_client import EbeanAspectV2Row
from tests.zdu.framework.phases.inject_traffic_dual import InjectTrafficDualPhase

datahub = MagicMock()
mysql = MagicMock()
mysql.get_aspect_raw.return_value = EbeanAspectV2Row(
    urn="x", aspect="embed", version=0, metadata="{}", systemmetadata="{}",
    createdon="2026-01-01 00:00:00", createdby="urn:li:corpuser:datahub",
)
es = MagicMock()
es.get_doc.return_value = {"urn": "placeholder"}  # always present in BOTH

phase = InjectTrafficDualPhase(datahub=datahub, mysql=mysql, es=es, n_dual_writes=5)
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
print("dual_write_urns count:", len(ctx.dual_write_urns))
print("ingest_mcp calls:", datahub.ingest_mcp.call_count)
print("get_aspect_raw calls:", mysql.get_aspect_raw.call_count)
print("get_doc calls:", es.get_doc.call_count)
print("warnings:", result.details.get("dual_write_warnings"))
assert result.status == "passed"
assert len(ctx.dual_write_urns) == 5
assert datahub.ingest_mcp.call_count == 5
assert mysql.get_aspect_raw.call_count == 5
assert es.get_doc.call_count == 10  # 5 × (OLD + NEW)
assert result.details.get("dual_write_warnings") == []
print("LIVE-WIRING OK")
PY
```

Expected: ends with `LIVE-WIRING OK`.

- [ ] **Step 2: Suite A regression check**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,sweep_and_io,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -30
```

Skip `rolling_restart` (real recreations would disrupt the dev stack). Pipeline runs `discovery → seed → snapshot_t0 → upgrade_blocking → inject_traffic_pre → inject_traffic_dual → validation`.

Expected: same 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP. Both `inject_traffic_pre` and `inject_traffic_dual` write 10 dashboards each (gap + dual sets — 20 total), verify in MySQL, and report passed.

- [ ] **Step 3: Cleanup the dual URNs** (optional but courteous)

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-dual-%)';
" 2>&1 | tail -3
```

(Plan 5 already cleans up `zdu-gap-%`; this adds `zdu-dual-%`.)

- [ ] **Step 4: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 6 wiring"
```

If nothing regressed, no commit needed.

---

## Task 6: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff e0f6cb2d0b..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-6.diff
wc -l /tmp/zdu-plan-6.diff
```

(`e0f6cb2d0b` is the last Plan 5 commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-6.diff`. This PR adds Phase 7 (`InjectTrafficDualPhase`) to the ZDU E2E framework. The phase is symmetric to Plan 5's `InjectTrafficPrePhase` but flips the index-presence assertion: writes via the NEW GMS during the dual-write window must appear in BOTH the OLD and NEW physical indices.
>
> Concretely:
>
> - `dual_write_urns: list[str]` slot on `TestContext` (placed between `rolling_restart` and `validation_results` for pipeline order).
> - `InjectTrafficDualPhase` class — uses injected `DataHubClient`/`MySQLClient`/`ElasticsearchClient`. 7 unit tests (5 happy + 2 failure) with mocked clients.
> - Runner wires it between `rolling_restart` and `sweep_and_io`.
> - README updated with Phase 7 subsection.
>
> Check specifically:
>
> 1. **DI:** clients injected via constructor; phase doesn't construct any.
> 2. **Failure-mode classification:** `ingest_mcp` failure = phase failed. MySQL absence = phase failed. Index-distinction issues (OLD missing OR NEW missing) = warning only, doesn't fail the phase.
> 3. **Partial-failure visibility:** ingest failure path records partial `dual_write_urns`; MySQL failure path records full ingested list. Test `test_ingest_failure_fails_phase_with_partial_dual_write_urns` verifies.
> 4. **URN naming:** `urn:li:dashboard:(test,zdu-dual-{i})` — distinct from `zdu-gap-` (Plan 5), `zdu-io-pool-` (seed), `zdu-tc-` (scenario).
> 5. **Pipeline-order field placement** in `TestContext`: `dual_write_urns` after `rolling_restart`, before `validation_results`.
> 6. **Symmetric pattern with Plan 5:** the two phases share `_find_dashboard_index`, `_sanity_check_single_index`, `_safe_get_doc` helpers in spirit. Plan 6 reimplements them rather than extracting a shared base — that's an acceptable YAGNI choice for now since Phase 5/7 differ in core verification logic. Confirm this duplication is bounded (small helpers, easy to consolidate later if a third similar phase emerges).
> 7. **Test quality:** mocked clients, behavior-focused assertions. The 5 happy tests cover: both-indices-present, OLD-missing, NEW-missing, degenerate single-image, no upgrade_blocking state.
> 8. **YAGNI:** No retry, no concurrent writes, no rollback-flag handling (TC-204 deferred), no delete-propagation (TC-208 deferred).
> 9. **Type hints complete.**
> 10. **Relative imports** match peer phase files.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 6"
```

---

## Self-Review

**Spec coverage** (against design doc Section 14.1 Phase 7):

| Requirement                                                 | Task                                                                                                             |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| New GMS write path (REST `ingestProposal`)                  | Task 2 (`ingest_mcp`)                                                                                            |
| MAE consumer applying MCL → ES with dual-write fan-out      | OUT OF SCOPE — Phase 7 only verifies the OUTCOME (presence in both indices); the fan-out itself is a MAE concern |
| Direct ES queries against both old and new physical indices | Task 2 (`_check_dual_write_fanout`)                                                                              |
| At least N writes (default 10) succeed                      | Task 2 (`_DEFAULT_N_DUAL_WRITES = 10`)                                                                           |
| Every entity in BOTH old and new physical indices           | Task 2 (best-effort warnings)                                                                                    |
| Document content matches in both indices                    | OUT OF SCOPE — presence-only check; content equality is Phase 10's job                                           |
| TC-204 (rollback flag off → NEW only)                       | OUT OF SCOPE — needs runtime config knob                                                                         |
| TC-205 (dualWriteStartTime once per index)                  | Plan 4 already captures this in `ctx.rolling_restart.dual_write_start_times`                                     |
| TC-208 (deletes propagate to both indices)                  | OUT OF SCOPE — separate test scenario                                                                            |

**Placeholder scan:** None.

**Type / signature consistency:**

- `TestContext.dual_write_urns: list[str]` (Task 1) — populated by Task 2's `phase.run()`.
- `InjectTrafficDualPhase(datahub, mysql, es, n_dual_writes, aspect_name)` (Task 2) — runner constructs (Task 3).
- `IndexState.old_backing_index_name`, `next_index_name` (existing) — read by `_check_dual_write_fanout`.
- `ElasticsearchClient.get_doc(index_name, doc_id)` (existing, F-1 URL-encoding fix in path) — used by `_safe_get_doc`.
- `MySQLClient.get_aspect_raw(urn, aspect)` (existing) — used directly.

**Risks called out:**

1. **Code duplication with Plan 5.** `_find_dashboard_index`, `_sanity_check_single_index`, `_safe_get_doc` are near-identical in both phases. Extracting a shared base class is YAGNI today (only two consumers), but if Phase 5/7-shaped phases proliferate (e.g., a hypothetical "InjectTrafficPostPhase" after Phase 8), refactor at that point.
2. **Eventual consistency.** Same as Plan 5 — `ingest_mcp` is async; the immediate ES `get_doc` calls may not see the doc yet. In practice this surfaces as warnings, not failures, but flaky live runs could surface this as a real issue. Phase 10 Validation may want to retry the index-presence assertions.
3. **Dual-write window narrowness.** In a real ZDU run, the window is bounded by Phase 8 marking indices `DUAL_WRITE_DISABLED`. Phase 7 runs before Phase 8 in the pipeline, so the window is open. No timing concern in this plan, but worth flagging for future test design.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-6-inject-traffic-dual.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
