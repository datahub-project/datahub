# ZDU E2E — Plan 1: Phase 3 `SnapshotT0Phase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 3 — `SnapshotT0Phase` — which captures pre-upgrade ES + MySQL state into `TestContext.snapshot_t0` so downstream phases (Phase 4 upgrade-blocking, Phase 10 validation) can detect what changed.

**Architecture:** One new phase class (`framework/phases/snapshot_t0.py`) follows the existing `Phase` ABC pattern from `phases/base.py`. Captures four things into a `SnapshotT0` dataclass: alias→indices map, doc counts, per-aspect schemaVersion histograms, and T0 epoch ms. Aliases are _discovered_ at runtime by listing all `*index_v2*` physical indices and querying their alias mapping — no hardcoded list — so the phase works against any DataHub deployment regardless of which entities exist. `MySQLClient` and `ElasticsearchClient` are injected (Foundation pattern); the phase has no live deps in unit tests.

**Tech Stack:** Python 3, pytest with mocks for unit tests, real Compose stack for integration. Reuses Foundation clients (`framework/mysql_client.py`, `framework/es_client.py`).

**Out of scope (deferred to later plans):**

- Phase 4 (`UpgradeBlockingPhase`) — uses the snapshot but is its own plan.
- Discovery of aspects under test from CSV — for this plan, the aspect list is hardcoded to `["embed", "globalTags"]` (the two aspects Suite A's TCs cover).
- Wiring `SnapshotT0Phase` into the runner's `phases = [...]` list — the runner change is in this plan, but downstream phases that READ `ctx.snapshot_t0` come later.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer` before merge.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add SnapshotT0 dataclass + ctx slot
├── phases/
│   └── snapshot_t0.py                           CREATE — SnapshotT0Phase
├── runner.py                                    MODIFY — insert SnapshotT0Phase between seed and upgrade
├── test_snapshot_t0.py                          CREATE — unit tests with mocked ES + MySQL
└── (no other files touched)
```

The phase reads from `ElasticsearchClient` and `MySQLClient` (both injected). It writes to `ctx.snapshot_t0`. It does NOT use `DataHubClient` (no GMS API calls — direct ES/DB only).

---

## Task 1: Add `SnapshotT0` dataclass to `TestContext`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Single-purpose data carrier with default-empty containers. Lives next to other phase-output dataclasses (`SeededEntity`, `IOObservation`, `IOWriteResult`, `ValidationResult`).

- [ ] **Step 1: Read the existing context.py to find the right insertion point**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework import context
import inspect
print(inspect.getsourcefile(context))
"
```

The file is at `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`. Read it. The dataclasses appear before `TestContext`; the `TestContext` slots have `# {Phase}Phase writes` comments to mark sections.

- [ ] **Step 2: Add the `SnapshotT0` dataclass before `TestContext`**

Use `Edit` to insert this block immediately after `IOWriteResult` and before `ValidationResult` (or wherever phase-output dataclasses are grouped — adjacent to similar items):

```python
@dataclass
class SnapshotT0:
    """Pre-upgrade ES + MySQL state captured before SystemUpdateBlocking runs.

    Written by ``SnapshotT0Phase``. Read by future ``UpgradeBlockingPhase``
    (to compute alias-swap deltas) and ``ValidationPhase`` (to verify
    post-upgrade invariants).
    """

    epoch_ms: int
    indices: dict[str, list[str]] = field(default_factory=dict)
    """alias name -> list of physical index names it points at (empty if none)."""

    doc_counts: dict[str, int] = field(default_factory=dict)
    """physical index name -> doc count at T0."""

    aspects_by_version: dict[str, dict[int | None, int]] = field(default_factory=dict)
    """aspect name -> {schemaVersion or None: row count} at T0."""

    upgrade_result_present: bool = False
    """True if a ``DataHubUpgradeResult`` exists for the configured upgrade
    version, indicating a prior partial/complete run that may need cleanup."""
```

- [ ] **Step 3: Add the `snapshot_t0` slot on `TestContext`**

Locate the `# DiscoveryPhase writes` / `# SeedPhase writes` block in `TestContext`. Insert after `seeded_entities` and before the `# UpgradePhase writes` block:

```python
    # SnapshotT0Phase writes
    snapshot_t0: SnapshotT0 | None = None
```

- [ ] **Step 4: Smoke-import**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import TestContext, SnapshotT0
ctx = TestContext()
assert ctx.snapshot_t0 is None
s = SnapshotT0(epoch_ms=1)
assert s.indices == {} and s.doc_counts == {} and s.aspects_by_version == {}
assert s.upgrade_result_present is False
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 5: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 87 pass (the same as the Foundation baseline).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add SnapshotT0 dataclass and TestContext slot"
```

---

## Task 2: Add the `aspects_under_test` configuration field

**Files:**

- Modify: `smoke-test/tests/zdu/framework/config.py`

**Pattern:** Same as Foundation config additions — list field with sensible default, `from_env` hook, no test required (config-only).

- [ ] **Step 1: Add the field**

In `smoke-test/tests/zdu/framework/config.py`, inside the `ZDUTestConfig` dataclass, add this near the existing test-hooks section:

```python
    # ── Phase 3 SnapshotT0 ────────────────────────────────────────────────────
    # Aspects whose schemaVersion histogram should be captured at T0.
    # Suite A targets "embed" and "globalTags"; future suites may extend.
    aspects_under_test: list[str] = field(
        default_factory=lambda: ["embed", "globalTags"]
    )
```

- [ ] **Step 2: Add env-var override in `from_env`**

In the comma-separated section of `from_env` (next to the existing `ZDU_SKIP_PHASES` and `ZDU_SUITES` blocks):

```python
        if v := os.environ.get("ZDU_ASPECTS_UNDER_TEST"):
            kwargs["aspects_under_test"] = [
                a.strip() for a in v.split(",") if a.strip()
            ]
```

- [ ] **Step 3: Smoke-test**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
c = ZDUTestConfig.from_env()
assert c.aspects_under_test == ['embed', 'globalTags'], c.aspects_under_test
"

ZDU_ASPECTS_UNDER_TEST=foo,bar,baz smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
c = ZDUTestConfig.from_env()
assert c.aspects_under_test == ['foo', 'bar', 'baz'], c.aspects_under_test
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 4: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 87 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/config.py
git commit -m "feat(zdu): add aspects_under_test config + ZDU_ASPECTS_UNDER_TEST env"
```

---

## Task 3: Implement `SnapshotT0Phase`

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/snapshot_t0.py`
- Create: `smoke-test/tests/zdu/framework/test_snapshot_t0.py`

**Pattern:** Phase ABC subclass following `phases/discovery.py`. Constructor takes injected `ElasticsearchClient`, `MySQLClient`, plus the aspects list and an optional upgrade-id-to-check. `run(ctx)` returns `PhaseResult` with `status="passed"` or `"failed"`. Best-effort: a single bad alias or aspect should not crash the phase — it logs and continues. Aliases are _discovered_ via `list_indices(prefix="")` rather than hardcoded.

### 3.1 — Write the failing tests

- [ ] **Step 1: Create the test file**

Create `smoke-test/tests/zdu/framework/test_snapshot_t0.py`:

```python
"""Unit tests for SnapshotT0Phase — uses mocked ES + MySQL clients."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import SnapshotT0, TestContext
from tests.zdu.framework.phases.snapshot_t0 import SnapshotT0Phase


@pytest.fixture
def es() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    return MagicMock()


@pytest.fixture
def phase(es: MagicMock, mysql: MagicMock) -> SnapshotT0Phase:
    return SnapshotT0Phase(
        es=es,
        mysql=mysql,
        aspects_under_test=["embed", "globalTags"],
        upgrade_id_to_check=None,
    )


class TestSnapshotT0PhaseHappyPath:
    def test_captures_aliases_doc_counts_and_aspects(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        # Pre-Phase-1 state: indices exist as physical indices with no aliases yet.
        es.list_indices.return_value = [
            "datasetindex_v2",
            "dashboardindex_v2",
            "datasetindex_v2_1714000000",  # already-reindexed style
        ]
        # The reindexed index has an alias; the others don't.
        es.get_alias_targets.side_effect = lambda alias: (
            ["datasetindex_v2_1714000000"] if alias == "datasetindex_v2_alias" else []
        )

        def doc_count(idx: str) -> int:
            return {
                "datasetindex_v2": 100,
                "dashboardindex_v2": 50,
                "datasetindex_v2_1714000000": 100,
            }[idx]

        es.get_doc_count.side_effect = doc_count
        mysql.count_aspects_by_schema_version.side_effect = lambda aspect: {
            "embed": {None: 5, 4: 1184},
            "globalTags": {2: 19},
        }[aspect]

        ctx = TestContext()
        result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.snapshot_t0 is not None
        snap = ctx.snapshot_t0
        # Doc counts captured for every physical index returned by list_indices.
        assert snap.doc_counts == {
            "datasetindex_v2": 100,
            "dashboardindex_v2": 50,
            "datasetindex_v2_1714000000": 100,
        }
        # Aspect histograms captured for both configured aspects.
        assert snap.aspects_by_version == {
            "embed": {None: 5, 4: 1184},
            "globalTags": {2: 19},
        }
        # T0 is a positive epoch_ms.
        assert snap.epoch_ms > 0


class TestSnapshotT0PhaseDefensiveErrors:
    def test_es_failure_marks_phase_failed_but_does_not_crash(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.side_effect = ConnectionError("boom")
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "boom" in (result.error or "")

    def test_one_aspect_query_failure_does_not_drop_other_aspects(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.return_value = ["datasetindex_v2"]
        es.get_doc_count.return_value = 0

        def aspect_query(aspect: str) -> dict:
            if aspect == "embed":
                raise RuntimeError("mysql blip")
            return {2: 7}

        mysql.count_aspects_by_schema_version.side_effect = aspect_query
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.snapshot_t0 is not None
        # embed missing from the histogram (skipped on error); globalTags present.
        assert "embed" not in ctx.snapshot_t0.aspects_by_version
        assert ctx.snapshot_t0.aspects_by_version["globalTags"] == {2: 7}

    def test_one_index_doc_count_failure_does_not_drop_other_indices(
        self, phase: SnapshotT0Phase, es: MagicMock, mysql: MagicMock
    ) -> None:
        es.list_indices.return_value = ["a_index_v2", "b_index_v2"]

        def doc_count(idx: str) -> int:
            if idx == "a_index_v2":
                raise ConnectionError("blip")
            return 42

        es.get_doc_count.side_effect = doc_count
        mysql.count_aspects_by_schema_version.return_value = {}
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        # a_index_v2 omitted, b_index_v2 captured.
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.doc_counts == {"b_index_v2": 42}


class TestSnapshotT0PhaseUpgradeResultCheck:
    def test_no_upgrade_id_means_no_check(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es, mysql=mysql, aspects_under_test=[], upgrade_id_to_check=None,
        )
        es.list_indices.return_value = []
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is False
        # MySQL not asked about upgrade results because upgrade_id_to_check is None.
        mysql.get_upgrade_result.assert_not_called()

    def test_upgrade_id_present_sets_flag_true(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es, mysql=mysql, aspects_under_test=[],
            upgrade_id_to_check="system-update-blocking",
        )
        es.list_indices.return_value = []
        mysql.get_upgrade_result.return_value = {"state": "SUCCEEDED"}
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is True
        mysql.get_upgrade_result.assert_called_once_with("system-update-blocking")

    def test_upgrade_id_absent_sets_flag_false(
        self, es: MagicMock, mysql: MagicMock
    ) -> None:
        phase = SnapshotT0Phase(
            es=es, mysql=mysql, aspects_under_test=[],
            upgrade_id_to_check="system-update-blocking",
        )
        es.list_indices.return_value = []
        mysql.get_upgrade_result.return_value = None
        ctx = TestContext()
        phase.run(ctx)
        assert ctx.snapshot_t0 is not None
        assert ctx.snapshot_t0.upgrade_result_present is False
```

- [ ] **Step 2: Run tests — expect failure**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_snapshot_t0.py -v
```

Expected: `ModuleNotFoundError: No module named 'tests.zdu.framework.phases.snapshot_t0'`.

### 3.2 — Implement the phase

- [ ] **Step 3: Create the phase file**

Create `smoke-test/tests/zdu/framework/phases/snapshot_t0.py`:

```python
"""Phase 3 — SnapshotT0Phase.

Captures pre-upgrade state into ``TestContext.snapshot_t0`` so downstream
phases can compute deltas after the upgrade runs:

- ES alias targets (alias name → list of physical indices) for every
  ``*index_v2*`` index discovered at runtime.
- Per-physical-index doc count.
- MySQL aspect-row count grouped by ``schemaVersion`` for each configured aspect.
- T0 epoch ms (used by Phase 4's ``[T0, T1]`` window assertions).
- Whether a prior ``DataHubUpgradeResult`` exists for the configured upgrade id.

The phase is best-effort per probe: a single failing alias / index / aspect
query is logged and skipped, but a connection-level failure on the initial
``list_indices`` call short-circuits the phase to ``failed``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import SnapshotT0, TestContext
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

# Discovery prefix for DataHub physical indices. All entity / graph /
# system_metadata indices end in ``_v2`` in the dev compose. We list once
# and probe each result's alias mapping individually.
_INDEX_PREFIX = ""

# Suffix used to identify DataHub physical indices.
_INDEX_SUFFIX = "index_v2"


class SnapshotT0Phase(Phase):
    name = "snapshot_t0"

    def __init__(
        self,
        es: ElasticsearchClient,
        mysql: MySQLClient,
        aspects_under_test: list[str],
        upgrade_id_to_check: str | None = None,
    ) -> None:
        self._es = es
        self._mysql = mysql
        self._aspects = aspects_under_test
        self._upgrade_id = upgrade_id_to_check

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        try:
            indices = self._discover_indices()
        except Exception as exc:
            log.exception("SnapshotT0Phase failed during index discovery")
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=str(exc),
            )

        snap = SnapshotT0(epoch_ms=int(time.time() * 1000))

        for idx in indices:
            self._record_doc_count(snap, idx)
            self._record_alias(snap, idx)

        for aspect in self._aspects:
            self._record_aspect_histogram(snap, aspect)

        if self._upgrade_id:
            self._record_upgrade_result_presence(snap)

        ctx.snapshot_t0 = snap

        log.info(
            "Snapshot T0 captured: %d indices, %d aliases, %d aspects, T0=%d",
            len(snap.doc_counts), len(snap.indices),
            len(snap.aspects_by_version), snap.epoch_ms,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "doc_counts": snap.doc_counts,
                "aliases": snap.indices,
                "aspects": {a: dict(v) for a, v in snap.aspects_by_version.items()},
                "epoch_ms": snap.epoch_ms,
                "upgrade_result_present": snap.upgrade_result_present,
            },
        )

    def _discover_indices(self) -> list[str]:
        """Return all DataHub-shape physical indices, filtered by suffix."""
        all_indices = self._es.list_indices(prefix=_INDEX_PREFIX)
        return [i for i in all_indices if _INDEX_SUFFIX in i]

    def _record_doc_count(self, snap: SnapshotT0, idx: str) -> None:
        try:
            snap.doc_counts[idx] = self._es.get_doc_count(idx)
        except Exception as exc:
            log.warning("doc count failed for %s: %s", idx, exc)

    def _record_alias(self, snap: SnapshotT0, idx: str) -> None:
        # Alias query is by alias name, not index name — to discover aliases
        # we treat each physical index as a candidate alias and check whether
        # ES resolves it to a different physical index. ES returns the empty
        # list when the name is itself a physical index (no alias).
        try:
            targets = self._es.get_alias_targets(idx)
        except Exception as exc:
            log.warning("alias query failed for %s: %s", idx, exc)
            return
        if targets:
            snap.indices[idx] = targets

    def _record_aspect_histogram(self, snap: SnapshotT0, aspect: str) -> None:
        try:
            snap.aspects_by_version[aspect] = (
                self._mysql.count_aspects_by_schema_version(aspect)
            )
        except Exception as exc:
            log.warning("aspect histogram failed for %s: %s", aspect, exc)

    def _record_upgrade_result_presence(self, snap: SnapshotT0) -> None:
        try:
            assert self._upgrade_id is not None  # narrow for type checker
            result = self._mysql.get_upgrade_result(self._upgrade_id)
            snap.upgrade_result_present = result is not None
        except Exception as exc:
            log.warning(
                "upgrade-result check failed for %s: %s", self._upgrade_id, exc
            )
```

- [ ] **Step 4: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_snapshot_t0.py -v
```

Expected: 7 tests pass (3 happy + 3 defensive + multiple upgrade-result variants — actual count may vary; all green).

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/snapshot_t0.py \
        smoke-test/tests/zdu/framework/test_snapshot_t0.py
git commit -m "feat(zdu): SnapshotT0Phase — pre-upgrade ES + MySQL state capture"
```

---

## Task 4: Wire `SnapshotT0Phase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert in the existing `phases = [...]` list between `seed` and `upgrade`. Pass injected clients (which the runner already constructs in `__init__`).

- [ ] **Step 1: Read the runner to locate the `phases` list**

Read `smoke-test/tests/zdu/framework/runner.py`. The list lives in `ZDUTestRunner.run()` and currently looks roughly like:

```python
        phases = [
            ("discovery", DiscoveryPhase(...)),
            ("seed", SeedPhase(...)),
            ("upgrade", UpgradePhase(...)),
            ("sweep_and_io", SweepAndIOPhase(...)),
            ("validation", ValidationPhase(...)),
        ]
```

- [ ] **Step 2: Add the import at module top**

Find the existing imports for phase classes (`from .phases.discovery import DiscoveryPhase`, etc.) and add:

```python
from .phases.snapshot_t0 import SnapshotT0Phase
```

- [ ] **Step 3: Insert `snapshot_t0` between `seed` and `upgrade`**

In the `phases = [...]` list, after the `("seed", ...)` tuple and before `("upgrade", ...)`:

```python
            (
                "snapshot_t0",
                SnapshotT0Phase(
                    es=self._es,
                    mysql=self._mysql,
                    aspects_under_test=self._config.aspects_under_test,
                    upgrade_id_to_check=None,
                ),
            ),
```

We pass `upgrade_id_to_check=None` for now — Phase 4 will know its own upgrade id and pass it explicitly when that plan lands.

- [ ] **Step 4: Smoke-test that the runner constructs**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
print('runner constructed OK')
"
```

Expected: `runner constructed OK`.

- [ ] **Step 5: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -8
```

Expected: existing 87 + new 7 = ~94 tests pass.

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire SnapshotT0Phase into the runner pipeline"
```

---

## Task 5: Live integration check against the running Compose stack

**Pre-requisite:** DataHub is up via `scripts/dev/datahub-dev.sh status` reporting `ready: true`.

This task verifies the phase against real ES + MySQL — the kind of contract issue that bit Foundation twice (JSON_EXTRACT typing and `get_doc` URL encoding).

- [ ] **Step 1: Run `SnapshotT0Phase` directly against the live stack**

```bash
cd smoke-test && venv/bin/python << 'PY'
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.es_client import ElasticsearchClient
from tests.zdu.framework.mysql_client import MySQLClient
from tests.zdu.framework.phases.snapshot_t0 import SnapshotT0Phase

cfg = ZDUTestConfig()
es = ElasticsearchClient(gms_url=cfg.gms_url, es_url=cfg.es_url, token=cfg.gms_token)
mysql = MySQLClient(host=cfg.mysql_host, port=cfg.mysql_port, user=cfg.mysql_user,
                   password=cfg.mysql_password, database=cfg.mysql_database)

phase = SnapshotT0Phase(
    es=es, mysql=mysql,
    aspects_under_test=cfg.aspects_under_test,
    upgrade_id_to_check="system-update-blocking",
)
ctx = TestContext()
result = phase.run(ctx)
print(f"phase status: {result.status}")
print(f"duration: {result.duration_s:.2f}s")
print(f"error: {result.error}")

snap = ctx.snapshot_t0
assert snap is not None, "snapshot_t0 not populated"
assert snap.epoch_ms > 0, f"epoch_ms not positive: {snap.epoch_ms}"

# Sanity: at least the standard DataHub indices are present.
expected_indices = ["datasetindex_v2", "dashboardindex_v2", "corpuserindex_v2"]
for idx in expected_indices:
    assert idx in snap.doc_counts, f"missing index {idx} in {sorted(snap.doc_counts)}"

# Aspect histograms present and have int|None keys (Foundation bug fix).
for aspect in cfg.aspects_under_test:
    assert aspect in snap.aspects_by_version, f"missing aspect {aspect}"
    hist = snap.aspects_by_version[aspect]
    for k in hist.keys():
        assert k is None or isinstance(k, int), \
            f"non-int|None key in {aspect} histogram: {k!r} type={type(k).__name__}"

print(f"\nDoc counts ({len(snap.doc_counts)} indices):")
for idx, n in sorted(snap.doc_counts.items()):
    print(f"  {idx}: {n}")

print(f"\nAspect histograms:")
for a, h in snap.aspects_by_version.items():
    print(f"  {a}: {h}")

print(f"\nAlias mappings ({len(snap.indices)} aliases): {snap.indices}")
print(f"\nupgrade_result_present: {snap.upgrade_result_present}")
print("\nLIVE OK")
PY
```

Expected output:

- `phase status: passed`
- duration < 5 s
- ≥10 physical indices in `doc_counts` (datasetindex_v2, dashboardindex_v2, corpuserindex_v2, etc.)
- `embed` and `globalTags` histograms with `int | None` keys (the Foundation `MySQLClient` fix is in the path)
- `aliases` likely empty in fresh DataHub (no Phase 1 reindex yet)
- `upgrade_result_present` likely True (DataHub bootstrap ran some upgrades)
- final `LIVE OK`

If the script fails any assertion, it surfaces a real bug — diagnose before committing.

- [ ] **Step 2: Run the existing Suite A E2E with snapshot_t0 in the path**

```bash
cd <REPO_ROOT>
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade \
smoke-test/venv/bin/python -m tests.zdu --suite a 2>&1 | tail -30
```

This runs Suite A through the new pipeline shape (`discovery → seed → snapshot_t0 → upgrade [skipped] → sweep_and_io → validation`). Expected: same Suite A results as the Foundation run (14 PASS, 7 XFAIL, 1 known-FAIL TC-020), plus a new `✓ snapshot_t0  passed` line in the phase report.

- [ ] **Step 3: Verify the JSON report**

```bash
smoke-test/venv/bin/python -c "
import json
r = json.load(open('smoke-test/build/zdu-test-report.json'))
phases = [(p['name'], p['status']) for p in r['phases']]
print('phases:', phases)
assert ('snapshot_t0', 'passed') in phases, f'snapshot_t0 missing or failed: {phases}'
# Ensure snapshot details landed in the report
snap_phase = next(p for p in r['phases'] if p['name'] == 'snapshot_t0')
assert 'epoch_ms' in snap_phase['details']
assert 'doc_counts' in snap_phase['details']
print('REPORT OK — snapshot_t0 captured', len(snap_phase['details']['doc_counts']), 'indices')
"
```

Expected: `REPORT OK — snapshot_t0 captured N indices` where N ≥ 10.

- [ ] **Step 4: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in snapshot_t0"
```

If nothing regressed, no commit needed for this task.

---

## Task 6: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
git diff master...HEAD -- smoke-test/tests/zdu/framework/ > /tmp/zdu-plan-1.diff
wc -l /tmp/zdu-plan-1.diff
```

(If `master` is too far behind, use the parent of the first Plan-1 commit instead — `git log --oneline | head -10` will show the boundary.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send the reviewer this prompt:

> Review the diff at `/tmp/zdu-plan-1.diff`. This PR adds Phase 3 (`SnapshotT0Phase`) to the ZDU E2E framework. It captures pre-upgrade ES + MySQL state into a new `SnapshotT0` dataclass on `TestContext`. The phase uses the existing Foundation clients (`ElasticsearchClient`, `MySQLClient`) and is wired into the runner between `seed` and `upgrade`.
>
> Check specifically:
>
> 1. Single Responsibility — does `SnapshotT0Phase` do one thing? No accidental scope creep.
> 2. Defensive parsing — per-probe failures (one bad alias, one missing aspect histogram, one index doc-count error) must NOT fail the whole phase. A connection-level failure at `list_indices` SHOULD fail the phase.
> 3. DI correctness — clients are injected, not constructed in the phase.
> 4. Type hints complete; no unjustified `Any`.
> 5. The integration script in Task 5 actually exercises the real-data paths the unit tests mock.
> 6. Aspect-histogram keys are `int | None`, not `str` (Foundation `MySQLClient` fix is in the call path).
> 7. The aliases captured represent real alias→index mappings, not noise.
> 8. The phase does not couple to any future Phase 4 / Phase 10 implementation.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on SnapshotT0Phase"
```

---

## Self-Review

**Spec coverage** (against design doc Section 5.3 + Section 14.1 Phase 3):

| Requirement                                           | Task                                                                                                            |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| ES `_cat/indices`, `_cat/aliases`, per-index `_count` | Task 3 (`_discover_indices`, `_record_alias`, `_record_doc_count`)                                              |
| MySQL `SELECT COUNT(*) ... GROUP BY schemaVersion`    | Task 3 (`_record_aspect_histogram` via `MySQLClient`)                                                           |
| `DataHubUpgradeResult` aspect read                    | Task 3 (`_record_upgrade_result_presence`)                                                                      |
| T0 wall-clock                                         | Task 3 (`int(time.time() * 1000)`)                                                                              |
| Pre-upgrade physical index names recorded             | Task 3 (covered by `doc_counts` and `indices`)                                                                  |
| Per-`schemaVersion` aspect counts captured            | Task 3 (`aspects_by_version`)                                                                                   |
| T0 epoch (millis) recorded                            | Task 3 (`epoch_ms`)                                                                                             |
| `DataHubUpgradeResult` absent OR terminal             | Task 3 (`upgrade_result_present` flag — caller decides what "terminal" means; this plan only records existence) |

**Gaps deliberately deferred:**

- The "absent OR in a terminal state" check requires parsing the upgrade result's `state` field. The Foundation `MySQLClient.get_upgrade_result` already returns the parsed JSON; downstream phases that care about state (Phase 4) will read it from `ctx.snapshot_t0.upgrade_result_present` plus their own state check. Keeping this phase narrow.
- "Per-aspect" config is hardcoded today; later plans may load it from `scenarios.csv` columns, but Suite A only needs `embed` + `globalTags`.

**Placeholder scan:** None.

**Type / signature consistency:**

- `SnapshotT0(epoch_ms: int, indices: dict[str, list[str]], doc_counts: dict[str, int], aspects_by_version: dict[str, dict[int | None, int]], upgrade_result_present: bool)` — same shape declared in Task 1 and constructed in Task 3.
- `SnapshotT0Phase(es: ElasticsearchClient, mysql: MySQLClient, aspects_under_test: list[str], upgrade_id_to_check: str | None)` — same in Task 3 implementation and Task 4 runner wiring.
- `MySQLClient.count_aspects_by_schema_version(aspect: str) -> dict[int | None, int]` — Foundation contract, used as-is in Task 3.
- `ElasticsearchClient.list_indices(prefix: str) -> list[str]` — Foundation contract, used as-is.
- `ElasticsearchClient.get_alias_targets(alias: str) -> list[str]` — Foundation contract.
- `ElasticsearchClient.get_doc_count(index_name: str) -> int` — Foundation contract.
- `MySQLClient.get_upgrade_result(upgrade_id: str) -> dict | None` — Foundation contract.

**Risks called out:**

- The "alias discovery" approach (treat every physical index name as a candidate alias) works in fresh DataHub where `get_alias_targets("datasetindex_v2")` returns `[]`. After Phase 1 runs, `datasetindex_v2` becomes an alias pointing at `datasetindex_v2_<ts>`; `get_alias_targets("datasetindex_v2_<ts>")` would still return `[]` since the timestamped index is itself physical. So the `indices` map only ever contains true alias→target pairs. Verified by the unit test `test_captures_aliases_doc_counts_and_aspects`.
- `_discover_indices` uses substring match on `"index_v2"`. Names like `corpuserindex_v2` and `system_metadata_service_v1` differ — `system_metadata_service_v1` won't match. That's intentional for this plan (entity indices only). Future plans may extend the suffix list.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-06-zdu-plan-1-snapshot-t0.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
