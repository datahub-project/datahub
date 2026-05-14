# ZDU E2E — Plan 2: Phase 4 `UpgradeBlockingPhase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 4 — `UpgradeBlockingPhase` — which runs `system-update -u SystemUpdateBlocking` against the live stack, watches `BuildIndicesIncrementalStep` log events, and captures the resulting `DataHubUpgradeResult.indicesState[*]` into `TestContext.upgrade_blocking` for downstream phases.

**Architecture:** One new phase class (`framework/phases/upgrade_blocking.py`) follows the existing `Phase` ABC pattern. It launches the upgrade-job container via the Foundation `DockerComposeClient.run_upgrade_job(...)`, streams stdout line-by-line, and pattern-matches Phase-1-specific log lines using new helpers in `log_monitor.py`. After the job exits cleanly, it queries MySQL for any `dataHubUpgradeResult` aspect that contains an `indicesState` payload and merges the live-observed alias swaps with the persisted state into a `UpgradeBlockingResult` dataclass.

**Tech Stack:** Python 3, pytest with mocks for unit tests, real Compose stack for integration. Reuses Foundation clients (`DockerComposeClient`, `MySQLClient`, `LogMonitor` patterns).

**Out of scope (deferred):**

- The Java-side `BUILD_INDICES_REINDEX_DELAY_MS` test hook (deferred to a small standalone plan — production code change requires its own focused review).
- Phase 5–10 implementations (each is its own plan).
- Test isolation between Suite A/B runs that share the upgrade-job container — Suite B will need scenarios.csv rows; Suite B implementation comes after Plan 2.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add IndexState + UpgradeBlockingResult
├── log_monitor.py                               MODIFY — add Phase1State, Phase1Event, _parse_phase1_line
├── runner.py                                    MODIFY — wire UpgradeBlockingPhase after legacy `upgrade`
├── test_log_monitor.py                          MODIFY — append Phase1 parse tests
├── phases/
│   └── upgrade_blocking.py                      CREATE
└── test_upgrade_blocking.py                     CREATE
```

The phase reads from `DockerComposeClient` (to launch the upgrade job + read GMS env vars) and `MySQLClient` (to capture the post-run `DataHubUpgradeResult`). It writes to `ctx.upgrade_blocking` and emits `Phase1Event`s only via its private log-tail loop.

---

## Task 1: Add `IndexState` + `UpgradeBlockingResult` dataclasses

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Single-purpose data carriers. Lives next to the `SnapshotT0` dataclass added in Plan 1.

- [ ] **Step 1: Read `context.py` to find the insertion point**

The file currently has dataclasses in this order: `SeededEntity`, `IOObservation`, `IOWriteResult`, `SnapshotT0` (Plan 1), `ValidationResult`, then `TestContext`. Insert the new dataclasses between `SnapshotT0` and `ValidationResult`.

- [ ] **Step 2: Append the dataclasses**

Insert this block between `SnapshotT0` and `ValidationResult`:

```python
@dataclass
class IndexState:
    """One row of ``DataHubUpgradeResult.indicesState`` after Phase 1 runs.

    Field names match the design-doc spec for required keys; values map
    directly from the parsed JSON. Optional fields are ``None`` when the
    upgrade step skipped persisting them (e.g., a no-reindex pass-through).
    """

    alias: str
    next_index_name: str | None = None
    old_backing_index_name: str | None = None
    reindex_start_time: int | None = None
    source_doc_count: int = 0
    task_id: str | None = None
    requires_data_backfill: bool = False
    status: str = "UNKNOWN"


@dataclass
class UpgradeBlockingResult:
    """Captured by ``UpgradeBlockingPhase`` after ``system-update -u SystemUpdateBlocking``.

    ``indices`` is the structured view derived from ``DataHubUpgradeResult.indicesState``.
    ``alias_swaps_observed`` is the list of ``(alias, next_index)`` pairs the framework
    saw in real time from the upgrade-job stdout — primary evidence the swap happened.
    ``raw`` is the full parsed ``DataHubUpgradeResult`` aspect for failure-bundle dumps.
    """

    indices: list[IndexState] = field(default_factory=list)
    alias_swaps_observed: list[tuple[str, str]] = field(default_factory=list)
    raw: dict | None = None
    duration_s: float = 0.0
    upgrade_id: str | None = None
```

- [ ] **Step 3: Add the slot on `TestContext`**

In `TestContext`, after `# SnapshotT0Phase writes` block (`snapshot_t0` field) and before `# UpgradePhase writes`, insert:

```python
    # UpgradeBlockingPhase writes
    upgrade_blocking: UpgradeBlockingResult | None = None
```

- [ ] **Step 4: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import (
    TestContext, IndexState, UpgradeBlockingResult,
)
ctx = TestContext()
assert ctx.upgrade_blocking is None
s = IndexState(alias='datasetindex_v2')
assert s.status == 'UNKNOWN'
r = UpgradeBlockingResult()
assert r.indices == [] and r.alias_swaps_observed == [] and r.raw is None
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 5: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 94 pass (the Plan 1 baseline).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add IndexState and UpgradeBlockingResult dataclasses"
```

---

## Task 2: Add Phase 1 log parsing to `log_monitor.py`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/log_monitor.py`
- Modify: `smoke-test/tests/zdu/framework/test_log_monitor.py` (append cases)

**Pattern:** Add a parallel parsing path alongside the existing `SweepState`/`SweepEvent`/`_parse_line` machinery. `Phase1Event` is a separate dataclass; the existing `SweepEvent` is unchanged.

**Production log lines** (verified in `BuildIndicesIncrementalStep.java`):

| Line                                                                  | Java line | Meaning                |
| --------------------------------------------------------------------- | --------- | ---------------------- |
| `No indices require incremental reindex`                              | 92        | Skip — nothing to do   |
| `Index {name} already COMPLETED in previous run, skipping`            | 107-110   | Skip — prior success   |
| `Index {name} already DUAL_WRITE_DISABLED in previous run, skipping`  | 107-110   | Skip — prior success   |
| `Resuming polling for index {name} -> {next}`                         | 130       | Resume case            |
| `Index {name} had 0 docs, next index created as empty, alias swapped` | 221-222   | Empty-source fast path |
| `Alias swapped: {alias} -> {next_index}`                              | 260       | Successful swap        |
| `Alias swap failed for {alias} -> {next}: doc count mismatch`         | 215, 254  | Failure                |

- [ ] **Step 1: Append failing tests to `test_log_monitor.py`**

```python
from tests.zdu.framework.log_monitor import (
    Phase1Event,
    Phase1State,
    _parse_phase1_line,
)


def test_parse_phase1_no_indices_require_reindex():
    line = "INFO  No indices require incremental reindex"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.NO_REINDEX_NEEDED


def test_parse_phase1_already_completed():
    line = "INFO  Index datasetindex_v2 already COMPLETED in previous run, skipping"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.SKIP_ALREADY_DONE
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_already_dual_write_disabled():
    line = "INFO  Index datasetindex_v2 already DUAL_WRITE_DISABLED in previous run, skipping"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.SKIP_ALREADY_DONE
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_resuming_polling():
    line = "INFO  Resuming polling for index datasetindex_v2 -> datasetindex_v2_1714000000"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.RESUMING_POLLING
    assert e.index_name == "datasetindex_v2"
    assert e.next_index_name == "datasetindex_v2_1714000000"


def test_parse_phase1_alias_swapped():
    line = "INFO  Alias swapped: datasetindex_v2 -> datasetindex_v2_1714000000"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.ALIAS_SWAPPED
    assert e.index_name == "datasetindex_v2"
    assert e.next_index_name == "datasetindex_v2_1714000000"


def test_parse_phase1_empty_source_swap():
    line = "INFO  Index dashboardindex_v2 had 0 docs, next index created as empty, alias swapped"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.ALIAS_SWAPPED
    assert e.index_name == "dashboardindex_v2"


def test_parse_phase1_alias_swap_failed():
    line = "ERROR  Alias swap failed for datasetindex_v2 -> datasetindex_v2_1714: doc count mismatch"
    e = _parse_phase1_line(line)
    assert e is not None
    assert e.state == Phase1State.REINDEX_FAILED
    assert e.index_name == "datasetindex_v2"


def test_parse_phase1_unrelated_returns_none():
    assert _parse_phase1_line("INFO  Some unrelated GMS chatter") is None
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v
```

Expected: `ImportError: cannot import name 'Phase1Event'`.

- [ ] **Step 3: Add `Phase1State` enum, `Phase1Event` dataclass, and `_parse_phase1_line` to `log_monitor.py`**

Add to `smoke-test/tests/zdu/framework/log_monitor.py` AFTER the existing `_GMS_PATTERNS` list and BEFORE the `_parse_line` function:

```python
class Phase1State(Enum):
    """Events emitted by ``BuildIndicesIncrementalStep`` during ``SystemUpdateBlocking``."""

    NO_REINDEX_NEEDED = "no_reindex_needed"
    SKIP_ALREADY_DONE = "skip_already_done"
    RESUMING_POLLING = "resuming_polling"
    ALIAS_SWAPPED = "alias_swapped"
    REINDEX_FAILED = "reindex_failed"


@dataclass
class Phase1Event:
    state: Phase1State
    timestamp: datetime
    message: str
    index_name: str | None = None
    next_index_name: str | None = None


# Patterns ordered by specificity. Each tuple: (regex, state, captures-index, captures-next).
# "Empty source swap" must precede the generic "Alias swapped" match because both can
# be present on the same job, but the empty-source line carries different group captures.
_PHASE1_PATTERNS: list[tuple[re.Pattern, Phase1State, bool, bool]] = [
    (
        re.compile(r"No indices require incremental reindex"),
        Phase1State.NO_REINDEX_NEEDED,
        False,
        False,
    ),
    (
        re.compile(
            r"Index (\S+) already (?:COMPLETED|DUAL_WRITE_DISABLED) in previous run, skipping"
        ),
        Phase1State.SKIP_ALREADY_DONE,
        True,
        False,
    ),
    (
        re.compile(r"Resuming polling for index (\S+) -> (\S+)"),
        Phase1State.RESUMING_POLLING,
        True,
        True,
    ),
    (
        # Empty-source fast path — alias still swapped but index name comes first in the line.
        re.compile(r"Index (\S+) had 0 docs, next index created as empty, alias swapped"),
        Phase1State.ALIAS_SWAPPED,
        True,
        False,
    ),
    (
        re.compile(r"Alias swapped: (\S+) -> (\S+)"),
        Phase1State.ALIAS_SWAPPED,
        True,
        True,
    ),
    (
        re.compile(r"Alias swap failed for (\S+) -> (\S+)"),
        Phase1State.REINDEX_FAILED,
        True,
        True,
    ),
]


def _parse_phase1_line(line: str) -> Phase1Event | None:
    """Parse a single line of upgrade-job stdout into a Phase1Event, or None.

    Patterns are tried in order; the first match wins. Independent of
    ``_parse_line`` so the existing SweepEvent flow is untouched.
    """
    for pattern, state, has_idx, has_next in _PHASE1_PATTERNS:
        m = pattern.search(line)
        if not m:
            continue
        idx = m.group(1) if has_idx and m.lastindex else None
        nxt = m.group(2) if has_next and m.lastindex and m.lastindex >= 2 else None
        return Phase1Event(
            state=state,
            timestamp=datetime.utcnow(),
            message=line.strip(),
            index_name=idx,
            next_index_name=nxt,
        )
    return None
```

The new code does NOT modify `_UPGRADE_PATTERNS`, `_GMS_PATTERNS`, `SweepState`, `SweepEvent`, or `_parse_line`. The existing log-monitor flow is preserved bit-for-bit.

- [ ] **Step 4: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v
```

Expected: 8 new tests pass + the existing log-monitor tests still pass (16 total in this file).

- [ ] **Step 5: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 102 pass (94 baseline + 8 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/log_monitor.py \
        smoke-test/tests/zdu/framework/test_log_monitor.py
git commit -m "feat(zdu): Phase1State + _parse_phase1_line for BuildIndicesIncrementalStep events"
```

---

## Task 3: Implement `UpgradeBlockingPhase`

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/upgrade_blocking.py`
- Create: `smoke-test/tests/zdu/framework/test_upgrade_blocking.py`

**Pattern:** Phase ABC subclass. Constructor takes injected `DockerComposeClient` + `MySQLClient` + the GMS service name + a job timeout. `run(ctx)` does:

1. Read token-service secrets from the running GMS container (re-using the working pattern from `phases/sweep_and_io.py` — the upgrade-job container needs them to start).
2. Launch the upgrade job via `docker.run_upgrade_job(env_overrides, service=upgrade_service, extra_args=["-u", "SystemUpdateBlocking"])`.
3. Stream `popen.stdout` line by line. For each line:
   - Try `_parse_phase1_line`. On `ALIAS_SWAPPED`, append `(index_name, next_index_name)` to `alias_swaps_observed`. On `REINDEX_FAILED`, mark the run as failed but keep reading until the process exits.
   - On any line, log it to `log.info("[upgrade] %s", line)` so users see progress.
4. Wait for `popen.wait()` with a timeout.
5. After exit:
   - If non-zero return code → `PhaseResult(status="failed", error=f"system-update exited rc={rc}")`.
   - Otherwise, query MySQL for any `dataHubUpgradeResult` aspect whose `metadata` JSON contains an `indicesState` field. Merge each entry into a list of `IndexState`. Find the one with the most entries (or matching the alias swaps we observed) and store as the canonical `UpgradeBlockingResult.raw`.

**The `extra_args` parameter on `run_upgrade_job` already exists** — added during Plan 0. Pass `["-u", "SystemUpdateBlocking"]`.

### 3.1 — Helper utility

Before writing the phase, isolate one piece of pure logic that's easier to unit-test on its own: the function that parses an `indicesState` payload into `list[IndexState]`. This is testable in pure Python without mocking anything.

- [ ] **Step 1: Write the failing test for the helper**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_upgrade_blocking.py`:

```python
"""Unit tests for UpgradeBlockingPhase and its parse_indices_state helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import IndexState, TestContext
from tests.zdu.framework.phases.upgrade_blocking import (
    UpgradeBlockingPhase,
    parse_indices_state,
)


class TestParseIndicesState:
    def test_full_payload(self) -> None:
        payload = {
            "datasetindex_v2": {
                "nextIndexName": "datasetindex_v2_1714000000",
                "oldBackingIndexName": "datasetindex_v2_old",
                "reindexStartTime": 1714000000,
                "sourceDocCount": 100,
                "taskId": "abc:42",
                "requiresDataBackfill": True,
                "status": "COMPLETED",
            },
            "dashboardindex_v2": {
                "nextIndexName": "dashboardindex_v2_1714000001",
                "oldBackingIndexName": "dashboardindex_v2_old",
                "reindexStartTime": 1714000001,
                "sourceDocCount": 0,
                "taskId": "",
                "requiresDataBackfill": False,
                "status": "COMPLETED",
            },
        }
        out = parse_indices_state(payload)
        assert {s.alias for s in out} == {"datasetindex_v2", "dashboardindex_v2"}
        ds = next(s for s in out if s.alias == "datasetindex_v2")
        assert ds.next_index_name == "datasetindex_v2_1714000000"
        assert ds.old_backing_index_name == "datasetindex_v2_old"
        assert ds.reindex_start_time == 1714000000
        assert ds.source_doc_count == 100
        assert ds.task_id == "abc:42"
        assert ds.requires_data_backfill is True
        assert ds.status == "COMPLETED"

    def test_missing_fields_default_to_none_or_zero(self) -> None:
        payload = {"datasetindex_v2": {"status": "IN_PROGRESS"}}
        out = parse_indices_state(payload)
        assert len(out) == 1
        s = out[0]
        assert s.alias == "datasetindex_v2"
        assert s.status == "IN_PROGRESS"
        assert s.next_index_name is None
        assert s.old_backing_index_name is None
        assert s.reindex_start_time is None
        assert s.source_doc_count == 0
        assert s.task_id is None
        assert s.requires_data_backfill is False

    def test_empty_payload_returns_empty_list(self) -> None:
        assert parse_indices_state({}) == []

    def test_non_dict_alias_value_skipped(self) -> None:
        # If a malformed entry has a non-dict value, skip it rather than crash.
        payload = {"datasetindex_v2": "not a dict", "dashboardindex_v2": {"status": "COMPLETED"}}
        out = parse_indices_state(payload)
        assert [s.alias for s in out] == ["dashboardindex_v2"]
```

- [ ] **Step 2: Run tests — expect failure**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_upgrade_blocking.py -v
```

Expected: `ImportError: cannot import name 'parse_indices_state'`.

- [ ] **Step 3: Create the phase file with the helper function**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/upgrade_blocking.py`:

```python
"""Phase 4 — UpgradeBlockingPhase.

Runs ``system-update -u SystemUpdateBlocking`` against the live stack and
captures the resulting ``DataHubUpgradeResult.indicesState[*]`` plus the
real-time log evidence of alias swaps into ``TestContext.upgrade_blocking``.

The phase does NOT rebuild containers — it assumes the running GMS image
already has the schema/mapping changes the upgrade job will apply. Use the
legacy ``UpgradePhase`` for image swaps.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import IndexState, TestContext, UpgradeBlockingResult
from ..docker_compose import DockerComposeClient
from ..log_monitor import Phase1State, _parse_phase1_line
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_TOKEN_KEYS = (
    "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
    "DATAHUB_TOKEN_SERVICE_SALT",
)
_DEFAULT_TIMEOUT_S = 600


def parse_indices_state(payload: dict) -> list[IndexState]:
    """Convert a parsed ``DataHubUpgradeResult.indicesState`` map into IndexState rows.

    Skips entries whose value is not a dict (defensive against malformed JSON).
    """
    out: list[IndexState] = []
    for alias, entry in payload.items():
        if not isinstance(entry, dict):
            log.warning("indicesState entry for %s is not a dict, skipping", alias)
            continue
        out.append(
            IndexState(
                alias=alias,
                next_index_name=entry.get("nextIndexName"),
                old_backing_index_name=entry.get("oldBackingIndexName"),
                reindex_start_time=entry.get("reindexStartTime"),
                source_doc_count=int(entry.get("sourceDocCount", 0) or 0),
                task_id=entry.get("taskId") or None,
                requires_data_backfill=bool(entry.get("requiresDataBackfill", False)),
                status=entry.get("status", "UNKNOWN"),
            )
        )
    return out


class UpgradeBlockingPhase(Phase):
    name = "upgrade_blocking"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        timeout_s: int = _DEFAULT_TIMEOUT_S,
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._timeout_s = timeout_s

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        token_env = self._read_token_env()
        proc = self._launch(token_env)

        alias_swaps: list[tuple[str, str]] = []
        any_failure = False
        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                line = raw_line.rstrip()
                if not line:
                    continue
                log.info("[upgrade] %s", line)
                event = _parse_phase1_line(line)
                if event is None:
                    continue
                if event.state == Phase1State.ALIAS_SWAPPED:
                    if event.index_name is not None:
                        alias_swaps.append(
                            (event.index_name, event.next_index_name or "")
                        )
                elif event.state == Phase1State.REINDEX_FAILED:
                    any_failure = True
        finally:
            try:
                proc.wait(timeout=self._timeout_s)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                duration = time.monotonic() - t0
                log.error("UpgradeBlocking timed out after %.1fs", duration)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=duration,
                    error=f"system-update exited via timeout after {self._timeout_s}s",
                )

        rc = proc.returncode
        duration = time.monotonic() - t0
        if rc != 0:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=f"system-update exited rc={rc}",
                details={"alias_swaps_observed": alias_swaps},
            )
        if any_failure:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error="alias swap failure observed in log stream",
                details={"alias_swaps_observed": alias_swaps},
            )

        captured = self._capture_indices_state(alias_swaps, duration)
        ctx.upgrade_blocking = captured
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "alias_swaps_observed": alias_swaps,
                "indices": [vars(i) for i in captured.indices],
                "upgrade_id": captured.upgrade_id,
                "duration_s": duration,
            },
        )

    def _read_token_env(self) -> dict[str, str]:
        env = self._docker.get_service_env(self._gms_service, list(_TOKEN_KEYS))
        if not env:
            log.warning(
                "Could not read token-service secrets from %s; upgrade container may fail",
                self._gms_service,
            )
        return env

    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
        return self._docker.run_upgrade_job(
            env_overrides=dict(token_env),
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateBlocking"],
        )

    def _capture_indices_state(
        self, alias_swaps: list[tuple[str, str]], duration: float
    ) -> UpgradeBlockingResult:
        """Find the DataHubUpgradeResult that contains indicesState and parse it.

        Phase 4's upgrade may write multiple DataHubUpgradeResult aspects (one per
        step). We accept the first one whose payload has an ``indicesState`` key.
        If none is found, return a result whose ``indices`` is empty but
        ``alias_swaps_observed`` still records what we saw in the log.
        """
        upgrade_id, raw = self._find_indices_state_aspect()
        indices = parse_indices_state(raw.get("indicesState", {})) if raw else []
        return UpgradeBlockingResult(
            indices=indices,
            alias_swaps_observed=alias_swaps,
            raw=raw,
            duration_s=duration,
            upgrade_id=upgrade_id,
        )

    def _find_indices_state_aspect(self) -> tuple[str | None, dict | None]:
        """Locate the dataHubUpgradeResult URN whose payload has indicesState.

        Returns (upgrade_id, parsed_payload) or (None, None) if absent.
        Implementation note: we don't know the exact upgrade-id used by
        SystemUpdateBlocking, so we delegate the URN discovery to MySQLClient
        via a list query rather than guessing.
        """
        return self._mysql.find_upgrade_result_with_field("indicesState")
```

Note: `parse_indices_state` is exposed as a module-level function (not a method) so it can be unit-tested without instantiating the phase.

This task references `MySQLClient.find_upgrade_result_with_field` which does NOT exist yet. Add it next.

- [ ] **Step 4: Add `find_upgrade_result_with_field` to `MySQLClient`**

In `smoke-test/tests/zdu/framework/mysql_client.py`, append a new method to the `MySQLClient` class:

```python
    def find_upgrade_result_with_field(
        self, field_name: str
    ) -> tuple[str | None, dict | None]:
        """Find any dataHubUpgradeResult whose parsed metadata contains ``field_name``.

        Returns ``(upgrade_id, parsed_dict)`` for the first match, or
        ``(None, None)`` if no match. ``upgrade_id`` is the URN suffix
        (e.g. ``"system-update-blocking"``). Used by ``UpgradeBlockingPhase``
        to discover the indicesState payload without hardcoding the URN.
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT urn, metadata FROM metadata_aspect_v2 "
                "WHERE aspect='dataHubUpgradeResult' AND version=0"
            )
            rows = cur.fetchall()
        for row in rows:
            md = row.get("metadata")
            if not md:
                continue
            try:
                parsed = json.loads(md)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and field_name in parsed:
                upgrade_id = row["urn"].removeprefix("urn:li:dataHubUpgrade:")
                return upgrade_id, parsed
        return None, None
```

- [ ] **Step 5: Add a unit test for `find_upgrade_result_with_field`**

Append to `smoke-test/tests/zdu/framework/test_mysql_client.py`:

```python
class TestFindUpgradeResultWithField:
    def test_returns_first_match(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:other", "metadata": json.dumps({"state": "SUCCEEDED"})},
            {
                "urn": "urn:li:dataHubUpgrade:system-update-blocking",
                "metadata": json.dumps({"indicesState": {"datasetindex_v2": {"status": "COMPLETED"}}}),
            },
        ]
        upgrade_id, payload = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "system-update-blocking"
        assert payload is not None
        assert "indicesState" in payload

    def test_returns_none_when_no_match(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:other", "metadata": json.dumps({"state": "SUCCEEDED"})},
        ]
        assert client.find_upgrade_result_with_field("indicesState") == (None, None)

    def test_skips_malformed_metadata(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:bad", "metadata": "{not json"},
            {
                "urn": "urn:li:dataHubUpgrade:good",
                "metadata": json.dumps({"indicesState": {}}),
            },
        ]
        upgrade_id, _ = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "good"

    def test_skips_non_dict_payload(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:bad", "metadata": "[]"},
            {
                "urn": "urn:li:dataHubUpgrade:good",
                "metadata": json.dumps({"indicesState": {}}),
            },
        ]
        upgrade_id, _ = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "good"
```

- [ ] **Step 6: Add unit tests for `UpgradeBlockingPhase` itself**

Append to `smoke-test/tests/zdu/framework/test_upgrade_blocking.py`:

```python
import io


def _popen_with_lines(lines: list[str], returncode: int = 0) -> MagicMock:
    """Build a MagicMock subprocess.Popen that yields ``lines`` on stdout and exits cleanly."""
    proc = MagicMock()
    proc.stdout = io.StringIO("\n".join(lines) + ("\n" if lines else ""))
    proc.returncode = returncode

    def wait(timeout: float | None = None) -> int:
        proc.returncode = returncode
        return returncode

    proc.wait.side_effect = wait
    proc.kill = MagicMock()
    return proc


@pytest.fixture
def docker() -> MagicMock:
    d = MagicMock()
    d.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k",
        "DATAHUB_TOKEN_SERVICE_SALT": "s",
    }
    return d


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.find_upgrade_result_with_field.return_value = (None, None)
    return m


@pytest.fixture
def phase(docker: MagicMock, mysql: MagicMock) -> UpgradeBlockingPhase:
    return UpgradeBlockingPhase(
        docker=docker,
        mysql=mysql,
        gms_service="datahub-gms-debug",
        upgrade_service="system-update-debug",
        timeout_s=60,
    )


class TestUpgradeBlockingPhaseHappyPath:
    def test_records_alias_swaps_and_indices_state(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            [
                "INFO  Starting incremental reindex",
                "INFO  Alias swapped: datasetindex_v2 -> datasetindex_v2_1714",
                "INFO  Index dashboardindex_v2 had 0 docs, next index created as empty, alias swapped",
                "INFO  job done",
            ],
            returncode=0,
        )
        mysql.find_upgrade_result_with_field.return_value = (
            "system-update-blocking",
            {
                "indicesState": {
                    "datasetindex_v2": {
                        "nextIndexName": "datasetindex_v2_1714",
                        "status": "COMPLETED",
                        "sourceDocCount": 100,
                    },
                    "dashboardindex_v2": {
                        "nextIndexName": "dashboardindex_v2_1714b",
                        "status": "COMPLETED",
                        "sourceDocCount": 0,
                    },
                }
            },
        )

        ctx = TestContext()
        result = phase.run(ctx)

        assert result.status == "passed", result.error
        assert ctx.upgrade_blocking is not None
        ub = ctx.upgrade_blocking
        assert ub.upgrade_id == "system-update-blocking"
        assert ("datasetindex_v2", "datasetindex_v2_1714") in ub.alias_swaps_observed
        # Empty-source variant has no next-index name in the log line; recorded as empty string.
        assert ("dashboardindex_v2", "") in ub.alias_swaps_observed
        assert {i.alias for i in ub.indices} == {"datasetindex_v2", "dashboardindex_v2"}

    def test_no_reindex_needed_still_passes(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  No indices require incremental reindex", "INFO  done"],
            returncode=0,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.upgrade_blocking is not None
        assert ctx.upgrade_blocking.alias_swaps_observed == []
        assert ctx.upgrade_blocking.indices == []


class TestUpgradeBlockingPhaseFailures:
    def test_nonzero_exit_marks_failed(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["ERROR  upgrade crashed"], returncode=1,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "rc=1" in (result.error or "")
        assert ctx.upgrade_blocking is None

    def test_alias_swap_failure_in_log_marks_failed(
        self, phase: UpgradeBlockingPhase, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            [
                "INFO  Starting incremental reindex",
                "ERROR  Alias swap failed for datasetindex_v2 -> datasetindex_v2_1714: doc count mismatch",
                "INFO  job done",
            ],
            returncode=0,  # job exits 0 even when a swap fails — log evidence is the source of truth
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "alias swap failure" in (result.error or "")


class TestUpgradeBlockingPhaseTimeout:
    def test_timeout_kills_process_and_returns_failed(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        # Build a Popen mock whose wait() raises TimeoutExpired.
        proc = MagicMock()
        proc.stdout = io.StringIO("")
        proc.wait.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=1)
        proc.kill = MagicMock()
        proc.returncode = -9
        docker.run_upgrade_job.return_value = proc

        phase = UpgradeBlockingPhase(
            docker=docker, mysql=mysql,
            gms_service="datahub-gms-debug", timeout_s=1,
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "timeout" in (result.error or "").lower()
        proc.kill.assert_called_once()
```

- [ ] **Step 7: Run all new tests**

```bash
smoke-test/venv/bin/python -m pytest \
  smoke-test/tests/zdu/framework/test_upgrade_blocking.py \
  smoke-test/tests/zdu/framework/test_mysql_client.py -v 2>&1 | tail -25
```

Expected: 4 (parse_indices_state) + 6 (UpgradeBlockingPhase) + 17 (mysql, was 13 + 4 new) = at least 27 newly passing tests in these two files.

- [ ] **Step 8: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 102 (Plan 2 Task 2 baseline) + 14 new = 116 pass.

- [ ] **Step 9: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/upgrade_blocking.py \
        smoke-test/tests/zdu/framework/test_upgrade_blocking.py \
        smoke-test/tests/zdu/framework/mysql_client.py \
        smoke-test/tests/zdu/framework/test_mysql_client.py
git commit -m "feat(zdu): UpgradeBlockingPhase — runs SystemUpdateBlocking, captures DataHubUpgradeResult"
```

---

## Task 4: Wire `UpgradeBlockingPhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert AFTER the legacy `("upgrade", ...)` and BEFORE `("sweep_and_io", ...)`. Both `upgrade` (rebuild) and `upgrade_blocking` (system-update job) coexist; users skip whichever they don't need via `ZDU_SKIP_PHASES`.

- [ ] **Step 1: Add the import**

In the imports block at module top, alongside other phase imports:

```python
from .phases.upgrade_blocking import UpgradeBlockingPhase
```

- [ ] **Step 2: Insert into the `phases` list**

In `ZDUTestRunner.run()`, the phases list currently has:

```python
        phases = [
            ("discovery", DiscoveryPhase(...)),
            ("seed", SeedPhase(...)),
            ("snapshot_t0", SnapshotT0Phase(...)),
            ("upgrade", UpgradePhase(...)),
            ("sweep_and_io", SweepAndIOPhase(...)),
            ("validation", ValidationPhase(...)),
        ]
```

Insert `upgrade_blocking` between `upgrade` and `sweep_and_io`:

```python
            (
                "upgrade_blocking",
                UpgradeBlockingPhase(
                    docker=self._docker,
                    mysql=self._mysql,
                    gms_service=self._config.gms_service,
                    upgrade_service=self._config.upgrade_service,
                    timeout_s=self._config.sweep_timeout_s,
                ),
            ),
```

The `gms_service`, `upgrade_service`, and `sweep_timeout_s` already exist on `ZDUTestConfig`.

- [ ] **Step 3: Smoke-test that the runner constructs**

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

- [ ] **Step 4: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ -v 2>&1 | tail -5
```

Expected: 116 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire UpgradeBlockingPhase into the runner pipeline"
```

---

## Task 5: Live integration check

**Pre-requisite:** Compose stack up via `scripts/dev/datahub-dev.sh status` reporting `ready: true`.

This is the validation step that catches contract issues — same pattern as Plan 1 Task 5.

- [ ] **Step 1: Run `UpgradeBlockingPhase` directly against the live stack**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.docker_compose import DockerComposeClient
from tests.zdu.framework.mysql_client import MySQLClient
from tests.zdu.framework.phases.upgrade_blocking import UpgradeBlockingPhase

cfg = ZDUTestConfig()
docker = DockerComposeClient(
    cfg.project_dir, cfg.compose_files, cfg.compose_profiles,
)
mysql = MySQLClient(
    host=cfg.mysql_host, port=cfg.mysql_port, user=cfg.mysql_user,
    password=cfg.mysql_password, database=cfg.mysql_database,
)
phase = UpgradeBlockingPhase(
    docker=docker, mysql=mysql,
    gms_service=cfg.gms_service,
    upgrade_service=cfg.upgrade_service,
    timeout_s=cfg.sweep_timeout_s,
)
ctx = TestContext()
result = phase.run(ctx)
print(f"phase status: {result.status}")
print(f"duration: {result.duration_s:.1f}s")
print(f"error: {result.error}")

ub = ctx.upgrade_blocking
if ub is None:
    raise SystemExit("FAIL: ctx.upgrade_blocking not populated")

print(f"\nupgrade_id: {ub.upgrade_id}")
print(f"alias_swaps_observed ({len(ub.alias_swaps_observed)}): {ub.alias_swaps_observed[:5]}")
print(f"indices ({len(ub.indices)}):")
for i in ub.indices[:5]:
    print(f"  {i.alias}: status={i.status} next={i.next_index_name} src_count={i.source_doc_count}")

# Sanity: in a fresh DataHub stack, no Phase 1 reindex should be needed.
# The job runs the full SystemUpdateBlocking sequence (which includes
# BuildIndicesIncrementalStep + many other steps), exits 0, and we should
# see "No indices require incremental reindex" or zero alias swaps.
assert result.status == "passed", f"phase failed: {result.error}"
print("\nLIVE OK")
PY
```

Expected: `phase status: passed`, duration well under `sweep_timeout_s`, **0 alias swaps observed in a fresh stack** (no schema changes since boot), and either `upgrade_id=None` (no `indicesState`-bearing aspect yet) or a small one if any prior run wrote it.

If this fails — particularly with a `subprocess` or `Popen` issue — diagnose before commit. Real-stack contract issues are exactly what Plan 0 caught twice.

- [ ] **Step 2: Run Suite A E2E with `upgrade_blocking` in the path**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -40
```

Expected: pipeline runs `discovery → seed → snapshot_t0 → upgrade_blocking → sweep_and_io → validation`. The `upgrade_blocking` phase prints alias-swap log evidence (none in a fresh stack), exits passed. Suite A scenarios produce identical results to the Plan 1 baseline (14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL).

- [ ] **Step 3: Verify the JSON report includes `upgrade_blocking`**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python -c "
import json, os
# Note: report path is relative to runner cwd; check both possible locations.
for p in ('build/zdu-test-report.json', 'smoke-test/build/zdu-test-report.json'):
    if os.path.exists(p):
        r = json.load(open(p))
        phases = [(x['name'], x['status']) for x in r['phases']]
        print(f'{p}: phases={phases}')
        if 'upgrade_blocking' in [x['name'] for x in r['phases']]:
            up = next(x for x in r['phases'] if x['name'] == 'upgrade_blocking')
            print(f'  upgrade_blocking details:')
            print(f'    alias_swaps={up[\"details\"].get(\"alias_swaps_observed\")}')
            print(f'    indices count={len(up[\"details\"].get(\"indices\", []))}')
            print(f'    upgrade_id={up[\"details\"].get(\"upgrade_id\")}')
            break
else:
    print('no report found in either location')
"
```

Expected: a phase entry for `upgrade_blocking` with `status=passed` and the alias-swaps / indices fields populated (or empty, in a fresh stack).

- [ ] **Step 4: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in upgrade_blocking"
```

If nothing regressed, no commit needed.

---

## Task 6: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff 4e86ef2e45..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-2.diff
wc -l /tmp/zdu-plan-2.diff
```

(`4e86ef2e45` is the last Plan 1 commit. If it's not the right boundary, use `git log --oneline | head -10` to find the correct one.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send the reviewer this prompt:

> Review the diff at `/tmp/zdu-plan-2.diff`. This PR adds Phase 4 (`UpgradeBlockingPhase`) to the ZDU E2E framework. It runs `system-update -u SystemUpdateBlocking` against the live stack via Foundation's `DockerComposeClient`, streams the upgrade-job stdout, parses Phase-1-specific log lines (alias swap, no-reindex, skip, failure), and captures the resulting `DataHubUpgradeResult.indicesState` from MySQL into a new `UpgradeBlockingResult` dataclass on `TestContext`.
>
> Check specifically:
>
> 1. **Phase ABC compliance:** consistent `name = "upgrade_blocking"` class attr.
> 2. **Subprocess lifecycle:** the popen is always waited; on timeout it's killed and waited; no zombie risk.
> 3. **Log parser cohesion:** `_parse_phase1_line` lives next to existing `_parse_line` in `log_monitor.py` and does not modify the existing `SweepEvent`/`_parse_line` flow.
> 4. **Defensive parsing:** `parse_indices_state` skips non-dict entries; `find_upgrade_result_with_field` skips malformed JSON and non-dict payloads.
> 5. **Type hints:** complete; no unjustified `Any` outside JSON-passthrough boundaries.
> 6. **DI:** clients injected, never constructed in the phase.
> 7. **Test quality:** popen mocks use `io.StringIO` for stdout; the timeout test triggers a real `subprocess.TimeoutExpired`; tests verify behavior, not mock internals.
> 8. **YAGNI:** no `restart`, `cleanup`, `rollback`, `__repr__`, or extra public methods on the phase.
> 9. **The `parse_indices_state` helper is exported at module level** (not a method) so it's testable independently — confirm.
> 10. **No coupling to legacy `UpgradePhase`:** UpgradeBlockingPhase doesn't read/depend on the rebuild step.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on UpgradeBlockingPhase"
```

---

## Self-Review

**Spec coverage** (against design doc Section 5.4 + Section 14.1 Phase 4):

| Requirement                                                            | Task                                                                                  |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `system-update-debug` running `-u SystemUpdateBlocking`                | Task 3 (`_launch` calls `run_upgrade_job(extra_args=["-u", "SystemUpdateBlocking"])`) |
| Watch `BuildIndicesIncrementalStep` events                             | Task 2 (`_parse_phase1_line` patterns) + Task 3 (loop reads stdout)                   |
| `Alias swapped: {alias} -> {nextIndex}` recorded                       | Task 3 (`alias_swaps.append`)                                                         |
| `Resuming polling for index ...` line observed (TC-110)                | Task 2 (pattern present); TC-110 itself comes when Suite B lands                      |
| `DataHubUpgradeResult.indicesState[*]` captured                        | Task 3 (`_capture_indices_state` + new `MySQLClient.find_upgrade_result_with_field`)  |
| Required keys per index recorded                                       | Task 3 (`parse_indices_state` reads all 7 fields)                                     |
| Old physical index still exists after alias swap (TC-101)              | Out of scope — Suite B's TC-101 executor will check this against ES post-phase        |
| Re-run after COMPLETED emits "already COMPLETED ... skipping" (TC-109) | Task 2 (pattern present); TC-109 itself in Suite B                                    |

**Gaps deliberately deferred:**

- `BUILD_INDICES_REINDEX_DELAY_MS` Java hook — its own plan.
- Suite B TC executors — separate plan after Plan 2 lands.
- Per-index `Polling task {taskId}: status=..., completed=N/M` event — useful for progress reporting but not required for any current TC. Add when Suite B's TC-110 needs it.

**Placeholder scan:** None. Every step has the actual code.

**Type / signature consistency:**

- `IndexState(alias: str, ...)` — Task 1 dataclass; Task 3 `parse_indices_state` constructs it; Task 3 `_capture_indices_state` returns `list[IndexState]`.
- `UpgradeBlockingResult(indices: list[IndexState], alias_swaps_observed: list[tuple[str, str]], raw: dict | None, duration_s: float, upgrade_id: str | None)` — Task 1 dataclass; Task 3 phase populates it; Task 4 runner reads `ctx.upgrade_blocking` (no, the runner doesn't — only future Suite B/D executors do).
- `Phase1State` enum + `Phase1Event` dataclass + `_parse_phase1_line` — all defined in Task 2; consumed in Task 3.
- `MySQLClient.find_upgrade_result_with_field(field_name: str) -> tuple[str | None, dict | None]` — Task 3 Step 4 adds; Task 3 Step 5 tests; Task 3 Step 6 phase calls.
- `UpgradeBlockingPhase(docker, mysql, gms_service, upgrade_service, timeout_s)` — Task 3 implements; Task 4 runner instantiates with the same signature.

**Risks called out:**

- `_find_indices_state_aspect` performs a table scan over `metadata_aspect_v2 WHERE aspect='dataHubUpgradeResult'`. In production this could be many rows; in dev it's a handful. Acceptable for now; if it becomes a hot path, an upgrade-id discovery hint can be added.
- The `alias_swaps_observed = ('dashboardindex_v2', '')` empty-second-element case for the empty-source fast path is technically a special value rather than `None`. Test `test_records_alias_swaps_and_indices_state` asserts the exact tuple. Consumers that care about the next-index name will check truthiness.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-06-zdu-plan-2-upgrade-blocking.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
