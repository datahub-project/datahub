# ZDU E2E — Plan 7: Phase 8 `UpgradeNonBlockingPhase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy `SweepAndIOPhase` placeholder with `UpgradeNonBlockingPhase` — the post-rolling-restart phase that runs `system-update -u SystemUpdateNonBlocking` against the live NEW GMS while concurrent reader/writer threads continue. The new phase captures three things the legacy placeholder did not: the post-sweep `DataHubUpgradeResult.indicesState` (symmetric to Plan 2's blocking capture), the per-index `DUAL_WRITE_DISABLED` markings observed in upgrade-job logs, and the `IncrementalReindexCatchUpStep` catch-up windows. These captures unblock Phase 10 Validation's "alias targets and dual-write state" assertion (design doc §5.10 dimension 5).

**Architecture:** The implementation is a **rename + extend** of `SweepAndIOPhase`. The legacy file is renamed to `phases/upgrade_nonblocking.py`, the class to `UpgradeNonBlockingPhase`, and the existing concurrent-IO harness is preserved verbatim. Three additions: (1) the upgrade subprocess is launched with explicit `-u SystemUpdateNonBlocking` extra args and a new `mysql: MySQLClient` injection; (2) the `LogMonitor` gains an optional `nonblocking_queue` and a `_parse_nonblocking_line` parser for two new log patterns; (3) at sweep `COMPLETED`, the phase queries MySQL for the post-sweep `DataHubUpgradeResult` and assembles `UpgradeNonBlockingResult` into `ctx.upgrade_nonblocking`. The runner tuple key flips from `sweep_and_io` → `upgrade_nonblocking`. No other phase code changes.

**Tech Stack:** Python 3 (existing). Reuses Foundation clients (`MySQLClient.find_upgrade_result_with_field` already used by Plan 2). No new dependencies.

**Out of scope (deferred):**

- TC-401 sweep cursor resumability — separate scenario.
- TC-307 catch-up resume from `lastUrn` checkpoint — separate scenario.
- TC-308 / TC-309 rollback flag toggling — needs runtime config knob.
- TC-204 rollback-flag-off → NEW only — Phase 7 deferral, still deferred.
- TC-501 / TC-502 read latency / write throughput SLOs — Phase 9's job (future Plan 8).
- Race-window assertions (TC-403) — existing IO harness exposes `ctx.io_write_results`; Phase 10 asserts.
- `AspectMigrationMutatorChain disabled` capture — design doc TC-019/TC-408 territory; Phase 10 will read raw upgrade-result for this if needed.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                MODIFY — add UpgradeNonBlockingResult dataclass + ctx.upgrade_nonblocking slot
├── log_monitor.py                            MODIFY — add NonBlockingState + NonBlockingEvent + _parse_nonblocking_line + LogMonitor.attach_nonblocking_queue
├── test_log_monitor.py                       MODIFY — add 4 parser unit tests
├── runner.py                                 MODIFY — import UpgradeNonBlockingPhase, replace sweep_and_io tuple
├── phases/
│   ├── sweep_and_io.py                       DELETE — replaced by upgrade_nonblocking.py
│   └── upgrade_nonblocking.py                CREATE — UpgradeNonBlockingPhase (renamed from SweepAndIOPhase, extended)
└── test_upgrade_nonblocking.py               CREATE — phase unit tests with mocked clients
```

The phase reads `ctx.io_pool_entities` (Plan F-1), writes to `ctx.upgrade_nonblocking` (new), and continues to write `ctx.io_observations`, `ctx.io_write_results`, `ctx.sweep_total_migrated` (existing harness fields preserved).

---

## Task 1: Add `UpgradeNonBlockingResult` dataclass + ctx slot

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Same shape as `UpgradeBlockingResult` but with two new fields capturing non-blocking-specific outputs.

- [ ] **Step 1: Add the dataclass**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`, locate the `UpgradeBlockingResult` dataclass (around line 84). Insert AFTER it, BEFORE the `RollingRestartResult` dataclass:

```python
@dataclass
class UpgradeNonBlockingResult:
    """Captured by ``UpgradeNonBlockingPhase`` after ``system-update -u SystemUpdateNonBlocking``.

    ``indices`` is the structured view derived from the post-sweep
    ``DataHubUpgradeResult.indicesState`` — symmetric to ``UpgradeBlockingResult.indices``
    but reflects the dual-write-disable / catch-up state, not the alias swap state.
    ``dual_write_disabled_indices`` is the list of physical index names the framework
    observed transition to ``DUAL_WRITE_DISABLED`` via the log line
    ``Marked index {name} as DUAL_WRITE_DISABLED``. ``catch_up_windows`` maps physical
    index name → ``(T0_ms, T1_ms)`` parsed from
    ``Catch-up for entity index {name}: window [{T0}, {T1}]``.
    ``raw`` is the full parsed ``DataHubUpgradeResult`` aspect for failure-bundle dumps.
    """

    indices: list[IndexState] = field(default_factory=list)
    dual_write_disabled_indices: list[str] = field(default_factory=list)
    catch_up_windows: dict[str, tuple[int, int]] = field(default_factory=dict)
    raw: dict | None = None
    duration_s: float = 0.0
    upgrade_id: str | None = None
```

- [ ] **Step 2: Add the slot to TestContext**

In the `TestContext` dataclass, locate the `# SweepAndIOPhase writes` block (currently around line 156). Replace the whole block with:

```python
    # UpgradeNonBlockingPhase writes
    upgrade_nonblocking: UpgradeNonBlockingResult | None = None
    sweep_events: Queue = field(default_factory=Queue)
    io_observations: list[IOObservation] = field(default_factory=list)
    io_write_results: list[IOWriteResult] = field(default_factory=list)
    sweep_total_migrated: int = 0
    # Dedicated entities for concurrent-write testing (separate from scenario entities)
    io_pool_entities: list[SeededEntity] = field(default_factory=list)
```

(Only the comment and the new `upgrade_nonblocking` field are new; the rest is unchanged.)

- [ ] **Step 3: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import TestContext, UpgradeNonBlockingResult, IndexState
ctx = TestContext()
assert ctx.upgrade_nonblocking is None
r = UpgradeNonBlockingResult(
    indices=[IndexState(alias='dashboardindex_v2', status='COMPLETED')],
    dual_write_disabled_indices=['dashboardindex_v2_old'],
    catch_up_windows={'dashboardindex_v2_new': (1700000000, 1700000300)},
)
ctx.upgrade_nonblocking = r
assert ctx.upgrade_nonblocking.dual_write_disabled_indices == ['dashboardindex_v2_old']
print('OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 153 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add UpgradeNonBlockingResult dataclass + ctx slot for Phase 8"
```

Re-stage if pre-commit hooks reformat.

---

## Task 2: Add `_parse_nonblocking_line` parser + LogMonitor extension + parser tests

**Files:**

- Modify: `smoke-test/tests/zdu/framework/log_monitor.py`
- Modify: `smoke-test/tests/zdu/framework/test_log_monitor.py`

**Pattern:** Mirrors the existing `_parse_phase1_line` (Plan 2) and `_parse_dual_write_line` (Plan 4) parsers. The new parser recognizes two log lines emitted by `IncrementalReindexCatchUpStep` during `SystemUpdateNonBlocking`. The LogMonitor extension is a single optional queue that defaults to None — when present, parsed events are pushed.

### 2.1 — Write failing parser tests

- [ ] **Step 1: Add tests to `test_log_monitor.py`**

Open `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_log_monitor.py`. Append:

```python
# ---------- _parse_nonblocking_line tests ----------


from tests.zdu.framework.log_monitor import (
    NonBlockingEvent,
    NonBlockingState,
    _parse_nonblocking_line,
)


class TestParseNonBlockingLine:
    def test_dual_write_disabled_line(self) -> None:
        line = "Marked index dashboardindex_v2_old as DUAL_WRITE_DISABLED"
        ev = _parse_nonblocking_line(line)
        assert ev is not None
        assert ev.state == NonBlockingState.DUAL_WRITE_DISABLED
        assert ev.index_name == "dashboardindex_v2_old"
        assert ev.window is None

    def test_catch_up_window_line(self) -> None:
        line = (
            "Catch-up for entity index dashboardindex_v2_new: "
            "window [1700000000, 1700000300]"
        )
        ev = _parse_nonblocking_line(line)
        assert ev is not None
        assert ev.state == NonBlockingState.CATCH_UP_WINDOW
        assert ev.index_name == "dashboardindex_v2_new"
        assert ev.window == (1700000000, 1700000300)

    def test_unrelated_line_returns_none(self) -> None:
        assert _parse_nonblocking_line("Sweep complete. Total migrated: 42") is None
        assert _parse_nonblocking_line("Alias swapped: x -> y") is None
        assert _parse_nonblocking_line("") is None

    def test_malformed_window_returns_none(self) -> None:
        # Missing comma between epochs
        line = "Catch-up for entity index foo: window [1700000000 1700000300]"
        assert _parse_nonblocking_line(line) is None
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'NonBlockingEvent'` (or similar collection error).

### 2.2 — Implement parser + types

- [ ] **Step 3: Add types and parser to `log_monitor.py`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/log_monitor.py`, add the following block AFTER the `_parse_dual_write_line` function (use `Read` to find the exact location around line 310+):

```python
class NonBlockingState(Enum):
    """Events emitted by ``IncrementalReindexCatchUpStep`` during ``SystemUpdateNonBlocking``."""

    DUAL_WRITE_DISABLED = "dual_write_disabled"
    CATCH_UP_WINDOW = "catch_up_window"


@dataclass
class NonBlockingEvent:
    state: NonBlockingState
    index_name: str
    timestamp: datetime
    message: str
    window: tuple[int, int] | None = None


_DUAL_WRITE_DISABLED_RE = re.compile(
    r"Marked index (?P<idx>\S+) as DUAL_WRITE_DISABLED"
)
_CATCH_UP_WINDOW_RE = re.compile(
    r"Catch-up for entity index (?P<idx>\S+): window \[(?P<t0>\d+),\s*(?P<t1>\d+)\]"
)


def _parse_nonblocking_line(line: str) -> NonBlockingEvent | None:
    if not line:
        return None
    m = _DUAL_WRITE_DISABLED_RE.search(line)
    if m:
        return NonBlockingEvent(
            state=NonBlockingState.DUAL_WRITE_DISABLED,
            index_name=m.group("idx"),
            timestamp=datetime.utcnow(),
            message=line,
        )
    m = _CATCH_UP_WINDOW_RE.search(line)
    if m:
        try:
            t0 = int(m.group("t0"))
            t1 = int(m.group("t1"))
        except ValueError:
            return None
        return NonBlockingEvent(
            state=NonBlockingState.CATCH_UP_WINDOW,
            index_name=m.group("idx"),
            timestamp=datetime.utcnow(),
            message=line,
            window=(t0, t1),
        )
    return None
```

- [ ] **Step 4: Plumb the new parser into `LogMonitor`**

Find the `LogMonitor.__init__` method in `log_monitor.py`. Add an optional parameter `nonblocking_queue: Queue[NonBlockingEvent] | None = None` to the constructor signature and store it as `self._nonblocking_queue = nonblocking_queue`.

Find the loop body inside `LogMonitor` that consumes upgrade-job stdout (the same loop that calls `_parse_line` for SweepEvent). After the existing parse calls, add:

```python
            if self._nonblocking_queue is not None:
                nb_event = _parse_nonblocking_line(line)
                if nb_event is not None:
                    self._nonblocking_queue.put(nb_event)
```

(Use `Read` to identify the exact insertion point — the loop where `_parse_line` is called.)

- [ ] **Step 5: Run parser tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v 2>&1 | tail -15
```

Expected: all parser tests pass (4 new + however many existed).

- [ ] **Step 6: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 157 pass (153 baseline + 4 new parser tests).

- [ ] **Step 7: Commit**

```bash
git add smoke-test/tests/zdu/framework/log_monitor.py \
        smoke-test/tests/zdu/framework/test_log_monitor.py
git commit -m "feat(zdu): NonBlockingEvent parser + LogMonitor queue plumb-through"
```

Re-stage if pre-commit hooks reformat.

---

## Task 3: Implement `UpgradeNonBlockingPhase` (rename + extend `SweepAndIOPhase`)

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/upgrade_nonblocking.py`
- Create: `smoke-test/tests/zdu/framework/test_upgrade_nonblocking.py`
- Delete: `smoke-test/tests/zdu/framework/phases/sweep_and_io.py`

**Pattern:** This is a **rename + extend**, not a rewrite. The new file is the legacy `sweep_and_io.py` with these surgical changes:

1. Class renamed `SweepAndIOPhase` → `UpgradeNonBlockingPhase`.
2. `name = "sweep_and_io"` → `name = "upgrade_nonblocking"`.
3. Constructor gains `mysql: MySQLClient` parameter (after `datahub`).
4. The two `run_upgrade_job(...)` calls inside `_run_sweep` gain `extra_args=["-u", "SystemUpdateNonBlocking"]`.
5. A new `nonblocking_queue: Queue[NonBlockingEvent] = Queue()` is instantiated in `run()` and passed into `LogMonitor(...)`.
6. After the sweep `COMPLETED` branch sets `ctx.sweep_total_migrated`, drain the `nonblocking_queue` and capture the upgrade result via `mysql.find_upgrade_result_with_field("indicesState")`. Build an `UpgradeNonBlockingResult` and assign to `ctx.upgrade_nonblocking`.

The concurrent-IO harness (`ConcurrentIOHarness`) and all sweep-event parsing logic are preserved verbatim. Suite A baseline relies on this — do not touch the harness.

### 3.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_upgrade_nonblocking.py`:

```python
"""Unit tests for UpgradeNonBlockingPhase — uses mocked Docker/DataHub/MySQL clients."""

from __future__ import annotations

from queue import Queue
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import IndexState, TestContext, UpgradeNonBlockingResult
from tests.zdu.framework.log_monitor import NonBlockingEvent, NonBlockingState, SweepEvent, SweepState
from tests.zdu.framework.phases.upgrade_nonblocking import UpgradeNonBlockingPhase


@pytest.fixture
def config() -> ZDUTestConfig:
    cfg = ZDUTestConfig()
    cfg.sweep_timeout_s = 30
    cfg.reader_workers = 0
    cfg.writer_workers = 0
    return cfg


@pytest.fixture
def docker() -> MagicMock:
    m = MagicMock()
    m.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "k",
        "DATAHUB_TOKEN_SERVICE_SALT": "s",
    }
    proc = MagicMock()
    proc.stdout = iter([])
    m.run_upgrade_job.return_value = proc
    return m


@pytest.fixture
def datahub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mysql() -> MagicMock:
    m = MagicMock()
    m.find_upgrade_result_with_field.return_value = (
        "urn:li:dataHubUpgrade:SystemUpdateNonBlocking",
        {
            "indicesState": {
                "dashboardindex_v2": {
                    "nextIndexName": "dashboardindex_v2_new",
                    "oldBackingIndexName": "dashboardindex_v2_old",
                    "sourceDocCount": 100,
                    "status": "DUAL_WRITE_DISABLED",
                }
            }
        },
    )
    return m


@pytest.fixture
def phase(
    config: ZDUTestConfig, docker: MagicMock, datahub: MagicMock, mysql: MagicMock
) -> UpgradeNonBlockingPhase:
    return UpgradeNonBlockingPhase(config=config, docker=docker, datahub=datahub, mysql=mysql)


class TestUpgradeNonBlockingPhase:
    def test_passes_explicit_nonblocking_extra_args_to_run_upgrade_job(
        self, phase: UpgradeNonBlockingPhase, docker: MagicMock
    ) -> None:
        # Arrange a sweep that completes immediately by injecting a COMPLETED event.
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=0,
        )
        ctx = TestContext()

        with patch.object(
            UpgradeNonBlockingPhase, "_drain_nonblocking_queue", return_value=([], {})
        ):
            with patch(
                "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
            ) as mon_cls:
                mon = mon_cls.return_value
                mon.start.return_value = None
                # Simulate the sweep_queue.get returning COMPLETED on first poll.
                with patch.object(Queue, "get", return_value=completed):
                    phase.run(ctx)

        # Verify run_upgrade_job was called with -u SystemUpdateNonBlocking
        calls = docker.run_upgrade_job.call_args_list
        assert len(calls) >= 1
        for call in calls:
            kwargs = call.kwargs
            assert kwargs.get("extra_args") == ["-u", "SystemUpdateNonBlocking"]

    def test_captures_upgrade_nonblocking_result_on_completion(
        self,
        phase: UpgradeNonBlockingPhase,
        mysql: MagicMock,
    ) -> None:
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=42,
        )
        nb_events = [
            NonBlockingEvent(
                state=NonBlockingState.DUAL_WRITE_DISABLED,
                index_name="dashboardindex_v2_old",
                timestamp=__import__("datetime").datetime.utcnow(),
                message="Marked index dashboardindex_v2_old as DUAL_WRITE_DISABLED",
            ),
            NonBlockingEvent(
                state=NonBlockingState.CATCH_UP_WINDOW,
                index_name="dashboardindex_v2_new",
                timestamp=__import__("datetime").datetime.utcnow(),
                message="Catch-up for entity index dashboardindex_v2_new: window [100, 200]",
                window=(100, 200),
            ),
        ]
        ctx = TestContext()

        with patch(
            "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
        ) as mon_cls:
            mon = mon_cls.return_value
            mon.start.return_value = None
            with patch.object(Queue, "get", return_value=completed):
                # Pre-stuff the nonblocking queue
                with patch.object(
                    UpgradeNonBlockingPhase,
                    "_drain_nonblocking_queue",
                    return_value=(
                        ["dashboardindex_v2_old"],
                        {"dashboardindex_v2_new": (100, 200)},
                    ),
                ):
                    result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.upgrade_nonblocking is not None
        assert ctx.upgrade_nonblocking.dual_write_disabled_indices == [
            "dashboardindex_v2_old"
        ]
        assert ctx.upgrade_nonblocking.catch_up_windows == {
            "dashboardindex_v2_new": (100, 200)
        }
        # indicesState parsed from the mocked DataHubUpgradeResult fixture
        assert len(ctx.upgrade_nonblocking.indices) == 1
        assert ctx.upgrade_nonblocking.indices[0].status == "DUAL_WRITE_DISABLED"
        assert ctx.upgrade_nonblocking.upgrade_id == "urn:li:dataHubUpgrade:SystemUpdateNonBlocking"
        # Sweep total still recorded (preserves Suite A behavior)
        assert ctx.sweep_total_migrated == 42

    def test_capture_result_skipped_when_mysql_returns_none(
        self,
        phase: UpgradeNonBlockingPhase,
        mysql: MagicMock,
    ) -> None:
        # MySQL has no upgrade result yet (fresh deployment) — phase still passes,
        # ctx.upgrade_nonblocking gets an empty-indices result.
        mysql.find_upgrade_result_with_field.return_value = (None, None)
        completed = SweepEvent(
            state=SweepState.COMPLETED,
            source="datahub-upgrade",
            timestamp=__import__("datetime").datetime.utcnow(),
            message="done",
            total_migrated=0,
        )
        ctx = TestContext()

        with patch(
            "tests.zdu.framework.phases.upgrade_nonblocking.LogMonitor"
        ) as mon_cls:
            mon_cls.return_value.start.return_value = None
            with patch.object(Queue, "get", return_value=completed):
                with patch.object(
                    UpgradeNonBlockingPhase,
                    "_drain_nonblocking_queue",
                    return_value=([], {}),
                ):
                    result = phase.run(ctx)

        assert result.status == "passed"
        assert ctx.upgrade_nonblocking is not None
        assert ctx.upgrade_nonblocking.indices == []
        assert ctx.upgrade_nonblocking.upgrade_id is None
        assert ctx.upgrade_nonblocking.raw is None
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_upgrade_nonblocking.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'UpgradeNonBlockingPhase'`.

### 3.2 — Implement the phase (rename `sweep_and_io.py` → `upgrade_nonblocking.py`)

- [ ] **Step 3: Copy `sweep_and_io.py` to `upgrade_nonblocking.py`**

```bash
cd <REPO_ROOT>
cp smoke-test/tests/zdu/framework/phases/sweep_and_io.py \
   smoke-test/tests/zdu/framework/phases/upgrade_nonblocking.py
```

- [ ] **Step 4: Apply the surgical edits**

Open `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/upgrade_nonblocking.py` and apply these changes:

**Edit 1: Module docstring at the top (file currently has none) — insert before `from __future__`:**

```python
"""Phase 8 — UpgradeNonBlockingPhase.

Runs ``system-update -u SystemUpdateNonBlocking`` against the live NEW GMS
while concurrent reader/writer threads continue. Captures:

- ``DataHubUpgradeResult.indicesState`` (post-sweep alias / dual-write state).
- Per-index ``DUAL_WRITE_DISABLED`` markings observed in upgrade-job logs.
- ``IncrementalReindexCatchUpStep`` ``[T0, T1]`` windows per index.
- The legacy concurrent-IO harness data (``ctx.io_observations``,
  ``ctx.io_write_results``, ``ctx.sweep_total_migrated``).

Replaces the legacy ``SweepAndIOPhase`` placeholder. The runner's pipeline
tuple key changes from ``"sweep_and_io"`` to ``"upgrade_nonblocking"``.
"""

```

**Edit 2: Imports — add `Queue` for the nonblocking queue, `NonBlockingEvent`, `NonBlockingState`, and `MySQLClient`.**

Replace the existing import block:

```python
from .base import Phase, PhaseResult
from ..config import ZDUTestConfig
from ..context import IOObservation, IOWriteResult, SeededEntity, TestContext
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient
from ..log_monitor import LogMonitor, SweepEvent, SweepState
```

with:

```python
from .base import Phase, PhaseResult
from ..config import ZDUTestConfig
from ..context import (
    IOObservation,
    IOWriteResult,
    SeededEntity,
    TestContext,
    UpgradeNonBlockingResult,
)
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient
from ..log_monitor import (
    LogMonitor,
    NonBlockingEvent,
    NonBlockingState,
    SweepEvent,
    SweepState,
)
from ..mysql_client import MySQLClient
from .upgrade_blocking import parse_indices_state
```

**Edit 3: Rename the class and add MySQL injection.**

Replace:

```python
class SweepAndIOPhase(Phase):
    name = "sweep_and_io"

    def __init__(
        self,
        config: ZDUTestConfig,
        docker: DockerComposeClient,
        datahub: DataHubClient,
    ) -> None:
        self._config = config
        self._docker = docker
        self._datahub = datahub
```

with:

```python
class UpgradeNonBlockingPhase(Phase):
    name = "upgrade_nonblocking"

    def __init__(
        self,
        config: ZDUTestConfig,
        docker: DockerComposeClient,
        datahub: DataHubClient,
        mysql: MySQLClient,
    ) -> None:
        self._config = config
        self._docker = docker
        self._datahub = datahub
        self._mysql = mysql
```

**Edit 4: Wire the nonblocking queue into LogMonitor.**

Inside `run()`, locate the existing block:

```python
        # sweep_queue: all log events → consumed by _run_sweep control loop
        # writer_queue: only BATCH_URNS events → forwarded by _run_sweep, consumed by writers
        sweep_queue: Queue[SweepEvent] = Queue()
        writer_queue: Queue[SweepEvent] = Queue()
        ctx.sweep_events = sweep_queue

        monitor = LogMonitor(
            self._docker,
            sweep_queue,
            since=datetime.utcnow(),
            gms_service=self._config.gms_service,
        )
```

and change it to:

```python
        # sweep_queue: all log events → consumed by _run_sweep control loop
        # writer_queue: only BATCH_URNS events → forwarded by _run_sweep, consumed by writers
        # nonblocking_queue: NonBlockingEvent (DUAL_WRITE_DISABLED, CATCH_UP_WINDOW)
        sweep_queue: Queue[SweepEvent] = Queue()
        writer_queue: Queue[SweepEvent] = Queue()
        nonblocking_queue: Queue[NonBlockingEvent] = Queue()
        ctx.sweep_events = sweep_queue
        self._nonblocking_queue = nonblocking_queue

        monitor = LogMonitor(
            self._docker,
            sweep_queue,
            since=datetime.utcnow(),
            gms_service=self._config.gms_service,
            nonblocking_queue=nonblocking_queue,
        )
```

**Edit 5: Pass `extra_args=["-u", "SystemUpdateNonBlocking"]` to BOTH `run_upgrade_job` calls in `_run_sweep`.**

Find the first `run_upgrade_job` call (the initial launch) and append `extra_args=["-u", "SystemUpdateNonBlocking"]` keyword:

```python
        proc = self._docker.run_upgrade_job(
            {
                **token_env,
                "ASPECT_MIGRATION_MUTATOR_ENABLED": "true",
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED": "true",
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_UPGRADE_VERSION": self._config.upgrade_version,
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS": str(
                    self._config.pre_write_delay_ms
                ),
            },
            service=self._config.upgrade_service,
            extra_args=["-u", "SystemUpdateNonBlocking"],
        )
```

Find the second `run_upgrade_job` call (the SKIPPED retry path) and add the same `extra_args` keyword.

**Edit 6: On `SweepState.COMPLETED`, capture the upgrade-nonblocking result before `return PhaseResult(...)`.**

Locate the existing COMPLETED branch:

```python
            elif event.state == SweepState.COMPLETED:
                ctx.sweep_total_migrated = event.total_migrated or 0
                log.info(
                    "Sweep COMPLETED — total migrated: %d (sweep took %.1fs)",
                    ctx.sweep_total_migrated,
                    time.monotonic() - sweep_start,
                )
                stop_event.set()
                return PhaseResult(
                    phase_name=self.name,
                    status="passed",
                    started_at=start,
                    details={
                        "total_migrated": ctx.sweep_total_migrated,
                        "sweep_duration_s": round(time.monotonic() - sweep_start, 1),
                        "io_reads": len(ctx.io_observations),
                        "io_writes": len(ctx.io_write_results),
                        "write_failures": sum(
                            1 for r in ctx.io_write_results if not r.passed
                        ),
                    },
                )
```

Replace with:

```python
            elif event.state == SweepState.COMPLETED:
                ctx.sweep_total_migrated = event.total_migrated or 0
                log.info(
                    "Sweep COMPLETED — total migrated: %d (sweep took %.1fs)",
                    ctx.sweep_total_migrated,
                    time.monotonic() - sweep_start,
                )
                stop_event.set()
                disabled, windows = self._drain_nonblocking_queue()
                ctx.upgrade_nonblocking = self._capture_upgrade_result(
                    disabled, windows, time.monotonic() - sweep_start
                )
                return PhaseResult(
                    phase_name=self.name,
                    status="passed",
                    started_at=start,
                    details={
                        "total_migrated": ctx.sweep_total_migrated,
                        "sweep_duration_s": round(time.monotonic() - sweep_start, 1),
                        "io_reads": len(ctx.io_observations),
                        "io_writes": len(ctx.io_write_results),
                        "write_failures": sum(
                            1 for r in ctx.io_write_results if not r.passed
                        ),
                        "dual_write_disabled_indices": disabled,
                        "catch_up_windows": windows,
                    },
                )
```

**Edit 7: Add the two new helper methods at the end of the class (after `_run_sweep`):**

```python
    def _drain_nonblocking_queue(
        self,
    ) -> tuple[list[str], dict[str, tuple[int, int]]]:
        """Drain the nonblocking_queue without blocking. Returns (disabled, windows)."""
        from queue import Empty

        disabled: list[str] = []
        windows: dict[str, tuple[int, int]] = {}
        q = getattr(self, "_nonblocking_queue", None)
        if q is None:
            return disabled, windows
        while True:
            try:
                ev = q.get_nowait()
            except Empty:
                break
            if ev.state == NonBlockingState.DUAL_WRITE_DISABLED:
                if ev.index_name not in disabled:
                    disabled.append(ev.index_name)
            elif ev.state == NonBlockingState.CATCH_UP_WINDOW and ev.window is not None:
                windows[ev.index_name] = ev.window
        return disabled, windows

    def _capture_upgrade_result(
        self,
        disabled: list[str],
        windows: dict[str, tuple[int, int]],
        duration_s: float,
    ) -> UpgradeNonBlockingResult:
        upgrade_id, raw = self._mysql.find_upgrade_result_with_field("indicesState")
        indices = parse_indices_state(raw.get("indicesState", {})) if raw else []
        return UpgradeNonBlockingResult(
            indices=indices,
            dual_write_disabled_indices=disabled,
            catch_up_windows=windows,
            raw=raw,
            duration_s=duration_s,
            upgrade_id=upgrade_id,
        )
```

- [ ] **Step 5: Delete the legacy file**

```bash
rm <REPO_ROOT>/smoke-test/tests/zdu/framework/phases/sweep_and_io.py
```

- [ ] **Step 6: Run new phase tests — expect pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_upgrade_nonblocking.py -v 2>&1 | tail -15
```

Expected: 3 tests pass.

- [ ] **Step 7: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 160 pass (157 baseline after Task 2 + 3 new phase tests).

(If `test_sweep_and_io.py` exists and now fails because `SweepAndIOPhase` is gone, delete that test file too — it has been superseded by `test_upgrade_nonblocking.py`. Note in the commit message.)

- [ ] **Step 8: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/upgrade_nonblocking.py \
        smoke-test/tests/zdu/framework/test_upgrade_nonblocking.py \
        smoke-test/tests/zdu/framework/phases/sweep_and_io.py
git commit -m "feat(zdu): UpgradeNonBlockingPhase — replace SweepAndIOPhase with non-blocking captures"
```

If `test_sweep_and_io.py` was deleted, include it in the same commit.

Re-stage if pre-commit hooks reformat.

---

## Task 4: Wire `UpgradeNonBlockingPhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** The runner currently has `("sweep_and_io", SweepAndIOPhase(self._config, self._docker, self._datahub))`. Update both the import and the tuple.

- [ ] **Step 1: Replace the import**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, find:

```python
from .phases.sweep_and_io import SweepAndIOPhase
```

Replace with:

```python
from .phases.upgrade_nonblocking import UpgradeNonBlockingPhase
```

- [ ] **Step 2: Replace the tuple**

Find:

```python
            (
                "sweep_and_io",
                SweepAndIOPhase(self._config, self._docker, self._datahub),
            ),
```

Replace with:

```python
            (
                "upgrade_nonblocking",
                UpgradeNonBlockingPhase(
                    config=self._config,
                    docker=self._docker,
                    datahub=self._datahub,
                    mysql=self._mysql,
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

Expected: `runner constructed OK`.

- [ ] **Step 4: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 160 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire UpgradeNonBlockingPhase into runner pipeline (replaces sweep_and_io)"
```

Re-stage if pre-commit hooks reformat.

---

## Task 5: README — Phase 8 description

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Insert Phase 8 subsection AFTER Phase 7**

Find the existing `### Phase 7: Inject Traffic Dual` subsection (added in Plan 6). Append a new subsection AFTER it:

```markdown
### Phase 8: Upgrade Non-Blocking

`UpgradeNonBlockingPhase` runs `system-update -u SystemUpdateNonBlocking` against the live NEW GMS while concurrent reader and writer threads continue to hit the system. It replaces the legacy `SweepAndIOPhase` placeholder.

Responsibilities:

- **Aspect schema sweep** — `MigrateAspectsStep` reads every `embed` and `globalTags` aspect and upgrades through the mutator chain. Concurrent writes from the IO-pool harness race with the sweep; `ConditionalWriteValidator` rejects stale sweep writes (tracked in `ctx.io_write_results`).
- **Catch-up backfill** — `IncrementalReindexCatchUpStep` backfills the `[T0, T1]` gap window into the new index. Each per-index window is parsed from the log line `Catch-up for entity index {name}: window [{T0}, {T1}]` and stored in `ctx.upgrade_nonblocking.catch_up_windows`.
- **Dual-write disable** — for indices where rollback dual-write is no longer needed, the upgrade marks them `DUAL_WRITE_DISABLED`. The framework parses `Marked index {name} as DUAL_WRITE_DISABLED` from upgrade-job logs and stores the names in `ctx.upgrade_nonblocking.dual_write_disabled_indices`.
- **Post-sweep upgrade-result capture** — on sweep `COMPLETED`, the phase queries MySQL for the resulting `DataHubUpgradeResult` aspect, parses `indicesState`, and stores the structured view in `ctx.upgrade_nonblocking.indices` (symmetric to `ctx.upgrade_blocking.indices` from Phase 4).

The phase preserves all legacy IO-harness captures (`ctx.io_observations`, `ctx.io_write_results`, `ctx.sweep_total_migrated`) for Suite A scenario validation. The deterministic race window (`pre_write_delay_ms = 500`) is unchanged.

If the upgrade subprocess fails or times out, the phase reports `failed` and the IO harness is shut down cleanly.
```

- [ ] **Step 2: Update the Phases overview section if it lists `sweep_and_io`**

Check around line 145 (the `## Phases` section). If the list includes `sweep_and_io`, replace it with `upgrade_nonblocking`. Same for any references in `### Skipping Phases`.

```bash
grep -n "sweep_and_io" smoke-test/tests/zdu/README.md
```

If hits remain, replace each with `upgrade_nonblocking`.

- [ ] **Step 3: Verify section ordering**

```bash
grep -nE "^##|^###" smoke-test/tests/zdu/README.md | head -30
```

`### Phase 8: Upgrade Non-Blocking` should appear AFTER `### Phase 7: Inject Traffic Dual`.

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Phase 8 UpgradeNonBlockingPhase behaviour"
```

Re-stage if pre-commit hooks reformat.

---

## Task 6: Live integration check

**Pre-requisite:** Compose stack up.

- [ ] **Step 1: Spy-based wiring check**

Verify the new phase constructs and exposes the right shape:

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies UpgradeNonBlockingPhase wiring."""
from unittest.mock import MagicMock
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext, UpgradeNonBlockingResult
from tests.zdu.framework.phases.upgrade_nonblocking import UpgradeNonBlockingPhase

cfg = ZDUTestConfig()
docker = MagicMock()
datahub = MagicMock()
mysql = MagicMock()
mysql.find_upgrade_result_with_field.return_value = (
    "urn:li:dataHubUpgrade:SystemUpdateNonBlocking",
    {"indicesState": {}},
)
phase = UpgradeNonBlockingPhase(config=cfg, docker=docker, datahub=datahub, mysql=mysql)
assert phase.name == "upgrade_nonblocking"
# Check the helper directly with a synthetic queue-drain
from tests.zdu.framework.log_monitor import NonBlockingEvent, NonBlockingState
from datetime import datetime
from queue import Queue

q: Queue = Queue()
q.put(NonBlockingEvent(NonBlockingState.DUAL_WRITE_DISABLED, "idx_a", datetime.utcnow(), "..."))
q.put(NonBlockingEvent(NonBlockingState.CATCH_UP_WINDOW, "idx_b", datetime.utcnow(), "...", window=(10, 20)))
phase._nonblocking_queue = q
disabled, windows = phase._drain_nonblocking_queue()
print("disabled:", disabled)
print("windows:", windows)
assert disabled == ["idx_a"]
assert windows == {"idx_b": (10, 20)}

result = phase._capture_upgrade_result(disabled, windows, 1.5)
assert isinstance(result, UpgradeNonBlockingResult)
assert result.dual_write_disabled_indices == ["idx_a"]
assert result.catch_up_windows == {"idx_b": (10, 20)}
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
ZDU_SKIP_PHASES=upgrade,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -40
```

Skip `upgrade` (legacy image-swap placeholder) and `rolling_restart` (real recreations would disrupt the dev stack). Pipeline runs `discovery → seed → snapshot_t0 → upgrade_blocking → inject_traffic_pre → inject_traffic_dual → upgrade_nonblocking → validation`.

Expected: same 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP. Specifically:

- `upgrade_nonblocking` phase status should be `passed`.
- The phase report should show non-zero `total_migrated` (since the sweep actually ran).
- `dual_write_disabled_indices` and `catch_up_windows` may be empty in dev (single-image stack) — that is expected when the upgrade-job determines no incremental work is needed.

- [ ] **Step 3: Inspect the JSON report for new fields**

```bash
python3 -c "
import json
data = json.load(open('<REPO_ROOT>/smoke-test/smoke-test/build/zdu-test-report.json'))
for p in data.get('phases', []):
    if p.get('name') == 'upgrade_nonblocking':
        import json as j
        print(j.dumps(p, indent=2)[:1500])
"
```

Verify the phase's `details` block contains the new keys: `total_migrated`, `dual_write_disabled_indices`, `catch_up_windows`.

- [ ] **Step 4: Cleanup the IO-pool URNs** (optional)

```bash
docker compose -f docker/profiles/docker-compose.yml exec -T mysql \
  mysql -udatahub -pdatahub datahub -e "
  DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dashboard:(test,zdu-io-pool-%)';
" 2>&1 | tail -3
```

(IO-pool URNs are seeded fresh on every run by `SeedPhase`, so cleanup isn't strictly required, but it keeps the DB tidy.)

- [ ] **Step 5: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 7 wiring"
```

If nothing regressed, no commit needed.

---

## Task 7: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff dcaf86c277..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-7.diff
wc -l /tmp/zdu-plan-7.diff
```

(`dcaf86c277` is the last Plan 6 commit. Adjust if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-7.diff`. This PR replaces the legacy `SweepAndIOPhase` placeholder with `UpgradeNonBlockingPhase` (Phase 8 in the ZDU pipeline). The phase runs `system-update -u SystemUpdateNonBlocking` against a live NEW GMS, captures the post-sweep `DataHubUpgradeResult.indicesState` (symmetric to Plan 2's `UpgradeBlockingPhase`), parses `DUAL_WRITE_DISABLED` and catch-up window log lines, and preserves the legacy concurrent-IO harness data for Suite A baseline.
>
> Concretely:
>
> - `UpgradeNonBlockingResult` dataclass on `TestContext` (placed between `UpgradeBlockingResult` and `RollingRestartResult` in the dataclass order; ctx slot under `# UpgradeNonBlockingPhase writes`).
> - `NonBlockingState` enum + `NonBlockingEvent` dataclass + `_parse_nonblocking_line` parser in `log_monitor.py`. `LogMonitor.__init__` gains an optional `nonblocking_queue` parameter; the upgrade-stdout loop pushes parsed events when the queue is non-None.
> - `UpgradeNonBlockingPhase` (renamed from `SweepAndIOPhase`) — same harness logic preserved, three additions: explicit `extra_args=["-u", "SystemUpdateNonBlocking"]` on both `run_upgrade_job` calls, `nonblocking_queue` plumbed into `LogMonitor`, and a post-COMPLETED helper `_capture_upgrade_result` that builds `UpgradeNonBlockingResult` from `mysql.find_upgrade_result_with_field("indicesState")` plus the drained queue.
> - Runner tuple key flips `sweep_and_io` → `upgrade_nonblocking`. Constructor adds `mysql=self._mysql`.
> - README has a new Phase 8 subsection.
>
> Check specifically:
>
> 1. **Failure-mode classification:** sweep timeout = phase failed; sweep `FAILED` event = phase failed; missing upgrade result in MySQL = phase still passes with empty `indices` (defensive — fresh deployment edge case).
> 2. **Suite A baseline preservation:** `ctx.io_observations`, `ctx.io_write_results`, `ctx.sweep_total_migrated`, `ctx.io_pool_entities` continue to be populated identically. The `pre_write_delay_ms` race-window mechanism is untouched.
> 3. **Symmetric pattern with Plan 2:** `UpgradeNonBlockingResult` mirrors `UpgradeBlockingResult` (both have `indices`, `raw`, `duration_s`, `upgrade_id`); `parse_indices_state` is reused across both phases.
> 4. **`_drain_nonblocking_queue` correctness:** non-blocking drain (no waiting); deduplicates `DUAL_WRITE_DISABLED` index names (since the upgrade may emit the same line on retries); preserves catch-up window tuples; tolerates missing `_nonblocking_queue` attribute (defensive).
> 5. **Parser regex robustness:** `_DUAL_WRITE_DISABLED_RE` and `_CATCH_UP_WINDOW_RE` reject malformed input, accept exact spec lines from design doc §7.5 (lines 489-500).
> 6. **LogMonitor backward compat:** `nonblocking_queue` defaults to None; existing callers (Plan 2's `UpgradeBlockingPhase` does NOT use LogMonitor for its capture — it reads stdout directly — so the only LogMonitor caller is `UpgradeNonBlockingPhase`. Confirm.)
> 7. **Pipeline order & key change:** runner key `sweep_and_io` → `upgrade_nonblocking`. Confirm any environment variable references (e.g. `ZDU_SKIP_PHASES`) in README and code are updated consistently.
> 8. **Test quality:** mocked clients, the 3 phase tests cover: explicit extra_args plumbing, full capture path, MySQL-returns-None defensive path.
> 9. **Type hints complete** — `UpgradeNonBlockingResult.catch_up_windows: dict[str, tuple[int, int]]`, parser return types, helper return types.
> 10. **YAGNI:** No retry, no sweep cursor resumption (TC-401 deferred), no rollback flag toggling (TC-308 / TC-309 deferred), no chain-disable capture (TC-019 deferred to Phase 10).
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 7"
```

---

## Self-Review

**Spec coverage** (against design doc §5.8 and §14.1 Phase 8):

| Requirement                                                              | Task                                                                 |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Run `system-update -u SystemUpdateNonBlocking`                           | Task 3 (`extra_args=["-u", "SystemUpdateNonBlocking"]`)              |
| `MigrateAspectsStep` sweep observation                                   | Task 3 (preserved from `SweepAndIOPhase`)                            |
| `Processing batch URNs` parsed and forwarded to writers                  | Task 3 (preserved)                                                   |
| Total migrated equals `sourceDocCount - alreadyMigrated`                 | Task 3 (`ctx.sweep_total_migrated`); assertion in Phase 10           |
| `Sweep complete, total {n}` observed                                     | Task 3 (preserved `SweepState.COMPLETED`)                            |
| `DataHubUpgradeResult.STARTED → COMPLETED`                               | Task 3 (`_capture_upgrade_result` reads the final aspect from MySQL) |
| `Catch-up for entity index {name}: window [{T0}, {T1}]` per index        | Task 2 + Task 3 (`catch_up_windows`)                                 |
| `Marked index {name} as DUAL_WRITE_DISABLED` parsed                      | Task 2 + Task 3 (`dual_write_disabled_indices`)                      |
| Per-URN race-window proof (sweep stale write rejected)                   | Task 3 (preserved from harness; assertion in Phase 10 / Validation)  |
| `[upgrade] sweep URN: ...` and `[gms] Producing MCL ...` line interleave | OUT OF SCOPE — pure log artifact; no programmatic assertion needed   |
| TC-401 (sweep cursor resumability)                                       | DEFERRED — separate scenario plan                                    |
| TC-307 (catch-up resume from `lastUrn`)                                  | DEFERRED — separate scenario plan                                    |
| TC-308 / TC-309 (rollback flag on/off)                                   | DEFERRED — needs runtime config knob                                 |
| TC-206 / TC-207 (poller log lines)                                       | DEFERRED — Phase 10 / future                                         |
| `AspectMigrationMutatorChain disabled` capture                           | DEFERRED — Phase 10 / future                                         |

**Placeholder scan:** None.

**Type / signature consistency:**

- `TestContext.upgrade_nonblocking: UpgradeNonBlockingResult | None` (Task 1) — populated by Task 3's `phase.run()`.
- `UpgradeNonBlockingPhase(config, docker, datahub, mysql)` (Task 3) — runner constructs (Task 4).
- `LogMonitor(docker, sweep_queue, since=, gms_service=, nonblocking_queue=None)` (Task 2) — keyword-arg signature; backward compat preserved.
- `_parse_nonblocking_line(line: str) -> NonBlockingEvent | None` (Task 2).
- `parse_indices_state(payload) -> list[IndexState]` (existing in `upgrade_blocking.py`, reused by `_capture_upgrade_result`).
- `MySQLClient.find_upgrade_result_with_field("indicesState") -> tuple[str | None, dict | None]` (existing, used by both Phase 2 and Phase 8).

**Risks called out:**

1. **Single-image dev workflow yields empty captures.** When the running stack is a single image tag (no real rolling restart), the upgrade job may determine no work is needed. `dual_write_disabled_indices` and `catch_up_windows` will be empty and `indices` may be empty too. Document this in the README — it's expected, not a regression.
2. **Ordering of capture vs. stop_event.set().** Task 3 captures the queue and upgrade result AFTER `stop_event.set()` but BEFORE `return`. The harness threads observe stop_event in their next polling loop — there is no race because the queue-drain operates on its own queue, not on the harness IO queues.
3. **MySQL fresh-deployment edge case.** Fresh stacks may have NO `DataHubUpgradeResult` yet (the test config uses a unique `upgrade_version` per run to force re-execution). The `_capture_upgrade_result` helper handles `raw=None` gracefully — `indices=[]`. Test `test_capture_result_skipped_when_mysql_returns_none` covers this.
4. **`sweep_and_io` env-skip key.** Users may have shell scripts that pass `ZDU_SKIP_PHASES=sweep_and_io`. Task 5 updates the README; existing scripts will silently no-op (the new phase key is `upgrade_nonblocking`). Document in the breaking-change line of the commit message in Task 4.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-7-upgrade-nonblocking.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Per session policy: defaulting to subagent-driven execution.
