# ZDU E2E — Plan 4: Phase 6 `RollingRestartPhase`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Phase 6 — `RollingRestartPhase` — which swaps the running GMS → MAE → MCE containers from `OLD_IMAGE_TAG` to `NEW_IMAGE_TAG` in sequence, then tails MAE logs to capture each index's `dualWriteStartTime` (T1) into `TestContext.rolling_restart` for downstream phases.

**Architecture:** One new phase class (`framework/phases/rolling_restart.py`) consumes F-3's two-image-tag plumbing. For each ZDU-relevant service in the fixed order GMS → MAE → MCE: set `compose_env={"DATAHUB_<SERVICE>_VERSION": new_tag, "DATAHUB_VERSION": new_tag}`, run `docker compose up -d {service}` to recreate the container, then wait for healthy. After all services restarted, tail the MAE log for `Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}` lines and record T1 per index. The phase has no side effects beyond container recreation; it doesn't write to MySQL or ES directly.

**Tech Stack:** Python 3 (existing), Docker Compose v2 (existing), `subprocess` for compose calls. No new dependencies.

**Out of scope (deferred):**

- Phase 5 `InjectTrafficPrePhase` (separate plan) — writes via OLD GMS in the `[T0, T1]` window before this phase runs.
- Phase 7 `InjectTrafficDualPhase` (separate plan) — writes via NEW GMS during the dual-write window after this phase.
- Phase 8 `UpgradeNonBlockingPhase` replacement of legacy `sweep_and_io` — separate plan.
- K8s-native rolling pod restart — out of scope per design doc; we approximate with sequenced compose recreations.
- Plan F-5 (test mutator for race window) — independent.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── context.py                                   MODIFY — add RollingRestartResult + ctx slot
├── log_monitor.py                               MODIFY — add DualWriteEvent + _parse_dual_write_line
├── docker_compose.py                            MODIFY — add recreate_service(service, compose_env, timeout)
├── runner.py                                    MODIFY — wire RollingRestartPhase
├── phases/
│   └── rolling_restart.py                       CREATE — RollingRestartPhase
├── test_log_monitor.py                          MODIFY — append dual-write parse tests
├── test_docker_compose.py                       MODIFY — append recreate_service tests
└── test_rolling_restart.py                      CREATE — phase unit tests with mocked docker
```

The phase reads `config.new_image_tag` (added in F-3) and uses `DockerComposeClient.recreate_service` for the actual swap. It tails MAE logs via the existing `DockerComposeClient.tail_service_logs(service, since)` helper.

---

## Task 1: Add `RollingRestartResult` dataclass to `TestContext`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`

**Pattern:** Single-purpose data carrier next to `SnapshotT0`, `IndexState`, `UpgradeBlockingResult`. The `dual_write_start_times` dict captures the per-index T1 epoch (the timestamp parsed from the MAE log line).

- [ ] **Step 1: Append the dataclass after `UpgradeBlockingResult`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/context.py`, insert this block between `UpgradeBlockingResult` and `ValidationResult`:

```python
@dataclass
class RollingRestartResult:
    """Captured by ``RollingRestartPhase`` after sequenced GMS → MAE → MCE swap.

    ``services_restarted`` is the ordered list of compose services that were
    successfully recreated and reported healthy. ``dual_write_start_times``
    maps physical-index name → T1 epoch ms, parsed from the MAE log line
    ``Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}``.
    Used by Phase 7 (InjectTrafficDual) and Phase 10 (Validation) to assert
    dual-write fan-out behaviour and the ``[T0, T1]`` window.
    """

    services_restarted: list[str] = field(default_factory=list)
    dual_write_start_times: dict[str, int] = field(default_factory=dict)
    duration_s: float = 0.0
```

- [ ] **Step 2: Add the slot on `TestContext`**

In the same file, in the `TestContext` dataclass, after the `# UpgradeBlockingPhase writes` block (`upgrade_blocking: UpgradeBlockingResult | None`), insert:

```python
    # RollingRestartPhase writes
    rolling_restart: RollingRestartResult | None = None
```

- [ ] **Step 3: Smoke-import**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.context import RollingRestartResult, TestContext
ctx = TestContext()
assert ctx.rolling_restart is None
r = RollingRestartResult()
assert r.services_restarted == [] and r.dual_write_start_times == {}
print('OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `OK`.

- [ ] **Step 4: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 123 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py
git commit -m "feat(zdu): add RollingRestartResult dataclass and TestContext slot"
```

---

## Task 2: Add dual-write log parser to `log_monitor.py`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/log_monitor.py`
- Modify: `smoke-test/tests/zdu/framework/test_log_monitor.py` (append cases)

**Pattern:** Same approach as Plan 2's `Phase1State` parser. Add a parallel `DualWriteEvent` dataclass and `_parse_dual_write_line` function. Strictly additive — does not modify existing `SweepEvent` / `Phase1Event` flows.

**Production log line** (verified in `metadata-io/src/main/java/com/linkedin/metadata/service/UpdateIndicesUpgradeStrategy.java:240`):

```
Recorded dual-write start time for index '{oldIndex}' (entity '{entityName}'): {timestampMs}
```

- [ ] **Step 1: Append failing tests to `test_log_monitor.py`**

Append (do NOT delete existing tests):

```python
from tests.zdu.framework.log_monitor import (
    DualWriteEvent,
    _parse_dual_write_line,
)


def test_parse_dual_write_started():
    line = (
        "INFO  Recorded dual-write start time for index "
        "'datasetindex_v2' (entity 'dataset'): 1714000000000"
    )
    e = _parse_dual_write_line(line)
    assert e is not None
    assert e.index_name == "datasetindex_v2"
    assert e.entity_name == "dataset"
    assert e.timestamp_ms == 1714000000000


def test_parse_dual_write_with_namespaced_entity():
    line = (
        "INFO  Recorded dual-write start time for index "
        "'dashboardindex_v2' (entity 'dashboard'): 1714000005000"
    )
    e = _parse_dual_write_line(line)
    assert e is not None
    assert e.index_name == "dashboardindex_v2"
    assert e.entity_name == "dashboard"
    assert e.timestamp_ms == 1714000005000


def test_parse_dual_write_unrelated_returns_none():
    assert _parse_dual_write_line("INFO  Some unrelated GMS chatter") is None
    assert _parse_dual_write_line("Recorded dual-write — but malformed") is None
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v 2>&1 | tail -15
```

Expected: `ImportError: cannot import name 'DualWriteEvent' from 'tests.zdu.framework.log_monitor'`.

- [ ] **Step 3: Add `DualWriteEvent` + `_parse_dual_write_line` to `log_monitor.py`**

Open `<REPO_ROOT>/smoke-test/tests/zdu/framework/log_monitor.py`. After the existing `Phase1State` enum + `Phase1Event` dataclass + `_PHASE1_PATTERNS` + `_parse_phase1_line` block, append (do NOT modify any existing code):

```python
@dataclass
class DualWriteEvent:
    """Emitted by MAE's ``UpdateIndicesUpgradeStrategy`` when an old index
    starts receiving dual writes for rollback safety.
    """

    index_name: str
    entity_name: str
    timestamp_ms: int
    timestamp: datetime
    message: str


_DUAL_WRITE_PATTERN = re.compile(
    r"Recorded dual-write start time for index '(\S+)' \(entity '(\S+)'\): (\d+)"
)


def _parse_dual_write_line(line: str) -> DualWriteEvent | None:
    """Parse a MAE log line into a DualWriteEvent, or None.

    Independent of ``_parse_line`` and ``_parse_phase1_line`` so the
    existing flows are untouched.
    """
    m = _DUAL_WRITE_PATTERN.search(line)
    if not m:
        return None
    return DualWriteEvent(
        index_name=m.group(1),
        entity_name=m.group(2),
        timestamp_ms=int(m.group(3)),
        timestamp=datetime.utcnow(),
        message=line.strip(),
    )
```

The `re`, `dataclass`, `datetime` imports are already present at the top of the file.

- [ ] **Step 4: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_log_monitor.py -v 2>&1 | tail -10
```

Expected: 3 new tests pass + existing 16 still pass = 19 tests.

- [ ] **Step 5: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 126 pass (123 baseline + 3 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/log_monitor.py \
        smoke-test/tests/zdu/framework/test_log_monitor.py
git commit -m "feat(zdu): DualWriteEvent + _parse_dual_write_line for MAE upgrade strategy"
```

---

## Task 3: Add `recreate_service` to `DockerComposeClient`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/docker_compose.py`
- Modify: `smoke-test/tests/zdu/framework/test_docker_compose.py` (append tests)

**Pattern:** Mirror `run_upgrade_job(compose_env=...)` from F-3 — same `compose_env` semantics (drives YAML substitution at compose-read time). After `up -d`, poll `docker compose ps --format '{{.Status}}'` until status contains `healthy`. Existing `wait_healthy(service, timeout_s)` already does this — reuse it.

- [ ] **Step 1: Append failing tests to `test_docker_compose.py`**

```python
class TestRecreateService:
    def test_runs_compose_up_d_with_compose_env(
        self, client: DockerComposeClient
    ) -> None:
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.run"
        ) as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            with patch.object(client, "wait_healthy") as mock_wait:
                client.recreate_service(
                    service="datahub-gms-debug",
                    compose_env={"DATAHUB_GMS_VERSION": "v1.6.0"},
                    timeout_s=120,
                )
                # First subprocess.run call is `docker compose up -d <service>`
                first_call = mock_run.call_args_list[0]
                cmd = first_call[0][0]
                assert "up" in cmd
                assert "-d" in cmd
                assert "datahub-gms-debug" in cmd
                # compose_env was passed via env=
                env_kw = first_call[1].get("env") or first_call.kwargs.get("env")
                assert env_kw is not None
                assert env_kw["DATAHUB_GMS_VERSION"] == "v1.6.0"
                # Then wait_healthy was called for the same service
                mock_wait.assert_called_once_with("datahub-gms-debug", timeout_s=120)

    def test_no_compose_env_does_not_pass_env_kwarg(
        self, client: DockerComposeClient
    ) -> None:
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.run"
        ) as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            with patch.object(client, "wait_healthy"):
                client.recreate_service(
                    service="datahub-gms-debug",
                    compose_env=None,
                    timeout_s=10,
                )
                first_call = mock_run.call_args_list[0]
                env_kw = first_call.kwargs.get("env")
                assert env_kw is None  # subprocess inherits parent env

    def test_failed_up_d_raises(self, client: DockerComposeClient) -> None:
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.run"
        ) as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1, stdout="", stderr="image not found",
            )
            with patch.object(client, "wait_healthy"):
                with pytest.raises(RuntimeError, match="image not found"):
                    client.recreate_service(
                        service="datahub-gms-debug",
                        compose_env={"DATAHUB_GMS_VERSION": "missing-tag"},
                        timeout_s=10,
                    )
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_docker_compose.py -v 2>&1 | tail -10
```

Expected: 3 new tests fail (`recreate_service` doesn't exist).

- [ ] **Step 3: Add `recreate_service` to `DockerComposeClient`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/docker_compose.py`, append a new method to the class (placement: after `wait_healthy`, before `run_upgrade_job`):

```python
    def recreate_service(
        self,
        service: str,
        compose_env: dict[str, str] | None = None,
        timeout_s: int = 120,
    ) -> None:
        """Recreate a single compose service with optional compose_env override.

        Runs ``docker compose up -d {service}`` so the existing container
        is stopped and recreated using the current YAML config (after any
        ``compose_env`` substitution). Waits for the service to report
        healthy via the existing ``wait_healthy`` poll.

        Args:
            service: compose service name (e.g. ``datahub-gms-debug``).
            compose_env: env vars for compose YAML substitution at
                read time (e.g. ``{"DATAHUB_GMS_VERSION": new_tag}``).
                ``None`` (default) inherits the parent process env.
            timeout_s: seconds to wait for ``healthy`` before raising.

        Raises:
            RuntimeError: if ``docker compose up -d`` returns non-zero.
            TimeoutError: if the service does not become healthy within
                ``timeout_s`` (propagated from ``wait_healthy``).
        """
        cmd = self._base_cmd() + ["up", "-d", service]
        log.info("Recreating service %s with compose_env=%s", service, compose_env)

        run_kwargs: dict[str, object] = {
            "cwd": self._project_dir,
            "capture_output": True,
            "text": True,
        }
        if compose_env is not None:
            merged_env = dict(os.environ)
            merged_env.update(compose_env)
            run_kwargs["env"] = merged_env

        if compose_env is not None:
            result = subprocess.run(cmd, env=run_kwargs["env"],
                                     cwd=run_kwargs["cwd"],
                                     capture_output=True, text=True)
        else:
            result = subprocess.run(cmd,
                                     cwd=run_kwargs["cwd"],
                                     capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose up -d {service} failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

        self.wait_healthy(service, timeout_s=timeout_s)
```

(The duplicated branch — once with `env=` and once without — mirrors the same defensive pattern F-3 used in `run_upgrade_job` to satisfy mypy's overloaded `subprocess.run` stubs.)

`os` is already imported at the top of the file (added in F-3).

- [ ] **Step 4: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_docker_compose.py -v 2>&1 | tail -10
```

Expected: 5 existing + 3 new = 8 tests pass.

- [ ] **Step 5: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 129 pass (126 baseline + 3 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/docker_compose.py \
        smoke-test/tests/zdu/framework/test_docker_compose.py
git commit -m "feat(zdu): DockerComposeClient.recreate_service for image-tag swap"
```

---

## Task 4: Implement `RollingRestartPhase`

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/rolling_restart.py`
- Create: `smoke-test/tests/zdu/framework/test_rolling_restart.py`

**Pattern:** Phase ABC subclass following `phases/discovery.py`. Constructor takes `docker`, `mae_service`, `services_in_order`, `new_image_tag`, `dual_write_timeout_s`. `run(ctx)` does:

1. For each service in `services_in_order` (default `["datahub-gms-debug", "datahub-mae-consumer-debug", "datahub-mce-consumer-debug"]`):
   - Compute the per-service `compose_env` via `_compose_env_for_service(service, new_image_tag)` (sets the right per-service var: `DATAHUB_GMS_VERSION` for gms, `DATAHUB_MAE_VERSION` for mae, etc., plus the global `DATAHUB_VERSION` fallback).
   - Call `docker.recreate_service(service, compose_env, timeout_s=...)`.
   - Append to `services_restarted`.
2. After all services restart, tail the MAE service logs starting from before the restart began (`since=phase_start`). For each line, parse with `_parse_dual_write_line`. Build `dual_write_start_times[index_name] = timestamp_ms`. Stop tailing after `dual_write_timeout_s` or when one event per index has been seen (we don't know how many indices in advance — use a simple deadline).
3. Write `RollingRestartResult` to `ctx.rolling_restart` and return.

### 4.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_rolling_restart.py`:

```python
"""Unit tests for RollingRestartPhase — uses mocked DockerComposeClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.context import RollingRestartResult, TestContext
from tests.zdu.framework.phases.rolling_restart import (
    RollingRestartPhase,
    _compose_env_for_service,
)


class TestComposeEnvForService:
    def test_gms_service_sets_gms_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-gms-debug", "v1.6.0")
        assert env["DATAHUB_GMS_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_mae_service_sets_mae_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-mae-consumer-debug", "v1.6.0")
        assert env["DATAHUB_MAE_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_mce_service_sets_mce_and_global_vars(self) -> None:
        env = _compose_env_for_service("datahub-mce-consumer-debug", "v1.6.0")
        assert env["DATAHUB_MCE_VERSION"] == "v1.6.0"
        assert env["DATAHUB_VERSION"] == "v1.6.0"

    def test_unknown_service_falls_back_to_global_only(self) -> None:
        env = _compose_env_for_service("custom-service", "v1.6.0")
        assert env == {"DATAHUB_VERSION": "v1.6.0"}


@pytest.fixture
def docker() -> MagicMock:
    return MagicMock()


@pytest.fixture
def phase(docker: MagicMock) -> RollingRestartPhase:
    return RollingRestartPhase(
        docker=docker,
        mae_service="datahub-mae-consumer-debug",
        services_in_order=[
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ],
        new_image_tag="v1.6.0",
        dual_write_timeout_s=5,
    )


class TestRollingRestartPhaseHappyPath:
    def test_recreates_services_in_order(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.tail_service_logs.return_value = iter(
            [
                "INFO  Recorded dual-write start time for index 'datasetindex_v2' "
                "(entity 'dataset'): 1714000000000",
                "INFO  Recorded dual-write start time for index 'dashboardindex_v2' "
                "(entity 'dashboard'): 1714000001000",
                "INFO  unrelated MAE chatter",
            ]
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        # Services were recreated in the configured order
        recreate_calls = [c.kwargs.get("service") or c.args[0]
                          for c in docker.recreate_service.call_args_list]
        assert recreate_calls == [
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ]
        # Each recreate carried the right per-service compose_env
        gms_env = docker.recreate_service.call_args_list[0].kwargs["compose_env"]
        assert gms_env["DATAHUB_GMS_VERSION"] == "v1.6.0"
        assert gms_env["DATAHUB_VERSION"] == "v1.6.0"
        # ctx.rolling_restart populated with both index → T1 mappings
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {
            "datasetindex_v2": 1714000000000,
            "dashboardindex_v2": 1714000001000,
        }
        assert ctx.rolling_restart.services_restarted == [
            "datahub-gms-debug",
            "datahub-mae-consumer-debug",
            "datahub-mce-consumer-debug",
        ]

    def test_no_dual_write_log_lines_yields_empty_dict(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        # MAE logs contain no dual-write lines (e.g. fresh stack with no aliases).
        docker.tail_service_logs.return_value = iter(
            ["INFO  startup boilerplate", "INFO  MAE ready"]
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed"
        assert ctx.rolling_restart is not None
        assert ctx.rolling_restart.dual_write_start_times == {}


class TestRollingRestartPhaseFailures:
    def test_recreate_service_failure_aborts_phase(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.recreate_service.side_effect = [
            None,  # gms ok
            RuntimeError("image not found: datahub-mae-consumer:missing"),
            None,  # mce — should never run
        ]
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "image not found" in (result.error or "")
        # Only gms was recorded as restarted; mae's failure aborts before mce
        assert docker.recreate_service.call_count == 2

    def test_health_timeout_propagates(
        self, phase: RollingRestartPhase, docker: MagicMock
    ) -> None:
        docker.recreate_service.side_effect = TimeoutError(
            "Service datahub-gms-debug not healthy after 120s"
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "failed"
        assert "not healthy" in (result.error or "")
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_rolling_restart.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'RollingRestartPhase' from 'tests.zdu.framework.phases.rolling_restart'` (module doesn't exist yet).

### 4.2 — Implement the phase

- [ ] **Step 3: Create the phase file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/rolling_restart.py`:

```python
"""Phase 6 — RollingRestartPhase.

Sequenced GMS → MAE → MCE container recreation with the NEW image tag.
After all services restart, tails MAE logs to capture each index's
``dualWriteStartTime`` (T1) into ``TestContext.rolling_restart``.

The phase doesn't write to MySQL or ES directly — it only manipulates
container state and reads logs. T1 capture relies on the MAE log line
``Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}``
emitted by ``UpdateIndicesUpgradeStrategy`` after the new MAE comes up.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import RollingRestartResult, TestContext
from ..docker_compose import DockerComposeClient
from ..log_monitor import _parse_dual_write_line

log = logging.getLogger(__name__)

_DEFAULT_SERVICES_IN_ORDER = (
    "datahub-gms-debug",
    "datahub-mae-consumer-debug",
    "datahub-mce-consumer-debug",
)
_DEFAULT_DUAL_WRITE_TIMEOUT_S = 60
_DEFAULT_RECREATE_TIMEOUT_S = 120


# Per-service compose-env keys that override the image tag for that service.
# The compose YAML chain is ``${DATAHUB_<SERVICE>_VERSION:-${DATAHUB_VERSION:-debug}}``;
# we set both so per-service and global fallbacks both resolve to ``new_tag``.
_PER_SERVICE_VERSION_KEY: dict[str, str] = {
    "datahub-gms-debug": "DATAHUB_GMS_VERSION",
    "datahub-mae-consumer-debug": "DATAHUB_MAE_VERSION",
    "datahub-mce-consumer-debug": "DATAHUB_MCE_VERSION",
}


def _compose_env_for_service(service: str, new_image_tag: str) -> dict[str, str]:
    """Build the compose_env dict that overrides ``service``'s image tag.

    Always sets ``DATAHUB_VERSION`` (global fallback). Sets the
    per-service var if recognised. Unknown services get only the global var.
    """
    env: dict[str, str] = {"DATAHUB_VERSION": new_image_tag}
    per_service_key = _PER_SERVICE_VERSION_KEY.get(service)
    if per_service_key is not None:
        env[per_service_key] = new_image_tag
    return env


class RollingRestartPhase(Phase):
    name = "rolling_restart"

    def __init__(
        self,
        docker: DockerComposeClient,
        mae_service: str = "datahub-mae-consumer-debug",
        services_in_order: tuple[str, ...] | list[str] = _DEFAULT_SERVICES_IN_ORDER,
        new_image_tag: str = "debug",
        dual_write_timeout_s: int = _DEFAULT_DUAL_WRITE_TIMEOUT_S,
        recreate_timeout_s: int = _DEFAULT_RECREATE_TIMEOUT_S,
    ) -> None:
        self._docker = docker
        self._mae_service = mae_service
        self._services_in_order = list(services_in_order)
        self._new_image_tag = new_image_tag
        self._dual_write_timeout_s = dual_write_timeout_s
        self._recreate_timeout_s = recreate_timeout_s

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        services_restarted: list[str] = []

        for service in self._services_in_order:
            try:
                self._docker.recreate_service(
                    service=service,
                    compose_env=_compose_env_for_service(service, self._new_image_tag),
                    timeout_s=self._recreate_timeout_s,
                )
            except Exception as exc:
                log.exception("RollingRestartPhase: recreate failed for %s", service)
                duration = time.monotonic() - t0
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=duration,
                    error=str(exc),
                    details={"services_restarted": services_restarted},
                )
            services_restarted.append(service)

        dual_write_start_times = self._capture_dual_write_start_times(start)

        result = RollingRestartResult(
            services_restarted=services_restarted,
            dual_write_start_times=dual_write_start_times,
            duration_s=time.monotonic() - t0,
        )
        ctx.rolling_restart = result
        log.info(
            "RollingRestart complete — %d services restarted, %d dual-write events",
            len(services_restarted),
            len(dual_write_start_times),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=result.duration_s,
            details={
                "services_restarted": services_restarted,
                "dual_write_start_times": dual_write_start_times,
            },
        )

    def _capture_dual_write_start_times(
        self, since: datetime
    ) -> dict[str, int]:
        """Tail MAE logs for ``Recorded dual-write start time`` lines.

        Stops when the deadline (``dual_write_timeout_s`` from now) expires.
        """
        deadline = time.monotonic() + self._dual_write_timeout_s
        out: dict[str, int] = {}
        try:
            for line in self._docker.tail_service_logs(self._mae_service, since=since):
                if time.monotonic() > deadline:
                    break
                event = _parse_dual_write_line(line)
                if event is None:
                    continue
                # Record the FIRST timestamp we see per index (T1 == first dual-write).
                if event.index_name not in out:
                    out[event.index_name] = event.timestamp_ms
        except Exception as exc:
            log.warning("Dual-write log tailing aborted: %s", exc)
        return out
```

- [ ] **Step 4: Run tests — expect pass**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_rolling_restart.py -v 2>&1 | tail -15
```

Expected: 4 (`_compose_env_for_service`) + 2 (happy path) + 2 (failures) = 8 tests pass.

- [ ] **Step 5: Run all framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 137 pass (129 baseline + 8 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/rolling_restart.py \
        smoke-test/tests/zdu/framework/test_rolling_restart.py
git commit -m "feat(zdu): RollingRestartPhase — sequenced GMS→MAE→MCE swap with T1 capture"
```

---

## Task 5: Wire `RollingRestartPhase` into the runner

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert AFTER `("upgrade_blocking", ...)` and BEFORE `("sweep_and_io", ...)`. Phase 6 needs the post-blocking-upgrade state but should run before any concurrent IO — matches the design doc's pipeline order.

- [ ] **Step 1: Add import**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, alongside other phase imports:

```python
from .phases.rolling_restart import RollingRestartPhase
```

- [ ] **Step 2: Insert into the `phases` list**

Find the `phases = [...]` list. Insert the new tuple between the existing `upgrade_blocking` and `sweep_and_io` entries:

```python
            (
                "rolling_restart",
                RollingRestartPhase(
                    docker=self._docker,
                    mae_service="datahub-mae-consumer-debug",
                    new_image_tag=self._config.new_image_tag,
                ),
            ),
```

We rely on the default `services_in_order`, `dual_write_timeout_s`, `recreate_timeout_s`. The `mae_service` matches the dev compose's debug variant.

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

- [ ] **Step 4: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 137 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire RollingRestartPhase into runner pipeline"
```

---

## Task 6: README — Phase 6 description

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Append a section under "Two-Image-Tag Testing"**

Find the existing Two-Image-Tag section. Append a "Phase 6: Rolling Restart" subsection (place after the "Defaults" paragraph):

```markdown
### Phase 6: Rolling Restart

`RollingRestartPhase` swaps the running GMS, MAE, and MCE containers
from `OLD_IMAGE_TAG` to `NEW_IMAGE_TAG` in sequence. For each service
it sets the per-service compose env (`DATAHUB_GMS_VERSION`,
`DATAHUB_MAE_VERSION`, `DATAHUB_MCE_VERSION`) plus the global
`DATAHUB_VERSION` fallback, runs `docker compose up -d {service}`,
and waits for healthy.

After all services restart, the phase tails the MAE log for the line
`Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}`
and records each index's T1 epoch in
`ctx.rolling_restart.dual_write_start_times`. This is the T1 in the
`[T0, T1]` window that Phase 7 (InjectTrafficDual, future) and Phase 8
(UpgradeNonBlocking, future) reason about.

If `recreate_service` fails for any service (image-pull error, health
timeout), the phase aborts and reports `failed` — subsequent services
in the order are NOT restarted, leaving the stack in a mixed state.
The operator must investigate before re-running.
```

- [ ] **Step 2: Run framework tests for sanity**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 137 pass.

- [ ] **Step 3: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Phase 6 RollingRestartPhase behaviour"
```

---

## Task 7: Spy-based integration check

**Pre-requisite:** Compose stack up.

A real two-image swap requires both OLD and NEW images built — that's an operator-side prep step. For first-pass validation we spy on `subprocess.run` to verify the framework correctly issues `docker compose up -d {service}` for each service in order with the right `compose_env`. The actual container behaviour is exercised by future end-to-end runs once two images exist.

- [ ] **Step 1: Run a spy script that intercepts `subprocess.run` for the recreate calls**

```bash
cd <REPO_ROOT>/smoke-test && venv/bin/python << 'PY'
"""Spy verifies RollingRestartPhase issues compose up -d for each service in order."""
from unittest.mock import MagicMock, patch
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.rolling_restart import RollingRestartPhase

docker = MagicMock()
calls: list[tuple[str, dict[str, str] | None]] = []

def spy_recreate(service, compose_env=None, timeout_s=120):
    calls.append((service, compose_env))

docker.recreate_service.side_effect = spy_recreate
# No dual-write events expected in spy mode (no real MAE running with OLD code path)
docker.tail_service_logs.return_value = iter([])

phase = RollingRestartPhase(
    docker=docker,
    mae_service="datahub-mae-consumer-debug",
    new_image_tag="v1.6.0-spy",
    dual_write_timeout_s=1,
)
ctx = TestContext()
result = phase.run(ctx)

print("phase status:", result.status)
print("services_restarted:", ctx.rolling_restart.services_restarted)
print("call order:", [c[0] for c in calls])
print("gms compose_env:", calls[0][1])
print("mae compose_env:", calls[1][1])
print("mce compose_env:", calls[2][1])

assert result.status == "passed"
assert [c[0] for c in calls] == [
    "datahub-gms-debug",
    "datahub-mae-consumer-debug",
    "datahub-mce-consumer-debug",
]
assert calls[0][1]["DATAHUB_GMS_VERSION"] == "v1.6.0-spy"
assert calls[1][1]["DATAHUB_MAE_VERSION"] == "v1.6.0-spy"
assert calls[2][1]["DATAHUB_MCE_VERSION"] == "v1.6.0-spy"
print("LIVE-WIRING OK")
PY
```

Expected output ends with `LIVE-WIRING OK`.

- [ ] **Step 2: Suite A regression check**

The default-tag path (no env vars set) means `new_image_tag="debug"` → recreate calls happen but the running services were already on `debug`, so compose's `up -d` is effectively a no-op recreation. Verify Suite A still produces the same baseline:

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,sweep_and_io,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -25
```

We skip `rolling_restart` here because real recreations on the existing dev stack would interrupt other testing. The baseline result must match Plan F-3's: 14 PASS / 7 XFAIL / 1 FAIL (TC-020) / 1 SKIP (TC-023).

- [ ] **Step 3: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 4 wiring"
```

If nothing regressed, no commit needed.

---

## Task 8: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff c2e687356a..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-4.diff
wc -l /tmp/zdu-plan-4.diff
```

(`c2e687356a` is the last F-3 commit. Adjust if newer commits land.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-4.diff`. This PR adds Phase 6 (`RollingRestartPhase`) to the ZDU E2E framework. The phase swaps GMS, MAE, MCE containers from OLD_IMAGE_TAG to NEW_IMAGE_TAG in sequence using F-3's `compose_env` plumbing, then tails MAE logs to capture per-index dual-write-start (T1) timestamps.
>
> Concretely:
>
> - `RollingRestartResult` dataclass + `TestContext` slot.
> - `DualWriteEvent` + `_parse_dual_write_line` in `log_monitor.py` (additive — does not modify existing flows).
> - `DockerComposeClient.recreate_service(service, compose_env, timeout_s)` — runs `docker compose up -d {service}` then waits healthy.
> - `RollingRestartPhase` class — sequenced recreate + log tail. Aborts on any service failure (does not roll back).
> - Runner wires it between `upgrade_blocking` and `sweep_and_io`.
>
> Check specifically:
>
> 1. **`compose_env` per service:** `_compose_env_for_service` sets BOTH the per-service var (`DATAHUB_<X>_VERSION`) AND the global `DATAHUB_VERSION`. This matches the compose YAML chain `${DATAHUB_<X>_VERSION:-${DATAHUB_VERSION:-debug}}`.
> 2. **Sequenced order is enforced:** any service failure aborts the loop; subsequent services don't recreate. Test `test_recreate_service_failure_aborts_phase` covers this.
> 3. **Backward compat:** `recreate_service(compose_env=None)` does not pass `env=` to `subprocess.run` (preserves parent env inheritance). Default `new_image_tag="debug"` matches compose YAML fallback.
> 4. **Log parser cohesion:** `_parse_dual_write_line` is a sibling of `_parse_phase1_line` and `_parse_line` in `log_monitor.py` — strictly additive.
> 5. **T1 capture:** `_capture_dual_write_start_times` records the FIRST timestamp per index (index_name dedup). `dual_write_timeout_s` bounds the tail.
> 6. **No state mutation on failure:** when `recreate_service` raises, `ctx.rolling_restart` is NOT populated (only `details.services_restarted` reflects the partial state).
> 7. **Type hints complete.**
> 8. **Test quality:** 4 + 2 + 2 = 8 tests cover compose_env helper, happy path (with and without dual-write logs), and failure modes. All mocked — no real docker calls.
> 9. **YAGNI:** No rollback logic, no retry, no parallel restart. Just sequential + abort-on-failure.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on Plan 4"
```

---

## Self-Review

**Spec coverage** (against design doc Section 5.6 + Section 14.1 Phase 6):

| Requirement                                          | Task                                                                          |
| ---------------------------------------------------- | ----------------------------------------------------------------------------- |
| Sequenced restart of GMS → MAE → MCE                 | Task 4 (`_DEFAULT_SERVICES_IN_ORDER` + sequential loop)                       |
| `docker compose up --build -d {service}` per service | Task 3 (`recreate_service` runs `docker compose up -d {service}`)             |
| Verify each service healthy before next restart      | Task 3 (`recreate_service` calls existing `wait_healthy`)                     |
| Tail MAE log for `Recorded dual-write start time`    | Task 4 (`_capture_dual_write_start_times` + Task 2's parser)                  |
| Capture per-index T1 epoch                           | Task 1 (`dual_write_start_times: dict[str, int]`)                             |
| T1 > T0 invariant                                    | OUT OF SCOPE — Phase 10 Validation will assert this; Plan 4 just records both |

**Note on `--build`:** The design doc says `docker compose up --build -d {service}`. Plan 4 uses `up -d` without `--build` because the F-3 architecture relies on PRE-BUILT image tags being available. `--build` would re-build from local source on every restart; that's not what we want when testing a specific OLD/NEW pair. If a future workflow needs build-on-restart, add a `build: bool = False` flag to `recreate_service`.

**Placeholder scan:** None.

**Type / signature consistency:**

- `RollingRestartResult(services_restarted: list[str], dual_write_start_times: dict[str, int], duration_s: float)` (Task 1) — used in Task 4's `_capture_dual_write_start_times` return + `run()` constructor.
- `DualWriteEvent(index_name, entity_name, timestamp_ms, timestamp, message)` (Task 2) — used by `_capture_dual_write_start_times` (consumes `event.index_name`, `event.timestamp_ms`).
- `DockerComposeClient.recreate_service(service, compose_env, timeout_s)` (Task 3) — invoked from `RollingRestartPhase.run()` (Task 4) with kwargs matching.
- `RollingRestartPhase(docker, mae_service, services_in_order, new_image_tag, dual_write_timeout_s, recreate_timeout_s)` (Task 4) — runner constructs with subset (Task 5).
- `_compose_env_for_service(service, new_image_tag) -> dict[str, str]` (Task 4) — module-level helper, tested independently.

**Risks called out:**

1. **Partial restart on failure.** If GMS recreates fine but MAE fails, the stack is left with NEW GMS + OLD MAE + OLD MCE — a mixed state. Plan 4 reports `failed` but doesn't roll back. The operator decides: re-run Phase 6 (which will retry from GMS — idempotent, since the GMS is already on NEW), or down + restart with OLD across the board. This is acceptable for a test framework but worth flagging.
2. **Dual-write timeout heuristic.** `dual_write_timeout_s=60` is a wall-clock bound on log tailing. If MAE startup is slow, T1 may not be recorded for some indices. Phase 4's behaviour: empty dict for those indices. Phase 10 Validation can assert this is a regression. Increase the timeout if MAE is consistently slow.
3. **Order of services.** Hard-coded GMS → MAE → MCE. If a future deployment introduces a new ZDU-relevant service (e.g., `datahub-actions`), Plan 4 doesn't include it. Add to `_DEFAULT_SERVICES_IN_ORDER` in a follow-up.
4. **Spy-only integration validation.** Real two-image swap is the operator's prep step. Plan 4 only verifies the framework's intent (correct calls, correct order, correct env). End-to-end image swap can be validated when a future PR provides built OLD/NEW image tags in CI.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-4-rolling-restart.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
