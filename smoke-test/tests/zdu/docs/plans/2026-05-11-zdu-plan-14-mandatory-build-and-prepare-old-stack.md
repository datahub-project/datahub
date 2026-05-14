# ZDU E2E — Plan 14: Mandatory Build Verification + `prepare_old_stack` Phase

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans`.

**Goal:** Close the one-button-workflow gap from Plan 13 by:

1. **Refactoring `BuildImagesPhase`** to use **persistent worktree folders** at `smoke-test/build/zdu-images/{old,new}/` (instead of `/tmp/zdu-old-{sha}` ephemerals). One folder per side — keeps Gradle's incremental-build cache between runs.
2. **Flipping the default** — build verification becomes **mandatory by default**. Opt-out via new env var `ZDU_SKIP_BUILD_IMAGES=1` (was opt-in via `ZDU_BUILD_IMAGES=1` in Plan 13).
3. **Adding `PrepareOldStackPhase`** (new Phase 0.5) — checks whether the running Compose stack matches `config.old_image_tag`; if not, recreates GMS/MAE/MCE with the OLD tag and waits for health. Discovery + seed then run against OLD as ZDU semantics require.

**Architecture:**

- New `BuildImagesPhase` worktree layout: `smoke-test/build/zdu-images/old/` (git worktree of `config.build_old_ref`, default `master`) and `smoke-test/build/zdu-images/new/` (git worktree of current branch HEAD). Worktrees persist across runs; on each run, the phase syncs the worktree HEAD to the configured ref via `git fetch && git reset --hard <ref>`.
- New `PrepareOldStackPhase` (`framework/phases/prepare_old_stack.py`) — runs as Stage 2 in `Runner.run()` (between `build_images` and `discovery`-list-construction). Reads `config.old_image_tag`, inspects current images via `DockerComposeClient.get_all_service_images()`, recreates services if mismatch via `recreate_service()` (reused from `RollingRestartPhase`).
- **Default flip:** `config.build_images = True` (was `False`). New env var `ZDU_SKIP_BUILD_IMAGES=1` to disable. Old env var `ZDU_BUILD_IMAGES=1` is now a no-op (kept for backward compat with one release; logs a deprecation warning).
- **`prepare_old_stack` default:** `config.prepare_old_stack = True`. New env var `ZDU_SKIP_PREPARE_OLD_STACK=1` to disable.

**Tech Stack:** Python 3 (existing). Reuses `DockerComposeClient.recreate_service`, `git worktree`, no new dependencies.

**Out of scope (deferred):**

- Cleanup of `smoke-test/build/zdu-images/` worktrees on test completion — leave them in place as build cache. Manual cleanup: `git worktree remove --force smoke-test/build/zdu-images/old smoke-test/build/zdu-images/new`.
- Pre-flight `docker pull` if neither building nor a local OLD/NEW image exists. Build path is now mandatory; if user genuinely needs to use a pre-pulled tag, they set `ZDU_SKIP_BUILD_IMAGES=1` + `ZDU_OLD_IMAGE_TAG=...`.
- Schema-compatibility check between OLD and NEW.
- Volume snapshot/restore for fast iteration on the same OLD↔NEW pair.
- Parallel side-by-side builds (still serial).

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── config.py                          MODIFY — flip build_images default; add prepare_old_stack: bool = True + env vars
├── context.py                         MODIFY — add PrepareOldStackResult + ctx slot
├── runner.py                          MODIFY — three-stage construction: build → prepare → downstream
├── phases/
│   ├── build_images.py                REFACTOR — persistent worktrees at smoke-test/build/zdu-images/{old,new}
│   └── prepare_old_stack.py           CREATE — PrepareOldStackPhase
├── test_build_images.py               MODIFY — update tests for persistent worktree paths
├── test_prepare_old_stack.py          CREATE — 6 unit tests
└── README.md + run_test.md            MODIFY — document the default-on flow + opt-outs
```

```
smoke-test/build/zdu-images/                  CREATE (runtime) — under **/build gitignore
├── old/                                      git worktree of `master`
└── new/                                      git worktree of current branch HEAD
```

---

## Task 1: Refactor `BuildImagesPhase` to persistent worktrees + flip default

**Files:**

- Modify: `smoke-test/tests/zdu/framework/config.py`
- Modify: `smoke-test/tests/zdu/framework/phases/build_images.py`
- Modify: `smoke-test/tests/zdu/framework/test_build_images.py`

### 1.1 Config flip

- [ ] **Step 1:** In `config.py`:

```python
    # ── BuildImages (Plan 13/14) ─────────────────────────────────────────────
    # Default-on: BuildImagesPhase runs every test and ensures OLD/NEW images
    # are built deterministically from git refs. Opt out via ZDU_SKIP_BUILD_IMAGES=1
    # when iterating on Python-only changes (uses cached debug images).
    build_images: bool = True
    build_old_ref: str = "master"
    # Persistent worktree root for OLD/NEW image builds.
    build_images_root: str = "smoke-test/build/zdu-images"
```

In `from_env`, replace the `ZDU_BUILD_IMAGES` parsing with:

```python
        # Bool fields
        if os.environ.get("ZDU_SKIP_BUILD_IMAGES") in ("1", "true", "True", "yes"):
            kwargs["build_images"] = False
        elif os.environ.get("ZDU_BUILD_IMAGES") in ("1", "true", "True", "yes"):
            # Backward-compat: ZDU_BUILD_IMAGES=1 was the Plan-13 opt-in. Now
            # it's the default, so this is a no-op but keep accepting it.
            kwargs["build_images"] = True
```

Plus new string field binding:

```python
            ("ZDU_BUILD_IMAGES_ROOT", "build_images_root"),
```

### 1.2 Phase refactor — persistent worktrees

- [ ] **Step 2:** Update `build_images.py`:

Replace the `/tmp/zdu-old-{sha8}` worktree path with a config-driven persistent path. New `_worktree_paths` helper:

```python
def _worktree_paths(self) -> tuple[Path, Path]:
    """Return (old_worktree, new_worktree) paths under build_images_root."""
    root = (self._repo_root / self._build_images_root).resolve()
    return root / "old", root / "new"
```

Constructor adds `build_images_root: str` param (default `"smoke-test/build/zdu-images"`).

Replace the worktree-add-then-remove pattern with sync-or-create:

```python
def _ensure_worktree(self, path: Path, ref: str) -> str:
    """Idempotent: if path exists and is a worktree, sync to ref.
    If not, create it. Return the resolved short SHA at path's HEAD."""
    if path.exists() and (path / ".git").exists():
        # Existing worktree — sync to ref
        self._run_git(["fetch", "origin", ref], cwd=path, check=False)
        self._run_git(["reset", "--hard", ref], cwd=path)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        # `git worktree add --detach <path> <ref>` creates a detached worktree.
        # If a stale worktree registration exists (e.g., dir was rm'd manually),
        # `git worktree prune` cleans it up first.
        self._run_git(["worktree", "prune"], cwd=self._repo_root, check=False)
        self._run_git(
            ["worktree", "add", "--detach", str(path), ref],
            cwd=self._repo_root,
        )
    sha_full = self._run_git(["rev-parse", "HEAD"], cwd=path)
    return sha_full[:8]
```

Then `_build_old` and `_build_new` both build from their respective worktrees:

```python
def _build_old(self, image_tag: str) -> None:
    old_wt, _ = self._worktree_paths()
    for service in self._services_to_build:
        result = self._run_gradle(
            [service, f"-PdockerVersion={image_tag}"], cwd=old_wt,
        )
        if result.returncode != 0:
            raise RuntimeError(...)

def _build_new(self, image_tag: str) -> None:
    _, new_wt = self._worktree_paths()
    for service in self._services_to_build:
        result = self._run_gradle(
            [service, f"-PdockerVersion={image_tag}"], cwd=new_wt,
        )
        if result.returncode != 0:
            raise RuntimeError(...)
```

`run(ctx, config)`:

1. `old_wt, new_wt = self._worktree_paths()`
2. `old_sha = self._ensure_worktree(old_wt, self._old_ref)` — also resolves SHA
3. `new_sha = self._ensure_worktree(new_wt, "HEAD")` — but HEAD must be resolved BEFORE worktree-add (because HEAD inside a new worktree would be detached at that ref). Resolve in repo first: `new_sha_full = self._run_git(["rev-parse", "HEAD"], cwd=self._repo_root)` → `new_sha = new_sha_full[:8]`. Then `_ensure_worktree(new_wt, new_sha_full)`.
4. Compute tags, check cache, build (only on cache miss), mutate config.

### 1.3 Tests

- [ ] **Step 3:** Update `test_build_images.py`:

The 7 existing tests need updating to mock `_ensure_worktree` and the persistent path. New tests to add:

- `test_existing_worktree_synced_to_ref` — mock `path.exists()` True, verify `git fetch + reset --hard` are called instead of `worktree add`.
- `test_new_worktree_uses_resolved_sha_not_head_string` — verifies HEAD is resolved BEFORE worktree-add so the new worktree is pinned to a specific commit.

### 1.4 Verification

- [ ] **Step 4:** Smoke-import + framework tests pass.
- [ ] **Step 5:** Commit:

```bash
git add smoke-test/tests/zdu/framework/config.py \
        smoke-test/tests/zdu/framework/phases/build_images.py \
        smoke-test/tests/zdu/framework/test_build_images.py
git commit -m "feat(zdu): BuildImagesPhase persistent worktrees + mandatory-by-default

Refactor /tmp/zdu-old-{sha} ephemeral worktrees into persistent folders
at smoke-test/build/zdu-images/{old,new}. Each side maintains a long-lived
git worktree that's fetched+reset to its configured ref on every run.
Gradle's incremental-build cache survives between runs.

Flip default: build_images=True. Opt-out via ZDU_SKIP_BUILD_IMAGES=1
(legacy ZDU_BUILD_IMAGES=1 kept for backward compat as a no-op)."
```

---

## Task 2: `PrepareOldStackPhase` + 6 unit tests

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py` (add `PrepareOldStackResult`)
- Modify: `smoke-test/tests/zdu/framework/config.py` (add `prepare_old_stack: bool = True` + env var)
- Create: `smoke-test/tests/zdu/framework/phases/prepare_old_stack.py`
- Create: `smoke-test/tests/zdu/framework/test_prepare_old_stack.py`

### 2.1 Dataclass + config

- [ ] **Step 1:** Add to `context.py`:

```python
@dataclass
class PrepareOldStackResult:
    """Captured by ``PrepareOldStackPhase`` when ``ZDU_SKIP_PREPARE_OLD_STACK`` is unset.

    ``current_images`` is the {service: image_string} snapshot taken
    BEFORE any restart. ``recreated_services`` is the list of services
    that were actually restarted (subset of ``services_inspected``).
    """

    old_image_tag: str = ""
    current_images: dict[str, str] = field(default_factory=dict)
    services_inspected: list[str] = field(default_factory=list)
    recreated_services: list[str] = field(default_factory=list)
    health_check_passed: bool = False
    duration_s: float = 0.0
```

Add slot to `TestContext` (before `# DiscoveryPhase writes`):

```python
    # PrepareOldStackPhase writes (Phase 0.5)
    prepare_old_stack: PrepareOldStackResult | None = None
```

Add to `config.py`:

```python
    # ── PrepareOldStack (Plan 14) ────────────────────────────────────────────
    # Default-on: ensure the running Compose stack matches config.old_image_tag
    # before seed/discovery run. Opt out via ZDU_SKIP_PREPARE_OLD_STACK=1.
    prepare_old_stack: bool = True
```

In `from_env`:

```python
        if os.environ.get("ZDU_SKIP_PREPARE_OLD_STACK") in ("1", "true", "True", "yes"):
            kwargs["prepare_old_stack"] = False
```

### 2.2 Failing tests

- [ ] **Step 2:** Create `test_prepare_old_stack.py`:

```python
"""Unit tests for PrepareOldStackPhase."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.prepare_old_stack import PrepareOldStackPhase


@pytest.fixture
def phase() -> PrepareOldStackPhase:
    docker = MagicMock()
    docker.get_all_service_images.return_value = {}
    return PrepareOldStackPhase(
        docker=docker,
        services_to_restart=("datahub-gms-debug", "datahub-mae-consumer-debug", "datahub-mce-consumer-debug"),
        gms_service="datahub-gms-debug",
        health_url="http://localhost:8080",
        timeout_s=180,
    )


class TestSkipsWhenDisabled:
    def test_skips_when_prepare_old_stack_false(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=False)
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert ctx.prepare_old_stack is None


class TestSkipsWhenTagIsDefault:
    def test_skips_when_old_image_tag_is_debug(self, phase) -> None:
        # If Phase 0 didn't run, old_image_tag is the default 'debug' —
        # nothing meaningful to restart to. Skip with a clear log.
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="debug")
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert "debug" in (result.details or {}).get("reason", "").lower()


class TestNoRestartWhenAlreadyOnOld:
    def test_passes_without_restart_when_image_matches(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        # The running GMS already uses the OLD tag in its image string.
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:zdu-old-abc12345",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:zdu-old-abc12345",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:zdu-old-abc12345",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.prepare_old_stack is not None
        assert ctx.prepare_old_stack.recreated_services == []
        phase._docker.recreate_service.assert_not_called()


class TestRecreatesWhenMismatched:
    def test_recreates_all_services_when_running_debug(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:debug",
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert phase._docker.recreate_service.call_count == 3
        assert ctx.prepare_old_stack.recreated_services == list(phase._services_to_restart)


class TestPartialMismatch:
    def test_recreates_only_mismatched_services(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",  # mismatch
            "datahub-mae-consumer-debug": "acryldata/datahub-mae-consumer:zdu-old-abc12345",  # match
            "datahub-mce-consumer-debug": "acryldata/datahub-mce-consumer:debug",  # mismatch
        }
        with patch.object(phase, "_wait_for_health", return_value=True):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        recreated = ctx.prepare_old_stack.recreated_services
        assert "datahub-gms-debug" in recreated
        assert "datahub-mce-consumer-debug" in recreated
        assert "datahub-mae-consumer-debug" not in recreated


class TestHealthCheckTimeout:
    def test_returns_failed_when_health_check_times_out(self, phase) -> None:
        cfg = ZDUTestConfig(prepare_old_stack=True, old_image_tag="zdu-old-abc12345")
        ctx = TestContext()
        phase._docker.get_all_service_images.return_value = {
            "datahub-gms-debug": "acryldata/datahub-gms:debug",
        }
        with patch.object(phase, "_wait_for_health", return_value=False):
            result = phase.run(ctx, cfg)
        assert result.status == "failed"
        assert "health" in (result.error or "").lower()
```

### 2.3 Implementation

- [ ] **Step 3:** Create `phases/prepare_old_stack.py`:

```python
"""Phase 0.5 — PrepareOldStackPhase.

Ensures the running Compose stack matches ``config.old_image_tag`` before
``seed`` / ``discovery`` run. Mirrors ``RollingRestartPhase`` but runs
INTO the OLD image (vs OLD→NEW) and fires BEFORE any test traffic so
service ordering is not as sensitive.

Skipped when ``ZDU_SKIP_PREPARE_OLD_STACK=1`` or when ``config.old_image_tag``
is the default ``debug`` (i.e., Phase 0 BuildImagesPhase didn't run).
"""

from __future__ import annotations

import logging
import time
import urllib.request
from datetime import datetime

from .base import Phase, PhaseResult
from ..config import ZDUTestConfig
from ..context import PrepareOldStackResult, TestContext
from ..docker_compose import DockerComposeClient

log = logging.getLogger(__name__)

# Per-service compose env: maps each service to its DATAHUB_*_VERSION key.
_SERVICE_VERSION_ENV: dict[str, str] = {
    "datahub-gms-debug": "DATAHUB_GMS_VERSION",
    "datahub-mae-consumer-debug": "DATAHUB_MAE_VERSION",
    "datahub-mce-consumer-debug": "DATAHUB_MCE_VERSION",
}


def _compose_env(service: str, tag: str) -> dict[str, str]:
    env: dict[str, str] = {"DATAHUB_VERSION": tag}
    if (per_svc := _SERVICE_VERSION_ENV.get(service)) is not None:
        env[per_svc] = tag
    return env


class PrepareOldStackPhase(Phase):
    name = "prepare_old_stack"

    def __init__(
        self,
        docker: DockerComposeClient,
        services_to_restart: tuple[str, ...],
        gms_service: str,
        health_url: str,
        timeout_s: int = 180,
    ) -> None:
        self._docker = docker
        self._services_to_restart = services_to_restart
        self._gms_service = gms_service
        self._health_url = health_url
        self._timeout_s = timeout_s

    def run(  # type: ignore[override]
        self, ctx: TestContext, config: ZDUTestConfig
    ) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if not config.prepare_old_stack:
            return PhaseResult(
                phase_name=self.name, status="skipped",
                started_at=start, duration_s=time.monotonic() - t0,
                details={"reason": "ZDU_SKIP_PREPARE_OLD_STACK set or config disabled"},
            )

        old_tag = config.old_image_tag
        if not old_tag or old_tag == "debug":
            return PhaseResult(
                phase_name=self.name, status="skipped",
                started_at=start, duration_s=time.monotonic() - t0,
                details={
                    "reason": (
                        f"old_image_tag is '{old_tag}' — Phase 0 build_images "
                        f"likely didn't run; skipping OLD-stack restart"
                    )
                },
            )

        current = self._docker.get_all_service_images()
        log.info(
            "[prepare-old-stack] current images: %s",
            {k: v for k, v in current.items() if k in self._services_to_restart},
        )

        mismatched = [
            svc for svc in self._services_to_restart
            if old_tag not in current.get(svc, "")
        ]

        if not mismatched:
            log.info("[prepare-old-stack] all services already on OLD (%s)", old_tag)
            ctx.prepare_old_stack = PrepareOldStackResult(
                old_image_tag=old_tag, current_images=current,
                services_inspected=list(self._services_to_restart),
                recreated_services=[], health_check_passed=True,
                duration_s=time.monotonic() - t0,
            )
            return PhaseResult(
                phase_name=self.name, status="passed",
                started_at=start, duration_s=time.monotonic() - t0,
                details={
                    "old_image_tag": old_tag,
                    "recreated_services": [],
                    "reason": "all services already on OLD",
                },
            )

        log.info(
            "[prepare-old-stack] recreating %d service(s) onto %s: %s",
            len(mismatched), old_tag, mismatched,
        )
        for service in mismatched:
            log.info("[prepare-old-stack] recreate %s @ %s", service, old_tag)
            self._docker.recreate_service(
                service=service, compose_env=_compose_env(service, old_tag),
                timeout_s=self._timeout_s,
            )

        healthy = self._wait_for_health(self._health_url, self._timeout_s)
        duration_s = time.monotonic() - t0
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag=old_tag, current_images=current,
            services_inspected=list(self._services_to_restart),
            recreated_services=mismatched, health_check_passed=healthy,
            duration_s=duration_s,
        )
        if not healthy:
            return PhaseResult(
                phase_name=self.name, status="failed",
                started_at=start, duration_s=duration_s,
                error=(
                    f"GMS health check did not pass after recreating {len(mismatched)} "
                    f"service(s) onto {old_tag} within {self._timeout_s}s"
                ),
                details={"recreated_services": mismatched, "old_image_tag": old_tag},
            )
        return PhaseResult(
            phase_name=self.name, status="passed",
            started_at=start, duration_s=duration_s,
            details={
                "old_image_tag": old_tag,
                "recreated_services": mismatched,
                "current_images_before": current,
            },
        )

    def _wait_for_health(self, url: str, timeout_s: int) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{url}/health", timeout=5) as resp:
                    if resp.status == 200:
                        return True
            except Exception:
                pass
            time.sleep(2)
        return False
```

- [ ] **Step 4:** Run tests — expect 6 pass.
- [ ] **Step 5:** Commit.

---

## Task 3: Three-stage runner restructure

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

Stages:

1. **Phase 0:** `build_images` runs (default-on; can be skipped via `config.skip_phases` or `config.build_images=False`).
2. **Phase 0.5:** `prepare_old_stack` runs (default-on; reads `config.old_image_tag` which Phase 0 just mutated).
3. **Phases 1-10:** Construct the rest of the phase list (now reading correct `config.old_image_tag` / `config.new_image_tag`), run as before.

Concrete diff:

```python
def run(self) -> ZDUReport:
    report = ZDUReport(config=self._config)
    ctx = TestContext(gms_url=self._config.gms_url)

    # ── Stage 1: build_images (Phase 0) ────────────────────────────────
    # (existing code — keep)

    # ── Stage 2: prepare_old_stack (Phase 0.5) — NEW ───────────────────
    if "prepare_old_stack" not in self._config.skip_phases:
        log.info("▶ Phase: prepare_old_stack")
        prepare_phase = PrepareOldStackPhase(
            docker=self._docker,
            services_to_restart=(
                "datahub-gms-debug",
                "datahub-mae-consumer-debug",
                "datahub-mce-consumer-debug",
            ),
            gms_service=self._config.gms_service,
            health_url=self._config.gms_url,
            timeout_s=180,
        )
        prepare_result = prepare_phase.run(ctx, self._config)
        report.phase_results.append(prepare_result)
        if prepare_result.status == "failed" and self._config.fail_fast:
            log.error("Phase prepare_old_stack FAILED — aborting (fail_fast=True)")
            report.ended_at = datetime.utcnow()
            report.scenario_results = ctx.validation_results
            self._reporter.write(report, ctx=ctx)
            return report
    else:
        log.info("Skipping phase: prepare_old_stack")

    # ── Stage 3: construct downstream phases (existing) ────────────────
    phases = [
        ("discovery", DiscoveryPhase(...)),
        ...
    ]
    ...
```

- [ ] **Step 1:** Apply restructure.
- [ ] **Step 2:** Smoke-test runner constructs.
- [ ] **Step 3:** Framework regression.
- [ ] **Step 4:** Commit.

---

## Task 4: README + run_test.md docs

- [ ] **Step 1:** README — replace "Phase 0: Build Images (opt-in)" subsection with "Phase 0 + 0.5: Build Images + Prepare OLD Stack (default-on)". Document opt-outs.
- [ ] **Step 2:** run_test.md — flip env-var table entries: `ZDU_BUILD_IMAGES` is deprecated/no-op, new entries `ZDU_SKIP_BUILD_IMAGES` and `ZDU_SKIP_PREPARE_OLD_STACK`.
- [ ] **Step 3:** Commit.

---

## Task 5: Live integration check

- [ ] **Step 1:** Test the default-on, cache-hit path:

```bash
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking \
ZDU_SKIP_BUILD_IMAGES=1 \
ZDU_SKIP_PREPARE_OLD_STACK=1 \
smoke-test/venv/bin/python -m tests.zdu --suite a --only-tc 1 2>&1 | tail -20
```

Expected: both new phases report `skipped`, TC-1 PASSes (existing dev behavior preserved when opted-out).

- [ ] **Step 2:** Test prepare_old_stack with default tag (should auto-skip):

```bash
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking \
ZDU_SKIP_BUILD_IMAGES=1 \
smoke-test/venv/bin/python -m tests.zdu --suite a --only-tc 1 2>&1 | tail -20
```

Expected: `build_images` skipped (env-var). `prepare_old_stack` runs but skips because `old_image_tag` is `debug`. Phases proceed normally.

- [ ] **Step 3 (optional, slow):** Full one-button test with actual build:

```bash
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking \
smoke-test/venv/bin/python -m tests.zdu --suite a --only-tc 1
```

Expected: cold cache → builds ~40-70 min, then `prepare_old_stack` recreates GMS/MAE/MCE onto `zdu-old-{sha}`, then TC-1 runs against the rebuilt OLD stack.

---

## Task 6: Final code review

- [ ] **Step 1:** Generate diff vs Plan 13 baseline.
- [ ] **Step 2:** Dispatch `feature-dev:code-reviewer`.
- [ ] **Step 3:** Address feedback.

---

## Self-Review

**Spec coverage** (against user's stated requirements):

| Requirement                                                             | Where                                                                                                                    |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Build verification is not optional                                      | Task 1.1 (flip default to True)                                                                                          |
| Two image-tag folders, one OLD from master, one NEW from current branch | Task 1.2 (`smoke-test/build/zdu-images/{old,new}/` worktrees)                                                            |
| Check deployed Docker build is OLD; if not, redeploy                    | Task 2 (`PrepareOldStackPhase` — mismatch → recreate)                                                                    |
| Discovery + seeding run on OLD build                                    | Task 3 (three-stage runner — Phase 0.5 runs before phase-list construction, so downstream phases see post-restart state) |
| Other steps continue                                                    | Existing pipeline unchanged from Plan 13                                                                                 |

**Risks:**

1. **Default-on build is heavy** (~40-70 min cold first time). Cache makes subsequent runs near-instant. README must warn loudly.
2. **`prepare_old_stack` tears down whatever's running.** A user iterating on the dev stack would lose state. Opt-out exists; document conspicuously.
3. **Worktree persistence + `**/build`gitignore** — verified that`smoke-test/build/zdu-images/`falls under`\*\*/build`. Worktrees survive `git clean -fdx` only if user explicitly excludes the path.
4. **`prepare_old_stack` auto-skip when tag = "debug"** is defensive — handles the case where user opts out of `build_images` but forgets to opt out of `prepare_old_stack`. Clearer than failing.
5. **Volume preservation** — `docker compose up -d <service>` recreates the container but preserves named volumes (mysql/opensearch data). Critical for ZDU semantics.
6. **Two new dataclasses + ctx slots** — backward-compatible additions; existing test fixtures construct `TestContext()` with all-default args.

---

Per session policy: defaulting to subagent-driven execution.
