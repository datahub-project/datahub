# ZDU E2E — Plan 13: BuildImagesPhase — OLD from `master`, NEW from `HEAD`

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans`.

**Goal:** Add a new Phase 0 `BuildImagesPhase` to the ZDU pipeline that deterministically builds the OLD image set from `master` HEAD and the NEW image set from the current branch HEAD, sets `ZDU_OLD_IMAGE_TAG` / `ZDU_NEW_IMAGE_TAG` on the runtime config, and caches by short-SHA so repeated runs are fast no-ops. Closes the gap left by Plan F-3: the framework had the OLD/NEW tag plumbing but no way to actually produce the two image sets from a developer's working tree.

**Architecture:** One new phase class (`framework/phases/build_images.py`). On run, it:

1. Resolves OLD and NEW git refs to short SHAs (`old_ref` defaults to `master`; NEW = current `HEAD`).
2. Computes deterministic image tags: `zdu-old-{sha8}` and `zdu-new-{sha8}`.
3. Checks if all required images already exist locally with those tags — if so, skip the build (cache hit).
4. For OLD: creates a temp `git worktree` checkout of the OLD ref at `/tmp/zdu-old-{sha8}`, runs `./gradlew <:service>:docker -PdockerVersion=zdu-old-{sha8}` from there for each ZDU-relevant service. Removes the worktree on completion.
5. For NEW: builds the same services from the current working tree with `-PdockerVersion=zdu-new-{sha8}`.
6. Mutates `config.old_image_tag` and `config.new_image_tag` to the new tag strings so downstream phases (`DiscoveryPhase`, `UpgradeBlockingPhase`, `RollingRestartPhase`, `UpgradeNonBlockingPhase`) pick them up.
7. Records build durations + cache-hit/miss + resolved SHAs into `ctx.image_build`.

**Opt-in by default.** Set `ZDU_BUILD_IMAGES=1` to enable. Without it, the phase is skipped (existing single-image-tag behaviour preserved).

**Tech Stack:** Python 3 (existing). Uses `git worktree` (available in all modern git). No new Python dependencies.

**Out of scope (deferred):**

- Frontend image (`datahub-frontend:docker`) — not in the ZDU-critical path; can be added later.
- BuildKit cache mounts — Gradle's `docker buildx build` already uses BuildKit; per-layer caching is automatic.
- Cross-platform/arm64 builds — single-platform (host arch) only.
- Image push to a remote registry — local-only.
- Schema-compatibility check between OLD and NEW — separate plan; the build phase only produces images.
- Building dependent services in parallel — sequential for now (Gradle's daemon mode shares cache across invocations).

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── config.py                                MODIFY — add build_images + old_ref fields + env binding
├── context.py                               MODIFY — add ImageBuildResult dataclass + ctx.image_build slot
├── runner.py                                MODIFY — wire BuildImagesPhase as Phase 0
├── phases/
│   └── build_images.py                      CREATE — BuildImagesPhase
└── test_build_images.py                     CREATE — unit tests (mocked subprocess + worktree)
```

---

## Task 1: `ImageBuildResult` dataclass + ctx slot + config fields

**Files:**

- Modify: `smoke-test/tests/zdu/framework/context.py`
- Modify: `smoke-test/tests/zdu/framework/config.py`

- [ ] **Step 1: Add `ImageBuildResult` dataclass** to `context.py` (after `UpgradeNonBlockingResult`, before `RollingRestartResult`):

```python
@dataclass
class ImageBuildResult:
    """Captured by ``BuildImagesPhase`` when ``ZDU_BUILD_IMAGES=1``.

    ``old_ref`` / ``new_ref`` are the resolved git refs (typically a branch
    name like ``master`` or ``HEAD``). ``old_sha`` / ``new_sha`` are the
    short SHAs the framework derives from those refs. ``old_image_tag`` /
    ``new_image_tag`` are the actual Docker tag strings produced
    (``zdu-old-{sha8}`` / ``zdu-new-{sha8}``).

    ``cache_hit`` is True when ALL required images already existed locally
    with the computed tags — no build was performed. ``services_built`` is
    the list of Gradle service paths the phase invoked (or would have, if
    not for the cache).
    """

    old_ref: str = "master"
    new_ref: str = "HEAD"
    old_sha: str = ""
    new_sha: str = ""
    old_image_tag: str = ""
    new_image_tag: str = ""
    cache_hit: bool = False
    services_built: list[str] = field(default_factory=list)
    duration_s: float = 0.0
```

- [ ] **Step 2: Add `ctx.image_build` slot** to `TestContext` (insert before `# DiscoveryPhase writes`):

```python
    # BuildImagesPhase writes (Phase 0 — opt-in via ZDU_BUILD_IMAGES=1)
    image_build: ImageBuildResult | None = None
```

- [ ] **Step 3: Add config fields** to `ZDUTestConfig` in `config.py`:

```python
    # ── BuildImages (Plan 13) ────────────────────────────────────────────────
    # When True, BuildImagesPhase runs first and produces the OLD/NEW images
    # from the configured git refs. Default off — existing single-image-tag
    # behaviour preserved.
    build_images: bool = False
    # Git ref to build OLD from. Default "master". The NEW image is always
    # built from the current working tree (HEAD).
    build_old_ref: str = "master"
```

And the env bindings:

```python
            ("ZDU_BUILD_IMAGES", "build_images"),
            ("ZDU_OLD_REF", "build_old_ref"),
```

The first parses as bool (`"1"` / `"true"` → True). Existing `_parse_bool` helper if any; otherwise inline.

- [ ] **Step 4: Smoke-import**

```bash
cd <REPO_ROOT>
ZDU_SKIP_BOOTJAR=1 smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext, ImageBuildResult
cfg = ZDUTestConfig()
assert cfg.build_images is False
assert cfg.build_old_ref == 'master'
ctx = TestContext()
assert ctx.image_build is None
ctx.image_build = ImageBuildResult(old_sha='abc12345', new_sha='def67890', old_image_tag='zdu-old-abc12345', new_image_tag='zdu-new-def67890')
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 5: Run framework tests — expect 242 pass.**

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/context.py smoke-test/tests/zdu/framework/config.py
git commit -m "feat(zdu): ImageBuildResult + ctx.image_build + ZDU_BUILD_IMAGES config"
```

---

## Task 2: `BuildImagesPhase` + unit tests

**Files:**

- Create: `smoke-test/tests/zdu/framework/phases/build_images.py`
- Create: `smoke-test/tests/zdu/framework/test_build_images.py`

**Pattern:** Standard `Phase` class. Constructor takes `repo_root: Path`, `old_ref: str`, `services_to_build: tuple[str, ...]`, `gradle_cmd: list[str]` (default `["./gradlew"]`). The phase invokes git + subprocess directly — no client classes needed.

ZDU-critical services to build:

- `:metadata-service:war:docker` (GMS)
- `:datahub-upgrade:docker`
- `:metadata-jobs:mae-consumer-job:docker`
- `:metadata-jobs:mce-consumer-job:docker`

(Frontend deferred per the scope note.)

### 2.1 — Write failing tests

- [ ] **Step 1:** Create `test_build_images.py`:

```python
"""Unit tests for BuildImagesPhase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.build_images import BuildImagesPhase


@pytest.fixture
def phase(tmp_path: Path) -> BuildImagesPhase:
    return BuildImagesPhase(
        repo_root=tmp_path,
        old_ref="master",
        services_to_build=(
            ":metadata-service:war:docker",
            ":datahub-upgrade:docker",
        ),
        gradle_cmd=["./gradlew"],
    )


class TestSkipsWhenDisabled:
    def test_skips_when_build_images_false(self, phase: BuildImagesPhase) -> None:
        cfg = ZDUTestConfig(build_images=False)
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert ctx.image_build is None


class TestShaResolution:
    def test_resolves_short_sha_from_git_ref(
        self, phase: BuildImagesPhase
    ) -> None:
        with patch.object(phase, "_run_git") as mock_git:
            mock_git.side_effect = [
                "abc1234567890abc1234567890abc12345678",  # master rev-parse
                "def0987654321def0987654321def09876543",  # HEAD rev-parse
            ]
            old_sha, new_sha = phase._resolve_shas()
        assert old_sha == "abc12345"
        assert new_sha == "def09876"


class TestCacheHit:
    def test_cache_hit_skips_build_and_returns_passed(
        self, phase: BuildImagesPhase
    ) -> None:
        # All images already exist locally with the computed tags.
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with patch.object(
            phase, "_resolve_shas", return_value=("abc12345", "def09876")
        ), patch.object(phase, "_all_images_exist", return_value=True):
            with patch.object(phase, "_build_old") as mock_old, patch.object(
                phase, "_build_new"
            ) as mock_new:
                result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.image_build is not None
        assert ctx.image_build.cache_hit is True
        assert ctx.image_build.old_image_tag == "zdu-old-abc12345"
        assert ctx.image_build.new_image_tag == "zdu-new-def09876"
        mock_old.assert_not_called()
        mock_new.assert_not_called()


class TestCacheMissBuildsBothSides:
    def test_cache_miss_builds_old_via_worktree_and_new_in_place(
        self, phase: BuildImagesPhase
    ) -> None:
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with patch.object(
            phase, "_resolve_shas", return_value=("abc12345", "def09876")
        ), patch.object(phase, "_all_images_exist", return_value=False), patch.object(
            phase, "_build_old"
        ) as mock_old, patch.object(phase, "_build_new") as mock_new:
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.image_build.cache_hit is False
        mock_old.assert_called_once_with("abc12345", "zdu-old-abc12345")
        mock_new.assert_called_once_with("def09876", "zdu-new-def09876")


class TestMutatesConfigTags:
    def test_run_sets_config_image_tags(self, phase: BuildImagesPhase) -> None:
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with patch.object(
            phase, "_resolve_shas", return_value=("abc12345", "def09876")
        ), patch.object(phase, "_all_images_exist", return_value=True):
            phase.run(ctx, cfg)
        assert cfg.old_image_tag == "zdu-old-abc12345"
        assert cfg.new_image_tag == "zdu-new-def09876"


class TestWorktreeLifecycle:
    def test_build_old_creates_and_removes_worktree(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with patch.object(phase, "_run_git") as mock_git, patch.object(
            phase, "_run_gradle"
        ) as mock_gradle:
            phase._build_old("abc12345", "zdu-old-abc12345")
        # First git call: worktree add. Last: worktree remove.
        calls = [c.args[0] for c in mock_git.call_args_list]
        assert any("worktree" in c and "add" in c for c in calls)
        assert any("worktree" in c and "remove" in c for c in calls)
        # Gradle invoked once for each service in services_to_build (2).
        assert mock_gradle.call_count == 2


class TestRaisesOnGradleFailure:
    def test_gradle_nonzero_returncode_raises(
        self, phase: BuildImagesPhase
    ) -> None:
        with patch.object(phase, "_run_git") as mock_git:
            mock_git.return_value = "abc1234567890abc1234567890abc12345678"
            with patch.object(
                phase, "_run_gradle"
            ) as mock_gradle, patch.object(phase, "_image_exists", return_value=False):
                mock_gradle.return_value = MagicMock(returncode=1, stderr="boom")
                cfg = ZDUTestConfig(build_images=True)
                ctx = TestContext()
                with pytest.raises(RuntimeError, match="gradle"):
                    phase.run(ctx, cfg)
```

(Adjust patch targets if internal method names differ.)

- [ ] **Step 2:** Run tests — expect `ImportError`.

### 2.2 — Implement the phase

- [ ] **Step 3:** Create `phases/build_images.py`:

```python
"""Phase 0 — BuildImagesPhase.

Builds Docker images deterministically from git refs:
- OLD image set: built from a temporary ``git worktree`` checkout of the
  configured OLD ref (default ``master``).
- NEW image set: built from the current working tree (``HEAD``).

Tags use short SHAs so repeated runs cache-hit: ``zdu-old-{sha8}`` and
``zdu-new-{sha8}``. The phase mutates ``config.old_image_tag`` and
``config.new_image_tag`` so downstream phases pick them up automatically.

Skipped unless ``ZDU_BUILD_IMAGES=1``. When skipped, the existing
single-image-tag behaviour (default ``debug``) is preserved.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .base import Phase, PhaseResult
from ..config import ZDUTestConfig
from ..context import ImageBuildResult, TestContext

log = logging.getLogger(__name__)

_DEFAULT_SERVICES = (
    ":metadata-service:war:docker",
    ":datahub-upgrade:docker",
    ":metadata-jobs:mae-consumer-job:docker",
    ":metadata-jobs:mce-consumer-job:docker",
)

# Gradle service path → Docker repo for image-existence checks.
_SERVICE_TO_DOCKER_REPO: dict[str, str] = {
    ":metadata-service:war:docker": "acryldata/datahub-gms",
    ":datahub-upgrade:docker": "acryldata/datahub-upgrade",
    ":metadata-jobs:mae-consumer-job:docker": "acryldata/datahub-mae-consumer",
    ":metadata-jobs:mce-consumer-job:docker": "acryldata/datahub-mce-consumer",
}


class BuildImagesPhase(Phase):
    name = "build_images"

    def __init__(
        self,
        repo_root: Path,
        old_ref: str = "master",
        services_to_build: tuple[str, ...] = _DEFAULT_SERVICES,
        gradle_cmd: list[str] | None = None,
    ) -> None:
        self._repo_root = repo_root
        self._old_ref = old_ref
        self._services_to_build = services_to_build
        self._gradle_cmd = list(gradle_cmd) if gradle_cmd else ["./gradlew"]

    def run(  # type: ignore[override]
        self, ctx: TestContext, config: ZDUTestConfig | None = None
    ) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if config is None or not config.build_images:
            log.info("[build-images] ZDU_BUILD_IMAGES not set — skipping")
            return PhaseResult(
                phase_name=self.name, status="skipped",
                started_at=start, duration_s=time.monotonic() - t0,
                details={"reason": "ZDU_BUILD_IMAGES disabled"},
            )

        old_sha, new_sha = self._resolve_shas()
        old_image_tag = f"zdu-old-{old_sha}"
        new_image_tag = f"zdu-new-{new_sha}"

        cache_hit = self._all_images_exist(old_image_tag, new_image_tag)
        services_built: list[str] = []

        if cache_hit:
            log.info(
                "[build-images] cache HIT: %s and %s already exist locally",
                old_image_tag, new_image_tag,
            )
        else:
            log.info(
                "[build-images] cache MISS: building OLD=%s NEW=%s",
                old_image_tag, new_image_tag,
            )
            self._build_old(old_sha, old_image_tag)
            self._build_new(new_sha, new_image_tag)
            services_built = list(self._services_to_build)

        # Mutate config so downstream phases use the new tags.
        config.old_image_tag = old_image_tag
        config.new_image_tag = new_image_tag

        duration_s = time.monotonic() - t0
        ctx.image_build = ImageBuildResult(
            old_ref=self._old_ref, new_ref="HEAD",
            old_sha=old_sha, new_sha=new_sha,
            old_image_tag=old_image_tag, new_image_tag=new_image_tag,
            cache_hit=cache_hit, services_built=services_built,
            duration_s=duration_s,
        )

        return PhaseResult(
            phase_name=self.name, status="passed",
            started_at=start, duration_s=duration_s,
            details={
                "old_image_tag": old_image_tag,
                "new_image_tag": new_image_tag,
                "cache_hit": cache_hit,
                "services_built": services_built,
                "old_ref": self._old_ref,
                "old_sha": old_sha,
                "new_sha": new_sha,
            },
        )

    # ─── Internals ─────────────────────────────────────────────────────────

    def _resolve_shas(self) -> tuple[str, str]:
        old_full = self._run_git(["rev-parse", self._old_ref])
        new_full = self._run_git(["rev-parse", "HEAD"])
        return old_full[:8], new_full[:8]

    def _all_images_exist(
        self, old_image_tag: str, new_image_tag: str
    ) -> bool:
        for svc in self._services_to_build:
            repo = _SERVICE_TO_DOCKER_REPO.get(svc)
            if repo is None:
                return False
            if not self._image_exists(f"{repo}:{old_image_tag}"):
                return False
            if not self._image_exists(f"{repo}:{new_image_tag}"):
                return False
        return True

    def _image_exists(self, image_ref: str) -> bool:
        result = subprocess.run(
            ["docker", "image", "inspect", image_ref],
            capture_output=True, text=True,
        )
        return result.returncode == 0

    def _build_old(self, old_sha: str, image_tag: str) -> None:
        worktree_path = Path(f"/tmp/zdu-old-{old_sha}")
        self._run_git(
            ["worktree", "add", "--detach", str(worktree_path), self._old_ref],
            cwd=self._repo_root,
        )
        try:
            for service in self._services_to_build:
                log.info(
                    "[build-images] building OLD %s → %s from worktree %s",
                    service, image_tag, worktree_path,
                )
                result = self._run_gradle(
                    [service, f"-PdockerVersion={image_tag}"],
                    cwd=worktree_path,
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"gradle build failed for OLD {service}: "
                        f"rc={result.returncode} stderr={result.stderr[:500]}"
                    )
        finally:
            self._run_git(
                ["worktree", "remove", "--force", str(worktree_path)],
                cwd=self._repo_root, check=False,
            )

    def _build_new(self, new_sha: str, image_tag: str) -> None:
        for service in self._services_to_build:
            log.info(
                "[build-images] building NEW %s → %s from %s",
                service, image_tag, self._repo_root,
            )
            result = self._run_gradle(
                [service, f"-PdockerVersion={image_tag}"],
                cwd=self._repo_root,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"gradle build failed for NEW {service}: "
                    f"rc={result.returncode} stderr={result.stderr[:500]}"
                )

    def _run_git(
        self, args: list[str], cwd: Path | None = None, check: bool = True
    ) -> str:
        cmd = ["git", *args]
        result = subprocess.run(
            cmd, cwd=cwd or self._repo_root, capture_output=True, text=True,
        )
        if check and result.returncode != 0:
            raise RuntimeError(
                f"git {' '.join(args)} failed: rc={result.returncode} "
                f"stderr={result.stderr[:500]}"
            )
        return result.stdout.strip()

    def _run_gradle(
        self, args: list[str], cwd: Path
    ) -> subprocess.CompletedProcess:
        cmd = [*self._gradle_cmd, *args]
        return subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True,
        )
```

- [ ] **Step 4:** Run tests — expect ~7 pass.

- [ ] **Step 5:** Run full framework suite — expect ~249 pass.

- [ ] **Step 6:** Commit.

```bash
git add smoke-test/tests/zdu/framework/phases/build_images.py \
        smoke-test/tests/zdu/framework/test_build_images.py
git commit -m "feat(zdu): BuildImagesPhase — produce zdu-old-{sha} and zdu-new-{sha} from git refs"
```

---

## Task 3: Wire `BuildImagesPhase` into the runner as Phase 0

**Files:**

- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Insert FIRST in the phases list — before `discovery`. Pass `config` through so the phase can read `build_images` and mutate `old_image_tag` / `new_image_tag` before downstream phases read them.

- [ ] **Step 1:** Add import:

```python
from pathlib import Path  # already imported
from .phases.build_images import BuildImagesPhase
```

- [ ] **Step 2:** Insert tuple as the FIRST entry in the `phases = [...]` list:

```python
            (
                "build_images",
                BuildImagesPhase(
                    repo_root=Path(config.project_dir).parent.parent
                    if "docker/profiles" in config.project_dir
                    else Path.cwd(),
                    old_ref=config.build_old_ref,
                ),
            ),
```

The `repo_root` derivation: `config.project_dir` is typically `docker/profiles` (absolute path under the repo); the repo root is its parent's parent. Fall back to `Path.cwd()` if the heuristic doesn't fire. Document the fallback in a comment.

- [ ] **Step 3:** Update the dispatch loop to pass `config` through to `BuildImagesPhase.run`. The cleanest approach: have `BuildImagesPhase.run` accept an optional second arg (`config`), and the loop detects `build_images` by phase name to pass it:

```python
        for phase_name, phase in phases:
            if phase_name in self._config.skip_phases:
                log.info("Skipping phase: %s", phase_name)
                continue

            log.info("▶ Phase: %s", phase_name)
            if phase_name == "build_images":
                result = phase.run(ctx, self._config)  # type: ignore[call-arg]
            else:
                result = phase.run(ctx)
            report.phase_results.append(result)
```

(Acceptable narrow special-case. Alternative: extend the `Phase` base-class signature; defer that refactor.)

- [ ] **Step 4:** Smoke-test runner constructs:

```bash
cd <REPO_ROOT>
ZDU_SKIP_BOOTJAR=1 smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
print('runner constructed OK')
"
```

Expected: `runner constructed OK`.

- [ ] **Step 5:** Run framework tests — expect ~249 pass.

- [ ] **Step 6:** Commit.

```bash
git add smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): wire BuildImagesPhase as Phase 0 in runner pipeline"
```

---

## Task 4: README — BuildImagesPhase docs

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`
- Modify: `smoke-test/tests/zdu/run_test.md`

- [ ] **Step 1:** Add a "Phase 0: Build Images (opt-in)" subsection in the Two-Image-Tag section of `README.md`:

````markdown
### Phase 0: Build Images (opt-in)

`BuildImagesPhase` deterministically builds the OLD and NEW image sets from git refs and sets `config.old_image_tag` / `config.new_image_tag` automatically — no manual `docker pull` or `./gradlew :*:docker` invocations needed.

**Behaviour:**

- **OLD image set** — built from a temporary `git worktree` checkout of the configured OLD ref (default `master`). Tag: `zdu-old-{sha8}` where `sha8` is the 8-char prefix of the ref's resolved commit SHA.
- **NEW image set** — built from the current working tree (`HEAD`). Tag: `zdu-new-{sha8}`.
- **Cached by SHA** — if all required images already exist locally with the computed tags, the phase is a no-op. Reruns are fast.
- **Services built:** `:metadata-service:war:docker`, `:datahub-upgrade:docker`, `:metadata-jobs:mae-consumer-job:docker`, `:metadata-jobs:mce-consumer-job:docker`. Frontend deferred.
- **Mutates config:** sets `config.old_image_tag` and `config.new_image_tag` before downstream phases run. Override `ZDU_OLD_IMAGE_TAG` / `ZDU_NEW_IMAGE_TAG` env vars are ignored when this phase runs.

**Opt-in:** Set `ZDU_BUILD_IMAGES=1`. Without it, the phase is skipped and the framework uses the manually-set (or default `debug`) image tags.

**Override OLD ref:** `ZDU_OLD_REF=v1.5.0` to build OLD from a release tag instead of `master`.

**Cold build time** is ~20–35 minutes per side (40–70 min total). Subsequent runs cache-hit and complete in seconds.

```bash
# First run — builds both sides
ZDU_BUILD_IMAGES=1 \
DATAHUB_GMS_TOKEN="$(grep '  token:' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m tests.zdu --suite a

# Subsequent runs — cache hit, near-instant
ZDU_BUILD_IMAGES=1 \
DATAHUB_GMS_TOKEN="$(grep '  token:' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m tests.zdu --suite a
```
````

````

- [ ] **Step 2:** Mirror a shorter version in `run_test.md` under "Two-image-tag CI mode (real ZDU)".

- [ ] **Step 3:** Commit.

```bash
git add smoke-test/tests/zdu/README.md smoke-test/tests/zdu/run_test.md
git commit -m "docs(zdu): document Phase 0 BuildImagesPhase"
````

---

## Task 5: Live integration check

**Pre-requisite:** Both git refs accessible, Docker daemon up.

- [ ] **Step 1:** Validate SHA resolution (no actual build):

```bash
cd <REPO_ROOT>/smoke-test && \
  ZDU_SKIP_BOOTJAR=1 venv/bin/python << 'PY'
from pathlib import Path
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.build_images import BuildImagesPhase

repo = Path("<REPO_ROOT>")
phase = BuildImagesPhase(repo_root=repo, old_ref="master")
old_sha, new_sha = phase._resolve_shas()
print(f"OLD master sha8: {old_sha}")
print(f"NEW HEAD sha8:   {new_sha}")
print(f"Would build: zdu-old-{old_sha} / zdu-new-{new_sha}")
PY
```

Expected: two distinct 8-char SHA prefixes.

- [ ] **Step 2:** Test the skip path with `ZDU_BUILD_IMAGES` unset:

```bash
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking \
venv/bin/python -m tests.zdu --suite a --only-tc 1 2>&1 | tail -15
```

Expected: `build_images` phase reports `skipped (reason: ZDU_BUILD_IMAGES disabled)`. Suite A TC-1 runs normally.

- [ ] **Step 3 (optional, slow):** Trigger an actual cold build:

```bash
ZDU_BUILD_IMAGES=1 \
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking,upgrade_blocking \
venv/bin/python -m tests.zdu --suite a --only-tc 1 2>&1 | tee /tmp/zdu-build.log | tail -10
```

This will take 40–70 min on a cold cache. The phase output should show "cache MISS: building OLD=zdu-old-{sha} NEW=zdu-new-{sha}" then per-service gradle invocations. Verify the images exist after the build:

```bash
docker images | grep "zdu-old-\|zdu-new-"
```

- [ ] **Step 4 (optional):** Re-run to verify cache hit:

```bash
ZDU_BUILD_IMAGES=1 \
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart,upgrade_nonblocking,upgrade_blocking \
venv/bin/python -m tests.zdu --suite a --only-tc 1 2>&1 | tail -5
```

Expected: phase completes in <2s with "cache HIT" log message.

- [ ] **Step 5:** Commit any live-validation fixes if needed.

---

## Task 6: Code review

- [ ] **Step 1:** Generate diff.
- [ ] **Step 2:** Dispatch `feature-dev:code-reviewer`.
- [ ] **Step 3:** Address feedback.

---

## Self-Review

**Spec coverage:**

- ✅ Build OLD from `master` HEAD via temp worktree (safe, non-destructive).
- ✅ Build NEW from current working tree (`HEAD`).
- ✅ Mutate `config.old_image_tag` / `config.new_image_tag` for downstream phases.
- ✅ Cache by short-SHA — fast reruns.
- ✅ Opt-in via `ZDU_BUILD_IMAGES=1` — single-image dev workflow unaffected by default.
- ❌ Frontend image — deferred.
- ❌ Schema-compatibility check — deferred.

**Type / signature consistency:**

- `ImageBuildResult` dataclass matches the conventional `*Result` shape.
- `BuildImagesPhase.run(ctx, config)` — accepts `config` as second positional arg; runner special-cases this phase name to pass it.

**Risks:**

1. **40–70 min cold build.** First run on a fresh machine is slow. Users will see no progress until each `:docker` task completes. Consider piping gradle output to stdout (currently captured) — but capturing keeps the test log readable. Trade-off accepted; document in the README.
2. **`git worktree` requires clean source tree.** If the user has uncommitted changes to files that are also modified between HEAD and master, `worktree add` may complain. The phase doesn't pre-check this; failure surfaces in the gradle output. Acceptable for an opt-in path.
3. **Worktree cleanup on failure.** The `try/finally` ensures the worktree is removed even if gradle fails. Removed with `--force` to handle any post-build artefacts.
4. **`config.project_dir` heuristic for `repo_root`.** Brittle if a user sets `ZDU_PROJECT_DIR` to a non-standard path. Falls back to `Path.cwd()`. Document.
5. **Image-existence check** uses `docker image inspect` rather than scanning `docker images` — handles tagged-but-orphaned layers correctly.
6. **No parallel builds.** Gradle daemon mode reuses cache across invocations, so serial isn't much slower than parallel for the same module hierarchy.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-11-zdu-plan-13-build-images-from-git-refs.md`.

Per session policy: defaulting to subagent-driven execution.
