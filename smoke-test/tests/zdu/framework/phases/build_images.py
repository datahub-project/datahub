"""Phase 0 — BuildImagesPhase.

Builds Docker images deterministically from git refs:
- OLD image set: built from a persistent ``git worktree`` at
  ``smoke-test/build/zdu-images/old/`` checked out to the configured OLD ref
  (default ``master``).
- NEW image set: built from a persistent ``git worktree`` at
  ``smoke-test/build/zdu-images/new/`` pinned to the current HEAD SHA.

Worktrees persist across runs; on each run the phase syncs them to the
configured ref via ``git fetch && git reset --hard <ref>``. This keeps
Gradle's incremental-build cache alive between invocations.

Tags use short SHAs so repeated runs cache-hit: ``zdu-old-{sha8}`` and
``zdu-new-{sha8}``. The phase mutates ``config.old_image_tag`` and
``config.new_image_tag`` so downstream phases pick them up automatically.

Skipped unless ``config.build_images`` is True (default). Opt out via
``ZDU_SKIP_BUILD_IMAGES=1``. Legacy ``ZDU_BUILD_IMAGES=1`` is accepted as a
no-op (it is now the default).
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .base import ConfiguredPhase, PhaseResult
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


class BuildImagesPhase(ConfiguredPhase):
    name = "build_images"

    def __init__(
        self,
        repo_root: Path,
        old_ref: str = "master",
        services_to_build: tuple[str, ...] = _DEFAULT_SERVICES,
        gradle_cmd: list[str] | None = None,
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._repo_root = repo_root
        self._old_ref = old_ref
        self._services_to_build = services_to_build
        self._gradle_cmd = list(gradle_cmd) if gradle_cmd else ["./gradlew"]
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext, config: ZDUTestConfig | None = None) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if config is None or not config.build_images:
            log.info("[build-images] build_images disabled — skipping")
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"reason": "build_images disabled"},
            )

        # Resolve NEW's full SHA in the repo BEFORE creating worktrees.
        # Passing the resolved SHA to _ensure_worktree pins the NEW worktree
        # to a specific commit rather than the moving "HEAD" string.
        new_sha_full = self._run_git(["rev-parse", "HEAD"], cwd=self._repo_root)

        old_wt, new_wt = self._worktree_paths()
        old_sha = self._ensure_worktree(old_wt, self._old_ref)
        new_sha = self._ensure_worktree(new_wt, new_sha_full)

        old_image_tag = f"zdu-old-{old_sha}"
        new_image_tag = f"zdu-new-{new_sha}"

        first_missing = self._first_missing_image(old_image_tag, new_image_tag)
        cache_hit = first_missing is None
        services_built: list[str] = []

        if cache_hit:
            log.info(
                "[build-images] cache HIT: %s and %s already exist locally",
                old_image_tag,
                new_image_tag,
            )
        else:
            # Name the missing image. Without this, every cache miss looks
            # identical in the log and you can't tell whether OLD or NEW (or
            # which of the 4 service images) got evicted from the daemon —
            # which matters because GMS / upgrade / mae / mce are pruned
            # independently (the running stack keeps GMS warm but not the
            # other three).
            log.info(
                "[build-images] cache MISS (first missing image: %s): "
                "building OLD=%s NEW=%s",
                first_missing,
                old_image_tag,
                new_image_tag,
            )
            self._build_old(old_image_tag)
            self._build_new(new_image_tag)
            services_built = list(self._services_to_build)

        # Mutate config so downstream phases use the new tags.
        config.old_image_tag = old_image_tag
        config.new_image_tag = new_image_tag

        duration_s = time.monotonic() - t0
        ctx.image_build = ImageBuildResult(
            old_ref=self._old_ref,
            new_ref="HEAD",
            old_sha=old_sha,
            new_sha=new_sha,
            old_image_tag=old_image_tag,
            new_image_tag=new_image_tag,
            cache_hit=cache_hit,
            services_built=services_built,
            duration_s=duration_s,
        )

        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration_s,
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

    def _worktree_paths(self) -> tuple[Path, Path]:
        """Return (old_worktree, new_worktree) paths under build_images_root."""
        root = (self._repo_root / self._build_images_root).resolve()
        return root / "old", root / "new"

    def _ensure_worktree(self, path: Path, ref: str) -> str:
        """Idempotent: if path exists and is a worktree, sync to ref.
        If not, create it. Return the resolved short SHA at path's HEAD."""
        if path.exists() and (path / ".git").exists():
            # Refuse to clobber uncommitted work — `git reset --hard` below
            # would silently discard it. Worktrees under build_images_root
            # are meant to be machine-managed; if someone edited files there
            # it's almost certainly a mistake worth surfacing.
            dirty = self._run_git(["status", "--porcelain"], cwd=path, check=False)
            if dirty.strip():
                raise RuntimeError(
                    f"ZDU worktree at {path} has uncommitted changes — "
                    f"refusing to `git reset --hard {ref}` and lose them. "
                    f"Either commit/stash them or `rm -rf {path}` if you "
                    f"are sure the changes are disposable.\n"
                    f"--- dirty files ---\n{dirty.strip()}"
                )
            # Existing worktree — sync to ref. fetch may fail for local-only
            # refs (e.g. a SHA not on origin), so check=False is intentional.
            self._run_git(["fetch", "origin", ref], cwd=path, check=False)
            self._run_git(["reset", "--hard", ref], cwd=path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            # If a stale worktree registration exists (e.g., dir was rm'd
            # manually), prune it before adding a fresh one.
            self._run_git(["worktree", "prune"], cwd=self._repo_root, check=False)
            self._run_git(
                ["worktree", "add", "--detach", str(path), ref],
                cwd=self._repo_root,
            )
        sha_full = self._run_git(["rev-parse", "HEAD"], cwd=path)
        return sha_full[:8]

    def _first_missing_image(
        self, old_image_tag: str, new_image_tag: str
    ) -> str | None:
        """Return the first ``repo:tag`` that's not in the local Docker daemon,
        or None when every expected image is present (cache HIT).

        Replaces the prior bool-returning ``_all_images_exist`` so the cache
        miss log line can name *which* image was missing — without that, every
        miss prints identically and you can't tell whether OLD or NEW (or
        which of the 4 service images) is the trigger.
        """
        for svc in self._services_to_build:
            repo = _SERVICE_TO_DOCKER_REPO.get(svc)
            if repo is None:
                return f"<no docker repo mapping for {svc}>"
            old_ref = f"{repo}:{old_image_tag}"
            if not self._image_exists(old_ref):
                return old_ref
            new_ref = f"{repo}:{new_image_tag}"
            if not self._image_exists(new_ref):
                return new_ref
        return None

    def _image_exists(self, image_ref: str) -> bool:
        result = subprocess.run(
            ["docker", "image", "inspect", image_ref],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0

    def _build_old(self, image_tag: str) -> None:
        old_wt, _ = self._worktree_paths()
        for service in self._services_to_build:
            log.info(
                "[build-images] building OLD %s → %s from worktree %s",
                service,
                image_tag,
                old_wt,
            )
            result = self._run_gradle(
                [service, f"-Ptag={image_tag}"],
                cwd=old_wt,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"gradle build failed for OLD {service}: "
                    f"rc={result.returncode} stderr={result.stderr[:500]}"
                )
            self._verify_image_tagged(service, image_tag, side="OLD")

    def _build_new(self, image_tag: str) -> None:
        _, new_wt = self._worktree_paths()
        for service in self._services_to_build:
            log.info(
                "[build-images] building NEW %s → %s from worktree %s",
                service,
                image_tag,
                new_wt,
            )
            result = self._run_gradle(
                [service, f"-Ptag={image_tag}"],
                cwd=new_wt,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"gradle build failed for NEW {service}: "
                    f"rc={result.returncode} stderr={result.stderr[:500]}"
                )
            self._verify_image_tagged(service, image_tag, side="NEW")

    def _verify_image_tagged(self, service: str, image_tag: str, side: str) -> None:
        # Trust-but-verify: rc==0 from gradle is necessary but not sufficient.
        # If the docker task ignores -Ptag (wrong property, plugin mismatch),
        # gradle still succeeds but tags as the default versionTag — the cache
        # check then misses on subsequent runs, and prepare_old_stack tries to
        # pull a non-existent image from docker.io. Fail loudly here instead.
        repo = _SERVICE_TO_DOCKER_REPO.get(service)
        if repo is None:
            raise RuntimeError(f"no docker repo mapping for service {service!r}")
        if not self._image_exists(f"{repo}:{image_tag}"):
            raise RuntimeError(
                f"gradle reported success for {side} {service} but "
                f"image {repo}:{image_tag} is not in the local daemon. "
                f"Most likely the docker task ignored -Ptag={image_tag} "
                f"and used the default versionTag instead."
            )

    def _run_git(
        self, args: list[str], cwd: Path | None = None, check: bool = True
    ) -> str:
        cmd = ["git", *args]
        result = subprocess.run(
            cmd,
            cwd=cwd or self._repo_root,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            raise RuntimeError(
                f"git {' '.join(args)} failed: rc={result.returncode} "
                f"stderr={result.stderr[:500]}"
            )
        return result.stdout.strip()

    def _run_gradle(self, args: list[str], cwd: Path) -> subprocess.CompletedProcess:
        cmd = [*self._gradle_cmd, *args]
        return subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
        )
