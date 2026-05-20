"""Phase — KillSwitchSweepPhase.

TC-324 (cursor resumability) — exercises the sweep's mid-execution
recovery path. The flow:

1. Bulk-seed N test aspects at OLD ``schemaVersion`` directly into MySQL
   (bypasses GMS write path so no mutator chain fires).
2. Start a fresh ``system-update -u SystemUpdateNonBlocking`` in detached
   mode with a deterministic container name.
3. Poll MySQL every ~200 ms for the count of seeded aspects that have
   reached the target ``schemaVersion``. When ≥ kill-threshold are
   migrated, SIGKILL the upgrade-job container.
4. Capture MySQL state at kill: ``DataHubUpgradeResult.state``,
   ``lastCreatedOnMs`` cursor, count of migrated aspects.
5. Remove the killed container (so the restart can reuse the name).
6. Restart ``system-update -u SystemUpdateNonBlocking`` (same upgrade-id;
   production code reads ``IN_PROGRESS`` MySQL state and resumes from
   the cursor).
7. Tail logs for the cursor-load message → ``resume_log_observed``.
8. Wait for completion. Capture final state.
9. Populate ``ctx.kill_switch_capture`` for the TC-324 validator.

The phase runs LATE in the pipeline (after the regular ZDU upgrade has
completed). The regular Suite N entities are already at target by then,
so our 1000 new test entities are the only rows the sweep would touch.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from ..constants import REPO_ROOT
from ..context import KillSwitchCapture, TestContext
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)


_DEFAULT_SEED_COUNT = 1000
_DEFAULT_KILL_THRESHOLD = 500
_DEFAULT_TARGET_SCHEMA_VERSION = 4
_DEFAULT_OLD_SCHEMA_VERSION = 1
_DEFAULT_ASPECT = "embed"
_DEFAULT_URN_PREFIX = "urn:li:dashboard:(test,zdu-tc-324-"
_DEFAULT_POLL_INTERVAL_S = 0.2
_DEFAULT_RESTART_TIMEOUT_S = 600
_KILL_CONTAINER_NAME = "zdu-tc324-killswitch-upgrade"
_RESTART_CONTAINER_NAME = "zdu-tc324-restart-upgrade"
# Cursor-load log line emitted by MigrateAspectsStep.java:129 when it
# resumes from a prior IN_PROGRESS state.
_RESUME_LOG_PATTERNS = (
    "Resuming from createdOn",
    "lastCreatedOnMs",
)


class KillSwitchSweepPhase(Phase):
    name = "kill_switch_sweep"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        seed_count: int = _DEFAULT_SEED_COUNT,
        kill_threshold: int = _DEFAULT_KILL_THRESHOLD,
        target_schema_version: int = _DEFAULT_TARGET_SCHEMA_VERSION,
        old_schema_version: int = _DEFAULT_OLD_SCHEMA_VERSION,
        aspect: str = _DEFAULT_ASPECT,
        urn_prefix: str = _DEFAULT_URN_PREFIX,
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
        restart_timeout_s: int = _DEFAULT_RESTART_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._seed_count = seed_count
        self._kill_threshold = kill_threshold
        self._target_schema_version = target_schema_version
        self._old_schema_version = old_schema_version
        self._aspect = aspect
        self._urn_prefix = urn_prefix
        self._poll_interval_s = poll_interval_s
        self._restart_timeout_s = restart_timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    # ── public entry point ─────────────────────────────────────────────────

    def run(self, ctx: TestContext) -> PhaseResult:  # noqa: C901
        started_at = datetime.utcnow()
        t0 = time.monotonic()
        cap = KillSwitchCapture(
            seed_count=self._seed_count,
            kill_threshold=self._kill_threshold,
        )
        ctx.kill_switch_capture = cap

        # Best-effort cleanup of stale containers from prior runs (idempotent).
        self._docker.remove_container(_KILL_CONTAINER_NAME)
        self._docker.remove_container(_RESTART_CONTAINER_NAME)

        try:
            self._seed_test_aspects()
        except Exception as exc:
            log.exception("[kill-switch] bulk seed failed: %s", exc)
            return self._fail(
                started_at, time.monotonic() - t0, f"bulk seed failed: {exc}"
            )

        # Phase 1: kill mid-sweep.
        kill_start = time.monotonic()
        try:
            self._run_kill_phase(ctx, cap)
        except Exception as exc:
            log.exception("[kill-switch] kill phase failed: %s", exc)
            cap.kill_phase_duration_s = time.monotonic() - kill_start
            return self._fail(
                started_at, time.monotonic() - t0, f"kill phase failed: {exc}"
            )
        cap.kill_phase_duration_s = time.monotonic() - kill_start

        # Phase 2: restart and verify resume.
        resume_start = time.monotonic()
        try:
            self._run_resume_phase(ctx, cap)
        except Exception as exc:
            log.exception("[kill-switch] resume phase failed: %s", exc)
            cap.resume_phase_duration_s = time.monotonic() - resume_start
            return self._fail(
                started_at, time.monotonic() - t0, f"resume phase failed: {exc}"
            )
        cap.resume_phase_duration_s = time.monotonic() - resume_start

        # Final state capture.
        cap.final_aspect_count_at_target = (
            self._mysql.count_aspects_at_schema_version_for_urn_prefix(
                self._urn_prefix, self._aspect, self._target_schema_version
            )
        )
        log.info(
            "[kill-switch] phase done: killed at %d/%d migrated, "
            "final %d/%d at target (kill %.1fs, resume %.1fs)",
            cap.aspects_migrated_at_kill,
            cap.seed_count,
            cap.final_aspect_count_at_target,
            cap.seed_count,
            cap.kill_phase_duration_s,
            cap.resume_phase_duration_s,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=started_at,
            duration_s=time.monotonic() - t0,
            details={
                "seed_count": cap.seed_count,
                "kill_threshold": cap.kill_threshold,
                "aspects_migrated_at_kill": cap.aspects_migrated_at_kill,
                "cursor_at_kill": cap.cursor_at_kill,
                "upgrade_state_at_kill": cap.upgrade_state_at_kill,
                "resume_log_observed": cap.resume_log_observed,
                "final_aspect_count_at_target": cap.final_aspect_count_at_target,
                "final_upgrade_state": cap.final_upgrade_state,
            },
        )

    # ── private helpers ────────────────────────────────────────────────────

    def _seed_test_aspects(self) -> None:
        log.info(
            "[kill-switch] bulk-seeding %d test aspects (%s) at schemaVersion=%d",
            self._seed_count,
            self._aspect,
            self._old_schema_version,
        )
        # systemmetadata at OLD schemaVersion — note: we deliberately omit
        # ``runId`` / ``lastObserved`` here because the sweep doesn't read
        # them, and a minimal payload keeps the bulk-insert SQL compact.
        sysmeta = json.dumps({"schemaVersion": self._old_schema_version})
        # Minimal embed metadata: the v1 chain expects ``renderUrl``.
        rows: list[tuple[str, str, str, str]] = []
        for i in range(1, self._seed_count + 1):
            urn = f"{self._urn_prefix}{i:04d})"
            metadata = json.dumps(
                {"renderUrl": f"http://zdu-test.example.com/tc-324/{i:04d}"}
            )
            rows.append((urn, self._aspect, metadata, sysmeta))
        seeded = self._mysql.bulk_seed_aspects(rows)
        log.info("[kill-switch] bulk-seed wrote %d rows", seeded)

    def _run_kill_phase(self, ctx: TestContext, cap: KillSwitchCapture) -> None:
        log.info(
            "[kill-switch] starting upgrade job (container=%s)",
            _KILL_CONTAINER_NAME,
        )
        proc = self._launch_upgrade(ctx, _KILL_CONTAINER_NAME)
        # Drain stdout in a background thread so the OS pipe buffer doesn't
        # fill and block the upgrade-job container.
        drain_thread = threading.Thread(
            target=_drain_stdout,
            args=(proc, "[kill-switch]"),
            daemon=True,
        )
        drain_thread.start()

        # Poll MySQL for migration progress. Maximum wait ~5 minutes; the
        # sweep typically processes 1000 aspects in 30-60 seconds.
        deadline = time.monotonic() + 300
        killed = False
        last_count = 0
        while time.monotonic() < deadline:
            count = self._mysql.count_aspects_at_schema_version_for_urn_prefix(
                self._urn_prefix, self._aspect, self._target_schema_version
            )
            if count != last_count:
                log.info(
                    "[kill-switch] migration progress: %d/%d at target",
                    count,
                    self._seed_count,
                )
                last_count = count
            if count >= self._kill_threshold:
                log.info(
                    "[kill-switch] kill threshold reached (%d >= %d), SIGKILL container",
                    count,
                    self._kill_threshold,
                )
                cap.aspects_migrated_at_kill = count
                self._capture_upgrade_state_at_kill(cap)
                self._docker.kill_container(_KILL_CONTAINER_NAME, signal="KILL")
                killed = True
                break
            time.sleep(self._poll_interval_s)

        if not killed:
            raise RuntimeError(
                "kill threshold never reached — upgrade either finished too "
                "fast or never migrated any aspect"
            )

        # Wait for the docker process to exit (it dies when the container
        # is killed). We allow a generous grace period because docker compose
        # CLI sometimes takes a few seconds to reap.
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            log.warning(
                "[kill-switch] docker compose CLI did not exit within 30s "
                "after container kill; sending SIGTERM"
            )
            proc.terminate()
            proc.wait(timeout=10)

        # Cleanup: remove the killed container so the restart can reuse the name.
        self._docker.remove_container(_KILL_CONTAINER_NAME)

    def _capture_upgrade_state_at_kill(self, cap: KillSwitchCapture) -> None:
        # MigrateAspects upgrade-id has a stable prefix; find the row whose
        # URN matches it. The framework already has a helper for prefix lookups.
        try:
            upgrade_id, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
                "migrate-aspects-"
            )
        except Exception as exc:
            log.warning(
                "[kill-switch] could not read MigrateAspects upgrade result: %s", exc
            )
            return
        if upgrade_id is None or parsed is None:
            log.warning(
                "[kill-switch] no MigrateAspects upgrade result found at kill time"
            )
            return
        cap.upgrade_state_at_kill = parsed.get("state")
        result = parsed.get("result") or {}
        cursor_str = result.get("lastCreatedOnMs")
        if cursor_str is not None:
            try:
                cap.cursor_at_kill = int(cursor_str)
            except (TypeError, ValueError):
                pass
        log.info(
            "[kill-switch] state at kill: state=%s cursor=%s",
            cap.upgrade_state_at_kill,
            cap.cursor_at_kill,
        )

    def _run_resume_phase(self, ctx: TestContext, cap: KillSwitchCapture) -> None:
        log.info(
            "[kill-switch] restarting upgrade job (container=%s) — expecting resume",
            _RESTART_CONTAINER_NAME,
        )
        proc = self._launch_upgrade(ctx, _RESTART_CONTAINER_NAME)
        # We tail stdout synchronously this time, looking for the resume
        # log line and waiting for completion.
        deadline = time.monotonic() + self._restart_timeout_s
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            if time.monotonic() > deadline:
                proc.kill()
                proc.wait(timeout=10)
                raise RuntimeError(
                    f"restart timed out after {self._restart_timeout_s}s"
                )
            line = raw_line.rstrip()
            if not line:
                continue
            log.info("[kill-switch-resume] %s", line)
            if not cap.resume_log_observed:
                for pattern in _RESUME_LOG_PATTERNS:
                    if pattern in line:
                        cap.resume_log_observed = True
                        log.info(
                            "[kill-switch] resume cursor-load message observed: %r",
                            pattern,
                        )
                        break

        # Wait for process exit.
        rc = proc.wait(timeout=max(deadline - time.monotonic(), 1.0))
        log.info("[kill-switch] restart exited rc=%d", rc)
        if rc != 0:
            raise RuntimeError(f"restart exited rc={rc}")

        # Capture final upgrade state.
        try:
            _, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
                "migrate-aspects-"
            )
            if parsed is not None:
                cap.final_upgrade_state = parsed.get("state")
        except Exception as exc:
            log.warning(
                "[kill-switch] could not read final MigrateAspects state: %s", exc
            )

        # Cleanup the restart container too.
        self._docker.remove_container(_RESTART_CONTAINER_NAME)

    def _launch_upgrade(
        self, ctx: TestContext, container_name: str
    ) -> "subprocess.Popen[str]":
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="kill_switch_sweep"
        )
        mount_env = worktree_mount_env(REPO_ROOT, self._build_images_root, "new")
        compose_env = {
            "DATAHUB_UPDATE_VERSION": self._new_image_tag,
            "DATAHUB_VERSION": self._new_image_tag,
            **(mount_env or {}),
            **token_env,
        }
        env_overrides = {
            **token_env,
            "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED": "true",
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED": "true",
            # The upgrade container starts with this false by default; passing
            # it explicitly registers the AspectMigrationMutator Spring beans
            # so MigrateAspects doesn't short-circuit with "no mutators
            # registered". Verified runtime requirement from existing phases.
            "ASPECT_MIGRATION_MUTATOR_ENABLED": "true",
            # Small batch + per-batch delay so the MySQL cursor advances
            # frequently and we can observe partial migration.
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_BATCH_SIZE": "50",
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_BATCH_DELAY_MS": "50",
        }
        return self._docker.run_upgrade_job(
            env_overrides=env_overrides,
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateNonBlocking"],
            compose_env=compose_env,
            container_name=container_name,
        )

    def _fail(self, started_at: datetime, duration_s: float, error: str) -> PhaseResult:
        return PhaseResult(
            phase_name=self.name,
            status="failed",
            started_at=started_at,
            duration_s=duration_s,
            error=error,
        )


def _drain_stdout(proc: "subprocess.Popen[str]", prefix: str) -> None:
    """Background-thread helper — reads stdout line-by-line until EOF.

    The OS pipe buffer for the upgrade-job container's stdout will fill
    after a few KB if we don't drain it, which causes the container to
    block on its log writes and the kill-switch test to hang.
    """
    if proc.stdout is None:
        return
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if line:
                log.info("%s %s", prefix, line)
    except (OSError, ValueError):
        # Pipe closed when process exits / kill — expected.
        pass
