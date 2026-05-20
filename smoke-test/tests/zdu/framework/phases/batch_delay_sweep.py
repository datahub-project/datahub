"""Phase — BatchDelaySweepPhase.

TC-325 (sweep respects ``batchDelayMs``) — verifies the sweep actually
sleeps for the configured ``delayMs`` between batches.

Flow:
  1. Bulk-seed N aspects (default 200) at OLD ``schemaVersion`` directly
     into MySQL with a deterministic URN prefix.
  2. Start ``system-update -u SystemUpdateNonBlocking`` with explicit
     ``SYSTEM_UPDATE_MIGRATE_ASPECTS_BATCH_SIZE`` and
     ``SYSTEM_UPDATE_MIGRATE_ASPECTS_DELAY_MS`` env overrides.
  3. Poll MySQL's ``migrate-aspects-<version>`` upgrade result every
     ~100ms. Each time the persisted ``lastCreatedOnMs`` cursor advances,
     record the monotonic-time sample — this corresponds to a batch's
     completion + checkpoint write.
  4. Wait for the upgrade-job process to exit cleanly.
  5. Compute the inter-batch gaps from consecutive cursor-advance samples
     and store them on ``ctx.batch_delay_capture`` for the TC-325 validator.

The validator (in ``sweep_executor.py``) asserts the median inter-batch
gap is at least ``delay_ms * tolerance`` (default 70% tolerance) to
account for timing slop from MySQL polling and the upgrade job's batch
processing time.

Runs LATE in the pipeline (after ``kill_switch_sweep``) so its 200 test
entities don't interfere with the regular Suite N captures. Test
entities use the URN prefix ``urn:li:dashboard:(test,zdu-tc-325-NNNN)``.
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
from ..context import BatchDelayCapture, TestContext
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)


_DEFAULT_SEED_COUNT = 200
_DEFAULT_BATCH_SIZE = 50
_DEFAULT_DELAY_MS = 500
_DEFAULT_OLD_SCHEMA_VERSION = 1
_DEFAULT_ASPECT = "embed"
_DEFAULT_URN_PREFIX = "urn:li:dashboard:(test,zdu-tc-325-"
_DEFAULT_POLL_INTERVAL_S = 0.1
_DEFAULT_RUN_TIMEOUT_S = 600
_CONTAINER_NAME = "zdu-tc325-batch-delay-upgrade"


class BatchDelaySweepPhase(Phase):
    name = "batch_delay_sweep"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        seed_count: int = _DEFAULT_SEED_COUNT,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        delay_ms: int = _DEFAULT_DELAY_MS,
        old_schema_version: int = _DEFAULT_OLD_SCHEMA_VERSION,
        aspect: str = _DEFAULT_ASPECT,
        urn_prefix: str = _DEFAULT_URN_PREFIX,
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
        run_timeout_s: int = _DEFAULT_RUN_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._seed_count = seed_count
        self._batch_size = batch_size
        self._delay_ms = delay_ms
        self._old_schema_version = old_schema_version
        self._aspect = aspect
        self._urn_prefix = urn_prefix
        self._poll_interval_s = poll_interval_s
        self._run_timeout_s = run_timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:
        started_at = datetime.utcnow()
        t0 = time.monotonic()
        cap = BatchDelayCapture(
            seed_count=self._seed_count,
            configured_batch_size=self._batch_size,
            configured_delay_ms=self._delay_ms,
        )
        ctx.batch_delay_capture = cap

        # Best-effort cleanup of any stale container.
        self._docker.remove_container(_CONTAINER_NAME)

        try:
            self._seed_test_aspects()
        except Exception as exc:
            log.exception("[batch-delay] bulk seed failed: %s", exc)
            return self._fail(
                started_at, time.monotonic() - t0, f"bulk seed failed: {exc}"
            )

        run_start = time.monotonic()
        try:
            self._run_sweep_and_poll(cap)
        except Exception as exc:
            log.exception("[batch-delay] sweep failed: %s", exc)
            cap.total_duration_s = time.monotonic() - run_start
            return self._fail(started_at, time.monotonic() - t0, f"sweep failed: {exc}")
        cap.total_duration_s = time.monotonic() - run_start

        # Capture final state.
        cap.final_count_at_target = (
            self._mysql.count_aspects_at_schema_version_for_urn_prefix(
                self._urn_prefix, self._aspect, 4
            )
        )
        try:
            _, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
                "migrate-aspects-"
            )
            if parsed is not None:
                cap.final_upgrade_state = parsed.get("state")
        except Exception as exc:
            log.warning("[batch-delay] could not read final upgrade state: %s", exc)

        log.info(
            "[batch-delay] phase done: %d cursor advances, %d/%d at target, "
            "total %.1fs (config: batch_size=%d delay_ms=%d)",
            len(cap.cursor_advance_timestamps_s),
            cap.final_count_at_target,
            cap.seed_count,
            cap.total_duration_s,
            cap.configured_batch_size,
            cap.configured_delay_ms,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=started_at,
            duration_s=time.monotonic() - t0,
            details={
                "seed_count": cap.seed_count,
                "configured_batch_size": cap.configured_batch_size,
                "configured_delay_ms": cap.configured_delay_ms,
                "cursor_advances_observed": len(cap.cursor_advance_timestamps_s),
                "total_duration_s": cap.total_duration_s,
                "final_count_at_target": cap.final_count_at_target,
                "final_upgrade_state": cap.final_upgrade_state,
            },
        )

    def _seed_test_aspects(self) -> None:
        log.info(
            "[batch-delay] bulk-seeding %d aspects at schemaVersion=%d",
            self._seed_count,
            self._old_schema_version,
        )
        sysmeta = json.dumps({"schemaVersion": self._old_schema_version})
        rows: list[tuple[str, str, str, str]] = []
        for i in range(1, self._seed_count + 1):
            urn = f"{self._urn_prefix}{i:04d})"
            metadata = json.dumps(
                {"renderUrl": f"http://zdu-test.example.com/tc-325/{i:04d}"}
            )
            rows.append((urn, self._aspect, metadata, sysmeta))
        seeded = self._mysql.bulk_seed_aspects(rows)
        log.info("[batch-delay] bulk-seed wrote %d rows", seeded)

    def _run_sweep_and_poll(self, cap: BatchDelayCapture) -> None:
        log.info(
            "[batch-delay] starting upgrade job (container=%s, batch_size=%d, delay_ms=%d)",
            _CONTAINER_NAME,
            self._batch_size,
            self._delay_ms,
        )
        proc = self._launch_upgrade()
        drain_thread = threading.Thread(target=_drain_stdout, args=(proc,), daemon=True)
        drain_thread.start()

        # Poll the MigrateAspects upgrade result's cursor (lastCreatedOnMs).
        # Each time it advances, record the timestamp — this corresponds to a
        # batch checkpoint.
        last_cursor: int | None = None
        deadline = time.monotonic() + self._run_timeout_s
        while True:
            # Process exited normally? We're done polling.
            if proc.poll() is not None:
                break
            if time.monotonic() > deadline:
                proc.kill()
                proc.wait(timeout=10)
                raise RuntimeError(f"sweep exceeded {self._run_timeout_s}s timeout")
            try:
                _, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
                    "migrate-aspects-"
                )
            except Exception:
                parsed = None
            current_cursor = self._extract_cursor(parsed)
            if current_cursor is not None and current_cursor != last_cursor:
                cap.cursor_advance_timestamps_s.append(time.monotonic())
                log.info(
                    "[batch-delay] cursor advance #%d: lastCreatedOnMs=%d",
                    len(cap.cursor_advance_timestamps_s),
                    current_cursor,
                )
                last_cursor = current_cursor
            time.sleep(self._poll_interval_s)

        rc = proc.returncode
        log.info("[batch-delay] upgrade-job exited rc=%d", rc)
        if rc != 0:
            raise RuntimeError(f"upgrade-job exited rc={rc}")
        self._docker.remove_container(_CONTAINER_NAME)

    @staticmethod
    def _extract_cursor(parsed: dict | None) -> int | None:
        if not parsed:
            return None
        result = parsed.get("result") or {}
        cursor_str = result.get("lastCreatedOnMs")
        if cursor_str is None:
            return None
        try:
            return int(cursor_str)
        except (TypeError, ValueError):
            return None

    def _launch_upgrade(self) -> "subprocess.Popen[str]":
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="batch_delay_sweep"
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
            "ASPECT_MIGRATION_MUTATOR_ENABLED": "true",
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_BATCH_SIZE": str(self._batch_size),
            "SYSTEM_UPDATE_MIGRATE_ASPECTS_DELAY_MS": str(self._delay_ms),
        }
        return self._docker.run_upgrade_job(
            env_overrides=env_overrides,
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateNonBlocking"],
            compose_env=compose_env,
            container_name=_CONTAINER_NAME,
        )

    def _fail(self, started_at: datetime, duration_s: float, error: str) -> PhaseResult:
        return PhaseResult(
            phase_name=self.name,
            status="failed",
            started_at=started_at,
            duration_s=duration_s,
            error=error,
        )


def _drain_stdout(proc: "subprocess.Popen[str]") -> None:
    """Background-thread helper — reads stdout line-by-line until EOF.

    Without this, the OS pipe buffer fills after a few KB and the upgrade-job
    container blocks on its log writes.
    """
    if proc.stdout is None:
        return
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if line:
                log.info("[batch-delay] %s", line)
    except (OSError, ValueError):
        pass
