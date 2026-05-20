"""Phase — SkipAlreadyMigratedSweepPhase.

TC-326 (sweep skips already-migrated rows) — verifies the per-aspect
``NOT LIKE '%"schemaVersion":<target>%'`` filter in
``EbeanAspectDao.streamAspectBatchesForMigration`` actually excludes
rows that are already at the target ``schemaVersion`` from the
migration stream, so the sweep neither rewrites them nor clobbers
their ``createdon`` / ``appSource`` provenance.

This is NOT a duplicate of TC-316 (re-run-after-SUCCEEDED no-op). TC-316
tests the upgrade-state short-circuit — when ``DataHubUpgradeResult.state``
is ``SUCCEEDED``, the entire step is skipped. TC-326 tests the
row-level SQL filter on a fresh ``PENDING`` run: even when the sweep
DOES run, already-at-target rows must be left untouched.

Flow:
  1. Bulk-seed N rows with mixed shape: half at OLD ``schemaVersion=1``,
     half at target ``schemaVersion=4``. Use a dedicated URN prefix
     ``urn:li:dashboard:(test,zdu-tc-326-NNNN)`` so the capture is
     unaffected by aspects seeded by prior phases.
  2. Snapshot the v4 rows' ``createdon`` + ``systemmetadata`` per URN
     via ``MySQLClient.get_aspect_raw`` — this is the bit-identical
     baseline we'll compare against post-sweep.
  3. Delete the ``migrate-aspects-<version>`` upgrade-result row so
     the upgrade framework's step-gating doesn't skip the
     MigrateAspects step entirely (it short-circuits when prev
     state=SUCCEEDED — same mechanism TC-316 asserts as a test). Without
     this reset the sweep is a no-op and TC-326's signal is masked.
  4. Run ``system-update -u SystemUpdateNonBlocking`` with default
     config (no batch-size / delay-ms overrides — we're testing the
     filter, not the throttle).
  5. Wait for upgrade-job exit; verify:
     - Every v1-seeded URN is now at ``schemaVersion=4`` in MySQL.
     - Every v4-seeded URN has ``createdon`` + ``systemmetadata``
       bit-identical to the pre-sweep snapshot — proving the sweep did
       not touch it.

JSON format note: production writes systemMetadata via Jackson with no
space after ``:``. The SQL filter pattern is ``%"schemaVersion":4%``
(also no space). To get the v4 seed rows correctly filtered out, this
phase emits the v4 systemMetadata via ``json.dumps(..., separators=(",", ":"))``
— without that, Python's default ``": "`` adds a space and the filter
pattern fails to match (the v4 rows enter the stream and get rewritten,
producing a false positive).
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
from ..context import SkipAlreadyMigratedCapture, TestContext
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)


_DEFAULT_SEED_V1_COUNT = 100
_DEFAULT_SEED_V4_COUNT = 100
_DEFAULT_TARGET_SCHEMA_VERSION = 4
_DEFAULT_OLD_SCHEMA_VERSION = 1
_DEFAULT_ASPECT = "embed"
_DEFAULT_URN_PREFIX = "urn:li:dashboard:(test,zdu-tc-326-"
_DEFAULT_RUN_TIMEOUT_S = 600
_CONTAINER_NAME = "zdu-tc326-skip-migrated-upgrade"


class SkipAlreadyMigratedSweepPhase(Phase):
    name = "skip_already_migrated_sweep"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        seed_v1_count: int = _DEFAULT_SEED_V1_COUNT,
        seed_v4_count: int = _DEFAULT_SEED_V4_COUNT,
        target_schema_version: int = _DEFAULT_TARGET_SCHEMA_VERSION,
        old_schema_version: int = _DEFAULT_OLD_SCHEMA_VERSION,
        aspect: str = _DEFAULT_ASPECT,
        urn_prefix: str = _DEFAULT_URN_PREFIX,
        run_timeout_s: int = _DEFAULT_RUN_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._seed_v1_count = seed_v1_count
        self._seed_v4_count = seed_v4_count
        self._target_schema_version = target_schema_version
        self._old_schema_version = old_schema_version
        self._aspect = aspect
        self._urn_prefix = urn_prefix
        self._run_timeout_s = run_timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:
        started_at = datetime.utcnow()
        t0 = time.monotonic()
        cap = SkipAlreadyMigratedCapture(
            seed_v1_count=self._seed_v1_count,
            seed_v4_count=self._seed_v4_count,
        )
        ctx.skip_migrated_capture = cap

        self._docker.remove_container(_CONTAINER_NAME)

        try:
            v1_urns, v4_urns = self._seed_mixed_aspects()
            v4_snapshot = self._snapshot_v4_rows(v4_urns)
        except Exception as exc:
            log.exception("[skip-migrated] seed/snapshot failed: %s", exc)
            return self._fail(
                started_at, time.monotonic() - t0, f"seed/snapshot failed: {exc}"
            )

        # Reset upgrade-state to absent (otherwise the upgrade framework's
        # step-gating skips the entire MigrateAspects step on prev
        # state=SUCCEEDED — the same property TC-316 asserts as a test).
        deleted = self._mysql.delete_upgrade_result_by_urn_prefix("migrate-aspects-")
        log.info(
            "[skip-migrated] cleared %d migrate-aspects upgrade-result row(s)", deleted
        )

        run_start = time.monotonic()
        try:
            self._run_sweep()
        except Exception as exc:
            log.exception("[skip-migrated] sweep failed: %s", exc)
            cap.total_duration_s = time.monotonic() - run_start
            return self._fail(started_at, time.monotonic() - t0, f"sweep failed: {exc}")
        cap.total_duration_s = time.monotonic() - run_start

        cap.post_sweep_v1_at_target_count = self._count_at_target(v1_urns)
        cap.post_sweep_v4_untouched_count = self._count_untouched(v4_urns, v4_snapshot)

        try:
            _, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
                "migrate-aspects-"
            )
            if parsed is not None:
                cap.final_upgrade_state = parsed.get("state")
        except Exception as exc:
            log.warning("[skip-migrated] could not read final upgrade state: %s", exc)

        log.info(
            "[skip-migrated] phase done: v1→target %d/%d, v4 untouched %d/%d, "
            "state=%s, total %.1fs",
            cap.post_sweep_v1_at_target_count,
            cap.seed_v1_count,
            cap.post_sweep_v4_untouched_count,
            cap.seed_v4_count,
            cap.final_upgrade_state,
            cap.total_duration_s,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=started_at,
            duration_s=time.monotonic() - t0,
            details={
                "seed_v1_count": cap.seed_v1_count,
                "seed_v4_count": cap.seed_v4_count,
                "post_sweep_v1_at_target_count": cap.post_sweep_v1_at_target_count,
                "post_sweep_v4_untouched_count": cap.post_sweep_v4_untouched_count,
                "final_upgrade_state": cap.final_upgrade_state,
                "total_duration_s": cap.total_duration_s,
            },
        )

    def _seed_mixed_aspects(self) -> tuple[list[str], list[str]]:
        """Bulk-seed v1 + v4 rows. Returns (v1_urns, v4_urns)."""
        # Compact JSON (no space after colon) so the production SQL filter
        # pattern ``%"schemaVersion":<target>%`` matches v4 rows.
        v1_sysmeta = json.dumps(
            {"schemaVersion": self._old_schema_version}, separators=(",", ":")
        )
        v4_sysmeta = json.dumps(
            {"schemaVersion": self._target_schema_version}, separators=(",", ":")
        )
        v1_urns: list[str] = []
        v4_urns: list[str] = []
        rows: list[tuple[str, str, str, str]] = []
        idx = 1
        for _ in range(self._seed_v1_count):
            urn = f"{self._urn_prefix}{idx:04d})"
            v1_urns.append(urn)
            md = json.dumps(
                {"renderUrl": f"http://zdu-test.example.com/tc-326/v1/{idx:04d}"}
            )
            rows.append((urn, self._aspect, md, v1_sysmeta))
            idx += 1
        for _ in range(self._seed_v4_count):
            urn = f"{self._urn_prefix}{idx:04d})"
            v4_urns.append(urn)
            md = json.dumps(
                {"renderUrl": f"http://zdu-test.example.com/tc-326/v4/{idx:04d}"}
            )
            rows.append((urn, self._aspect, md, v4_sysmeta))
            idx += 1
        seeded = self._mysql.bulk_seed_aspects(rows)
        log.info(
            "[skip-migrated] bulk-seed wrote %d rows (%d v%d + %d v%d)",
            seeded,
            self._seed_v1_count,
            self._old_schema_version,
            self._seed_v4_count,
            self._target_schema_version,
        )
        return v1_urns, v4_urns

    def _snapshot_v4_rows(
        self, v4_urns: list[str]
    ) -> dict[str, tuple[str, str | None]]:
        """Read ``(createdon, systemmetadata)`` per URN for the v4-seeded
        rows so we can verify they're bit-identical after the sweep.
        """
        snap: dict[str, tuple[str, str | None]] = {}
        for urn in v4_urns:
            row = self._mysql.get_aspect_raw(urn, self._aspect)
            if row is None:
                log.warning("[skip-migrated] v4 snapshot missing row for %s", urn)
                continue
            snap[urn] = (row.createdon, row.systemmetadata)
        return snap

    def _count_at_target(self, v1_urns: list[str]) -> int:
        """Count v1-seeded URNs that are now at the target schemaVersion."""
        n = 0
        for urn in v1_urns:
            sv = self._mysql.get_schema_version(urn, self._aspect)
            if sv == self._target_schema_version:
                n += 1
        return n

    def _count_untouched(
        self,
        v4_urns: list[str],
        snapshot: dict[str, tuple[str, str | None]],
    ) -> int:
        """Count v4-seeded URNs whose ``(createdon, systemmetadata)`` is
        bit-identical to ``snapshot`` — proving the sweep did not touch them.
        """
        n = 0
        for urn in v4_urns:
            baseline = snapshot.get(urn)
            if baseline is None:
                continue
            row = self._mysql.get_aspect_raw(urn, self._aspect)
            if row is None:
                continue
            if row.createdon == baseline[0] and row.systemmetadata == baseline[1]:
                n += 1
        return n

    def _run_sweep(self) -> None:
        log.info("[skip-migrated] starting upgrade job (container=%s)", _CONTAINER_NAME)
        proc = self._launch_upgrade()
        drain_thread = threading.Thread(target=_drain_stdout, args=(proc,), daemon=True)
        drain_thread.start()

        try:
            deadline = time.monotonic() + self._run_timeout_s
            while proc.poll() is None:
                if time.monotonic() > deadline:
                    proc.kill()
                    proc.wait(timeout=10)
                    raise RuntimeError(f"sweep exceeded {self._run_timeout_s}s timeout")
                time.sleep(0.5)

            rc = proc.returncode
            log.info("[skip-migrated] upgrade-job exited rc=%d", rc)
            if rc != 0:
                raise RuntimeError(f"upgrade-job exited rc={rc}")
        finally:
            # Always reap the container — both on the success path and on
            # error paths (timeout, non-zero rc, Docker hiccup). Without
            # this the leaked container persists until the next E2E run.
            self._docker.remove_container(_CONTAINER_NAME)

    def _launch_upgrade(self) -> "subprocess.Popen[str]":
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="skip_already_migrated_sweep"
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
    """Background drain so the upgrade-job container doesn't block on a
    full stdout pipe.
    """
    if proc.stdout is None:
        return
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if line:
                log.info("[skip-migrated] %s", line)
    except (OSError, ValueError):
        pass
