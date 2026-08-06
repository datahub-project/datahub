"""Phase — UpgradeBlockingReRunPhase.

Runs ``system-update -u SystemUpdateBlocking`` a SECOND time, after Phase 6
(``UpgradeBlockingPhase``) has already completed. Production code at
``BuildIndicesIncrementalStep.java:103-112`` reads
``IncrementalReindexState.Status`` from MySQL and emits
``Index <name> already COMPLETED in previous run, skipping`` for any index it
processed before. This phase captures those events so TC-109 can assert the
second run is a no-op (no new physical indices created, every previously
reindexed alias either skipped or absent from the rerun log).

This phase intentionally does NOT fail-fast on observed re-reindexes — the
TC-109 validator surfaces those. The phase only fails on shell exit-rc != 0
or on timeout, so a clean run with one regression still produces ctx data
for the validator to consume.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from .upgrade_blocking import _start_deadline_watchdog
from ..constants import REPO_ROOT
from ..context import TestContext, UpgradeBlockingReRunResult
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..log_monitor import Phase1State, _parse_phase1_line
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 600


class UpgradeBlockingReRunPhase(Phase):
    name = "upgrade_blocking_rerun"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._timeout_s = timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        # If Phase 6 didn't run, the rerun has no baseline to test against.
        # Mark skipped (not failed) so this doesn't block downstream phases.
        if ctx.upgrade_blocking is None:
            log.info(
                "[upgrade-blocking-rerun] skipping — Phase 6 (upgrade_blocking) did not run"
            )
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={"reason": "Phase 6 (upgrade_blocking) did not run"},
            )

        t0 = time.monotonic()
        deadline = t0 + self._timeout_s
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="upgrade_blocking_rerun"
        )
        proc = self._launch(token_env)

        skip_already_done: list[str] = []
        rerun_swaps: list[tuple[str, str]] = []
        timed_out = False
        assert proc.stdout is not None

        watchdog_fired = _start_deadline_watchdog(proc, deadline)

        for raw_line in proc.stdout:
            if time.monotonic() > deadline:
                timed_out = True
                break
            line = raw_line.rstrip()
            if not line:
                continue
            log.info("[upgrade-rerun] %s", line)
            event = _parse_phase1_line(line)
            if event is None:
                continue
            if event.state == Phase1State.SKIP_ALREADY_DONE and event.index_name:
                skip_already_done.append(event.index_name)
            elif (
                event.state == Phase1State.ALIAS_SWAPPED
                and event.index_name is not None
            ):
                rerun_swaps.append((event.index_name, event.next_index_name or ""))

        if watchdog_fired.is_set():
            timed_out = True

        if timed_out:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log.error("UpgradeBlockingReRun process did not exit after kill")
            duration = time.monotonic() - t0
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=f"system-update rerun timed out after {self._timeout_s}s",
                details={
                    "skip_already_done_aliases": skip_already_done,
                    "rerun_alias_swaps_observed": rerun_swaps,
                },
            )

        try:
            proc.wait(timeout=max(deadline - time.monotonic(), 1.0))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            duration = time.monotonic() - t0
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=(
                    f"system-update rerun exited stdout but did not finish "
                    f"reaping within {self._timeout_s}s"
                ),
                details={
                    "skip_already_done_aliases": skip_already_done,
                    "rerun_alias_swaps_observed": rerun_swaps,
                },
            )

        rc = proc.returncode
        duration = time.monotonic() - t0

        ctx.upgrade_blocking_rerun = UpgradeBlockingReRunResult(
            skip_already_done_aliases=skip_already_done,
            rerun_alias_swaps_observed=rerun_swaps,
            duration_s=duration,
            upgrade_id=None,
            rerun_exit_code=rc,
        )

        if rc != 0:
            # Capture rerun_exit_code in ctx (already done above) so the
            # TC-109 validator can surface the failure with full context,
            # but mark the phase failed so downstream knows.
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=f"system-update rerun exited rc={rc}",
                details={
                    "skip_already_done_aliases": skip_already_done,
                    "rerun_alias_swaps_observed": rerun_swaps,
                    "rerun_exit_code": rc,
                },
            )

        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "skip_already_done_aliases": skip_already_done,
                "rerun_alias_swaps_observed": rerun_swaps,
                "rerun_exit_code": rc,
                "duration_s": duration,
            },
        )

    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
        # Identical wiring to UpgradeBlockingPhase: same host mounts, same
        # env overrides. The production code path that detects prior
        # completion lives in BuildIndicesIncrementalStep — no different
        # config needed.
        mount_env = worktree_mount_env(
            REPO_ROOT,
            self._build_images_root,
            "new",
        )
        if mount_env:
            log.info(
                "[upgrade-rerun] pinning host mounts to NEW worktree: %s",
                sorted(mount_env.keys()),
            )
        compose_env = {
            "DATAHUB_UPDATE_VERSION": self._new_image_tag,
            "DATAHUB_VERSION": self._new_image_tag,
            **(mount_env or {}),
            **token_env,
        }
        env_overrides = {
            **token_env,
            "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED": "true",
            "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX": "true",
        }
        return self._docker.run_upgrade_job(
            env_overrides=env_overrides,
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateBlocking"],
            compose_env=compose_env,
        )
