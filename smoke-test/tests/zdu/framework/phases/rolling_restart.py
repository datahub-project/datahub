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
import threading
import time
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from ..constants import (
    GMS_SERVICE,
    MAE_SERVICE,
    PER_SERVICE_VERSION_KEY,
    REPO_ROOT,
    ZDU_SERVICES_IN_ORDER,
)
from ..context import RollingRestartResult, TestContext
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..log_monitor import _parse_dual_write_line

log = logging.getLogger(__name__)

_DEFAULT_DUAL_WRITE_TIMEOUT_S = 60
_DEFAULT_RECREATE_TIMEOUT_S = 120

# Local aliases kept for diff-clarity during the centralization pass.
# _REQUIRED_PASSTHROUGH_KEYS dropped — caller switched to read_token_passthrough.
_DEFAULT_SERVICES_IN_ORDER = ZDU_SERVICES_IN_ORDER
_PER_SERVICE_VERSION_KEY = PER_SERVICE_VERSION_KEY


def _compose_env_for_service(
    service: str,
    new_image_tag: str,
    passthrough: dict[str, str] | None = None,
    mount_env: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the compose_env dict that overrides ``service``'s image tag.

    Always sets ``DATAHUB_VERSION`` (global fallback). Sets the
    per-service var if recognised. Unknown services get only the global var.
    Merges ``passthrough`` env (token-service secrets) so cascade-recreated
    system-update-debug doesn't crash on Spring init.

    Also forces METADATA_SERVICE_AUTH_ENABLED=false so the recreated GMS
    keeps the auth-bypass that SetupOldStackPhase set up. Compose's YAML
    substitution `${METADATA_SERVICE_AUTH_ENABLED:-true}` reads from the
    parent process env, not the env_file — without this, the rolling
    restart silently re-enables auth and downstream phases (e.g.
    inject_traffic_dual) hit 401 because their token's MySQL state was
    wiped by the nuke.
    """
    env: dict[str, str] = {
        "DATAHUB_VERSION": new_image_tag,
        "METADATA_SERVICE_AUTH_ENABLED": "false",
        # G20a's reindex flags reach the recreated GMS via env_file
        # (zdu-test.env loaded by ${DATAHUB_LOCAL_COMMON_ENV} substitution
        # in the GMS compose YAML), so they don't need to live in
        # compose_env here. compose_env feeds YAML `${...}` interpolation,
        # not container env.
    }
    per_service_key = _PER_SERVICE_VERSION_KEY.get(service)
    if per_service_key is not None:
        env[per_service_key] = new_image_tag
    if passthrough:
        env.update(passthrough)
    # G20c — when the NEW worktree is materialized, pin host mounts to NEW so
    # the recreated services use NEW PDL / GMS war / upgrade jar instead of
    # whatever the dev's working tree last built.
    if mount_env:
        env.update(mount_env)
    return env


class RollingRestartPhase(Phase):
    name = "rolling_restart"

    def __init__(
        self,
        docker: DockerComposeClient,
        mae_service: str = MAE_SERVICE,
        gms_service: str = GMS_SERVICE,
        services_in_order: tuple[str, ...] | list[str] = _DEFAULT_SERVICES_IN_ORDER,
        new_image_tag: str = "debug",
        dual_write_timeout_s: int = _DEFAULT_DUAL_WRITE_TIMEOUT_S,
        recreate_timeout_s: int = _DEFAULT_RECREATE_TIMEOUT_S,
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mae_service = mae_service
        # G20d — also tail GMS for the dual-write line. The debug profile
        # collapses MAE into GMS (no standalone datahub-mae-consumer-debug),
        # so UpdateIndicesUpgradeStrategy emits "Recorded dual-write start
        # time …" into the GMS container's stdout instead. Without this,
        # the tail returns empty and dual_write_start_times stays {}, which
        # cascades into empty catch_up_windows and starves
        # TC-301/303/304's validators. On a real two-service profile, the
        # MAE tail captures the line first and GMS sees nothing — the
        # "first timestamp per index wins" merge in _capture_dual_write...
        # makes the extra tail a safe no-op there.
        self._gms_service = gms_service
        self._services_in_order = list(services_in_order)
        self._new_image_tag = new_image_tag
        self._dual_write_timeout_s = dual_write_timeout_s
        self._recreate_timeout_s = recreate_timeout_s
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        services_restarted: list[str] = []

        # Filter to services actually running in the current compose profile
        # (debug profile collapses MAE+MCE into GMS — they're not separate
        # services there). Mirrors PrepareOldStackPhase.run.
        current = self._docker.get_all_service_images()
        present_services = [s for s in self._services_in_order if s in current]
        absent_services = [s for s in self._services_in_order if s not in current]
        if absent_services:
            log.info(
                "[rolling-restart] services not in current stack, skipping: %s",
                absent_services,
            )

        # Extract token-service secrets so cascade-recreated system-update-debug
        # doesn't crash on Spring init. Pulled from the first present service
        # (typically GMS).
        passthrough: dict[str, str] = {}
        if present_services:
            passthrough = read_token_passthrough(
                self._docker, present_services[0], purpose="rolling_restart"
            )

        # G20c — pin recreated services' host mounts to the NEW worktree.
        mount_env = worktree_mount_env(
            REPO_ROOT,
            self._build_images_root,
            "new",
        )
        if mount_env:
            log.info(
                "[rolling-restart] pinning host mounts to NEW worktree: %s",
                sorted(mount_env.keys()),
            )

        for service in present_services:
            try:
                self._docker.recreate_service(
                    service=service,
                    compose_env=_compose_env_for_service(
                        service,
                        self._new_image_tag,
                        passthrough=passthrough,
                        mount_env=mount_env,
                    ),
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

    def _capture_dual_write_start_times(self, since: datetime) -> dict[str, int]:
        """Tail MAE + GMS logs in parallel for ``Recorded dual-write start time``.

        Each candidate service gets its own daemon thread so the deadline can
        fire even when one of them goes quiet (the blocking readline in
        ``tail_service_logs`` would otherwise prevent the deadline check from
        being evaluated). Results merge into a shared dict guarded by a lock;
        the FIRST timestamp per index wins so the embedded-MAE-in-GMS path
        (debug profile) and the standalone-MAE path (production) both produce
        the same shape without ordering surprises.
        """
        out: dict[str, int] = {}
        out_lock = threading.Lock()
        stop = threading.Event()

        present = set(self._docker.get_all_service_images().keys())
        candidates = [s for s in (self._mae_service, self._gms_service) if s in present]
        if not candidates:
            log.warning(
                "No MAE or GMS service present in current stack — "
                "dual-write start times will not be captured. "
                "Looked for: %s",
                [self._mae_service, self._gms_service],
            )
            return out
        log.info(
            "[rolling-restart] tailing %d service(s) for dual-write start times: %s",
            len(candidates),
            candidates,
        )

        def _tail(service: str) -> None:
            try:
                # B3 — pass the shared ``stop`` event into the tail. When set,
                # the docker compose logs subprocess is terminated, stdout
                # closes, and the ``for line in ...`` loop returns naturally.
                # Without this, the daemon thread parks on readline() past
                # the deadline and the `docker compose logs --follow`
                # subprocess accumulates across phases.
                for line in self._docker.tail_service_logs(
                    service, since=since, stop_event=stop
                ):
                    if stop.is_set():
                        return
                    event = _parse_dual_write_line(line)
                    if event is None:
                        continue
                    # First timestamp per index wins (T1 == first dual-write).
                    with out_lock:
                        if event.index_name not in out:
                            out[event.index_name] = event.timestamp_ms
            except Exception as exc:
                log.warning("Dual-write tail of %s aborted: %s", service, exc)

        threads = [
            threading.Thread(
                target=_tail, args=(svc,), daemon=True, name=f"dual-write-tail-{svc}"
            )
            for svc in candidates
        ]
        for t in threads:
            t.start()
        deadline = time.monotonic() + self._dual_write_timeout_s
        for t in threads:
            remaining = max(deadline - time.monotonic(), 0.0)
            t.join(timeout=remaining)
        # Signal both the watchdog in tail_service_logs (which terminates the
        # subprocess) and the inner loop's stop-check. Then wait briefly for
        # the threads to actually exit so the docker-compose-logs subprocesses
        # don't leak across phases. ``daemon=True`` means anything still alive
        # at this point dies at process exit; the join is best-effort cleanup.
        stop.set()
        cleanup_deadline = time.monotonic() + 2.0
        for t in threads:
            remaining = max(cleanup_deadline - time.monotonic(), 0.0)
            t.join(timeout=remaining)
        return out
