"""Phase 4 — UpgradeBlockingPhase.

Runs ``system-update -u SystemUpdateBlocking`` against the live stack and
captures the resulting ``DataHubUpgradeResult.indicesState[*]`` plus the
real-time log evidence of alias swaps into ``TestContext.upgrade_blocking``.

The phase does NOT rebuild containers — it assumes the running GMS image
already has the schema/mapping changes the upgrade job will apply. Use the
legacy ``UpgradePhase`` for image swaps.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import time
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from ..constants import REPO_ROOT
from ..context import IndexState, TestContext, UpgradeBlockingResult
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..log_monitor import Phase1State, _parse_phase1_line
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 600


def _start_deadline_watchdog(
    proc: "subprocess.Popen[str]", deadline: float
) -> threading.Event:
    """Spawn a daemon thread that terminates ``proc`` once ``deadline`` passes.

    Returns an event the caller can check post-loop to disambiguate "natural
    exit" from "watchdog tripped". Daemon=True so the thread doesn't block
    process exit if the deadline is far in the future.
    """
    fired = threading.Event()

    def _watchdog() -> None:
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        if proc.poll() is None:
            fired.set()
            proc.terminate()

    threading.Thread(target=_watchdog, daemon=True, name="upgrade-deadline").start()
    return fired


def parse_indices_state(payload: dict) -> list[IndexState]:
    """Convert a parsed ``DataHubUpgradeResult.indicesState`` map into IndexState rows.

    Skips entries whose value is not a dict (defensive against malformed JSON).
    Kept for backward compatibility; the canonical capture path now goes
    through ``_parse_flat_indices_state`` (real-stack output ships values as
    flat ``<alias>.<key>`` keys under ``result``, not nested under ``indicesState``).
    """
    out: list[IndexState] = []
    for alias, entry in payload.items():
        if not isinstance(entry, dict):
            log.warning("indicesState entry for %s is not a dict, skipping", alias)
            continue
        out.append(
            IndexState(
                alias=alias,
                next_index_name=entry.get("nextIndexName"),
                old_backing_index_name=entry.get("oldBackingIndexName"),
                reindex_start_time=entry.get("reindexStartTime"),
                source_doc_count=int(entry.get("sourceDocCount", 0) or 0),
                task_id=entry.get("taskId") or None,
                requires_data_backfill=bool(entry.get("requiresDataBackfill", False)),
                status=entry.get("status", "UNKNOWN"),
            )
        )
    return out


_INT_KEYS = {"sourceDocCount", "reindexStartTime", "reindexCompleteTime"}
_BOOL_KEYS = {"requiresDataBackfill"}


def _coerce_flat_value(key: str, value: object) -> object:
    """String→int/bool coercion for the flat-key shape MySQL emits.

    ``BuildIndicesIncrementalStep`` writes every value as a string
    (``"19"``, ``"true"``, ``"COMPLETED"``); coerce here so validators see
    typed values. Defensive against missing / non-numeric inputs.
    """
    if key in _INT_KEYS:
        try:
            return int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return 0
    if key in _BOOL_KEYS:
        return str(value).lower() == "true"
    return value


def _parse_flat_indices_state(result_block: dict) -> dict[str, dict]:
    """Convert flat ``<alias>.<field>`` keys under ``result`` into a per-alias dict.

    Real-stack ``BuildIndicesIncremental_<version>.result`` shape::

        {
          "dashboardindex_v2.status": "COMPLETED",
          "dashboardindex_v2.taskId": "...",
          "dashboardindex_v2.nextIndexName": "...",
          "dashboardindex_v2.sourceDocCount": "19",
          "dashboardindex_v2.requiresDataBackfill": "true",
          "schemafieldindex_v2.status": "COMPLETED",
          ...
        }

    Returns the unflattened form::

        {
          "dashboardindex_v2": {"status": "COMPLETED", "taskId": "...",
                                "sourceDocCount": 19,
                                "requiresDataBackfill": True, ...},
          "schemafieldindex_v2": {...},
        }
    """
    per_alias: dict[str, dict] = {}
    for flat_key, value in result_block.items():
        if "." not in flat_key:
            continue
        alias, _, sub_key = flat_key.partition(".")
        entry = per_alias.setdefault(alias, {})
        entry[sub_key] = _coerce_flat_value(sub_key, value)
    return per_alias


class UpgradeBlockingPhase(Phase):
    name = "upgrade_blocking"

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

    def run(self, ctx: TestContext) -> PhaseResult:  # noqa: C901
        # Complexity is over the default 15-branch threshold because this is
        # a stream-parser loop with several distinct event states (alias
        # swap, in-place update, reindex failure, watchdog timeout, exit
        # code, reap timeout). Splitting it costs more readability than the
        # threshold buys back.
        start = datetime.utcnow()
        t0 = time.monotonic()
        deadline = t0 + self._timeout_s

        token_env = self._read_token_env()
        proc = self._launch(token_env)

        alias_swaps: list[tuple[str, str]] = []
        indices_updated_in_place: list[str] = []
        any_failure = False
        timed_out = False
        assert proc.stdout is not None

        # B4 — readline blocks indefinitely when the child stops emitting
        # output. The watchdog terminates the child once the deadline expires
        # so stdout closes and the for-loop exits on EOF.
        watchdog_fired = _start_deadline_watchdog(proc, deadline)

        for raw_line in proc.stdout:
            # Per-line deadline check is still useful for the common case
            # where the child IS emitting lines but the loop has run past
            # the deadline (each iteration does work; without this break we'd
            # consume EOF without recording the timeout).
            if time.monotonic() > deadline:
                timed_out = True
                break
            line = raw_line.rstrip()
            if not line:
                continue
            log.info("[upgrade] %s", line)
            event = _parse_phase1_line(line)
            if event is None:
                continue
            if event.state == Phase1State.ALIAS_SWAPPED:
                if event.index_name is not None:
                    alias_swaps.append((event.index_name, event.next_index_name or ""))
            elif event.state == Phase1State.MAPPINGS_UPDATE_IN_PLACE:
                if event.index_name is not None:
                    indices_updated_in_place.append(event.index_name)
            elif event.state == Phase1State.REINDEX_FAILED:
                any_failure = True

        # If the watchdog tripped before any line surfaced, the child got
        # terminated and the loop exited on EOF — surface it as a timeout.
        if watchdog_fired.is_set():
            timed_out = True

        if timed_out:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # Fallback: process refused to die after kill — log and move on.
                log.error("UpgradeBlocking process did not exit after kill")
            duration = time.monotonic() - t0
            log.error("UpgradeBlocking timed out after %.1fs", duration)
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=f"system-update timed out after {self._timeout_s}s",
                details={"alias_swaps_observed": alias_swaps},
            )

        # Stdout reached EOF — process should exit imminently. Wait briefly to reap.
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
                error=f"system-update exited stdout but did not finish reaping within {self._timeout_s}s",
                details={"alias_swaps_observed": alias_swaps},
            )

        rc = proc.returncode
        duration = time.monotonic() - t0
        if rc != 0:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error=f"system-update exited rc={rc}",
                details={"alias_swaps_observed": alias_swaps},
            )
        if any_failure:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration,
                error="alias swap failure observed in log stream",
                details={"alias_swaps_observed": alias_swaps},
            )

        captured = self._capture_indices_state(alias_swaps, duration)
        captured.indices_updated_in_place = indices_updated_in_place
        ctx.upgrade_blocking = captured
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "alias_swaps_observed": alias_swaps,
                "indices": [vars(i) for i in captured.indices],
                "indices_updated_in_place": indices_updated_in_place,
                "upgrade_id": captured.upgrade_id,
                "duration_s": duration,
            },
        )

    def _read_token_env(self) -> dict[str, str]:
        return read_token_passthrough(
            self._docker, self._gms_service, purpose="upgrade_blocking"
        )

    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
        # token_env goes into compose_env too — Compose reads the compose file
        # at process-launch time and ${DATAHUB_TOKEN_SERVICE_*} substitutions in
        # sibling services emit one warning per occurrence when those vars are
        # missing from the parent process env (yielding ~80 cosmetic warnings
        # per upgrade phase). Forwarding via container ``-e`` alone doesn't help
        # — those go to the container, not to Compose itself.
        # G20c — point the system-update container's dev host mounts at the
        # NEW worktree's build outputs so it runs NEW PDL + NEW upgrade jar.
        # Without this, the dev mount under ../../datahub-upgrade/build/libs/
        # silently overrides the NEW image's bundled artifacts with whatever
        # the dev's working tree last built — collapsing the two-image
        # distinction. None when the NEW worktree isn't materialized; the
        # YAML defaults (dev tree mount) then apply, which is still the right
        # behavior for skip-build flows.
        mount_env = worktree_mount_env(
            REPO_ROOT,
            self._build_images_root,
            "new",
        )
        if mount_env:
            log.info(
                "[upgrade-blocking] pinning host mounts to NEW worktree: %s",
                sorted(mount_env.keys()),
            )
        compose_env = {
            "DATAHUB_UPDATE_VERSION": self._new_image_tag,
            "DATAHUB_VERSION": self._new_image_tag,
            **(mount_env or {}),
            **token_env,
        }
        # G20a — the JVM inside the one-shot system-update-debug container
        # reads these from process env (Spring application.yaml resolves
        # ${ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED} at boot).
        # The system-update service has NO env_file directive in the compose
        # YAML, so zdu-test.env doesn't reach it — must be passed via -e flags.
        # Selects BuildIndicesIncrementalStep (which persists state) over the
        # legacy Pre/Step/Post path (which doesn't).
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

    def _capture_indices_state(
        self, alias_swaps: list[tuple[str, str]], duration: float
    ) -> UpgradeBlockingResult:
        """Locate the ``BuildIndicesIncremental_*`` upgrade result and unflatten
        its per-alias state.

        Real-stack BuildIndicesIncrementalStep persists per-alias state as flat
        ``<alias>.<key>`` keys under the top-level ``result`` block, not as a
        nested ``indicesState`` dict — so the legacy
        ``find_upgrade_result_with_field("indicesState")`` lookup always
        returns None. The new ``find_upgrade_result_by_urn_prefix`` matches
        on URN, then ``_parse_flat_indices_state`` unflattens.
        """
        upgrade_id, parsed = self._mysql.find_upgrade_result_by_urn_prefix(
            "BuildIndicesIncremental_"
        )
        raw_per_alias: dict[str, dict] = {}
        indices: list[IndexState] = []
        if parsed and isinstance(parsed.get("result"), dict):
            raw_per_alias = _parse_flat_indices_state(parsed["result"])
            indices = [
                IndexState(
                    alias=alias,
                    next_index_name=entry.get("nextIndexName"),
                    # oldBackingIndexName is not emitted by BuildIndicesIncrementalStep
                    # — confirmed via real-stack capture inspection (Plan 15).
                    old_backing_index_name=None,
                    reindex_start_time=entry.get("reindexStartTime"),
                    source_doc_count=int(entry.get("sourceDocCount", 0) or 0),
                    task_id=entry.get("taskId"),
                    requires_data_backfill=bool(
                        entry.get("requiresDataBackfill", False)
                    ),
                    status=entry.get("status", "UNKNOWN"),
                )
                for alias, entry in raw_per_alias.items()
            ]
        return UpgradeBlockingResult(
            indices=indices,
            alias_swaps_observed=alias_swaps,
            raw=raw_per_alias,
            duration_s=duration,
            upgrade_id=upgrade_id,
        )
