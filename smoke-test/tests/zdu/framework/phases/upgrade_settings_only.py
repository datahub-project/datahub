"""Phase — UpgradeSettingsOnlyPhase.

Runs ``system-update -u SystemUpdateBlocking`` a third time with only the
entity indices' ``refresh_interval`` changed. Search V3 applies a dynamic
setting in place: ``BuildIndicesIncrementalStep`` routes the index through
``ESIndexBuilder.buildIndex``, which updates the settings and logs
``Updated index <alias> settings. desired refresh_interval=...``, with no
reindex. Used by TC-103.

A PDL change cannot exercise V3's in-place path: every new searchable field is
mirrored under the existing ``_aspects`` object, so ``ReindexConfig`` sees a
modified field rather than a pure addition and rebuilds every index.

Runs just before ``validation`` so the new setting cannot affect the earlier
phases. Only fails on a non-zero exit or a timeout; the TC-103 validator
judges the captured data.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from .upgrade_blocking import _start_deadline_watchdog
from ..constants import REPO_ROOT, ZDU_SETTINGS_ONLY_REVISION
from ..context import SettingsOnlyUpgradeResult, TestContext
from ..docker_compose import DockerComposeClient
from ..es_client import ElasticsearchClient
from ..host_mounts import worktree_mount_env

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 600
# The entity default is 3s; any other value gives every index a settings diff.
_REFRESH_INTERVAL_SECONDS = 7

_SETTINGS_UPDATED = re.compile(
    r"Updated index (\S+) settings\. desired refresh_interval=(\S+)"
)
_REINDEX_SUBMITTED = re.compile(r"submitted _reindex task \S+ from (\S+) to")


def parse_settings_only_line(line: str, result: SettingsOnlyUpgradeResult) -> None:
    """Record a settings update or a reindex task from one upgrade log line."""
    if match := _SETTINGS_UPDATED.search(line):
        result.settings_updated[match.group(1)] = match.group(2)
    elif match := _REINDEX_SUBMITTED.search(line):
        result.reindex_tasks.append(match.group(1))


class UpgradeSettingsOnlyPhase(Phase):
    name = "upgrade_settings_only"

    def __init__(
        self,
        docker: DockerComposeClient,
        es: ElasticsearchClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._es = es
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._timeout_s = timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        if ctx.upgrade_blocking is None:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={"reason": "upgrade_blocking did not run"},
            )

        t0 = time.monotonic()
        deadline = t0 + self._timeout_s
        result = SettingsOnlyUpgradeResult(
            desired_refresh_interval=f"{_REFRESH_INTERVAL_SECONDS}s"
        )
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="upgrade_settings_only"
        )
        proc = self._launch(token_env)
        assert proc.stdout is not None
        watchdog_fired = _start_deadline_watchdog(proc, deadline)
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if line:
                log.info("[upgrade-settings] %s", line)
                parse_settings_only_line(line, result)

        timed_out = watchdog_fired.is_set()
        try:
            proc.wait(timeout=max(deadline - time.monotonic(), 1.0))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            timed_out = True
        result.exit_code = proc.returncode
        result.duration_s = time.monotonic() - t0

        for state in ctx.upgrade_blocking.indices:
            try:
                value = self._es.get_refresh_interval(state.alias)
            except Exception as exc:
                log.warning(
                    "[upgrade-settings] settings read failed for %s: %s",
                    state.alias,
                    exc,
                )
                continue
            if value is not None:
                result.es_refresh_intervals[state.alias] = value
        ctx.upgrade_settings_only = result

        details = {
            "desired_refresh_interval": result.desired_refresh_interval,
            "settings_updated": len(result.settings_updated),
            "reindex_tasks": result.reindex_tasks,
            "exit_code": result.exit_code,
        }
        if timed_out or result.exit_code != 0:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=result.duration_s,
                error=(
                    f"settings-only system-update timed out after {self._timeout_s}s"
                    if timed_out
                    else f"settings-only system-update exited rc={result.exit_code}"
                ),
                details=details,
            )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=result.duration_s,
            details=details,
        )

    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
        # Same wiring as UpgradeBlockingReRunPhase plus the setting change, so
        # the target mapping matches what is live and only settings differ.
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
            "DATAHUB_REVISION": ZDU_SETTINGS_ONLY_REVISION,
            "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX": "true",
            "ELASTICSEARCH_INDEX_REFRESH_INTERVAL_ENTITY_SECONDS": str(
                _REFRESH_INTERVAL_SECONDS
            ),
        }
        return self._docker.run_upgrade_job(
            env_overrides=env_overrides,
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateBlocking"],
            compose_env=compose_env,
        )
