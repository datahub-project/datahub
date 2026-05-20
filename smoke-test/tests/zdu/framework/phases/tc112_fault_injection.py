"""Phase — TC-112 Doc Count Mismatch Fault Injection.

Runs AFTER ``runtime_migration`` and BEFORE ``validation``. Engineers a
doc-count mismatch by:

1. Creating a new ES physical index with the same mapping as a previously-
   reindexed alias, but only a fraction of the docs.
2. Patching MySQL ``BuildIndicesIncremental_<version>``'s flat-dotted state
   for that alias: ``status=IN_PROGRESS``, ``nextIndexName=<bad_target>``,
   ``sourceDocCount=<real_source_count>``, ``taskId=""``.
3. Running ``system-update -u SystemUpdateBlocking`` once more.

Production enters the resume path at ``BuildIndicesIncrementalStep.java:122-178``,
calls ``validateAndSwapAlias``, observes ``getCount(alias) != getCount(bad_target)``,
logs ``Doc count mismatch for alias swap``, and exits FAILED.

The phase captures the mismatch log lines via ``Phase1State.DOC_COUNT_MISMATCH``
and writes a ``Tc112FaultInjectionResult`` to ``ctx``. The TC-112 validator in
``Phase1ReindexExecutor`` reads this. The phase itself always returns
``status="passed"`` (the upgrade failure is the EXPECTED outcome); the validator
surfaces the real signal.

KNOWN RISK: the resume path calls ``pollReindexCompletion(taskId)`` BEFORE
``validateAndSwapAlias``. With an empty taskId, polling may error out
(e.g., "task not found") and exit FAILED via a different log path — in that
case ``mismatch_events_observed`` stays empty and the TC-112 validator FAILS
with a clear actual_result naming the wrong-path exit code.
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
from ..context import Tc112FaultInjectionResult, TestContext
from ..docker_compose import DockerComposeClient
from ..es_client import ElasticsearchClient
from ..host_mounts import worktree_mount_env
from ..log_monitor import Phase1State, _parse_phase1_line
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 600


class Tc112FaultInjectionPhase(Phase):
    name = "tc112_fault_injection"

    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        es: ElasticsearchClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        new_image_tag: str = "debug",
        build_images_root: str = "smoke-test/build/zdu-images",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._es = es
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._timeout_s = timeout_s
        self._new_image_tag = new_image_tag
        self._build_images_root = build_images_root

    def run(self, ctx: TestContext) -> PhaseResult:  # noqa: C901
        # Complexity is over the 15-branch threshold because this is the
        # same stream-parser pattern as UpgradeBlockingPhase.run. Splitting
        # costs readability.
        start = datetime.utcnow()
        if ctx.upgrade_blocking is None:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={"reason": "Phase 6 (upgrade_blocking) did not run"},
            )

        # Pick the first alias that took the real-reindex path in Phase 6.
        target_alias = next(
            (a for a, n in ctx.upgrade_blocking.alias_swaps_observed if n),
            None,
        )
        if target_alias is None:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={
                    "reason": "no real-reindex alias available to inject fault into"
                },
            )

        source_count = self._es.get_doc_count(target_alias)
        if source_count < 2:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={
                    "reason": f"source {target_alias} has {source_count} docs — "
                    f"cannot produce a meaningful mismatch"
                },
            )

        bad_target = f"{target_alias.replace('-', '_')}_tc112_bad"
        partial_count = max(1, source_count // 4)
        copied = self._es.clone_index_with_partial_docs(
            target_alias, bad_target, partial_count
        )
        log.info(
            "[tc112] staged bad target: alias=%s source_count=%d "
            "bad_target=%s bad_target_count=%d",
            target_alias,
            source_count,
            bad_target,
            copied,
        )

        upgrade_id, _ = self._mysql.find_upgrade_result_by_urn_prefix(
            "BuildIndicesIncremental_"
        )
        if upgrade_id is None:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - time.monotonic(),
                details={"reason": "No BuildIndicesIncremental_* row in MySQL"},
            )
        full_urn = f"urn:li:dataHubUpgrade:{upgrade_id}"
        patched = self._mysql.patch_upgrade_result_per_alias(
            full_urn,
            target_alias,
            status="IN_PROGRESS",
            nextIndexName=bad_target,
            sourceDocCount=str(source_count),
            taskId="",
        )
        log.info(
            "[tc112] patched %d MySQL state keys for %s on %s",
            patched,
            target_alias,
            full_urn,
        )

        t0 = time.monotonic()
        deadline = t0 + self._timeout_s
        token_env = read_token_passthrough(
            self._docker, self._gms_service, purpose="tc112_fault_injection"
        )
        proc = self._launch(token_env)

        mismatch_events: list[tuple[str, str]] = []
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
            log.info("[tc112-fault] %s", line)
            event = _parse_phase1_line(line)
            if event is None:
                continue
            if (
                event.state == Phase1State.DOC_COUNT_MISMATCH
                and event.index_name is not None
            ):
                mismatch_events.append((event.index_name, event.next_index_name or ""))

        if watchdog_fired.is_set():
            timed_out = True
        if timed_out:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log.error("[tc112] process did not exit after kill")
        else:
            try:
                proc.wait(timeout=max(deadline - time.monotonic(), 1.0))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        rc = proc.returncode if proc.returncode is not None else -1
        duration = time.monotonic() - t0

        ctx.tc112_fault_injection = Tc112FaultInjectionResult(
            target_alias=target_alias,
            bad_target_index=bad_target,
            source_doc_count=source_count,
            target_doc_count=copied,
            mismatch_events_observed=mismatch_events,
            upgrade_exit_code=rc,
            duration_s=duration,
        )

        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "target_alias": target_alias,
                "bad_target_index": bad_target,
                "source_doc_count": source_count,
                "target_doc_count": copied,
                "mismatch_events_observed": mismatch_events,
                "upgrade_exit_code": rc,
                "duration_s": duration,
            },
        )

    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
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
            "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX": "true",
        }
        return self._docker.run_upgrade_job(
            env_overrides=env_overrides,
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateBlocking"],
            compose_env=compose_env,
        )
