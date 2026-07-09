"""Phase 8 — UpgradeNonBlockingPhase.

Runs ``system-update -u SystemUpdateNonBlocking`` against the live NEW GMS
while concurrent reader/writer threads continue. Captures:

- ``DataHubUpgradeResult.indicesState`` (post-sweep alias / dual-write state).
- Per-index ``DUAL_WRITE_DISABLED`` markings observed in upgrade-job logs.
- ``IncrementalReindexCatchUpStep`` ``[T0, T1]`` windows per index.
- The legacy concurrent-IO harness data (``ctx.io_observations``,
  ``ctx.io_write_results``, ``ctx.sweep_total_migrated``).

Replaces the legacy ``SweepAndIOPhase`` placeholder. The runner's pipeline
tuple key changes from ``"sweep_and_io"`` to ``"upgrade_nonblocking"``.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Empty, Queue

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from .upgrade_blocking import parse_indices_state
from ..config import ZDUTestConfig
from ..constants import REPO_ROOT
from ..context import (
    IOObservation,
    IOWriteResult,
    SeededEntity,
    TestContext,
    UpgradeNonBlockingResult,
)
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env
from ..log_monitor import (
    LogMonitor,
    NonBlockingEvent,
    NonBlockingState,
    SweepEvent,
    SweepState,
)
from ..mysql_client import MySQLClient

# _URN_SYNC_ENABLED was a module-level toggle (set False to fall back to timed
# writers); promoted to ZDUTestConfig.urn_sync_enabled / ZDU_URN_SYNC_ENABLED
# env var so the choice is config-driven and discoverable, not source-edit-only.

log = logging.getLogger(__name__)

_SWEEP_START_TIMEOUT = 120  # seconds to wait for STARTED event
_BATCH_POLL_INTERVAL = 1  # seconds between queue drain cycles


class ConcurrentIOHarness:
    def __init__(
        self,
        datahub: DataHubClient,
        seeded_entities: list[SeededEntity],
        io_pool_entities: list[SeededEntity],
        reader_workers: int,
        writer_workers: int,
        stop_event: threading.Event,
        observations: list[IOObservation],
        write_results: list[IOWriteResult],
        sweep_queue: Queue,
        urn_sync_enabled: bool = True,
    ) -> None:
        self._datahub = datahub
        self._seeded = seeded_entities
        # Readers observe all seeded entities; writers only target the dedicated pool
        # so concurrent writes never corrupt scenario-specific test entities.
        self._io_pool = io_pool_entities
        self._io_pool_by_urn: dict[str, SeededEntity] = {
            e.urn: e for e in io_pool_entities
        }
        self._urn_sync_enabled = urn_sync_enabled
        self._reader_workers = reader_workers
        self._writer_workers = writer_workers
        self._stop = stop_event
        self._observations = observations
        self._write_results = write_results
        self._sweep_queue = sweep_queue
        self._pool: ThreadPoolExecutor | None = None

    def start(self) -> None:
        total = self._reader_workers + self._writer_workers
        if total == 0:
            return
        self._pool = ThreadPoolExecutor(max_workers=total, thread_name_prefix="zdu-io")
        for i in range(self._reader_workers):
            self._pool.submit(self._reader_loop, i)
        for i in range(self._writer_workers):
            self._pool.submit(self._writer_loop, i)

    def stop(self) -> None:
        self._stop.set()
        if self._pool:
            self._pool.shutdown(wait=True, cancel_futures=False)

    def _reader_loop(self, worker_id: int) -> None:
        while not self._stop.is_set():
            for entity in list(self._seeded):
                if self._stop.is_set():
                    return
                try:
                    resp = self._datahub.get_aspect(entity.urn, entity.aspect_name)
                    self._observations.append(
                        IOObservation(
                            worker=f"reader-{worker_id}",
                            urn=entity.urn,
                            aspect_name=entity.aspect_name,
                            observed_version=resp.schema_version,
                            expected_version=entity.expected_schema_version,
                            timestamp=datetime.utcnow(),
                        )
                    )
                except Exception as exc:
                    log.debug("reader-%d error on %s: %s", worker_id, entity.urn, exc)
            time.sleep(0.5)

    def _write_entity(self, worker_id: int, entity: SeededEntity) -> None:
        """Re-ingest old-version payload; verify write-path mutator chain upgrades it."""
        try:
            self._datahub.ingest_mcp(
                entity.urn,
                entity.aspect_name,
                entity.seeded_data,
                system_metadata={},
            )
            verify = self._datahub.get_aspect(entity.urn, entity.aspect_name)
            passed = verify.schema_version == entity.expected_schema_version
            self._write_results.append(
                IOWriteResult(
                    worker=f"writer-{worker_id}",
                    urn=entity.urn,
                    observed_version=verify.schema_version,
                    expected_version=entity.expected_schema_version,
                    passed=passed,
                    timestamp=datetime.utcnow(),
                )
            )
            if not passed:
                log.warning(
                    "writer-%d: %s at schemaVersion=%d, expected %d",
                    worker_id,
                    entity.urn,
                    verify.schema_version,
                    entity.expected_schema_version,
                )
        except Exception as exc:
            log.debug("writer-%d error on %s: %s", worker_id, entity.urn, exc)
            self._write_results.append(
                IOWriteResult(
                    worker=f"writer-{worker_id}",
                    urn=entity.urn,
                    observed_version=0,
                    expected_version=entity.expected_schema_version,
                    passed=False,
                    timestamp=datetime.utcnow(),
                    error=str(exc),
                )
            )

    def _writer_loop(self, worker_id: int) -> None:
        if not self._io_pool:
            return

        if self._urn_sync_enabled:
            # Sweep-synchronized mode: block on BATCH_URNS events from the sweep log.
            # The sweep logs URNs BEFORE calling ingestProposal, then sleeps
            # PRE_WRITE_DELAY_MS. This guarantees our write lands inside that window,
            # forcing ConditionalWriteValidator to reject the sweep's write.
            while not self._stop.is_set():
                try:
                    event = self._sweep_queue.get(timeout=0.5)
                except Empty:
                    continue
                if event.state != SweepState.BATCH_URNS or not event.batch_urns:
                    continue
                for urn in event.batch_urns:
                    if self._stop.is_set():
                        return
                    entity = self._io_pool_by_urn.get(urn)
                    if entity:
                        log.debug("writer-%d: synchronized write to %s", worker_id, urn)
                        self._write_entity(worker_id, entity)
        else:
            # Fallback: timed writes cycling through the IO pool (probabilistic overlap).
            entities = list(self._io_pool)
            idx = worker_id % len(entities)
            while not self._stop.is_set():
                self._write_entity(worker_id, entities[idx % len(entities)])
                idx += 1
                time.sleep(1.0)


class UpgradeNonBlockingPhase(Phase):
    name = "upgrade_nonblocking"

    def __init__(
        self,
        config: ZDUTestConfig,
        docker: DockerComposeClient,
        datahub: DataHubClient,
        mysql: MySQLClient,
    ) -> None:
        self._config = config
        self._docker = docker
        self._datahub = datahub
        self._mysql = mysql

    def run(self, ctx: TestContext) -> PhaseResult:
        t0 = time.monotonic()
        stop_event = threading.Event()
        # sweep_queue: all log events → consumed by _run_sweep control loop
        # writer_queue: only BATCH_URNS events → forwarded by _run_sweep, consumed by writers
        # nonblocking_queue: NonBlockingEvent (DUAL_WRITE_DISABLED, CATCH_UP_WINDOW)
        sweep_queue: Queue[SweepEvent] = Queue()
        writer_queue: Queue[SweepEvent] = Queue()
        nonblocking_queue: Queue[NonBlockingEvent] = Queue()
        ctx.sweep_events = sweep_queue
        self._nonblocking_queue = nonblocking_queue

        monitor = LogMonitor(
            self._docker,
            sweep_queue,
            since=datetime.utcnow(),
            gms_service=self._config.gms_service,
            nonblocking_queue=nonblocking_queue,
        )
        monitor.start()

        harness = ConcurrentIOHarness(
            datahub=self._datahub,
            seeded_entities=ctx.seeded_entities,
            io_pool_entities=ctx.io_pool_entities,
            reader_workers=self._config.reader_workers,
            writer_workers=self._config.writer_workers,
            stop_event=stop_event,
            observations=ctx.io_observations,
            write_results=ctx.io_write_results,
            sweep_queue=writer_queue,
            urn_sync_enabled=self._config.urn_sync_enabled,
        )
        harness.start()

        # B2 — if _run_sweep raises, the prior code's ``finally`` left
        # ``result`` as ``None`` and let the exception propagate out of the
        # phase. The runner has no per-phase try/except, so the entire report
        # was abandoned (``report.ended_at`` never set, partial JSON on disk).
        # Convert any unexpected exception into a failed PhaseResult so the
        # pipeline cleans up and writes a complete report.
        result: PhaseResult | None = None
        try:
            try:
                result = self._run_sweep(
                    ctx, sweep_queue, writer_queue, stop_event, t0, monitor
                )
            except Exception as exc:
                log.exception("upgrade_nonblocking._run_sweep crashed")
                result = PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=datetime.utcnow(),
                    error=f"_run_sweep raised {type(exc).__name__}: {exc}",
                )
            return result
        finally:
            harness.stop()
            monitor.stop()
            if result is not None:
                result.duration_s = time.monotonic() - t0

    def _run_sweep(
        self,
        ctx: TestContext,
        queue: Queue[SweepEvent],
        writer_queue: Queue[SweepEvent],
        stop_event: threading.Event,
        t0: float,
        monitor: LogMonitor,
    ) -> PhaseResult:
        start = datetime.utcnow()

        # The upgrade container requires token-service secrets that are dynamically
        # generated at stack startup. Read them from the running GMS container so
        # the one-off `docker compose run` container can initialise Spring correctly.
        token_env = read_token_passthrough(
            self._docker, self._config.gms_service, purpose="upgrade_nonblocking"
        )

        # token_env also goes into compose_env: Compose substitutes
        # ${DATAHUB_TOKEN_SERVICE_*} in sibling services at compose-read time
        # and warns once per occurrence (~80 cosmetic warnings per run) when
        # those vars are missing from the parent-process env. -e flags only
        # populate the container env, not Compose's substitution layer.
        compose_env = dict(token_env)
        # G20c — pin the host mounts on the one-shot upgrade container to
        # the NEW worktree so the bundled jar gets overlaid with NEW build
        # outputs (not whatever the dev working tree last built). Same logic
        # as upgrade_blocking: None when the NEW worktree isn't present, in
        # which case the YAML defaults (dev tree mount) apply.
        mount_env = worktree_mount_env(
            REPO_ROOT,
            self._config.build_images_root,
            "new",
        )
        if mount_env:
            log.info(
                "[upgrade-nonblocking] pinning host mounts to NEW worktree: %s",
                sorted(mount_env.keys()),
            )
            compose_env.update(mount_env)
        # G20a — same reasoning as upgrade_blocking._launch: ZDU reindex
        # flags need to reach the JVM as -e vars (env_overrides), not just
        # compose_env, because the system-update YAML has no env_file
        # directive. Without these, the nonblocking step also branches to
        # the legacy path and never persists upgrade-state for catch-up
        # validation in suite C/D.
        proc = self._docker.run_upgrade_job(
            {
                **token_env,
                "ASPECT_MIGRATION_MUTATOR_ENABLED": "true",
                # Loads ZduTestMutatorConfiguration so the test-only mutator beans
                # wire into the chain (independent flag from production gate above).
                "ZDU_TEST_FRAMEWORK_ENABLED": "true",
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED": "true",
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_UPGRADE_VERSION": self._config.upgrade_version,
                "SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS": str(
                    self._config.pre_write_delay_ms
                ),
                "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED": "true",
                "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX": "true",
            },
            service=self._config.upgrade_service,
            extra_args=["-u", "SystemUpdateNonBlocking"],
            compose_env=compose_env,
        )
        monitor.attach_upgrade_popen(proc)
        log.info(
            "Upgrade job started (upgradeVersion=%s, preWriteDelayMs=%d)",
            self._config.upgrade_version,
            self._config.pre_write_delay_ms,
        )

        state = "waiting"
        sweep_start = time.monotonic()
        retried = False

        while True:
            elapsed = time.monotonic() - t0
            if elapsed > self._config.sweep_timeout_s:
                stop_event.set()
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    error=f"Sweep timed out after {self._config.sweep_timeout_s}s",
                )

            try:
                event = queue.get(timeout=_BATCH_POLL_INTERVAL)
            except Empty:
                continue

            if event.state == SweepState.STARTED and state == "waiting":
                log.info("Sweep STARTED (elapsed %.1fs)", time.monotonic() - t0)
                state = "sweeping"
                sweep_start = time.monotonic()

            elif event.state == SweepState.SKIPPED and not retried:
                log.warning(
                    "Sweep SKIPPED (prior SUCCEEDED result exists). "
                    "Deleting prior upgrade result and retrying…"
                )
                # B1 — kill+reap the original proc before launching the retry.
                # Without this, the first system-update process and its stdout
                # pump (LogMonitor's daemon tail) stay alive in the background
                # and can emit late events into the same queue the retry uses,
                # racing with the retry's signals. Reaping here keeps the
                # sweep state-machine single-producer.
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    log.warning(
                        "Original upgrade proc didn't exit on terminate; killing"
                    )
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        log.error(
                            "Original upgrade proc still alive after kill — leaking"
                        )
                upgrade_urn = (
                    f"urn:li:dataHubUpgrade:"
                    f"migrate-aspects-{self._config.upgrade_version}"
                )
                self._datahub.delete_entity(upgrade_urn)
                self._config.upgrade_version = f"zdu-test-{int(time.time() * 1000)}"
                retry_proc = self._docker.run_upgrade_job(
                    {
                        **token_env,
                        "ASPECT_MIGRATION_MUTATOR_ENABLED": "true",
                        # Loads ZduTestMutatorConfiguration so the test-only mutator beans
                        # wire into the chain (independent flag from production gate above).
                        "ZDU_TEST_FRAMEWORK_ENABLED": "true",
                        "SYSTEM_UPDATE_MIGRATE_ASPECTS_ENABLED": "true",
                        "SYSTEM_UPDATE_MIGRATE_ASPECTS_UPGRADE_VERSION": self._config.upgrade_version,
                        "SYSTEM_UPDATE_MIGRATE_ASPECTS_PRE_WRITE_DELAY_MS": str(
                            self._config.pre_write_delay_ms
                        ),
                        "ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED": "true",
                        "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX": "true",
                    },
                    service=self._config.upgrade_service,
                    extra_args=["-u", "SystemUpdateNonBlocking"],
                    compose_env=compose_env,
                )
                monitor.attach_upgrade_popen(retry_proc)
                retried = True

            elif event.state == SweepState.BATCH_URNS:
                # Forward to writers so they can fire synchronized concurrent writes.
                writer_queue.put(event)
                log.debug("Sweep batch URNs forwarded to writers: %s", event.batch_urns)

            elif event.state == SweepState.BATCH and state == "sweeping":
                if event.batch_count:
                    log.info("Sweep batch: migrated %d aspect(s)", event.batch_count)

            elif event.state == SweepState.COMPLETED:
                ctx.sweep_total_migrated = event.total_migrated or 0
                log.info(
                    "Sweep COMPLETED — total migrated: %d (sweep took %.1fs)",
                    ctx.sweep_total_migrated,
                    time.monotonic() - sweep_start,
                )
                stop_event.set()
                disabled, windows = self._drain_nonblocking_queue()
                ctx.upgrade_nonblocking = self._capture_upgrade_result(
                    disabled, windows, time.monotonic() - sweep_start
                )
                return PhaseResult(
                    phase_name=self.name,
                    status="passed",
                    started_at=start,
                    details={
                        "total_migrated": ctx.sweep_total_migrated,
                        "sweep_duration_s": round(time.monotonic() - sweep_start, 1),
                        "io_reads": len(ctx.io_observations),
                        "io_writes": len(ctx.io_write_results),
                        "write_failures": sum(
                            1 for r in ctx.io_write_results if not r.passed
                        ),
                        "dual_write_disabled_indices": disabled,
                        "catch_up_windows": windows,
                    },
                )

            elif event.state == SweepState.FAILED:
                stop_event.set()
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    error=f"Sweep step reported FAILED: {event.message}",
                )

    def _drain_nonblocking_queue(
        self,
    ) -> tuple[list[str], dict[str, tuple[int, int]]]:
        """Drain the nonblocking_queue without blocking. Returns (disabled, windows)."""
        disabled: list[str] = []
        windows: dict[str, tuple[int, int]] = {}
        q = getattr(self, "_nonblocking_queue", None)
        if q is None:
            return disabled, windows
        while True:
            try:
                ev = q.get_nowait()
            except Empty:
                break
            if ev.state == NonBlockingState.DUAL_WRITE_DISABLED:
                if ev.index_name not in disabled:
                    disabled.append(ev.index_name)
            elif ev.state == NonBlockingState.CATCH_UP_WINDOW and ev.window is not None:
                windows[ev.index_name] = ev.window
        return disabled, windows

    def _capture_upgrade_result(
        self,
        disabled: list[str],
        windows: dict[str, tuple[int, int]],
        duration_s: float,
    ) -> UpgradeNonBlockingResult:
        upgrade_id, raw = self._mysql.find_upgrade_result_with_field("indicesState")
        indices = parse_indices_state(raw.get("indicesState", {})) if raw else []
        return UpgradeNonBlockingResult(
            indices=indices,
            dual_write_disabled_indices=disabled,
            catch_up_windows=windows,
            raw=raw,
            duration_s=duration_s,
            upgrade_id=upgrade_id,
        )
