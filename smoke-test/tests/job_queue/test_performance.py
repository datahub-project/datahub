"""Performance tests for the job queue SDK.

This is a **reporting tool, not a CI gate** — no performance assertions,
just structured output.  Run on-demand (e.g. before a release) with:

    scripts/dev/datahub-dev.sh test tests/job_queue/test_performance.py

HTTP call model (drives what we're measuring):
- try_claim success (with hint)  = 2 HTTP calls (write + read_new_version)
- try_claim success (no hint)    = 3 HTTP calls (read_versioned + write + read_new_version)
- try_claim already-claimed      = 1 HTTP call (read_versioned, bail) or 0 (hint, bail)
- release success                = 1 HTTP call (write with cached data)
"""

import logging
import math
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
)
from datahub.sdk.patterns.job_queue import Claim

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class LatencyStats:
    """Computed from a list of elapsed-second samples."""

    count: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    stddev_ms: float

    @classmethod
    def from_samples(cls, samples_sec: List[float]) -> "LatencyStats":
        if not samples_sec:
            return cls(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        ms = [s * 1000 for s in samples_sec]
        ms_sorted = sorted(ms)
        n = len(ms_sorted)

        def _percentile(pct: float) -> float:
            idx = (pct / 100) * (n - 1)
            lo = int(math.floor(idx))
            hi = min(lo + 1, n - 1)
            frac = idx - lo
            return ms_sorted[lo] + frac * (ms_sorted[hi] - ms_sorted[lo])

        return cls(
            count=n,
            mean_ms=statistics.mean(ms),
            p50_ms=_percentile(50),
            p95_ms=_percentile(95),
            p99_ms=_percentile(99),
            min_ms=ms_sorted[0],
            max_ms=ms_sorted[-1],
            stddev_ms=statistics.stdev(ms) if n > 1 else 0.0,
        )

    def log_summary(self, label: str) -> None:
        logger.info(
            "%-20s  n=%d  mean=%.1fms  p50=%.1fms  p95=%.1fms  p99=%.1fms  "
            "min=%.1fms  max=%.1fms  stddev=%.1fms",
            label,
            self.count,
            self.mean_ms,
            self.p50_ms,
            self.p95_ms,
            self.p99_ms,
            self.min_ms,
            self.max_ms,
            self.stddev_ms,
        )


@dataclass
class ContentionResult:
    worker_id: int
    won: bool
    latency_sec: float


def _create_execution_request(graph, request_id: str) -> str:
    urn = f"urn:li:dataHubExecutionRequest:{request_id}"
    aspect = ExecutionRequestInputClass(
        task="RUN_INGEST",
        args={
            "recipe": '{"source": {"type": "demo-data", "config": {}}}',
            "version": "1.0.0",
        },
        executorId="perf-test",
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="MANUAL_INGESTION_SOURCE",
            ingestionSource="urn:li:dataHubIngestionSource:perf-test",
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=request_id),
        entityUrn=urn,
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=aspect,
        changeType="UPSERT",
    )
    graph.emit_mcp(mcpw)
    return urn


def _delete_execution_request(graph, urn: str) -> None:
    for aspect_name in (
        "dataHubExecutionRequestResult",
        "dataHubExecutionRequestInput",
    ):
        try:
            graph.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspectName=aspect_name,
                    changeType=ChangeTypeClass.DELETE,
                )
            )
        except Exception:
            pass
    try:
        graph.hard_delete_entity(urn)
    except Exception:
        pass


def create_test_entities(graph, count: int) -> List[str]:
    """Batch-create ExecutionRequest entities, return URNs."""
    urns = []
    for _ in range(count):
        request_id = str(uuid.uuid4())
        urns.append(_create_execution_request(graph, request_id))
    logger.info("Created %d test entities", count)
    return urns


def cleanup_entities(graph, urns: List[str]) -> None:
    """Delete entities, swallowing per-entity errors."""
    for urn in urns:
        _delete_execution_request(graph, urn)
    logger.info("Cleaned up %d entities", len(urns))


def _make_claim(graph, jitter_max_ms: float = 0.0) -> Claim:
    return Claim(
        graph=graph,
        aspect_name="dataHubExecutionRequestResult",
        aspect_class=ExecutionRequestResultClass,
        make_claimed=lambda owner, current: ExecutionRequestResultClass(
            status="RUNNING",
            startTimeMs=int(time.time() * 1000),
            executorInstanceId=owner,
        ),
        make_released=lambda owner, current: ExecutionRequestResultClass(
            status="SUCCESS",
            startTimeMs=int(time.time() * 1000),
            executorInstanceId=owner,
        ),
        is_claimed=lambda aspect: aspect.status == "RUNNING",  # type: ignore[attr-defined]
        jitter_max_ms=jitter_max_ms,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

PERF_MARKER = pytest.mark.performance


@PERF_MARKER
@pytest.mark.remote_executor
def test_single_operation_latencies(
    auth_session,
    openapi_graph_client,  # noqa: ARG001
):
    """Baseline latencies with zero contention."""
    graph = openapi_graph_client
    iterations = 20
    urns = create_test_entities(graph, iterations)
    try:
        claim_times: List[float] = []
        release_times: List[float] = []
        round_trip_times: List[float] = []

        for urn in urns:
            claim = _make_claim(graph)

            t0 = time.perf_counter()
            claimed = claim.try_claim(urn, "perf-worker")
            t1 = time.perf_counter()

            if not claimed:
                logger.warning("Claim failed for %s, skipping", urn)
                continue

            released = claim.release(urn, "perf-worker")
            t2 = time.perf_counter()

            if not released:
                logger.warning("Release failed for %s", urn)

            claim_times.append(t1 - t0)
            release_times.append(t2 - t1)
            round_trip_times.append(t2 - t0)

        logger.info("=== Single Operation Latencies (n=%d) ===", len(claim_times))
        LatencyStats.from_samples(claim_times).log_summary("try_claim")
        LatencyStats.from_samples(release_times).log_summary("release")
        LatencyStats.from_samples(round_trip_times).log_summary("round_trip")
    finally:
        cleanup_entities(graph, urns)


@PERF_MARKER
@pytest.mark.remote_executor
def test_contention_scaling(auth_session, openapi_graph_client):  # noqa: ARG001
    """N workers race to claim one entity, measure winner/loser latencies."""
    graph = openapi_graph_client
    worker_counts = [2, 5, 10]

    for num_workers in worker_counts:
        urns = create_test_entities(graph, 1)
        urn = urns[0]
        try:
            claims = [_make_claim(graph) for _ in range(num_workers)]
            results: List[ContentionResult] = []

            wall_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {}
                for i in range(num_workers):

                    def _worker(idx=i, _claims=claims, _urn=urn):
                        t0 = time.perf_counter()
                        won = _claims[idx].try_claim(_urn, f"worker-{idx}")
                        elapsed = time.perf_counter() - t0
                        return ContentionResult(idx, won, elapsed)

                    futures[executor.submit(_worker)] = i

                for future in as_completed(futures):
                    results.append(future.result())
            wall_elapsed = time.perf_counter() - wall_start

            winners = [r for r in results if r.won]
            losers = [r for r in results if not r.won]

            winner_latency: Optional[float] = None
            if winners:
                winner_latency = winners[0].latency_sec * 1000

            loser_stats = LatencyStats.from_samples([r.latency_sec for r in losers])

            logger.info("=== Contention Scaling (workers=%d) ===", num_workers)
            logger.info(
                "  winners=%d  winner_latency=%.1fms  wall_clock=%.1fms",
                len(winners),
                winner_latency if winner_latency is not None else 0.0,
                wall_elapsed * 1000,
            )
            loser_stats.log_summary("  loser_latency")

            # Release winner so cleanup can proceed
            for r in winners:
                claims[r.worker_id].release(urn, f"worker-{r.worker_id}")
        finally:
            cleanup_entities(graph, urns)


@PERF_MARKER
@pytest.mark.remote_executor
def test_sequential_throughput(auth_session, openapi_graph_client):  # noqa: ARG001
    """Single worker processes a batch serially."""
    graph = openapi_graph_client
    batch_sizes = [10, 25, 50]

    for batch_size in batch_sizes:
        urns = create_test_entities(graph, batch_size)
        try:
            claim_latencies: List[float] = []
            release_latencies: List[float] = []
            claim_successes = 0
            release_successes = 0

            total_start = time.perf_counter()

            for urn in urns:
                claim = _make_claim(graph)
                t0 = time.perf_counter()
                if claim.try_claim(urn, "perf-worker"):
                    claim_latencies.append(time.perf_counter() - t0)
                    claim_successes += 1

                    t1 = time.perf_counter()
                    if claim.release(urn, "perf-worker"):
                        release_latencies.append(time.perf_counter() - t1)
                        release_successes += 1
                    else:
                        release_latencies.append(time.perf_counter() - t1)
                else:
                    claim_latencies.append(time.perf_counter() - t0)
            total_elapsed = time.perf_counter() - total_start
            claim_elapsed = sum(claim_latencies)
            release_elapsed = sum(release_latencies)

            total_ops = claim_successes + release_successes
            claims_per_sec = claim_successes / claim_elapsed if claim_elapsed > 0 else 0
            releases_per_sec = (
                release_successes / release_elapsed if release_elapsed > 0 else 0
            )
            total_ops_per_sec = total_ops / total_elapsed if total_elapsed > 0 else 0

            logger.info("=== Sequential Throughput (batch=%d) ===", batch_size)
            logger.info(
                "  claims: %d/%d  releases: %d/%d",
                claim_successes,
                batch_size,
                release_successes,
                claim_successes,
            )
            logger.info(
                "  claims/sec=%.1f  releases/sec=%.1f  total_ops/sec=%.1f  "
                "wall_clock=%.1fs",
                claims_per_sec,
                releases_per_sec,
                total_ops_per_sec,
                total_elapsed,
            )
            LatencyStats.from_samples(claim_latencies).log_summary("  claim_latency")
            LatencyStats.from_samples(release_latencies).log_summary(
                "  release_latency"
            )
        finally:
            cleanup_entities(graph, urns)


@PERF_MARKER
@pytest.mark.remote_executor
def test_concurrent_throughput(auth_session, openapi_graph_client):  # noqa: ARG001
    """K workers process independent (non-overlapping) entities in parallel."""
    graph = openapi_graph_client
    total_entities = 20
    worker_counts = [2, 5, 10]

    for num_workers in worker_counts:
        urns = create_test_entities(graph, total_entities)
        try:
            # Partition entities among workers
            partitions: List[List[str]] = [[] for _ in range(num_workers)]
            for i, urn in enumerate(urns):
                partitions[i % num_workers].append(urn)

            worker_results: List[dict] = []

            def _worker_fn(worker_id: int, worker_urns: List[str]) -> dict:
                claim = _make_claim(graph)
                successes = 0
                failures = 0
                latencies: List[float] = []
                for urn in worker_urns:
                    t0 = time.perf_counter()
                    if claim.try_claim(urn, f"worker-{worker_id}"):
                        latencies.append(time.perf_counter() - t0)
                        successes += 1
                        claim.release(urn, f"worker-{worker_id}")
                    else:
                        latencies.append(time.perf_counter() - t0)
                        failures += 1
                return {
                    "worker_id": worker_id,
                    "successes": successes,
                    "failures": failures,
                    "latencies": latencies,
                }

            wall_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {
                    executor.submit(_worker_fn, i, partitions[i]): i
                    for i in range(num_workers)
                }
                for future in as_completed(futures):
                    worker_results.append(future.result())
            wall_elapsed = time.perf_counter() - wall_start

            total_successes = sum(r["successes"] for r in worker_results)
            total_failures = sum(r["failures"] for r in worker_results)
            all_latencies = [lat for r in worker_results for lat in r["latencies"]]
            contention_rate = (
                total_failures / (total_successes + total_failures)
                if (total_successes + total_failures) > 0
                else 0.0
            )

            total_claims_per_sec = (
                total_successes / wall_elapsed if wall_elapsed > 0 else 0
            )
            per_worker = total_claims_per_sec / num_workers if num_workers > 0 else 0

            logger.info(
                "=== Concurrent Throughput (workers=%d, entities=%d) ===",
                num_workers,
                total_entities,
            )
            logger.info(
                "  successes=%d  failures=%d  contention_rate=%.2f",
                total_successes,
                total_failures,
                contention_rate,
            )
            logger.info(
                "  total_claims/sec=%.1f  per_worker_claims/sec=%.1f  wall_clock=%.1fs",
                total_claims_per_sec,
                per_worker,
                wall_elapsed,
            )
            LatencyStats.from_samples(all_latencies).log_summary("  claim_latency")
        finally:
            cleanup_entities(graph, urns)


@PERF_MARKER
@pytest.mark.remote_executor
def test_cas_conflict_rate(auth_session, openapi_graph_client):  # noqa: ARG001
    """Stress the 412 conflict path with more workers than entities."""
    graph = openapi_graph_client
    pool_size = 5
    num_workers = 10
    rounds = 3

    for round_num in range(rounds):
        urns = create_test_entities(graph, pool_size)
        try:
            claims = [_make_claim(graph) for _ in range(num_workers)]
            results: List[ContentionResult] = []

            wall_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = []
                for worker_id in range(num_workers):
                    for urn in urns:

                        def _attempt(wid=worker_id, u=urn, _claims=claims):
                            t0 = time.perf_counter()
                            won = _claims[wid].try_claim(u, f"worker-{wid}")
                            elapsed = time.perf_counter() - t0
                            return ContentionResult(wid, won, elapsed)

                        futures.append(executor.submit(_attempt))

                for future in as_completed(futures):
                    results.append(future.result())
            wall_elapsed = time.perf_counter() - wall_start

            winners = [r for r in results if r.won]
            losers = [r for r in results if not r.won]
            total_attempts = len(results)
            success_rate = len(winners) / total_attempts if total_attempts > 0 else 0.0
            conflict_rate = len(losers) / total_attempts if total_attempts > 0 else 0.0

            logger.info(
                "=== CAS Conflict Rate (round=%d, pool=%d, workers=%d) ===",
                round_num + 1,
                pool_size,
                num_workers,
            )
            logger.info(
                "  attempts=%d  successes=%d  conflicts=%d",
                total_attempts,
                len(winners),
                len(losers),
            )
            logger.info(
                "  success_rate=%.2f  conflict_rate=%.2f  wall_clock=%.1fs",
                success_rate,
                conflict_rate,
                wall_elapsed,
            )
            LatencyStats.from_samples([r.latency_sec for r in winners]).log_summary(
                "  winner_latency"
            )
            LatencyStats.from_samples([r.latency_sec for r in losers]).log_summary(
                "  loser_latency"
            )

            # Release all held URNs across all claim instances
            for i, claim in enumerate(claims):
                for held_urn in list(claim.held_urns):
                    try:
                        claim.release(held_urn, f"worker-{i}")
                    except Exception:
                        pass
        finally:
            cleanup_entities(graph, urns)


@dataclass
class _JitterRunResult:
    """Results from a single jitter contention run."""

    label: str
    wall_clock_ms: float
    winner_latency_ms: float
    loser_mean_ms: float
    num_winners: int
    num_losers: int
    http_calls: int
    effective_http_per_sec: float


def _run_contention_round(
    graph, urn: str, num_workers: int, jitter_max_ms: float, label: str
) -> _JitterRunResult:
    """Run a single contention round and return structured results."""
    claims = [
        _make_claim(graph, jitter_max_ms=jitter_max_ms) for _ in range(num_workers)
    ]
    results: List[ContentionResult] = []

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for i in range(num_workers):

            def _worker(idx=i):
                t0 = time.perf_counter()
                won = claims[idx].try_claim(urn, f"worker-{idx}")
                elapsed = time.perf_counter() - t0
                return ContentionResult(idx, won, elapsed)

            futures[executor.submit(_worker)] = i

        for future in as_completed(futures):
            results.append(future.result())
    wall_elapsed = time.perf_counter() - wall_start

    winners = [r for r in results if r.won]
    losers = [r for r in results if not r.won]

    winner_latency_ms = winners[0].latency_sec * 1000 if winners else 0.0
    loser_mean_ms = (
        statistics.mean([r.latency_sec for r in losers]) * 1000 if losers else 0.0
    )

    # HTTP calls: winner=3 (read_versioned + write + read_new_version),
    # loser=1 (read_versioned, bail early)
    # (smoke tests use separate discovery/claim aspects, so no-hint path)
    http_calls = len(winners) * 3 + len(losers) * 1
    effective_http_per_sec = http_calls / wall_elapsed if wall_elapsed > 0 else 0.0

    # Release winners
    for r in winners:
        claims[r.worker_id].release(urn, f"worker-{r.worker_id}")

    return _JitterRunResult(
        label=label,
        wall_clock_ms=wall_elapsed * 1000,
        winner_latency_ms=winner_latency_ms,
        loser_mean_ms=loser_mean_ms,
        num_winners=len(winners),
        num_losers=len(losers),
        http_calls=http_calls,
        effective_http_per_sec=effective_http_per_sec,
    )


@PERF_MARKER
@pytest.mark.remote_executor
def test_jittered_backoff_contention(
    auth_session,
    openapi_graph_client,  # noqa: ARG001
):
    """Compare thundering-herd (no jitter) vs jittered backoff for claim contention."""
    graph = openapi_graph_client
    num_workers = 100

    # --- Baseline: no jitter (thundering herd) ---
    urns = create_test_entities(graph, 1)
    try:
        baseline = _run_contention_round(
            graph, urns[0], num_workers, jitter_max_ms=0.0, label="baseline"
        )
    finally:
        cleanup_entities(graph, urns)

    # --- Jittered: spread over 500ms ---
    urns = create_test_entities(graph, 1)
    try:
        jittered_500 = _run_contention_round(
            graph, urns[0], num_workers, jitter_max_ms=500.0, label="jitter-500"
        )
    finally:
        cleanup_entities(graph, urns)

    # --- Wide jitter: spread over 3000ms ---
    urns = create_test_entities(graph, 1)
    try:
        jittered_3000 = _run_contention_round(
            graph, urns[0], num_workers, jitter_max_ms=3000.0, label="jitter-3000"
        )
    finally:
        cleanup_entities(graph, urns)

    # --- Log side-by-side comparison ---
    logger.info("=== Jittered Backoff Contention (workers=%d) ===", num_workers)
    fmt = (
        "  %-14s  wall=%.0fms  winner=%.1fms  loser_mean=%.1fms  "
        "http_calls=%d  effective_http/s=%.0f"
    )
    for run in (baseline, jittered_500, jittered_3000):
        logger.info(
            fmt,
            run.label + ":",
            run.wall_clock_ms,
            run.winner_latency_ms,
            run.loser_mean_ms,
            run.http_calls,
            run.effective_http_per_sec,
        )
