"""
Shared seeder infrastructure for DataHub performance-test datapacks.

All domain seeders (seed_search.py, seed_lineage.py, …) import from here
to avoid duplicating the threading / stats / progress-reporting boilerplate.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Iterator

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter


# ── Thread-safe counters ───────────────────────────────────────────────────────

@dataclass
class _Stats:
    emitted: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, emitted: int = 0, errors: int = 0) -> None:
        with self._lock:
            self.emitted += emitted
            self.errors += errors


# ── Worker thread ──────────────────────────────────────────────────────────────

def _worker(
    worker_id: int,
    q: "queue.Queue[list[MetadataChangeProposalWrapper] | None]",
    host: str,
    token: str | None,
    stats: _Stats,
) -> None:
    emitter = DatahubRestEmitter(gms_server=host, token=token)
    while True:
        batch = q.get()
        if batch is None:  # poison pill
            q.task_done()
            break
        ok = err = 0
        for mcp in batch:
            try:
                emitter.emit(mcp)
                ok += 1
            except Exception:
                err += 1
        stats.add(emitted=ok, errors=err)
        q.task_done()


# ── Progress reporter ──────────────────────────────────────────────────────────

def _progress_reporter(
    stats: _Stats,
    total_mcps: int,
    stop_event: threading.Event,
) -> None:
    start = time.monotonic()
    prev_emitted = 0
    prev_time = start
    while not stop_event.is_set():
        time.sleep(10)
        now = time.monotonic()
        emitted = stats.emitted
        elapsed = now - start
        delta_e = emitted - prev_emitted
        delta_t = now - prev_time
        rps = delta_e / delta_t if delta_t > 0 else 0
        pct = emitted / total_mcps * 100 if total_mcps else 0
        eta_s = (total_mcps - emitted) / rps if rps > 0 else float("inf")
        eta_str = f"{eta_s / 60:.1f} min" if eta_s < 1e8 else "?"
        print(
            f"  [{elapsed:6.0f}s] {emitted:>10,} / {total_mcps:,} MCPs "
            f"({pct:5.1f}%)  {rps:>6.0f} MCP/s  ETA {eta_str}  errors={stats.errors}",
            flush=True,
        )
        prev_emitted = emitted
        prev_time = now


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_seed(
    host: str,
    token: str | None,
    workers: int,
    batch_size: int,
    mcp_iter: Iterator[MetadataChangeProposalWrapper],
    total_mcps: int,
) -> _Stats:
    """
    Drain *mcp_iter* into GMS using a worker-thread pool and a bounded queue.

    Returns a _Stats instance with final emitted/errors counts.
    Handles KeyboardInterrupt gracefully (flushes in-flight work then stops).
    """
    q: "queue.Queue[list[MetadataChangeProposalWrapper] | None]" = queue.Queue(
        maxsize=workers * 4
    )
    stats = _Stats()

    threads = [
        threading.Thread(
            target=_worker,
            args=(i, q, host, token, stats),
            daemon=True,
            name=f"worker-{i}",
        )
        for i in range(workers)
    ]
    for t in threads:
        t.start()

    stop_progress = threading.Event()
    progress_thread = threading.Thread(
        target=_progress_reporter,
        args=(stats, total_mcps, stop_progress),
        daemon=True,
    )
    progress_thread.start()

    batch: list[MetadataChangeProposalWrapper] = []
    try:
        for mcp in mcp_iter:
            batch.append(mcp)
            if len(batch) >= batch_size:
                q.put(batch)
                batch = []
        if batch:
            q.put(batch)
    except KeyboardInterrupt:
        print("\nInterrupted — flushing remaining work …")

    for _ in threads:
        q.put(None)
    for t in threads:
        t.join()

    stop_progress.set()
    progress_thread.join(timeout=2)
    return stats
