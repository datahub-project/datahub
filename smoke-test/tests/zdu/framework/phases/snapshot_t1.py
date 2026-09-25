"""Phase — SnapshotT1Phase.

Captures post-upgrade ES doc counts for every index that was snapshotted at
``T0`` (post-seed, pre-upgrade). Runs AFTER ``upgrade_blocking_rerun`` and
BEFORE ``inject_traffic_pre`` so the counts reflect only what the upgrade
did — no traffic-injection noise from the rolling-restart phases.

Implementation strategy: re-query each ``ctx.snapshot_t0.doc_counts`` key
via the alias name (ES resolves through to the current physical index, even
if the alias was swapped to a new versioned physical during
``upgrade_blocking``). This keeps the t0 and t1 dicts directly comparable
without any key remapping.

Used by TC-109 (doc-count preservation across upgrade).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import SnapshotT0, TestContext
from ..es_client import ElasticsearchClient

log = logging.getLogger(__name__)


class SnapshotT1Phase(Phase):
    name = "snapshot_t1"

    def __init__(self, es: ElasticsearchClient) -> None:
        self._es = es

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        if ctx.snapshot_t0 is None:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={"reason": "ctx.snapshot_t0 not populated"},
            )

        snap = SnapshotT0(epoch_ms=int(time.time() * 1000))
        for name in ctx.snapshot_t0.doc_counts:
            try:
                snap.doc_counts[name] = self._es.get_doc_count(name)
            except Exception as exc:
                log.warning("[snapshot_t1] doc count failed for %s: %s", name, exc)

        ctx.snapshot_t1 = snap
        duration = time.monotonic() - t0
        log.info(
            "Snapshot T1 captured: %d indices, T1=%d",
            len(snap.doc_counts),
            snap.epoch_ms,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "doc_counts": snap.doc_counts,
                "epoch_ms": snap.epoch_ms,
            },
        )
