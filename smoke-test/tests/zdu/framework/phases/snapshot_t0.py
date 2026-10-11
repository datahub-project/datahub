"""Phase 3 — SnapshotT0Phase.

Captures pre-upgrade state into ``TestContext.snapshot_t0`` so downstream
phases can compute deltas after the upgrade runs:

- ES alias targets (alias name -> list of physical indices) for every
  ``*index_v2*`` index discovered at runtime.
- Per-physical-index doc count.
- MySQL aspect-row count grouped by ``schemaVersion`` for each configured aspect.
- T0 epoch ms (used by Phase 4's ``[T0, T1]`` window assertions).
- Whether a prior ``DataHubUpgradeResult`` exists for the configured upgrade id.

The phase is best-effort per probe: a single failing alias / index / aspect
query is logged and skipped, but a connection-level failure on the initial
``list_indices`` call short-circuits the phase to ``failed``.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import SnapshotT0, TestContext
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

# Discovery prefix for DataHub physical indices. We list once
# and probe each result's alias mapping individually.
_INDEX_PREFIX = ""

# Substring used to identify DataHub physical indices.
_INDEX_SUFFIX = "index_v2"


class SnapshotT0Phase(Phase):
    name = "snapshot_t0"

    def __init__(
        self,
        es: ElasticsearchClient,
        mysql: MySQLClient,
        aspects_under_test: list[str],
        upgrade_id_to_check: str | None = None,
    ) -> None:
        self._es = es
        self._mysql = mysql
        self._aspects = aspects_under_test
        self._upgrade_id = upgrade_id_to_check

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        try:
            indices = self._discover_indices()
        except Exception as exc:
            log.exception("SnapshotT0Phase failed during index discovery")
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=str(exc),
            )

        snap = SnapshotT0(epoch_ms=int(time.time() * 1000))

        for idx in indices:
            self._record_doc_count(snap, idx)
            self._record_alias(snap, idx)

        for aspect in self._aspects:
            self._record_aspect_histogram(snap, aspect)

        if self._upgrade_id:
            self._record_upgrade_result_presence(snap)

        ctx.snapshot_t0 = snap

        log.info(
            "Snapshot T0 captured: %d indices, %d aliases, %d aspects, T0=%d",
            len(snap.doc_counts),
            len(snap.indices),
            len(snap.aspects_by_version),
            snap.epoch_ms,
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "doc_counts": snap.doc_counts,
                "aliases": snap.indices,
                "aspects": {a: dict(v) for a, v in snap.aspects_by_version.items()},
                "epoch_ms": snap.epoch_ms,
                "upgrade_result_present": snap.upgrade_result_present,
            },
        )

    def _discover_indices(self) -> list[str]:
        """Return all DataHub-shape physical indices, filtered by suffix."""
        all_indices = self._es.list_indices(prefix=_INDEX_PREFIX)
        return [i for i in all_indices if _INDEX_SUFFIX in i]

    def _record_doc_count(self, snap: SnapshotT0, idx: str) -> None:
        try:
            snap.doc_counts[idx] = self._es.get_doc_count(idx)
        except Exception as exc:
            log.warning("doc count failed for %s: %s", idx, exc)

    def _record_alias(self, snap: SnapshotT0, idx: str) -> None:
        # Alias query is by alias name. We treat each physical index as a
        # candidate alias and check whether ES resolves it to a different
        # physical index. ES returns the empty list when the name is itself
        # a physical index (no alias).
        try:
            targets = self._es.get_alias_targets(idx)
        except Exception as exc:
            log.warning("alias query failed for %s: %s", idx, exc)
            return
        if targets:
            snap.indices[idx] = targets

    def _record_aspect_histogram(self, snap: SnapshotT0, aspect: str) -> None:
        try:
            snap.aspects_by_version[aspect] = (
                self._mysql.count_aspects_by_schema_version(aspect)
            )
        except Exception as exc:
            log.warning("aspect histogram failed for %s: %s", aspect, exc)

    def _record_upgrade_result_presence(self, snap: SnapshotT0) -> None:
        try:
            assert self._upgrade_id is not None  # narrow for type checker
            result = self._mysql.get_upgrade_result(self._upgrade_id)
            snap.upgrade_result_present = result is not None
        except Exception as exc:
            log.warning("upgrade-result check failed for %s: %s", self._upgrade_id, exc)
