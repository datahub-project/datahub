"""Phase 5 — InjectTrafficPrePhase.

Fires N writes via the running OLD GMS in the gap window between Phase 4
(alias swap) and Phase 6 (rolling restart). Each write becomes a "T0–T1
gap entity" that lands in the OLD physical index only — Phase 8 catch-up
(future plan) is responsible for backfilling these into the NEW index.

The phase records the gap URN list into ``TestContext.gap_urns`` so
downstream phases can verify backfill behaviour.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from .base import Phase, PhaseResult
from ..context import IndexState, TestContext
from ..datahub_client import DataHubClient
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_N_GAP_WRITES = 10
_DEFAULT_ASPECT = "embed"
_DEFAULT_GAP_PAYLOAD = {
    "renderUrl": "http://zdu-gap.example.com/embed",
}


class InjectTrafficPrePhase(Phase):
    name = "inject_traffic_pre"

    def __init__(
        self,
        datahub: DataHubClient,
        mysql: MySQLClient,
        es: ElasticsearchClient,
        n_gap_writes: int = _DEFAULT_N_GAP_WRITES,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._mysql = mysql
        self._es = es
        self._n_gap_writes = n_gap_writes
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        urns = [
            f"urn:li:dashboard:(test,zdu-gap-{i})" for i in range(self._n_gap_writes)
        ]
        ingested: list[str] = []

        for urn in urns:
            try:
                self._datahub.ingest_mcp(
                    urn,
                    self._aspect_name,
                    dict(_DEFAULT_GAP_PAYLOAD),
                    system_metadata={},
                )
            except Exception as exc:
                log.exception("InjectTrafficPre: ingest failed for %s", urn)
                # Record the partial list of successfully-ingested URNs so
                # downstream phases can reason about what made it through.
                ctx.gap_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"ingest failed for {urn}: {exc}",
                    details={"gap_urns": list(ingested)},
                )
            ingested.append(urn)

        for urn in urns:
            row = self._mysql.get_aspect_raw(urn, self._aspect_name)
            if row is None:
                # Record the full ingested list so Phase 8 (future) sees what
                # actually made it through ingest, even if MySQL was incomplete.
                ctx.gap_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"{urn}: missing from MySQL after ingest",
                    details={"gap_urns": list(ingested)},
                )

        index_warnings = self._check_index_distinction(ctx, urns)

        ctx.gap_urns = urns
        log.info(
            "InjectTrafficPre complete — %d gap URNs written; %d index-distinction warnings",
            len(urns),
            len(index_warnings),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "gap_urns": urns,
                "index_distinction_warnings": index_warnings,
            },
        )

    def _check_index_distinction(self, ctx: TestContext, urns: list[str]) -> list[str]:
        """Verify each URN appears in OLD index and not in NEW.

        Returns warning strings. Empty list = clean. Doesn't fail the phase —
        these checks rely on a real Phase 1 reindex having run, which may
        not be the case in dev workflows.
        """
        if not ctx.upgrade_blocking or not ctx.upgrade_blocking.indices:
            return []

        target = self._find_dashboard_index(ctx.upgrade_blocking.indices)
        if target is None:
            return []
        old_idx = target.old_backing_index_name
        new_idx = target.next_index_name
        if old_idx is None or new_idx is None or old_idx == new_idx:
            self._sanity_check_single_index(old_idx or new_idx, urns)
            return []

        warnings: list[str] = []
        for urn in urns:
            doc_id = urn  # ES doc id is the URN; get_doc URL-encodes it.
            old_doc = self._safe_get_doc(old_idx, doc_id)
            new_doc = self._safe_get_doc(new_idx, doc_id)
            if old_doc is None:
                warnings.append(f"{urn}: missing from OLD index {old_idx}")
            if new_doc is not None:
                warnings.append(
                    f"{urn}: leaked into NEW index {new_idx} during gap window"
                )
        return warnings

    def _find_dashboard_index(self, indices: list[IndexState]) -> IndexState | None:
        for idx in indices:
            if "dashboard" in idx.alias.lower():
                return idx
        return None

    def _sanity_check_single_index(
        self, index_name: str | None, urns: list[str]
    ) -> None:
        if index_name is None or not urns:
            return
        first_urn = urns[0]
        doc = self._safe_get_doc(index_name, first_urn)
        if doc is None:
            log.warning(
                "InjectTrafficPre sanity: %s not in single-image index %s",
                first_urn,
                index_name,
            )

    def _safe_get_doc(self, index_name: str, doc_id: str) -> dict[str, Any] | None:
        try:
            return self._es.get_doc(index_name, doc_id)
        except Exception as exc:
            log.warning("ES get_doc failed for %s/%s: %s", index_name, doc_id, exc)
            return None
