"""Phase 7 — InjectTrafficDualPhase.

Fires N writes via the running NEW GMS (post-rolling-restart) during the
dual-write window. Each write should appear in BOTH the OLD physical
index (rollback safety net) and the NEW physical index (primary).

The phase records the dual-write URN list into
``TestContext.dual_write_urns`` so Phase 8 (UpgradeNonBlockingPhase) and
Phase 10 (Validation) can verify dual-write disable behaviour.
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

_DEFAULT_N_DUAL_WRITES = 10
_DEFAULT_ASPECT = "embed"
_DEFAULT_DUAL_PAYLOAD = {
    "renderUrl": "http://zdu-dual.example.com/embed",
}


class InjectTrafficDualPhase(Phase):
    name = "inject_traffic_dual"

    def __init__(
        self,
        datahub: DataHubClient,
        mysql: MySQLClient,
        es: ElasticsearchClient,
        n_dual_writes: int = _DEFAULT_N_DUAL_WRITES,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._mysql = mysql
        self._es = es
        self._n_dual_writes = n_dual_writes
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        urns = [
            f"urn:li:dashboard:(test,zdu-dual-{i})" for i in range(self._n_dual_writes)
        ]
        ingested: list[str] = []

        for urn in urns:
            try:
                self._datahub.ingest_mcp(
                    urn,
                    self._aspect_name,
                    dict(_DEFAULT_DUAL_PAYLOAD),
                    system_metadata={},
                )
            except Exception as exc:
                log.exception("InjectTrafficDual: ingest failed for %s", urn)
                # Record the partial list of successfully-ingested URNs.
                ctx.dual_write_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"ingest failed for {urn}: {exc}",
                    details={"dual_write_urns": list(ingested)},
                )
            ingested.append(urn)

        for urn in urns:
            row = self._mysql.get_aspect_raw(urn, self._aspect_name)
            if row is None:
                # Record the full ingested list — all ingests succeeded
                # before MySQL came up missing this row.
                ctx.dual_write_urns = list(ingested)
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"{urn}: missing from MySQL after ingest",
                    details={"dual_write_urns": list(ingested)},
                )

        warnings = self._check_dual_write_fanout(ctx, urns)

        ctx.dual_write_urns = urns
        log.info(
            "InjectTrafficDual complete — %d dual-write URNs written; %d warnings",
            len(urns),
            len(warnings),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "dual_write_urns": urns,
                "dual_write_warnings": warnings,
            },
        )

    def _check_dual_write_fanout(self, ctx: TestContext, urns: list[str]) -> list[str]:
        """Verify each URN appears in BOTH OLD and NEW physical indices.

        Best-effort — relies on a real Phase 1 reindex having run. Returns
        warning strings; doesn't fail the phase.
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
            if new_doc is None:
                warnings.append(f"{urn}: missing from NEW index {new_idx}")
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
                "InjectTrafficDual sanity: %s not in single-image index %s",
                first_urn,
                index_name,
            )

    def _safe_get_doc(self, index_name: str, doc_id: str) -> dict[str, Any] | None:
        try:
            return self._es.get_doc(index_name, doc_id)
        except Exception as exc:
            log.warning("ES get_doc failed for %s/%s: %s", index_name, doc_id, exc)
            return None
