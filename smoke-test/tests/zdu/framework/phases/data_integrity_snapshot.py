"""Phase — DataIntegritySnapshotPhase.

Runs AFTER ``upgrade_nonblocking`` and BEFORE ``validation``. Queries ES for
every gap and dual-write URN to verify they're searchable post-Phase-10.
Captures presence in the entity index alias and aspect counts in the
``system_metadata_service_v1`` index.

Used by Suite D's data-integrity validators (TC-201, TC-204). The original
Suite D scenarios target production catch-up mechanics (dual_write_start_times,
catch_up_windows) — those are blocked on dev because the upstream
``BuildIndicesIncrementalStep`` doesn't persist ``oldBackingIndexName`` in
MySQL state. The data-integrity reframe asserts the OUTCOME (every URN
written across the rolling-restart + catch-up window is preserved in ES)
without depending on the specific production code path.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import DataIntegritySnapshot, TestContext
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient

log = logging.getLogger(__name__)

_DEFAULT_ENTITY_INDEX_ALIAS = "dashboardindex_v2"
_SYSTEM_METADATA_INDEX = "system_metadata_service_v1"
# Aspect we drive the migration on; every gap/dual URN writes this aspect.
_EMBED_ASPECT = "embed"


class DataIntegritySnapshotPhase(Phase):
    name = "data_integrity_snapshot"

    def __init__(
        self,
        es: ElasticsearchClient,
        mysql: MySQLClient | None = None,
        entity_index_alias: str = _DEFAULT_ENTITY_INDEX_ALIAS,
        systemmetadata_index: str = _SYSTEM_METADATA_INDEX,
    ) -> None:
        self._es = es
        self._mysql = mysql
        self._entity_index_alias = entity_index_alias
        self._systemmetadata_index = systemmetadata_index

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        urns = list(ctx.gap_urns) + list(ctx.dual_write_urns)
        if not urns:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=0.0,
                details={"reason": "no gap_urns or dual_write_urns to validate"},
            )

        snap = DataIntegritySnapshot(entity_index_alias=self._entity_index_alias)
        for urn in urns:
            try:
                entity_hits = self._es.count_matching_urn(self._entity_index_alias, urn)
                snap.entity_index_presence[urn] = entity_hits > 0
            except Exception as exc:
                log.warning(
                    "[data-integrity] entity-index query failed for %s: %s", urn, exc
                )
                snap.entity_index_presence[urn] = False
            try:
                snap.systemmetadata_counts[urn] = self._es.count_matching_urn(
                    self._systemmetadata_index, urn
                )
            except Exception as exc:
                log.warning(
                    "[data-integrity] systemMetadata query failed for %s: %s", urn, exc
                )
                snap.systemmetadata_counts[urn] = 0
            if self._mysql is not None:
                try:
                    snap.embed_schema_versions[urn] = self._mysql.get_schema_version(
                        urn, _EMBED_ASPECT
                    )
                except Exception as exc:
                    log.warning(
                        "[data-integrity] MySQL schemaVersion query failed for %s: %s",
                        urn,
                        exc,
                    )
                    snap.embed_schema_versions[urn] = None

        ctx.data_integrity_snapshot = snap
        n_entity_present = sum(1 for v in snap.entity_index_presence.values() if v)
        n_sysmeta_present = sum(1 for v in snap.systemmetadata_counts.values() if v > 0)
        duration = time.monotonic() - t0
        log.info(
            "Data integrity snapshot: %d/%d URNs in %s, %d/%d URNs in %s (%d aspect "
            "entries total)",
            n_entity_present,
            len(urns),
            self._entity_index_alias,
            n_sysmeta_present,
            len(urns),
            self._systemmetadata_index,
            sum(snap.systemmetadata_counts.values()),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration,
            details={
                "urns_checked": len(urns),
                "entity_index_alias": self._entity_index_alias,
                "entity_index_present": n_entity_present,
                "systemmetadata_index_present": n_sysmeta_present,
                "systemmetadata_total_aspects": sum(
                    snap.systemmetadata_counts.values()
                ),
                "embed_schema_versions_captured": len(snap.embed_schema_versions),
            },
        )
