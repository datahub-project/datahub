"""Report and confidence-rank helpers for the BigID DataHub connector."""

from __future__ import annotations

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class BigIDSourceReport(StaleEntityRemovalSourceReport):
    glossary_terms_emitted: int = 0
    glossary_nodes_emitted: int = 0
    tag_entities_emitted: int = 0
    datasets_enriched: int = 0
    datasets_created: int = 0
    columns_enriched: int = 0
    datasets_skipped_unstructured: int = 0
    datasets_enriched_unstructured: int = 0
    classifiers_without_glossary_id: int = 0
    classifier_terms_emitted: int = 0
    idsor_terms_emitted: int = 0
    findings_below_threshold: int = 0
    connections_without_platform: LossyList[str] = field(default_factory=LossyList)

    def report_connection_no_platform(self, conn_name: str) -> None:
        self.connections_without_platform.append(conn_name)


_CONFIDENCE_FLOAT = {"HIGH": 0.75, "MEDIUM": 0.50, "LOW": 0.25}


def _rank_to_float(rank: str) -> float:
    return _CONFIDENCE_FLOAT.get((rank or "").upper(), 0.0)
