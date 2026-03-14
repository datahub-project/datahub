from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class OmniSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for the Omni BI platform source."""

    filtered: LossyList[str] = field(default_factory=LossyList)
    connections_scanned: int = 0
    models_scanned: int = 0
    documents_scanned: int = 0
    dashboards_scanned: int = 0
    semantic_datasets_emitted: int = 0
    physical_datasets_emitted: int = 0
    dataset_lineage_edges_emitted: int = 0
    fine_grained_lineage_edges_exact: int = 0
    fine_grained_lineage_edges_derived: int = 0
    fine_grained_lineage_edges_unresolved: int = 0

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)
