from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class OmniSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for the Omni BI platform source."""

    connections_scanned: int = 0
    models_scanned: int = 0
    topics_scanned: int = 0
    documents_scanned: int = 0
    dashboards_scanned: int = 0
    semantic_datasets_emitted: int = 0
    physical_datasets_emitted: int = 0
    dataset_lineage_edges_emitted: int = 0
    fine_grained_lineage_edges_exact: int = 0
    fine_grained_lineage_edges_derived: int = 0
    fine_grained_lineage_edges_unresolved: int = 0
