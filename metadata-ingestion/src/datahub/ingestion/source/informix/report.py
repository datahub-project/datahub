from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class InformixSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: int = 0
    row_counts_emitted: int = 0
    views_with_lineage: int = 0
    view_lineage_failures: int = 0
