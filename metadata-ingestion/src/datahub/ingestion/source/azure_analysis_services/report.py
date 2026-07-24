from dataclasses import dataclass

from datahub.ingestion.source.common.m_query.report import MQueryLineageReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class AzureAnalysisServicesReport(StaleEntityRemovalSourceReport, MQueryLineageReport):
    # Entity counters.
    databases_scanned: int = 0
    tables_scanned: int = 0
    columns_scanned: int = 0
    measures_scanned: int = 0
    calculated_tables_scanned: int = 0
    relationships_scanned: int = 0
    roles_scanned: int = 0

    # Lineage counters.
    tables_with_upstream_lineage: int = 0
    tables_without_upstream_lineage: int = 0
    column_lineage_edges: int = 0
    intra_model_dax_edges: int = 0

    # Degradation counters.
    tables_skipped: int = 0
    model_definition_failures: int = 0

    filtered_databases: LossyList[str] = None  # type: ignore[assignment]
    filtered_tables: LossyList[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # The base report sets up its own state (e.g. the file-based workunit
        # dict) in __post_init__, so it must run first.
        super().__post_init__()
        # LossyList is not a valid dataclass default; initialise here so each
        # report instance gets its own list.
        self.filtered_databases = LossyList()
        self.filtered_tables = LossyList()

    def report_database_filtered(self, name: str) -> None:
        self.filtered_databases.append(name)

    def report_table_filtered(self, name: str) -> None:
        self.filtered_tables.append(name)
