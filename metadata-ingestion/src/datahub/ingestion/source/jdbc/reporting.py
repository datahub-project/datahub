from dataclasses import dataclass

from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport


@dataclass
class JDBCSourceReport(
    SQLSourceReport, StaleEntityRemovalSourceReport, IngestionStageReport
):
    """Report for JDBC source ingestion"""

    tables_scanned: int = 0
    views_scanned: int = 0
    stored_procedures_scanned: int = 0
    filtered_schemas: int = 0
    filtered_tables: int = 0
    filtered_views: int = 0
    filtered_stored_procedures: int = 0

    def report_table_scanned(self, table: str) -> None:
        super().report_entity_scanned(table)
        self.tables_scanned += 1

    def report_view_scanned(self, view: str) -> None:
        super().report_entity_scanned(view)
        self.views_scanned += 1

    def report_stored_procedure_scanned(self, proc: str) -> None:
        super().report_entity_scanned(proc)
        self.stored_procedures_scanned += 1

    def report_schema_filtered(self, schema: str) -> None:
        self.filtered_schemas += 1
        self.report_dropped(f"Schema: {schema}")

    def report_table_filtered(self, table: str) -> None:
        self.filtered_tables += 1
        self.report_dropped(f"Table: {table}")

    def report_view_filtered(self, view: str) -> None:
        self.filtered_views += 1
        self.report_dropped(f"View: {view}")

    def report_stored_procedure_filtered(self, proc: str) -> None:
        self.filtered_stored_procedures += 1
        self.report_dropped(f"Stored Procedure: {proc}")

    def set_ingestion_stage(self, dataset: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{dataset}: {stage}")
