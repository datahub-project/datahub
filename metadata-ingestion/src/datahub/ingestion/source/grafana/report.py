from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class GrafanaSourceReport(StaleEntityRemovalSourceReport):
    # Entity counters
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    folders_scanned: int = 0
    datasets_scanned: int = 0

    # Lineage counters
    panels_with_lineage: int = 0
    panels_without_lineage: int = 0
    lineage_extraction_failures: int = 0
    sql_parsing_attempts: int = 0
    sql_parsing_successes: int = 0
    sql_parsing_failures: int = 0

    # Schema extraction counters
    panels_with_schema_fields: int = 0
    panels_without_schema_fields: int = 0

    # Warning counters
    permission_warnings: int = 0
    datasource_warnings: int = 0
    panel_parsing_warnings: int = 0

    def report_dashboard_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_chart_scanned(self) -> None:
        self.charts_scanned += 1

    def report_folder_scanned(self) -> None:
        self.folders_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1

    # Lineage reporting methods
    def report_lineage_extracted(self) -> None:
        """Report successful lineage extraction for a panel"""
        self.panels_with_lineage += 1

    def report_no_lineage(self) -> None:
        """Report that no lineage was found for a panel"""
        self.panels_without_lineage += 1

    def report_lineage_extraction_failure(self) -> None:
        """Report failure to extract lineage for a panel"""
        self.lineage_extraction_failures += 1

    def report_sql_parsing_attempt(self) -> None:
        """Report attempt to parse SQL"""
        self.sql_parsing_attempts += 1

    def report_sql_parsing_success(self) -> None:
        """Report successful SQL parsing"""
        self.sql_parsing_successes += 1

    def report_sql_parsing_failure(self) -> None:
        """Report failed SQL parsing"""
        self.sql_parsing_failures += 1

    # Schema field reporting methods
    def report_schema_fields_extracted(self) -> None:
        """Report that schema fields were extracted for a panel"""
        self.panels_with_schema_fields += 1

    def report_no_schema_fields(self) -> None:
        """Report that no schema fields were found for a panel"""
        self.panels_without_schema_fields += 1

    # Warning reporting methods
    def report_permission_warning(self) -> None:
        """Report a permission-related warning"""
        self.permission_warnings += 1

    def report_datasource_warning(self) -> None:
        """Report a datasource-related warning"""
        self.datasource_warnings += 1

    def report_panel_parsing_warning(self) -> None:
        """Report a panel parsing warning"""
        self.panel_parsing_warnings += 1
