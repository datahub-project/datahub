from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class MicroStrategyReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    folders_scanned: int = 0
    dashboards_scanned: int = 0
    reports_scanned: int = 0
    charts_scanned: int = 0
    datasets_scanned: int = 0
    source_warehouses_scanned: int = 0
    source_warehouse_api_failures: int = 0
    dashboard_dependencies_scanned: int = 0
    report_definition_api_failures: int = 0
    report_sql_view_api_failures: int = 0
    metric_expressions_scanned: int = 0
    metric_expression_api_failures: int = 0
    model_tables_scanned: int = 0
    model_lineage_api_failures: int = 0
    model_lineage_edges: int = 0
    warehouse_lineage_sql_views_scanned: int = 0
    warehouse_lineage_edges: int = 0
    warehouse_lineage_api_failures: int = 0
    metric_fields_scanned: int = 0
    attribute_fields_scanned: int = 0
    temporal_fields_scanned: int = 0
    chart_lineage_edges: int = 0
    dashboard_dataset_edges: int = 0
    sessions_reauthenticated: int = 0
    unresolved_visualizations: int = 0
    visualizations_suppressed_ambiguous: int = 0
    visualizations_bound_by_derived_objects: int = 0
    warehouse_upstreams_pruned_by_field_evidence: int = 0
    api_errors: int = 0
    malformed_objects_skipped: LossyList[str] = field(default_factory=LossyList)
    filtered_projects: LossyList[str] = field(default_factory=LossyList)
    filtered_dashboards: LossyList[str] = field(default_factory=LossyList)
    filtered_reports: LossyList[str] = field(default_factory=LossyList)
    sql_parse_failures: LossyList[str] = field(default_factory=LossyList)
    sql_view_rows_unmatched: int = 0
    sql_view_rows_without_context: int = 0
    sql_views_without_statement: int = 0
    failed_metric_model_ids: LossyList[str] = field(default_factory=LossyList)

    def report_project_scanned(self) -> None:
        self.projects_scanned += 1

    def report_folder_scanned(self) -> None:
        self.folders_scanned += 1

    def report_dashboard_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_report_scanned(self) -> None:
        self.reports_scanned += 1

    def report_chart_scanned(self) -> None:
        self.charts_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1

    def report_source_warehouses_scanned(self, count: int) -> None:
        self.source_warehouses_scanned += count

    def report_source_warehouse_api_failure(self) -> None:
        self.source_warehouse_api_failures += 1

    def report_dashboard_dependencies_scanned(self, count: int) -> None:
        self.dashboard_dependencies_scanned += count

    def report_report_definition_api_failure(self) -> None:
        self.report_definition_api_failures += 1

    def report_report_sql_view_api_failure(self) -> None:
        self.report_sql_view_api_failures += 1

    def report_metric_expression_scanned(self) -> None:
        self.metric_expressions_scanned += 1

    def report_metric_expression_api_failure(self) -> None:
        self.metric_expression_api_failures += 1

    def report_model_tables_scanned(self, count: int) -> None:
        self.model_tables_scanned += count

    def report_model_lineage_api_failure(self) -> None:
        self.model_lineage_api_failures += 1

    def report_model_lineage_edges(self, count: int) -> None:
        self.model_lineage_edges += count

    def report_warehouse_lineage_sql_views_scanned(self, count: int) -> None:
        self.warehouse_lineage_sql_views_scanned += count

    def report_warehouse_lineage_edges(self, count: int) -> None:
        self.warehouse_lineage_edges += count

    def report_warehouse_lineage_api_failure(self) -> None:
        self.warehouse_lineage_api_failures += 1

    def report_metric_field(self) -> None:
        self.metric_fields_scanned += 1

    def report_attribute_field(self, temporal: bool = False) -> None:
        self.attribute_fields_scanned += 1
        if temporal:
            self.temporal_fields_scanned += 1

    def report_chart_lineage_edges(self, count: int) -> None:
        self.chart_lineage_edges += count

    def report_dashboard_dataset_edges(self, count: int) -> None:
        self.dashboard_dataset_edges += count

    def report_unresolved_visualization(self) -> None:
        self.unresolved_visualizations += 1

    def report_visualizations_bound_by_derived_objects(self, count: int) -> None:
        self.visualizations_bound_by_derived_objects += count

    def report_visualization_suppressed_ambiguous(self) -> None:
        self.visualizations_suppressed_ambiguous += 1

    def report_session_reauthenticated(self) -> None:
        self.sessions_reauthenticated += 1

    def report_warehouse_upstreams_pruned(self, count: int) -> None:
        self.warehouse_upstreams_pruned_by_field_evidence += count

    def report_api_error(self) -> None:
        self.api_errors += 1

    def report_malformed_object(self, context: str) -> None:
        self.malformed_objects_skipped.append(context)

    def report_sql_parse_failure(self, context: str) -> None:
        self.sql_parse_failures.append(context)

    def report_sql_view_row_unmatched(self) -> None:
        self.sql_view_rows_unmatched += 1

    def report_sql_view_row_without_context(self) -> None:
        self.sql_view_rows_without_context += 1

    def report_sql_view_without_statement(self) -> None:
        self.sql_views_without_statement += 1

    def report_failed_metric_model(self, metric_id: str) -> None:
        self.failed_metric_model_ids.append(metric_id)
