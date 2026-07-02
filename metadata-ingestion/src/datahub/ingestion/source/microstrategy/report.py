from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


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
    unresolved_visualizations: int = 0
    api_errors: int = 0

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

    def report_api_error(self) -> None:
        self.api_errors += 1
