from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


class GrafanaSourceReport(StaleEntityRemovalSourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    folders_scanned: int = 0
    datasets_scanned: int = 0

    def report_dashboard_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_chart_scanned(self) -> None:
        self.charts_scanned += 1

    def report_folder_scanned(self) -> None:
        self.folders_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1
