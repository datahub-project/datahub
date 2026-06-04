"""Custom report class for Fabric Data Factory connector."""

from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.source.fabric.common.report import FabricClientReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class FabricDataFactoryClientReport(FabricClientReport):
    """Client report for Fabric Data Factory REST API operations."""


@dataclass
class FabricDataFactorySourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for Fabric Data Factory source."""

    # Entity counts
    workspaces_scanned: int = 0
    pipelines_scanned: int = 0
    activities_scanned: int = 0
    pipeline_runs_scanned: int = 0
    activity_runs_scanned: int = 0

    # Filtered entities
    filtered_workspaces: LossyList[str] = field(default_factory=LossyList)
    filtered_pipelines: LossyList[str] = field(default_factory=LossyList)

    # Lineage extraction tracking
    lineage_extracted: int = 0
    lineage_failed: int = 0
    lineage_failed_details: LossyList[str] = field(default_factory=LossyList)
    unmapped_connection_types: LossyDict[str, int] = field(default_factory=LossyDict)

    # Client report
    client_report: Optional[FabricDataFactoryClientReport] = None

    def report_workspace_scanned(self) -> None:
        self.workspaces_scanned += 1

    def report_workspace_filtered(self, workspace_name: str) -> None:
        self.filtered_workspaces.append(workspace_name)

    def report_pipeline_scanned(self) -> None:
        self.pipelines_scanned += 1

    def report_pipeline_filtered(self, pipeline_name: str) -> None:
        self.filtered_pipelines.append(pipeline_name)

    def report_activity_scanned(self) -> None:
        self.activities_scanned += 1

    def report_pipeline_run_scanned(self) -> None:
        self.pipeline_runs_scanned += 1

    def report_activity_run_scanned(self) -> None:
        self.activity_runs_scanned += 1

    def report_lineage_extracted(self) -> None:
        self.lineage_extracted += 1

    def report_lineage_failed(self, activity_key: str) -> None:
        self.lineage_failed += 1
        self.lineage_failed_details.append(activity_key)

    def report_unmapped_connection_type(self, connection_type: str) -> None:
        current = self.unmapped_connection_types.get(connection_type, 0)
        self.unmapped_connection_types[connection_type] = current + 1
