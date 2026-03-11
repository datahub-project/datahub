"""Custom report class for Fabric Data Factory connector."""

from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.source.fabric.common.report import FabricClientReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class FabricDataFactoryClientReport(FabricClientReport):
    """Client report for Fabric Data Factory REST API operations."""


@dataclass
class FabricDataFactorySourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for Fabric Data Factory source."""

    # Entity counts
    workspaces_scanned: int = 0
    pipelines_scanned: int = 0
    copyjobs_scanned: int = 0
    dataflows_scanned: int = 0
    activities_scanned: int = 0
    pipeline_runs_scanned: int = 0

    # Filtered entities
    filtered_workspaces: LossyList[str] = field(default_factory=LossyList)
    filtered_pipelines: LossyList[str] = field(default_factory=LossyList)
    filtered_copyjobs: LossyList[str] = field(default_factory=LossyList)
    filtered_dataflows: LossyList[str] = field(default_factory=LossyList)

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

    def report_copyjob_scanned(self) -> None:
        self.copyjobs_scanned += 1

    def report_copyjob_filtered(self, copyjob_name: str) -> None:
        self.filtered_copyjobs.append(copyjob_name)

    def report_dataflow_scanned(self) -> None:
        self.dataflows_scanned += 1

    def report_dataflow_filtered(self, dataflow_name: str) -> None:
        self.filtered_dataflows.append(dataflow_name)

    def report_activity_scanned(self) -> None:
        self.activities_scanned += 1

    def report_pipeline_run_scanned(self) -> None:
        self.pipeline_runs_scanned += 1
