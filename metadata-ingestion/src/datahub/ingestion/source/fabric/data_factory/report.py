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

    # Column-level lineage tracking (Copy activities)
    column_lineage_extracted: int = 0
    column_lineage_activities_explicit: int = 0
    column_lineage_activities_auto_mapped: int = 0
    column_lineage_activities_auto_created_sink: int = 0
    column_lineage_skipped_no_schema: int = 0
    column_lineage_skipped_no_schema_details: LossyList[str] = field(
        default_factory=LossyList
    )
    column_lineage_skipped_unresolvable_mappings: int = 0
    column_lineage_skipped_unresolvable_mappings_details: LossyList[str] = field(
        default_factory=LossyList
    )
    # Explicit mapping entries that are not name-based (e.g. ordinal-only),
    # dropped from activities whose other mappings were emitted.
    column_lineage_mappings_skipped: int = 0
    # Source columns with no same-named sink column under by-name mapping.
    column_lineage_unmatched_columns: int = 0
    column_lineage_skipped_dynamic_translator: int = 0
    column_lineage_skipped_unsupported_translator: int = 0
    column_lineage_skipped_translator_details: LossyList[str] = field(
        default_factory=LossyList
    )
    column_lineage_schema_lookup_failed: int = 0
    column_lineage_failed: int = 0

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

    def report_column_lineage_explicit(
        self, num_edges: int, num_skipped_mappings: int = 0
    ) -> None:
        self.column_lineage_activities_explicit += 1
        self.column_lineage_extracted += num_edges
        self.column_lineage_mappings_skipped += num_skipped_mappings

    def report_column_lineage_unresolvable_mappings(self, activity_key: str) -> None:
        self.column_lineage_skipped_unresolvable_mappings += 1
        self.column_lineage_skipped_unresolvable_mappings_details.append(activity_key)

    def report_column_lineage_auto_mapped(
        self, num_edges: int, num_unmatched_columns: int = 0
    ) -> None:
        self.column_lineage_activities_auto_mapped += 1
        self.column_lineage_extracted += num_edges
        self.column_lineage_unmatched_columns += num_unmatched_columns

    def report_column_lineage_auto_created_sink(self, num_edges: int) -> None:
        self.column_lineage_activities_auto_created_sink += 1
        self.column_lineage_extracted += num_edges

    def report_column_lineage_no_schema(self, activity_key: str) -> None:
        self.column_lineage_skipped_no_schema += 1
        self.column_lineage_skipped_no_schema_details.append(activity_key)

    def report_column_lineage_dynamic_translator(self, activity_key: str) -> None:
        self.column_lineage_skipped_dynamic_translator += 1
        self.column_lineage_skipped_translator_details.append(
            f"{activity_key} (dynamic)"
        )

    def report_column_lineage_unsupported_translator(self, activity_key: str) -> None:
        self.column_lineage_skipped_unsupported_translator += 1
        self.column_lineage_skipped_translator_details.append(
            f"{activity_key} (unsupported)"
        )

    def report_column_lineage_schema_lookup_failed(self) -> None:
        self.column_lineage_schema_lookup_failed += 1

    def report_column_lineage_failed(self) -> None:
        self.column_lineage_failed += 1
