"""Custom report class for Fabric OneLake connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from datahub.ingestion.source.fabric.common.report import FabricClientReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.stats_collections import TopKDict

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.schema_report import (
        SqlAnalyticsEndpointReport,
    )
    from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport


@dataclass
class FabricOneLakeClientReport(FabricClientReport):
    """Client report for Fabric OneLake REST API operations.

    Extends FabricClientReport for OneLake-specific client metrics.
    Currently inherits all functionality from FabricClientReport but can be
    extended with OneLake-specific metrics in the future.
    """


@dataclass
class FabricOneLakeSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for Fabric OneLake source.

    Tracks metrics specific to OneLake ingestion including counts of
    workspaces, lakehouses, warehouses, schemas, and tables.
    """

    # Entity counts
    workspaces_scanned: int = 0
    lakehouses_scanned: int = 0
    warehouses_scanned: int = 0
    schemas_scanned: int = 0
    tables_scanned: int = 0
    views_scanned: int = 0

    # Filtered entities
    filtered_workspaces: LossyList[str] = field(default_factory=LossyList)
    filtered_lakehouses: LossyList[str] = field(default_factory=LossyList)
    filtered_warehouses: LossyList[str] = field(default_factory=LossyList)
    filtered_tables: LossyList[str] = field(default_factory=LossyList)
    filtered_views: LossyList[str] = field(default_factory=LossyList)

    # Views whose definition was unavailable (e.g. caller lacks
    # `VIEW DEFINITION` permission); their lineage cannot be parsed.
    views_missing_definition: LossyList[str] = field(default_factory=LossyList)

    # API metrics (can be populated from FabricClientReport)
    api_calls_total_count: int = 0
    api_calls_total_error_count: int = 0

    # Client report (optional, can be set from OneLakeClient)
    client_report: Optional[FabricOneLakeClientReport] = None

    # Schema extraction report (optional, can be set from schema extraction client)
    schema_report: Optional["SqlAnalyticsEndpointReport"] = None

    # SQL parsing aggregator report (lineage / view parsing metrics)
    sql_aggregator: Optional["SqlAggregatorReport"] = None

    num_usage_queries_fetched: int = 0
    num_usage_queries_skipped: TopKDict[str, int] = field(default_factory=TopKDict)
    usage_extraction_per_item_sec: LossyDict[str, float] = field(
        default_factory=LossyDict
    )
    usage_start_time: Optional[datetime] = None
    usage_end_time: Optional[datetime] = None
    usage_run_skipped: bool = False

    def report_usage_query_skipped(self, reason: str) -> None:
        """Record a queryinsights row that was skipped before reaching the aggregator."""
        self.num_usage_queries_skipped[reason] = (
            self.num_usage_queries_skipped.get(reason, 0) + 1
        )

    def report_workspace_scanned(self) -> None:
        """Increment workspaces scanned counter."""
        self.workspaces_scanned += 1

    def report_workspace_filtered(self, workspace_name: str) -> None:
        """Record a filtered workspace."""
        self.filtered_workspaces.append(workspace_name)

    def report_lakehouse_scanned(self) -> None:
        """Increment lakehouses scanned counter."""
        self.lakehouses_scanned += 1

    def report_lakehouse_filtered(self, lakehouse_name: str) -> None:
        """Record a filtered lakehouse."""
        self.filtered_lakehouses.append(lakehouse_name)

    def report_warehouse_scanned(self) -> None:
        """Increment warehouses scanned counter."""
        self.warehouses_scanned += 1

    def report_warehouse_filtered(self, warehouse_name: str) -> None:
        """Record a filtered warehouse."""
        self.filtered_warehouses.append(warehouse_name)

    def report_schema_scanned(self) -> None:
        """Increment schemas scanned counter."""
        self.schemas_scanned += 1

    def report_table_scanned(self) -> None:
        """Increment tables scanned counter."""
        self.tables_scanned += 1

    def report_table_filtered(self, table_name: str) -> None:
        """Record a filtered table."""
        self.filtered_tables.append(table_name)

    def report_view_scanned(self) -> None:
        """Increment views scanned counter."""
        self.views_scanned += 1

    def report_view_filtered(self, view_name: str) -> None:
        """Record a filtered view."""
        self.filtered_views.append(view_name)

    def report_view_missing_definition(self, view_name: str) -> None:
        """Record a view whose SQL definition was unavailable for lineage parsing."""
        self.views_missing_definition.append(view_name)

    def report_api_call(self) -> None:
        """Track an API call."""
        self.api_calls_total_count += 1

    def report_api_error(self, endpoint: str, error: str) -> None:
        """Record an API error."""
        self.api_calls_total_error_count += 1
        self.report_warning(
            title="API Error",
            message="Failed to call Fabric REST API.",
            context=f"endpoint={endpoint}, error={error}",
        )
