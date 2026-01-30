"""Custom report class for Fabric OneLake connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from datahub.ingestion.source.fabric.common.report import FabricClientReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.schema_report import (
        SqlAnalyticsEndpointReport,
    )


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

    # Filtered entities
    filtered_workspaces: LossyList[str] = field(default_factory=LossyList)
    filtered_lakehouses: LossyList[str] = field(default_factory=LossyList)
    filtered_warehouses: LossyList[str] = field(default_factory=LossyList)
    filtered_tables: LossyList[str] = field(default_factory=LossyList)

    # API metrics (can be populated from FabricClientReport)
    api_calls_total_count: int = 0
    api_calls_total_error_count: int = 0

    # Client report (optional, can be set from OneLakeClient)
    client_report: Optional[FabricOneLakeClientReport] = None

    # Schema extraction report (optional, can be set from schema extraction client)
    schema_report: Optional["SqlAnalyticsEndpointReport"] = None

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
