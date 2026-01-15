"""REST API client for Microsoft Fabric OneLake."""

import logging
from typing import Iterator, Optional

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.base_client import BaseFabricClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
    FabricWorkspace,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeClientReport

logger = logging.getLogger(__name__)


def _parse_table_name(full_name: str) -> tuple[str, str]:
    """Parse schema and table name from fully qualified name.

    Args:
        full_name: Fully qualified table name (e.g., "schema.table" or "table")

    Returns:
        Tuple of (schema_name, table_name)
    """
    if "." in full_name:
        schema_name, table_name = full_name.rsplit(".", 1)
    else:
        schema_name = "dbo"  # Default schema
        table_name = full_name
    return schema_name, table_name


class OneLakeClient(BaseFabricClient):
    """Client for Microsoft Fabric OneLake REST API.

    Provides methods to interact with Fabric workspaces, lakehouses, warehouses, and tables.
    """

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricOneLakeClientReport] = None,
    ):
        """Initialize OneLake client.

        Args:
            auth_helper: Authentication helper
            timeout: Request timeout
            report: Optional OneLake client report
        """
        super().__init__(auth_helper, timeout, report)

    def get_base_endpoint(self) -> str:
        """Get the base API endpoint for OneLake client."""
        return "workspaces"

    def list_workspaces(self) -> Iterator[FabricWorkspace]:
        """List all accessible Fabric workspaces.

        Yields:
            FabricWorkspace objects
        """
        logger.info("Listing Fabric workspaces for OneLake")
        for workspace_data in super()._list_workspaces_raw():
            yield FabricWorkspace(
                id=workspace_data.get("id", ""),
                name=workspace_data.get("displayName", ""),
                description=workspace_data.get("description"),
                type=workspace_data.get("type"),
                capacity_id=workspace_data.get("capacityId"),
            )

    def list_lakehouses(self, workspace_id: str) -> Iterator[FabricLakehouse]:
        """List lakehouses in a workspace.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/list-lakehouses

        Args:
            workspace_id: Workspace GUID

        Yields:
            FabricLakehouse objects
        """
        logger.info(f"Listing lakehouses for workspace {workspace_id}")
        try:
            response = self.get(f"workspaces/{workspace_id}/lakehouses")
            data = response.json()
            lakehouses = data.get("value", [])
            logger.info(
                f"Found {len(lakehouses)} lakehouse(s) in workspace {workspace_id}"
            )

            for lakehouse_data in lakehouses:
                logger.debug(
                    f"Processing lakehouse: {lakehouse_data.get('displayName', 'Unknown')}"
                )
                yield FabricLakehouse(
                    id=lakehouse_data.get("id", ""),
                    name=lakehouse_data.get("displayName", ""),
                    type="Lakehouse",
                    workspace_id=workspace_id,
                    description=lakehouse_data.get("description"),
                )
        except Exception as e:
            logger.error(f"Failed to list lakehouses for workspace {workspace_id}: {e}")
            raise

    def list_warehouses(self, workspace_id: str) -> Iterator[FabricWarehouse]:
        """List warehouses in a workspace.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items/list-warehouses

        Args:
            workspace_id: Workspace GUID

        Yields:
            FabricWarehouse objects
        """
        logger.info(f"Listing warehouses for workspace {workspace_id}")
        try:
            response = self.get(f"workspaces/{workspace_id}/warehouses")
            data = response.json()
            warehouses = data.get("value", [])
            logger.info(
                f"Found {len(warehouses)} warehouse(s) in workspace {workspace_id}"
            )

            for warehouse_data in warehouses:
                logger.debug(
                    f"Processing warehouse: {warehouse_data.get('displayName', 'Unknown')}"
                )
                yield FabricWarehouse(
                    id=warehouse_data.get("id", ""),
                    name=warehouse_data.get("displayName", ""),
                    type="Warehouse",
                    workspace_id=workspace_id,
                    description=warehouse_data.get("description"),
                )
        except Exception as e:
            logger.error(f"Failed to list warehouses for workspace {workspace_id}: {e}")
            raise

    def list_lakehouse_tables(
        self, workspace_id: str, lakehouse_id: str
    ) -> Iterator[FabricTable]:
        """List all tables in a lakehouse.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/tables/list

        Args:
            workspace_id: Workspace GUID
            lakehouse_id: Lakehouse GUID

        Yields:
            FabricTable objects
        """
        logger.info(f"Listing tables for lakehouse {lakehouse_id}")
        try:
            response = self.get(
                f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
            )
            data = response.json()

            tables = data.get("value", [])
            logger.info(f"Found {len(tables)} table(s) in lakehouse {lakehouse_id}")

            for table_data in tables:
                full_name = table_data.get("name", "")
                schema_name, table_name = _parse_table_name(full_name)
                logger.debug(f"Processing table: {schema_name}.{table_name}")

                yield FabricTable(
                    name=table_name,
                    schema_name=schema_name,
                    item_id=lakehouse_id,
                    workspace_id=workspace_id,
                    description=table_data.get("description"),
                )

        except Exception as e:
            logger.error(f"Failed to list tables for lakehouse {lakehouse_id}: {e}")
            raise

    def list_warehouse_tables(
        self, workspace_id: str, warehouse_id: str
    ) -> Iterator[FabricTable]:
        """List all tables in a warehouse.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/tables/list

        Args:
            workspace_id: Workspace GUID
            warehouse_id: Warehouse GUID

        Yields:
            FabricTable objects
        """
        logger.info(f"Listing tables for warehouse {warehouse_id}")
        try:
            response = self.get(
                f"workspaces/{workspace_id}/warehouses/{warehouse_id}/tables"
            )
            data = response.json()

            tables = data.get("value", [])
            logger.info(f"Found {len(tables)} table(s) in warehouse {warehouse_id}")

            for table_data in tables:
                full_name = table_data.get("name", "")
                schema_name, table_name = _parse_table_name(full_name)
                logger.debug(f"Processing table: {schema_name}.{table_name}")

                yield FabricTable(
                    name=table_name,
                    schema_name=schema_name,
                    item_id=warehouse_id,
                    workspace_id=workspace_id,
                    description=table_data.get("description"),
                )

        except Exception as e:
            logger.error(f"Failed to list tables for warehouse {warehouse_id}: {e}")
            raise

    # TODO: Implement batch schema extraction for tables
    # The Fabric REST API Table model does not include column schema information.
    # To extract table schemas (column names, data types, nullability, etc.), we need to:
    # 1. Use the SQL Analytics endpoint (available for both Lakehouses and Warehouses)
    # 2. Query INFORMATION_SCHEMA.COLUMNS using a SQL query like:
    #    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
    #    FROM INFORMATION_SCHEMA.COLUMNS
    #    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    # 3. This requires:
    #    - SQL endpoint connection string template (with {workspace_id} and {item_id} placeholders)
    #    - pyodbc library for ODBC connections
    #    - Proper authentication (can use same TokenCredential via ActiveDirectoryMsi or similar)
    # Reference: https://learn.microsoft.com/en-us/fabric/database/sql/sql-analytics-endpoint
