"""REST API client for Microsoft Fabric OneLake."""

import logging
from typing import Callable, Iterator, Optional, TypeVar

import requests

from datahub.ingestion.source.fabric.common.auth import (
    ONELAKE_STORAGE_SCOPE,
    FabricAuthHelper,
)
from datahub.ingestion.source.fabric.common.base_client import BaseFabricClient
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.constants import FABRIC_SYSTEM_SCHEMAS
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeClientReport

logger = logging.getLogger(__name__)

# OneLake Delta Table APIs base URL
ONELAKE_TABLE_API_BASE_URL = "https://onelake.table.fabric.microsoft.com"

# Page size requested from the Unity Catalog list endpoints (`max_results`).
# The service may return fewer and caps large values; paging continues until
# no `next_page_token` is returned.
ONELAKE_CATALOG_PAGE_SIZE = 1000


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

    T = TypeVar("T", FabricLakehouse, FabricWarehouse)

    def _list_items(
        self,
        workspace_id: str,
        item_type: str,
        endpoint_suffix: str,
        factory: Callable[..., T],
    ) -> Iterator[T]:
        """List items (lakehouses or warehouses) in a workspace.

        Args:
            workspace_id: Workspace GUID
            item_type: Item type name for logging (e.g., "lakehouse", "warehouse")
            endpoint_suffix: API endpoint suffix (e.g., "lakehouses", "warehouses")
            factory: Factory function to create the item object

        Yields:
            Item objects (FabricLakehouse or FabricWarehouse)
        """
        logger.info(f"Listing {item_type}s for workspace {workspace_id}")
        try:
            response = self.get(f"workspaces/{workspace_id}/{endpoint_suffix}")
            data = response.json()
            items = data.get("value", [])
            logger.info(
                f"Found {len(items)} {item_type}(s) in workspace {workspace_id}"
            )

            for item_data in items:
                logger.debug(
                    f"Processing {item_type}: {item_data.get('displayName', 'Unknown')}"
                )
                yield factory(
                    id=item_data.get("id", ""),
                    name=item_data.get("displayName", ""),
                    type=item_type.capitalize(),
                    workspace_id=workspace_id,
                    description=item_data.get("description"),
                )
        except Exception as e:
            logger.error(
                f"Failed to list {item_type}s for workspace {workspace_id}: {e}"
            )
            raise

    def list_lakehouses(self, workspace_id: str) -> Iterator[FabricLakehouse]:
        """List lakehouses in a workspace.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/list-lakehouses

        Args:
            workspace_id: Workspace GUID

        Yields:
            FabricLakehouse objects
        """
        yield from self._list_items(
            workspace_id=workspace_id,
            item_type="lakehouse",
            endpoint_suffix="lakehouses",
            factory=FabricLakehouse,
        )

    def list_warehouses(self, workspace_id: str) -> Iterator[FabricWarehouse]:
        """List warehouses in a workspace.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items/list-warehouses

        Args:
            workspace_id: Workspace GUID

        Yields:
            FabricWarehouse objects
        """
        yield from self._list_items(
            workspace_id=workspace_id,
            item_type="warehouse",
            endpoint_suffix="warehouses",
            factory=FabricWarehouse,
        )

    def _is_lakehouse_schemas_enabled(
        self, workspace_id: str, lakehouse_id: str
    ) -> bool:
        """Check if a lakehouse has schemas enabled.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/get-lakehouse

        Args:
            workspace_id: Workspace GUID
            lakehouse_id: Lakehouse GUID

        Returns:
            True if schemas are enabled, False otherwise
        """
        try:
            response = self.get(f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
            data = response.json()
            properties = data.get("properties", {})
            # If defaultSchema exists in properties, schemas are enabled
            return "defaultSchema" in properties
        except Exception as e:
            logger.warning(
                f"Failed to check if lakehouse {lakehouse_id} has schemas enabled: {e}. "
                "Assuming schemas are disabled."
            )
            return False

    def _paginate_unity_catalog(
        self,
        url: str,
        params: dict[str, str],
        items_key: str,
        description: str,
    ) -> Iterator[dict]:
        """Yield every item from a paginated OneLake Unity Catalog list endpoint.

        The list endpoints return at most one page per call and carry the next
        page in `next_page_token`. The token is sent back as `page_token`.
        Reference: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview

        Args:
            url: Full endpoint URL
            params: Initial query parameters
            items_key: Response key holding the page's items ("schemas" or "tables")
            description: Used in log messages

        Yields:
            Item dictionaries from each page
        """
        headers = {}
        try:
            # Use Storage audience for OneLake Table APIs
            headers["Authorization"] = self.auth_helper.get_authorization_header(
                scope=ONELAKE_STORAGE_SCOPE
            )
        except Exception as e:
            logger.error(f"Failed to get authorization header: {e}")
            raise

        request_params = dict(params)
        request_params["max_results"] = str(ONELAKE_CATALOG_PAGE_SIZE)
        page = 1
        total = 0
        seen_tokens: set[str] = set()
        while True:
            response = self._session.get(
                url, headers=headers, params=request_params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            items = data.get(items_key, []) or []
            total += len(items)
            logger.debug(f"Page {page}: got {len(items)} item(s) for {description}")
            yield from items

            next_token = data.get("next_page_token")
            if not next_token:
                break
            if next_token in seen_tokens:
                # Guard against an endpoint that keeps returning the same token.
                logger.warning(
                    f"Repeated page token while listing {description}; stopping "
                    f"after {page} page(s)."
                )
                break
            seen_tokens.add(next_token)
            request_params["page_token"] = next_token
            page += 1

        logger.info(f"Found {total} item(s) for {description} across {page} page(s)")

    def _list_schemas_via_onelake_api(
        self,
        workspace_id: str,
        item_id: str,
        item_label: str = "lakehouse",
        missing_ok: bool = False,
    ) -> Iterator[str]:
        """List schemas in a lakehouse or warehouse using OneLake Delta Table APIs.

        Reference: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            item_label: Used in log messages
            missing_ok: When True, a 404 yields nothing instead of failing

        Yields:
            Schema names
        """
        url = f"{ONELAKE_TABLE_API_BASE_URL}/delta/{workspace_id}/{item_id}/api/2.1/unity-catalog/schemas"
        params = {"catalog_name": item_id}

        logger.debug(
            f"Listing schemas via OneLake Table API for {item_label} {item_id}"
        )
        try:
            for schema_data in self._paginate_unity_catalog(
                url,
                params,
                items_key="schemas",
                description=f"schemas in {item_label} {item_id}",
            ):
                schema_name = schema_data.get("name", "")
                if schema_name:
                    logger.debug(f"Found schema: {schema_name}")
                    yield schema_name

        except requests.exceptions.HTTPError as e:
            if missing_ok and e.response is not None and e.response.status_code == 404:
                logger.warning(
                    f"{item_label.capitalize()} {item_id} has no OneLake schema catalog "
                    "(404). Tables will not be listed for this item."
                )
                return
            self.report.report_error()
            logger.error(
                f"HTTP error {e.response.status_code} listing schemas for {item_label} {item_id}: {e.response.text}"
            )
            raise
        except Exception as e:
            self.report.report_error()
            logger.error(f"Failed to list schemas for {item_label} {item_id}: {e}")
            raise

    def _list_tables_per_schema_via_onelake_api(
        self,
        workspace_id: str,
        item_id: str,
        schema_name: str,
        item_label: str = "lakehouse",
    ) -> Iterator[FabricTable]:
        """List tables in a specific schema using OneLake Delta Table APIs.

        Reference: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            schema_name: Schema name
            item_label: Used in log messages

        Yields:
            FabricTable objects
        """
        url = f"{ONELAKE_TABLE_API_BASE_URL}/delta/{workspace_id}/{item_id}/api/2.1/unity-catalog/tables"
        params = {"catalog_name": item_id, "schema_name": schema_name}

        logger.debug(
            f"Listing tables in schema {schema_name} via OneLake Table API for {item_label} {item_id}"
        )
        try:
            for table_data in self._paginate_unity_catalog(
                url,
                params,
                items_key="tables",
                description=f"tables in schema {schema_name} of {item_label} {item_id}",
            ):
                table_name = table_data.get("name", "")
                if table_name:
                    logger.debug(f"Processing table: {schema_name}.{table_name}")
                    yield FabricTable(
                        name=table_name,
                        schema_name=schema_name,
                        item_id=item_id,
                        workspace_id=workspace_id,
                        description=table_data.get(
                            "comment"
                        ),  # OneLake API uses "comment" instead of "description"
                    )

        except requests.exceptions.HTTPError as e:
            self.report.report_error()
            logger.error(
                f"HTTP error {e.response.status_code} listing tables in schema {schema_name} for {item_label} {item_id}: {e.response.text}"
            )
            raise
        except Exception as e:
            self.report.report_error()
            logger.error(
                f"Failed to list tables in schema {schema_name} for {item_label} {item_id}: {e}"
            )
            raise

    def list_lakehouse_tables(
        self, workspace_id: str, lakehouse_id: str
    ) -> Iterator[FabricTable]:
        """List all tables in a lakehouse.

        Handles both schemas-enabled and schemas-disabled lakehouses:
        - For schemas-disabled: Uses Fabric REST API /tables endpoint
        - For schemas-enabled: Uses OneLake Delta Table APIs to list schemas, then tables per schema

        References:
        - Fabric REST API: https://learn.microsoft.com/en-us/rest/api/fabric/tables/list
        - OneLake Delta APIs: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview

        Args:
            workspace_id: Workspace GUID
            lakehouse_id: Lakehouse GUID

        Yields:
            FabricTable objects
        """
        logger.info(f"Listing tables for lakehouse {lakehouse_id}")

        # Check if schemas are enabled
        schemas_enabled = self._is_lakehouse_schemas_enabled(workspace_id, lakehouse_id)

        if schemas_enabled:
            logger.info(
                f"Lakehouse {lakehouse_id} has schemas enabled, using OneLake Delta Table APIs"
            )
            # List schemas, then tables per schema
            try:
                for schema_name in self._list_schemas_via_onelake_api(
                    workspace_id, lakehouse_id
                ):
                    yield from self._list_tables_per_schema_via_onelake_api(
                        workspace_id, lakehouse_id, schema_name
                    )
            except requests.exceptions.HTTPError as e:
                # If OneLake APIs fail with 401/403, it might be an authentication/permission issue
                # Log warning and return empty (don't fail the entire ingestion)
                if e.response.status_code in (401, 403):
                    logger.warning(
                        f"OneLake Delta Table APIs require additional permissions or different authentication. "
                        f"Unable to list tables for schemas-enabled lakehouse {lakehouse_id}. "
                        f"Error: {e.response.text}. "
                        f"Please ensure your identity has 'Lakehouse.Read.All' or 'Lakehouse.ReadWrite.All' permissions."
                    )
                    # Return empty iterator instead of raising
                    return
                else:
                    logger.error(
                        f"Failed to list tables for schemas-enabled lakehouse {lakehouse_id}: {e}"
                    )
                    raise
            except Exception as e:
                logger.error(
                    f"Failed to list tables for schemas-enabled lakehouse {lakehouse_id}: {e}"
                )
                raise
        else:
            logger.info(
                f"Lakehouse {lakehouse_id} does not have schemas enabled, using Fabric REST API"
            )
            # Use standard REST API endpoint
            try:
                response = self.get(
                    f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
                )
                data = response.json()

                # For schemas-disabled lakehouses, tables are in "data" key, not "value"
                # Reference: https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables
                tables = data.get("data", []) or data.get("value", [])
                logger.info(f"Found {len(tables)} table(s) in lakehouse {lakehouse_id}")

                for table_data in tables:
                    full_name = table_data.get("name", "")
                    # For schemas-disabled lakehouses, table names don't include schema prefix
                    # Use empty string for schema_name so URN generation can skip it
                    if "." in full_name:
                        schema_name, table_name = full_name.rsplit(".", 1)
                    else:
                        schema_name = ""  # Empty schema for schemas-disabled lakehouses
                        table_name = full_name

                    logger.debug(
                        f"Processing table: {schema_name}.{table_name}"
                        if schema_name
                        else f"Processing table: {table_name}"
                    )

                    yield FabricTable(
                        name=table_name,
                        schema_name=schema_name,
                        item_id=lakehouse_id,
                        workspace_id=workspace_id,
                        description=table_data.get("description"),
                    )

            except requests.exceptions.HTTPError as e:
                # Check if error is due to schemas being enabled (but detection failed)
                if (
                    e.response.status_code == 400
                    and "UnsupportedOperationForSchemasEnabledLakehouse"
                    in e.response.text
                ):
                    logger.warning(
                        f"Lakehouse {lakehouse_id} appears to have schemas enabled (detection failed), "
                        "falling back to OneLake Delta Table APIs"
                    )
                    # Fallback to schema-based approach
                    try:
                        for schema_name in self._list_schemas_via_onelake_api(
                            workspace_id, lakehouse_id
                        ):
                            yield from self._list_tables_per_schema_via_onelake_api(
                                workspace_id, lakehouse_id, schema_name
                            )
                    except Exception as fallback_error:
                        logger.error(
                            f"Failed to list tables using fallback method for lakehouse {lakehouse_id}: {fallback_error}"
                        )
                        raise
                else:
                    self.report.report_error()
                    logger.error(
                        f"HTTP error {e.response.status_code} listing tables for lakehouse {lakehouse_id}: {e.response.text}"
                    )
                    raise
            except Exception as e:
                self.report.report_error()
                logger.error(f"Failed to list tables for lakehouse {lakehouse_id}: {e}")
                raise

    def list_warehouse_tables(
        self, workspace_id: str, warehouse_id: str
    ) -> Iterator[FabricTable]:
        """List all tables in a warehouse via the OneLake Delta Table APIs.

        The Fabric REST `warehouses/{id}/tables` endpoint returns 404 for many
        warehouses, which previously dropped every table that had not shown up
        in query history. Warehouses are schema-enabled, so the same Unity
        Catalog list used for schemas-enabled lakehouses applies here.

        System schemas (`sys`, `INFORMATION_SCHEMA`, `queryinsights`) are
        skipped. A 404 means the item has no OneLake catalog (for example a
        staging warehouse) and yields nothing.

        Reference: https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview

        Args:
            workspace_id: Workspace GUID
            warehouse_id: Warehouse GUID

        Yields:
            FabricTable objects
        """
        logger.info(f"Listing tables for warehouse {warehouse_id}")
        system_schemas = {name.lower() for name in FABRIC_SYSTEM_SCHEMAS}
        try:
            for schema_name in self._list_schemas_via_onelake_api(
                workspace_id,
                warehouse_id,
                item_label="warehouse",
                missing_ok=True,
            ):
                if schema_name.lower() in system_schemas:
                    logger.debug(
                        f"Skipping system schema {schema_name} in warehouse {warehouse_id}"
                    )
                    continue
                yield from self._list_tables_per_schema_via_onelake_api(
                    workspace_id,
                    warehouse_id,
                    schema_name,
                    item_label="warehouse",
                )
        except Exception as e:
            logger.error(f"Failed to list tables for warehouse {warehouse_id}: {e}")
            raise
