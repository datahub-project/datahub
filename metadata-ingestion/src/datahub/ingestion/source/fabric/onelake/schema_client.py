"""SQL Analytics Endpoint client for schema extraction from Fabric OneLake."""

import logging
import re
import struct
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import Literal, assert_never

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.common.base_client import BaseFabricClient

from datahub.ingestion.source.fabric.common.auth import (
    SQL_ANALYTICS_ENDPOINT_SCOPE,
    FabricAuthHelper,
)
from datahub.ingestion.source.fabric.onelake.config import SqlEndpointConfig
from datahub.ingestion.source.fabric.onelake.models import FabricColumn
from datahub.ingestion.source.fabric.onelake.schema_report import (
    SqlAnalyticsEndpointReport,
)

logger = logging.getLogger(__name__)

# SQL Server connection attribute for Azure AD access token
# Attribute ID: 1256 is the numeric identifier for the "Access Token" connection option
# in SQL Server ODBC drivers.
# Purpose: Enables passwordless authentication using Azure AD tokens (OAuth 2.0 bearer tokens).
# Used with: pyodbc.connect() via the attrs_before parameter.
SQL_COPT_SS_ACCESS_TOKEN = 1256

# Pattern to match Fabric SQL Analytics Endpoint hostname in connection strings
_FABRIC_ENDPOINT_HOST_PATTERN = re.compile(
    r"[a-zA-Z0-9_-]+\.datawarehouse\.fabric\.microsoft\.com"
)


def _extract_endpoint_host_from_connection_string(conn_str: str) -> Optional[str]:
    """Extract Fabric SQL Analytics Endpoint hostname from a connection string.

    Handles: bare hostname, Server=host;..., Data Source=host;...
    """
    s = conn_str.strip()
    if not s or ".datawarehouse.fabric.microsoft.com" not in s:
        return None
    match = _FABRIC_ENDPOINT_HOST_PATTERN.search(s)
    if match:
        return match.group(0)
    return None


class SchemaExtractionClient(Protocol):
    """Protocol for schema extraction clients.

    This protocol allows for future implementation of alternative schema extraction
    methods (e.g., OneLake Delta Table APIs, Delta Log parsing) while maintaining
    a consistent interface.
    """

    def get_all_table_columns(
        self,
        workspace_id: str,
        item_id: str,
    ) -> Dict[Tuple[str, str], List[FabricColumn]]:
        """Get column metadata for all tables in a workspace/item.

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID

        Returns:
            Dictionary mapping (schema_name, table_name) to list of FabricColumn objects
        """
        ...


def create_schema_extraction_client(
    method: Literal["sql_analytics_endpoint"],
    auth_helper: FabricAuthHelper,
    config: "SqlEndpointConfig",
    report: Optional[SqlAnalyticsEndpointReport],
    workspace_id: str,
    item_id: str,
    item_type: Literal["Lakehouse", "Warehouse"],
    base_client: "BaseFabricClient",
    item_display_name: str,
) -> SchemaExtractionClient:
    """Factory method to create schema extraction client based on method.

    For SQL Analytics Endpoint method, this fetches the endpoint URL and creates
    a client configured for the specific workspace/item.

    Args:
        method: Schema extraction method (currently only "sql_analytics_endpoint" is supported)
        auth_helper: Fabric authentication helper
        config: SQL endpoint configuration
        report: Optional report for tracking metrics
        workspace_id: Workspace GUID (required for sql_analytics_endpoint)
        item_id: Lakehouse or Warehouse GUID (required for sql_analytics_endpoint)
        item_type: "Lakehouse" or "Warehouse" (required for sql_analytics_endpoint)
        base_client: Base Fabric client for making API calls (required for sql_analytics_endpoint)
        item_display_name: Display name of the lakehouse/warehouse; used as Database=
            in the ODBC connection string (Fabric expects name, not GUID). Required.

    Returns:
        Schema extraction client instance configured for the workspace/item

    Raises:
        ValueError: If method is not supported or required parameters are missing
    """
    if method == "sql_analytics_endpoint":
        try:
            # Fetch endpoint URL for this item
            endpoint_url = SqlAnalyticsEndpointClient.get_sql_analytics_endpoint_url(
                base_client, workspace_id, item_id, item_type
            )
            if not endpoint_url:
                raise ValueError(
                    f"SQL Analytics Endpoint URL is required for {item_type} {item_id}. "
                    "The endpoint URL must be obtained from the Fabric API and cannot be constructed. "
                    "See: https://learn.microsoft.com/en-us/fabric/data-warehouse/what-is-the-sql-analytics-endpoint-for-a-lakehouse"
                )

            logger.info(
                f"Using SQL Analytics Endpoint URL for {item_type} {item_id}: {endpoint_url}"
            )

            return SqlAnalyticsEndpointClient(
                auth_helper=auth_helper,
                config=config,
                endpoint_url=endpoint_url,  # Pre-configured with endpoint URL
                report=report,
                item_display_name=item_display_name,
            )
        except Exception:
            # Track failure in report if available
            if report is not None:
                report.failures += 1
            # Re-raise the exception
            raise
    else:
        assert_never(method)


class SqlAnalyticsEndpointClient:
    """Client for querying SQL Analytics Endpoint to extract table schemas.

    Uses SQLAlchemy with pyodbc driver and Azure AD token injection for authentication.
    """

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        config: SqlEndpointConfig,
        endpoint_url: str,
        item_display_name: str,
        report: Optional[SqlAnalyticsEndpointReport] = None,
    ):
        """Initialize SQL Analytics Endpoint client.

        Args:
            auth_helper: Fabric authentication helper for token acquisition
            config: SQL endpoint configuration
            endpoint_url: Optional pre-fetched endpoint URL. If provided, will be used
                for all connections. If None, will use fallback server per connection.
            item_display_name: Display name of the lakehouse/warehouse; used as Database=
                in the ODBC connection string (Fabric expects name, not GUID). Required
                for successful connection; callers must pass the item's display name.
            report: Optional report for tracking metrics
        """
        self.auth_helper = auth_helper
        self.config = config
        self.report = report or SqlAnalyticsEndpointReport()
        self.endpoint_url = endpoint_url
        self.item_display_name = item_display_name.strip()
        # Engine cache: key is (workspace_id, item_id, endpoint_url) tuple
        # Caches SQLAlchemy engines per workspace/item/endpoint combination for efficiency
        self._engines: Dict[Tuple[str, str, Optional[str]], Engine] = {}  # type: ignore

    @classmethod
    def get_sql_analytics_endpoint_url(
        cls,
        base_client: "BaseFabricClient",
        workspace_id: str,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
    ) -> Optional[str]:
        """Get the SQL Analytics Endpoint URL for a given item.

        The endpoint URL is found in properties.sqlEndpointProperties.connectionString
        as a direct hostname (e.g., "xxx.datawarehouse.fabric.microsoft.com").

        Each Lakehouse/Warehouse has its own unique endpoint identifier that cannot be
        predicted from workspace_id alone. The endpoint must be obtained from the API.

        References:
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/what-is-the-sql-analytics-endpoint-for-a-lakehouse
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-connectivity
        - https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/get-lakehouse
        - https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items/get-warehouse

        Args:
            base_client: Base Fabric client for making API calls
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            item_type: "Lakehouse" or "Warehouse"

        Returns:
            SQL Analytics Endpoint URL if available, None otherwise
        """
        try:
            endpoint_path = (
                f"workspaces/{workspace_id}/lakehouses/{item_id}"
                if item_type == "Lakehouse"
                else f"workspaces/{workspace_id}/warehouses/{item_id}"
            )
            response = base_client.get(endpoint_path)
            data = response.json()

            properties = data.get("properties", {})
            sql_endpoint_props = properties.get("sqlEndpointProperties")

            if isinstance(sql_endpoint_props, dict):
                # Lakehouse: connectionString is often just the hostname
                conn_str = sql_endpoint_props.get("connectionString")
                if conn_str and isinstance(conn_str, str):
                    if ".datawarehouse.fabric.microsoft.com" in conn_str:
                        return conn_str.strip()

            # Warehouse API may expose connectionString at properties level
            # (e.g. staging warehouses: properties=['connectionInfo', 'connectionString', ...])
            conn_str = properties.get("connectionString")
            if conn_str and isinstance(conn_str, str):
                url = _extract_endpoint_host_from_connection_string(conn_str)
                if url:
                    return url
            connection_info = properties.get("connectionInfo")
            if isinstance(connection_info, dict):
                conn_str = connection_info.get("connectionString")
                if conn_str and isinstance(conn_str, str):
                    url = _extract_endpoint_host_from_connection_string(conn_str)
                    if url:
                        return url

            logger.warning(
                f"No SQL Analytics Endpoint URL found for {item_type} {item_id}. "
                f"Response structure: properties={list(properties.keys())[:10]}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Failed to get SQL Analytics Endpoint URL for {item_type} {item_id}: {e}"
            )
            return None

    def get_table_columns(
        self,
        workspace_id: str,
        item_id: str,
        schema_name: str,
        table_name: str,
    ) -> List[FabricColumn]:
        """Get column metadata for a single table.

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of FabricColumn objects
        """
        if not self.endpoint_url:
            raise ValueError(
                f"SQL Analytics Endpoint URL is required for item {item_id}. "
                "The endpoint URL must be obtained from the Fabric API and cannot be constructed. "
                "See: https://learn.microsoft.com/en-us/fabric/data-warehouse/what-is-the-sql-analytics-endpoint-for-a-lakehouse"
            )

        try:
            assert self.endpoint_url is not None
            engine = self._get_engine(workspace_id, item_id, self.endpoint_url)
            query = text(
                """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    ORDINAL_POSITION,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
                """
            )

            with engine.connect() as connection:
                result = connection.execute(
                    query,
                    {"schema_name": schema_name, "table_name": table_name},
                )
                columns = []
                for row in result:
                    columns.append(
                        FabricColumn(
                            name=row.COLUMN_NAME,
                            data_type=row.DATA_TYPE,  # Keep as returned (uppercase from INFORMATION_SCHEMA)
                            is_nullable=row.IS_NULLABLE == "YES",
                            ordinal_position=row.ORDINAL_POSITION,
                            description=None,  # INFORMATION_SCHEMA doesn't provide descriptions
                        )
                    )
                return columns
        except SQLAlchemyError as e:
            logger.warning(
                f"Failed to extract schema for {schema_name}.{table_name} "
                f"in workspace {workspace_id}, item {item_id}: {e}"
            )
            if self.report:
                self.report.failures += 1
            return []
        except Exception as e:
            logger.error(
                f"Unexpected error extracting schema for {schema_name}.{table_name}: {e}",
                exc_info=True,
            )
            if self.report:
                self.report.failures += 1
            return []

    def get_all_table_columns(
        self,
        workspace_id: str,
        item_id: str,
    ) -> Dict[Tuple[str, str], List[FabricColumn]]:
        """Get column metadata for all tables in a workspace/item.

        Fetches schema for all tables in a single query for efficiency.
        Uses the endpoint_url configured during client initialization.

        References:
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-connectivity
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/connect-to-fabric-data-warehouse

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID

        Returns:
            Dictionary mapping (schema_name, table_name) to list of FabricColumn objects
        """

        try:
            engine = self._get_engine(workspace_id, item_id, self.endpoint_url)

            # Query all columns for all tables in the database
            query = text(
                """
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    ORDINAL_POSITION,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """
            )

            with engine.connect() as connection:
                result = connection.execute(query)
                columns_by_table: Dict[Tuple[str, str], List[FabricColumn]] = {}
                for row in result:
                    # INFORMATION_SCHEMA returns uppercase, but we'll lowercase for consistency
                    # with type mapping (resolve_sql_type handles case-insensitive lookup)
                    schema_name = row.TABLE_SCHEMA
                    table_name = row.TABLE_NAME
                    key = (schema_name, table_name)
                    if key not in columns_by_table:
                        columns_by_table[key] = []
                    columns_by_table[key].append(
                        FabricColumn(
                            name=row.COLUMN_NAME,
                            data_type=row.DATA_TYPE,  # Keep as returned (uppercase)
                            is_nullable=row.IS_NULLABLE == "YES",
                            ordinal_position=row.ORDINAL_POSITION,
                            description=None,
                        )
                    )
                return columns_by_table
        except SQLAlchemyError as e:
            error_msg = str(e)
            logger.warning(
                f"Failed to extract schema for all tables "
                f"in workspace {workspace_id}, item {item_id}: {error_msg}"
            )
            if self.report:
                self.report.failures += 1
            # Re-raise the exception
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error extracting schema for all tables: {e}",
                exc_info=True,
            )
            if self.report:
                self.report.failures += 1
            # Re-raise the exception
            raise

    def _get_engine(self, workspace_id: str, item_id: str, endpoint_url: str) -> Engine:
        """Get or create SQLAlchemy engine for a workspace/item combination.

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            endpoint_url: SQL Analytics Endpoint URL (required)

        Returns:
            SQLAlchemy Engine instance
        """
        key = (workspace_id, item_id, endpoint_url)
        if key not in self._engines:
            self._engines[key] = self._create_engine(
                workspace_id, item_id, endpoint_url
            )
        return self._engines[key]

    def _create_engine(
        self, workspace_id: str, item_id: str, endpoint_url: str
    ) -> Engine:
        """Create SQLAlchemy engine with Azure AD token injection.

        References:
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/connect-to-fabric-data-warehouse

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            endpoint_url: SQL Analytics Endpoint URL (required, must be obtained from API)

        Returns:
            SQLAlchemy Engine instance
        """
        connection_string = self._get_connection_string(
            workspace_id, item_id, endpoint_url
        )

        logger.info(
            f"Creating SQL Analytics Endpoint connection: "
            f"workspace_id={workspace_id}, item_id={item_id}, "
            f"server={endpoint_url}, connection_string={connection_string}"
        )

        # Get token before creating engine (needed for attrs_before)
        # SQL Analytics Endpoint requires database scope for Azure AD authentication
        token = self.auth_helper.get_bearer_token(scope=SQL_ANALYTICS_ENDPOINT_SCOPE)

        # Convert token to bytes format required by pyodbc
        # Token needs to be encoded as UTF-16-LE and packed with length prefix
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s", len(token_bytes), token_bytes
        )

        # Create a custom connection creator that passes attrs_before to pyodbc
        # SQLAlchemy doesn't support attrs_before directly, so the code uses a custom
        # connection creator that passes the token to pyodbc before the connection is
        # established. This enables Azure AD authentication for the Fabric SQL Analytics Endpoint.
        import pyodbc as pyodbc_module

        def connect_with_token():
            """Custom connection creator that injects Azure AD token via attrs_before.

            This function is passed to SQLAlchemy's create_engine() as the 'creator'
            parameter. It wraps pyodbc.connect() and injects the Azure AD access token
            using the attrs_before parameter with SQL_COPT_SS_ACCESS_TOKEN (1256).
            This enables passwordless authentication with the Fabric SQL Analytics Endpoint.
            """
            return pyodbc_module.connect(
                connection_string,
                timeout=self.config.query_timeout,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
            )

        # Create engine with custom connection creator
        # Use connection pooling for efficiency
        engine = create_engine(
            "mssql+pyodbc://",
            creator=connect_with_token,
            pool_pre_ping=True,  # Verify connections before using
            pool_recycle=3600,  # Recycle connections after 1 hour
        )

        logger.info(
            "Successfully configured SQL Analytics Endpoint connection with token"
        )

        return engine

    def _get_connection_string(
        self, workspace_id: str, item_id: str, endpoint_url: str
    ) -> str:
        """Build ODBC connection string from individual config options.

        The endpoint URL format is: <unique-identifier>.datawarehouse.fabric.microsoft.com
        This must be obtained from the Fabric API and cannot be constructed.

        Fabric SQL Analytics Endpoint expects Database= to be the lakehouse/warehouse
        display name, not the GUID. Using the GUID causes "database was not found".

        References:
        - https://learn.microsoft.com/en-us/fabric/data-warehouse/connect-to-fabric-data-warehouse

        Args:
            workspace_id: Workspace GUID
            item_id: Lakehouse or Warehouse GUID
            endpoint_url: SQL Analytics Endpoint URL (required, must be obtained from API)

        Returns:
            ODBC connection string
        """
        parts = [
            f"Driver={{{self.config.odbc_driver}}};",
            f"Server={endpoint_url};",
            f"Database={self.item_display_name};",
            f"Encrypt={self.config.encrypt};",
            f"TrustServerCertificate={self.config.trust_server_certificate};",
        ]
        return "".join(parts)

    def close(self) -> None:
        """Close all engine connections."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
