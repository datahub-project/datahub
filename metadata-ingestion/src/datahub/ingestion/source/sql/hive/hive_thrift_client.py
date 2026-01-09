"""
Thrift client for Hive Metastore with Kerberos/SASL authentication support.

This module provides a wrapper around pymetastore's ThriftHiveMetastore client
with support for Kerberos authentication via SASL/GSSAPI, including hostname
override for environments where the connection hostname differs from the
Kerberos principal hostname (e.g., when connecting through a load balancer).

NOTE: Retry Logic Implementation
--------------------------------
This module uses `tenacity` for retry logic, which is consistent with other
DataHub connectors (slack, mode). Tenacity is included as an extra dependency.
Install with: pip install 'acryl-datahub[hive-metastore-thrift]'
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace, TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
)

from pymetastore.hive_metastore import ThriftHiveMetastore
from pymetastore.hive_metastore.ttypes import GetTableRequest
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TException
from thrift.transport import TSocket, TTransport

if TYPE_CHECKING:
    from thrift_sasl import TSaslClientTransport

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Hive table type for views
VIRTUAL_VIEW_TYPE = "VIRTUAL_VIEW"


# =============================================================================
# Dataclasses for failure tracking keys
# =============================================================================


@dataclass(frozen=True)
class DatabaseKey:
    """Key for tracking database-level failures."""

    catalog_name: Optional[str]
    db_name: str


@dataclass(frozen=True)
class TableKey:
    """Key for tracking table-level failures and caching."""

    catalog_name: Optional[str]
    db_name: str
    table_name: str


# Exceptions that indicate a catalog API is not supported (HMS 2.x)
# These should trigger fallback to non-catalog APIs.
#
# FALLBACK STRATEGY: When a catalog-aware API fails with one of these exceptions,
# we fall back to the non-catalog HMS 2.x API.
#
# IMPORTANT: We intentionally do NOT catch TTransportException here because:
# 1. Network errors should propagate so users know there's a connectivity issue
# 2. Silently falling back on network errors could mask real problems
# 3. Only "method not found" type errors should trigger fallback
_CATALOG_NOT_SUPPORTED_EXCEPTIONS = (
    AttributeError,  # Method doesn't exist on client
    TException,  # Thrift "method not found" or protocol errors (but not transport errors)
)

# More specific exception for when we need to distinguish API not found vs other errors
_CATALOG_API_NOT_FOUND_EXCEPTIONS = (
    AttributeError,  # Method doesn't exist on client
)


class HMSConnectionError(Exception):
    """Custom exception for HMS connection errors with helpful messages."""

    pass


def _wrap_thrift_error(e: Exception, use_kerberos: bool) -> Exception:
    """
    Wrap Thrift errors with more helpful messages.

    Args:
        e: The original exception
        use_kerberos: Whether Kerberos auth is enabled in config

    Returns:
        A more descriptive exception or the original exception
    """
    error_str = str(e).lower()

    # Check for auth-related connection errors
    is_auth_error = (
        isinstance(e, BrokenPipeError)
        or "broken pipe" in error_str
        or "read 0 bytes" in error_str  # EOF - server closed connection
        or "end of file" in error_str
        or "connection reset" in error_str
    )

    if is_auth_error:
        if not use_kerberos:
            return HMSConnectionError(
                f"Connection closed by HMS server. "
                f"This typically means the Hive Metastore requires SASL/Kerberos authentication, "
                f"but 'use_kerberos' is set to False. "
                f"Try setting 'use_kerberos: true' and configuring 'kerberos_service_name'. "
                f"Original error: {e}"
            )
        else:
            return HMSConnectionError(
                f"Connection closed by HMS server. "
                f"Kerberos is enabled but the connection failed. Check that: "
                f"1) You have a valid Kerberos ticket (run 'klist' to verify) "
                f"2) The 'kerberos_service_name' matches the HMS principal "
                f"3) Network connectivity to the HMS server is stable. "
                f"Original error: {e}"
            )

    return e


class ThriftInspectorAdapter:
    """
    Minimal adapter that provides an Inspector-like interface for HiveMetastoreSource.

    HiveMetastoreSource uses Inspector primarily to extract the database name
    via `inspector.engine.url.database`. This adapter provides just enough
    structure to satisfy that interface.
    """

    def __init__(self, database: str):
        """
        Create an adapter that mimics SQLAlchemy Inspector for database name access.

        Args:
            database: The database/catalog name to return
        """
        # Create a fake engine.url.database structure that HiveMetastoreSource expects
        self.engine = SimpleNamespace(url=SimpleNamespace(database=database))


@dataclass
class ThriftConnectionConfig:
    """Configuration for HMS Thrift client connection."""

    host: str
    port: int = 9083
    use_kerberos: bool = True
    kerberos_service_name: str = "hive"
    kerberos_hostname_override: Optional[str] = None
    timeout_seconds: int = 60
    max_retries: int = (
        3  # Uses exponential backoff internally (multiplier=2, max_wait=30s)
    )


class HiveMetastoreThriftClient:
    """
    Client for connecting to Hive Metastore via Thrift protocol.

    Supports both plain and Kerberized (SASL/GSSAPI) connections with
    automatic retry logic for transient failures.
    """

    def __init__(self, config: ThriftConnectionConfig):
        self.config = config
        self._transport: Optional[TTransport.TBufferedTransport] = None
        self._client: Optional[ThriftHiveMetastore.Client] = None
        # Track failures for reporting - use dicts to avoid duplicates
        # (both iter_table_rows and iter_table_properties_rows call _iter_tables_and_views)
        self._database_failures: Dict[DatabaseKey, str] = {}
        self._table_failures: Dict[TableKey, str] = {}
        # Cache table info to avoid duplicate API calls
        self._table_cache: Dict[TableKey, Dict[str, Any]] = {}

    def get_database_failures(self) -> List[tuple[str, str]]:
        """Get list of database failures as (db_name, error_message) tuples.

        Note: For multi-catalog scenarios, the db_name may be prefixed with catalog.
        """
        result: List[tuple[str, str]] = []
        for key, err in self._database_failures.items():
            # Include catalog in db_name for clarity if present
            display_name = (
                f"{key.catalog_name}.{key.db_name}" if key.catalog_name else key.db_name
            )
            result.append((display_name, err))
        return result

    def get_table_failures(self) -> List[tuple[str, str, str]]:
        """Get list of table failures as (db_name, table_name, error_message) tuples.

        Note: For multi-catalog scenarios, the db_name may be prefixed with catalog.
        """
        result: List[tuple[str, str, str]] = []
        for key, err in self._table_failures.items():
            # Include catalog in db_name for clarity if present
            display_name = (
                f"{key.catalog_name}.{key.db_name}" if key.catalog_name else key.db_name
            )
            result.append((display_name, key.table_name, err))
        return result

    def clear_failures(self) -> None:
        """Clear tracked failures and table cache."""
        self._database_failures.clear()
        self._table_failures.clear()
        self._table_cache.clear()

    def _create_sasl_transport(self, socket: TSocket.TSocket) -> "TSaslClientTransport":
        """Create SASL transport for Kerberos authentication."""
        try:
            from pyhive.sasl_compat import PureSASLClient
            from thrift_sasl import TSaslClientTransport
        except ImportError as e:
            raise ImportError(
                "Kerberos authentication requires 'thrift-sasl' and 'acryl-pyhive[hive-pure-sasl]'. "
                f"Install with: pip install thrift-sasl acryl-pyhive[hive-pure-sasl]. Error: {e}"
            ) from e

        # Use hostname override if provided, otherwise use connection host
        # This handles the case where the Kerberos principal hostname differs
        # from the actual connection hostname (e.g., load balancer scenarios)
        sasl_host = self.config.kerberos_hostname_override or self.config.host

        logger.debug(
            f"Setting up SASL/GSSAPI transport with service={self.config.kerberos_service_name}, "
            f"host={sasl_host} (connection host={self.config.host})"
        )

        def sasl_factory() -> PureSASLClient:
            return PureSASLClient(
                sasl_host,
                service=self.config.kerberos_service_name,
                mechanism="GSSAPI",
            )

        return TSaslClientTransport(sasl_factory, "GSSAPI", socket)

    def connect(self) -> None:
        """Establish connection to HMS."""
        logger.info(f"Connecting to HMS at {self.config.host}:{self.config.port}")

        socket = TSocket.TSocket(self.config.host, self.config.port)
        socket.setTimeout(self.config.timeout_seconds * 1000)

        if self.config.use_kerberos:
            self._transport = self._create_sasl_transport(socket)
        else:
            self._transport = TTransport.TBufferedTransport(socket)

        self._transport.open()

        protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = ThriftHiveMetastore.Client(protocol)

        logger.info("Successfully connected to HMS")

    def close(self) -> None:
        """Close the connection."""
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception as e:
                logger.warning(f"Error closing transport: {e}")
            self._transport = None
            self._client = None

    def __enter__(self) -> "HiveMetastoreThriftClient":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    @property
    def client(self) -> "ThriftHiveMetastore.Client":
        """Get the underlying Thrift client."""
        if self._client is None:
            raise RuntimeError("Client not connected. Call connect() first.")
        return self._client

    def _get_retry_decorator(
        self,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Create a retry decorator with exponential backoff.

        Note: BrokenPipeError is NOT retried as it typically indicates
        an authentication mismatch (e.g., HMS requires SASL but client
        didn't use it). Such errors are wrapped with helpful messages.
        """

        def should_retry(exception: BaseException) -> bool:
            """Determine if an exception should trigger a retry."""
            # Don't retry on BrokenPipeError - it's not transient
            if isinstance(exception, BrokenPipeError):
                return False
            # Check for broken pipe in nested exceptions
            if hasattr(exception, "inner") and isinstance(
                exception.inner, BrokenPipeError
            ):
                return False
            # Retry on transport/connection errors
            return isinstance(
                exception,
                (TTransport.TTransportException, ConnectionError, TimeoutError),
            )

        return retry(
            retry=retry_if_exception(should_retry),
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(multiplier=2, max=30),  # Sensible defaults
            reraise=True,
        )

    def _call_with_error_handling(
        self, func: Callable[[], Any], operation_name: str = "HMS operation"
    ) -> Any:
        """
        Call a function with error handling and helpful error messages.

        Args:
            func: The function to call
            operation_name: Description of the operation for error messages

        Returns:
            The result of the function call

        Raises:
            HMSConnectionError: With helpful message for connection issues
            Other exceptions: Re-raised with original context
        """
        try:
            return func()
        except Exception as e:
            # Check if this looks like an auth-related connection error
            error_str = str(e).lower()
            is_auth_error = (
                isinstance(e, BrokenPipeError)
                or "broken pipe" in error_str
                or "read 0 bytes" in error_str
                or "end of file" in error_str
                or "connection reset" in error_str
            )

            if is_auth_error:
                raise _wrap_thrift_error(e, self.config.use_kerberos) from e

            # Check for BrokenPipeError in the exception chain
            current: Optional[BaseException] = e
            while current is not None:
                if isinstance(current, BrokenPipeError):
                    raise _wrap_thrift_error(current, self.config.use_kerberos) from e
                # Check TTransportException's inner attribute
                if hasattr(current, "inner"):
                    inner = getattr(current, "inner", None)
                    inner_str = str(inner).lower() if inner else ""
                    if (
                        isinstance(inner, BrokenPipeError)
                        or "read 0 bytes" in inner_str
                    ):
                        # inner is a known Exception type here
                        error_to_wrap: Exception = (
                            inner if isinstance(inner, Exception) else e
                        )
                        raise _wrap_thrift_error(
                            error_to_wrap,
                            self.config.use_kerberos,
                        ) from e
                    current = inner
                else:
                    current = current.__cause__
            # Re-raise original exception if not a wrapped error type
            raise

    # -------------------------------------------------------------------------
    # HMS API Methods - return data in dict format matching hive_metastore.py
    # With retry logic for transient failures
    # -------------------------------------------------------------------------

    def supports_catalogs(self) -> bool:
        """
        Check if the HMS server supports catalogs (HMS 3.x feature).

        Returns True if get_catalogs() returns a non-empty list,
        False otherwise (HMS 2.x or catalog feature disabled).

        Note: Only catches AttributeError (method doesn't exist on HMS 2.x).
        Network/auth errors will propagate up to caller.
        """
        try:
            catalogs = self.get_catalogs()
            return len(catalogs) > 0
        except AttributeError:
            # HMS 2.x doesn't have get_catalogs method
            logger.debug("HMS 2.x detected - catalogs not supported")
            return False

    def get_all_databases(self, catalog_name: Optional[str] = None) -> List[str]:
        """
        Get all database names with retry logic.

        Args:
            catalog_name: Optional catalog name for HMS 3.x. If provided and HMS
                          supports catalogs, uses catalog-aware pattern syntax.

        For HMS 3.x, databases in a specific catalog can be listed using the
        pattern syntax: @{catalog_name}# (e.g., @spark_catalog#)
        """

        @self._get_retry_decorator()
        def _call() -> List[str]:
            if catalog_name:
                try:
                    # HMS 3.x uses pattern @{catalog_name}# to list databases in a catalog
                    pattern = f"@{catalog_name}#"
                    result = self.client.get_databases(pattern)
                    return list(result) if result else []
                except _CATALOG_NOT_SUPPORTED_EXCEPTIONS as e:
                    logger.debug(
                        f"Catalog-aware get_databases failed for catalog '{catalog_name}', "
                        f"falling back to default: {e}"
                    )
                    return self.client.get_all_databases()
            return self.client.get_all_databases()

        return self._call_with_error_handling(_call, "get_all_databases")

    def get_database(
        self, db_name: str, catalog_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get database metadata with retry logic."""

        @self._get_retry_decorator()
        def _call() -> Dict[str, Any]:
            if catalog_name:
                try:
                    # HMS 3.x API with catalog support
                    db = self.client.get_database_req(
                        {"name": db_name, "catalogName": catalog_name}
                    )
                except _CATALOG_NOT_SUPPORTED_EXCEPTIONS as e:
                    logger.debug(
                        f"Catalog-aware get_database failed, falling back: {e}"
                    )
                    db = self.client.get_database(db_name)
            else:
                db = self.client.get_database(db_name)
            return {
                "name": db.name,
                "description": db.description,
                "location": db.locationUri,
                "parameters": dict(db.parameters) if db.parameters else {},
            }

        return _call()

    def get_all_tables(
        self, db_name: str, catalog_name: Optional[str] = None
    ) -> List[str]:
        """
        Get all table names in a database with retry logic.

        Args:
            db_name: Database name
            catalog_name: Optional catalog name for HMS 3.x

        For HMS 3.x with catalog support, uses the pattern syntax
        @{catalog_name}#{db_name} to specify the catalog context.
        Falls back to standard get_all_tables() for HMS 2.x.
        """

        @self._get_retry_decorator()
        def _call() -> List[str]:
            if catalog_name:
                try:
                    # HMS 3.x uses pattern @{catalog}#{db} to list tables in a catalog
                    pattern = f"@{catalog_name}#{db_name}"
                    result = self.client.get_all_tables(pattern)
                    return list(result) if result else []
                except _CATALOG_NOT_SUPPORTED_EXCEPTIONS as e:
                    logger.debug(
                        f"Catalog-aware get_all_tables failed for catalog '{catalog_name}', "
                        f"falling back to default: {e}"
                    )
                    return self.client.get_all_tables(db_name)
            return self.client.get_all_tables(db_name)

        return _call()

    def get_table(
        self, db_name: str, table_name: str, catalog_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get table metadata in a format compatible with hive_metastore.py.
        Includes retry logic for transient failures.

        Args:
            db_name: Database name
            table_name: Table name
            catalog_name: Optional catalog name for HMS 3.x
        """

        @self._get_retry_decorator()
        def _call() -> Dict[str, Any]:
            if catalog_name:
                try:
                    # HMS 3.x API - get_table_req with catalog
                    req = GetTableRequest(
                        catName=catalog_name, dbName=db_name, tblName=table_name
                    )
                    response = self.client.get_table_req(req)
                    table = response.table
                except _CATALOG_NOT_SUPPORTED_EXCEPTIONS as e:
                    logger.debug(
                        f"Catalog-aware get_table failed for catalog '{catalog_name}', "
                        f"falling back: {e}"
                    )
                    table = self.client.get_table(db_name, table_name)
            else:
                table = self.client.get_table(db_name, table_name)

            # Get table location from storage descriptor
            table_location = ""
            if table.sd and table.sd.location:
                table_location = table.sd.location

            # Extract table parameters
            parameters = dict(table.parameters) if table.parameters else {}

            return {
                "db_name": db_name,
                "table_name": table.tableName,
                "table_type": table.tableType,
                "create_time": table.createTime,
                "location": table_location,
                "parameters": parameters,
                "view_original_text": table.viewOriginalText,
                "view_expanded_text": table.viewExpandedText,
                "partition_keys": [
                    {
                        "col_name": pk.name,
                        "col_type": pk.type,
                        "col_description": pk.comment or "",
                    }
                    for pk in (table.partitionKeys or [])
                ],
            }

        return _call()

    def get_fields(
        self, db_name: str, table_name: str, catalog_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get column/field metadata for a table with retry logic.

        Args:
            db_name: Database name
            table_name: Table name
            catalog_name: Optional catalog name for HMS 3.x

        Note: The Thrift get_fields() API doesn't have a catalog-aware variant.
        For HMS 3.x with catalog context, we extract columns from the table's
        StorageDescriptor (fetched via get_table_req with catalog).
        """

        @self._get_retry_decorator()
        def _call() -> List[Dict[str, Any]]:
            if catalog_name:
                # For HMS 3.x with catalog: get columns from table's StorageDescriptor
                # This ensures we get columns from the correct catalog context
                try:
                    req = GetTableRequest(
                        catName=catalog_name, dbName=db_name, tblName=table_name
                    )
                    response = self.client.get_table_req(req)
                    table = response.table

                    # Extract columns from StorageDescriptor
                    if table.sd and table.sd.cols:
                        return [
                            {
                                "col_name": col.name,
                                "col_type": col.type,
                                "col_description": col.comment or "",
                            }
                            for col in table.sd.cols
                        ]
                    return []
                except _CATALOG_API_NOT_FOUND_EXCEPTIONS as e:
                    # API doesn't exist - fall back to standard get_fields
                    logger.debug(
                        f"Catalog-aware get_table_req not available for fields, "
                        f"falling back: {e}"
                    )
                    fields = self.client.get_fields(db_name, table_name)
                    return [
                        {
                            "col_name": field.name,
                            "col_type": field.type,
                            "col_description": field.comment or "",
                        }
                        for field in fields
                    ]
                except TException as e:
                    # Check if it's an unsupported method error
                    error_str = str(e).lower()
                    if "unknown" in error_str or "invalid" in error_str:
                        logger.debug(
                            f"Catalog-aware API failed, falling back to get_fields: {e}"
                        )
                        fields = self.client.get_fields(db_name, table_name)
                        return [
                            {
                                "col_name": field.name,
                                "col_type": field.type,
                                "col_description": field.comment or "",
                            }
                            for field in fields
                        ]
                    raise

            # Standard HMS 2.x API
            fields = self.client.get_fields(db_name, table_name)
            return [
                {
                    "col_name": field.name,
                    "col_type": field.type,
                    "col_description": field.comment or "",
                }
                for field in fields
            ]

        return _call()

    def get_table_with_columns(
        self, db_name: str, table_name: str, catalog_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get table metadata including columns in the format expected by
        hive_metastore.py's WorkUnit generation.

        Args:
            db_name: Database name
            table_name: Table name
            catalog_name: Optional catalog name for HMS 3.x
        """
        table_info = self.get_table(db_name, table_name, catalog_name)
        fields = self.get_fields(db_name, table_name, catalog_name)

        # Build column list with table-level info attached
        columns = []

        # Add regular columns
        for col in fields:
            columns.append(
                {
                    "col_name": col["col_name"],
                    "col_type": col["col_type"],
                    "col_description": col["col_description"],
                    "is_partition_col": False,
                    "table_type": table_info["table_type"],
                    "table_location": table_info["location"],
                    "create_date": str(table_info.get("create_time", "")),
                }
            )

        # Add partition columns
        for pk in table_info["partition_keys"]:
            columns.append(
                {
                    "col_name": pk["col_name"],
                    "col_type": pk["col_type"],
                    "col_description": pk["col_description"],
                    "is_partition_col": True,
                    "table_type": table_info["table_type"],
                    "table_location": table_info["location"],
                    "create_date": str(table_info.get("create_time", "")),
                }
            )

        return {
            "table_info": table_info,
            "columns": columns,
        }

    # -------------------------------------------------------------------------
    # HMS 3.x Catalog Support
    # -------------------------------------------------------------------------

    def get_catalogs(self) -> List[str]:
        """
        Get all catalog names (HMS 3.x only).

        Returns empty list if the HMS version doesn't support catalogs (HMS 2.x).
        Raises exceptions for real errors (network, auth) that shouldn't be silently ignored.
        """

        @self._get_retry_decorator()
        def _call() -> List[str]:
            try:
                response = self.client.get_catalogs()
                return list(response.names) if response.names else []
            except (AttributeError, TException) as e:
                # HMS 2.x doesn't have catalog support - this is expected
                logger.debug(f"get_catalogs not supported (likely HMS 2.x): {e}")
                return []
            # Note: TTransportException is NOT caught here - network errors should propagate

        return _call()

    # -------------------------------------------------------------------------
    # SQL Row Format Methods
    # These methods return data in the exact format expected by HiveMetastoreSource's
    # _fetch_*_rows() methods, enabling seamless integration with the parent class.
    # -------------------------------------------------------------------------

    def _iter_tables_and_views(
        self,
        databases: List[str],
        table_type_filter: Optional[Callable[[str], bool]] = None,
        catalog_name: Optional[str] = None,
    ) -> Generator[Tuple[str, str, Dict[str, Any]], None, None]:
        """
        Shared generator for iterating over tables/views.

        This DRY helper consolidates the common iteration pattern used by
        iter_table_rows(), iter_view_rows(), and iter_table_properties_rows().

        Args:
            databases: List of database names to iterate over
            table_type_filter: Optional function to filter by table_type
                               (e.g., lambda t: t != "VIRTUAL_VIEW" for tables only)
            catalog_name: Optional catalog name for HMS 3.x

        Yields:
            Tuple of (db_name, table_name, table_info dict)

        Note:
            - Table info is cached to avoid duplicate API calls when called multiple times
            - Cache keys include catalog_name to handle multi-catalog scenarios
            - Failures are tracked in dicts (not lists) to avoid duplicate entries
            - Use get_database_failures() and get_table_failures() to retrieve failures
        """
        for db_name in databases:
            # Cache key includes catalog for multi-catalog support
            db_key = DatabaseKey(catalog_name=catalog_name, db_name=db_name)

            # Skip databases that already failed
            if db_key in self._database_failures:
                continue

            try:
                table_names = self.get_all_tables(db_name, catalog_name)
            except Exception as e:
                error_msg = f"Failed to get tables for database {db_name}: {e}"
                logger.warning(error_msg)
                self._database_failures[db_key] = str(e)
                continue

            for table_name in table_names:
                # Cache key includes catalog for multi-catalog support
                table_key = TableKey(
                    catalog_name=catalog_name, db_name=db_name, table_name=table_name
                )

                # Skip tables that already failed
                if table_key in self._table_failures:
                    continue

                # Use cached table info if available
                if table_key in self._table_cache:
                    table_info = self._table_cache[table_key]
                else:
                    try:
                        table_info = self.get_table(db_name, table_name, catalog_name)
                        self._table_cache[table_key] = table_info
                    except Exception as e:
                        error_msg = f"Failed to get table {db_name}.{table_name}: {e}"
                        logger.warning(error_msg)
                        self._table_failures[table_key] = str(e)
                        continue

                # Apply table type filter
                if table_type_filter and not table_type_filter(
                    table_info["table_type"]
                ):
                    continue

                yield db_name, table_name, table_info

    @staticmethod
    def _format_create_date(create_time: Optional[int]) -> str:
        """Format Unix timestamp to YYYY-MM-DD string."""
        if not create_time:
            return ""
        try:
            dt = datetime.fromtimestamp(create_time)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return str(create_time)

    def iter_table_rows(
        self,
        databases: List[str],
        catalog_name: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over table/column rows in SQL row format.

        Args:
            databases: List of database names to iterate over
            catalog_name: Optional catalog name for HMS 3.x

        Yields rows matching the format of HiveMetastoreSource._TABLES_SQL_STATEMENT:
        - schema_name: str
        - table_name: str
        - table_type: str
        - create_date: str
        - col_name: str
        - col_sort_order: int
        - col_description: str
        - col_type: str
        - is_partition_col: int (0 or 1)
        - table_location: str
        """
        for db_name, table_name, table_info in self._iter_tables_and_views(
            databases,
            table_type_filter=lambda t: t != VIRTUAL_VIEW_TYPE,  # Exclude views
            catalog_name=catalog_name,
        ):
            create_date = self._format_create_date(table_info.get("create_time"))

            # Get columns
            try:
                fields = self.get_fields(db_name, table_name, catalog_name)
            except Exception as e:
                logger.warning(f"Failed to get fields for {db_name}.{table_name}: {e}")
                table_key = TableKey(
                    catalog_name=catalog_name, db_name=db_name, table_name=table_name
                )
                self._table_failures[table_key] = f"Failed to get fields: {e}"
                fields = []

            # Yield regular columns
            for idx, col_field in enumerate(fields):
                yield {
                    "schema_name": db_name,
                    "table_name": table_name,
                    "table_type": table_info["table_type"],
                    "create_date": create_date,
                    "col_name": col_field["col_name"],
                    "col_sort_order": idx,
                    "col_description": col_field.get("col_description", ""),
                    "col_type": col_field["col_type"],
                    "is_partition_col": 0,
                    "table_location": table_info.get("location", ""),
                }

            # Yield partition columns
            for idx, pk in enumerate(table_info.get("partition_keys", [])):
                yield {
                    "schema_name": db_name,
                    "table_name": table_name,
                    "table_type": table_info["table_type"],
                    "create_date": create_date,
                    "col_name": pk["col_name"],
                    "col_sort_order": len(fields) + idx,
                    "col_description": pk.get("col_description", ""),
                    "col_type": pk["col_type"],
                    "is_partition_col": 1,
                    "table_location": table_info.get("location", ""),
                }

    def iter_view_rows(
        self,
        databases: List[str],
        catalog_name: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over Hive view rows in SQL row format.

        Args:
            databases: List of database names to iterate over
            catalog_name: Optional catalog name for HMS 3.x

        Yields rows matching the format of HiveMetastoreSource._HIVE_VIEWS_SQL_STATEMENT:
        - schema_name: str
        - table_name: str
        - table_type: str (VIRTUAL_VIEW)
        - view_expanded_text: str
        - description: str
        - create_date: str
        - col_name: str
        - col_sort_order: int
        - col_description: str
        - col_type: str
        """
        for db_name, table_name, table_info in self._iter_tables_and_views(
            databases,
            table_type_filter=lambda t: t == VIRTUAL_VIEW_TYPE,  # Only views
            catalog_name=catalog_name,
        ):
            create_date = self._format_create_date(table_info.get("create_time"))
            description = table_info.get("parameters", {}).get("comment", "")

            # Get columns
            try:
                fields = self.get_fields(db_name, table_name, catalog_name)
            except Exception as e:
                logger.warning(
                    f"Failed to get fields for view {db_name}.{table_name}: {e}"
                )
                table_key = TableKey(
                    catalog_name=catalog_name, db_name=db_name, table_name=table_name
                )
                self._table_failures[table_key] = f"Failed to get view fields: {e}"
                fields = []

            for idx, col_field in enumerate(fields):
                yield {
                    "schema_name": db_name,
                    "table_name": table_name,
                    "table_type": table_info["table_type"],
                    "view_expanded_text": table_info.get("view_expanded_text", ""),
                    "description": description,
                    "create_date": create_date,
                    "col_name": col_field["col_name"],
                    "col_sort_order": idx,
                    "col_description": col_field.get("col_description", ""),
                    "col_type": col_field["col_type"],
                }

    def iter_schema_rows(
        self,
        databases: List[str],
        catalog_name: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over schema/database rows in SQL row format.

        Args:
            databases: List of database names to iterate over
            catalog_name: Optional catalog name for HMS 3.x (unused, for API consistency)

        Yields rows matching the format of HiveMetastoreSource._SCHEMAS_SQL_STATEMENT:
        - schema: str
        """
        for db_name in databases:
            yield {"schema": db_name}

    def iter_table_properties_rows(
        self,
        databases: List[str],
        catalog_name: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over table properties rows in SQL row format.

        Args:
            databases: List of database names to iterate over
            catalog_name: Optional catalog name for HMS 3.x

        Yields rows matching the format of HiveMetastoreSource._HIVE_PROPERTIES_SQL_STATEMENT:
        - schema_name: str
        - table_name: str
        - PARAM_KEY: str
        - PARAM_VALUE: str
        """
        for db_name, table_name, table_info in self._iter_tables_and_views(
            databases,
            table_type_filter=lambda t: t != VIRTUAL_VIEW_TYPE,  # Exclude views
            catalog_name=catalog_name,
        ):
            # Yield each parameter as a row
            for param_key, value in table_info.get("parameters", {}).items():
                if value is not None:
                    yield {
                        "schema_name": db_name,
                        "table_name": table_name,
                        "PARAM_KEY": param_key,
                        "PARAM_VALUE": str(value),
                    }
