"""
Protocol definition for Hive Metastore data fetching.

This module defines the HiveDataFetcher Protocol that abstracts how metadata
is retrieved from Hive Metastore, allowing different implementations (SQL, Thrift)
to be used interchangeably via composition.

The Protocol-based design enables:
- Clean separation between data fetching and metadata processing
- Easy testing with mock implementations
- Runtime type checking via @runtime_checkable

Implementations:
- SQLAlchemyDataFetcher (hive_sql_fetcher.py): Direct database access
- ThriftDataFetcher (hive_thrift_fetcher.py): HMS Thrift API access
"""

import logging
from typing import Any, Dict, Iterable, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class HiveDataFetcher(Protocol):
    """
    Protocol for fetching Hive Metastore data.

    Implementations provide data in a consistent row format that the
    HiveMetadataProcessor can consume regardless of the underlying
    connection method (SQL, Thrift, etc.).

    All methods return Iterable[Dict[str, Any]] matching the TypedDict
    formats defined in hive_metastore_config.py:
    - fetch_table_rows() -> TableRow format
    - fetch_view_rows() -> ViewRow format
    - fetch_schema_rows() -> SchemaRow format
    - fetch_table_properties_rows() -> TablePropertiesRow format
    """

    def fetch_table_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch table/column rows from the data source.

        Returns:
            Iterable of dicts with keys:
            - schema_name: str (database name)
            - table_name: str
            - table_type: str (EXTERNAL_TABLE, MANAGED_TABLE)
            - create_date: str (YYYY-MM-DD)
            - col_name: str
            - col_sort_order: int
            - col_description: str
            - col_type: str
            - is_partition_col: int (0 or 1)
            - table_location: str

        Note:
            Each row represents one column. Tables with N columns produce N rows.
            Partition columns have is_partition_col=1.
        """
        ...

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch Hive view rows from the data source.

        Returns:
            Iterable of dicts with keys:
            - schema_name: str
            - table_name: str
            - table_type: str (VIRTUAL_VIEW)
            - view_expanded_text: str
            - description: str
            - create_date: str (YYYY-MM-DD)
            - col_name: str
            - col_sort_order: int
            - col_description: str
            - col_type: str

        Note:
            Each row represents one column. Views with N columns produce N rows.
        """
        ...

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch schema/database rows from the data source.

        Returns:
            Iterable of dicts with keys:
            - schema: str (database name)
        """
        ...

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch table properties rows from the data source.

        Returns:
            Iterable of dicts with keys:
            - schema_name: str
            - table_name: str
            - PARAM_KEY: str
            - PARAM_VALUE: str

        Note:
            Each row represents one property. Tables with N properties produce N rows.
        """
        ...

    def close(self) -> None:
        """Close any open connections."""
        ...
