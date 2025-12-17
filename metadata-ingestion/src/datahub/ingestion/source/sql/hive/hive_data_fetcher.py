"""
Protocol and implementations for Hive Metastore data fetching.

This module defines the HiveDataFetcher Protocol that abstracts how metadata
is retrieved from Hive Metastore, allowing different implementations (SQL, Thrift)
to be used interchangeably via composition.

The Protocol-based design enables:
- Clean separation between data fetching and metadata processing
- Easy testing with mock implementations
- Runtime type checking via @runtime_checkable
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Protocol, runtime_checkable

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_metastore_source import HiveMetastore

logger = logging.getLogger(__name__)


# =============================================================================
# Protocol Definition
# =============================================================================


@runtime_checkable
class HiveDataFetcher(Protocol):
    """
    Protocol for fetching Hive Metastore data.

    Implementations provide data in a consistent row format that the
    HiveMetadataProcessor can consume regardless of the underlying
    connection method (SQL, Thrift, etc.).

    All methods return Iterable[Dict[str, Any]] matching the TypedDict
    formats defined in hive_metastore_source.py:
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


# =============================================================================
# SQL Statements for Direct Database Access
# =============================================================================

# MySQL statements
_TABLES_SQL_STATEMENT = """
SELECT source.* FROM
(SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,
       p.PKEY_COMMENT as col_description, p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
{where_clause_suffix}
UNION
SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
        c.COMMENT as col_description, c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
{where_clause_suffix}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

_TABLES_POSTGRES_SQL_STATEMENT = """
SELECT source.* FROM
(SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
        to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name, p."INTEGER_IDX" as col_sort_order,
        p."PKEY_COMMENT" as col_description, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
{where_clause_suffix}
UNION
SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
       to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
       c."INTEGER_IDX" as col_sort_order, c."COMMENT" as col_description, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
{where_clause_suffix}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

_HIVE_VIEWS_SQL_STATEMENT = """
SELECT source.* FROM
(SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type, t.VIEW_EXPANDED_TEXT as view_expanded_text, tp.PARAM_VALUE as description,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
        c.COMMENT as col_description, c.TYPE_NAME as col_type
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment')
WHERE t.TBL_TYPE IN ('VIRTUAL_VIEW')
{where_clause_suffix}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

_HIVE_VIEWS_POSTGRES_SQL_STATEMENT = """
SELECT source.* FROM
(SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type, t."VIEW_EXPANDED_TEXT" as view_expanded_text, tp."PARAM_VALUE" as description,
       to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
       c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
LEFT JOIN "TABLE_PARAMS" tp ON (t."TBL_ID" = tp."TBL_ID" AND tp."PARAM_KEY"='comment')
WHERE t."TBL_TYPE" IN ('VIRTUAL_VIEW')
{where_clause_suffix}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

_HIVE_PROPERTIES_SQL_STATEMENT = """
SELECT d.NAME as schema_name, t.TBL_NAME as table_name, tp.PARAM_KEY, tp.PARAM_VALUE
FROM TABLE_PARAMS tp
JOIN TBLS t on t.TBL_ID = tp.TBL_ID
JOIN DBS d on d.DB_ID = t.DB_ID
WHERE 1
{where_clause_suffix}
ORDER BY tp.TBL_ID desc;
"""

_HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT = """
SELECT d."NAME" as schema_name, t."TBL_NAME" as table_name, tp."PARAM_KEY", tp."PARAM_VALUE"
FROM "TABLE_PARAMS" tp
JOIN "TBLS" t on t."TBL_ID" = tp."TBL_ID"
JOIN "DBS" d on d."DB_ID" = t."DB_ID"
WHERE 1 = 1
{where_clause_suffix}
ORDER BY tp."TBL_ID" desc;
"""

_SCHEMAS_SQL_STATEMENT = """
SELECT d.NAME as `schema`
FROM DBS d
WHERE 1
{where_clause_suffix}
ORDER BY d.NAME desc;
"""

_SCHEMAS_POSTGRES_SQL_STATEMENT = """
SELECT d."NAME" as "schema"
FROM "DBS" d
WHERE 1 = 1
{where_clause_suffix}
ORDER BY d."NAME" desc;
"""

# Presto view statements (for presto/trino modes)
_VIEWS_SQL_STATEMENT = """
SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
{where_clause_suffix}
ORDER BY t.TBL_ID desc;
"""

_VIEWS_POSTGRES_SQL_STATEMENT = """
SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'
{where_clause_suffix}
ORDER BY t."TBL_ID" desc;
"""


# =============================================================================
# SQLAlchemy Client
# =============================================================================


class SQLAlchemyClient:
    """Low-level SQLAlchemy client for executing queries against HMS database."""

    def __init__(self, config: "HiveMetastore"):
        self.config = config
        self._connection = None

    @property
    def connection(self) -> Any:
        """Lazy connection initialization."""
        if self._connection is None:
            url = self.config.get_sql_alchemy_url()
            engine = create_engine(url, **self.config.options)
            self._connection = engine.connect()
        return self._connection

    def execute_query(self, query: str) -> Iterable[Dict[str, Any]]:
        """Execute SQL query and return results as dicts."""
        results = self.connection.execute(text(query))
        # Convert Row objects to dicts for consistent interface
        for row in results:
            yield dict(row._mapping)

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None


# =============================================================================
# SQLAlchemy Data Fetcher Implementation
# =============================================================================


class SQLAlchemyDataFetcher:
    """
    Data fetcher using direct SQL queries against HMS database.

    This implementation connects directly to the Hive Metastore backend
    database (MySQL/PostgreSQL) and executes SQL queries to fetch metadata.

    Supports:
    - MySQL and PostgreSQL HMS backends
    - WHERE clause filtering
    - Presto/Trino view extraction
    """

    def __init__(self, config: "HiveMetastore"):
        """
        Initialize the SQLAlchemy data fetcher.

        Args:
            config: HiveMetastore configuration with database connection settings
        """
        self.config = config
        self._client = SQLAlchemyClient(config)

    def _is_postgresql(self) -> bool:
        """Check if the backend is PostgreSQL."""
        return "postgresql" in self.config.scheme

    def _get_db_filter_where_clause(self) -> str:
        """Get WHERE clause for database filtering."""
        if self.config.metastore_db_name is None:
            return ""
        if self.config.database:
            if self._is_postgresql():
                return f"AND d.\"NAME\" = '{self.config.database}'"
            else:
                return f"AND d.NAME = '{self.config.database}'"
        return ""

    def _build_where_clause(self, suffix_config: str) -> str:
        """Combine config suffix with database filter."""
        db_filter = self._get_db_filter_where_clause()
        return f"{suffix_config} {db_filter}".strip()

    def fetch_table_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table/column rows via SQL."""
        where_clause = self._build_where_clause(self.config.tables_where_clause_suffix)
        statement = (
            _TABLES_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else _TABLES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch Hive view rows via SQL."""
        where_clause = self._build_where_clause(self.config.views_where_clause_suffix)
        statement = (
            _HIVE_VIEWS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else _HIVE_VIEWS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch schema/database rows via SQL."""
        where_clause = self._build_where_clause(self.config.schemas_where_clause_suffix)
        statement = (
            _SCHEMAS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else _SCHEMAS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table properties rows via SQL."""
        where_clause = self._build_where_clause(self.config.tables_where_clause_suffix)
        statement = (
            _HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause
            )
            if self._is_postgresql()
            else _HIVE_PROPERTIES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_presto_view_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch Presto/Trino view rows via SQL.

        This is a separate method for presto/trino modes which have a different
        view storage format (base64-encoded JSON in VIEW_ORIGINAL_TEXT).
        """
        where_clause = self._build_where_clause(self.config.views_where_clause_suffix)
        statement = (
            _VIEWS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else _VIEWS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def close(self) -> None:
        """Close the database connection."""
        self._client.close()
