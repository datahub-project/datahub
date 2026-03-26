"""
SQLAlchemy-based data fetcher for Hive Metastore.

This module provides direct SQL access to the Hive Metastore backend database
(MySQL/PostgreSQL) for metadata extraction. It includes:
- SQL query templates for different database backends
- SQLAlchemyClient for low-level database access
- SQLAlchemyDataFetcher implementing the HiveDataFetcher protocol

Security: Uses parameterized queries (:db_name bind parameter) for database
filtering to prevent SQL injection. Query construction uses only constant
string concatenation - no user data is ever interpolated into SQL strings.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_metastore_config import HiveMetastore

logger = logging.getLogger(__name__)


# =============================================================================
# SQL Query Builder
#
# Builds queries by concatenating constant strings only. The :db_name placeholder
# is a SQLAlchemy bind parameter - the actual value is never in the SQL string.
# =============================================================================

# Database filter clauses - these are CONSTANT strings, not user input
_MYSQL_DB_FILTER = " AND d.NAME = :db_name"
_POSTGRES_DB_FILTER = ' AND d."NAME" = :db_name'
_MYSQL_DB_FILTER_WHERE = " WHERE d.NAME = :db_name"
_POSTGRES_DB_FILTER_WHERE = ' WHERE d."NAME" = :db_name'


def _build_query(
    base_query: str,
    with_db_filter: bool,
    is_postgresql: bool,
    needs_where: bool = False,
) -> str:
    """
    Build SQL query with optional database filter.

    This function only concatenates CONSTANT strings - no user data is ever
    included in the SQL. The :db_name is a bind parameter placeholder that
    SQLAlchemy handles safely.

    Args:
        base_query: Base SQL query (constant string)
        with_db_filter: Whether to add database filter
        is_postgresql: Use PostgreSQL syntax (quoted identifiers)
        needs_where: If True, use WHERE instead of AND (for queries without existing WHERE)

    Returns:
        Complete SQL query string with placeholder if filtering enabled
    """
    if not with_db_filter:
        return base_query

    if needs_where:
        db_filter = (
            _POSTGRES_DB_FILTER_WHERE if is_postgresql else _MYSQL_DB_FILTER_WHERE
        )
    else:
        db_filter = _POSTGRES_DB_FILTER if is_postgresql else _MYSQL_DB_FILTER

    return base_query + db_filter


# =============================================================================
# SQL Statement Templates
#
# Base queries without database filter. The filter is added by _build_query().
# =============================================================================

# MySQL - Tables (partition keys + columns)
_TABLES_MYSQL_BASE = """
SELECT source.* FROM
(SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,
       p.PKEY_COMMENT as col_description, p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE'){filter1}
UNION
SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
       c.COMMENT as col_description, c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE'){filter2}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

# PostgreSQL - Tables
_TABLES_POSTGRES_BASE = """
SELECT source.* FROM
(SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
        to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name, p."INTEGER_IDX" as col_sort_order,
        p."PKEY_COMMENT" as col_description, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE'){filter1}
UNION
SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
       to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
       c."INTEGER_IDX" as col_sort_order, c."COMMENT" as col_description, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE'){filter2}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

# MySQL - Hive Views
_VIEWS_MYSQL_BASE = """
SELECT source.* FROM
(SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type, t.VIEW_EXPANDED_TEXT as view_expanded_text, tp.PARAM_VALUE as description,
       FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
       c.COMMENT as col_description, c.TYPE_NAME as col_type
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment')
WHERE t.TBL_TYPE IN ('VIRTUAL_VIEW'){filter}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

# PostgreSQL - Hive Views
_VIEWS_POSTGRES_BASE = """
SELECT source.* FROM
(SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type, t."VIEW_EXPANDED_TEXT" as view_expanded_text, tp."PARAM_VALUE" as description,
       to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
       c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
LEFT JOIN "TABLE_PARAMS" tp ON (t."TBL_ID" = tp."TBL_ID" AND tp."PARAM_KEY"='comment')
WHERE t."TBL_TYPE" IN ('VIRTUAL_VIEW'){filter}
) source
ORDER by tbl_id desc, col_sort_order asc;
"""

# MySQL - Properties
_PROPERTIES_MYSQL_BASE = """
SELECT d.NAME as schema_name, t.TBL_NAME as table_name, tp.PARAM_KEY, tp.PARAM_VALUE
FROM TABLE_PARAMS tp
JOIN TBLS t on t.TBL_ID = tp.TBL_ID
JOIN DBS d on d.DB_ID = t.DB_ID{filter}
ORDER BY tp.TBL_ID desc;
"""

# PostgreSQL - Properties
_PROPERTIES_POSTGRES_BASE = """
SELECT d."NAME" as schema_name, t."TBL_NAME" as table_name, tp."PARAM_KEY", tp."PARAM_VALUE"
FROM "TABLE_PARAMS" tp
JOIN "TBLS" t on t."TBL_ID" = tp."TBL_ID"
JOIN "DBS" d on d."DB_ID" = t."DB_ID"{filter}
ORDER BY tp."TBL_ID" desc;
"""

# MySQL - Schemas
_SCHEMAS_MYSQL_BASE = """
SELECT d.NAME as `schema`
FROM DBS d{filter}
ORDER BY d.NAME desc;
"""

# PostgreSQL - Schemas
_SCHEMAS_POSTGRES_BASE = """
SELECT d."NAME" as "schema"
FROM "DBS" d{filter}
ORDER BY d."NAME" desc;
"""

# MySQL - Presto Views
_PRESTO_VIEWS_MYSQL_BASE = """
SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'{filter}
ORDER BY t.TBL_ID desc;
"""

# PostgreSQL - Presto Views
_PRESTO_VIEWS_POSTGRES_BASE = """
SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'{filter}
ORDER BY t."TBL_ID" desc;
"""


def _get_tables_query(with_db_filter: bool, is_postgresql: bool) -> str:
    """Get tables query with optional db filter."""
    # Tables query has UNION, so filter needs to be in both parts
    filter_clause = _POSTGRES_DB_FILTER if is_postgresql else _MYSQL_DB_FILTER
    base = _TABLES_POSTGRES_BASE if is_postgresql else _TABLES_MYSQL_BASE
    if with_db_filter:
        return base.replace("{filter1}", filter_clause).replace(
            "{filter2}", filter_clause
        )
    return base.replace("{filter1}", "").replace("{filter2}", "")


def _get_views_query(with_db_filter: bool, is_postgresql: bool) -> str:
    """Get Hive views query with optional db filter."""
    filter_clause = _POSTGRES_DB_FILTER if is_postgresql else _MYSQL_DB_FILTER
    base = _VIEWS_POSTGRES_BASE if is_postgresql else _VIEWS_MYSQL_BASE
    return base.replace("{filter}", filter_clause if with_db_filter else "")


def _get_properties_query(with_db_filter: bool, is_postgresql: bool) -> str:
    """Get properties query with optional db filter."""
    # Properties query needs WHERE clause (no existing WHERE)
    filter_clause = (
        _POSTGRES_DB_FILTER_WHERE if is_postgresql else _MYSQL_DB_FILTER_WHERE
    )
    base = _PROPERTIES_POSTGRES_BASE if is_postgresql else _PROPERTIES_MYSQL_BASE
    return base.replace("{filter}", filter_clause if with_db_filter else "")


def _get_schemas_query(with_db_filter: bool, is_postgresql: bool) -> str:
    """Get schemas query with optional db filter."""
    filter_clause = (
        _POSTGRES_DB_FILTER_WHERE if is_postgresql else _MYSQL_DB_FILTER_WHERE
    )
    base = _SCHEMAS_POSTGRES_BASE if is_postgresql else _SCHEMAS_MYSQL_BASE
    return base.replace("{filter}", filter_clause if with_db_filter else "")


def _get_presto_views_query(with_db_filter: bool, is_postgresql: bool) -> str:
    """Get Presto views query with optional db filter."""
    filter_clause = _POSTGRES_DB_FILTER if is_postgresql else _MYSQL_DB_FILTER
    base = _PRESTO_VIEWS_POSTGRES_BASE if is_postgresql else _PRESTO_VIEWS_MYSQL_BASE
    return base.replace("{filter}", filter_clause if with_db_filter else "")


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

    def execute_query(
        self, query: str, bind_params: Optional[Dict[str, Any]] = None
    ) -> Iterable[Dict[str, Any]]:
        """
        Execute SQL query and return results as dicts.

        Args:
            query: SQL query string, may contain :param_name placeholders
            bind_params: Optional dict of parameter values to bind
        """
        sql = text(query)
        if bind_params:
            results = self.connection.execute(sql, bind_params)
        else:
            results = self.connection.execute(sql)
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
    - Parameterized database filtering (safe from SQL injection)
    - Presto/Trino view extraction

    Security: Uses parameterized queries (:db_name bind parameter) for database
    name filtering. Query construction only concatenates constant strings -
    user data is never interpolated into SQL.
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

    def _should_filter_by_database(self) -> bool:
        """Check if database filtering should be applied."""
        return self.config.metastore_db_name is not None and bool(self.config.database)

    def _get_db_filter_params(self) -> Optional[Dict[str, Any]]:
        """
        Get bind parameters for database filtering.

        Returns:
            Dict with db_name parameter if filtering is enabled, None otherwise.
        """
        if self._should_filter_by_database():
            return {"db_name": self.config.database}
        return None

    def fetch_table_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table/column rows via SQL."""
        statement = _get_tables_query(
            self._should_filter_by_database(), self._is_postgresql()
        )
        return self._client.execute_query(statement, self._get_db_filter_params())

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch Hive view rows via SQL."""
        statement = _get_views_query(
            self._should_filter_by_database(), self._is_postgresql()
        )
        return self._client.execute_query(statement, self._get_db_filter_params())

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch schema/database rows via SQL."""
        statement = _get_schemas_query(
            self._should_filter_by_database(), self._is_postgresql()
        )
        return self._client.execute_query(statement, self._get_db_filter_params())

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table properties rows via SQL."""
        statement = _get_properties_query(
            self._should_filter_by_database(), self._is_postgresql()
        )
        return self._client.execute_query(statement, self._get_db_filter_params())

    def fetch_presto_view_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetch Presto/Trino view rows via SQL.

        This is a separate method for presto/trino modes which have a different
        view storage format (base64-encoded JSON in VIEW_ORIGINAL_TEXT).
        """
        statement = _get_presto_views_query(
            self._should_filter_by_database(), self._is_postgresql()
        )
        return self._client.execute_query(statement, self._get_db_filter_params())

    def close(self) -> None:
        """Close the database connection."""
        self._client.close()
