"""
SQLAlchemy-based data fetcher for Hive Metastore.

This module provides direct SQL access to the Hive Metastore backend database
(MySQL/PostgreSQL) for metadata extraction. It includes:
- SQL query templates for different database backends
- SQLAlchemyClient for low-level database access
- SQLAlchemyDataFetcher implementing the HiveDataFetcher protocol
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_metastore_config import HiveMetastore

logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements for Direct Database Access
# =============================================================================

# MySQL statements
TABLES_SQL_STATEMENT = """
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

TABLES_POSTGRES_SQL_STATEMENT = """
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

HIVE_VIEWS_SQL_STATEMENT = """
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

HIVE_VIEWS_POSTGRES_SQL_STATEMENT = """
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

HIVE_PROPERTIES_SQL_STATEMENT = """
SELECT d.NAME as schema_name, t.TBL_NAME as table_name, tp.PARAM_KEY, tp.PARAM_VALUE
FROM TABLE_PARAMS tp
JOIN TBLS t on t.TBL_ID = tp.TBL_ID
JOIN DBS d on d.DB_ID = t.DB_ID
WHERE 1
{where_clause_suffix}
ORDER BY tp.TBL_ID desc;
"""

HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT = """
SELECT d."NAME" as schema_name, t."TBL_NAME" as table_name, tp."PARAM_KEY", tp."PARAM_VALUE"
FROM "TABLE_PARAMS" tp
JOIN "TBLS" t on t."TBL_ID" = tp."TBL_ID"
JOIN "DBS" d on d."DB_ID" = t."DB_ID"
WHERE 1 = 1
{where_clause_suffix}
ORDER BY tp."TBL_ID" desc;
"""

SCHEMAS_SQL_STATEMENT = """
SELECT d.NAME as `schema`
FROM DBS d
WHERE 1
{where_clause_suffix}
ORDER BY d.NAME desc;
"""

SCHEMAS_POSTGRES_SQL_STATEMENT = """
SELECT d."NAME" as "schema"
FROM "DBS" d
WHERE 1 = 1
{where_clause_suffix}
ORDER BY d."NAME" desc;
"""

# Presto view statements (for presto/trino modes)
VIEWS_SQL_STATEMENT = """
SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
{where_clause_suffix}
ORDER BY t.TBL_ID desc;
"""

VIEWS_POSTGRES_SQL_STATEMENT = """
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
            TABLES_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else TABLES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch Hive view rows via SQL."""
        where_clause = self._build_where_clause(self.config.views_where_clause_suffix)
        statement = (
            HIVE_VIEWS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else HIVE_VIEWS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch schema/database rows via SQL."""
        where_clause = self._build_where_clause(self.config.schemas_where_clause_suffix)
        statement = (
            SCHEMAS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else SCHEMAS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table properties rows via SQL."""
        where_clause = self._build_where_clause(self.config.tables_where_clause_suffix)
        statement = (
            HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause
            )
            if self._is_postgresql()
            else HIVE_PROPERTIES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
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
            VIEWS_POSTGRES_SQL_STATEMENT.format(where_clause_suffix=where_clause)
            if self._is_postgresql()
            else VIEWS_SQL_STATEMENT.format(where_clause_suffix=where_clause)
        )
        return self._client.execute_query(statement)

    def close(self) -> None:
        """Close the database connection."""
        self._client.close()
