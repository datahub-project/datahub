import logging
from typing import List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

logger = logging.getLogger(__name__)


class MSSQLQuery:
    """SQL queries for extracting query history from MS SQL Server."""

    @staticmethod
    def _build_exclude_clause(
        exclude_patterns: Optional[List[str]], column_expr: str
    ) -> str:
        """Build SQL WHERE clause for excluding patterns using parameterized queries."""
        if not exclude_patterns:
            return ""

        # Pattern values are bound by SQLAlchemy, preventing injection
        conditions = [
            f"{column_expr} NOT LIKE :exclude_{i}"
            for i, _ in enumerate(exclude_patterns)
        ]
        return "AND " + " AND ".join(conditions)

    @staticmethod
    def _build_exclude_params(
        exclude_patterns: Optional[List[str]], base_params: dict
    ) -> dict:
        """Build parameter dict with exclude patterns (exclude_0, exclude_1, etc.)."""
        params = base_params.copy()
        if exclude_patterns:
            for i, pattern in enumerate(exclude_patterns):
                params[f"exclude_{i}"] = pattern
        return params

    @staticmethod
    def _finalize_query(
        query_template: str,
        exclude_patterns: Optional[List[str]],
        limit: int,
        min_calls: int,
    ) -> Tuple[TextClause, dict]:
        """Finalize query by building params and wrapping in TextClause."""
        params = MSSQLQuery._build_exclude_params(
            exclude_patterns, {"limit": limit, "min_calls": min_calls}
        )
        return text(query_template), params

    @staticmethod
    def check_query_store_enabled() -> TextClause:
        """Check if Query Store is enabled for the current database."""
        return text("""
            SELECT 
                CASE 
                    WHEN actual_state_desc IN ('READ_WRITE', 'READ_ONLY') THEN 1
                    ELSE 0
                END AS is_enabled
            FROM sys.database_query_store_options
        """)

    @staticmethod
    def check_dmv_permissions() -> TextClause:
        """Check if user has VIEW SERVER STATE permission for DMVs."""
        return text("""
            SELECT 
                HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS has_view_server_state
        """)

    @staticmethod
    def get_query_history_from_query_store(
        database: Optional[str],
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, dict]:
        """
        Extract query history from Query Store (SQL Server 2016+).

        Query Store provides detailed query performance stats and full query text.
        This is the preferred method when available.
        """
        exclude_clause = MSSQLQuery._build_exclude_clause(
            exclude_patterns, "qt.query_sql_text"
        )

        query = f"""
            SELECT TOP(:limit)
                CAST(q.query_id AS VARCHAR(50)) AS query_id,
                qt.query_sql_text AS query_text,
                SUM(rs.count_executions) AS execution_count,
                SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_exec_time_ms,
                DB_NAME() AS database_name,
                NULL AS user_name
            FROM sys.query_store_query AS q
            INNER JOIN sys.query_store_query_text AS qt
                ON q.query_text_id = qt.query_text_id
            INNER JOIN sys.query_store_plan AS p
                ON q.query_id = p.query_id
            INNER JOIN sys.query_store_runtime_stats AS rs
                ON p.plan_id = rs.plan_id
            WHERE 
                rs.count_executions >= :min_calls
                {exclude_clause}
                AND qt.query_sql_text IS NOT NULL
                AND LEN(qt.query_sql_text) > 0  -- Filters empty strings and whitespace-only queries (LEN ignores trailing spaces)
            GROUP BY q.query_id, qt.query_sql_text
            HAVING SUM(rs.count_executions) >= :min_calls
            ORDER BY SUM(rs.avg_duration * rs.count_executions) DESC
        """

        return MSSQLQuery._finalize_query(query, exclude_patterns, limit, min_calls)

    @staticmethod
    def get_query_history_from_dmv(
        database: Optional[str],
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, dict]:
        """
        Extract query history from Dynamic Management Views (DMVs).

        Uses sys.dm_exec_cached_plans and sys.dm_exec_query_stats for cached queries.
        This is the fallback method for older versions or when Query Store is disabled.

        Note: DMVs only contain cached queries, so some historical queries may be missing.
        """
        exclude_clause = MSSQLQuery._build_exclude_clause(
            exclude_patterns, "CAST(st.text AS NVARCHAR(MAX))"
        )

        query = f"""
            SELECT TOP(:limit)
                CAST(qs.sql_handle AS VARCHAR(50)) AS query_id,
                CAST(st.text AS NVARCHAR(MAX)) AS query_text,
                qs.execution_count AS execution_count,
                qs.total_elapsed_time / 1000.0 AS total_exec_time_ms,
                DB_NAME() AS database_name,
                NULL AS user_name
            FROM sys.dm_exec_query_stats AS qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
            WHERE 
                qs.execution_count >= :min_calls
                {exclude_clause}
                AND st.text IS NOT NULL
                AND LEN(st.text) > 0  -- Filters empty strings and whitespace-only queries (LEN ignores trailing spaces)
                AND st.dbid = DB_ID()
            ORDER BY qs.total_elapsed_time DESC
        """

        return MSSQLQuery._finalize_query(query, exclude_patterns, limit, min_calls)

    @staticmethod
    def get_mssql_version() -> TextClause:
        """Get SQL Server version number."""
        return text("""
            SELECT 
                CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) AS version,
                CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS major_version
        """)
