from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


class MSSQLQuery:
    """SQL queries for extracting query history from MS SQL Server."""

    @staticmethod
    def _build_exclude_clause(
        exclude_patterns: Optional[List[str]], column_expr: str
    ) -> str:
        """Build SQL WHERE clause for excluding patterns."""
        if not exclude_patterns:
            return ""

        conditions = []
        for i in range(len(exclude_patterns)):
            condition = f"{column_expr} NOT LIKE :exclude_{i}"
            conditions.append(condition)

        return "AND " + " AND ".join(conditions)

    @staticmethod
    def _build_exclude_params(
        exclude_patterns: Optional[List[str]],
        base_params: Dict[str, Union[int, str]],
    ) -> Dict[str, Union[int, str]]:
        """Build parameter dict with exclude patterns."""
        params = base_params.copy()
        if exclude_patterns:
            for i, pattern in enumerate(exclude_patterns):
                key = f"exclude_{i}"
                params[key] = pattern
        return params

    @staticmethod
    def _finalize_query(
        query_template: str,
        exclude_patterns: Optional[List[str]],
        limit: int,
        min_calls: int,
    ) -> Tuple[TextClause, Dict[str, Union[int, str]]]:
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
    def _validate_query_params(limit: int, min_calls: int) -> None:
        """Validate query history parameters."""
        if limit <= 0:
            raise ValueError(f"limit must be positive, got: {limit}")
        if min_calls < 0:
            raise ValueError(f"min_calls must be non-negative, got: {min_calls}")

    @staticmethod
    def _get_query_history_base(
        exclude_column_expr: str,
        query_template_builder: str,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, Dict[str, Union[int, str]]]:
        """Common logic for Query Store and DMV query history extraction."""
        MSSQLQuery._validate_query_params(limit, min_calls)

        exclude_clause = MSSQLQuery._build_exclude_clause(
            exclude_patterns, exclude_column_expr
        )

        query = query_template_builder.format(exclude_clause=exclude_clause)

        return MSSQLQuery._finalize_query(query, exclude_patterns, limit, min_calls)

    @staticmethod
    def get_query_history_from_query_store(
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, Dict[str, Union[int, str]]]:
        """Extract query history from Query Store (SQL Server 2016+)."""
        query_template = """
            SELECT TOP(:limit)
                CAST(q.query_id AS VARCHAR(50)) AS query_id,
                qt.query_sql_text AS query_text,
                SUM(rs.count_executions) AS execution_count,
                SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_exec_time_ms,
                DB_NAME() AS database_name
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
                AND LEN(qt.query_sql_text) > 0
            GROUP BY q.query_id, qt.query_sql_text
            HAVING SUM(rs.count_executions) >= :min_calls
            ORDER BY SUM(rs.avg_duration * rs.count_executions) DESC
        """

        return MSSQLQuery._get_query_history_base(
            exclude_column_expr="qt.query_sql_text",
            query_template_builder=query_template,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_query_history_from_dmv(
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, Dict[str, Union[int, str]]]:
        """Extract query history from DMVs (fallback for SQL Server 2014 or when Query Store is disabled)."""
        query_template = """
            SELECT TOP(:limit)
                CAST(qs.sql_handle AS VARCHAR(50)) AS query_id,
                CAST(st.text AS NVARCHAR(MAX)) AS query_text,
                qs.execution_count AS execution_count,
                qs.total_elapsed_time / 1000.0 AS total_exec_time_ms,
                DB_NAME() AS database_name
            FROM sys.dm_exec_query_stats AS qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
            WHERE 
                qs.execution_count >= :min_calls
                {exclude_clause}
                AND st.text IS NOT NULL
                AND LEN(st.text) > 0
                AND st.dbid = DB_ID()
            ORDER BY qs.total_elapsed_time DESC
        """

        return MSSQLQuery._get_query_history_base(
            exclude_column_expr="CAST(st.text AS NVARCHAR(MAX))",
            query_template_builder=query_template,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_mssql_version() -> TextClause:
        """Get SQL Server version number."""
        return text("""
            SELECT 
                CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) AS version,
                CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS major_version
        """)
