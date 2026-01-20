import re
from typing import Optional


class PostgresQuery:
    """Utility class for Postgres-specific SQL queries."""

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """Validate identifier contains only safe characters to prevent SQL injection."""
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        if not re.match(r"^[a-zA-Z0-9_\-]+$", identifier):
            raise ValueError(
                f"Invalid identifier '{identifier}': must contain only alphanumeric characters, underscores, and hyphens"
            )
        return identifier

    @staticmethod
    def _build_pg_stat_filter(
        database: Optional[str],
        params: dict[str, str | int],
        additional_filters: Optional[list[str]] = None,
    ) -> str:
        """Build WHERE clause for pg_stat_statements queries with database filter."""
        filters = additional_filters or []

        if database:
            safe_database = PostgresQuery._sanitize_identifier(database)
            params["database"] = safe_database
            filters.append("d.datname = :database")

        return " AND ".join(filters)

    @staticmethod
    def check_pg_stat_statements_enabled() -> str:
        """Check if pg_stat_statements extension is installed."""
        return """
        SELECT EXISTS (
            SELECT 1
            FROM pg_extension
            WHERE extname = 'pg_stat_statements'
        ) as enabled
        """

    @staticmethod
    def check_pg_stat_statements_permissions() -> str:
        """Check if user has pg_read_all_stats role or superuser privileges."""
        return """
        SELECT
            pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') as has_stats_role,
            usesuper as is_superuser
        FROM pg_user
        WHERE usename = current_user
        """

    @staticmethod
    def get_pg_stat_statements_version() -> str:
        """Get installed pg_stat_statements extension version."""
        return """
        SELECT extversion
        FROM pg_extension
        WHERE extname = 'pg_stat_statements'
        """

    @staticmethod
    def get_query_history(
        database: Optional[str] = None,
        limit: int = 1000,
        min_calls: int = 1,
        exclude_patterns: Optional[list[str]] = None,
    ) -> tuple[str, dict[str, str | int]]:
        """
        Extract query history from pg_stat_statements.

        Returns parameterized query with bind parameters for SQL injection prevention.
        Use with: connection.execute(text(query), params)
        """
        if limit <= 0 or not isinstance(limit, int):
            raise ValueError(f"limit must be a positive integer, got: {limit}")
        if min_calls < 0 or not isinstance(min_calls, int):
            raise ValueError(
                f"min_calls must be non-negative integer, got: {min_calls}"
            )

        filters = [
            "s.query IS NOT NULL",
            "s.query != '<insufficient privilege>'",
            "s.calls >= :min_calls",
        ]

        params: dict[str, str | int] = {"min_calls": min_calls, "limit": limit}

        default_exclusions = [
            "pg_stat_statements",
            "information_schema",
            "pg_catalog.pg_",
            "SHOW",
            "SET ",
        ]

        if exclude_patterns:
            for i, pattern in enumerate(exclude_patterns):
                param_name = f"exclude_pattern_{i}"
                params[param_name] = f"%{pattern}%"
                filters.append(f"s.query NOT ILIKE :{param_name}")

        for pattern in default_exclusions:
            filters.append(f"s.query NOT ILIKE '%{pattern}%'")

        where_clause = PostgresQuery._build_pg_stat_filter(database, params, filters)

        query = f"""
        SELECT
            s.queryid::text as query_id,
            s.query as query_text,
            s.calls as execution_count,
            s.total_exec_time as total_exec_time_ms,
            s.mean_exec_time as mean_exec_time_ms,
            s.min_exec_time as min_exec_time_ms,
            s.max_exec_time as max_exec_time_ms,
            s.rows as total_rows,
            s.shared_blks_hit as shared_blocks_hit,
            s.shared_blks_read as shared_blocks_read,
            r.rolname as user_name,
            d.datname as database_name
        FROM pg_stat_statements s
        LEFT JOIN pg_roles r ON s.userid = r.oid
        LEFT JOIN pg_database d ON s.dbid = d.oid
        WHERE {where_clause}
        ORDER BY s.total_exec_time DESC, s.calls DESC
        LIMIT :limit
        """

        return query, params

    @staticmethod
    def get_queries_by_type(
        query_type: str = "INSERT",
        database: Optional[str] = None,
        limit: int = 500,
    ) -> tuple[str, dict[str, str | int]]:
        """
        Extract queries of specific type (INSERT, UPDATE, DELETE, etc.).

        Returns parameterized query with bind parameters for SQL injection prevention.
        Use with: connection.execute(text(query), params)
        """
        if limit <= 0 or not isinstance(limit, int):
            raise ValueError(f"limit must be a positive integer, got: {limit}")

        if not re.match(r"^[A-Z\s]+$", query_type.upper()):
            raise ValueError(
                f"Invalid query_type '{query_type}': must contain only uppercase letters and spaces"
            )
        safe_query_type = query_type.upper()

        params: dict[str, str | int] = {
            "query_type_pattern": f"{safe_query_type}%",
            "limit": limit,
        }

        filters = [
            "s.query ILIKE :query_type_pattern",
            "s.query != '<insufficient privilege>'",
            "s.calls >= 1",
        ]

        where_clause = PostgresQuery._build_pg_stat_filter(database, params, filters)

        query = f"""
        SELECT
            s.queryid::text as query_id,
            s.query as query_text,
            s.calls as execution_count,
            s.total_exec_time as total_exec_time_ms,
            r.rolname as user_name,
            d.datname as database_name
        FROM pg_stat_statements s
        LEFT JOIN pg_roles r ON s.userid = r.oid
        LEFT JOIN pg_database d ON s.dbid = d.oid
        WHERE {where_clause}
        ORDER BY s.calls DESC
        LIMIT :limit
        """

        return query, params

    @staticmethod
    def get_top_tables_by_query_count(limit: int = 100) -> str:
        """Get most frequently queried tables using heuristic pattern matching."""
        if limit <= 0 or not isinstance(limit, int):
            raise ValueError(f"limit must be a positive integer, got: {limit}")

        return f"""
        WITH table_refs AS (
            SELECT
                query,
                calls,
                regexp_matches(
                    query,
                    'FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*\\.)?([a-zA-Z_][a-zA-Z0-9_]*)',
                    'gi'
                ) as table_match
            FROM pg_stat_statements
            WHERE query ILIKE '%FROM%'
        )
        SELECT
            table_match[2] as table_name,
            SUM(calls) as total_calls,
            COUNT(DISTINCT query) as query_count
        FROM table_refs
        WHERE table_match[2] IS NOT NULL
        GROUP BY table_match[2]
        ORDER BY total_calls DESC
        LIMIT {limit}
        """
