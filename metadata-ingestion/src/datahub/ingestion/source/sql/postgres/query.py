"""SQL queries for Postgres query history and lineage extraction."""


class PostgresQuery:
    """Utility class for Postgres-specific SQL queries."""

    @staticmethod
    def check_pg_stat_statements_enabled() -> str:
        """
        Check if pg_stat_statements extension is installed and enabled.

        Returns a boolean indicating extension availability.
        """
        return """
        SELECT EXISTS (
            SELECT 1
            FROM pg_extension
            WHERE extname = 'pg_stat_statements'
        ) as enabled
        """

    @staticmethod
    def check_pg_stat_statements_permissions() -> str:
        """
        Verify current user has permissions to read pg_stat_statements.

        Checks for pg_read_all_stats role or superuser privileges.
        """
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
        database: str | None = None,
        limit: int = 1000,
        min_calls: int = 1,
        exclude_patterns: list[str] | None = None,
    ) -> str:
        """
        Extract query history from pg_stat_statements.

        Note: pg_stat_statements aggregates queries by normalized form,
        so parameterized queries are deduplicated. The extension tracks
        cumulative statistics since last reset, not individual executions.

        Args:
            database: Filter by database name (optional)
            limit: Maximum queries to return
            min_calls: Minimum execution count filter
            exclude_patterns: SQL LIKE patterns to exclude (e.g., '%pg_catalog%')

        Returns:
            SQL query string to extract query history
        """
        filters = [
            "s.query IS NOT NULL",
            "s.query != '<insufficient privilege>'",
            f"s.calls >= {min_calls}",
        ]

        default_exclusions = [
            "pg_stat_statements",
            "information_schema",
            "pg_catalog.pg_",
            "SHOW",
            "SET ",
        ]

        if exclude_patterns:
            default_exclusions.extend(exclude_patterns)

        for pattern in default_exclusions:
            filters.append(f"s.query NOT ILIKE '%{pattern}%'")

        if database:
            filters.append(f"d.datname = '{database}'")

        where_clause = " AND ".join(filters)

        return f"""
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
        LIMIT {limit}
        """

    @staticmethod
    def get_queries_by_type(
        query_type: str = "INSERT",
        database: str | None = None,
        limit: int = 500,
    ) -> str:
        """
        Extract queries of specific type (INSERT, UPDATE, DELETE, CREATE TABLE AS, etc.).

        Useful for focusing on queries that produce lineage (DML operations).

        Args:
            query_type: SQL command type (INSERT, UPDATE, DELETE, CREATE TABLE AS, etc.)
            database: Filter by database
            limit: Maximum queries to return
        """
        filters = [
            f"s.query ILIKE '{query_type}%'",
            "s.query != '<insufficient privilege>'",
            "s.calls >= 1",
        ]

        if database:
            filters.append(f"d.datname = '{database}'")

        where_clause = " AND ".join(filters)

        return f"""
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
        LIMIT {limit}
        """

    @staticmethod
    def get_top_tables_by_query_count(limit: int = 100) -> str:
        """
        Get most frequently queried tables from pg_stat_statements.

        Uses heuristic pattern matching to extract table names from queries.
        Not 100% accurate but useful for identifying important tables.
        """
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
