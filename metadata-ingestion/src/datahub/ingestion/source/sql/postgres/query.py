"""SQL queries for Postgres query history and lineage extraction."""

import re


class PostgresQuery:
    """Utility class for Postgres-specific SQL queries."""

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """
        Sanitize database/schema/table identifiers for safe SQL usage.

        Validates that identifier only contains safe characters (alphanumeric, underscore, hyphen).
        Raises ValueError for invalid identifiers to prevent SQL injection.
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        if not re.match(r"^[a-zA-Z0-9_\-]+$", identifier):
            raise ValueError(
                f"Invalid identifier '{identifier}': must contain only alphanumeric characters, underscores, and hyphens"
            )
        return identifier

    @staticmethod
    def _escape_like_pattern(pattern: str) -> str:
        """
        Escape special characters in LIKE patterns to prevent SQL injection.

        Escapes %, _, and backslash characters while preserving intended wildcards.
        User-provided patterns should be wrapped in % wildcards after escaping.
        """
        # Escape backslash first, then other special chars
        escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return escaped

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
            database: Filter by database name (optional, sanitized for SQL injection prevention)
            limit: Maximum queries to return (must be positive integer)
            min_calls: Minimum execution count filter (must be non-negative integer)
            exclude_patterns: SQL LIKE patterns to exclude (e.g., 'pg_catalog', 'temp')
                             Note: Patterns are escaped and wrapped in % wildcards automatically

        Returns:
            SQL query string to extract query history

        Raises:
            ValueError: If database name contains invalid characters
        """
        # Validate numeric parameters
        if limit <= 0 or not isinstance(limit, int):
            raise ValueError(f"limit must be a positive integer, got: {limit}")
        if min_calls < 0 or not isinstance(min_calls, int):
            raise ValueError(
                f"min_calls must be non-negative integer, got: {min_calls}"
            )

        filters = [
            "s.query IS NOT NULL",
            "s.query != '<insufficient privilege>'",
            f"s.calls >= {min_calls}",
        ]

        # Default patterns to exclude (no user input, safe to use directly)
        default_exclusions = [
            "pg_stat_statements",
            "information_schema",
            "pg_catalog.pg_",
            "SHOW",
            "SET ",
        ]

        # User-provided patterns: escape special chars to prevent SQL injection
        if exclude_patterns:
            for pattern in exclude_patterns:
                # Escape pattern and wrap in % wildcards
                escaped = PostgresQuery._escape_like_pattern(pattern)
                filters.append(f"s.query NOT ILIKE '%{escaped}%'")

        # Default exclusions can be used directly (no user input)
        for pattern in default_exclusions:
            filters.append(f"s.query NOT ILIKE '%{pattern}%'")

        # Sanitize database identifier to prevent SQL injection
        if database:
            safe_database = PostgresQuery._sanitize_identifier(database)
            filters.append(f"d.datname = '{safe_database}'")

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
                       Sanitized to prevent SQL injection
            database: Filter by database (sanitized for SQL injection prevention)
            limit: Maximum queries to return (must be positive integer)

        Raises:
            ValueError: If query_type or database contains invalid characters
        """
        # Validate limit
        if limit <= 0 or not isinstance(limit, int):
            raise ValueError(f"limit must be a positive integer, got: {limit}")

        # Sanitize query_type - allow only SQL command keywords
        # Valid examples: INSERT, UPDATE, DELETE, SELECT, CREATE TABLE AS, etc.
        if not re.match(r"^[A-Z\s]+$", query_type.upper()):
            raise ValueError(
                f"Invalid query_type '{query_type}': must contain only uppercase letters and spaces"
            )
        safe_query_type = query_type.upper()

        filters = [
            f"s.query ILIKE '{safe_query_type}%'",
            "s.query != '<insufficient privilege>'",
            "s.calls >= 1",
        ]

        # Sanitize database identifier
        if database:
            safe_database = PostgresQuery._sanitize_identifier(database)
            filters.append(f"d.datname = '{safe_database}'")

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

        Args:
            limit: Maximum tables to return (must be positive integer)

        Raises:
            ValueError: If limit is not a positive integer
        """
        # Validate limit
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
