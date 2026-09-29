import re
from typing import Dict, List, Optional, Union

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

# Databases Postgres/RDS always excludes from enumeration, independent of
# database_pattern -- template databases can't be connected to (see
# https://www.postgresql.org/docs/current/manage-ag-templatedbs.html) and
# rdsadmin is AWS RDS's internal administrative database. Single source of
# truth for PostgresQuery.list_databases() below and the agent probe
# (PostgresConfig.default_databases()).
POSTGRES_SYSTEM_DATABASES = ("template0", "template1", "rdsadmin")
_SYSTEM_DATABASE_EXCLUSION = ", ".join(
    f"'{name}'" for name in POSTGRES_SYSTEM_DATABASES
)


class PostgresQuery:
    """Utility class for Postgres-specific SQL queries."""

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """Validate identifier contains only safe characters to prevent SQL injection."""
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
            raise ValueError(
                f"Invalid identifier '{identifier}': must contain only alphanumeric characters and underscores, starting with letter or underscore"
            )
        return identifier

    @staticmethod
    def _build_pg_stat_filter(
        database: Optional[str],
        params: Dict[str, Union[str, int]],
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
    def check_pg_stat_statements_enabled() -> TextClause:
        """Check if pg_stat_statements extension is installed."""
        return text(
            """
        SELECT EXISTS (
            SELECT 1
            FROM pg_extension
            WHERE extname = 'pg_stat_statements'
        ) as enabled
        """
        )

    @staticmethod
    def check_pg_stat_statements_permissions() -> TextClause:
        """Check if user has pg_read_all_stats role or superuser privileges."""
        return text(
            """
        SELECT
            pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') as has_stats_role,
            usesuper as is_superuser
        FROM pg_user
        WHERE usename = current_user
        """
        )

    @staticmethod
    def get_postgres_version() -> TextClause:
        """Get PostgreSQL server version number."""
        return text(
            "SELECT current_setting('server_version_num')::integer as version_num"
        )

    @staticmethod
    def list_databases(conn: Connection) -> List[str]:
        """List databases visible on this connection, minus Postgres/RDS
        system databases (see POSTGRES_SYSTEM_DATABASES). Does not apply
        database_pattern -- callers (PostgresSource.get_inspectors() and the
        agent probe) apply that themselves, so both filter on the exact same
        raw listing rather than each re-deriving it.
        """
        rows = conn.execute(
            f"SELECT datname from pg_database where datname not in ({_SYSTEM_DATABASE_EXCLUSION})"
        )
        return [str(row["datname"]) for row in rows]

    @staticmethod
    def get_query_history(
        database: Optional[str] = None,
        limit: int = 1000,
        min_calls: int = 1,
        exclude_patterns: Optional[list[str]] = None,
    ) -> tuple[TextClause, Dict[str, Union[str, int]]]:
        """
        Extract query history from pg_stat_statements.

        Returns parameterized query with bind parameters for SQL injection prevention.
        Use with: connection.execute(query, params)
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

        params: Dict[str, Union[str, int]] = {"min_calls": min_calls, "limit": limit}

        default_exclusions = [
            "pg_stat_statements",
            "information_schema",
            "pg_catalog.pg_",
            "SHOW",
            "SET ",
        ]

        pattern_index = 0
        for pattern in default_exclusions:
            param_name = f"exclude_pattern_{pattern_index}"
            params[param_name] = f"%{pattern}%"
            filters.append(f"s.query NOT ILIKE :{param_name}")
            pattern_index += 1

        if exclude_patterns:
            for pattern in exclude_patterns:
                param_name = f"exclude_pattern_{pattern_index}"
                params[param_name] = f"%{pattern}%"
                filters.append(f"s.query NOT ILIKE :{param_name}")
                pattern_index += 1

        where_clause = PostgresQuery._build_pg_stat_filter(database, params, filters)

        query = text(
            f"""
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
        )

        return query, params
