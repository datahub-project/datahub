import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from sqlalchemy import text
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError

from datahub.ingestion.source.sql.postgres.query import PostgresQuery
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlUnderstandingError,
    UnsupportedStatementTypeError,
)
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from datahub.ingestion.source.sql.postgres.source import PostgresConfig
    from datahub.ingestion.source.sql.sql_common import SQLSourceReport

logger = logging.getLogger(__name__)


@dataclass
class PostgresQueryEntry:
    """Represents a single query entry from pg_stat_statements."""

    query_id: str
    query_text: str
    execution_count: int
    total_exec_time_ms: float
    user_name: Optional[str]
    database_name: str

    @property
    def avg_exec_time_ms(self) -> float:
        """Calculate average execution time."""
        return (
            self.total_exec_time_ms / self.execution_count
            if self.execution_count > 0
            else 0.0
        )


class PostgresLineageExtractor:
    """
    Extracts lineage from Postgres query history.

    Uses pg_stat_statements to capture executed queries and feeds them
    to SqlParsingAggregator for lineage extraction via sqlglot.
    """

    def __init__(
        self,
        config: "PostgresConfig",
        connection: "Connection",
        report: "SQLSourceReport",
        sql_aggregator: SqlParsingAggregator,
        default_schema: str = "public",
    ) -> None:
        self.config = config
        self.connection = connection
        self.report = report
        self.sql_aggregator = sql_aggregator
        self.default_schema = default_schema

        self.queries_extracted = 0
        self.queries_parsed = 0
        self.queries_failed = 0

    def check_prerequisites(self) -> tuple[bool, str]:
        """Verify pg_stat_statements is properly configured."""
        try:
            result = self.connection.execute(
                PostgresQuery.check_pg_stat_statements_enabled()
            )
            row = result.fetchone()

            if not row or not row[0]:
                return (
                    False,
                    "pg_stat_statements extension not installed. "
                    "Install with: CREATE EXTENSION pg_stat_statements;",
                )

        except (DatabaseError, OperationalError) as e:
            logger.warning("Failed to check pg_stat_statements extension: %s", e)
            return False, f"Extension check failed: {e}"

        try:
            result = self.connection.execute(
                PostgresQuery.check_pg_stat_statements_permissions()
            )
            row = result.fetchone()

            if row:
                has_stats_role = row[0]
                is_superuser = row[1]

                if not (has_stats_role or is_superuser):
                    return (
                        False,
                        "Insufficient permissions. Grant pg_read_all_stats role: "
                        "GRANT pg_read_all_stats TO <user>;",
                    )
            else:
                return (
                    False,
                    "Could not verify pg_stat_statements permissions. "
                    "Unable to determine user roles.",
                )

        except (DatabaseError, OperationalError) as e:
            logger.error("Failed to check permissions: %s", e)
            return False, f"Permission check failed: {e}"

        return True, "Prerequisites met"

    def extract_query_history(self) -> list[PostgresQueryEntry]:
        """Extract queries from pg_stat_statements ordered by execution time/count."""
        is_ready, message = self.check_prerequisites()
        if not is_ready:
            logger.error(
                "pg_stat_statements not ready: %s. Query-based lineage will be skipped.",
                message,
            )
            self.report.report_failure(
                message=message,
                context="pg_stat_statements_not_ready",
            )
            return []

        logger.info("Prerequisites check: %s", message)

        query, params = PostgresQuery.get_query_history(
            database=self.config.database,
            limit=self.config.max_queries_to_extract or 1000,
            min_calls=self.config.min_query_calls or 1,
            exclude_patterns=self.config.query_exclude_patterns,
        )

        with PerfTimer() as timer:
            try:
                result = self.connection.execute(text(query), params)

                queries = []
                for row in result:
                    self.queries_extracted += 1

                    queries.append(
                        PostgresQueryEntry(
                            query_id=row["query_id"],
                            query_text=row["query_text"],
                            execution_count=row["execution_count"],
                            total_exec_time_ms=row["total_exec_time_ms"],
                            user_name=row["user_name"],
                            database_name=row["database_name"],
                        )
                    )

                logger.info(
                    "Extracted %d queries from pg_stat_statements in %.2f seconds",
                    self.queries_extracted,
                    timer.elapsed_seconds(),
                )

                self.report.num_queries_extracted = self.queries_extracted
                return queries

            except (DatabaseError, OperationalError, ProgrammingError) as e:
                # Expected database errors: connection issues, permission denied, invalid SQL, etc.
                logger.error("Failed to extract query history: %s", e)
                self.report.report_failure(
                    message=str(e),
                    context="query_history_extraction_failed",
                )
                return []

    def populate_lineage_from_queries(self) -> None:
        """Extract lineage from query history and add to SQL aggregator."""
        if not self.config.include_query_lineage:
            logger.info("Query-based lineage extraction disabled in config")
            return

        logger.info(
            "Starting query-based lineage extraction (max_queries=%s)",
            self.config.max_queries_to_extract,
        )

        queries = self.extract_query_history()

        with PerfTimer() as timer:
            for query_entry in queries:
                try:
                    self.sql_aggregator.add_observed_query(
                        ObservedQuery(
                            query=query_entry.query_text,
                            default_db=query_entry.database_name,
                            default_schema=self.default_schema,
                            timestamp=None,
                            user=(
                                CorpUserUrn(query_entry.user_name)
                                if query_entry.user_name
                                else None
                            ),
                            session_id=f"queryid:{query_entry.query_id}",
                        )
                    )

                    self.queries_parsed += 1

                except (
                    SqlUnderstandingError,
                    UnsupportedStatementTypeError,
                    ValueError,
                    KeyError,
                ) as e:
                    logger.warning(
                        "Failed to parse query %s: %s. Query: %s...",
                        query_entry.query_id,
                        e,
                        query_entry.query_text[:100],
                    )
                    self.queries_failed += 1

        logger.info(
            "Processed %d queries for lineage extraction (%d failed) in %.2f seconds",
            self.queries_parsed,
            self.queries_failed,
            timer.elapsed_seconds(),
        )

        self.report.num_queries_parsed = self.queries_parsed
        self.report.num_queries_parse_failures = self.queries_failed

    def extract_usage_statistics(self) -> dict[str, int]:
        """Extract usage statistics from query history."""
        return {
            "total_queries_extracted": self.queries_extracted,
            "total_queries_parsed": self.queries_parsed,
            "total_queries_failed": self.queries_failed,
        }
