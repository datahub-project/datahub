"""Query-based lineage extraction for Postgres using pg_stat_statements."""

import logging
from dataclasses import dataclass

from datahub.ingestion.source.sql.postgres.query import PostgresQuery
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


@dataclass
class PostgresQueryEntry:
    """Represents a single query entry from pg_stat_statements."""

    query_id: str
    query_text: str
    execution_count: int
    total_exec_time_ms: float
    user_name: str | None
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
        config,
        connection,
        report,
        sql_aggregator: SqlParsingAggregator,
        default_schema: str = "public",
    ):
        """
        Initialize lineage extractor.

        Args:
            config: PostgresConfig with lineage settings
            connection: Database connection (SQLAlchemy)
            report: Ingestion report for tracking stats
            sql_aggregator: SQL parser for lineage extraction
            default_schema: Default schema for unqualified tables
        """
        self.config = config
        self.connection = connection
        self.report = report
        self.sql_aggregator = sql_aggregator
        self.default_schema = default_schema

        self.queries_extracted = 0
        self.queries_parsed = 0
        self.queries_failed = 0

    def check_prerequisites(self) -> tuple[bool, str]:
        """
        Verify pg_stat_statements is properly configured.

        Returns:
            Tuple of (is_ready: bool, message: str)
        """
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

        except Exception as e:
            logger.warning(f"Failed to check pg_stat_statements extension: {e}")
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
                logger.warning("Could not verify pg_stat_statements permissions")

        except Exception as e:
            logger.warning(f"Failed to check permissions: {e}")

        return True, "Prerequisites met"

    def extract_query_history(self) -> list[PostgresQueryEntry]:
        """
        Extract queries from pg_stat_statements.

        Yields query entries ordered by importance (execution time/count).
        """
        is_ready, message = self.check_prerequisites()
        if not is_ready:
            logger.error(
                f"pg_stat_statements not ready: {message}. "
                f"Query-based lineage will be skipped."
            )
            self.report.report_warning(
                key="pg_stat_statements_not_ready",
                reason=message,
            )
            return []

        logger.info(f"Prerequisites check: {message}")

        query = PostgresQuery.get_query_history(
            database=self.config.database,
            limit=self.config.max_queries_to_extract or 1000,
            min_calls=self.config.min_query_calls or 1,
            exclude_patterns=self.config.query_exclude_patterns,
        )

        with PerfTimer() as timer:
            try:
                result = self.connection.execute(query)

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
                    f"Extracted {self.queries_extracted} queries from pg_stat_statements "
                    f"in {timer.elapsed_seconds():.2f} seconds"
                )

                self.report.num_queries_extracted = self.queries_extracted
                return queries

            except Exception as e:
                logger.error(f"Failed to extract query history: {e}")
                self.report.report_failure(
                    key="query_history_extraction_failed",
                    reason=str(e),
                )
                return []

    def populate_lineage_from_queries(
        self,
        discovered_tables: set[str],
    ) -> None:
        """
        Extract lineage from query history and add to SQL aggregator.

        The SqlParsingAggregator will:
        1. Parse each query with sqlglot
        2. Extract table-level lineage (INSERT INTO SELECT, CTAS, etc.)
        3. Extract column-level lineage where possible
        4. Build lineage graph for discovered tables

        Args:
            discovered_tables: Set of table URNs discovered during schema extraction
        """
        if not self.config.include_query_lineage:
            logger.info("Query-based lineage extraction disabled in config")
            return

        logger.info(
            f"Starting query-based lineage extraction "
            f"(max_queries={self.config.max_queries_to_extract})"
        )

        queries = self.extract_query_history()

        with PerfTimer() as timer:
            for query_entry in queries:
                try:
                    self.sql_aggregator.add_observed_query(
                        query=query_entry.query_text,
                        default_db=query_entry.database_name,
                        default_schema=self.default_schema,
                        query_timestamp=None,
                        user=(
                            f"urn:li:corpuser:{query_entry.user_name}"
                            if query_entry.user_name
                            else None
                        ),
                        session_id=f"queryid:{query_entry.query_id}",
                    )

                    self.queries_parsed += 1

                except Exception as e:
                    logger.debug(
                        f"Failed to parse query {query_entry.query_id}: {e}. "
                        f"Query: {query_entry.query_text[:100]}..."
                    )
                    self.queries_failed += 1

        logger.info(
            f"Processed {self.queries_parsed} queries for lineage extraction "
            f"({self.queries_failed} failed) in {timer.elapsed_seconds():.2f} seconds"
        )

        self.report.num_queries_parsed = self.queries_parsed
        self.report.num_queries_parse_failures = self.queries_failed

    def extract_usage_statistics(self) -> dict[str, int]:
        """
        Extract usage statistics from query history.

        Returns summary of query patterns, top users, most queried tables.
        """
        return {
            "total_queries_extracted": self.queries_extracted,
            "total_queries_parsed": self.queries_parsed,
            "total_queries_failed": self.queries_failed,
        }
