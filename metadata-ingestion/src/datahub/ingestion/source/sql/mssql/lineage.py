import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError

from datahub.ingestion.source.sql.mssql.query import MSSQLQuery
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

    from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
    from datahub.ingestion.source.sql.sql_common import SQLSourceReport

logger = logging.getLogger(__name__)


@dataclass
class MSSQLQueryEntry:
    """Represents a single query entry from MS SQL Server query history."""

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


class MSSQLLineageExtractor:
    """
    Extracts lineage from MS SQL Server query history.

    Supports two extraction methods:
    1. Query Store (SQL Server 2016+) - preferred when available
    2. DMVs (sys.dm_exec_cached_plans) - fallback for older versions
    """

    def __init__(
        self,
        config: "SQLServerConfig",
        connection: "Connection",
        report: "SQLSourceReport",
        sql_aggregator: SqlParsingAggregator,
        default_schema: str = "dbo",
    ) -> None:
        self.config = config
        self.connection = connection
        self.report = report
        self.sql_aggregator = sql_aggregator
        self.default_schema = default_schema

        self.queries_extracted = 0
        self.queries_parsed = 0
        self.queries_failed = 0

    def _check_version(self) -> Optional[int]:
        """Check SQL Server version and return major version number."""
        result = self.connection.execute(MSSQLQuery.get_mssql_version())
        row = result.fetchone()

        if row:
            major_version = row["major_version"] if row["major_version"] else 0
            logger.info(
                f"SQL Server version detected: {row['version']} (major: {major_version})"
            )

            if major_version < 13:
                logger.warning(
                    f"SQL Server version {major_version} detected. "
                    "Query Store requires SQL Server 2016+ (version 13). "
                    "Falling back to DMV-based extraction."
                )

            return major_version

        return None

    def _check_query_store_available(self) -> bool:
        """Check if Query Store is enabled."""
        result = self.connection.execute(MSSQLQuery.check_query_store_enabled())
        row = result.fetchone()

        if row and row["is_enabled"]:
            logger.info(
                "Query Store is enabled - using Query Store for query extraction"
            )
            return True

        logger.info("Query Store is not enabled - falling back to DMV-based extraction")
        return False

    def _check_dmv_permissions(self) -> bool:
        """Check if user has VIEW SERVER STATE permission for DMVs."""
        result = self.connection.execute(MSSQLQuery.check_dmv_permissions())
        row = result.fetchone()

        if not row or not row["has_view_server_state"]:
            logger.error(
                "Insufficient permissions. Grant VIEW SERVER STATE permission: "
                "GRANT VIEW SERVER STATE TO [datahub_user];"
            )
            return False

        return True

    def check_prerequisites(self) -> tuple[bool, str, str]:
        """Verify query history prerequisites and determine extraction method."""
        self._check_version()

        try:
            if self._check_query_store_available():
                return True, "Query Store is enabled", "query_store"
        except (DatabaseError, OperationalError, ProgrammingError) as e:
            logger.info(
                f"Query Store not available ({e}), falling back to DMV-based extraction"
            )

        try:
            if not self._check_dmv_permissions():
                return (
                    False,
                    "Insufficient permissions. Grant VIEW SERVER STATE permission: "
                    "GRANT VIEW SERVER STATE TO [datahub_user];",
                    "none",
                )
        except (DatabaseError, OperationalError) as e:
            logger.error("Failed to check permissions: %s", e)
            return False, f"Permission check failed: {e}", "none"

        return True, "DMV-based extraction available", "dmv"

    def extract_query_history(self) -> list[MSSQLQueryEntry]:
        """Extract queries using the best available method (Query Store or DMVs)."""
        is_ready, message, method = self.check_prerequisites()

        if not is_ready:
            logger.error(
                f"Query history extraction not available: {message}. "
                "Query-based lineage will be skipped."
            )
            self.report.report_failure(
                message=message,
                context="query_history_not_ready",
            )
            return []

        logger.info("Prerequisites check: %s", message)

        if method == "query_store":
            query, params = MSSQLQuery.get_query_history_from_query_store(
                database=self.config.database,
                limit=self.config.max_queries_to_extract,
                min_calls=self.config.min_query_calls,
                exclude_patterns=self.config.query_exclude_patterns,
            )
        else:  # dmv
            query, params = MSSQLQuery.get_query_history_from_dmv(
                database=self.config.database,
                limit=self.config.max_queries_to_extract,
                min_calls=self.config.min_query_calls,
                exclude_patterns=self.config.query_exclude_patterns,
            )

        with PerfTimer() as timer:
            try:
                result = self.connection.execute(query, params)

                queries = []
                for row in result:
                    self.queries_extracted += 1

                    queries.append(
                        MSSQLQueryEntry(
                            query_id=str(row["query_id"]),
                            query_text=row["query_text"],
                            execution_count=row["execution_count"],
                            total_exec_time_ms=float(row["total_exec_time_ms"]),
                            user_name=row["user_name"],
                            database_name=row["database_name"],
                        )
                    )

                logger.info(
                    f"Extracted {self.queries_extracted} queries from {method} in {timer.elapsed_seconds():.2f} seconds"
                )

                self.report.num_queries_extracted = self.queries_extracted
                return queries

            except (DatabaseError, OperationalError, ProgrammingError) as e:
                # Expected database errors: connection issues, permission denied, Query Store disabled, etc.
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
            f"Starting query-based lineage extraction (max_queries={self.config.max_queries_to_extract})"
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
                ) as e:
                    logger.warning(
                        f"Failed to parse query {query_entry.query_id}: {e}. Query: {query_entry.query_text[:100]}..."
                    )
                    self.queries_failed += 1
                except (ValueError, KeyError) as e:
                    logger.error(
                        f"Unexpected error processing query {query_entry.query_id}: {e} ({type(e).__name__}). "
                        f"Query: {query_entry.query_text[:100]}... "
                        "This may indicate a bug - please report this issue.",
                        exc_info=True,
                    )
                    self.queries_failed += 1

        logger.info(
            f"Processed {self.queries_parsed} queries for lineage extraction ({self.queries_failed} failed) "
            f"in {timer.elapsed_seconds():.2f} seconds"
        )

        self.report.num_queries_parsed = self.queries_parsed
        self.report.num_queries_parse_failures = self.queries_failed
