import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple, Optional

from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError

from datahub.ingestion.source.sql.mssql.query import MSSQLQuery
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
    from sqlalchemy.sql.elements import TextClause

    from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
    from datahub.ingestion.source.sql.sql_common import SQLSourceReport

logger = logging.getLogger(__name__)


class PrerequisiteResult(NamedTuple):
    is_ready: bool
    message: str
    method: str  # "query_store", "dmv", or "none"


@dataclass
class MSSQLQueryEntry:
    """Represents a single query entry from MS SQL Server query history."""

    query_id: str
    query_text: str
    execution_count: int
    total_exec_time_ms: float
    database_name: str


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

    def _execute_boolean_check(
        self,
        query: "TextClause",
        field_name: str,
        success_msg: str,
        failure_msg: str,
        failure_is_error: bool = True,
    ) -> bool:
        """Execute query and check boolean field, logging the result."""
        result = self.connection.execute(query)
        row = result.fetchone()

        if row and row[field_name]:
            logger.info(success_msg)
            return True

        if failure_is_error:
            logger.error(failure_msg)
        else:
            logger.info(failure_msg)
        return False

    def _check_version(self) -> Optional[int]:
        """Check SQL Server version and return major version number."""
        result = self.connection.execute(MSSQLQuery.get_mssql_version())
        row = result.fetchone()

        if row:
            major_version = row["major_version"] if row["major_version"] else 0
            logger.info(
                "SQL Server version detected: %s (major: %s)",
                row["version"],
                major_version,
            )

            if major_version < 13:
                logger.warning(
                    "SQL Server version %d detected. "
                    "Query Store requires SQL Server 2016+ (version 13). "
                    "Falling back to DMV-based extraction.",
                    major_version,
                )

            return major_version

        return None

    def _check_query_store_available(self) -> bool:
        """Check if Query Store is enabled."""
        return self._execute_boolean_check(
            query=MSSQLQuery.check_query_store_enabled(),
            field_name="is_enabled",
            success_msg="Query Store is enabled - using Query Store for query extraction",
            failure_msg="Query Store is not enabled - falling back to DMV-based extraction",
            failure_is_error=False,
        )

    def _check_dmv_permissions(self) -> bool:
        """Check if user has VIEW SERVER STATE permission for DMVs."""
        return self._execute_boolean_check(
            query=MSSQLQuery.check_dmv_permissions(),
            field_name="has_view_server_state",
            success_msg="VIEW SERVER STATE permission granted",
            failure_msg="Insufficient permissions. Grant VIEW SERVER STATE permission: "
            "GRANT VIEW SERVER STATE TO [datahub_user];",
        )

    def _try_query_store_check(self) -> bool:
        """Try Query Store check, log exceptions, return False on any failure."""
        try:
            return self._check_query_store_available()
        except (DatabaseError, OperationalError, ProgrammingError) as e:
            logger.info(
                "Query Store not available (disabled or unsupported SQL Server version: %s), falling back to DMV-based extraction",
                e,
            )
        except Exception as e:
            logger.warning(
                "Unexpected error checking Query Store: %s. Falling back to DMV-based extraction.",
                e,
            )
        return False

    def _try_dmv_check(self) -> PrerequisiteResult:
        """Try DMV permissions check, return appropriate status."""
        try:
            if not self._check_dmv_permissions():
                return PrerequisiteResult(
                    is_ready=False,
                    message="Insufficient permissions. Grant VIEW SERVER STATE permission: "
                    "GRANT VIEW SERVER STATE TO [datahub_user];",
                    method="none",
                )
            return PrerequisiteResult(
                is_ready=True, message="DMV-based extraction available", method="dmv"
            )
        except (DatabaseError, OperationalError) as e:
            logger.error(
                "Database error checking DMV permissions: %s. Verify database connectivity and user permissions.",
                e,
            )
            return PrerequisiteResult(
                is_ready=False,
                message=f"Permission check failed: {e}",
                method="none",
            )
        except Exception as e:
            logger.error(
                "Unexpected error checking DMV permissions: %s. This may indicate a configuration bug.",
                e,
                exc_info=True,
            )
            return PrerequisiteResult(
                is_ready=False,
                message=f"Unexpected permission check failure: {e}",
                method="none",
            )

    def check_prerequisites(self) -> PrerequisiteResult:
        """Verify query history prerequisites and determine extraction method."""
        self._check_version()

        if self._try_query_store_check():
            return PrerequisiteResult(
                is_ready=True, message="Query Store is enabled", method="query_store"
            )

        return self._try_dmv_check()

    def extract_query_history(self) -> list[MSSQLQueryEntry]:
        """Extract queries using the best available method (Query Store or DMVs)."""
        prereq = self.check_prerequisites()

        if not prereq.is_ready:
            logger.warning(
                "Query history extraction not available: %s. "
                "Query-based lineage will be skipped.",
                prereq.message,
            )
            return []

        logger.info("Prerequisites check: %s", prereq.message)

        if prereq.method == "query_store":
            query, params = MSSQLQuery.get_query_history_from_query_store(
                limit=self.config.max_queries_to_extract,
                min_calls=self.config.min_query_calls,
                exclude_patterns=self.config.query_exclude_patterns,
            )
        else:  # dmv
            query, params = MSSQLQuery.get_query_history_from_dmv(
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
                            database_name=row["database_name"],
                        )
                    )

                logger.info(
                    "Extracted %d queries from %s in %.2f seconds",
                    self.queries_extracted,
                    prereq.method,
                    timer.elapsed_seconds(),
                )

                self.report.num_queries_extracted = self.queries_extracted
                return queries

            except (DatabaseError, OperationalError, ProgrammingError) as e:
                logger.error(
                    "Database error during query extraction from %s: %s. "
                    "This may indicate missing permissions, disabled Query Store, or connectivity issues.",
                    prereq.method,
                    e,
                )
                self.report.report_failure(
                    message=f"Database error: {e}",
                    context="query_history_extraction_database_error",
                )
                return []
            except (KeyError, TypeError) as e:
                logger.error(
                    "Query result structure mismatch when extracting from %s: %s. "
                    "Expected columns: query_id, query_text, execution_count, total_exec_time_ms, database_name. "
                    "This likely indicates a SQL Server version incompatibility or query definition bug.",
                    prereq.method,
                    e,
                    exc_info=True,
                )
                self.report.report_failure(
                    message=f"Query structure error: {e} - check SQL Server version compatibility",
                    context="query_history_extraction_structure_error",
                )
                return []
            except Exception as e:
                logger.error(
                    "Unexpected error during query extraction from %s: %s (%s). "
                    "This is likely a bug - please report this issue with your SQL Server version and configuration.",
                    prereq.method,
                    e,
                    type(e).__name__,
                    exc_info=True,
                )
                self.report.report_failure(
                    message=f"Unexpected error: {e} ({type(e).__name__})",
                    context="query_history_extraction_unexpected_error",
                )
                return []

    def populate_lineage_from_queries(self) -> None:
        """Extract lineage from query history and add to SQL aggregator."""
        if not self.config.include_query_lineage:
            logger.debug("Query-based lineage extraction disabled in config")
            return

        logger.debug(
            "Starting query-based lineage extraction (max_queries=%d)",
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
                            user=None,
                            session_id=f"queryid:{query_entry.query_id}",
                        )
                    )

                    self.queries_parsed += 1

                except (
                    SqlUnderstandingError,
                    UnsupportedStatementTypeError,
                ) as e:
                    logger.warning(
                        "Unable to parse query %s (complex/unsupported SQL syntax): %s. Query: %s...",
                        query_entry.query_id,
                        e,
                        query_entry.query_text[:100],
                    )
                    self.queries_failed += 1
                except (ValueError, KeyError, AttributeError) as e:
                    logger.error(
                        "Data structure error processing query %s: %s (%s). Query: %s... "
                        "This indicates a bug in ObservedQuery construction or SQL aggregator configuration. "
                        "Please report this issue with your DataHub version.",
                        query_entry.query_id,
                        e,
                        type(e).__name__,
                        query_entry.query_text[:100],
                        exc_info=True,
                    )
                    self.queries_failed += 1
                except Exception as e:
                    logger.error(
                        "Unexpected error processing query %s: %s (%s). Query: %s... "
                        "This is an unhandled exception - please report this issue.",
                        query_entry.query_id,
                        e,
                        type(e).__name__,
                        query_entry.query_text[:100],
                        exc_info=True,
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
