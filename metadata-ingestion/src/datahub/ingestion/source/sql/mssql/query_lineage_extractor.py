import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError

from datahub.ingestion.source.sql.mssql.query import (
    AZURE_SQL_DATABASE_ENGINE_EDITION,
    AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION,
    QUERY_LOG_MIN_MAJOR_VERSION,
    MSSQLQuery,
    QueryHistorySource,
    QueryHistoryWindow,
)
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
    from sqlalchemy.engine import Connection, RowMapping
    from sqlalchemy.sql.elements import TextClause

    from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
    from datahub.ingestion.source.sql.sql_common import SQLSourceReport

logger = logging.getLogger(__name__)


class ServerContext(NamedTuple):
    engine_edition: Optional[int]
    current_database: Optional[str]


# (source, path, database for database-scoped sessions) -> log rows.
QueryLogCache = Dict[Tuple[str, str, Optional[str]], Sequence["RowMapping"]]


class PrerequisiteResult(NamedTuple):
    is_ready: bool
    message: str
    method: str  # "query_store", "dmv", or "none"


@dataclass(frozen=True)
class MSSQLQueryExecution:
    """Executions of a query attributed to one usage bucket in the window.

    With Query Store this is the exact in-window count for the bucket. With the
    DMV fallback it is the plan's cumulative count when the plan was cached
    inside the window, otherwise 1 (see MSSQLQuery.get_query_history_from_dmv).
    """

    timestamp: datetime
    count: int
    # Login that ran the query; only the audit log and Extended Events record it.
    user: Optional[str] = None


@dataclass
class MSSQLQueryEntry:
    """Represents a single query entry from MS SQL Server query history."""

    query_id: str
    query_text: str
    execution_count: int
    total_exec_time_ms: float
    database_name: str
    # Groups statements into one parser session, so a temp table created by
    # one statement resolves for later ones. Query Store has no batch identity,
    # so it uses the query id; the DMV fallback uses the batch's sql_handle.
    session_key: Optional[str] = None
    # Executions inside the ingestion window; execution_count above is the
    # lifetime total used only for ranking.
    executions: List[MSSQLQueryExecution] = field(default_factory=list)


_UNDATED = datetime.min.replace(tzinfo=timezone.utc)
# Warn when at least this share of a database's logged executions are
# sp_execute calls that couldn't be attributed.
_UNMAPPED_EXECUTION_WARNING_SHARE = 0.1

# Rolling file suffix SQL Server appends to audit/XE target files:
# <name>_<partition>_<timestamp>.sqlaudit|.xel
_ROLLING_FILE_SUFFIX_PATTERN = re.compile(r"_\d+_\d+\.(sqlaudit|xel)$", re.IGNORECASE)


def query_log_file_pattern(current_file: str) -> str:
    """Turn the target's current file into a path that reads all its rolled files.

    Local paths take a `*` wildcard; Azure Blob Storage URLs take a name prefix
    and reject wildcards.
    """
    if current_file.lower().startswith("https://"):
        return _ROLLING_FILE_SUFFIX_PATTERN.sub("_", current_file)
    return _ROLLING_FILE_SUFFIX_PATTERN.sub(r"*.\1", current_file)


def _to_naive_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _to_aware_utc(value: datetime) -> datetime:
    # The history SQL already converts to UTC; drivers return it as naive.
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
        query_log_cache: Optional["QueryLogCache"] = None,
    ) -> None:
        self.config = config
        self.connection = connection
        self.report = report
        self.sql_aggregator = sql_aggregator
        self.default_schema = default_schema
        # Shared across the per-database extractors of one run, so a server
        # audit or server-scoped event session is read once, not per database.
        self.query_log_cache: QueryLogCache = (
            query_log_cache if query_log_cache is not None else {}
        )

        self.queries_extracted = 0
        # Set when a query log (audit / Extended Events) path was resolved.
        self.query_log_found = False
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
        row = result.mappings().fetchone()

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
        row = result.mappings().fetchone()

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

    def _window(self) -> QueryHistoryWindow:
        return QueryHistoryWindow(
            start_time=_to_naive_utc(self.config.start_time),
            end_time=_to_naive_utc(self.config.end_time),
            bucket_duration=self.config.bucket_duration,
        )

    def _server_context(self) -> ServerContext:
        row = (
            self.connection.execute(MSSQLQuery.get_engine_edition())
            .mappings()
            .fetchone()
        )
        if row is None:
            return ServerContext(engine_edition=None, current_database=None)
        return ServerContext(
            engine_edition=row["engine_edition"],
            current_database=row["current_database"],
        )

    def _discover_query_log_path(
        self, source: QueryHistorySource, is_azure_sql_database: bool
    ) -> Optional[str]:
        """Find the audit / Extended Events file pattern when not configured."""
        if source == QueryHistorySource.AUDIT_LOG:
            if is_azure_sql_database:
                self.report.failure(
                    title="Audit log location required",
                    message=(
                        "Azure SQL Database does not expose its audit location through "
                        "T-SQL. Set query_history_path to the audit storage URL, e.g. "
                        "https://<account>.blob.core.windows.net/sqldbauditlogs/<server>/"
                    ),
                    context=self._database_name(),
                )
                return None
            discovery = MSSQLQuery.discover_audit_log_files()
            column = "audit_file_path"
            requirement = (
                "a started file audit with a BATCH_COMPLETED_GROUP specification"
            )
        else:
            discovery = MSSQLQuery.discover_extended_events_files(
                database_scoped=is_azure_sql_database
            )
            column = "file_name"
            requirement = (
                "a running Extended Events session with an event_file target "
                "capturing sql_batch_completed or rpc_completed with the login"
            )

        rows = self.connection.execute(discovery).mappings().fetchall()
        paths = [str(row[column]) for row in rows if row[column]]
        if not paths:
            # A warning per database: database-scoped audits and sessions may
            # cover only some databases. The source reports a failure when no
            # database had a log.
            self.report.warning(
                title="No query log found",
                message=(
                    f"query_history_source is {source.value} but this database has "
                    f"no {requirement.removeprefix('a ')}. Configure one, or set "
                    "query_history_path."
                ),
                context=self._database_name(),
            )
            return None
        if len(paths) > 1:
            self.report.info(
                title="Multiple query logs found",
                message="Using the first; set query_history_path to choose another.",
                context=", ".join(paths),
            )
        path = query_log_file_pattern(paths[0])
        logger.info("Discovered %s at %s", source.value, path)
        return path

    def _database_name(self) -> str:
        return str(self.connection.engine.url.database or "")

    def extract_query_history(self) -> List[MSSQLQueryEntry]:
        """Extract queries from the configured query history source."""
        if self.config.query_history_source == QueryHistorySource.QUERY_STORE:
            return self._extract_from_query_store()
        return self._extract_from_query_log(self.config.query_history_source)

    def _extract_from_query_store(self) -> List[MSSQLQueryEntry]:
        prereq = self.check_prerequisites()
        if not prereq.is_ready:
            logger.warning(
                "Query history extraction not available: %s. "
                "Query-based lineage will be skipped.",
                prereq.message,
            )
            return []
        logger.info("Prerequisites check: %s", prereq.message)
        history_query = (
            MSSQLQuery.get_query_history_from_query_store
            if prereq.method == "query_store"
            else MSSQLQuery.get_query_history_from_dmv
        )
        query, params = history_query(
            window=self._window(),
            limit=self.config.max_queries_to_extract,
            min_calls=self.config.min_query_calls,
            exclude_patterns=self.config.query_exclude_patterns,
        )
        return self._run_query_history(
            lambda: list(self.connection.execute(query, params).mappings()),
            prereq.method,
        )

    def _extract_from_query_log(
        self, source: QueryHistorySource
    ) -> List[MSSQLQueryEntry]:
        try:
            context = self._server_context()
            edition = context.engine_edition
            is_azure_sql_database = edition == AZURE_SQL_DATABASE_ENGINE_EDITION
            if edition not in (
                AZURE_SQL_DATABASE_ENGINE_EDITION,
                AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION,
            ):
                major_version = self._check_version()
                if (
                    major_version is not None
                    and major_version < QUERY_LOG_MIN_MAJOR_VERSION
                ):
                    self.report.failure(
                        title="SQL Server version not supported for query logs",
                        message=(
                            f"query_history_source: {source.value} needs SQL Server "
                            "2017 or later. Use query_store instead."
                        ),
                        context=self._database_name(),
                    )
                    return []
            path = self.config.query_history_path or self._discover_query_log_path(
                source, is_azure_sql_database
            )
        except (DatabaseError, OperationalError, ProgrammingError) as e:
            self.report.failure(
                title="Query log discovery failed",
                message=(
                    "Could not look up the audit / Extended Events target. Grant "
                    "the permissions listed in the docs, or set query_history_path."
                ),
                context=self._database_name(),
                exc=e,
            )
            return []
        if path is None:
            return []
        self.query_log_found = True

        if source == QueryHistorySource.AUDIT_LOG:
            query, params = MSSQLQuery.get_query_history_from_audit_log(
                path=path,
                window=self._window(),
                limit=self.config.max_queries_to_extract,
                min_calls=self.config.min_query_calls,
                exclude_patterns=self.config.query_exclude_patterns,
                statements_truncated=is_azure_sql_database,
            )
        else:
            query, params = MSSQLQuery.get_query_history_from_extended_events(
                path=path,
                window=self._window(),
                limit=self.config.max_queries_to_extract,
                min_calls=self.config.min_query_calls,
                exclude_patterns=self.config.query_exclude_patterns,
                database_scoped=is_azure_sql_database,
            )
        # The log covers every database (a database-scoped Azure session only
        # its own), so it is read once and each database takes its rows.
        current_database = (context.current_database or "").lower()
        cache_key = (
            source.value,
            path,
            current_database if is_azure_sql_database else None,
        )

        def fetch() -> Sequence["RowMapping"]:
            rows = self.query_log_cache.get(cache_key)
            if rows is None:
                rows = list(self.connection.execute(query, params).mappings())
                self.query_log_cache[cache_key] = rows
            return [
                row
                for row in rows
                if str(row["database_name"]).lower() == current_database
            ]

        return self._run_query_history(fetch, source.value, is_query_log=True)

    def _run_query_history(
        self,
        fetch: Callable[[], Sequence["RowMapping"]],
        method: str,
        is_query_log: bool = False,
    ) -> List[MSSQLQueryEntry]:
        with PerfTimer() as timer:
            try:
                rows = fetch()

                # History queries return one row per (query, usage bucket[,
                # user]) with executions in the window, so fold rows back into
                # one entry per query.
                queries_by_id: Dict[str, MSSQLQueryEntry] = {}
                for row in rows:
                    if is_query_log:
                        executions = int(row["window_execution_count"] or 0)
                        if row["is_truncated"]:
                            # Azure SQL Auditing cuts statements at 4000
                            # characters; parsing the rest gives partial lineage.
                            self.report.num_query_log_truncated_statements += executions
                            continue
                        if row["is_unmapped"]:
                            # sp_execute whose prepare isn't in the window or
                            # the event lacks a session id.
                            self.report.num_query_log_unmapped_executions += executions
                            continue
                        self.report.num_query_log_rpc_executions_unwrapped += int(
                            row["wrapped_execution_count"] or 0
                        )
                    query_id = str(row["query_id"])
                    # A temp-table statement comes back once per session, each
                    # a separate parser session; the aggregator still merges
                    # them into one Query entity by fingerprint.
                    fold_key = f"{query_id}|{row['session_key']}"
                    entry = queries_by_id.get(fold_key)
                    if entry is None:
                        entry = MSSQLQueryEntry(
                            query_id=query_id,
                            session_key=row["session_key"],
                            query_text=row["query_text"],
                            execution_count=row["execution_count"],
                            total_exec_time_ms=float(row["total_exec_time_ms"]),
                            database_name=row["database_name"],
                        )
                        queries_by_id[fold_key] = entry
                        self.queries_extracted += 1

                    last_execution_time = row["last_execution_time_utc"]
                    window_execution_count = row["window_execution_count"]
                    if last_execution_time is not None and window_execution_count:
                        entry.executions.append(
                            MSSQLQueryExecution(
                                timestamp=_to_aware_utc(last_execution_time),
                                count=int(window_execution_count),
                                user=row["user_name"],
                            )
                        )
                queries = list(queries_by_id.values())
                if is_query_log:
                    self._warn_on_unmapped_executions(rows)

                logger.info(
                    "Extracted %d queries from %s in %.2f seconds",
                    self.queries_extracted,
                    method,
                    timer.elapsed_seconds(),
                )

                # One extractor runs per database; the report covers all of them.
                self.report.num_queries_extracted += len(queries)
                return queries

            except (DatabaseError, OperationalError, ProgrammingError) as e:
                logger.error(
                    "Database error during query extraction from %s: %s. "
                    "This may indicate missing permissions, disabled Query Store, or connectivity issues.",
                    method,
                    e,
                )
                self.report.failure(
                    message="Database error during query history extraction",
                    context="query_history_extraction_database_error",
                    exc=e,
                )
                return []
            except (KeyError, TypeError) as e:
                logger.error(
                    "Query result structure mismatch when extracting from %s: %s. "
                    "Expected columns: query_id, query_text, execution_count, total_exec_time_ms, database_name. "
                    "This likely indicates a SQL Server version incompatibility or query definition bug.",
                    method,
                    e,
                    exc_info=True,
                )
                self.report.failure(
                    message="Query structure error - check SQL Server version compatibility",
                    context="query_history_extraction_structure_error",
                    exc=e,
                )
                return []
            except Exception as e:
                logger.error(
                    "Unexpected error during query extraction from %s: %s (%s). "
                    "This is likely a bug - please report this issue with your SQL Server version and configuration.",
                    method,
                    e,
                    type(e).__name__,
                    exc_info=True,
                )
                self.report.failure(
                    message="Unexpected error during query history extraction",
                    context="query_history_extraction_unexpected_error",
                    exc=e,
                )
                return []

    def _warn_on_unmapped_executions(self, rows: Sequence["RowMapping"]) -> None:
        """Counts are partial when many sp_execute calls can't be tied to their
        prepare, e.g. the log was rotated or the event lacks session_id."""
        total = sum(int(row["window_execution_count"] or 0) for row in rows)
        unmapped = sum(
            int(row["window_execution_count"] or 0)
            for row in rows
            if row["is_unmapped"]
        )
        if total and unmapped / total >= _UNMAPPED_EXECUTION_WARNING_SHARE:
            self.report.warning(
                title="Prepared-statement executions not attributed",
                message=(
                    "Some sp_execute calls could not be matched to the statement they "
                    "ran, so query counts for prepared statements are partial. Check "
                    "that the log retains the sessions' prepares and that Extended "
                    "Events sessions collect the session_id action."
                ),
                context=f"{self._database_name()}: {unmapped} of {total} executions",
            )

    def _user_urn(self, login: Optional[str]) -> Optional[CorpUserUrn]:
        """Map a SQL Server login to a DataHub user.

        Windows logins drop their DOMAIN\\ prefix; Microsoft Entra logins are
        already emails. email_domain is appended to names without one.
        """
        if not login or not login.strip():
            return None
        name = login.strip().split("\\")[-1].lower()
        if "@" not in name and self.config.email_domain:
            name = f"{name}@{self.config.email_domain}"
        return CorpUserUrn(name)

    def _build_observed_queries(
        self, query_entry: MSSQLQueryEntry
    ) -> List[ObservedQuery]:
        """One ObservedQuery per in-window execution bucket (and user), oldest first.

        Each carries a timestamp and its execution count so the aggregator can
        emit per-query usage and Query entities for read-only queries. A query
        with no in-window executions is still added once without a timestamp
        so lineage from older history is kept. Only the audit log and Extended
        Events record the executing user.
        """
        session_id = f"queryid:{query_entry.session_key or query_entry.query_id}"
        query_text = query_entry.query_text
        if not query_entry.executions:
            return [
                ObservedQuery(
                    query=query_text,
                    default_db=query_entry.database_name,
                    default_schema=self.default_schema,
                    timestamp=None,
                    user=None,
                    session_id=session_id,
                )
            ]
        return [
            ObservedQuery(
                query=query_text,
                default_db=query_entry.database_name,
                default_schema=self.default_schema,
                timestamp=execution.timestamp,
                user=self._user_urn(execution.user),
                session_id=session_id,
                usage_multiplier=execution.count,
            )
            for execution in sorted(query_entry.executions, key=lambda e: e.timestamp)
        ]

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

        # The aggregator assumes observations arrive in increasing timestamp
        # order (latest_timestamp, session handling), so order across queries,
        # not just within one. Undated (out-of-window) entries go first.
        observations = [
            (query_entry, observed_query)
            for query_entry in queries
            for observed_query in self._build_observed_queries(query_entry)
        ]
        observations.sort(key=lambda pair: pair[1].timestamp or _UNDATED)
        self.report.num_queries_without_window_executions += sum(
            1 for query_entry in queries if not query_entry.executions
        )

        failed_query_ids: Set[str] = set()
        with PerfTimer() as timer:
            for query_entry, observed_query in observations:
                if query_entry.query_id in failed_query_ids:
                    continue
                try:
                    self.sql_aggregator.add_observed_query(observed_query)
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
                    failed_query_ids.add(query_entry.query_id)
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
                    failed_query_ids.add(query_entry.query_id)
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
                    failed_query_ids.add(query_entry.query_id)

        self.queries_failed = len(failed_query_ids)
        self.queries_parsed = len(queries) - self.queries_failed
        logger.info(
            "Processed %d queries for lineage extraction (%d failed) in %.2f seconds",
            self.queries_parsed,
            self.queries_failed,
            timer.elapsed_seconds(),
        )

        self.report.num_queries_parsed += self.queries_parsed
        self.report.num_queries_parse_failures += self.queries_failed
