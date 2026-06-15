import logging
import random
import re
import time
import traceback
from collections import defaultdict
from collections.abc import Generator
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from threading import Event, Lock, Thread, current_thread
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Union,
)

# This import verifies that the dependencies are available.
import teradatasqlalchemy.types as custom_types
from pydantic import field_validator, model_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import (
    DatabaseError,
    NotSupportedError,
    OperationalError,
    TimeoutError as PoolTimeoutError,
)
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql.expression import text
from teradatasqlalchemy.dialect import TeradataDialect
from teradatasqlalchemy.options import configure

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp_builder import add_owner_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.stats_collections import TopKDict

logger: logging.Logger = logging.getLogger(__name__)

# Precompiled regex pattern for case-insensitive "(not casespecific)" removal
NOT_CASESPECIFIC_PATTERN = re.compile(r"\(not casespecific\)", re.IGNORECASE)

# Teradata uses a two-tier database.table naming approach without default database prefixing
DEFAULT_NO_DATABASE_TERADATA = None

# Common excluded databases used in multiple places
EXCLUDED_DATABASES = [
    "All",
    "Crashdumps",
    "Default",
    "DemoNow_Monitor",
    "EXTUSER",
    "External_AP",
    "GLOBAL_FUNCTIONS",
    "LockLogShredder",
    "PUBLIC",
    "SQLJ",
    "SYSBAR",
    "SYSJDBC",
    "SYSLIB",
    "SYSSPATIAL",
    "SYSUDTLIB",
    "SYSUIF",
    "SysAdmin",
    "Sys_Calendar",
    "SystemFe",
    "TDBCMgmt",
    "TDMaps",
    "TDPUSER",
    "TDQCD",
    "TDStats",
    "TD_ANALYTICS_DB",
    "TD_SERVER_DB",
    "TD_SYSFNLIB",
    "TD_SYSGPL",
    "TD_SYSXML",
    "TDaaS_BAR",
    "TDaaS_DB",
    "TDaaS_Maint",
    "TDaaS_Monitor",
    "TDaaS_Support",
    "TDaaS_TDBCMgmt1",
    "TDaaS_TDBCMgmt2",
    "dbcmngr",
    "mldb",
    "system",
    "tapidb",
    "tdwm",
    "val",
    "dbc",
]

register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.INTERVAL_DAY, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MONTH, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR_TO_MONTH, TimeTypeClass)
register_custom_type(custom_types.MBB, BytesTypeClass)
register_custom_type(custom_types.MBR, BytesTypeClass)
register_custom_type(custom_types.GEOMETRY, BytesTypeClass)
register_custom_type(custom_types.TDUDT, BytesTypeClass)
register_custom_type(custom_types.XML, BytesTypeClass)
register_custom_type(custom_types.PERIOD_TIME, TimeTypeClass)
register_custom_type(custom_types.PERIOD_DATE, TimeTypeClass)
register_custom_type(custom_types.PERIOD_TIMESTAMP, TimeTypeClass)

# ---------------------------------------------------------------------------
# Exponential-backoff retry helpers
# ---------------------------------------------------------------------------

_RETRY_MAX_ATTEMPTS = 3
_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
_RETRY_BACKOFF_CAP_SECONDS = 30.0  # full-jitter ceiling per attempt; protects against runaway sleeps if user sets large retry_initial_backoff_seconds

# Numeric Teradata error codes that indicate a PERMANENT failure.  Matched
# against str(exc) before any retry logic — codes are stable across driver
# versions and more reliable than substring matching.
#
#   8017 — "The UserId, Password or Account is invalid." (auth failure)
#   3003 — "Logon failed." / "Invalid password." (auth failure)
#   3523 — "[user] does not have [access type] access to [db].[table]."
#   3524 — "[user] does not have [access type] access to database [db]."
#   3802 — "Database '<name>' does not exist." (object not found — no amount of
#           backoff will create the database; treat as permanent so probe queries
#           such as _check_historical_table_exists propagate cleanly to callers)
#
# Source: Teradata Database Messages, doc B035-1096.
_PERMANENT_ERROR_CODE_RE: re.Pattern = re.compile(
    r"\[Error (?:8017|3003|3523|3524|3802)\]"
)

# Substrings in Teradata / network error messages that indicate a PERMANENT
# failure — auth, authorisation, or configuration errors that no amount of
# backoff can resolve.  Secondary fallback after _PERMANENT_ERROR_CODE_RE for
# messages that carry no numeric code or use unusual driver formatting.
_PERMANENT_ERROR_SUBSTRINGS: Tuple[str, ...] = (
    "authentication failed",
    "logon failed",
    "invalid logon",
    "invalid user",
    "permission denied",
    "access denied",
    "no access",
    "tdgss configuration",  # permanent TDGSS mis-configuration (vs. transient handshake)
)

# Substrings found in Teradata / network error messages that are safe to retry
# on an *existing* connection (execute / fetchmany).
#
# Prefer _RETRYABLE_ERROR_CODE_RE for primary classification — numeric codes are
# stable across Teradata versions and driver wordings.  These substrings are a
# secondary fallback for messages that carry no numeric code.  Each entry is
# documented with the exact Teradata / driver message it targets so future
# maintainers can verify and prune false matches.
#
#   "transaction aborted" — backup for [Error 2631] / [Error 3111]; Teradata
#                           prints "Transaction aborted." ahead of the numeric
#                           code in some driver versions.
#   "database restart"    — backup for [Error 3597] / [Error 3111]; appears as
#                           "Database restart in progress, please resubmit."
#   "timed out"           — network / I-O timeout (not auth or config related);
#                           matches "connect timed out", "connection timed out",
#                           "request timed out", etc.  The deny-list above
#                           prevents overlap with auth-failure messages.
#   "i/o timeout"         — driver-level I/O timeout distinct from "timed out".
_RETRYABLE_ERROR_SUBSTRINGS: Tuple[str, ...] = (
    "transaction aborted",
    "database restart",
    "timed out",
    "i/o timeout",
)

# Additional substrings that indicate a dead/reset socket.  These are NOT safe
# to retry on an existing connection (the socket is gone), but ARE safe to
# retry at connect time because engine.connect() opens a fresh socket.
_RETRYABLE_CONNECT_EXTRA_SUBSTRINGS: Tuple[str, ...] = (
    "connection reset",
    "broken pipe",
    " eof",
    "socket closed",
)

# Teradata numeric error codes that are safe to retry.  Matched against
# str(exc) because the driver formats messages as "[Error NNNN]: <text>".
#
#   2631 — deadlock; transaction aborted due to deadlock
#   2639 — sorry, too many simultaneous transactions
#   3111 — the transaction has been timed out
#   3120 — the request is aborted because of a database recovery
#   3598 — concurrent change conflict on database - try again
#   3897 — request aborted due to system crash. Resubmit
#   3603 — concurrent change conflict on table - try again
#
# Source: Teradata Database Messages, doc B035-1096.
# Link: https://docs.teradata.com/r/Enterprise_IntelliFlex_Lake_VMware/Teradata-Call-Level-Interface-Version-2-Reference-for-Mainframe-Attached-Systems-20.00/Error-and-Failure-Codes/Error-and-Failure-Codes
#
_RETRYABLE_ERROR_CODE_RE: re.Pattern = re.compile(
    r"\[Error (?:2631|2639|3111|3120|3598|3897|3603)\]"
)


def _jittered_backoff(attempt: int, initial_backoff_seconds: float) -> float:
    """Return a full-jitter exponential backoff duration for *attempt* (0-based).

    Full jitter (``uniform(0, cap)``) is the recommended strategy when many
    concurrent workers share the same resource: pure exponential backoff causes
    a thundering-herd re-surge because all threads sleep for the *same* duration
    and then hammer the server simultaneously.  Full jitter spreads retries
    uniformly across the window so the server load stays roughly constant.
    """
    cap = min(initial_backoff_seconds * (2**attempt), _RETRY_BACKOFF_CAP_SECONDS)
    return random.uniform(0, cap)


def _should_retry(exc: BaseException) -> bool:
    """Return True when *exc* is retryable on the **same** connection.

    Dead-socket errors (connection reset, broken pipe, etc.) are intentionally
    excluded: retrying ``execute()`` or ``fetchmany()`` on a dead socket will
    always fail again.  Use :func:`_should_retry_connect` for the connect step
    where a fresh socket is opened on each attempt.

    Classification order (deny checked before allow at each layer):
    1. Permanent error codes (_PERMANENT_ERROR_CODE_RE) — most reliable signal;
       stable numeric codes that unambiguously identify unrecoverable errors.
    2. Permanent substrings (_PERMANENT_ERROR_SUBSTRINGS) — catches messages
       from driver versions that omit or reformat the numeric code.
    3. Retryable error codes (_RETRYABLE_ERROR_CODE_RE) — primary allow signal.
    4. Retryable substrings (_RETRYABLE_ERROR_SUBSTRINGS) — secondary fallback
       for transient messages that carry no numeric code.
    """
    if isinstance(exc, PoolTimeoutError):
        return True
    if isinstance(exc, (OperationalError, DatabaseError)):
        raw = str(exc)
        msg = raw.lower()
        # Steps 1-2: deny-list — permanent errors are never retried.
        if _PERMANENT_ERROR_CODE_RE.search(raw):
            return False
        if any(s in msg for s in _PERMANENT_ERROR_SUBSTRINGS):
            return False
        # Steps 3-4: allow-list — retry only on confirmed transient signals.
        if _RETRYABLE_ERROR_CODE_RE.search(raw):
            return True
        if any(s in msg for s in _RETRYABLE_ERROR_SUBSTRINGS):
            return True
    return False


def _should_retry_connect(exc: BaseException) -> bool:
    """Return True when *exc* is retryable at **connect** time.

    A superset of :func:`_should_retry`: includes dead-socket signals
    (connection reset, broken pipe, EOF, socket closed) because
    ``engine.connect()`` opens a fresh socket on every call, so these errors
    are recoverable by simply trying again.
    """
    if _should_retry(exc):
        return True
    if isinstance(exc, (OperationalError, DatabaseError)):
        msg = str(exc).lower()
        if any(s in msg for s in _RETRYABLE_CONNECT_EXTRA_SUBSTRINGS):
            return True
    return False


def _retry_loop(
    op: Any,
    classify: Any,
    max_attempts: int,
    initial_backoff_seconds: float,
    op_name: str = "DB operation",
    report: Optional["TeradataReport"] = None,
    on_retry: Optional[Any] = None,
    on_terminal: Optional[Any] = None,
) -> Any:
    """Generic exponential-backoff retry scaffold shared by all retry helpers.

    Runs ``op()`` up to *max_attempts* times, retrying only while
    ``classify(exc)`` returns True.  On each retry the loop sleeps for
    ``_jittered_backoff(attempt, initial_backoff_seconds)`` seconds and
    increments ``report.num_db_retries`` (when *report* is given).

    Callbacks:
        on_retry(exc, attempt, backoff): called after classify returns True,
            before sleeping.  Receives the 0-based attempt index and the
            computed backoff duration so callers can emit context-rich log
            messages.  When omitted, a generic ``logger.warning`` is emitted.
        on_terminal(exc, attempt): called just before the final raise — either
            because the error is non-retryable or all attempts are exhausted.
            Used to emit a breadcrumb into the ingestion report.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
    for attempt in range(max_attempts):
        try:
            return op()
        except Exception as exc:
            if not classify(exc) or attempt == max_attempts - 1:
                if on_terminal is not None:
                    on_terminal(exc, attempt)
                raise
            backoff = _jittered_backoff(attempt, initial_backoff_seconds)
            if on_retry is not None:
                on_retry(exc, attempt, backoff)
            else:
                logger.warning(
                    f"{op_name} (attempt {attempt + 1}/{max_attempts}): {exc}. "
                    f"Retrying in {backoff:.2f}s..."
                )
            if report is not None:
                report.increment_db_retries()
            time.sleep(backoff)
    raise AssertionError("unreachable")  # classify returned True but loop ended


@contextmanager
def _engine_connect_with_retry(
    engine: Engine,
    max_attempts: int = _RETRY_MAX_ATTEMPTS,
    initial_backoff_seconds: float = _RETRY_INITIAL_BACKOFF_SECONDS,
    report: Optional["TeradataReport"] = None,
) -> Generator[Connection, None, None]:
    """Context-manager that acquires a SQLAlchemy connection with exponential-backoff
    retries on retryable errors.  Only the *connect* step is retried; errors
    that occur inside the ``with`` block are propagated normally.

    PoolTimeoutError and generic retryable errors are handled in a single
    except branch: PoolTimeoutError is already a subclass of Exception and
    _should_retry_connect returns True for it, so no separate branch is needed.
    The isinstance check below only gates the pool-exhaustion counter and the
    more specific log message.
    """

    def _on_retry(exc: Exception, attempt: int, backoff: float) -> None:
        if isinstance(exc, PoolTimeoutError):
            thread = current_thread()
            logger.warning(
                f"Connection pool exhausted "
                f"[thread={thread.name!r} tid={thread.ident}] "
                f"(attempt {attempt + 1}/{max_attempts}): {exc}. "
                f"Retrying in {backoff:.2f}s..."
            )
            if report is not None:
                report.increment_pool_timeout_retries()
        else:
            logger.warning(
                f"Retryable DB error acquiring connection "
                f"(attempt {attempt + 1}/{max_attempts}): {exc}. "
                f"Retrying in {backoff:.2f}s..."
            )

    conn: Connection = _retry_loop(
        op=engine.connect,
        classify=_should_retry_connect,
        max_attempts=max_attempts,
        initial_backoff_seconds=initial_backoff_seconds,
        report=report,
        on_retry=_on_retry,
    )
    try:
        yield conn
    finally:
        conn.close()


def _execute_with_retry(
    conn: Connection,
    stmt: Any,
    params: Optional[Dict[str, Any]] = None,
    max_attempts: int = _RETRY_MAX_ATTEMPTS,
    initial_backoff_seconds: float = _RETRY_INITIAL_BACKOFF_SECONDS,
    report: Optional["TeradataReport"] = None,
    warn_on_permanent_failure: bool = True,
) -> Any:
    """Execute *stmt* on *conn* with exponential-backoff retries on retryable errors.

    Args:
        warn_on_permanent_failure: When True (default), a ``report.warning`` is
            emitted for permanent first-attempt failures so that callers without
            a try/except still surface a context-rich breadcrumb in the ingestion
            report.  Pass False for intentional probe queries
            that handle the exception themselves (e.g. ``_check_historical_table_exists``).
    """

    def _on_terminal(exc: Exception, attempt: int) -> None:
        if report is None:
            return
        if _should_retry(exc) and attempt > 0:
            report.warning(
                title="Database execute failed after retries",
                message=(
                    f"A retryable error persisted after {attempt + 1} attempt(s). "
                    "The operation has been abandoned. "
                    "Check Teradata connectivity and cluster stability."
                ),
                context=str(exc),
                exc=exc,
            )
        elif warn_on_permanent_failure:
            # Permanent error: callers without a try/except would otherwise
            # surface only a bare SQLAlchemy traceback with no report entry.
            report.warning(
                title="Database execute failed",
                message=(
                    f"Statement failed on attempt {attempt + 1}: {exc}. "
                    f"Statement: {stmt}"
                ),
                context=str(exc),
                exc=exc,
            )

    return _retry_loop(
        op=lambda: conn.execute(stmt, params)
        if params is not None
        else conn.execute(stmt),
        classify=_should_retry,
        max_attempts=max_attempts,
        initial_backoff_seconds=initial_backoff_seconds,
        op_name="Retryable DB error on execute",
        report=report,
        on_terminal=_on_terminal,
    )


def _fetchmany_with_retry(
    result: Any,
    batch_size: int,
    max_attempts: int = _RETRY_MAX_ATTEMPTS,
    initial_backoff_seconds: float = _RETRY_INITIAL_BACKOFF_SECONDS,
    report: Optional["TeradataReport"] = None,
) -> List[Any]:
    """Fetch the next batch from *result* with exponential-backoff retries.

    Retry semantics: only meaningful when the *result* cursor remains valid
    after the error (e.g. a brief network hiccup that leaves the cursor
    intact).  For server-side streaming cursors (``stream_results=True``) a
    transient error that invalidates the cursor cannot be recovered by retrying
    ``fetchmany()`` — the server-side cursor position is lost and subsequent
    calls will either fail again or silently skip rows.  In that case the retry
    loop exhausts its attempts and re-raises; the caller is responsible for
    higher-level recovery (e.g. restarting the query).
    """

    def _on_terminal(exc: Exception, attempt: int) -> None:
        if report is not None:
            if _should_retry(exc) and attempt > 0:
                report.warning(
                    title="Database fetchmany failed after retries",
                    message=(
                        f"A retryable error persisted after {attempt + 1} attempt(s). "
                        "The fetch has been abandoned. "
                        "Check Teradata connectivity and cluster stability."
                    ),
                    context=str(exc),
                    exc=exc,
                )
            else:
                report.warning(
                    title="Database fetchmany failed",
                    message=f"fetchmany failed on attempt {attempt + 1}: {exc}.",
                    context=str(exc),
                    exc=exc,
                )

    return _retry_loop(  # type: ignore[return-value]
        op=lambda: result.fetchmany(batch_size),
        classify=_should_retry,
        max_attempts=max_attempts,
        initial_backoff_seconds=initial_backoff_seconds,
        op_name="Retryable DB error on fetchmany",
        report=report,
        on_terminal=_on_terminal,
    )


@dataclass
class TeradataTable:
    database: str
    name: str
    description: Optional[str]
    object_type: str
    create_timestamp: datetime
    last_alter_name: Optional[str]
    last_alter_timestamp: Optional[datetime]
    request_text: Optional[str]


# Bounded cache so multiple schemas stay resident across sequential database processing.
# Connection objects are hashable by identity; each unique connection creates a separate
# entry. Entries for closed connections are never reused but the bound prevents unbounded
# accumulation (32 covers any realistic number of concurrently active schemas).
@lru_cache(maxsize=32)
def get_schema_columns(
    self: Any, connection: Connection, dbc_columns: str, schema: str
) -> Dict[str, List[Any]]:
    start_time = time.time()
    columns: Dict[str, List[Any]] = {}
    columns_query = f"select * from dbc.{dbc_columns} where DatabaseName (NOT CASESPECIFIC) = :schema (NOT CASESPECIFIC) order by TableName, ColumnId"
    rows = _execute_with_retry(
        connection,
        text(columns_query),
        {"schema": schema},
        report=getattr(self, "report", None),
    ).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.TableName not in columns:
            columns[row_mapping.TableName] = []

        columns[row_mapping.TableName].append(row_mapping)

    end_time = time.time()
    extraction_time = end_time - start_time
    logger.info(
        f"Column extraction for schema '{schema}' completed in {extraction_time:.2f} seconds"
    )

    if hasattr(self, "report"):
        self.report.add_column_extraction_duration(extraction_time)

    return columns


@lru_cache(maxsize=32)
def get_schema_pk_constraints(
    self: Any, connection: Connection, schema: str
) -> Dict[str, List[Any]]:
    dbc_indices = "IndicesV" + "X" if configure.usexviews else "IndicesV"
    primary_keys: Dict[str, List[Any]] = {}
    stmt = f"select * from dbc.{dbc_indices} where DatabaseName (NOT CASESPECIFIC) = :schema (NOT CASESPECIFIC) and IndexType = 'K' order by IndexNumber"
    rows = _execute_with_retry(
        connection,
        text(stmt),
        {"schema": schema},
        report=getattr(self, "report", None),
    ).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.TableName not in primary_keys:
            primary_keys[row_mapping.TableName] = []

        primary_keys[row_mapping.TableName].append(row_mapping)

    return primary_keys


def optimized_get_pk_constraint(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    **kw: Dict[str, Any],
) -> Dict:
    """
    Override
    TODO: Check if we need PRIMARY Indices or PRIMARY KEY Indices
    TODO: Check for border cases (No PK Indices)
    """

    if schema is None:
        schema = self.default_schema_name

    # Default value for 'usexviews' is False so use dbc.IndicesV by default
    # dbc_indices = self.__get_xviews_obj("IndicesV")

    # table_obj = table(
    #    dbc_indices, column("ColumnName"), column("IndexName"), schema="dbc"
    # )

    res = []
    pk_keys = self.get_schema_pk_constraints(connection, schema)
    res = pk_keys.get(table_name, [])

    index_columns = list()
    index_name = None

    for index_column in res:
        index_columns.append(self.normalize_name(index_column.ColumnName))
        index_name = self.normalize_name(
            index_column.IndexName
        )  # There should be just one IndexName

        if hasattr(self, "report"):
            self.report.increment_primary_keys_processed()

    return {"constrained_columns": index_columns, "name": index_name}


def _read_padded_char_field(row: Any, field: str) -> Optional[str]:
    """Read a CHAR(N) field that Teradata returns space-padded, return it stripped.

    Returns None when the field is missing or not a string — callers should
    leave any existing value alone in that case."""
    raw = getattr(row, field, None) if not isinstance(row, dict) else row.get(field)
    return raw.strip() if isinstance(raw, str) else None


def _strip_padded_nullable(row: Any, col_info: Dict[str, Any]) -> None:
    """Re-derive col_info['nullable'] by stripping CHAR(1) padding from row.Nullable.

    teradatasqlalchemy does a strict `row['Nullable'] == 'Y'` check, but
    Teradata returns CHAR(1) values space-padded ('Y '/'N '), so the check
    evaluates False for every nullable column. No-op when the row's Nullable
    isn't a string (e.g. None for view columns without QVCI enabled)."""
    val = _read_padded_char_field(row, "Nullable")
    if val is not None:
        col_info["nullable"] = val == "Y"


def _strip_padded_autoincrement(row: Any, col_info: Dict[str, Any]) -> None:
    """Re-derive col_info['autoincrement'] by stripping padding from row.IdColType.

    Same root cause as _strip_padded_nullable: teradatasqlalchemy checks
    `row['IdColType'] in ('GA', 'GD')` strictly, but Teradata returns IdColType
    space-padded ('GA  '), so the check evaluates False for every identity
    column. No-op when the raw value isn't a string."""
    val = _read_padded_char_field(row, "IdColType")
    if val is not None:
        col_info["autoincrement"] = val in ("GA", "GD")


def optimized_get_columns(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    tables_cache: Optional[MutableMapping[str, List[TeradataTable]]] = None,
    use_qvci: bool = False,
    use_dbc_columns_for_views: bool = False,
    tables_needing_extraction: Optional[Set[Tuple[str, str]]] = None,
    **kw: Dict[str, Any],
) -> List[Dict]:
    tables_cache = tables_cache or {}
    if schema is None:
        schema = self.default_schema_name

    # Incremental extraction: skip column fetch for tables unchanged since the watermark
    if (
        tables_needing_extraction is not None
        and (schema.lower(), table_name) not in tables_needing_extraction
    ):
        logger.debug(
            f"Skipping column extraction for {schema}.{table_name} (unchanged since watermark)"
        )
        return []

    # Using 'help schema.table.*' statements has been considered.
    # The DBC.ColumnsV provides the default value which is not available
    # with the 'help column' commands result.

    td_table: Optional[TeradataTable] = None
    # Check if the object is a view
    for t in tables_cache.get(schema.lower(), []):
        if t.name == table_name:
            td_table = t
            break

    if td_table is None:
        logger.warning(
            f"Table {table_name} not found in cache for schema {schema}, not getting columns"
        )
        return []

    res: List[Any] = []
    if td_table.object_type == "View" and not use_qvci:
        if use_dbc_columns_for_views:
            # Attempt bulk dbc.ColumnsV fetch first. dbc.ColumnsV has ColumnType for views,
            # but columns defined as derived expressions (e.g., col1 + col2) will have
            # null/empty ColumnType. Fall back to HELP only when that occurs.
            dbc_col_view = "columnsV" + ("X" if configure.usexviews else "")
            dbc_res = self.get_schema_columns(connection, dbc_col_view, schema).get(
                table_name, []
            )
            columns_missing_type = [
                row
                for row in dbc_res
                if not getattr(row, "ColumnType", None)
                or not str(getattr(row, "ColumnType", "")).strip()
            ]
            if dbc_res and not columns_missing_type:
                # All columns have explicit types — no HELP call needed
                res = dbc_res
            else:
                # One or more derived-expression columns; fall back to HELP
                res = self._get_column_help(
                    connection, schema, table_name, column_name=None
                )
                col_info_list = []
                for r in res:
                    updated_column_info_dict = self._update_column_help_info(r._mapping)
                    col_info_list.append(dict(r._mapping, **(updated_column_info_dict)))
                res = col_info_list
        else:
            # Conservative default: always use HELP for views for accurate type information.
            # dbc.ColumnsV does not resolve derived expression types for views.
            res = self._get_column_help(
                connection, schema, table_name, column_name=None
            )
            col_info_list = []
            for r in res:
                updated_column_info_dict = self._update_column_help_info(r._mapping)
                col_info_list.append(dict(r._mapping, **(updated_column_info_dict)))
            res = col_info_list
    else:
        # Default value for 'usexviews' is False so use dbc.ColumnsV by default
        dbc_columns = "columnsQV" if use_qvci else "columnsV"
        dbc_columns = dbc_columns + "X" if configure.usexviews else dbc_columns
        res = self.get_schema_columns(connection, dbc_columns, schema).get(
            table_name, []
        )

    start_time = time.time()

    final_column_info = []
    # Don't care about ART tables now
    # Ignore the non-functional column in a PTI table
    for row in res:
        try:
            col_info = self._get_column_info(row)
            _strip_padded_nullable(row, col_info)
            _strip_padded_autoincrement(row, col_info)

            # Add CommentString as comment field for column description
            if hasattr(row, "CommentString") and row.CommentString:
                col_info["comment"] = row.CommentString.strip()
            elif (
                isinstance(row, dict)
                and "CommentString" in row
                and row["CommentString"]
            ):
                col_info["comment"] = row["CommentString"].strip()

            if "TSColumnType" in col_info and col_info["TSColumnType"] is not None:
                if (
                    col_info["ColumnName"] == "TD_TIMEBUCKET"
                    and col_info["TSColumnType"].strip() == "TB"
                ):
                    continue
            final_column_info.append(col_info)

            if hasattr(self, "report"):
                self.report.increment_columns_processed()

        except Exception as e:
            column_name = getattr(row, "ColumnName", "unknown")
            if hasattr(self, "report"):
                self.report.increment_column_extraction_failures()
                self.report.warning(
                    title="Column extraction failed",
                    message=f"Failed to process column {column_name!r}. The column will be omitted from the schema.",
                    context=str(e),
                    exc=e,
                )
            else:
                logger.error(f"Failed to process column {column_name}: {e}")
            continue

    if hasattr(self, "report"):
        self.report.add_column_extraction_duration(time.time() - start_time)

    return final_column_info


@lru_cache(maxsize=32)
def get_schema_foreign_keys(
    self: Any, connection: Connection, schema: str
) -> Dict[str, List[Any]]:
    dbc_child_parent_table = (
        "All_RI_ChildrenV" + "X" if configure.usexviews else "All_RI_ChildrenV"
    )
    foreign_keys: Dict[str, List[Any]] = {}
    stmt = f"""
    SELECT dbc."All_RI_ChildrenV"."ChildDB",  dbc."All_RI_ChildrenV"."ChildTable", dbc."All_RI_ChildrenV"."IndexID", dbc."{dbc_child_parent_table}"."IndexName", dbc."{dbc_child_parent_table}"."ChildKeyColumn", dbc."{dbc_child_parent_table}"."ParentDB", dbc."{dbc_child_parent_table}"."ParentTable", dbc."{dbc_child_parent_table}"."ParentKeyColumn"
        FROM dbc."{dbc_child_parent_table}"
    WHERE ChildDB = :schema ORDER BY "IndexID" ASC
    """
    rows = _execute_with_retry(
        connection,
        text(stmt),
        {"schema": schema},
        report=getattr(self, "report", None),
    ).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.ChildTable not in foreign_keys:
            foreign_keys[row_mapping.ChildTable] = []

        foreign_keys[row_mapping.ChildTable].append(row_mapping)

    return foreign_keys


def optimized_get_foreign_keys(self, connection, table_name, schema=None, **kw):
    """
    Overrides base class method
    """

    if schema is None:
        schema = self.default_schema_name
    # Default value for 'usexviews' is False so use DBC.All_RI_ChildrenV by default
    res = self.get_schema_foreign_keys(connection, schema).get(table_name, [])

    def grouper(fk_row):
        return {
            "name": fk_row.IndexName or fk_row.IndexID,  # ID if IndexName is None
            "schema": fk_row.ParentDB,
            "table": fk_row.ParentTable,
        }

    # TODO: Check if there's a better way
    fk_dicts = list()
    for constraint_info, constraint_cols in groupby_unsorted(res, grouper):
        fk_dict = {
            "name": str(constraint_info["name"]),
            "constrained_columns": list(),
            "referred_table": constraint_info["table"],
            "referred_schema": constraint_info["schema"],
            "referred_columns": list(),
        }

        for constraint_col in constraint_cols:
            fk_dict["constrained_columns"].append(
                self.normalize_name(constraint_col.ChildKeyColumn)
            )
            fk_dict["referred_columns"].append(
                self.normalize_name(constraint_col.ParentKeyColumn)
            )

        fk_dicts.append(fk_dict)

    return fk_dicts


def optimized_get_view_definition(
    self: Any,
    connection: Connection,
    view_name: str,
    schema: Optional[str] = None,
    tables_cache: Optional[MutableMapping[str, List[TeradataTable]]] = None,
    **kw: Dict[str, Any],
) -> Optional[str]:
    tables_cache = tables_cache or {}
    if schema is None:
        schema = self.default_schema_name

    schema_key = schema.lower()
    if schema_key not in tables_cache:
        return None

    for table in tables_cache[schema_key]:
        if table.name == view_name:
            return self.normalize_name(table.request_text)

    return None


@dataclass
class TeradataReport(SQLSourceReport, BaseTimeWindowReport):
    # Column extraction metrics
    num_columns_processed: int = 0
    num_column_extraction_failures: int = 0
    num_primary_keys_processed: int = 0
    column_extraction_duration_seconds: float = 0.0

    # View processing metrics (actively used)
    num_views_processed: int = 0
    num_view_processing_failures: int = 0
    num_view_processing_timeouts: int = 0
    view_extraction_total_time_seconds: float = 0.0
    view_extraction_average_time_seconds: float = 0.0
    slowest_view_processing_time_seconds: float = 0.0
    slowest_view_name: TopKDict[str, float] = field(default_factory=TopKDict)
    stalled_views: TopKDict[str, float] = field(default_factory=TopKDict)

    # Connection pool performance metrics (actively used)
    connection_pool_wait_time_seconds: float = 0.0
    connection_pool_max_wait_time_seconds: float = 0.0
    num_pool_timeout_retries: int = 0

    # Database-level metrics similar to BigQuery's approach (actively used)
    num_database_tables_to_scan: TopKDict[str, int] = field(default_factory=TopKDict)
    num_database_views_to_scan: TopKDict[str, int] = field(default_factory=TopKDict)

    # Tables excluded from profiling because their size exceeds
    profiling_skipped_size_limit: TopKDict[str, int] = field(default_factory=TopKDict)

    # Global metadata extraction timing (single query for all databases)
    metadata_extraction_total_sec: float = 0.0

    # Lineage extraction query time range (actively used)
    lineage_start_time: Optional[datetime] = None
    lineage_end_time: Optional[datetime] = None

    # Audit query processing statistics
    num_audit_query_entries_processed: int = 0

    # Retry statistics
    num_db_retries: int = 0

    # Single internal lock — not serialised, not compared.  Protects all report
    # fields that are mutated from ThreadPoolExecutor worker threads.
    _lock: Lock = field(default_factory=Lock, init=False, repr=False, compare=False)

    @contextmanager
    def atomic(self) -> Generator[None, None, None]:
        """Take the report lock for a multi-field atomic update."""
        with self._lock:
            yield

    def increment_db_retries(self) -> None:
        with self._lock:
            self.num_db_retries += 1

    def increment_pool_timeout_retries(self) -> None:
        with self._lock:
            self.num_pool_timeout_retries += 1

    def increment_columns_processed(self) -> None:
        with self._lock:
            self.num_columns_processed += 1

    def increment_column_extraction_failures(self) -> None:
        with self._lock:
            self.num_column_extraction_failures += 1

    def increment_primary_keys_processed(self) -> None:
        with self._lock:
            self.num_primary_keys_processed += 1

    def add_column_extraction_duration(self, seconds: float) -> None:
        with self._lock:
            self.column_extraction_duration_seconds += seconds


class BaseTeradataConfig(TwoTierSQLAlchemyConfig):
    scheme: str = Field(default="teradatasql", description="database scheme")


class TeradataConfig(BaseTeradataConfig, BaseTimeWindowConfig):
    databases: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of databases to ingest. If not specified, all databases will be ingested."
            " Even if this is specified, databases will still be filtered by `database_pattern`."
        ),
    )

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=EXCLUDED_DATABASES),
        description="Regex patterns for databases to filter in ingestion.",
    )
    include_table_lineage: bool = Field(
        default=False,
        description="Whether to include table lineage in the ingestion. "
        "This requires to have the table lineage feature enabled.",
    )

    include_view_lineage: bool = Field(
        default=True,
        description="Whether to include view lineage in the ingestion. "
        "This requires to have the view lineage feature enabled.",
    )

    include_queries: bool = Field(
        default=True,
        description="Whether to generate query entities for SQL queries. "
        "Query entities provide metadata about individual SQL queries including "
        "execution timestamps, user information, and query text.",
    )
    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    default_db: Optional[str] = Field(
        default=None,
        description="The default database to use for unqualified table names",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistic.",
    )

    use_file_backed_cache: bool = Field(
        default=True,
        description="Whether to use a file backed cache for the view definitions.",
    )

    use_qvci: bool = Field(
        default=False,
        description="Whether to use QVCI to get column information. This is faster but requires to have QVCI enabled.",
    )

    include_historical_lineage: bool = Field(
        default=False,
        description="Whether to include historical lineage data from PDCRINFO.DBQLSqlTbl_Hst in addition to current DBC.QryLogV data. "
        "This provides access to historical query logs that may have been archived. "
        "The historical table existence is checked automatically and gracefully falls back to current data only if not available.",
    )

    use_server_side_cursors: bool = Field(
        default=True,
        description="Enable server-side cursors for large result sets using SQLAlchemy's stream_results. "
        "This reduces memory usage by streaming results from the database server. "
        "Automatically falls back to client-side batching if server-side cursors are not supported.",
    )

    max_workers: int = Field(
        default=10,
        description="Maximum number of worker threads to use for parallel processing. "
        "Controls the level of concurrency for operations like view processing.",
    )

    max_pool_size: int = Field(
        default=13,
        ge=1,
        le=50,
        description=(
            "Ceiling on the number of concurrent Teradata connections used during "
            "parallel view processing. The actual pool size is "
            "min(max_workers, max_pool_size), so this value only takes effect when "
            "max_workers exceeds it. For example, max_workers=10 with max_pool_size=13 "
            "creates a pool of 10, not 13. "
            "The upper bound of 50 is a conservative ingestion-time safety ceiling, "
            "not a Teradata system limit. Teradata's per-user MAXSESSIONS parameter "
            "is typically 64–200+ depending on the platform and user profile. "
        ),
    )

    extract_ownership: bool = Field(
        default=False,
        description=(
            "Whether to extract ownership information for tables and views based on their creator. "
            "When enabled, the table/view creator from Teradata's system tables "
            "will be added as an owner with DATAOWNER type. "
            "Ownership is applied using OVERWRITE mode, meaning any existing ownership "
            "information (including manually added or modified owners from the UI) "
            "will be replaced. Use with caution."
        ),
    )

    column_extraction_watermark: Optional[datetime] = Field(
        default=None,
        description=(
            "Skip column extraction for tables/views whose LastAlterTimeStamp is older than this "
            "timestamp. Set to the start time of the last successful ingestion run to enable "
            "incremental column extraction. Mutually exclusive with column_extraction_days_back. "
            "At 13k tables where ~200 change per day this can reduce ingestion from hours to minutes."
        ),
    )

    column_extraction_days_back: Optional[int] = Field(
        default=None,
        description=(
            "Skip column extraction for tables/views not altered within the last N days. "
            "Computed at runtime as now() - N days, so the recipe never needs updating. "
            "A value of 3 for a daily schedule covers up to two missed runs with no gap risk. "
            "Mutually exclusive with column_extraction_watermark."
        ),
    )

    @field_validator("column_extraction_watermark", mode="after")
    @classmethod
    def _normalize_watermark_timezone(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Normalize watermark to a naive UTC datetime.

        Teradata's LastAlterTimeStamp is returned as a timezone-naive datetime by
        SQLAlchemy. Comparing a timezone-aware watermark against a naive timestamp
        raises TypeError at runtime. If the user supplies a timezone-aware value we
        strip the tzinfo after converting to UTC so the comparison is always safe.
        """
        if v is not None and v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v

    @model_validator(mode="after")
    def _validate_column_extraction_options(self) -> "TeradataConfig":
        if (
            self.column_extraction_watermark is not None
            and self.column_extraction_days_back is not None
        ):
            raise ValueError(
                "column_extraction_watermark and column_extraction_days_back are mutually exclusive. "
                "Set one or the other, not both."
            )
        return self

    use_dbc_columns_for_views: bool = Field(
        default=False,
        description=(
            "When True, attempt to use dbc.ColumnsV for view column metadata (faster bulk fetch) "
            "and fall back to HELP statements only for views where any column has a null/unknown "
            "ColumnType (e.g., derived expression columns). Can cut HELP calls by 80-90%% for "
            "installations where most view columns have explicit types. Set to False (default) "
            "to always use HELP for views, which is the conservative but slower approach."
        ),
    )

    request_timeout_ms: int = Field(
        default=120000,
        description=(
            "Request timeout in milliseconds for Teradata query execution. "
            "Increase this when queries against large system tables (e.g., DBC.QryLogV) time out "
            "silently and fall back. Default is 120000 (2 minutes)."
        ),
    )

    connect_timeout_ms: int = Field(
        default=30000,
        description=(
            "Connection timeout in milliseconds when establishing Teradata connections. "
            "Default is 30000 (30 seconds)."
        ),
    )

    connection_pool_timeout_ms: int = Field(
        default=60000,
        ge=1,
        le=600_000,
        description=(
            "How long, in milliseconds, a worker thread will wait for a free connection "
            "from the pool before raising a PoolTimeoutError. "
            "PoolTimeoutError is a retryable condition: the connector will sleep with "
            "full-jitter exponential backoff and try again up to retry_max_attempts times. "
            "Increase this when parallel view processing saturates the pool on large "
            "schemas (watch num_pool_timeout_retries in the ingestion report). "
            "Decrease it to surface pool-exhaustion failures faster on small installations. "
            "Default is 60000 (60 seconds)."
        ),
    )

    retry_max_attempts: int = Field(
        default=_RETRY_MAX_ATTEMPTS,
        ge=1,
        le=10,
        description=(
            "Maximum total attempts (initial + retries) for retryable database operations "
            "(connect, execute, fetchmany). "
            "Retryable conditions: pool exhaustion, transaction-aborted messages, "
            "dead-socket signals at connect time, and Teradata error codes "
            "2631/3111/3120/3597/3598/3897. "
            "Permanent errors (auth failures, permission denied, object does not exist) "
            "are never retried regardless of this setting. "
            "Worst-case added latency per operation is approximately "
            "retry_max_attempts × connection_pool_timeout_ms plus backoff sleeps "
            f"(each capped at {_RETRY_BACKOFF_CAP_SECONDS}s). "
            "Increase when ingesting from a busy or flaky cluster; "
            "decrease to surface persistent errors faster. "
            "Default is 3."
        ),
    )

    retry_initial_backoff_seconds: float = Field(
        default=_RETRY_INITIAL_BACKOFF_SECONDS,
        gt=0,
        description=(
            "Seed value, in seconds, for the full-jitter exponential backoff between "
            "retry attempts. Each retry sleeps for a duration drawn uniformly from "
            f"[0, min(initial * 2^attempt, {_RETRY_BACKOFF_CAP_SECONDS})] seconds. "
            "The 30-second cap prevents runaway sleep times even when retry_max_attempts "
            "is set high (e.g. initial=1.0, attempt=10 would be 1024s without the cap). "
            "Increase this to spread retries further apart on a heavily loaded cluster; "
            "decrease it for faster recovery on transient blips. "
            "Default is 1.0."
        ),
    )

    view_processing_timeout_seconds: int = Field(
        default=1800,
        description=(
            "Maximum wall-clock time, in seconds, that a single view may spend in the "
            "parallel view-processing pool before the connector abandons it and moves on. "
            "Set to 0 to disable. Stalled views are reported as warnings and counted in "
            "`num_view_processing_timeouts`. This protects bulk ingestion from silent hangs "
            "when a Teradata query blocks indefinitely (e.g., on a dropped TCP connection). "
            "Default is 1800 (30 minutes)."
        ),
    )

    view_processing_heartbeat_seconds: int = Field(
        default=30,
        description=(
            "How often, in seconds, to emit a 'view processing heartbeat' log line during "
            "parallel view processing. The heartbeat reports completed/in-progress counts "
            "and the longest-running view, making it possible to diagnose silent halts in "
            "the executor. Set to 0 to disable. Default is 30 seconds."
        ),
    )

    lineage_fetch_stall_warning_seconds: int = Field(
        default=300,
        description=(
            "If no lineage row batch arrives from DBC.QryLogV within this many seconds, "
            "emit a warning identifying the stalled phase. Set to 0 to disable. "
            "Default is 300 (5 minutes)."
        ),
    )


@platform_name("Teradata")
@config_class(TeradataConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.DATABASE,
    ],
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on",
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.OWNERSHIP,
    "Optionally enabled via configuration (extract_ownership)",
)
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_FINE, "Optionally enabled via configuration")
@capability(SourceCapability.USAGE_STATS, "Optionally enabled via configuration")
@capability(
    SourceCapability.OPERATION_CAPTURE,
    "Optionally enabled via `include_usage_statistics`; controlled by `usage.include_operational_stats`",
)
class TeradataSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    """

    config: TeradataConfig

    QUERY_TEXT_CURRENT_QUERIES: str = """
    SELECT
        s.QueryID as "query_id",
        UserName as "user",
        StartTime AT TIME ZONE 'GMT' as "timestamp",
        DefaultDatabase as default_database,
        s.SqlTextInfo as "query_text",
        s.SqlRowNo as "row_no"
    FROM "DBC".QryLogV as l
    JOIN "DBC".QryLogSqlV as s on s.QueryID = l.QueryID
    WHERE
        l.ErrorCode = 0
        AND l.statementtype not in (
        'Unrecognized type',
        'Create Database/User',
        'Help',
        'Modify Database',
        'Drop Table',
        'Show',
        'Not Applicable',
        'Grant',
        'Abort',
        'Database',
        'Flush Query Logging',
        'Null',
        'Begin/End DBQL',
        'Revoke'
    )
        and "timestamp" >= TIMESTAMP '{start_time}'
        and "timestamp" < TIMESTAMP '{end_time}'
        and s.CollectTimeStamp >= TIMESTAMP '{start_time}'
        and default_database not in ('DEMONOW_MONITOR')
        {databases_filter}
    ORDER BY "timestamp", "query_id", "row_no"
    """.strip()

    QUERY_TEXT_HISTORICAL_UNION: str = """
    SELECT
        "query_id",
        "user",
        "timestamp",
        default_database,
        "query_text",
        "row_no"
    FROM (
        SELECT
            h.QueryID as "query_id",
            h.UserName as "user",
            h.StartTime AT TIME ZONE 'GMT' as "timestamp",
            h.DefaultDatabase as default_database,
            h.SqlTextInfo as "query_text",
            h.SqlRowNo as "row_no"
        FROM "PDCRINFO".DBQLSqlTbl_Hst as h
        WHERE
            h.ErrorCode = 0
            AND h.statementtype not in (
            'Unrecognized type',
            'Create Database/User',
            'Help',
            'Modify Database',
            'Drop Table',
            'Show',
            'Not Applicable',
            'Grant',
            'Abort',
            'Database',
            'Flush Query Logging',
            'Null',
            'Begin/End DBQL',
            'Revoke'
        )
            and h.StartTime AT TIME ZONE 'GMT' >= TIMESTAMP '{start_time}'
            and h.StartTime AT TIME ZONE 'GMT' < TIMESTAMP '{end_time}'
            and h.CollectTimeStamp >= TIMESTAMP '{start_time}'
            and h.DefaultDatabase not in ('DEMONOW_MONITOR')
            {databases_filter_history}

        UNION

        SELECT
            s.QueryID as "query_id",
            l.UserName as "user",
            l.StartTime AT TIME ZONE 'GMT' as "timestamp",
            l.DefaultDatabase as default_database,
            s.SqlTextInfo as "query_text",
            s.SqlRowNo as "row_no"
        FROM "DBC".QryLogV as l
        JOIN "DBC".QryLogSqlV as s on s.QueryID = l.QueryID
        WHERE
            l.ErrorCode = 0
            AND l.statementtype not in (
            'Unrecognized type',
            'Create Database/User',
            'Help',
            'Modify Database',
            'Drop Table',
            'Show',
            'Not Applicable',
            'Grant',
            'Abort',
            'Database',
            'Flush Query Logging',
            'Null',
            'Begin/End DBQL',
            'Revoke'
        )
            and l.StartTime AT TIME ZONE 'GMT' >= TIMESTAMP '{start_time}'
            and l.StartTime AT TIME ZONE 'GMT' < TIMESTAMP '{end_time}'
            and s.CollectTimeStamp >= TIMESTAMP '{start_time}'
            and l.DefaultDatabase not in ('DEMONOW_MONITOR')
            {databases_filter}
    ) as combined_results
    ORDER BY "timestamp", "query_id", "row_no"
    """.strip()

    _TABLES_AND_VIEWS_QUERY_TEMPLATE: str = """
SELECT
    t.DataBaseName,
    t.TableName as name,
    t.CommentString as description,
    CASE t.TableKind
         WHEN 'I' THEN 'Join index'
         WHEN 'N' THEN 'Hash index'
         WHEN 'T' THEN 'Table'
         WHEN 'V' THEN 'View'
         WHEN 'O' THEN 'NoPI Table'
         WHEN 'Q' THEN 'Queue table'
    END AS object_type,
    t.CreateTimeStamp,
    t.LastAlterName,
    t.LastAlterTimeStamp,
    t.RequestText,
    t.CreatorName
FROM dbc.TablesV t
WHERE DataBaseName NOT IN ({excluded_dbs}){db_allowlist}
AND t.TableKind in ('T', 'V', 'Q', 'O')
ORDER by DataBaseName, TableName;
""".strip()

    # Returns only the tables whose total size is at or above the limit. We then
    # subtract these from the full table list, so any table missing from
    # DBC.TableSizeV (new/zero-perm tables, permission asymmetry) remains a
    # profiling candidate (fail-open)
    PROFILE_OVERSIZED_TABLES_QUERY: str = """
    SELECT TableName AS name
    FROM DBC.TableSizeV
    WHERE DatabaseName (NOT CASESPECIFIC) = :schema (NOT CASESPECIFIC)
    GROUP BY TableName
    HAVING SUM(CurrentPerm) >= :size_limit_bytes
    """.strip()

    def _build_tables_and_views_query(self) -> str:
        excluded_dbs = ",".join([f"'{db}'" for db in EXCLUDED_DATABASES])

        # When config.databases is set push filtering into SQL so we only fetch
        # metadata for the configured databases. Without this, a Teradata system
        # with thousands of databases loads ALL their table/view metadata into
        # _tables_cache even though only a small subset will ever be processed.
        # On large installations this is the primary driver of OOM crashes.
        db_allowlist = ""
        if self.config.databases:
            # Teradata identifiers cannot contain single quotes; strip defensively.
            safe_names = [db.replace("'", "") for db in self.config.databases]
            # (NOT CASESPECIFIC) on both sides so the filter works even if the
            # session collation is set to CASESPECIFIC (rare but seen in compliance-
            # configured installations); the audit-log query below uses the same idiom.
            db_allowlist = "\nAND DataBaseName (NOT CASESPECIFIC) IN ({})".format(
                ",".join(f"'{db}' (NOT CASESPECIFIC)" for db in safe_names)
            )

        return self._TABLES_AND_VIEWS_QUERY_TEMPLATE.format(
            excluded_dbs=excluded_dbs,
            db_allowlist=db_allowlist,
        )

    _tables_cache: MutableMapping[str, List[TeradataTable]] = defaultdict(list)
    # Cache mapping (schema, entity_name) -> creator_name for table/view ownership
    _table_creator_cache: MutableMapping[Tuple[str, str], str] = {}
    _tables_cache_lock = Lock()  # Protect shared cache from concurrent access
    _pooled_engine: Optional[Engine] = None  # Reusable pooled engine
    _pooled_engine_lock = Lock()  # Protect engine creation

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

        self.report: TeradataReport = TeradataReport()
        self.graph: Optional[DataHubGraph] = ctx.graph
        # Populated by cache_tables_and_views() when column_extraction_watermark is set;
        # None means "extract all", a set means "only extract these (schema, table) pairs"
        self._tables_needing_column_extraction: Optional[Set[Tuple[str, str]]] = None
        # May be reduced below config.max_workers when max_pool_size caps the pool.
        # Stored here instead of mutating config so that config remains an immutable
        # record of user intent across sequential recipe runs.
        self._effective_max_workers: int = config.max_workers

        self.schema_resolver = self._init_schema_resolver()

        # Initialize SqlParsingAggregator for modern lineage processing
        logger.info("Initializing SqlParsingAggregator for enhanced lineage processing")
        self.aggregator = SqlParsingAggregator(
            platform="teradata",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.schema_resolver,
            graph=self.ctx.graph,
            generate_lineage=self.config.include_view_lineage
            or self.config.include_table_lineage,
            generate_queries=self.config.include_queries,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_query_usage_statistics=self.config.include_usage_statistics,
            generate_operations=self.config.usage.include_operational_stats
            if self.config.include_usage_statistics
            else False,
            usage_config=self.config.usage
            if self.config.include_usage_statistics
            else None,
            eager_graph_load=False,
        )
        self.report.sql_aggregator = self.aggregator.report

        if self.config.include_tables or self.config.include_views:
            with self.report.new_stage("Table and view discovery"):
                self.cache_tables_and_views()
                logger.info(f"Found {len(self._tables_cache)} tables and views")
            setattr(self, "loop_tables", self.cached_loop_tables)  # noqa: B010
            setattr(self, "loop_views", self.cached_loop_views)  # noqa: B010
            setattr(  # noqa: B010
                self, "get_table_properties", self.cached_get_table_properties
            )

            tables_cache = self._tables_cache
            # Capture config values now (before lambda shadows 'self' with TeradataDialect instance)
            _use_qvci = self.config.use_qvci
            _use_dbc_columns_for_views = self.config.use_dbc_columns_for_views
            _tables_needing_extraction = self._tables_needing_column_extraction
            setattr(  # noqa: B010
                TeradataDialect,
                "get_columns",
                lambda self,
                connection,
                table_name,
                schema=None,
                use_qvci=_use_qvci,
                use_dbc_columns_for_views=_use_dbc_columns_for_views,
                tables_needing_extraction=_tables_needing_extraction,
                **kw: optimized_get_columns(
                    self,
                    connection,
                    table_name,
                    schema,
                    tables_cache=tables_cache,
                    use_qvci=use_qvci,
                    use_dbc_columns_for_views=use_dbc_columns_for_views,
                    tables_needing_extraction=tables_needing_extraction,
                    **kw,
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_pk_constraint",
                lambda self,
                connection,
                table_name,
                schema=None,
                **kw: optimized_get_pk_constraint(
                    self, connection, table_name, schema, **kw
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_foreign_keys",
                lambda self,
                connection,
                table_name,
                schema=None,
                **kw: optimized_get_foreign_keys(
                    self, connection, table_name, schema, **kw
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_columns",
                lambda self, connection, dbc_columns, schema: get_schema_columns(
                    self, connection, dbc_columns, schema
                ),
            )

            # Disabling the below because the cached view definition is not the view definition the column in tablesv actually holds the last statement executed against the object... not necessarily the view definition
            # setattr(
            #   TeradataDialect,
            #    "get_view_definition",
            #   lambda self, connection, view_name, schema=None, **kw: optimized_get_view_definition(
            #        self, connection, view_name, schema, tables_cache=tables_cache, **kw
            #    ),
            # )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_pk_constraints",
                lambda self, connection, schema: get_schema_pk_constraints(
                    self, connection, schema
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_foreign_keys",
                lambda self, connection, schema: get_schema_foreign_keys(
                    self, connection, schema
                ),
            )
        else:
            logger.info(
                "Disabling stale entity removal as tables and views are disabled"
            )
            if self.config.stateful_ingestion:
                self.config.stateful_ingestion.remove_stale_metadata = False

    def _add_default_options(self, sql_config: SQLCommonConfig) -> None:
        """Add Teradata-specific default options"""
        super()._add_default_options(sql_config)
        if sql_config.is_profiling_enabled():
            # Sqlalchemy uses QueuePool by default however Teradata uses SingletonThreadPool.
            # SingletonThreadPool does not support parellel connections. For using profiling, we need to use QueuePool.
            # https://docs.sqlalchemy.org/en/20/core/pooling.html#connection-pool-configuration
            # https://github.com/Teradata/sqlalchemy-teradata/issues/96
            sql_config.options.setdefault("poolclass", QueuePool)

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        identifier = f"{schema}.{entity}"
        if self.config.convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    @classmethod
    def create(cls, config_dict, ctx):
        config = TeradataConfig.model_validate(config_dict)
        return cls(config, ctx)

    # ------------------------------------------------------------------
    # Retry helpers — thin wrappers that bind config values so call sites
    # don't repeat max_attempts / initial_backoff_seconds everywhere.
    # ------------------------------------------------------------------

    @contextmanager
    def _retry_connect(
        self, engine: Engine, max_attempts: Optional[int] = None
    ) -> Generator[Connection, None, None]:
        """Acquire a connection with config-driven retry / backoff."""
        with _engine_connect_with_retry(
            engine,
            max_attempts=(
                max_attempts
                if max_attempts is not None
                else self.config.retry_max_attempts
            ),
            initial_backoff_seconds=self.config.retry_initial_backoff_seconds,
            report=self.report,
        ) as conn:
            yield conn

    def _retry_execute(
        self,
        conn: Connection,
        stmt: Any,
        params: Optional[Dict[str, Any]] = None,
        warn_on_permanent_failure: bool = True,
    ) -> Any:
        """Execute a statement with config-driven retry / backoff."""
        return _execute_with_retry(
            conn,
            stmt,
            params,
            max_attempts=self.config.retry_max_attempts,
            initial_backoff_seconds=self.config.retry_initial_backoff_seconds,
            report=self.report,
            warn_on_permanent_failure=warn_on_permanent_failure,
        )

    def _retry_fetchmany(self, result: Any, batch_size: int) -> List[Any]:
        """Fetch a batch with config-driven retry / backoff."""
        return _fetchmany_with_retry(
            result,
            batch_size,
            max_attempts=self.config.retry_max_attempts,
            initial_backoff_seconds=self.config.retry_initial_backoff_seconds,
            report=self.report,
        )

    def _get_schema_names_with_retry(self, engine: Engine) -> List[str]:
        """Fetch schema names, retrying the full connect+query sequence on transient errors.

        Both engine.connect() and get_schema_names() can fail transiently.
        _retry_connect only retries the connect step; failures inside the ``with``
        block propagate out unretried.  This method owns the outer retry loop so
        that a transient blip during either step triggers a fresh attempt from
        scratch rather than aborting schema discovery entirely.

        max_attempts=1 is passed to _retry_connect to prevent a nested retry
        layer that would otherwise produce up to retry_max_attempts² total attempts.
        """
        max_attempts = self.config.retry_max_attempts
        initial_backoff = self.config.retry_initial_backoff_seconds
        for attempt in range(max_attempts):
            try:
                with self._retry_connect(engine, max_attempts=1) as conn:
                    return inspect(conn).get_schema_names()
            except Exception as exc:
                if not _should_retry_connect(exc) or attempt == max_attempts - 1:
                    self.report.failure(
                        title="Schema discovery failed"
                        if attempt == 0
                        else "Schema discovery failed after retries",
                        message=(
                            f"Could not list schemas after {attempt + 1} attempt(s). "
                            "Check network connectivity, authentication, and "
                            "database permissions."
                        ),
                        context=str(exc),
                        exc=exc,
                    )
                    raise
                backoff = _jittered_backoff(attempt, initial_backoff)
                logger.warning(
                    f"Retryable error fetching schema names "
                    f"(attempt {attempt + 1}/{max_attempts}): {exc}. "
                    f"Retrying in {backoff:.2f}s..."
                )
                self.report.increment_db_retries()
                time.sleep(backoff)
        raise AssertionError("unreachable")  # loop always raises or returns

    def _init_schema_resolver(self) -> SchemaResolver:
        if not self.config.include_tables or not self.config.include_views:
            if self.ctx.graph:
                try:
                    return provide_schema_resolver(
                        graph=self.ctx.graph,
                        platform=self.platform,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                except Exception as e:
                    self.report.report_warning(
                        message="Failed to bulk-load schemas from DataHub for SQL lineage. "
                        "Lineage resolution will fall back to lazy on-demand schema fetching.",
                        context=str(e),
                        exc=e,
                    )
            else:
                logger.warning(
                    "Failed to load schema info from DataHub as DataHubGraph is missing.",
                )
        # Pass graph for lazy on-demand resolution of cross-recipe upstream tables.
        return SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )

    def get_inspectors(self):
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.
        url = self.config.get_sql_alchemy_url()

        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self._base_engine_options())

        try:
            # Get list of databases first.
            # When the user supplied an explicit list we avoid an unnecessary DB
            # round-trip entirely.  In the discovery path (else branch) we call
            # inspector.get_schema_names() which issues a live query; retry with a
            # fresh connection on transient failures so a single blip doesn't abort
            # the whole run.
            if self.config.database and self.config.database != "":
                databases: List[str] = [self.config.database]
            elif self.config.databases:
                databases = list(self.config.databases)
            else:
                databases = self._get_schema_names_with_retry(engine)

            # When the user supplied an explicit database list, validate each entry
            # against dbc.TablesV (populated into _tables_cache during discovery).
            # Without this, a typo in `databases` silently emits a container URN for
            # a database that does not exist on the source. Only applies when
            # discovery actually ran (include_tables or include_views).
            user_supplied_databases = bool(
                (self.config.database and self.config.database != "")
                or self.config.databases
            )
            # Only validate when discovery actually produced an inventory we can
            # check against. An empty cache means either discovery was disabled
            # (include_tables/include_views both False) or it ran and genuinely
            # found nothing — in both cases there is no oracle to validate against
            # and we fall back to trusting the user's list.
            have_db_inventory = (
                self.config.include_tables or self.config.include_views
            ) and bool(self._tables_cache)

            # Create separate connections for each database to avoid connection lifecycle issues
            for db in databases:
                if not self.config.database_pattern.allowed(db):
                    continue
                if (
                    user_supplied_databases
                    and have_db_inventory
                    and db.lower() not in self._tables_cache
                ):
                    self.report.warning(
                        title="Configured database not found on source",
                        message=(
                            f"Database {db!r} is listed in the connector config but no "
                            "tables or views were found for it in dbc.TablesV. Skipping "
                            "to avoid emitting a container URN for a database that does "
                            "not exist (or that exists but is empty). Check for typos or "
                            "remove the entry."
                        ),
                    )
                    continue
                try:
                    conn_cm = self._retry_connect(engine)
                    conn = conn_cm.__enter__()
                except Exception as e:
                    self.report.warning(
                        title="Failed to inspect database",
                        message=f"Could not acquire a connection to database {db!r} "
                        f"after {self.config.retry_max_attempts} attempts. Skipping.",
                        context=str(e),
                        exc=e,
                    )
                    continue  # do not fall through to yield with an invalid connection
                try:
                    db_inspector = inspect(conn)
                    db_inspector._datahub_database = db  # type: ignore[attr-defined]
                    yield db_inspector
                finally:
                    conn_cm.__exit__(None, None, None)

        finally:
            engine.dispose()

    def get_db_name(self, inspector: Inspector) -> str:
        if hasattr(inspector, "_datahub_database"):
            return inspector._datahub_database

        engine = inspector.engine

        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            return str(engine.url.database).strip('"')
        else:
            raise Exception("Unable to get database name from Sqlalchemy inspector")

    def cached_loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[MetadataWorkUnit]:
        setattr(  # noqa: B010
            inspector,
            "get_table_names",
            lambda schema: [
                i.name
                for i in filter(
                    lambda t: t.object_type != "View",
                    self._tables_cache.get(schema.lower(), []),
                )
            ],
        )
        yield from super().loop_tables(inspector, schema, sql_config)

    def cached_get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        cache_entries = self._tables_cache.get(schema.lower(), [])
        for entry in cache_entries:
            if entry.name == table:
                description = entry.description
                if entry.object_type == "View" and entry.request_text:
                    properties["view_definition"] = entry.request_text
                break
        return description, properties, location

    def _get_creator_for_entity(self, schema: str, entity_name: str) -> Optional[str]:
        """Get creator name for a table or view."""
        with self._tables_cache_lock:
            return self._table_creator_cache.get((schema.lower(), entity_name))

    def _emit_ownership_if_available(
        self,
        dataset_name: str,
        schema: str,
        entity_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ownership metadata for a dataset if creator information is available."""
        if not self.config.extract_ownership:
            return

        creator_name = self._get_creator_for_entity(schema, entity_name)
        if creator_name:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            owner_urn = make_user_urn(creator_name)
            yield from add_owner_to_entity_wu(
                entity_type="dataset",
                entity_urn=dataset_urn,
                owner_urn=owner_urn,
            )

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader] = None,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """Override to add ownership metadata based on table creator."""
        yield from self._emit_ownership_if_available(dataset_name, schema, table)
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """Override to add ownership metadata based on view creator."""
        yield from self._emit_ownership_if_available(dataset_name, schema, view)
        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )

    def cached_loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[MetadataWorkUnit]:
        start_time = time.time()

        # Get view names from cache
        view_names = [
            i.name
            for i in filter(
                lambda t: t.object_type == "View",
                self._tables_cache.get(schema.lower(), []),
            )
        ]
        actual_view_count = len(view_names)

        if actual_view_count == 0:
            end_time = time.time()
            processing_time = end_time - start_time
            logger.info(
                f"View processing for schema '{schema}' completed in {processing_time:.2f} seconds (0 views, 0 work units)"
            )
            return

        # Use custom threading implementation with connection pooling
        work_unit_count = 0

        for work_unit in self._loop_views_with_connection_pool(
            view_names, schema, sql_config
        ):
            work_unit_count += 1
            yield work_unit

        end_time = time.time()
        processing_time = end_time - start_time

        logger.info(
            f"View processing for schema '{schema}' completed in {processing_time:.2f} seconds ({actual_view_count} views, {work_unit_count} work units)"
        )

        # Update report timing metrics
        if hasattr(self, "report"):
            self.report.view_extraction_total_time_seconds += processing_time
            self.report.num_views_processed += actual_view_count

            # Track slowest view processing at view level (will be updated by individual view processing)
            # Note: slowest_view_name now tracks individual views, not schemas

            # Calculate average processing time per view
            if self.report.num_views_processed > 0:
                self.report.view_extraction_average_time_seconds = (
                    self.report.view_extraction_total_time_seconds
                    / self.report.num_views_processed
                )

    def _loop_views_with_connection_pool(
        self, view_names: List[str], schema: str, sql_config: SQLCommonConfig
    ) -> Iterable[Union[MetadataWorkUnit, Any]]:
        """
        Process views using individual database connections per thread for true parallelization.

        Each thread gets its own connection from a QueuePool, enabling true concurrent processing.
        """
        if self._effective_max_workers == 1:
            # Single-threaded processing - no need for complexity
            yield from self._process_views_single_threaded(
                view_names, schema, sql_config
            )
            return

        logger.info(
            f"Processing {len(view_names)} views with {self._effective_max_workers} worker threads"
        )

        # Get or create reusable pooled engine
        engine = self._get_or_create_pooled_engine()

        try:

            def process_single_view(
                view_name: str,
            ) -> List[Union[MetadataWorkUnit, Any]]:
                """Process a single view with its own database connection."""
                results: List[Union[MetadataWorkUnit, Any]] = []

                # Detailed timing measurements for bottleneck analysis
                timings = {
                    "connection_acquire": 0.0,
                    "view_processing": 0.0,
                    "work_unit_generation": 0.0,
                    "total": 0.0,
                }

                total_start = time.time()
                try:
                    # Measure connection acquisition time
                    conn_start = time.time()
                    with self._retry_connect(engine) as conn:
                        timings["connection_acquire"] = time.time() - conn_start

                        # Update connection pool metrics
                        with self.report.atomic():
                            pool_wait_time = timings["connection_acquire"]
                            self.report.connection_pool_wait_time_seconds += (
                                pool_wait_time
                            )
                            if (
                                pool_wait_time
                                > self.report.connection_pool_max_wait_time_seconds
                            ):
                                self.report.connection_pool_max_wait_time_seconds = (
                                    pool_wait_time
                                )

                        # Set database context once per connection/thread
                        # This allows HELP commands to work without requiring database in connection string
                        self._retry_execute(
                            conn,
                            text(f'DATABASE "{schema}"'),
                        )

                        # Measure view processing setup
                        processing_start = time.time()
                        thread_inspector = inspect(conn)
                        # Inherit database information for Teradata two-tier architecture
                        thread_inspector._datahub_database = schema  # type: ignore

                        dataset_name = self.get_identifier(
                            schema=schema, entity=view_name, inspector=thread_inspector
                        )

                        # Thread-safe reporting
                        with self.report.atomic():
                            self.report.report_entity_scanned(
                                dataset_name, ent_type="view"
                            )

                        if not sql_config.view_pattern.allowed(dataset_name):
                            with self.report.atomic():
                                self.report.report_dropped(dataset_name)
                            return results

                        timings["view_processing"] = time.time() - processing_start

                        # Measure work unit generation
                        wu_start = time.time()
                        for work_unit in self._process_view(
                            dataset_name=dataset_name,
                            inspector=thread_inspector,
                            schema=schema,
                            view=view_name,
                            sql_config=sql_config,
                        ):
                            results.append(work_unit)
                        timings["work_unit_generation"] = time.time() - wu_start

                    # Track individual view timing
                    timings["total"] = time.time() - total_start

                    with self.report.atomic():
                        self.report.slowest_view_name[f"{schema}.{view_name}"] = (
                            timings["total"]
                        )

                except Exception as e:
                    with self.report.atomic():
                        self.report.num_view_processing_failures += 1
                        full_traceback = traceback.format_exc()
                        logger.error(
                            f"Failed to process view {schema}.{view_name}: {str(e)}"
                        )
                        logger.error(f"Full traceback: {full_traceback}")
                        self.report.warning(
                            f"Error processing view {schema}.{view_name}",
                            context=f"View: {schema}.{view_name}, Error: {str(e)}",
                            exc=e,
                        )

                return results

            # Concurrent view processing with hang protection.
            #
            # The ThreadPoolExecutor is intentionally NOT used as a context manager:
            # its __exit__ calls shutdown(wait=True), which blocks indefinitely if any
            # worker thread is stuck (e.g., in a hung DB call). Instead we drive the
            # executor explicitly so a stalled view is abandoned after
            # view_processing_timeout_seconds and the ingestion continues. Abandoned
            # threads will exit when their underlying I/O returns or when the process
            # terminates.
            executor = ThreadPoolExecutor(
                max_workers=self._effective_max_workers,
                thread_name_prefix="teradata-view",
            )
            try:
                started_at_by_future: Dict[Future, Tuple[str, float]] = {}
                remaining_futures: Set[Future] = set()
                for view_name in view_names:
                    fut = executor.submit(process_single_view, view_name)
                    started_at_by_future[fut] = (view_name, time.time())
                    remaining_futures.add(fut)

                total_views = len(remaining_futures)
                completed_count = 0
                abandoned_count = 0
                per_view_timeout = self.config.view_processing_timeout_seconds
                heartbeat_interval = self.config.view_processing_heartbeat_seconds
                # If heartbeat is disabled, still wake periodically so we can
                # detect stalled futures within per_view_timeout granularity.
                wait_step = (
                    heartbeat_interval
                    if heartbeat_interval > 0
                    else max(min(per_view_timeout, 60), 5)
                    if per_view_timeout > 0
                    else None
                )
                last_heartbeat_at = time.time()

                while remaining_futures:
                    done_set, _ = wait(
                        remaining_futures,
                        timeout=wait_step,
                        return_when=FIRST_COMPLETED,
                    )

                    for fut in done_set:
                        view_name, _started = started_at_by_future.pop(fut)
                        remaining_futures.discard(fut)
                        try:
                            # Future is already complete here, but use a small
                            # timeout as a belt-and-suspenders guard.
                            results = fut.result(timeout=1)
                            for result in results:
                                yield result
                        except Exception as e:
                            with self.report.atomic():
                                self.report.warning(
                                    "Error in thread processing view",
                                    context=f"{schema}.{view_name}",
                                    exc=e,
                                )
                        completed_count += 1

                    # Abandon any view that has exceeded the per-view timeout.
                    if per_view_timeout > 0 and started_at_by_future:
                        now = time.time()
                        stalled = [
                            (fut, name, now - started)
                            for fut, (name, started) in list(
                                started_at_by_future.items()
                            )
                            if now - started > per_view_timeout
                        ]
                        for fut, name, elapsed in stalled:
                            logger.error(
                                f"Abandoning view {schema}.{name} after "
                                f"{elapsed:.0f}s (view_processing_timeout_seconds="
                                f"{per_view_timeout}). The worker thread may still "
                                f"be blocked in I/O; it will be released when the "
                                f"underlying call returns or the process exits."
                            )
                            with self.report.atomic():
                                self.report.num_view_processing_timeouts += 1
                                self.report.stalled_views[f"{schema}.{name}"] = elapsed
                                self.report.warning(
                                    "View processing timed out",
                                    context=(
                                        f"{schema}.{name} did not complete within "
                                        f"{per_view_timeout}s (ran for "
                                        f"{elapsed:.0f}s)"
                                    ),
                                )
                            fut.cancel()
                            started_at_by_future.pop(fut, None)
                            remaining_futures.discard(fut)
                            abandoned_count += 1

                    # Periodic heartbeat so silent halts surface in logs.
                    now = time.time()
                    if (
                        heartbeat_interval > 0
                        and remaining_futures
                        and now - last_heartbeat_at >= heartbeat_interval
                    ):
                        in_progress = len(started_at_by_future)
                        longest_name: Optional[str] = None
                        longest_elapsed = 0.0
                        for name, started in started_at_by_future.values():
                            elapsed = now - started
                            if elapsed > longest_elapsed:
                                longest_elapsed = elapsed
                                longest_name = name
                        longest_suffix = (
                            f", longest_running={schema}.{longest_name} "
                            f"({longest_elapsed:.0f}s)"
                            if longest_name is not None
                            else ""
                        )
                        logger.info(
                            f"View processing heartbeat: schema={schema}, "
                            f"completed={completed_count}/{total_views}, "
                            f"in_progress={in_progress}, "
                            f"abandoned={abandoned_count}{longest_suffix}"
                        )
                        last_heartbeat_at = now
            finally:
                # cancel_futures=True cancels pending (not-yet-started) tasks.
                # wait=False ensures we never block on a hung worker thread —
                # essential for the hang-protection guarantee above.
                executor.shutdown(wait=False, cancel_futures=True)

        finally:
            # Don't dispose the reusable engine here - it will be cleaned up in close()
            pass

    def _process_views_single_threaded(
        self, view_names: List[str], schema: str, sql_config: SQLCommonConfig
    ) -> Iterable[Union[MetadataWorkUnit, Any]]:
        """Process views sequentially with a single connection."""
        engine = self.get_metadata_engine()

        try:
            with self._retry_connect(engine) as conn:
                inspector = inspect(conn)

                # Set database context once for all views in this schema
                # This allows HELP commands to work without requiring database in connection string
                self._retry_execute(
                    conn,
                    text(f'DATABASE "{schema}"'),
                )
                logger.debug(f"Set database context to {schema} for view processing")

                for view_name in view_names:
                    view_start_time = time.time()
                    try:
                        dataset_name = self.get_identifier(
                            schema=schema, entity=view_name, inspector=inspector
                        )

                        self.report.report_entity_scanned(dataset_name, ent_type="view")

                        if not sql_config.view_pattern.allowed(dataset_name):
                            self.report.report_dropped(dataset_name)
                            continue

                        # Process the view and yield results
                        for work_unit in self._process_view(
                            dataset_name=dataset_name,
                            inspector=inspector,
                            schema=schema,
                            view=view_name,
                            sql_config=sql_config,
                        ):
                            yield work_unit

                        # Track individual view timing
                        view_end_time = time.time()
                        view_processing_time = view_end_time - view_start_time
                        self.report.slowest_view_name[f"{schema}.{view_name}"] = (
                            view_processing_time
                        )

                    except Exception as e:
                        full_traceback = traceback.format_exc()
                        logger.error(
                            f"Failed to process view {schema}.{view_name}: {str(e)}"
                        )
                        logger.error(f"Full traceback: {full_traceback}")
                        self.report.warning(
                            f"Error processing view {schema}.{view_name}",
                            context=f"View: {schema}.{view_name}, Error: {str(e)}",
                            exc=e,
                        )

        finally:
            engine.dispose()

    def _get_or_create_pooled_engine(self) -> Engine:
        """Get or create a reusable SQLAlchemy engine with QueuePool for concurrent connections."""
        with self._pooled_engine_lock:
            if self._pooled_engine is None:
                url = self.config.get_sql_alchemy_url()

                # Optimal connection pool sizing to match max_workers exactly
                # Teradata driver can be sensitive to high connection counts, so cap at reasonable limit
                max_safe_connections = self.config.max_pool_size

                # Adjust max_workers to match available connection pool capacity
                effective_max_workers = min(
                    self.config.max_workers, max_safe_connections
                )

                # Set pool size to match effective workers for optimal performance
                base_connections = min(
                    effective_max_workers, 8
                )  # Reasonable base connections
                max_overflow = (
                    effective_max_workers - base_connections
                )  # Remaining as overflow

                # Log adjustment if max_workers was reduced
                if effective_max_workers < self.config.max_workers:
                    logger.warning(
                        f"Reduced max_workers from {self.config.max_workers} to {effective_max_workers} to match Teradata connection pool capacity"
                    )

                self._effective_max_workers = effective_max_workers

                pool_options = {
                    **self._base_engine_options(),
                    "poolclass": QueuePool,
                    "pool_size": base_connections,
                    "max_overflow": max_overflow,
                    "pool_pre_ping": True,
                    "pool_recycle": 1800,
                    "pool_reset_on_return": "rollback",
                }
                # Use setdefault so that a user-supplied pool_timeout in
                # config.options is not silently overridden by the default.
                pool_options.setdefault(
                    "pool_timeout", self.config.connection_pool_timeout_ms / 1000
                )

                self._pooled_engine = create_engine(url, **pool_options)
                logger.info(
                    f"Created optimized Teradata connection pool: {base_connections} base + {max_overflow} overflow = {base_connections + max_overflow} max connections (matching {effective_max_workers} workers)"
                )

            return self._pooled_engine

    def cache_tables_and_views(self) -> None:
        with self.report.new_stage("Cache tables and views"):
            engine = self.get_metadata_engine()
            try:
                database_counts: Dict[str, Dict[str, int]] = defaultdict(
                    lambda: {"tables": 0, "views": 0}
                )

                watermark = self.config.column_extraction_watermark
                if (
                    watermark is None
                    and self.config.column_extraction_days_back is not None
                ):
                    # Use Teradata's CURRENT_TIMESTAMP so the watermark is in the same
                    # time reference as LastAlterTimeStamp. Using the client clock risks
                    # skew when the client and server are in different timezones.
                    with self._retry_connect(engine) as conn:
                        ts_row = self._retry_execute(
                            conn,
                            text("SELECT CURRENT_TIMESTAMP(0)"),
                        ).fetchone()
                    td_now = ts_row[0] if ts_row else datetime.now()
                    if hasattr(td_now, "tzinfo") and td_now.tzinfo is not None:
                        td_now = td_now.replace(tzinfo=None)
                    watermark = td_now - timedelta(
                        days=self.config.column_extraction_days_back
                    )
                if watermark is not None:
                    # Initialise the set; only tables altered at or after the watermark are added
                    self._tables_needing_column_extraction = set()
                    logger.info(
                        f"Incremental column extraction enabled with watermark {watermark}. "
                        "Tables/views with LastAlterTimeStamp before the watermark will be skipped."
                    )

                with self._retry_connect(engine) as conn:
                    result = self._retry_execute(
                        conn,
                        text(self._build_tables_and_views_query()),
                    )
                    batch_size = 5000
                    while True:
                        batch = self._retry_fetchmany(result, batch_size)
                        if not batch:
                            break
                        for entry in batch:
                            table = TeradataTable(
                                database=entry.DataBaseName.strip(),
                                name=entry.name.strip(),
                                description=entry.description.strip()
                                if entry.description
                                else None,
                                object_type=entry.object_type,
                                create_timestamp=entry.CreateTimeStamp,
                                last_alter_name=entry.LastAlterName,
                                last_alter_timestamp=entry.LastAlterTimeStamp,
                                request_text=(
                                    entry.RequestText.strip()
                                    if entry.object_type == "View" and entry.RequestText
                                    else None
                                ),
                            )

                            # Count objects per database for metrics
                            if table.object_type == "View":
                                database_counts[table.database]["views"] += 1
                            else:
                                database_counts[table.database]["tables"] += 1

                            with self._tables_cache_lock:
                                # Cache key is lowercased so lookups by schema name from
                                # config.databases (case as the user typed it) match entries
                                # populated from dbc.TablesV (returned in Teradata's stored case).
                                self._tables_cache[table.database.lower()].append(table)
                                creator_name = (entry.CreatorName or "").strip()
                                if creator_name:
                                    self._table_creator_cache[
                                        (table.database.lower(), table.name)
                                    ] = creator_name

                            # Track which tables need column extraction under incremental mode
                            if (
                                watermark is not None
                                and self._tables_needing_column_extraction is not None
                            ):
                                last_alter = table.last_alter_timestamp
                                # Include when timestamp is missing (conservative) or at/after watermark
                                if last_alter is None or last_alter >= watermark:
                                    self._tables_needing_column_extraction.add(
                                        (table.database.lower(), table.name)
                                    )

                if self._tables_needing_column_extraction is not None:
                    total = sum(len(tables) for tables in self._tables_cache.values())
                    changed = len(self._tables_needing_column_extraction)
                    logger.info(
                        f"Incremental extraction: {changed}/{total} tables/views have changed "
                        f"since watermark and will have columns extracted."
                    )

                for database, counts in database_counts.items():
                    self.report.num_database_tables_to_scan[database] = counts["tables"]
                    self.report.num_database_views_to_scan[database] = counts["views"]

            finally:
                engine.dispose()

    def _reconstruct_queries_streaming(
        self, entries: Iterable[Any]
    ) -> Iterable[ObservedQuery]:
        """Reconstruct complete queries from database entries in streaming fashion.

        This method processes entries in order and reconstructs multi-row queries
        by concatenating rows with the same query_id.
        """
        current_query_id = None
        current_query_parts = []
        current_query_metadata = None

        for entry in entries:
            # Count each audit query entry processed
            self.report.num_audit_query_entries_processed += 1

            query_id = getattr(entry, "query_id", None)
            query_text = str(getattr(entry, "query_text", ""))

            if query_id != current_query_id:
                # New query started - yield the previous one if it exists
                if current_query_id is not None and current_query_parts:
                    yield self._create_observed_query_from_parts(
                        current_query_parts, current_query_metadata
                    )

                # Start new query
                current_query_id = query_id
                current_query_parts = [query_text] if query_text else []
                current_query_metadata = entry
            else:
                # Same query - append the text
                if query_text:
                    current_query_parts.append(query_text)

        # Yield the last query if it exists
        if current_query_id is not None and current_query_parts:
            yield self._create_observed_query_from_parts(
                current_query_parts, current_query_metadata
            )

    def _create_observed_query_from_parts(
        self, query_parts: List[str], metadata_entry: Any
    ) -> ObservedQuery:
        """Create ObservedQuery from reconstructed query parts and metadata."""
        # Join all parts to form the complete query
        # Teradata fragments are split at fixed lengths without artificial breaks
        full_query_text = "".join(query_parts)

        # Extract metadata
        session_id = getattr(metadata_entry, "session_id", None)
        timestamp = getattr(metadata_entry, "timestamp", None)
        user = getattr(metadata_entry, "user", None)
        default_database = getattr(metadata_entry, "default_database", None)

        # Apply Teradata-specific query transformations
        cleaned_query = NOT_CASESPECIFIC_PATTERN.sub("", full_query_text)

        # For Teradata's two-tier architecture (database.table), we should not set default_db
        # to avoid incorrect URN generation like "dbc.database.table" instead of "database.table"
        # The SQL parser will treat database.table references correctly without default_db
        return ObservedQuery(
            query=cleaned_query,
            session_id=session_id,
            timestamp=timestamp,
            user=CorpUserUrn(user) if user else None,
            default_db=DEFAULT_NO_DATABASE_TERADATA,  # Teradata uses two-tier database.table naming without default database prefixing
            default_schema=default_database,
        )

    def _convert_entry_to_observed_query(self, entry: Any) -> ObservedQuery:
        """Convert database query entry to ObservedQuery for SqlParsingAggregator.

        DEPRECATED: This method is deprecated in favor of _reconstruct_queries_streaming
        which properly handles multi-row queries. This method does not handle queries
        that span multiple rows correctly and should not be used.
        """
        # Extract fields from database result
        query_text = str(entry.query_text).strip()
        session_id = getattr(entry, "session_id", None)
        timestamp = getattr(entry, "timestamp", None)
        user = getattr(entry, "user", None)
        default_database = getattr(entry, "default_database", None)

        # Apply Teradata-specific query transformations
        cleaned_query = NOT_CASESPECIFIC_PATTERN.sub("", query_text)

        # For Teradata's two-tier architecture (database.table), we should not set default_db
        # to avoid incorrect URN generation like "dbc.database.table" instead of "database.table"
        # However, we should set default_schema for unqualified table references
        return ObservedQuery(
            query=cleaned_query,
            session_id=session_id,
            timestamp=timestamp,
            user=CorpUserUrn(user) if user else None,
            default_db=DEFAULT_NO_DATABASE_TERADATA,  # Teradata uses two-tier database.table naming without default database prefixing
            default_schema=default_database,  # Set default_schema for unqualified table references
        )

    def _fetch_lineage_entries_chunked(self) -> Iterable[Any]:
        """Fetch lineage entries using server-side cursor to handle large result sets efficiently."""
        queries = self._make_lineage_queries()

        # Stall-detection watchdog. Running fetchmany() against DBC.QryLogV can
        # hang silently on large installations (server-side query stuck, dropped
        # TCP connection, etc.); without this the connector goes dark with no
        # log output between batches. The watchdog runs on a daemon thread and
        # only writes log lines — it does not interrupt the fetch.
        stall_seconds = self.config.lineage_fetch_stall_warning_seconds
        phase_state: Dict[str, Any] = {
            "phase": "starting",
            "query_index": 0,
            "last_event_at": time.time(),
        }
        phase_state_lock = Lock()
        watchdog_stop = Event()

        def _watchdog() -> None:
            check_interval = max(min(stall_seconds, 60), 10)
            while not watchdog_stop.wait(check_interval):
                with phase_state_lock:
                    phase = phase_state["phase"]
                    query_index = phase_state["query_index"]
                    elapsed = time.time() - phase_state["last_event_at"]
                if phase == "completed":
                    return
                if elapsed > stall_seconds:
                    logger.warning(
                        f"Lineage fetch stall: no progress in {elapsed:.0f}s "
                        f"(phase={phase}, query_index={query_index}). The "
                        f"Teradata cursor may be blocked or the query is still "
                        f"executing on the server. Investigate "
                        f"DBC.SessionInfoV / network keepalive if this persists."
                    )

        watchdog_thread: Optional[Thread] = None
        if stall_seconds > 0:
            watchdog_thread = Thread(
                target=_watchdog,
                daemon=True,
                name="teradata-lineage-watchdog",
            )
            watchdog_thread.start()

        def _mark_phase(phase: str, query_index: int = 0) -> None:
            with phase_state_lock:
                phase_state["phase"] = phase
                phase_state["query_index"] = query_index
                phase_state["last_event_at"] = time.time()

        fetch_engine = self.get_metadata_engine()
        try:
            with self._retry_connect(fetch_engine) as conn:
                cursor_type = (
                    "server-side"
                    if self.config.use_server_side_cursors
                    else "client-side"
                )

                total_count_all_queries = 0

                for query_index, query in enumerate(queries, 1):
                    logger.info(
                        f"Executing lineage query {query_index}/{len(queries)} for time range {self.config.start_time} to {self.config.end_time} with {cursor_type} cursor..."
                    )
                    _mark_phase("executing_query", query_index)

                    # Use helper method to try server-side cursor with fallback
                    result = self._execute_with_cursor_fallback(conn, query)
                    _mark_phase("awaiting_first_batch", query_index)

                    # Stream results in batches to avoid memory issues
                    batch_size = 5000
                    batch_count = 0
                    query_total_count = 0

                    while True:
                        # _fetchmany_with_retry handles transient errors that leave
                        # the cursor intact (e.g. a brief network hiccup).  It does
                        # NOT recover a server-side cursor whose position has been
                        # lost — in that case retries exhaust and the exception
                        # propagates to the outer except block.  The stall-detection
                        # watchdog above covers the complementary failure mode where
                        # fetchmany() hangs rather than raises.
                        batch = self._retry_fetchmany(result, batch_size)
                        if not batch:
                            break

                        batch_count += 1
                        query_total_count += len(batch)
                        total_count_all_queries += len(batch)
                        _mark_phase("fetching_batches", query_index)

                        logger.info(
                            f"Query {query_index} - Fetched batch {batch_count}: {len(batch)} lineage entries (query total: {query_total_count})"
                        )
                        yield from batch

                    logger.info(
                        f"Completed query {query_index}: {query_total_count} lineage entries in {batch_count} batches"
                    )

                logger.info(
                    f"Completed fetching all queries: {total_count_all_queries} total lineage entries from {len(queries)} queries"
                )
                _mark_phase("completed")

        except Exception as e:
            self.report.warning(
                title="Lineage fetch failed",
                message=(
                    "Failed to fetch lineage entries from Teradata audit logs. "
                    "Lineage data for this run will be incomplete. "
                    "Check Teradata connectivity and DBC.QryLogV access."
                ),
                context=str(e),
                exc=e,
            )
            raise
        finally:
            watchdog_stop.set()
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=5)
            fetch_engine.dispose()

    def _check_historical_table_exists(self) -> bool:
        """
        Check if the PDCRINFO.DBQLSqlTbl_Hst table exists and is accessible.
        DBQL rows are periodically moved to history table and audit queries might not exist in DBC already.
        There is not guarantee that the historical table exists, so we need to check it.

        Returns:
            bool: True if the historical table exists and is accessible, False otherwise.
        """
        engine = self.get_metadata_engine()
        try:
            # Use a simple query to check if the table exists and is accessible
            check_query = """
                SELECT TOP 1 QueryID 
                FROM PDCRINFO.DBQLSqlTbl_Hst 
                WHERE 1=0
            """
            with self._retry_connect(engine) as conn:
                # Probe query — the except block below owns all severity
                # decisions, so suppress the default permanent-failure warning.
                self._retry_execute(
                    conn, text(check_query), warn_on_permanent_failure=False
                )
                logger.info(
                    "Historical lineage table PDCRINFO.DBQLSqlTbl_Hst is available"
                )
                return True
        except Exception as e:
            if isinstance(e, PoolTimeoutError):
                self.report.warning(
                    title="Connection pool exhausted checking historical lineage table",
                    message=(
                        f"Could not acquire a connection to verify PDCRINFO.DBQLSqlTbl_Hst "
                        f"after {self.config.retry_max_attempts} attempts — the connection pool "
                        f"was exhausted. Historical lineage will be skipped for this run. "
                        f"Consider increasing connection_pool_timeout_ms or reducing max_workers."
                    ),
                    exc=e,
                )
            elif _should_retry_connect(e):
                self.report.warning(
                    title="Historical lineage table unreachable",
                    message=(
                        f"Historical lineage table PDCRINFO.DBQLSqlTbl_Hst check failed "
                        f"after {self.config.retry_max_attempts} attempts due to a transient "
                        f"error: {e}. Historical lineage will be skipped for this run."
                    ),
                    exc=e,
                )
            else:
                logger.info(
                    f"Historical lineage table PDCRINFO.DBQLSqlTbl_Hst is not available: {e}"
                )
            return False
        finally:
            engine.dispose()

    def _make_lineage_queries(self) -> List[str]:
        if self.config.databases:
            scoped_databases = self.config.databases
        elif self._tables_cache:
            # Derive the scope from databases discovered during cache_tables_and_views(),
            # filtered by database_pattern. This avoids scanning the entire DBC.QryLogV
            # audit log (which is enormous on large installations) when the user hasn't
            # set config.databases explicitly but has a database_pattern allowlist.
            scoped_databases = [
                db
                for db in self._tables_cache
                if self.config.database_pattern.allowed(db)
            ]
        else:
            scoped_databases = []

        # Use NOT CASESPECIFIC so the filter matches regardless of how the database
        # name is stored in DBC.QryLogV (Teradata stores it uppercase by default).
        databases_filter = (
            "and l.DefaultDatabase (NOT CASESPECIFIC) in ({databases})".format(
                databases=",".join(
                    [f"'{db}' (NOT CASESPECIFIC)" for db in scoped_databases]
                )
            )
            if scoped_databases
            else ""
        )

        queries = []

        # Check if historical lineage is configured and available
        if (
            self.config.include_historical_lineage
            and self._check_historical_table_exists()
        ):
            logger.info(
                "Using UNION query to combine historical and current lineage data to avoid duplicates"
            )
            # For historical query, we need the database filter for historical part
            databases_filter_history = (
                databases_filter.replace("l.DefaultDatabase", "h.DefaultDatabase")
                if databases_filter
                else ""
            )

            union_query = self.QUERY_TEXT_HISTORICAL_UNION.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
                databases_filter_history=databases_filter_history,
            )
            queries.append(union_query)
        else:
            if self.config.include_historical_lineage:
                logger.warning(
                    "Historical lineage was requested but PDCRINFO.DBQLSqlTbl_Hst table is not available. Falling back to current data only."
                )

            # Use current-only query when historical data is not available
            current_query = self.QUERY_TEXT_CURRENT_QUERIES.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
            )
            queries.append(current_query)

        return queries

    def _base_engine_options(self) -> Dict[str, Any]:
        """Return engine kwargs shared by all engines (pooled and non-pooled).

        Only includes options accepted by every pool class. pool_timeout is
        QueuePool-specific and must be added separately for pooled engines;
        passing it to SingletonThreadPool (used by the schema-discovery engine)
        raises an Invalid argument error at create_engine() time.
        """
        opts: Dict[str, Any] = dict(self.config.options)
        connect_args = dict(opts.pop("connect_args", {}))
        connect_args.setdefault("connect_timeout", str(self.config.connect_timeout_ms))
        connect_args.setdefault("request_timeout", str(self.config.request_timeout_ms))
        opts["connect_args"] = connect_args
        return opts

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self._base_engine_options())

    def _execute_with_cursor_fallback(
        self, connection: Connection, query: str, params: Optional[Dict] = None
    ) -> Any:
        """
        Execute query with server-side cursor if enabled and supported, otherwise fall back to regular execution.

        Args:
            connection: Database connection
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result object
        """
        if self.config.use_server_side_cursors:
            try:
                streaming_conn = connection.execution_options(stream_results=True)
                result = self._retry_execute(
                    streaming_conn,
                    text(query),
                    params=params,
                )
                logger.debug(
                    "Successfully using server-side cursor for query execution"
                )
                return result

            except NotSupportedError as e:
                # Driver explicitly signals that server-side cursors / stream_results
                # are not supported.  Fall back to client-side buffering.
                self.report.warning(
                    title="Server-side cursor not supported — falling back to client-side execution",
                    message=(
                        "stream_results=True is not supported by this Teradata driver version. "
                        "Client-side buffering will be used instead, which may increase memory usage "
                        "for large result sets. Consider setting use_server_side_cursors: false."
                    ),
                    exc=e,
                )
            except (OperationalError, DatabaseError) as e:
                msg = str(e).lower()
                # Only fall back when the error clearly indicates that the
                # server-side cursor / streaming mode itself is not supported.
                # Require BOTH a "not supported / unsupported" token AND a
                # "cursor / stream" token to co-occur so that SQL text which
                # incidentally contains "cursor" or "stream" (e.g. a CTE named
                # stream_events, or a DBC query mentioning cursors in
                # RequestText) is not mis-classified as a cursor-mode failure.
                # All other OperationalError / DatabaseError instances (auth
                # failures, "database does not exist", SQL syntax errors, OOM)
                # must propagate so the caller can handle or surface them.
                _not_supported = "not supported" in msg or "unsupported" in msg
                _cursor_or_stream = "cursor" in msg or "stream" in msg
                if not (_not_supported and _cursor_or_stream):
                    raise
                self.report.warning(
                    title="Server-side cursor not supported — falling back to client-side execution",
                    message=(
                        "Streaming cursor mode failed with a driver-level error. "
                        "Falling back to client-side buffering. "
                        "Consider setting use_server_side_cursors: false to suppress this warning."
                    ),
                    exc=e,
                )

        # Client-side buffered execution (fallback or use_server_side_cursors=False).
        return self._retry_execute(
            connection,
            text(query),
            params=params,
        )

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Override to prevent parent class from generating aggregator work units during schema extraction.

        We handle aggregator generation manually after populating it with audit log data.
        """
        # Do nothing - we'll call the parent implementation manually after populating the aggregator
        return iter([])

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting Teradata metadata extraction")

        # Step 1: Schema extraction first (parent class will skip aggregator generation due to our override)
        with self.report.new_stage("Schema metadata extraction"):
            yield from super().get_workunits_internal()
            logger.info("Completed schema metadata extraction")

        # Step 2: Lineage extraction after schema extraction
        # This allows lineage processing to have access to all discovered schema information
        with self.report.new_stage("Audit log extraction and lineage processing"):
            self._populate_aggregator_from_audit_logs()
            # Call parent implementation directly to generate aggregator work units
            yield from super()._generate_aggregator_workunits()
            logger.info("Completed lineage processing")

    def _populate_aggregator_from_audit_logs(self) -> None:
        """SqlParsingAggregator-based lineage extraction with enhanced capabilities."""
        with self.report.new_stage("Lineage extraction from Teradata audit logs"):
            # Record the lineage query time range in the report
            self.report.lineage_start_time = self.config.start_time
            self.report.lineage_end_time = self.config.end_time

            logger.info(
                f"Starting lineage extraction from Teradata audit logs (time range: {self.config.start_time} to {self.config.end_time})"
            )

            if (
                self.config.include_table_lineage
                or self.config.include_usage_statistics
            ):
                # Step 1: Stream query entries from database with memory-efficient processing
                with self.report.new_stage("Fetching lineage entries from Audit Logs"):
                    queries_processed = 0

                    # Use streaming query reconstruction for memory efficiency
                    for observed_query in self._reconstruct_queries_streaming(
                        self._fetch_lineage_entries_chunked()
                    ):
                        self.aggregator.add(observed_query)

                        queries_processed += 1
                        if queries_processed % 10000 == 0:
                            logger.info(
                                f"Processed {queries_processed} queries to aggregator"
                            )

                    if queries_processed == 0:
                        logger.info("No lineage entries found")
                        return

                    logger.info(
                        f"Completed adding {queries_processed} queries to SqlParsingAggregator"
                    )

            logger.info("Completed lineage extraction from Teradata audit logs")

    def close(self) -> None:
        """Clean up resources when source is closed."""
        logger.info("Closing SqlParsingAggregator")
        self.aggregator.close()
        self.schema_resolver.close()

        # Clean up pooled engine
        with self._pooled_engine_lock:
            if self._pooled_engine is not None:
                logger.info("Disposing pooled engine")
                self._pooled_engine.dispose()
                self._pooled_engine = None

        try:
            # Clear class-level caches so memory is released between recipe runs in the
            # same process. Without this, sequential recipes accumulate all TeradataTable
            # objects (including view request_text) and creator metadata indefinitely.
            with self._tables_cache_lock:
                self._tables_cache.clear()
                self._table_creator_cache.clear()

            # Clear module-level LRU caches for the same reason — schema column/PK/FK
            # data is per-connection and must not carry over to the next recipe run.
            get_schema_columns.cache_clear()
            get_schema_pk_constraints.cache_clear()
            get_schema_foreign_keys.cache_clear()
        except Exception as e:
            logger.warning(f"Failed to clear caches during close: {e}")

        # Report failed views summary
        super().close()

    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime],
        schema: str,
    ) -> Optional[List[str]]:
        """Return the tables in `schema` eligible to profile (fail-open).

        Teradata can profile-scan a multi-GB table for a very long time, so we use
        DBC space accounting to skip tables above `profile_table_size_limit` (GB)
        before any profiling query is issued. Row-count filtering is not supported
        because Teradata has no cheap, reliable DBC row count without COLLECT STATS.

        We start from the full table list and remove only the tables DBC reports as
        oversized. Any table absent from DBC.TableSizeV (new/zero-perm tables or
        permission asymmetry) is therefore treated as eligible
        """
        size_limit_gb = self.config.profiling.profile_table_size_limit
        if size_limit_gb is None:
            # Nothing we can filter on -> let the base treat all tables as eligible.
            raise NotImplementedError("Teradata only supports size-based candidates")

        size_limit_bytes = size_limit_gb * 1024**3

        try:
            with inspector.engine.connect() as conn:
                rows = self._retry_execute(
                    conn,
                    text(self.PROFILE_OVERSIZED_TABLES_QUERY),
                    {"schema": schema, "size_limit_bytes": size_limit_bytes},
                ).fetchall()
        except Exception as e:
            # Sizing relies on SELECT access to DBC.TableSizeV, which locked-down
            # Teradata instances routinely withhold. Fall back to "no filtering"
            # (return None) so profiling proceeds for all tables rather than
            # failing the entire run.
            self.report.warning(
                title="Could not size tables for profiling",
                message=(
                    "Failed to query DBC.TableSizeV to apply profile_table_size_limit. "
                    "Profiling will proceed without size-based filtering for this schema. "
                    "Ensure the ingestion user has SELECT on DBC.TableSizeV to skip large tables."
                ),
                context=f"schema={schema}",
                exc=e,
            )
            return None

        # Teradata object names are case-insensitive, and DBC casing may differ
        # from what the dialect returns, so match case-insensitively.
        oversized = {row.name.strip().lower() for row in rows}
        candidates = [
            self.get_identifier(schema=schema, entity=table, inspector=inspector)
            for table in inspector.get_table_names(schema)
            if table.strip().lower() not in oversized
        ]
        if oversized:
            self.report.profiling_skipped_size_limit[schema] = len(oversized)
        logger.info(
            "Profiling %d tables in %s; skipped %d above the %d GB size limit.",
            len(candidates),
            schema,
            len(oversized),
            size_limit_gb,
        )
        return candidates
