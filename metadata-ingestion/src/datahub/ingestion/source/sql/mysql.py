# This import verifies that the dependencies are available.
import logging
import re
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
)

import pymysql  # noqa: F401
from pydantic import model_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect, text, util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

from datahub.configuration.common import (
    AllowDenyPattern,
    HiddenFromDocs,
    SupportedSources,
)
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.models import (
    BaseProcedure,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.schema_classes import BytesTypeClass, QueryLanguageClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

_SYSTEM_SCHEMAS = frozenset(
    {"information_schema", "performance_schema", "mysql", "sys"}
)

# QueuePool-only sizing options that NullPool rejects, so they must be dropped
# from the ephemeral usage engine (which forces NullPool). DataHub itself only
# auto-injects `max_overflow` (SQLAlchemySource._add_default_options, when
# profiling is enabled); the other three are stripped defensively in case a user
# sets them via `config.options`.
_QUEUE_POOL_ONLY_OPTIONS = frozenset(
    {"pool_size", "max_overflow", "pool_timeout", "pool_use_lifo"}
)

# One row per normalized statement: DIGEST_TEXT has literals stripped to `?`,
# COUNT_STAR counts executions since the last reset, LAST_SEEN is the most recent.
# Low-overhead query history when performance_schema is on (vs. the general log).
_PERFORMANCE_SCHEMA_DIGEST_QUERY = """
SELECT
    SCHEMA_NAME,
    DIGEST_TEXT,
    COUNT_STAR,
    LAST_SEEN
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
  AND SCHEMA_NAME IS NOT NULL
  AND LAST_SEEN >= :start_time
  AND LAST_SEEN <= :end_time
ORDER BY LAST_SEEN
"""

# Each row is a single executed statement with literal text, the executing user,
# and a real timestamp. Requires general_log=ON and log_output=TABLE. `Connect`
# and `Init DB` rows carry the session's current database (the missing piece in
# general_log), so they are read alongside `Query` rows to resolve unqualified
# table names. `Connect` matters because clients that select a database at
# connection time never emit an explicit `Init DB`.
_GENERAL_LOG_QUERY = """
SELECT
    event_time,
    user_host,
    thread_id,
    command_type,
    CONVERT(argument USING utf8mb4) AS argument
FROM mysql.general_log
WHERE command_type IN ('Query', 'Init DB', 'Connect')
  AND event_time >= :start_time
  AND event_time <= :end_time
ORDER BY event_time, thread_id
"""

# user_host is formatted as `priv_user[login_user] @ host [ip]`; capture login_user.
_USER_HOST_RE = re.compile(r"^[^\[]*\[([^\]]+)\]")

# Leading `USE <db>` switches the session's current database.
_USE_STATEMENT_RE = re.compile(r"^\s*USE\s+`?([^\s`;]+)`?", re.IGNORECASE)

# `Connect` events record the session's initial default schema as
# "<user>@<host> on <db> using <protocol>". When the client connects without
# selecting a database the `<db>` slot is empty ("... on  using ...") and this
# does not match, leaving the session's schema unknown until a USE/Init DB.
_CONNECT_DB_RE = re.compile(r"\bon\s+(\S+)\s+using\b", re.IGNORECASE)

# Statement kinds worth parsing for lineage/usage; everything else (SET, SHOW,
# COMMIT, administrative commands) carries no dataset usage and is skipped.
_DML_LEADING_KEYWORDS = frozenset(
    {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "WITH", "CALL", "MERGE"}
)

# Cap the per-session current-db map so a large general_log on a busy server
# can't grow it without bound (LRU eviction in _fetch_general_log_queries).
_MAX_TRACKED_SESSIONS = 10_000

# How many of the slowest tables to name in the post-run advice, and the minimum
# elapsed seconds for the advice to fire at all. Below this threshold profiling
# is healthy and the advice would be noise.
_EXPENSIVE_TABLES_TOP_N = 5
_EXPENSIVE_TABLES_SLOWEST_MIN_S = 30


def _parse_general_log_user(user_host: Optional[str]) -> Optional[str]:
    if not user_host:
        return None
    match = _USER_HOST_RE.match(user_host)
    return match.group(1) if match else None


SET.__repr__ = util.generic_repr  # type:ignore

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
POINT = make_sqlalchemy_type("POINT")
LINESTRING = make_sqlalchemy_type("LINESTRING")
POLYGON = make_sqlalchemy_type("POLYGON")
DECIMAL128 = make_sqlalchemy_type("DECIMAL128")

register_custom_type(GEOMETRY)
register_custom_type(POINT)
register_custom_type(LINESTRING)
register_custom_type(POLYGON)
register_custom_type(DECIMAL128)
register_custom_type(BIT, BytesTypeClass)

base.ischema_names["geometry"] = GEOMETRY
base.ischema_names["point"] = POINT
base.ischema_names["linestring"] = LINESTRING
base.ischema_names["polygon"] = POLYGON
base.ischema_names["decimal128"] = DECIMAL128


class MySQLAuthMode(StrEnum):
    """Authentication mode for MySQL connection."""

    PASSWORD = "PASSWORD"
    AWS_IAM = "AWS_IAM"


class MySQLUsageSource(StrEnum):
    PERFORMANCE_SCHEMA = "performance_schema"
    GENERAL_LOG = "general_log"


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: HiddenFromDocs[str] = "mysql+pymysql"

    # Authentication configuration
    auth_mode: MySQLAuthMode = Field(
        default=MySQLAuthMode.PASSWORD,
        description="Authentication mode to use for the MySQL connection. "
        "Options are 'PASSWORD' (default) for standard username/password authentication, "
        "or 'AWS_IAM' for AWS RDS IAM authentication.",
    )
    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS configuration for RDS IAM authentication (only used when auth_mode is AWS_IAM). "
        "Provides full control over AWS credentials, region, profiles, role assumption, retry logic, and proxy settings. "
        "If not explicitly configured, boto3 will automatically use the default credential chain and region from "
        "environment variables (AWS_DEFAULT_REGION, AWS_REGION), AWS config files (~/.aws/config), or IAM role metadata.",
    )


class MySQLProfilingConfig(GEProfilingConfig):
    # Per-source override, following the Athena/Dremio precedent
    # (AthenaProfilingConfig.partition_profiling_enabled,
    # ProfileConfig.include_field_median_value).
    # Redeclared with Annotated[...] (not plain Optional[int]) so schema_extra.supported_sources
    # is preserved on the subclass field — a plain redeclaration drops it.
    profile_table_row_limit: Annotated[
        Optional[int], SupportedSources(["mysql", "mariadb", "doris", "tidb"])
    ] = Field(
        default=None,
        description="MySQL: profile tables only if their estimated row count is less than this. "
        "Defaults to `null` (no limit) — set explicitly to guardrail large tables. The "
        "estimate comes from `information_schema.tables.table_rows`, which is a "
        "storage-engine stat that can be stale.",
    )
    profile_table_size_limit: Annotated[
        Optional[int], SupportedSources(["mysql", "mariadb", "doris", "tidb"])
    ] = Field(
        default=None,
        description="MySQL: profile tables only if their size is less than specified GBs. "
        "Defaults to `null` (no limit) — set explicitly to guardrail large tables. "
        "The size is `data_length` from `information_schema.tables`.",
    )

    @model_validator(mode="after")
    def _validate_positive_limits(self) -> "MySQLProfilingConfig":
        # A value of 0 (or any negative) would make every table exceed the limit,
        # silently excluding the whole instance from profiling — surfaced only as
        # the generic "No tables passed the row/size guardrail" info. Reject it so
        # the misconfiguration fails fast; use null to disable either filter.
        for name in ("profile_table_row_limit", "profile_table_size_limit"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(
                    f"{name} must be greater than 0 (or null to disable "
                    f"filtering); got {value}."
                )
        return self


class MySQLConfig(MySQLConnectionConfig, TwoTierSQLAlchemyConfig):
    profiling: MySQLProfilingConfig = Field(
        default_factory=MySQLProfilingConfig,
        description="Configuration for profiling tables.",
    )

    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistics and query-based lineage from query history. "
        "The source of that history is controlled by `usage_source`.",
    )

    usage_source: MySQLUsageSource = Field(
        default=MySQLUsageSource.PERFORMANCE_SCHEMA,
        description="Where to read query history from. `performance_schema` (default) reads "
        "normalized digests from `events_statements_summary_by_digest` (no setup, no per-user "
        "attribution). Its `COUNT_STAR` is cumulative since the last counter reset (server "
        "restart or table truncation), so the first ingestion after enabling usage can report "
        "a large one-day spike attributing all history-to-date to a single timestamp. "
        "`general_log` reads literal statements with user and timestamp from `mysql.general_log` "
        "(requires `general_log=ON` and `log_output=TABLE`).",
    )

    usage: BaseUsageConfig = Field(
        default_factory=BaseUsageConfig,
        description="Usage statistics config. Only used when `include_usage_statistics` is enabled.",
    )

    email_domain: Optional[str] = Field(
        default=None,
        description="Email domain of your organisation, appended to `general_log` usernames "
        "(e.g. LDAP logins) so users display correctly. Ignored if the username already looks like "
        "an email. Only used with `usage_source: general_log`.",
    )


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Optionally enabled via `include_usage_statistics`. Reads query history from "
    "`performance_schema` digests (default) or `mysql.general_log` "
    "(`usage_source: general_log`), which also yields query-based table lineage.",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default for views via `include_view_lineage`. Table-level lineage is "
    "also derived from query history when `include_usage_statistics` is enabled.",
    subtype_modifier=[
        SourceCapabilityModifier.VIEW,
        SourceCapabilityModifier.TABLE,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default for views via `include_view_column_lineage`. Column-level "
    "lineage is also derived from query history when `include_usage_statistics` is "
    "enabled.",
    subtype_modifier=[
        SourceCapabilityModifier.VIEW,
        SourceCapabilityModifier.TABLE,
    ],
)
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    Metadata for databases, schemas, and tables
    Column types and schema associated with each table
    Table, row, and column statistics via optional SQL profiling
    """

    config: MySQLConfig

    def __init__(self, config: MySQLConfig, ctx: Any):
        super().__init__(config, ctx, self.get_platform())

        self._discovered_lower_cache: Optional[Set[str]] = None
        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = None
        # Guardrail cache populated by add_profile_metadata's information_schema sweep
        # and consumed by generate_profile_candidates / is_dataset_eligible_for_profiling.
        # Kept on the source (not on the shared ProfileMetadata) so sql_common.py stays
        # untouched — these fields are MySQL-specific. Keyed by "{schema}.{table}".
        # data_length lives on profile_metadata_info.dataset_name_to_storage_bytes
        # (base-owned, also feeds sizeInBytes); only table_rows is MySQL-local here.
        self._table_rows_cache: Dict[str, Optional[int]] = {}
        # True once a sweep populated dataset_name_to_storage_bytes, so the guardrail
        # can run at all. Set on both the primary and the fallback path. Load-bearing:
        # add_profile_metadata is wrapped in try/except in get_profiling_internal that
        # only warns, so a sweep that raises leaves this False — generate_profile_candidates
        # then fails open (None candidates). Distinguishes "sweep failed" (None) from
        # "instance has no tables" (empty candidate list).
        self._profile_sweep_ran: bool = False
        # True only after the four-column sweep completes (table_rows cached). The
        # three-column fallback fills storage bytes but not the row cache, so this
        # stays False there and profile_table_row_limit is unenforceable for the run.
        self._table_rows_available: bool = False
        # dataset_name -> "row" | "size": set while filtering, consumed to attribute skips.
        self._guardrail_skip: Dict[str, str] = {}
        if config.auth_mode == MySQLAuthMode.AWS_IAM:
            hostname, port = parse_host_port(config.host_port, default_port=3306)
            if port is None:
                raise ValueError("Port must be specified for RDS IAM authentication")

            if not config.username:
                raise ValueError("username is required for RDS IAM authentication")

            self._rds_iam_token_manager = RDSIAMTokenManager(
                endpoint=hostname,
                username=config.username,
                port=port,
                aws_config=config.aws_config,
            )

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.model_validate(config_dict)
        return cls(config, ctx)

    def close(self) -> None:
        # super().close() must run unconditionally: the parent chain reaches
        # StatefulIngestionSourceBase.close(), which commits the ingestion checkpoint
        # (prepare_for_commit) and drives stale-entity soft deletion. An early return
        # here would skip that on every ordinary run — no profiling, or no slow table.
        # The advice is best-effort: a failure there must not propagate and fail the
        # pipeline at its last step, so log and swallow.
        try:
            self._maybe_emit_expensive_tables_advice()
        except Exception:
            logger.warning("expensive-tables advice failed", exc_info=True)
        finally:
            super().close()

    def _maybe_emit_expensive_tables_advice(self) -> None:
        # Uses report.info (not report.warning) so it surfaces without counting toward
        # --strict-warnings failure. message is a constant literal and the formatted table
        # list goes in context= so StructuredLogs dedupes on f"{title}-{message}".
        # Doris and TiDB inherit this method via MySQLSource; the advice names MySQL-shaped
        # config, so they opt out here rather than receive it.
        if self.platform not in ("mysql", "mariadb"):
            return
        timings = self.report.profiling_time_taken_per_table_secs
        if not timings:
            return
        top = sorted(timings.items(), key=lambda pair: pair[1], reverse=True)[
            :_EXPENSIVE_TABLES_TOP_N
        ]
        # Name only tables that actually crossed the threshold — the top-N sort
        # otherwise drags fast tables into a list whose headline is "slowest".
        top = [(name, t) for name, t in top if t >= _EXPENSIVE_TABLES_SLOWEST_MIN_S]
        if not top:
            return
        formatted = ", ".join(f"{name} ({t:.1f}s)" for name, t in top)
        self.report.info(
            title="Profiling: expensive tables",
            message=(
                "These MySQL tables took the longest to profile. If profiling is too slow or "
                "risks OOM, set `profiling.profile_table_row_limit` and/or "
                "`profiling.profile_table_size_limit` to skip large tables, or lower "
                "`profiling.max_workers` — concurrent full scans on a single-primary row "
                "store multiply peak memory rather than increasing throughput, so a low value "
                "(e.g. 5) can relieve memory pressure."
            ),
            context=formatted,
        )

    def _setup_rds_iam_event_listener(
        self, engine: "Engine", database_name: Optional[str] = None
    ) -> None:
        """Setup SQLAlchemy event listener to inject RDS IAM tokens."""
        if not (
            self.config.auth_mode == MySQLAuthMode.AWS_IAM
            and self._rds_iam_token_manager
        ):
            return

        def do_connect_listener(_dialect, _conn_rec, _cargs, cparams):
            if not self._rds_iam_token_manager:
                raise RuntimeError("RDS IAM Token Manager is not initialized")
            cparams["password"] = self._rds_iam_token_manager.get_token()
            # PyMySQL requires SSL to be enabled for RDS IAM authentication.
            # Preserve any existing SSL configuration, otherwise enable with default settings.
            # The {"ssl": True} dict is a workaround to make PyMySQL recognize that SSL
            # should be enabled, since the library requires a truthy value in the ssl parameter.
            # See https://pymysql.readthedocs.io/en/latest/modules/connections.html#pymysql.connections.Connection
            cparams["ssl"] = cparams.get("ssl") or {"ssl": True}

        event.listen(engine, "do_connect", do_connect_listener)  # type: ignore[misc]

    def get_inspectors(self):
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        self._setup_rds_iam_event_listener(engine)

        try:
            with engine.connect() as conn:
                inspector = inspect(conn)
                if self.config.database and self.config.database != "":
                    databases = [self.config.database]
                else:
                    databases = inspector.get_schema_names()
        finally:
            # Only used to list databases; dispose so it does not hold a pooled
            # connection open for the whole reflection/profiling run.
            engine.dispose()

        for db in databases:
            if not self.config.database_pattern.allowed(db):
                continue
            db_url = self.config.get_sql_alchemy_url(current_db=db)
            # config.options carries the max_overflow that _add_default_options injects when
            # profiling is on, so this per-DB QueuePool can grow to profiling.max_workers
            # connections (QueuePool accepts it). PR #18319 fixes the mirror-image case where the
            # same injected option breaks the NullPool usage engine — same root cause.
            db_engine = create_engine(db_url, **self.config.options)
            self._setup_rds_iam_event_listener(db_engine, database_name=db)
            try:
                with db_engine.connect() as conn:
                    inspector = inspect(conn)
                    # Invariant: the caller must complete all reflection + profiling for this db
                    # before requesting the next inspector — the finally below disposes the engine
                    # on resume, so deferring work past the yield would run it on a torn-down engine.
                    yield inspector
            finally:
                # Dispose once the inspector is consumed; otherwise each engine's
                # pool keeps one connection open per database for the whole run,
                # exhausting servers with a low max_user_connections limit.
                db_engine.dispose()

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if not self.config.is_profiling_enabled():
            return
        # Unfiltered information_schema sweep shared with the guardrail (see
        # generate_profile_candidates). No WHERE clause on table_type: sizeInBytes is
        # cached for views too, and the dialect's get_table_names already filters to
        # base tables, so a table_type filter here would only drop rows the guardrail
        # would never read. MySQL upper-cases information_schema labels; MariaDB/Doris/
        # TiDB keep the selected case — unpack positionally so access is case-independent.
        # table_rows is an InnoDB *estimate* (stale until ANALYZE TABLE); data_length is
        # in bytes. Acceptable for a guardrail, not an accurate count.
        # MySQL 8 caches data-dictionary stats in `information_schema_stats_expiry` (24h
        # default), so values can be up to that stale between ANALYZE TABLEs.
        # Primary: four columns including table_rows for the guardrail. A platform
        # whose information_schema omits table_rows, a proxy that rewrites or rejects
        # the query, or a dropped connection makes it raise; fall back to master's
        # three-column form for sizeInBytes only. The fallback still sets
        # _profile_sweep_ran (storage bytes are populated, so the size limit can
        # run) but leaves _table_rows_available False, so profile_table_row_limit
        # is unenforceable for the run. (Insufficient privileges do not land here —
        # MySQL filters information_schema rows by grant instead of erroring.)
        try:
            with inspector.engine.connect() as conn:
                for (
                    table_schema,
                    table_name,
                    data_length,
                    table_rows,
                ) in conn.execute(
                    text(
                        "SELECT table_schema, table_name, data_length, table_rows "
                        "FROM information_schema.tables"
                    )
                ):
                    key = f"{table_schema}.{table_name}"
                    self.profile_metadata_info.dataset_name_to_storage_bytes[key] = (
                        data_length
                    )
                    self._table_rows_cache[key] = table_rows
            self._profile_sweep_ran = True
            self._table_rows_available = True
        except SQLAlchemyError as e:
            with inspector.engine.connect() as conn:
                for table_schema, table_name, data_length in conn.execute(
                    text(
                        "SELECT table_schema, table_name, data_length "
                        "FROM information_schema.tables"
                    )
                ):
                    key = f"{table_schema}.{table_name}"
                    self.profile_metadata_info.dataset_name_to_storage_bytes[key] = (
                        data_length
                    )
            self._profile_sweep_ran = True
            # Reported only once the fallback has succeeded, so the "sizeInBytes is
            # unaffected" claim is true when it is made; if the fallback raises too,
            # the exception propagates to get_profiling_internal, which warns for it.
            # Warn only when the row limit is set — that is the only limit lost on
            # the fallback path. A user who set only the size limit loses nothing
            # (data_length was fetched), and a warning would fail --strict-warnings
            # runs over a degradation that did not happen.
            log = (
                self.report.warning
                if self.config.profiling.profile_table_row_limit is not None
                else self.report.info
            )
            log(
                title="Profiling row/size guardrail disabled",
                message=(
                    "Could not read table_rows from information_schema.tables, so "
                    "the row limit is disabled for this run. The size limit still "
                    "applies, and sizeInBytes is unaffected — it fell back to the "
                    "data_length-only query."
                ),
                context=self.platform,
                exc=e,
            )

    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime],
        schema: str,
    ) -> Optional[List[str]]:
        # profile_if_updated_since_days is not enforced here — candidate selection is row/size
        # based only. Info once per source when it is set, so a user does not assume freshness
        # filtering is applied. Emitted here (not in __init__) so it lands in the live report
        # even when a subclass reassigns self.report after super().__init__ (e.g. Doris).
        if self.config.profiling.profile_if_updated_since_days is not None:
            self.report.info(
                title="Profiling does not support profile_if_updated_since_days",
                message="This setting will be ignored. Tables are selected for profiling by "
                "row/size limits only (profile_table_row_limit / profile_table_size_limit).",
            )

        # When both row/size limits are None there is nothing to enforce, so return no candidate
        # filter (loop_profiler_requests then falls back to get_table_names).
        row_limit = self.config.profiling.profile_table_row_limit
        size_limit_gb = self.config.profiling.profile_table_size_limit
        if row_limit is None and size_limit_gb is None:
            return None

        size_limit_bytes = (
            size_limit_gb * 1024**3 if size_limit_gb is not None else None
        )

        # The guardrail reads the cache populated by add_profile_metadata (an unfiltered
        # information_schema sweep, shared with sizeInBytes). add_profile_metadata is called
        # inside a try/except Exception in get_profiling_internal that only warns, so a
        # sweep that raises leaves _profile_sweep_ran False — fail open to no guardrail
        # rather than dropping every profile in the run (an empty candidate list is
        # additive and would exclude all tables).
        if not self._profile_sweep_ran:
            return None
        # Fallback path: storage bytes are populated (size limit enforceable) but
        # table_rows are not (row limit unenforceable). If only the row limit is
        # configured there is nothing to enforce — return None rather than build a
        # full candidate list that retains every table and then pays an O(n) membership
        # test per table in is_dataset_eligible_for_profiling for a filter that excludes
        # nothing. With the size limit set, fall through and let the loop filter on
        # data_length (table_rows is None for every table, so the row check no-ops).
        if not self._table_rows_available and size_limit_gb is None:
            return None

        # Apply row/size limits in Python. A table whose table_rows or data_length is NULL
        # is retained — NULL stats must not silently drop a table from profiling. The size
        # guardrail uses data_length, the same value sizeInBytes reads — the two measures
        # now agree. Row limit takes precedence over size limit when both exclude a
        # table — the cheaper-to-fix reason is reported first.
        table_names = inspector.get_table_names(schema)
        candidates: List[str] = []
        for table_name in table_names:
            dataset_name = self.get_identifier(
                schema=schema, entity=table_name, inspector=inspector
            )
            table_rows = self._table_rows_cache.get(dataset_name)
            if (
                row_limit is not None
                and table_rows is not None
                and table_rows > row_limit
            ):
                self._guardrail_skip[dataset_name] = "row"
                continue
            data_length = self.profile_metadata_info.dataset_name_to_storage_bytes.get(
                dataset_name
            )
            if (
                size_limit_bytes is not None
                and data_length is not None
                and data_length > size_limit_bytes
            ):
                self._guardrail_skip[dataset_name] = "size"
                continue
            candidates.append(dataset_name)

        # An empty candidate list drops every profile in the schema (the list is
        # additive). Distinguish "schema has no base tables" (no info needed) from
        # "every table exceeded the limits" (info so the operator knows profiles were
        # dropped). Reuse the table_names already fetched for the loop — a second
        # call would be redundant, and a raise here would surface in
        # loop_profiler_requests's own get_table_names call regardless, so a
        # handler that emitted a false "guardrail" notice and then
        # died anyway would be worse than none.
        if not candidates and table_names:
            self.report.info(
                title="No tables passed the row/size guardrail",
                message=(
                    "Profiling will be skipped for every table in this schema. Either "
                    "every table exceeds the configured row/size limits, or "
                    "information_schema is not returning them (restricted grants, a "
                    "rewriting proxy, or a catalog mismatch)."
                ),
                context=f"Schema: {schema}",
            )

        return candidates

    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        schema: str,
        inspector: Inspector,
        profile_candidates: Optional[List[str]],
    ) -> bool:
        # Run table/profile pattern checks, but pass profile_candidates=None so the base
        # does NOT count guardrailed tables into profiling_skipped_other (which would
        # double-count). We then attribute candidate misses to the bucket recorded at
        # filter time (_guardrail_skip), falling back to profiling_skipped_other only for
        # tables excluded for reasons the guardrail didn't record.
        if not super().is_dataset_eligible_for_profiling(
            dataset_name, schema, inspector, profile_candidates=None
        ):
            return False
        if profile_candidates is not None and dataset_name not in profile_candidates:
            reason = self._guardrail_skip.get(dataset_name)
            if reason == "row":
                self.report.profiling_skipped_row_limit[schema] += 1
            elif reason == "size":
                self.report.profiling_skipped_size_limit[schema] += 1
            else:
                self.report.profiling_skipped_other[schema] += 1
            return False
        return True

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Get stored procedures for a specific schema.
        """
        base_procedures = []
        with inspector.engine.connect() as conn:
            procedures = conn.execute(
                """
                SELECT ROUTINE_NAME AS name, 
                    ROUTINE_DEFINITION AS definition, 
                    EXTERNAL_LANGUAGE AS language
                FROM information_schema.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND ROUTINE_SCHEMA = %s
                """,
                (schema,),
            )

            procedure_rows = list(procedures)
            for row in procedure_rows:
                base_procedures.append(
                    BaseProcedure(
                        name=row.name,
                        # information_schema.ROUTINES.EXTERNAL_LANGUAGE is NULL for
                        # natively-written SQL procedures (the common case) and only
                        # populated for MLE procedures (MySQL 8.0+ JavaScript / Java).
                        # generate_procedure_lineage gates on QueryLanguageClass.SQL,
                        # so without this default the lineage extractor would silently
                        # skip every native procedure on MySQL/MariaDB.
                        language=row.language or QueryLanguageClass.SQL,
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=row.definition,
                        created=None,
                        last_altered=None,
                        extra_properties=None,
                        comment=None,
                    )
                )
            return base_procedures

    def _create_aggregator(self) -> SqlParsingAggregator:
        # Base __init__ calls this before our __init__ body, so only self.config /
        # self.platform / self.ctx are safe to read. Overriding (vs. swapping
        # self.aggregator later) keeps the base's single-aggregator contract.
        if not self.config.include_usage_statistics:
            return super()._create_aggregator()

        # Base builds a lineage-only aggregator; usage also needs query + usage stats.
        return SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            # Query history yields table lineage; always on with usage, independent
            # of include_view_lineage (which only governs view-definition lineage).
            generate_lineage=True,
            generate_queries=True,
            generate_query_usage_statistics=True,
            generate_usage_statistics=True,
            # Operations stay off for both usage sources. performance_schema
            # digests carry no actor (actorless operations are low value); for
            # general_log an actor is available, but we keep the behavior uniform
            # rather than emit an operation aspect per logged statement.
            generate_operations=False,
            usage_config=self.config.usage,
            eager_graph_load=False,
            is_allowed_table=self._is_allowed_table,
            is_temp_table=self._is_temp_table,
        )

    def _save_schema_to_resolver(self) -> bool:
        # is_temp_table reads discovered_datasets, which is only filled when
        # schemas are saved; usage needs it regardless of view lineage.
        return (
            super()._save_schema_to_resolver() or self.config.include_usage_statistics
        )

    def _is_allowed_database(self, database: str) -> bool:
        if database.lower() in _SYSTEM_SCHEMAS:
            return False
        return self.config.database_pattern.allowed(database)

    def _is_allowed_table(self, name: str) -> bool:
        # name is the two-tier "database.table" name. Unlike the fetch-time
        # filters (which only see a query's default schema), this also drops
        # tables referenced in databases excluded by database_pattern.
        return self._is_allowed_database(name.split(".", 1)[0])

    def _is_temp_table(self, name: str) -> bool:
        # Tables we never ingested are treated as temp: the aggregator resolves
        # lineage through them but doesn't emit them, avoiding phantom datasets
        # (temp tables, filtered-out databases, and mis-quoted `db.table` refs
        # the parser expands to db.db.table). A table excluded only by
        # table_pattern is likewise "temp" here: lineage flows through it rather
        # than being cut off.
        if name in self.discovered_datasets or name.lower() in self._discovered_lower():
            return False
        self.report.num_usage_references_suppressed_as_temp += 1
        self.report.usage_references_suppressed_as_temp_sample.append(name)
        return True

    def _discovered_lower(self) -> Set[str]:
        # Lowercased view of discovered_datasets for case-insensitive matching:
        # the parser lowercases unresolved MySQL URNs (not in
        # PLATFORMS_WITH_CASE_SENSITIVE_TABLES), so a reference whose case differs
        # from the catalog would otherwise miss a real, ingested table. Built once
        # lazily; discovered_datasets is fully populated before the usage phase.
        if self._discovered_lower_cache is None:
            self._discovered_lower_cache = {d.lower() for d in self.discovered_datasets}
        return self._discovered_lower_cache

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Runs after the base registers table schemas, so unqualified references resolve.
        if self.config.include_usage_statistics:
            self._populate_usage_aggregator()
        yield from super()._generate_aggregator_workunits()

    def _populate_usage_aggregator(self) -> None:
        if self.config.usage_source == MySQLUsageSource.GENERAL_LOG:
            fetch = self._fetch_general_log_queries
            failure_title = "Failed to read usage from general_log"
            failure_hint = (
                "Ensure general_log=ON, log_output=TABLE, and the user has SELECT on "
                "mysql.general_log. Usage statistics were skipped."
            )
        else:
            fetch = self._fetch_performance_schema_queries
            failure_title = "Failed to read usage from performance_schema"
            failure_hint = (
                "Ensure the statements_digest consumer is enabled and the user has SELECT "
                "on performance_schema. Usage statistics were skipped."
            )

        try:
            # Materialize so fetch()'s connection closes before we feed the
            # aggregator; otherwise an aggregator error leaks the open connection.
            # The whole result set is held in memory; use a server-side cursor if a
            # huge general_log ever OOMs.
            queries = list(fetch())
            for observed_query in queries:
                self.aggregator.add(observed_query)
        except SQLAlchemyError as e:
            # Metadata is already emitted; a query-history read failure (disabled
            # consumer, missing grant) must not abort the run. Catch only DB errors
            # so programming bugs still surface.
            self.report.warning(title=failure_title, message=failure_hint, exc=e)

    @contextmanager
    def _usage_connection(self) -> Iterator["Connection"]:
        """Yield a UTC-pinned connection from a single-use, disposed-on-exit engine."""
        # NullPool + dispose() so this one-shot fetch never leaves connections
        # open. poolclass is forced last so a pooled class in options (intended
        # for the long-lived inspection engine) can't silently re-pool this
        # ephemeral engine; QueuePool-only options are dropped (see
        # _QUEUE_POOL_ONLY_OPTIONS) because NullPool rejects them.
        usage_options = {
            key: value
            for key, value in self.config.options.items()
            if key not in _QUEUE_POOL_ONLY_OPTIONS
        }
        engine = create_engine(
            self.config.get_sql_alchemy_url(),
            **{**usage_options, "poolclass": NullPool},
        )
        self._setup_rds_iam_event_listener(engine)
        try:
            with engine.connect() as conn:
                # Timestamps render in the session tz; pin UTC so naive reads are UTC.
                conn.execute(text("SET time_zone = '+00:00'"))
                yield conn
        finally:
            engine.dispose()

    def _fetch_performance_schema_queries(self) -> Iterable[ObservedQuery]:
        with self._usage_connection() as conn:
            rows = conn.execute(
                text(_PERFORMANCE_SCHEMA_DIGEST_QUERY),
                {
                    "start_time": self.config.usage.start_time,
                    "end_time": self.config.usage.end_time,
                },
            )
            for row in rows:
                schema_name = row.SCHEMA_NAME
                if not self._is_allowed_database(schema_name):
                    continue

                count = int(row.COUNT_STAR or 0)
                if count <= 0:
                    continue

                # Session is pinned to UTC, so a naive LAST_SEEN is already UTC.
                timestamp = row.LAST_SEEN
                if timestamp is not None and timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                yield ObservedQuery(
                    query=row.DIGEST_TEXT,
                    timestamp=timestamp,
                    # Two-tier: schema acts as the database, so default_schema (not
                    # default_db) yields schema.table URNs. Digests have no actor.
                    default_schema=schema_name,
                    usage_multiplier=count,
                )

    def _fetch_general_log_queries(self) -> Iterable[ObservedQuery]:
        with self._usage_connection() as conn:
            rows = conn.execute(
                text(_GENERAL_LOG_QUERY),
                {
                    "start_time": self.config.usage.start_time,
                    "end_time": self.config.usage.end_time,
                },
            )
            # general_log has no schema column; track each session's current db
            # from Connect / Init DB / USE to resolve unqualified table names.
            # LRU-capped (see _MAX_TRACKED_SESSIONS), refreshing recency on writes
            # and reads.
            session_db: OrderedDict[str, str] = OrderedDict()

            def _remember_db(session_id: str, db: str) -> None:
                session_db[session_id] = db
                session_db.move_to_end(session_id)
                if len(session_db) > _MAX_TRACKED_SESSIONS:
                    session_db.popitem(last=False)

            for row in rows:
                session_id = str(row.thread_id)
                argument = row.argument or ""

                if row.command_type == "Connect":
                    connect_match = _CONNECT_DB_RE.search(argument)
                    if connect_match:
                        _remember_db(session_id, connect_match.group(1).strip("`"))
                    continue

                if row.command_type == "Init DB":
                    _remember_db(session_id, argument.strip().strip("`"))
                    continue

                use_match = _USE_STATEMENT_RE.match(argument)
                if use_match:
                    _remember_db(session_id, use_match.group(1))
                    continue

                if not self._is_dml_statement(argument):
                    continue

                schema_name = session_db.get(session_id)
                if schema_name is None:
                    # No Init DB/USE seen for this session, so the system-schema and
                    # database_pattern filters below cannot be applied. Skip rather
                    # than emit an unfiltered query (unqualified tables wouldn't
                    # resolve without a schema anyway).
                    logger.debug(
                        "general_log statement on thread %s has no known database; "
                        "skipping: %s",
                        session_id,
                        argument,
                    )
                    continue

                # Refresh recency so long-lived active sessions aren't evicted.
                session_db.move_to_end(session_id)
                if not self._is_allowed_database(schema_name):
                    continue

                timestamp = row.event_time
                if timestamp is not None and timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                yield ObservedQuery(
                    query=argument,
                    timestamp=timestamp,
                    user=self._general_log_user_urn(row.user_host),
                    default_schema=schema_name,
                    session_id=session_id,
                    usage_multiplier=1,
                )

    @staticmethod
    def _is_dml_statement(argument: str) -> bool:
        stripped = argument.lstrip()
        if not stripped:
            return False
        leading_keyword = re.split(r"[\s(]", stripped, maxsplit=1)[0].upper()
        return leading_keyword in _DML_LEADING_KEYWORDS

    def _general_log_user_urn(self, user_host: Optional[str]) -> Optional[CorpUserUrn]:
        user = _parse_general_log_user(user_host)
        if not user:
            return None
        # LDAP/db logins are not emails; append the configured domain so usage maps
        # to the real user. Leave it alone if it already looks like an email.
        if "@" not in user and self.config.email_domain:
            user = f"{user}@{self.config.email_domain}"
        return CorpUserUrn(user)
