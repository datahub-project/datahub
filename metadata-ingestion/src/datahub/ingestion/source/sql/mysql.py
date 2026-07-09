# This import verifies that the dependencies are available.
import logging
import re
from collections import OrderedDict
from contextlib import contextmanager
from datetime import timezone
from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Optional

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect, text, util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
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
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.base import (
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


class MySQLConfig(MySQLConnectionConfig, TwoTierSQLAlchemyConfig):
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
@support_status(SupportStatus.CERTIFIED)
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

        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = None
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

        with engine.connect() as conn:
            inspector = inspect(conn)
            if self.config.database and self.config.database != "":
                databases = [self.config.database]
            else:
                databases = inspector.get_schema_names()
            for db in databases:
                if self.config.database_pattern.allowed(db):
                    url = self.config.get_sql_alchemy_url(current_db=db)
                    db_engine = create_engine(url, **self.config.options)
                    self._setup_rds_iam_event_listener(db_engine, database_name=db)

                    with db_engine.connect() as conn:
                        inspector = inspect(conn)
                        yield inspector

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if not self.config.is_profiling_enabled():
            return
        with inspector.engine.connect() as conn:
            for row in conn.execute(
                "SELECT table_schema, table_name, data_length from information_schema.tables"
            ):
                self.profile_metadata_info.dataset_name_to_storage_bytes[
                    f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
                ] = row.DATA_LENGTH

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
        )

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
        # ephemeral engine.
        engine = create_engine(
            self.config.get_sql_alchemy_url(),
            **{**self.config.options, "poolclass": NullPool},
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
                if schema_name.lower() in _SYSTEM_SCHEMAS:
                    continue
                if not self.config.database_pattern.allowed(schema_name):
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
                if schema_name.lower() in _SYSTEM_SCHEMAS:
                    continue
                if not self.config.database_pattern.allowed(schema_name):
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
