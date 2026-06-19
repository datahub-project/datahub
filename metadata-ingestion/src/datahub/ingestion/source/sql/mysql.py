# This import verifies that the dependencies are available.
import logging
from datetime import timezone
from typing import TYPE_CHECKING, Any, Iterable, List, Optional

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect, util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.reflection import Inspector

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

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
# COUNT_STAR is the execution count since the table was last reset, LAST_SEEN is
# the most recent execution. This is the only query history enabled by default on
# MariaDB 10.11 without the general query log.
_PERFORMANCE_SCHEMA_DIGEST_QUERY = """
SELECT
    SCHEMA_NAME,
    DIGEST_TEXT,
    COUNT_STAR,
    LAST_SEEN
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
  AND SCHEMA_NAME IS NOT NULL
  AND LAST_SEEN >= %(start_time)s
  AND LAST_SEEN <= %(end_time)s
ORDER BY LAST_SEEN
"""

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
        description="Generate usage statistics and query-based lineage from "
        "`performance_schema.events_statements_summary_by_digest`. Requires the "
        "`statements_digest` consumer and `SELECT` on `performance_schema`.",
    )

    usage: BaseUsageConfig = Field(
        default_factory=BaseUsageConfig,
        description="Usage statistics config. Only used when `include_usage_statistics` is enabled.",
    )


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Optionally enabled via `include_usage_statistics` (reads `performance_schema`)",
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

        if config.include_usage_statistics:
            self.aggregator = self._build_usage_aggregator()
            self.report.sql_aggregator = self.aggregator.report

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

    def _build_usage_aggregator(self) -> SqlParsingAggregator:
        # The base class builds a lineage-only aggregator; swap in one that also
        # emits usage and query entities for the digests we feed it later.
        return SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_lineage=True,
            generate_queries=True,
            generate_query_usage_statistics=True,
            generate_usage_statistics=True,
            # Digests carry no actor, so write-operation stats would be low value.
            generate_operations=False,
            usage_config=self.config.usage,
            eager_graph_load=False,
        )

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Runs after the base has registered table schemas, so unqualified table
        # references in the digests can resolve.
        if self.config.include_usage_statistics:
            self._populate_aggregator_from_performance_schema()
        yield from super()._generate_aggregator_workunits()

    def _populate_aggregator_from_performance_schema(self) -> None:
        try:
            for observed_query in self._fetch_performance_schema_queries():
                self.aggregator.add(observed_query)
        except Exception as e:
            # Metadata/profiling is already emitted; a performance_schema failure
            # (consumer disabled, missing grant) must not abort the run.
            self.report.warning(
                title="Failed to read usage from performance_schema",
                message="Ensure the statements_digest consumer is enabled and the user "
                "has SELECT on performance_schema. Usage statistics were skipped.",
                exc=e,
            )

    def _fetch_performance_schema_queries(self) -> Iterable[ObservedQuery]:
        engine = create_engine(self.config.get_sql_alchemy_url(), **self.config.options)
        self._setup_rds_iam_event_listener(engine)
        with engine.connect() as conn:
            rows = conn.execute(
                _PERFORMANCE_SCHEMA_DIGEST_QUERY,
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

                # LAST_SEEN is in the server timezone; treat it as UTC so it lands
                # in the usage window (bucket may drift by the server's UTC offset).
                timestamp = row.LAST_SEEN
                if timestamp is not None and timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                yield ObservedQuery(
                    query=row.DIGEST_TEXT,
                    timestamp=timestamp,
                    # Two-tier: the schema is the database, so default_schema (not
                    # default_db) yields `schema.table` URNs. Digests are aggregated
                    # across users, so there is no actor to attribute.
                    default_schema=schema_name,
                    usage_multiplier=count,
                )
