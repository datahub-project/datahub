import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, Union

# This import verifies that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy.dialects.postgresql as custom_types

# GeoAlchemy adds support for PostGIS extensions in SQLAlchemy. In order to
# activate it, we must import it so that it can hook into SQLAlchemy. While
# we don't use the Geometry type that we import, we do care about the side
# effects of the import. For more details, see here:
# https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html#reflecting-tables.
from geoalchemy2 import Geometry  # noqa: F401
from pydantic import BaseModel, field_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.engine.reflection import Inspector

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.emitter.mcp_builder import mcps_from_mce
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
from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.sql.postgres.lineage import PostgresLineageExtractor
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BytesTypeClass,
    MapTypeClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(custom_types.ARRAY, ArrayTypeClass)
register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.JSONB, BytesTypeClass)
register_custom_type(custom_types.HSTORE, MapTypeClass)


VIEW_LINEAGE_QUERY = """
WITH RECURSIVE view_deps AS (
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
UNION
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
INNER JOIN view_deps vd
    ON vd.dependent_schema = source_ns.nspname
    AND vd.dependent_view = source_table.relname
    AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
)


SELECT source_table, source_schema, dependent_view, dependent_schema
FROM view_deps
WHERE NOT (source_schema = 'information_schema' OR source_schema = 'pg_catalog')
ORDER BY source_schema, source_table;
"""


class ViewLineageEntry(BaseModel):
    # note that the order matches our query above
    # so pydantic is able to parse the tuple using parse_obj
    source_table: str
    source_schema: str
    dependent_view: str
    dependent_schema: str


class PostgresAuthMode(StrEnum):
    """Authentication mode for PostgreSQL connection."""

    PASSWORD = "PASSWORD"
    AWS_IAM = "AWS_IAM"


class BasePostgresConfig(BasicSQLAlchemyConfig):
    scheme: str = Field(default="postgresql+psycopg2", description="database scheme")
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["information_schema"])
    )

    # Authentication configuration
    auth_mode: PostgresAuthMode = Field(
        default=PostgresAuthMode.PASSWORD,
        description="Authentication mode to use for the PostgreSQL connection. "
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


class PostgresConfig(BasePostgresConfig, BaseUsageConfig):
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for databases to filter in ingestion. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )
    database: Optional[str] = Field(
        default=None,
        description="database (catalog). If set to Null, all databases will be considered for ingestion.",
    )
    initial_database: Optional[str] = Field(
        default="postgres",
        description=(
            "Initial database used to query for the list of databases, when ingesting multiple databases. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    include_query_lineage: bool = Field(
        default=False,
        description=(
            "Enable query-based lineage extraction from pg_stat_statements. "
            "Requires the pg_stat_statements extension to be installed and enabled. "
            "See documentation for setup instructions."
        ),
    )

    max_queries_to_extract: int = Field(
        default=1000,
        description=(
            "Maximum number of queries to extract from pg_stat_statements "
            "for lineage analysis. Queries are prioritized by execution time and frequency."
        ),
    )

    min_query_calls: Optional[int] = Field(
        default=1,
        description=(
            "Minimum number of executions required for a query to be included. "
            "Set higher to focus on frequently-used queries."
        ),
    )

    query_exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "SQL LIKE patterns to exclude from query extraction. "
            "Example: ['%pg_catalog%', '%temp_%'] to exclude catalog and temp tables."
        ),
    )

    @field_validator("max_queries_to_extract")
    @classmethod
    def validate_max_queries_to_extract(cls, value: int) -> int:
        """Validate max_queries_to_extract is within reasonable range."""
        if value <= 0:
            raise ValueError("max_queries_to_extract must be positive")
        if value > 10000:
            raise ValueError(
                "max_queries_to_extract must be <= 10000 to avoid memory issues"
            )
        return value

    @field_validator("min_query_calls")
    @classmethod
    def validate_min_query_calls(cls, value: Optional[int]) -> Optional[int]:
        """Validate min_query_calls is non-negative."""
        if value is not None and value < 0:
            raise ValueError("min_query_calls must be non-negative")
        return value

    @field_validator("query_exclude_patterns")
    @classmethod
    def validate_query_exclude_patterns(
        cls, value: Optional[List[str]]
    ) -> Optional[List[str]]:
        """Validate query_exclude_patterns has reasonable limits."""
        if value is None:
            return value

        if len(value) > 100:
            raise ValueError(
                "query_exclude_patterns must have <= 100 patterns to avoid performance issues"
            )

        for pattern in value:
            if len(pattern) > 500:
                raise ValueError(
                    f"Pattern '{pattern[:50]}...' exceeds 500 characters. "
                    "Use shorter patterns to avoid performance issues."
                )

        return value


@platform_name("Postgres")
@config_class(PostgresConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class PostgresSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, tables, and stored procedures
    - Column types associated with each table
    - Also supports PostGIS extensions
    - Table, row, and column statistics via optional SQL profiling
    """

    config: PostgresConfig

    def __init__(self, config: PostgresConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())

        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = None
        if config.auth_mode == PostgresAuthMode.AWS_IAM:
            hostname, port = parse_host_port(config.host_port, default_port=5432)
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

        self.sql_aggregator: Optional[SqlParsingAggregator] = None
        if self.config.include_query_lineage:
            self.sql_aggregator = SqlParsingAggregator(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
                generate_lineage=True,
                generate_queries=True,
                generate_usage_statistics=self.config.include_usage_statistics,
            )
            logger.info("SQL parsing aggregator initialized for query-based lineage")

    def get_platform(self):
        return "postgres"

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _setup_rds_iam_event_listener(
        self, engine: "Engine", database_name: Optional[str] = None
    ) -> None:
        """Setup SQLAlchemy event listener to inject RDS IAM tokens."""
        if not (
            self.config.auth_mode == PostgresAuthMode.AWS_IAM
            and self._rds_iam_token_manager
        ):
            return

        def do_connect_listener(_dialect, _conn_rec, _cargs, cparams):
            if not self._rds_iam_token_manager:
                raise RuntimeError("RDS IAM Token Manager is not initialized")
            cparams["password"] = self._rds_iam_token_manager.get_token()
            if cparams.get("sslmode") not in ("require", "verify-ca", "verify-full"):
                cparams["sslmode"] = "require"

        event.listen(engine, "do_connect", do_connect_listener)  # type: ignore[misc]

    def get_inspectors(self) -> Iterable[Inspector]:
        # Note: get_sql_alchemy_url will choose `sqlalchemy_uri` over the passed in database
        url = self.config.get_sql_alchemy_url(
            database=self.config.database or self.config.initial_database
        )

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        self._setup_rds_iam_event_listener(engine)

        with engine.connect() as conn:
            if self.config.database or self.config.sqlalchemy_uri:
                inspector = inspect(conn)
                yield inspector
            else:
                # pg_database catalog -  https://www.postgresql.org/docs/current/catalog-pg-database.html
                # exclude template databases - https://www.postgresql.org/docs/current/manage-ag-templatedbs.html
                # exclude rdsadmin - AWS RDS administrative database
                databases = conn.execute(
                    "SELECT datname from pg_database where datname not in ('template0', 'template1', 'rdsadmin')"
                )
                for db in databases:
                    if not self.config.database_pattern.allowed(db["datname"]):
                        continue

                    url = self.config.get_sql_alchemy_url(database=db["datname"])
                    db_engine = create_engine(url, **self.config.options)
                    self._setup_rds_iam_event_listener(
                        db_engine, database_name=db["datname"]
                    )

                    with db_engine.connect() as conn:
                        inspector = inspect(conn)
                        yield inspector

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_workunits_internal()

        if self.views_failed_parsing:
            for inspector in self.get_inspectors():
                if self.config.include_view_lineage:
                    yield from self._get_view_lineage_workunits(inspector)

        if self.config.include_query_lineage and self.sql_aggregator:
            yield from self._get_query_based_lineage_workunits()

    def _get_view_lineage_elements(
        self, inspector: Inspector
    ) -> Dict[Tuple[str, str], List[str]]:
        data: List[ViewLineageEntry] = []
        with inspector.engine.connect() as conn:
            results = conn.execute(VIEW_LINEAGE_QUERY)
            if results.returns_rows is False:
                return {}

            for row in results:
                data.append(ViewLineageEntry.model_validate(row))

        lineage_elements: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        # Loop over the lineages in the JSON data.
        for lineage in data:
            if not self.config.view_pattern.allowed(lineage.dependent_view):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            if not self.config.schema_pattern.allowed(lineage.dependent_schema):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            key = (lineage.dependent_view, lineage.dependent_schema)
            # Append the source table to the list.
            lineage_elements[key].append(
                mce_builder.make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=self.get_identifier(
                        schema=lineage.source_schema,
                        entity=lineage.source_table,
                        inspector=inspector,
                    ),
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            )

        return lineage_elements

    def _get_view_lineage_workunits(
        self, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        lineage_elements = self._get_view_lineage_elements(inspector)

        if not lineage_elements:
            return None

        # Loop over the lineage elements dictionary.
        for key, source_tables in lineage_elements.items():
            # Split the key into dependent view and dependent schema
            dependent_view, dependent_schema = key

            # Construct a lineage object.
            view_identifier = self.get_identifier(
                schema=dependent_schema, entity=dependent_view, inspector=inspector
            )
            if view_identifier not in self.views_failed_parsing:
                return
            urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=view_identifier,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            # use the mce_builder to ensure that the change proposal inherits
            # the correct defaults for auditHeader and systemMetadata
            lineage_mce = mce_builder.make_lineage_mce(
                source_tables,
                urn,
            )

            for item in mcps_from_mce(lineage_mce):
                yield item.as_workunit()

    def _get_query_based_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract and emit query-based lineage using pg_stat_statements.

        This supplements view-based lineage with lineage extracted from
        executed queries (INSERT INTO SELECT, CTAS, etc.).
        """
        logger.info("Starting query-based lineage extraction from pg_stat_statements")

        discovered_tables = set(self.report.tables_scanned or [])

        for inspector in self.get_inspectors():
            lineage_extractor = PostgresLineageExtractor(
                config=self.config,
                connection=inspector.engine.connect(),
                report=self.report,
                sql_aggregator=self.sql_aggregator,
                default_schema="public",
            )

            lineage_extractor.populate_lineage_from_queries(discovered_tables)

        with PerfTimer() as timer:
            mcp_count = 0
            for mcp in self.sql_aggregator.gen_metadata():
                yield mcp.as_workunit()
                mcp_count += 1

        logger.info(
            f"Generated {mcp_count} lineage workunits from queries "
            f"in {timer.elapsed_seconds():.2f} seconds"
        )

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        regular = f"{schema}.{entity}"
        if self.config.database:
            return f"{self.config.database}.{regular}"
        current_database = self.get_db_name(inspector)
        return f"{current_database}.{regular}"

    def add_profile_metadata(self, inspector: Inspector) -> None:
        try:
            with inspector.engine.connect() as conn:
                for row in conn.execute(
                    """SELECT table_catalog, table_schema, table_name, pg_table_size('"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"') AS table_size FROM information_schema.TABLES"""
                ):
                    self.profile_metadata_info.dataset_name_to_storage_bytes[
                        self.get_identifier(
                            schema=row.table_schema,
                            entity=row.table_name,
                            inspector=inspector,
                        )
                    ] = row.table_size
        except Exception as e:
            logger.error(f"failed to fetch profile metadata: {e}")

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
                    SELECT
                        p.proname AS name,
                        l.lanname AS language,
                        pg_get_function_arguments(p.oid) AS arguments,
                        pg_get_functiondef(p.oid) AS definition,
                        obj_description(p.oid, 'pg_proc') AS comment
                    FROM
                        pg_proc p
                    JOIN
                        pg_namespace n ON n.oid = p.pronamespace
                    JOIN
                        pg_language l ON l.oid = p.prolang
                    WHERE
                        p.prokind = 'p'
                        AND n.nspname = %s;
                """,
                (schema,),
            )

            procedure_rows = list(procedures)
            for row in procedure_rows:
                base_procedures.append(
                    BaseProcedure(
                        name=row.name,
                        language=row.language,
                        argument_signature=row.arguments,
                        return_type=None,
                        procedure_definition=row.definition,
                        created=None,
                        last_altered=None,
                        comment=row.comment,
                        extra_properties=None,
                    )
                )
            return base_procedures
