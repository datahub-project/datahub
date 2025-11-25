import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

# This import verifies that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy.dialects.postgresql as custom_types

# GeoAlchemy adds support for PostGIS extensions in SQLAlchemy. In order to
# activate it, we must import it so that it can hook into SQLAlchemy. While
# we don't use the Geometry type that we import, we do care about the side
# effects of the import. For more details, see here:
# https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html#reflecting-tables.
from geoalchemy2 import Geometry  # noqa: F401
from pydantic import BaseModel
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect, text
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
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLCommonConfig,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BytesTypeClass,
    MapTypeClass,
)
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


class ForeignTableMetadata(BaseModel):
    """Metadata for a Foreign Data Wrapper (FDW) table."""

    table_name: str
    server_name: Optional[str] = None
    fdw_name: Optional[str] = None
    server_options: Optional[str] = None
    table_options: Optional[str] = None


# Query to get foreign tables in a schema
FOREIGN_TABLES_QUERY = """
    SELECT
        c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema
      AND c.relkind = 'f'
    ORDER BY c.relname
"""

# Query to get FDW metadata for a specific foreign table
FOREIGN_TABLE_METADATA_QUERY = """
    SELECT
        c.relname AS table_name,
        fs.srvname AS server_name,
        fdw.fdwname AS fdw_name,
        array_to_string(fs.srvoptions, ', ') AS server_options,
        array_to_string(ft.ftoptions, ', ') AS table_options
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
    JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
    JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
    WHERE n.nspname = :schema
      AND c.relname = :table
"""


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


class PostgresConfig(BasePostgresConfig):
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

    include_foreign_tables: bool = Field(
        default=True,
        description="Include Foreign Data Wrapper (FDW) tables in ingestion. "
        "Foreign tables expose external data sources (SQL Server, Oracle, S3, etc.) as Postgres tables.",
    )

    foreign_table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for foreign tables to filter in ingestion. "
        "Specify regex to match the entire foreign table name in database.schema.table_name format.",
    )


@platform_name("Postgres")
@config_class(PostgresConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class PostgresSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, tables, stored procedures, and foreign tables
    - Column types associated with each table
    - Foreign Data Wrapper (FDW) tables that expose external data sources
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

    def get_foreign_table_names(self, inspector: Inspector, schema: str) -> List[str]:
        """
        Get foreign table names from a schema using Foreign Data Wrappers (FDW).

        Args:
            inspector: SQLAlchemy inspector
            schema: Schema name

        Returns:
            List of foreign table names
        """
        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(
                    text(FOREIGN_TABLES_QUERY),
                    {"schema": schema},
                )
                return [row[0] for row in result]
        except Exception as e:
            logger.warning(f"Failed to fetch foreign tables for schema {schema}: {e}")
            return []

    def get_foreign_table_metadata(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[ForeignTableMetadata]:
        """
        Get FDW-specific metadata for a foreign table.

        Args:
            inspector: SQLAlchemy inspector
            schema: Schema name
            table: Foreign table name

        Returns:
            ForeignTableMetadata object or None if not found
        """
        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(
                    text(FOREIGN_TABLE_METADATA_QUERY),
                    {"schema": schema, "table": table},
                )
                row = result.fetchone()
                if row:
                    return ForeignTableMetadata(
                        table_name=row[0],
                        server_name=row[1],
                        fdw_name=row[2],
                        server_options=row[3],
                        table_options=row[4],
                    )
        except Exception as e:
            logger.warning(f"Failed to fetch FDW metadata for {schema}.{table}: {e}")
        return None

    def loop_foreign_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        """
        Process foreign tables similar to regular tables.

        Args:
            inspector: SQLAlchemy inspector
            schema: Schema name
            sql_config: SQL configuration

        Yields:
            MetadataWorkUnit or SqlWorkUnit
        """
        if not self.config.include_foreign_tables:
            return

        tables_seen: Set[str] = set()

        try:
            for table in self.get_foreign_table_names(inspector, schema):
                dataset_name = self.get_identifier(
                    schema=schema, entity=table, inspector=inspector
                )

                if dataset_name not in tables_seen:
                    tables_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} already seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="table")

                # Apply both table_pattern and foreign_table_pattern
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                if not self.config.foreign_table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_foreign_table(
                        dataset_name,
                        inspector,
                        schema,
                        table,
                        sql_config,
                    )
                except Exception as e:
                    self.report.warning(
                        "Error processing foreign table",
                        context=f"{schema}.{table}",
                        exc=e,
                    )
        except Exception as e:
            self.report.failure(
                "Error processing foreign tables",
                context=schema,
                exc=e,
            )

    def _process_foreign_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        Process a single foreign table and generate work units.

        Args:
            dataset_name: Full dataset identifier
            inspector: SQLAlchemy inspector
            schema: Schema name
            table: Foreign table name
            sql_config: SQL configuration

        Yields:
            SqlWorkUnit or MetadataWorkUnit
        """
        # Get FDW metadata
        fdw_metadata = self.get_foreign_table_metadata(inspector, schema, table)

        # Process the foreign table using the standard _process_table method
        # but we'll add FDW-specific custom properties
        yield from self._process_table(
            dataset_name,
            inspector,
            schema,
            table,
            sql_config,
            data_reader=None,  # Foreign tables may not support profiling
        )

        # If we have FDW metadata, emit additional custom properties
        if fdw_metadata:
            from datahub.emitter import mce_builder
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
                DatasetPropertiesClass,
            )

            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            # Get existing properties and add FDW metadata
            custom_properties = {
                "is_foreign_table": "true",
            }

            if fdw_metadata.server_name:
                custom_properties["fdw_server"] = fdw_metadata.server_name

            if fdw_metadata.fdw_name:
                custom_properties["fdw_type"] = fdw_metadata.fdw_name

            if fdw_metadata.server_options:
                custom_properties["fdw_server_options"] = fdw_metadata.server_options

            if fdw_metadata.table_options:
                custom_properties["fdw_table_options"] = fdw_metadata.table_options

            # Create a patch MCP to add custom properties
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=table,
                    customProperties=custom_properties,
                ),
            )

            yield mcp.as_workunit()

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        Override to include foreign tables after regular tables.

        Args:
            inspector: SQLAlchemy inspector
            schema: Schema name
            sql_config: SQL configuration

        Yields:
            SqlWorkUnit or MetadataWorkUnit
        """
        # First, process regular tables using parent implementation
        yield from super().loop_tables(inspector, schema, sql_config)

        # Then, process foreign tables
        yield from self.loop_foreign_tables(inspector, schema, sql_config)
