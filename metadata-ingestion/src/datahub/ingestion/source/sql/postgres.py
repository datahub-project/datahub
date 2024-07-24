import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

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
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

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
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BytesTypeClass,
    MapTypeClass,
)

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


class BasePostgresConfig(BasicSQLAlchemyConfig):
    scheme: str = Field(default="postgresql+psycopg2", description="database scheme")
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["information_schema"])
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


@platform_name("Postgres")
@config_class(PostgresConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
class PostgresSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Also supports PostGIS extensions
    - Table, row, and column statistics via optional SQL profiling
    """

    config: PostgresConfig

    def __init__(self, config: PostgresConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "postgres"

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        # Note: get_sql_alchemy_url will choose `sqlalchemy_uri` over the passed in database
        url = self.config.get_sql_alchemy_url(
            database=self.config.database or self.config.initial_database
        )
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            if self.config.database or self.config.sqlalchemy_uri:
                inspector = inspect(conn)
                yield inspector
            else:
                # pg_database catalog -  https://www.postgresql.org/docs/current/catalog-pg-database.html
                # exclude template databases - https://www.postgresql.org/docs/current/manage-ag-templatedbs.html
                databases = conn.execute(
                    "SELECT datname from pg_database where datname not in ('template0', 'template1')"
                )
                for db in databases:
                    if not self.config.database_pattern.allowed(db["datname"]):
                        continue
                    url = self.config.get_sql_alchemy_url(database=db["datname"])
                    with create_engine(url, **self.config.options).connect() as conn:
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
                data.append(ViewLineageEntry.parse_obj(row))

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
