import functools
import json
import logging
import time
import uuid
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Dict, Iterable, List, Optional, Union

import pydantic
import sqlalchemy
import trino
from packaging import version
from pydantic import Field
from sqlalchemy import exc, sql
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import TypeEngine
from trino.sqlalchemy import datatype
from trino.sqlalchemy.dialect import TrinoDialect

from datahub.configuration.common import HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    get_schema_metadata,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaMetadata,
)
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

logger = logging.getLogger(__name__)

register_custom_type(datatype.ROW, RecordTypeClass)
register_custom_type(datatype.MAP, MapTypeClass)
register_custom_type(datatype.DOUBLE, NumberTypeClass)


@dataclass
class TrinoReport(SQLSourceReport):
    """Extended report for Trino source with connector lineage metrics"""

    # Connector lineage metrics
    num_connector_lineage_processed: int = 0
    num_column_lineage_processed: int = 0
    connector_lineage_time_seconds: float = 0.0
    column_lineage_time_seconds: float = 0.0


KNOWN_CONNECTOR_PLATFORM_MAPPING = {
    "bigquery": "bigquery",
    "cassandra": "cassandra",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "db2": "db2",
    "delta_lake": "delta-lake",
    "delta-lake": "delta-lake",
    "druid": "druid",
    "elasticsearch": "elasticsearch",
    "glue": "glue",
    "hive": "hive",
    "hudi": "hudi",
    "iceberg": "iceberg",
    "mariadb": "mariadb",
    "mongodb": "mongodb",
    "mysql": "mysql",
    "oracle": "oracle",
    "pinot": "pinot",
    "postgresql": "postgres",
    "redshift": "redshift",
    "snowflake_distributed": "snowflake",
    "snowflake_parallel": "snowflake",
    "snowflake_jdbc": "snowflake",
    "sqlserver": "mssql",
    "teradata": "teradata",
    "vertica": "vertica",
}

TWO_TIER_CONNECTORS = [
    "cassandra",
    "clickhouse",
    "delta-lake",
    "delta_lake",
    "druid",
    "elasticsearch",
    "glue",
    "hive",
    "hudi",
    "iceberg",
    "mariadb",
    "mongodb",
    "mysql",
    "pinot",
]

PROPERTIES_TABLE_SUPPORTED_CONNECTORS = ["hive", "iceberg"]

# Type JSON was introduced in trino sqlalchemy dialect in version 0.317.0
if version.parse(trino.__version__) >= version.parse("0.317.0"):
    register_custom_type(datatype.JSON, RecordTypeClass)


@functools.lru_cache
def gen_catalog_connector_dict(engine: Engine) -> Dict[str, str]:
    query = dedent(
        """
        SELECT *
        FROM "system"."metadata"."catalogs"
    """
    ).strip()
    res = engine.execute(sql.text(query))
    return {row.catalog_name: row.connector_name for row in res}


def get_catalog_connector_name(engine: Engine, catalog_name: str) -> Optional[str]:
    return gen_catalog_connector_dict(engine).get(catalog_name)


# Read only table names and skip view names, as view names will also be returned
# from get_view_names
@reflection.cache  # type: ignore
def get_table_names(self, connection, schema: str = None, **kw):  # type: ignore
    schema = schema or self._get_default_schema_name(connection)
    if schema is None:
        raise exc.NoSuchTableError("schema is required")
    query = dedent(
        """
        SELECT "table_name"
        FROM "information_schema"."tables"
        WHERE "table_schema" = :schema and "table_type" != 'VIEW'
    """
    ).strip()
    res = connection.execute(sql.text(query), schema=schema)
    return [row.table_name for row in res]


# Include all table properties instead of only "comment" property
@reflection.cache  # type: ignore
def get_table_comment(self, connection, table_name: str, schema: str = None, **kw):  # type: ignore
    try:
        catalog_name = self._get_default_catalog_name(connection)
        if catalog_name is None:
            raise exc.NoSuchTableError("catalog is required in connection")
        connector_name = get_catalog_connector_name(connection.engine, catalog_name)
        if (
            connector_name is not None
            and connector_name in PROPERTIES_TABLE_SUPPORTED_CONNECTORS
        ):
            properties_table = self._get_full_table(f"{table_name}$properties", schema)
            query = f"SELECT * FROM {properties_table}"
            rows = connection.execute(sql.text(query)).fetchall()

            # Generate properties dictionary.
            properties = {}

            if len(rows) == 0:
                # No properties found, return empty dictionary
                return {}

            # Check if using the old format (key, value columns)
            if (
                connector_name == "iceberg"
                and len(rows[0]) == 2
                and "key" in rows[0]
                and "value" in rows[0]
            ):
                #  https://trino.io/docs/current/connector/iceberg.html#properties-table
                for row in rows:
                    if row["value"] is not None:
                        properties[row["key"]] = row["value"]
                return {"text": properties.get("comment"), "properties": properties}
            elif connector_name == "hive" and len(rows[0]) > 1 and len(rows) == 1:
                # https://trino.io/docs/current/connector/hive.html#properties-table
                row = rows[0]
                for col_name, col_value in row.items():
                    if col_value is not None:
                        properties[col_name] = col_value
                return {"text": properties.get("comment"), "properties": properties}

            # If we can't get the properties we still fallback to the default
        return self.get_table_comment_default(connection, table_name, schema)
    except Exception as e:
        logging.warning(
            f"Failed to get table comment for {table_name} in {schema}: {e}"
        )
        return {}


# Include column comment, original trino datatype as full_type
@reflection.cache  # type: ignore
def _get_columns(self, connection, table_name, schema: str = None, **kw):  # type: ignore
    schema = schema or self._get_default_schema_name(connection)
    query = dedent(
        """
        SELECT
            "column_name",
            "data_type",
            "column_default",
            UPPER("is_nullable") AS "is_nullable",
            "comment"
        FROM "information_schema"."columns"
        WHERE "table_schema" = :schema
            AND "table_name" = :table
        ORDER BY "ordinal_position" ASC
    """
    ).strip()
    res = connection.execute(sql.text(query), schema=schema, table=table_name)
    columns = []
    for record in res:
        column = dict(
            name=record.column_name,
            type=datatype.parse_sqltype(record.data_type),
            nullable=record.is_nullable == "YES",
            default=record.column_default,
            comment=record.comment,
        )
        columns.append(column)
    return columns


TrinoDialect.get_table_comment_default = TrinoDialect.get_table_comment
TrinoDialect.get_table_names = get_table_names
TrinoDialect.get_table_comment = get_table_comment
TrinoDialect._get_columns = _get_columns


class ConnectorDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    connector_database: Optional[str] = Field(
        default=None,
        description="Database name for three-tier connectors (e.g., PostgreSQL, Snowflake). "
        "Required for three-tier connectors, not used for two-tier connectors. "
        "Example: 'my_database' for PostgreSQL catalog.",
    )
    connector_platform: Optional[str] = Field(
        default=None,
        description="A connector's actual platform name. If not provided, will take from metadata tables"
        "Eg: hive catalog can have a connector platform as 'hive' or 'glue' or some other metastore.",
    )


class TrinoConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: HiddenFromDocs[str] = Field(default="trino")
    database: str = Field(description="database (catalog)")

    catalog_to_connector_details: Dict[str, ConnectorDetail] = Field(
        default={},
        description="A mapping of trino catalog to its connector details like connector database, env and platform instance."
        "This configuration is used to build lineage to the underlying connector. Use catalog name as key.",
    )

    ingest_lineage_to_connectors: bool = Field(
        default=True,
        description="Whether lineage of datasets to connectors should be ingested",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="Whether column-level lineage to upstream connectors should be ingested. "
        "Requires ingest_lineage_to_connectors to be enabled.",
    )

    trino_as_primary: bool = Field(
        default=True,
        description="Experimental feature. Whether trino dataset should be primary entity of the set of siblings",
    )

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        return f"{self.database}.{schema}.{table}"

    @pydantic.model_validator(mode="after")
    def validate_column_lineage_requires_connector_lineage(self) -> "TrinoConfig":
        if self.include_column_lineage and not self.ingest_lineage_to_connectors:
            raise ValueError(
                "include_column_lineage requires ingest_lineage_to_connectors to be enabled. "
                "Either set ingest_lineage_to_connectors=True or set include_column_lineage=False."
            )
        return self


@platform_name("Trino", doc_order=1)
@config_class(TrinoConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default for views via `include_view_lineage`, and to upstream connectors via `ingest_lineage_to_connectors`",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default for views via `include_view_column_lineage`, and to upstream connectors via `include_column_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
class TrinoSource(SQLAlchemySource):
    """

    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types and schema associated with each table
    - Table, row, and column statistics via optional SQL profiling

    """

    config: TrinoConfig
    report: TrinoReport

    def __init__(
        self, config: TrinoConfig, ctx: PipelineContext, platform: str = "trino"
    ):
        super().__init__(config, ctx, platform)
        self.report = TrinoReport()

    def get_db_name(self, inspector: Inspector) -> str:
        if self.config.database:
            return f"{self.config.database}"
        else:
            return super().get_db_name(inspector)

    def _get_source_dataset_urn(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
    ) -> Optional[str]:
        catalog_name = dataset_name.split(".")[0]
        connector_name = get_catalog_connector_name(inspector.engine, catalog_name)
        if not connector_name:
            return None
        connector_details = self.config.catalog_to_connector_details.get(
            catalog_name, ConnectorDetail()
        )
        connector_platform_name = KNOWN_CONNECTOR_PLATFORM_MAPPING.get(
            connector_details.connector_platform or connector_name
        )
        if not connector_platform_name:
            logging.debug(f"Platform '{connector_platform_name}' is not yet supported.")
            return None

        if connector_platform_name in TWO_TIER_CONNECTORS:  # connector is two tier
            return make_dataset_urn_with_platform_instance(
                platform=connector_platform_name,
                name=f"{schema}.{table}",
                platform_instance=connector_details.platform_instance,
                env=connector_details.env,
            )
        elif connector_details.connector_database:  # else connector is three tier
            return make_dataset_urn_with_platform_instance(
                platform=connector_platform_name,
                name=f"{connector_details.connector_database}.{schema}.{table}",
                platform_instance=connector_details.platform_instance,
                env=connector_details.env,
            )
        else:
            logging.warning(f"Connector database missing for Catalog '{catalog_name}'.")
        return None

    def _get_schema_metadata_for_lineage(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        is_view: bool = False,
    ) -> Optional[SchemaMetadata]:
        """
        Helper method to fetch schema metadata for column-level lineage generation.

        This method is called when include_column_lineage is enabled to get the schema
        information needed to create fine-grained lineage. It may add latency to ingestion
        as it makes additional database calls.

        Args:
            dataset_name: Full dataset name
            inspector: SQLAlchemy inspector
            schema: Schema name
            table: Table or view name
            is_view: Whether this is a view (excludes partition keys)

        Returns:
            SchemaMetadata if successful, None if an error occurs
        """
        try:
            columns = self._get_columns(dataset_name, inspector, schema, table)
            pk_constraints = inspector.get_pk_constraint(table, schema)

            extra_tags = self.get_extra_tags(inspector, schema, table)

            partitions = (
                None if is_view else self.get_partitions(inspector, schema, table)
            )

            schema_fields = self.get_schema_fields(
                dataset_name,
                columns,
                inspector,
                pk_constraints,
                tags=extra_tags,
                partition_keys=partitions,
            )

            return get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                pk_constraints,
                canonical_schema=schema_fields,
            )
        except sqlalchemy.exc.SQLAlchemyError as e:
            self.report.warning(
                title="Failed to Fetch Schema Metadata for Column Lineage",
                message="Unable to fetch schema metadata for column-level lineage. Skipping column lineage for this table. Table-level lineage will still be captured.",
                context=dataset_name,
                exc=e,
            )
            return None

    def gen_siblings_workunit(
        self,
        dataset_urn: str,
        source_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate sibling workunit for both trino dataset and its connector source dataset
        """
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=Siblings(
                primary=self.config.trino_as_primary, siblings=[source_dataset_urn]
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=source_dataset_urn,
            aspect=Siblings(
                primary=not self.config.trino_as_primary, siblings=[dataset_urn]
            ),
        ).as_workunit()

    def _match_upstream_field_path(
        self,
        trino_field_path: str,
        upstream_schema: Dict[str, SchemaField],
    ) -> Optional[str]:
        """
        Match a Trino field path to the corresponding upstream connector field path.

        Handles v1/v2 field path format differences by trying both exact match and normalized match.

        Args:
            trino_field_path: Field path from Trino schema
            upstream_schema: Dict mapping field paths to SchemaField objects from upstream

        Returns:
            The upstream field path that matches, or None if no match found
        """
        if trino_field_path in upstream_schema:
            return trino_field_path

        trino_field_v1 = get_simple_field_path_from_v2_field_path(trino_field_path)

        for upstream_field_path in upstream_schema:
            upstream_field_v1 = get_simple_field_path_from_v2_field_path(
                upstream_field_path
            )
            if trino_field_v1 == upstream_field_v1:
                return upstream_field_path

        return None

    def gen_lineage_workunit(
        self,
        dataset_urn: str,
        source_dataset_urn: str,
        schema_metadata: Optional[SchemaMetadata] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate dataset to source connector lineage workunit with optional column-level lineage.

        This creates lineage between a Trino dataset and its corresponding upstream connector dataset
        (e.g., Trino's hive_catalog.schema1.users → Hive's schema1.users).

        Column-level lineage uses a 1:1 name-based mapping approach, which is accurate because:

        For TABLES:
        - Trino tables are direct mappings to upstream connector tables
        - The schema in Trino matches the upstream source exactly
        - Column names and types are preserved as-is from the connector

        For VIEWS:
        - Trino views that exist in the connector (e.g., Hive views) have matching schemas
        - The view definition resides in the connector, so Trino exposes the same columns

        The lineage generation intelligently handles v1/v2 field path format differences by:
        1. Looking up the upstream schema from DataHub (if available)
        2. Matching Trino fields to upstream fields using normalized field paths
        3. Falling back to direct field path usage if DataHub is unavailable

        Note: This is separate from view-to-table lineage, which is handled by the base
        SQLAlchemySource class via SQL parsing of view definitions (include_view_lineage config).
        That mechanism correctly handles transformations, aliases, and joins within Trino views.
        """
        fine_grained_lineages: Optional[List[FineGrainedLineage]] = None

        if (
            self.config.include_column_lineage
            and schema_metadata is not None
            and schema_metadata.fields
        ):
            upstream_schema: Optional[Dict[str, SchemaField]] = None
            if self.ctx.graph:
                try:
                    upstream_schema_metadata = self.ctx.graph.get_aspect(
                        source_dataset_urn, SchemaMetadata
                    )
                    if upstream_schema_metadata and upstream_schema_metadata.fields:
                        upstream_schema = {
                            field.fieldPath: field
                            for field in upstream_schema_metadata.fields
                        }
                        logger.debug(
                            f"Found upstream schema for {source_dataset_urn} with {len(upstream_schema)} fields"
                        )
                except Exception as e:
                    logger.debug(
                        f"Could not fetch upstream schema for {source_dataset_urn}: {e}"
                    )

            fine_grained_lineages = []
            for field in schema_metadata.fields:
                if upstream_schema:
                    upstream_field_path = self._match_upstream_field_path(
                        field.fieldPath, upstream_schema
                    )
                    if not upstream_field_path:
                        logger.debug(
                            f"Could not match field {field.fieldPath} to upstream schema for {source_dataset_urn}"
                        )
                        continue
                else:
                    upstream_field_path = field.fieldPath

                fine_grained_lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(
                                source_dataset_urn, upstream_field_path
                            )
                        ],
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            make_schema_field_urn(dataset_urn, field.fieldPath)
                        ],
                    )
                )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineage(
                upstreams=[
                    Upstream(dataset=source_dataset_urn, type=DatasetLineageType.VIEW)
                ],
                fineGrainedLineages=fine_grained_lineages or None,
            ),
        ).as_workunit()

    def _gen_connector_lineage_workunits(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table_or_view: str,
        is_view: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate connector lineage workunits for a Trino table or view.

        This creates lineage from Trino datasets to their upstream connector sources,
        with optional column-level lineage.

        Args:
            dataset_name: Full dataset name
            inspector: SQLAlchemy inspector
            schema: Schema name
            table_or_view: Table or view name
            is_view: Whether this is a view (affects partition handling)
        """
        start_time = time.time()

        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        source_dataset_urn = self._get_source_dataset_urn(
            dataset_name, inspector, schema, table_or_view
        )
        if source_dataset_urn:
            self.report.num_connector_lineage_processed += 1

            schema_metadata = None
            if self.config.include_column_lineage:
                column_start = time.time()
                schema_metadata = self._get_schema_metadata_for_lineage(
                    dataset_name, inspector, schema, table_or_view, is_view=is_view
                )
                column_duration = time.time() - column_start
                if schema_metadata:
                    self.report.num_column_lineage_processed += 1
                self.report.column_lineage_time_seconds += column_duration

                if column_duration > 5.0:
                    logger.debug(
                        f"Column lineage for {dataset_name} took {column_duration:.2f}s - "
                        "consider optimizing database queries or limiting scope with table_pattern"
                    )

            yield from self.gen_siblings_workunit(dataset_urn, source_dataset_urn)
            yield from self.gen_lineage_workunit(
                dataset_urn, source_dataset_urn, schema_metadata
            )

            self.report.connector_lineage_time_seconds += time.time() - start_time

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )
        if self.config.ingest_lineage_to_connectors:
            yield from self._gen_connector_lineage_workunits(
                dataset_name, inspector, schema, table, is_view=False
            )

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )
        if self.config.ingest_lineage_to_connectors:
            yield from self._gen_connector_lineage_workunits(
                dataset_name, inspector, schema, view, is_view=True
            )

    @classmethod
    def create(cls, config_dict, ctx):
        config = TrinoConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: dict,
        inspector: Inspector,
        pk_constraints: Optional[dict] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        fields = super().get_schema_fields_for_column(
            dataset_name,
            column,
            inspector,
            pk_constraints,
        )

        if isinstance(column["type"], (datatype.ROW, sqltypes.ARRAY, datatype.MAP)):
            assert len(fields) == 1
            field = fields[0]
            # Get avro schema for subfields along with parent complex field
            avro_schema = self.get_avro_schema_from_data_type(
                column["type"], column["name"]
            )

            newfields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=True
            )

            # First field is the parent complex field
            newfields[0].nullable = field.nullable
            newfields[0].description = field.description
            newfields[0].isPartOfKey = field.isPartOfKey
            return newfields

        return fields

    def get_avro_schema_from_data_type(
        self, column_type: TypeEngine, column_name: str
    ) -> Dict[str, Any]:
        # Below Record structure represents the dataset level
        # Inner fields represent the complex field (struct/array/map/union)
        return {
            "type": "record",
            "name": "__struct_",
            "fields": [{"name": column_name, "type": _parse_datatype(column_type)}],
        }


_all_atomic_types = {
    sqltypes.BOOLEAN: "boolean",
    sqltypes.SMALLINT: "int",
    sqltypes.INTEGER: "int",
    sqltypes.BIGINT: "long",
    sqltypes.REAL: "float",
    datatype.DOUBLE: "double",  # type: ignore
    sqltypes.VARCHAR: "string",
    sqltypes.CHAR: "string",
    sqltypes.JSON: "record",
}


def _parse_datatype(s):
    if isinstance(s, sqlalchemy.types.ARRAY):
        return {
            "type": "array",
            "items": _parse_datatype(s.item_type),
            "native_data_type": repr(s),
        }
    elif isinstance(s, datatype.MAP):
        kt = _parse_datatype(s.key_type)
        vt = _parse_datatype(s.value_type)
        # keys are assumed to be strings in avro map
        return {
            "type": "map",
            "values": vt,
            "native_data_type": repr(s),
            "key_type": kt,
            "key_native_data_type": repr(s.key_type),
        }
    elif isinstance(s, datatype.ROW):
        return _parse_struct_fields(s.attr_types)
    else:
        return _parse_basic_datatype(s)


def _parse_struct_fields(parts):
    fields = []
    for name_and_type in parts:
        field_name = name_and_type[0].strip()
        field_type = _parse_datatype(name_and_type[1])
        fields.append({"name": field_name, "type": field_type})
    return {
        "type": "record",
        "name": "__struct_{}".format(str(uuid.uuid4()).replace("-", "")),
        "fields": fields,
        "native_data_type": f"ROW({parts})",
    }


def _parse_basic_datatype(s):
    for sql_type in _all_atomic_types:
        if isinstance(s, sql_type):
            return {
                "type": _all_atomic_types[sql_type],
                "native_data_type": repr(s),
                "_nullable": True,
            }

    if isinstance(s, sqlalchemy.types.DECIMAL):
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": s.precision,  # type: ignore
            "scale": s.scale,  # type: ignore
            "native_data_type": repr(s),
            "_nullable": True,
        }
    elif isinstance(s, sqlalchemy.types.Date):
        return {
            "type": "int",
            "logicalType": "date",
            "native_data_type": repr(s),
            "_nullable": True,
        }
    elif isinstance(s, (sqlalchemy.types.DATETIME, sqlalchemy.types.TIMESTAMP)):
        return {
            "type": "int",
            "logicalType": "timestamp-millis",
            "native_data_type": repr(s),
            "_nullable": True,
        }

    return {"type": "null", "native_data_type": repr(s)}
