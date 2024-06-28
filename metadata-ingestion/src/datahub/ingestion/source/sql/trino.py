import functools
import json
import logging
import uuid
from textwrap import dedent
from typing import Any, Dict, Iterable, List, Optional, Union

import sqlalchemy
import trino
from packaging import version
from pydantic.fields import Field
from sqlalchemy import exc, sql
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import TypeEngine
from trino.sqlalchemy import datatype
from trino.sqlalchemy.dialect import TrinoDialect

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
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
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
)

register_custom_type(datatype.ROW, RecordTypeClass)
register_custom_type(datatype.MAP, MapTypeClass)
register_custom_type(datatype.DOUBLE, NumberTypeClass)


KNOWN_CONNECTOR_PLATFORM_MAPPING = {
    "clickhouse": "clickhouse",
    "hive": "hive",
    "glue": "glue",
    "iceberg": "iceberg",
    "mysql": "mysql",
    "postgresql": "postgres",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "snowflake_distributed": "snowflake",
    "snowflake_parallel": "snowflake",
    "snowflake_jdbc": "snowflake",
}

TWO_TIER_CONNECTORS = ["clickhouse", "hive", "glue", "mysql", "iceberg"]

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
        if connector_name is None:
            return {}
        if connector_name in PROPERTIES_TABLE_SUPPORTED_CONNECTORS:
            properties_table = self._get_full_table(f"{table_name}$properties", schema)
            query = f"SELECT * FROM {properties_table}"
            row = connection.execute(sql.text(query)).fetchone()

            # Generate properties dictionary.
            properties = {}
            if row:
                for col_name, col_value in row.items():
                    if col_value is not None:
                        properties[col_name] = col_value

            return {"text": properties.get("comment", None), "properties": properties}
        else:
            return self.get_table_comment_default(connection, table_name, schema)
    except Exception:
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
    connector_database: Optional[str] = Field(default=None, description="")
    connector_platform: Optional[str] = Field(
        default=None,
        description="A connector's actual platform name. If not provided, will take from metadata tables"
        "Eg: hive catalog can have a connector platform as 'hive' or 'glue' or some other metastore.",
    )


class TrinoConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: str = Field(default="trino", description="", hidden_from_docs=True)
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

    trino_as_primary: bool = Field(
        default=True,
        description="Experimental feature. Whether trino dataset should be primary entity of the set of siblings",
    )

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        return f"{self.database}.{schema}.{table}"


@platform_name("Trino", doc_order=1)
@config_class(TrinoConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class TrinoSource(SQLAlchemySource):
    """

    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types and schema associated with each table
    - Table, row, and column statistics via optional SQL profiling

    """

    config: TrinoConfig

    def __init__(
        self, config: TrinoConfig, ctx: PipelineContext, platform: str = "trino"
    ):
        super().__init__(config, ctx, platform)

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

    def gen_lineage_workunit(
        self,
        dataset_urn: str,
        source_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate dataset to source connector lineage workunit
        """
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineage(
                upstreams=[
                    Upstream(dataset=source_dataset_urn, type=DatasetLineageType.VIEW)
                ]
            ),
        ).as_workunit()

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
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            source_dataset_urn = self._get_source_dataset_urn(
                dataset_name, inspector, schema, table
            )
            if source_dataset_urn:
                yield from self.gen_siblings_workunit(dataset_urn, source_dataset_urn)
                yield from self.gen_lineage_workunit(dataset_urn, source_dataset_urn)

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
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            source_dataset_urn = self._get_source_dataset_urn(
                dataset_name, inspector, schema, view
            )
            if source_dataset_urn:
                yield from self.gen_siblings_workunit(dataset_urn, source_dataset_urn)

    @classmethod
    def create(cls, config_dict, ctx):
        config = TrinoConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: dict,
        pk_constraints: Optional[dict] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        fields = super().get_schema_fields_for_column(
            dataset_name, column, pk_constraints
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
    for sql_type in _all_atomic_types.keys():
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
