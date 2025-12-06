import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pydantic import field_validator
from pydantic.fields import Field

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from pyhive.sqlalchemy_hive import HiveDate, HiveDecimal, HiveDialect, HiveTimestamp
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import HiddenFromDocs
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.common.subtypes import (
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sql.hive.exceptions import InvalidDatasetIdentifierError
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineage,
    HiveStorageLineageConfigMixin,
)
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
    TimeTypeClass,
    ViewPropertiesClass,
)
from datahub.utilities import config_clean
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column

logger = logging.getLogger(__name__)

register_custom_type(HiveDate, DateTypeClass)
register_custom_type(HiveTimestamp, TimeTypeClass)
register_custom_type(HiveDecimal, NumberTypeClass)


try:
    from databricks_dbapi.sqlalchemy_dialects.hive import DatabricksPyhiveDialect
    from pyhive.sqlalchemy_hive import _type_map
    from sqlalchemy import types, util
    from sqlalchemy.engine import reflection

    @reflection.cache  # type: ignore
    def dbapi_get_columns_patched(self, connection, table_name, schema=None, **kw):
        """Patches the get_columns method from dbapi (databricks_dbapi.sqlalchemy_dialects.base) to pass the native type through"""
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != "# col_name"]
        result = []
        for col_name, col_type, _comment in rows:
            # Handle both oss hive and Databricks' hive partition header, respectively
            if col_name in ("# Partition Information", "# Partitioning"):
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            orig_col_type = col_type  # keep a copy
            col_type = re.search(r"^\w+", col_type).group(0)  # type: ignore
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn(
                    "Did not recognize type '{}' of column '{}'".format(
                        col_type, col_name
                    )
                )
                coltype = types.NullType  # type: ignore
            result.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": True,
                    "default": None,
                    "full_type": orig_col_type,  # pass it through
                    "comment": _comment,
                }
            )
        return result

    DatabricksPyhiveDialect.get_columns = dbapi_get_columns_patched
except ModuleNotFoundError:
    pass
except Exception as exp:
    logger.warning(f"Failed to patch method due to {exp}")


@reflection.cache  # type: ignore
def get_view_names_patched(self, connection, schema=None, **kw):
    query = "SHOW VIEWS"
    if schema:
        query += " IN " + self.identifier_preparer.quote_identifier(schema)
    return [row[0] for row in connection.execute(query)]


@reflection.cache  # type: ignore
def get_view_definition_patched(self, connection, view_name, schema=None, **kw):
    full_table = self.identifier_preparer.quote_identifier(view_name)
    if schema:
        full_table = "{}.{}".format(
            self.identifier_preparer.quote_identifier(schema),
            self.identifier_preparer.quote_identifier(view_name),
        )
    # Hive responds to the SHOW CREATE TABLE with the full view DDL,
    # including the view definition. However, for multiline view definitions,
    # it returns multiple rows (of one column each), each with a part of the definition.
    # Any whitespace at the beginning/end of each view definition line is lost.
    rows = connection.execute(f"SHOW CREATE TABLE {full_table}").fetchall()
    parts = [row[0] for row in rows]
    return "\n".join(parts)


HiveDialect.get_view_names = get_view_names_patched
HiveDialect.get_view_definition = get_view_definition_patched


class HiveConfig(TwoTierSQLAlchemyConfig, HiveStorageLineageConfigMixin):
    # defaults
    scheme: HiddenFromDocs[str] = Field(default="hive")

    # Overriding as table location lineage is richer implementation here than with include_table_location_lineage
    include_table_location_lineage: HiddenFromDocs[bool] = Field(default=False)

    @field_validator("host_port", mode="after")
    @classmethod
    def clean_host_port(cls, v: str) -> str:
        return config_clean.remove_protocol(v)


@platform_name("Hive")
@config_class(HiveConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default for views via `include_view_lineage`, and to upstream/downstream storage via `emit_storage_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default for views via `include_view_column_lineage`, and to storage via `include_column_lineage` when storage lineage is enabled",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
class HiveSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Detailed table and storage information
    - Table, row, and column statistics via optional SQL profiling.

    """

    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "hive")
        self.storage_lineage = HiveStorageLineage(
            config=config,
            env=config.env,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for tables and their storage lineage."""
        for wu in super().get_workunits_internal():
            yield wu

            if not isinstance(wu, MetadataWorkUnit):
                continue

            # Get dataset URN and required aspects using workunit methods
            try:
                dataset_urn = wu.get_urn()
            except (AttributeError, KeyError) as e:
                logger.debug(f"Workunit {wu.id} does not have a URN: {e}")
                continue

            try:
                dataset_props = wu.get_aspect_of_type(DatasetPropertiesClass)
                schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            except (AttributeError, KeyError) as e:
                logger.debug(f"Could not extract aspects from workunit {wu.id}: {e}")
                continue

            # Only proceed if we have the necessary properties for storage lineage
            if not dataset_props or not dataset_props.customProperties:
                continue

            location = dataset_props.customProperties.get("Location")
            if not location:
                continue

            table = {"StorageDescriptor": {"Location": location}}

            try:
                yield from self.storage_lineage.get_lineage_mcp(
                    dataset_urn=dataset_urn,
                    table=table,
                    dataset_schema=schema_metadata,
                )
            except (ValueError, TypeError, KeyError) as e:
                self.report.warning(
                    message="Failed to generate storage lineage",
                    context=dataset_urn,
                    exc=e,
                )

    def get_schema_names(self, inspector):
        if not isinstance(self.config, HiveConfig):
            raise TypeError(f"Expected HiveConfig, got {type(self.config).__name__}")
        # This condition restricts the ingestion to the specified database.
        if self.config.database:
            return [self.config.database]
        else:
            return super().get_schema_names(inspector)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        inspector: Inspector,
        pk_constraints: Optional[Dict[Any, Any]] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaFieldClass]:
        fields = super().get_schema_fields_for_column(
            dataset_name,
            column,
            inspector,
            pk_constraints,
            partition_keys=partition_keys,
        )

        if self._COMPLEX_TYPE.match(fields[0].nativeDataType) and isinstance(
            fields[0].type.type, NullTypeClass
        ):
            if len(fields) != 1:
                logger.warning(
                    f"Expected exactly 1 field for complex type {fields[0].nativeDataType}, "
                    f"got {len(fields)} fields. Skipping complex type expansion.",
                    extra={"dataset": dataset_name, "column": column.get("name")},
                )
                return fields

            field = fields[0]
            avro_schema = get_avro_schema_for_hive_column(
                column["name"], field.nativeDataType
            )

            new_fields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=True
            )

            new_fields[0].nullable = field.nullable
            new_fields[0].description = field.description
            new_fields[0].isPartOfKey = field.isPartOfKey
            return new_fields

        return fields

    # Hive SQLAlchemy connector returns views as tables in get_table_names.
    # See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273.
    # This override makes sure that we ingest view definitions for views
    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

        try:
            view_definition = inspector.get_view_definition(view, schema)
            # Some dialects return a TextClause instead of a raw string, so we need to convert them to a string.
            view_definition = str(view_definition) if view_definition else ""
        except NotImplementedError:
            view_definition = ""

        if view_definition:
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

            # Emit SubTypes aspect for views
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
            ).as_workunit()

        if view_definition and self.config.include_view_lineage:
            default_db = None
            default_schema = None
            try:
                default_db, default_schema = self.get_db_schema(dataset_name)
            except InvalidDatasetIdentifierError as e:
                logger.warning(f"Invalid view identifier '{dataset_name}': {e}")
                return

            self.aggregator.add_view_definition(
                view_urn=dataset_urn,
                view_definition=view_definition,
                default_db=default_db,
                default_schema=default_schema,
            )

    def get_partitions(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[List[str]]:
        partition_columns: List[dict] = inspector.get_indexes(
            table_name=table, schema=schema
        )
        for partition_column in partition_columns:
            if partition_column.get("column_names"):
                return partition_column.get("column_names")

        return []

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        (description, properties, location) = super().get_table_properties(
            inspector, schema, table
        )

        new_properties = {}
        for key, value in properties.items():
            if key and key[-1] == ":":
                new_properties[key[:-1]] = value
            else:
                new_properties[key] = value
        return (description, new_properties, location)
