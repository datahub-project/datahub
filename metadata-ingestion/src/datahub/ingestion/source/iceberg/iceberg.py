import json
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

from iceberg.api import types as IcebergTypes
from iceberg.api.table import Table
from iceberg.api.types.types import NestedField
from iceberg.core.base_table import BaseTable
from iceberg.core.filesystem.filesystem_tables import FilesystemTables
from iceberg.exceptions import NoSuchTableException

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
    make_user_urn,
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
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergSourceConfig,
    IcebergSourceReport,
)
from datahub.ingestion.source.iceberg.iceberg_profiler import IcebergProfiler
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

LOGGER = logging.getLogger(__name__)

_all_atomic_types = {
    IcebergTypes.BooleanType: "boolean",
    IcebergTypes.IntegerType: "int",
    IcebergTypes.LongType: "long",
    IcebergTypes.FloatType: "float",
    IcebergTypes.DoubleType: "double",
    IcebergTypes.BinaryType: "bytes",
    IcebergTypes.StringType: "string",
}


@platform_name("Iceberg")
@support_status(SupportStatus.TESTING)
@config_class(IcebergSourceConfig)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Optionally enabled via configuration, an Iceberg instance represents the datalake name where the table is stored.",
)
@capability(SourceCapability.DOMAINS, "Currently not supported.", supported=False)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration.")
@capability(
    SourceCapability.PARTITION_SUPPORT, "Currently not supported.", supported=False
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default.")
@capability(
    SourceCapability.OWNERSHIP,
    "Optionally enabled via configuration by specifying which Iceberg table property holds user or group ownership.",
)
class IcebergSource(Source):
    """
    ## Integration Details

    The DataHub Iceberg source plugin extracts metadata from [Iceberg tables](https://iceberg.apache.org/spec/) stored in a distributed or local file system.
    Typically, Iceberg tables are stored in a distributed file system like S3 or Azure Data Lake Storage (ADLS) and registered in a catalog.  There are various catalog
    implementations like Filesystem-based, RDBMS-based or even REST-based catalogs.  This Iceberg source plugin relies on the
    [Iceberg python_legacy library](https://github.com/apache/iceberg/tree/master/python_legacy) and its support for catalogs is limited at the moment.
    A new version of the [Iceberg Python library](https://github.com/apache/iceberg/tree/master/python) is currently in development and should fix this.
    Because of this limitation, this source plugin **will only ingest HadoopCatalog-based tables that have a `version-hint.text` metadata file**.

    Ingestion of tables happens in 2 steps:
    1. Discover Iceberg tables stored in file system.
    2. Load discovered tables using Iceberg python_legacy library

    The current implementation of the Iceberg source plugin will only discover tables stored in a local file system or in ADLS.  Support for S3 could
    be added fairly easily.
    """

    def __init__(self, config: IcebergSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.PLATFORM: str = "iceberg"
        self.report: IcebergSourceReport = IcebergSourceReport()
        self.config: IcebergSourceConfig = config
        self.iceberg_client: FilesystemTables = config.filesystem_tables

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "IcebergSource":
        config = IcebergSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for dataset_path, dataset_name in self.config.get_paths():  # Tuple[str, str]
            try:
                if not self.config.table_pattern.allowed(dataset_name):
                    # Path contained a valid Iceberg table, but is rejected by pattern.
                    self.report.report_dropped(dataset_name)
                    continue

                # Try to load an Iceberg table.  Might not contain one, this will be caught by NoSuchTableException.
                table: Table = self.iceberg_client.load(dataset_path)
                yield from self._create_iceberg_workunit(dataset_name, table)
            except NoSuchTableException:
                # Path did not contain a valid Iceberg table. Silently ignore this.
                LOGGER.debug(
                    f"Path {dataset_path} does not contain table {dataset_name}"
                )
                pass
            except Exception as e:
                self.report.report_failure("general", f"Failed to create workunit: {e}")
                LOGGER.exception(
                    f"Exception while processing table {dataset_path}, skipping it.",
                )

    def _create_iceberg_workunit(
        self, dataset_name: str, table: Table
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(dataset_name)
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            self.PLATFORM,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )

        custom_properties: Dict = dict(table.properties())
        custom_properties["location"] = table.location()
        try:
            if isinstance(table, BaseTable) and table.current_snapshot():
                custom_properties["snapshot-id"] = str(
                    table.current_snapshot().snapshot_id
                )
                custom_properties[
                    "manifest-list"
                ] = table.current_snapshot().manifest_location
        except KeyError:
            # The above API is not well implemented, and can throw KeyError when there is no data.
            pass
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=table.properties().get("comment", None),
            customProperties=custom_properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        dataset_ownership = self._get_ownership_aspect(table)
        if dataset_ownership:
            dataset_snapshot.aspects.append(dataset_ownership)

        schema_metadata = self._create_schema_metadata(dataset_name, table)
        dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        dpi_aspect = self._get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        if self.config.profiling.enabled:
            profiler = IcebergProfiler(self.report, self.config.profiling)
            yield from profiler.profile_table(dataset_name, dataset_urn, table)

    def _get_ownership_aspect(self, table: Table) -> Optional[OwnershipClass]:
        owners = []
        if self.config.user_ownership_property:
            if self.config.user_ownership_property in table.properties():
                user_owner = table.properties()[self.config.user_ownership_property]
                owners.append(
                    OwnerClass(
                        owner=make_user_urn(user_owner),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                        source=None,
                    )
                )
        if self.config.group_ownership_property:
            if self.config.group_ownership_property in table.properties():
                group_owner = table.properties()[self.config.group_ownership_property]
                owners.append(
                    OwnerClass(
                        owner=make_group_urn(group_owner),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                        source=None,
                    )
                )
        if owners:
            return OwnershipClass(owners=owners)
        return None

    def _get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.PLATFORM),
                    instance=make_dataplatform_instance_urn(
                        self.PLATFORM, self.config.platform_instance
                    ),
                ),
            )
            wu = MetadataWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu

        return None

    def _create_schema_metadata(
        self, dataset_name: str, table: Table
    ) -> SchemaMetadata:
        schema_fields: List[SchemaField] = self._get_schema_fields(
            table.schema().columns()
        )
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchema(rawSchema=repr(table.schema())),
            fields=schema_fields,
        )
        return schema_metadata

    def _get_schema_fields(self, columns: Tuple) -> List[SchemaField]:
        canonical_schema: List[SchemaField] = []
        for column in columns:
            fields = self._get_schema_fields_for_column(column)
            canonical_schema.extend(fields)
        return canonical_schema

    def _get_schema_fields_for_column(
        self,
        column: NestedField,
    ) -> List[SchemaField]:
        field_type: IcebergTypes.Type = column.type
        if field_type.is_primitive_type() or field_type.is_nested_type():
            avro_schema: Dict = self._get_avro_schema_from_data_type(column)
            schema_fields: List[SchemaField] = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=column.is_optional
            )
            return schema_fields

        raise ValueError(f"Invalid Iceberg field type: {field_type}")

    def _get_avro_schema_from_data_type(self, column: NestedField) -> Dict[str, Any]:
        """
        See Iceberg documentation for Avro mapping:
        https://iceberg.apache.org/#spec/#appendix-a-format-specific-requirements
        """
        # The record structure represents the dataset level.
        # The inner fields represent the complex field (struct/array/map/union).
        return {
            "type": "record",
            "name": "__struct_",
            "fields": [
                {
                    "name": column.name,
                    "type": _parse_datatype(column.type, column.is_optional),
                    "doc": column.doc,
                }
            ],
        }

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass


def _parse_datatype(type: IcebergTypes.Type, nullable: bool = False) -> Dict[str, Any]:
    # Check for complex types: struct, list, map
    if type.is_list_type():
        list_type: IcebergTypes.ListType = type
        return {
            "type": "array",
            "items": _parse_datatype(list_type.element_type),
            "native_data_type": str(type),
            "_nullable": nullable,
        }
    elif type.is_map_type():
        # The Iceberg Map type will be handled differently.  The idea is to translate the map
        # similar to the Map.Entry struct of Java i.e. as an array of map_entry struct, where
        # the map_entry struct has a key field and a value field. The key and value type can
        # be complex or primitive types.
        map_type: IcebergTypes.MapType = type
        map_entry: Dict[str, Any] = {
            "type": "record",
            "name": _gen_name("__map_entry_"),
            "fields": [
                {
                    "name": "key",
                    "type": _parse_datatype(map_type.key_type(), False),
                },
                {
                    "name": "value",
                    "type": _parse_datatype(map_type.value_type(), True),
                },
            ],
        }
        return {
            "type": "array",
            "items": map_entry,
            "native_data_type": str(type),
            "_nullable": nullable,
        }
    elif type.is_struct_type():
        structType: IcebergTypes.StructType = type
        return _parse_struct_fields(structType.fields, nullable)
    else:
        # Primitive types
        return _parse_basic_datatype(type, nullable)


def _parse_struct_fields(parts: Tuple[NestedField], nullable: bool) -> Dict[str, Any]:
    fields = []
    for nested_field in parts:  # type: NestedField
        field_name = nested_field.name
        field_type = _parse_datatype(nested_field.type, nested_field.is_optional)
        fields.append({"name": field_name, "type": field_type, "doc": nested_field.doc})
    return {
        "type": "record",
        "name": _gen_name("__struct_"),
        "fields": fields,
        "native_data_type": "struct<{}>".format(parts),
        "_nullable": nullable,
    }


def _parse_basic_datatype(
    type: IcebergTypes.PrimitiveType, nullable: bool
) -> Dict[str, Any]:
    """
    See https://iceberg.apache.org/#spec/#avro
    """
    # Check for an atomic types.
    for iceberg_type in _all_atomic_types.keys():
        if isinstance(type, iceberg_type):
            return {
                "type": _all_atomic_types[iceberg_type],
                "native_data_type": repr(type),
                "_nullable": nullable,
            }

    # Fixed is a special case where it is not an atomic type and not a logical type.
    if isinstance(type, IcebergTypes.FixedType):
        fixed_type: IcebergTypes.FixedType = type
        return {
            "type": "fixed",
            "name": _gen_name("__fixed_"),
            "size": fixed_type.length,
            "native_data_type": repr(fixed_type),
            "_nullable": nullable,
        }

    # Not an atomic type, so check for a logical type.
    if isinstance(type, IcebergTypes.DecimalType):
        # Also of interest: https://avro.apache.org/docs/current/spec.html#Decimal
        decimal_type: IcebergTypes.DecimalType = type
        return {
            # "type": "bytes", # when using bytes, avro drops _nullable attribute and others.  See unit test.
            "type": "fixed",  # to fix avro bug ^ resolved by using a fixed type
            "name": _gen_name(
                "__fixed_"
            ),  # to fix avro bug ^ resolved by using a fixed type
            "size": 1,  # to fix avro bug ^ resolved by using a fixed type
            "logicalType": "decimal",
            "precision": decimal_type.precision,
            "scale": decimal_type.scale,
            "native_data_type": repr(decimal_type),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.UUIDType):
        uuid_type: IcebergTypes.UUIDType = type
        return {
            "type": "string",
            "logicalType": "uuid",
            "native_data_type": repr(uuid_type),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.DateType):
        date_type: IcebergTypes.DateType = type
        return {
            "type": "int",
            "logicalType": "date",
            "native_data_type": repr(date_type),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.TimeType):
        time_type: IcebergTypes.TimeType = type
        return {
            "type": "long",
            "logicalType": "time-micros",
            "native_data_type": repr(time_type),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.TimestampType):
        timestamp_type: IcebergTypes.TimestampType = type
        # Avro supports 2 types of timestamp:
        #  - Timestamp: independent of a particular timezone or calendar (TZ information is lost)
        #  - Local Timestamp: represents a timestamp in a local timezone, regardless of what specific time zone is considered local
        # utcAdjustment: bool = True
        return {
            "type": "long",
            "logicalType": "timestamp-micros",
            # Commented out since Avro's Python implementation (1.11.0) does not support local-timestamp-micros, even though it exists in the spec.
            # See bug report: https://issues.apache.org/jira/browse/AVRO-3476 and PR https://github.com/apache/avro/pull/1634
            # "logicalType": "timestamp-micros"
            # if timestamp_type.adjust_to_utc
            # else "local-timestamp-micros",
            "native_data_type": repr(timestamp_type),
            "_nullable": nullable,
        }

    return {"type": "null", "native_data_type": repr(type)}


def _gen_name(prefix: str) -> str:
    return f"{prefix}{str(uuid.uuid4()).replace('-', '')}"
