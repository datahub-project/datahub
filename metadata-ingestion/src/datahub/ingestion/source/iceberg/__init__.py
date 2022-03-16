import json
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

from azure.storage.filedatalake import PathProperties
from iceberg.api import types as IcebergTypes
from iceberg.api.table import Table
from iceberg.api.types.types import NestedField
from iceberg.exceptions import NoSuchTableException

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
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
    SchemaField,
    SchemalessClass,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
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


class IcebergSource(Source):
    config: IcebergSourceConfig
    report: IcebergSourceReport = IcebergSourceReport()

    def __init__(self, config: IcebergSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.platform = "iceberg"
        self.fsClient = config.filesystem_client
        self.icebergClient = config.filesystem_tables

    @classmethod
    def create(cls, config_dict, ctx):
        config = IcebergSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Current code supports this table name scheme:
          {namespaceName}/{tableName}
        where each name is only one level deep
        """
        for tablePath in self.config.get_paths():
            try:
                # TODO Stripping 'base_path/' from tablePath.  Weak code, need to be improved later
                datasetName = ".".join(
                    tablePath[len(self.config.base_path) + 1 :].split("/")
                )

                if not self.config.table_pattern.allowed(datasetName):
                    # Path contained a valid Iceberg table, but is rejected by pattern.
                    self.report.report_dropped(datasetName)
                    continue

                # Try to load an Iceberg table.  Might not contain one, this will be caught by NoSuchTableException.
                table: Table = self.icebergClient.load(
                    f"{self.config.filesystem_url}/{tablePath}"
                )

                yield from self._createIcebergWorkunit(datasetName, table)
            except NoSuchTableException:
                # Path did not contain a valid Iceberg table. Silently ignore this.
                pass
            except Exception as e:
                self.report.report_failure("general", f"Failed to create workunit: {e}")
                LOGGER.exception(
                    f"Exception while processing table {self.config.filesystem_url}/{tablePath}, skipping it.",
                    e,
                )

    def _createIcebergWorkunit(
        self, datasetName: str, table: Table
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(datasetName)
        datasetUrn = make_dataset_urn_with_platform_instance(
            self.platform,
            datasetName,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=datasetUrn,
            aspects=[],
        )

        customProperties = dict(table.properties())
        customProperties["location"] = table.location()
        # A table might not have any snapshots, i.e. no data.
        if len(table.snapshots()) > 0:
            customProperties["snapshot-id"] = str(table.current_snapshot().snapshot_id)
            customProperties[
                "manifest-list"
            ] = table.current_snapshot().manifest_location
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=table.properties().get("comment", None),
            customProperties=customProperties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # TODO: Should we use last-updated-ms instead?  YES
        # last_updated_time_millis = (
        #     table.current_snapshot().timestamp_millis
        # )
        # TODO: The following seems to go in OperationClass.  It is usually part of the "usage" source.  Should I create a new python module?
        # operation_aspect = OperationClass(
        #     timestampMillis=get_sys_time(),
        #     lastUpdatedTimestamp=last_updated_time_millis,
        #     operationType=OperationTypeClass.UNKNOWN,
        # )
        # TODO: How do I add an operation_aspect?

        if "owner" in table.properties():
            ownerEmail = table.properties()["owner"]
            owners = [
                OwnerClass(
                    owner=make_user_urn(ownerEmail),
                    type=OwnershipTypeClass.PRODUCER,
                    source=None,
                )
            ]
            dataset_ownership = OwnershipClass(owners=owners)
            dataset_snapshot.aspects.append(dataset_ownership)

        schemaMetadata = self._createSchemaMetadata(datasetName, table)
        dataset_snapshot.aspects.append(schemaMetadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=datasetName, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=datasetUrn)
        if dpi_aspect:
            yield dpi_aspect

        if self.config.profiling.enabled:
            profiler = IcebergProfiler(self.report, self.config.profiling)
            yield from profiler.profileTable(
                self.config.env, datasetName, datasetUrn, table, schemaMetadata
            )

    def get_dataplatform_instance_aspect(
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
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = MetadataWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None

    def _createSchemaMetadata(self, dataset_name: str, table: Table) -> SchemaMetadata:
        schema_fields = self.get_schema_fields(table.schema().columns())

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=SchemalessClass(),  # TODO: Not sure what to use here...
            fields=schema_fields,
        )
        return schema_metadata

    def get_schema_fields(self, columns: Tuple) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            fields = self.get_schema_fields_for_column(column)
            canonical_schema.extend(fields)
        return canonical_schema

    def get_schema_fields_for_column(
        self,
        column: NestedField,
    ) -> List[SchemaField]:
        field_type: IcebergTypes.Type = column.type
        if field_type.is_primitive_type():
            avro_schema = self.get_avro_schema_from_data_type(column)
            newfields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=column.is_optional
            )
            assert len(newfields) == 1
            # newfields[0].nullable = column.is_optional
            # newfields[0].description = column.doc
            return newfields
        elif field_type.is_nested_type():
            # Get avro schema for subfields along with parent complex field
            avro_schema = self.get_avro_schema_from_data_type(column)
            newfields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=column.is_optional
            )
            # First field is the parent complex field
            # newfields[0].nullable = column.is_optional
            return newfields
        else:
            raise ValueError()

    def get_avro_schema_from_data_type(self, column: NestedField) -> Dict[str, Any]:
        """
        See Iceberg documentation for Avro mapping:
        https://iceberg.apache.org/#spec/#appendix-a-format-specific-requirements
        """
        # Below Record structure represents the dataset level
        # Inner fields represent the complex field (struct/array/map/union)
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
        listType: IcebergTypes.ListType = type
        return {
            "type": "array",
            "items": _parse_datatype(listType.element_type),
            "native_data_type": str(type),
            "_nullable": nullable,
        }
    elif type.is_map_type():
        mapType: IcebergTypes.MapType = type
        kt = _parse_datatype(mapType.key_type())
        vt = _parse_datatype(mapType.value_type())
        # keys are assumed to be strings in avro map
        return {
            "type": "map",
            "values": vt,
            "native_data_type": str(mapType),
            "key_type": kt,
            "key_native_data_type": repr(mapType.key_type()),
        }
    elif type.is_struct_type():
        structType: IcebergTypes.StructType = type
        return _parse_struct_fields(structType.fields, nullable)
    else:
        # Primitive types
        return _parse_basic_datatype(type, nullable)


def _parse_struct_fields(parts: Tuple[NestedField], nullable: bool) -> Dict[str, Any]:
    fields = []
    nestedField: NestedField
    for nestedField in parts:
        field_name = nestedField.name
        field_type = _parse_datatype(nestedField.type, nestedField.is_optional)
        fields.append({"name": field_name, "type": field_type})
    return {
        "type": "record",
        "name": "__struct_{}".format(str(uuid.uuid4()).replace("-", "")),
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
        fixedType: IcebergTypes.FixedType = type
        return {
            "type": "fixed",
            "name": "name",  # TODO: Pass-in field name since it is required by Avro spec
            "size": fixedType.length,
            "native_data_type": repr(fixedType),
            "_nullable": nullable,
        }

    # Not an atomic type, so check for a logical type.
    if isinstance(type, IcebergTypes.DecimalType):
        # Also of interest: https://avro.apache.org/docs/current/spec.html#Decimal
        decimalType: IcebergTypes.DecimalType = type
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": decimalType.precision,
            "scale": decimalType.scale,
            "native_data_type": repr(decimalType),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.UUIDType):
        uuidType: IcebergTypes.UUIDType = type
        return {
            "type": "string",
            "logicalType": "uuid",
            "native_data_type": repr(uuidType),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.DateType):
        dateType: IcebergTypes.DateType = type
        return {
            "type": "int",
            "logicalType": "date",
            "native_data_type": repr(dateType),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.TimeType):
        timeType: IcebergTypes.TimeType = type
        return {
            "type": "long",
            "logicalType": "time-micros",
            "native_data_type": repr(timeType),
            "_nullable": nullable,
        }
    elif isinstance(type, IcebergTypes.TimestampType):
        timestampType: IcebergTypes.TimestampType = type
        # Avro supports 2 types of timestamp:
        #  - Timestamp: independent of a particular timezone or calendar (TZ information is lost)
        #  - Local Timestamp: represents a timestamp in a local timezone, regardless of what specific time zone is considered local
        # utcAdjustment: bool = True
        return {
            "type": "long",
            "logicalType": "timestamp-micros"
            if timestampType.adjust_to_utc
            else "local-timestamp-micros",
            "native_data_type": repr(timestampType),
            "_nullable": nullable,
        }

    return {"type": "null", "native_data_type": repr(type)}
