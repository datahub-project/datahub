import json
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

from iceberg.api import types as IcebergTypes
from iceberg.api.table import Table
from iceberg.api.types.types import NestedField
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
        self.iceberg_client = config.filesystem_tables

    @classmethod
    def create(cls, config_dict, ctx):
        config = IcebergSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for dataset_path, dataset_name in self.config.get_paths():
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
                pass
            except Exception as e:
                self.report.report_failure("general", f"Failed to create workunit: {e}")
                LOGGER.exception(
                    f"Exception while processing table {dataset_path}, skipping it.",
                    e,
                )

    def _create_iceberg_workunit(
        self, dataset_name: str, table: Table
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(dataset_name)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )

        custom_properties = dict(table.properties())
        custom_properties["location"] = table.location()
        # A table might not have any snapshots, i.e. no data.
        if len(table.snapshots()) > 0:
            custom_properties["snapshot-id"] = str(table.current_snapshot().snapshot_id)
            custom_properties[
                "manifest-list"
            ] = table.current_snapshot().manifest_location
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=table.properties().get("comment", None),
            customProperties=custom_properties,
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
        if len(owners) > 0:
            # TODO: Consider using builder if I can provide OwnershipTypeClass:
            # return make_ownership_aspect_from_urn_list([make_user_urn(user_owner)], OwnershipTypeClass.TECHNICAL_OWNER)
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

    def _create_schema_metadata(
        self, dataset_name: str, table: Table
    ) -> SchemaMetadata:
        schema_fields = self._get_schema_fields(table.schema().columns())

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=SchemalessClass(),  # TODO: Not sure what to use here...
            fields=schema_fields,
        )
        return schema_metadata

    def _get_schema_fields(self, columns: Tuple) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            fields = self._get_schema_fields_for_column(column)
            canonical_schema.extend(fields)
        return canonical_schema

    def _get_schema_fields_for_column(
        self,
        column: NestedField,
    ) -> List[SchemaField]:
        field_type: IcebergTypes.Type = column.type
        if field_type.is_primitive_type():
            avro_schema = self._get_avro_schema_from_data_type(column)
            newfields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=column.is_optional
            )
            assert len(newfields) == 1
            return newfields
        elif field_type.is_nested_type():
            # Get avro schema for subfields along with parent complex field
            avro_schema = self._get_avro_schema_from_data_type(column)
            newfields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=column.is_optional
            )
            return newfields
        else:
            raise ValueError(f"Invalid Iceberg field type: {field_type}")

    def _get_avro_schema_from_data_type(self, column: NestedField) -> Dict[str, Any]:
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
        list_type: IcebergTypes.ListType = type
        return {
            "type": "array",
            "items": _parse_datatype(list_type.element_type),
            "native_data_type": str(type),
            "_nullable": nullable,
        }
    elif type.is_map_type():
        map_type: IcebergTypes.MapType = type
        kt = _parse_datatype(map_type.key_type())
        vt = _parse_datatype(map_type.value_type())
        # keys are assumed to be strings in avro map
        return {
            "type": "map",
            "values": vt,
            "native_data_type": str(map_type),
            "key_type": kt,
            "key_native_data_type": repr(map_type.key_type()),
        }
    elif type.is_struct_type():
        structType: IcebergTypes.StructType = type
        return _parse_struct_fields(structType.fields, nullable)
    else:
        # Primitive types
        return _parse_basic_datatype(type, nullable)


def _parse_struct_fields(parts: Tuple[NestedField], nullable: bool) -> Dict[str, Any]:
    fields = []
    nested_field: NestedField
    for nested_field in parts:
        field_name = nested_field.name
        field_type = _parse_datatype(nested_field.type, nested_field.is_optional)
        fields.append({"name": field_name, "type": field_type, "doc": nested_field.doc})
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
        fixed_type: IcebergTypes.FixedType = type
        return {
            "type": "fixed",
            "name": "name",  # TODO: Pass-in field name since it is required by Avro spec
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
            "name": "bogus",  # to fix avro bug ^ resolved by using a fixed type
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
            # See bug report: https://issues.apache.org/jira/browse/AVRO-3476
            # "logicalType": "timestamp-micros"
            # if timestamp_type.adjust_to_utc
            # else "local-timestamp-micros",
            "native_data_type": repr(timestamp_type),
            "_nullable": nullable,
        }

    return {"type": "null", "native_data_type": repr(type)}
