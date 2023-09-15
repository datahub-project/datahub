import sys

if sys.version_info < (3, 8):
    raise ImportError("Iceberg is only supported on Python 3.8+")

import json
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema, SchemaVisitorPerPrimitiveType, visit
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergSourceConfig,
    IcebergSourceReport,
)
from datahub.ingestion.source.iceberg.iceberg_profiler import IcebergProfiler
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

LOGGER = logging.getLogger(__name__)


@platform_name("Iceberg")
@support_status(SupportStatus.TESTING)
@config_class(IcebergSourceConfig)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Optionally enabled via configuration, an Iceberg instance represents the catalog name where the table is stored.",
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
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class IcebergSource(StatefulIngestionSourceBase):
    """
    ## Integration Details

    The DataHub Iceberg source plugin extracts metadata from [Iceberg tables](https://iceberg.apache.org/spec/) stored in a distributed or local file system.
    Typically, Iceberg tables are stored in a distributed file system like S3 or Azure Data Lake Storage (ADLS) and registered in a catalog.  There are various catalog
    implementations like Filesystem-based, RDBMS-based or even REST-based catalogs.  This Iceberg source plugin relies on the
    [pyiceberg library](https://py.iceberg.apache.org/).
    """

    def __init__(self, config: IcebergSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.platform: str = "iceberg"
        self.report: IcebergSourceReport = IcebergSourceReport()
        self.config: IcebergSourceConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "IcebergSource":
        config = IcebergSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _get_datasets(self, catalog: Catalog) -> Iterable[Identifier]:
        for namespace in catalog.list_namespaces():
            yield from catalog.list_tables(namespace)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            catalog = self.config.get_catalog()
        except Exception as e:
            LOGGER.error("Failed to get catalog", exc_info=True)
            self.report.report_failure(
                "get-catalog", f"Failed to get catalog {self.config.catalog.name}: {e}"
            )
            return

        for dataset_path in self._get_datasets(catalog):
            dataset_name = ".".join(dataset_path)
            if not self.config.table_pattern.allowed(dataset_name):
                # Dataset name is rejected by pattern, report as dropped.
                self.report.report_dropped(dataset_name)
                continue

            try:
                # Try to load an Iceberg table.  Might not contain one, this will be caught by NoSuchIcebergTableError.
                table = catalog.load_table(dataset_path)
                yield from self._create_iceberg_workunit(dataset_name, table)
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
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],
        )

        # Dataset properties aspect.
        custom_properties = table.metadata.properties.copy()
        custom_properties["location"] = table.metadata.location
        custom_properties["format-version"] = str(table.metadata.format_version)
        if table.current_snapshot():
            custom_properties["snapshot-id"] = str(table.current_snapshot().snapshot_id)
            custom_properties["manifest-list"] = table.current_snapshot().manifest_list
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=table.metadata.properties.get("comment", None),
            customProperties=custom_properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # Dataset ownership aspect.
        dataset_ownership = self._get_ownership_aspect(table)
        if dataset_ownership:
            dataset_snapshot.aspects.append(dataset_ownership)

        schema_metadata = self._create_schema_metadata(dataset_name, table)
        dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=dataset_name, mce=mce)

        dpi_aspect = self._get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        if self.config.is_profiling_enabled():
            profiler = IcebergProfiler(self.report, self.config.profiling)
            yield from profiler.profile_table(dataset_name, dataset_urn, table)

    def _get_ownership_aspect(self, table: Table) -> Optional[OwnershipClass]:
        owners = []
        if self.config.user_ownership_property:
            if self.config.user_ownership_property in table.metadata.properties:
                user_owner = table.metadata.properties[
                    self.config.user_ownership_property
                ]
                owners.append(
                    OwnerClass(
                        owner=make_user_urn(user_owner),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                        source=None,
                    )
                )
        if self.config.group_ownership_property:
            if self.config.group_ownership_property in table.metadata.properties:
                group_owner = table.metadata.properties[
                    self.config.group_ownership_property
                ]
                owners.append(
                    OwnerClass(
                        owner=make_group_urn(group_owner),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                        source=None,
                    )
                )
        return OwnershipClass(owners=owners) if owners else None

    def _get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

        return None

    def _create_schema_metadata(
        self, dataset_name: str, table: Table
    ) -> SchemaMetadata:
        schema_fields = self._get_schema_fields_for_schema(table.schema())
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchema(rawSchema=str(table.schema())),
            fields=schema_fields,
        )
        return schema_metadata

    def _get_schema_fields_for_schema(
        self,
        schema: Schema,
    ) -> List[SchemaField]:
        avro_schema = visit(schema, ToAvroSchemaIcebergVisitor())
        schema_fields = schema_util.avro_schema_to_mce_fields(
            json.dumps(avro_schema), default_nullable=False
        )
        return schema_fields

    def get_report(self) -> SourceReport:
        return self.report


class ToAvroSchemaIcebergVisitor(SchemaVisitorPerPrimitiveType[Dict[str, Any]]):
    """Implementation of a visitor to build an Avro schema as a dictionary from an Iceberg schema."""

    @staticmethod
    def _gen_name(prefix: str) -> str:
        return f"{prefix}{str(uuid.uuid4()).replace('-', '')}"

    def schema(self, schema: Schema, struct_result: Dict[str, Any]) -> Dict[str, Any]:
        return struct_result

    def struct(
        self, struct: StructType, field_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        nullable = True
        return {
            "type": "record",
            "name": self._gen_name("__struct_"),
            "fields": field_results,
            "native_data_type": str(struct),
            "_nullable": nullable,
        }

    def field(self, field: NestedField, field_result: Dict[str, Any]) -> Dict[str, Any]:
        field_result["_nullable"] = not field.required
        return {
            "name": field.name,
            "type": field_result,
            "doc": field.doc,
        }

    def list(
        self, list_type: ListType, element_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "type": "array",
            "items": element_result,
            "native_data_type": str(list_type),
            "_nullable": not list_type.element_required,
        }

    def map(
        self,
        map_type: MapType,
        key_result: Dict[str, Any],
        value_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        # The Iceberg Map type will be handled differently.  The idea is to translate the map
        # similar to the Map.Entry struct of Java i.e. as an array of map_entry struct, where
        # the map_entry struct has a key field and a value field. The key and value type can
        # be complex or primitive types.
        key_result["_nullable"] = False
        value_result["_nullable"] = not map_type.value_required
        map_entry = {
            "type": "record",
            "name": self._gen_name("__map_entry_"),
            "fields": [
                {
                    "name": "key",
                    "type": key_result,
                },
                {
                    "name": "value",
                    "type": value_result,
                },
            ],
        }
        return {
            "type": "array",
            "items": map_entry,
            "native_data_type": str(map_type),
        }

    def visit_fixed(self, fixed_type: FixedType) -> Dict[str, Any]:
        return {
            "type": "fixed",
            "name": self._gen_name("__fixed_"),
            "size": len(fixed_type),
            "native_data_type": str(fixed_type),
        }

    def visit_decimal(self, decimal_type: DecimalType) -> Dict[str, Any]:
        # Also of interest: https://avro.apache.org/docs/current/spec.html#Decimal
        return {
            # "type": "bytes", # when using bytes, avro drops _nullable attribute and others.  See unit test.
            "type": "fixed",  # to fix avro bug ^ resolved by using a fixed type
            "name": self._gen_name(
                "__fixed_"
            ),  # to fix avro bug ^ resolved by using a fixed type
            "size": 1,  # to fix avro bug ^ resolved by using a fixed type
            "logicalType": "decimal",
            "precision": decimal_type.precision,
            "scale": decimal_type.scale,
            "native_data_type": str(decimal_type),
        }

    def visit_boolean(self, boolean_type: BooleanType) -> Dict[str, Any]:
        return {
            "type": "boolean",
            "native_data_type": str(boolean_type),
        }

    def visit_integer(self, integer_type: IntegerType) -> Dict[str, Any]:
        return {
            "type": "int",
            "native_data_type": str(integer_type),
        }

    def visit_long(self, long_type: LongType) -> Dict[str, Any]:
        return {
            "type": "long",
            "native_data_type": str(long_type),
        }

    def visit_float(self, float_type: FloatType) -> Dict[str, Any]:
        return {
            "type": "float",
            "native_data_type": str(float_type),
        }

    def visit_double(self, double_type: DoubleType) -> Dict[str, Any]:
        return {
            "type": "double",
            "native_data_type": str(double_type),
        }

    def visit_date(self, date_type: DateType) -> Dict[str, Any]:
        return {
            "type": "int",
            "logicalType": "date",
            "native_data_type": str(date_type),
        }

    def visit_time(self, time_type: TimeType) -> Dict[str, Any]:
        return {
            "type": "long",
            "logicalType": "time-micros",
            "native_data_type": str(time_type),
        }

    def visit_timestamp(self, timestamp_type: TimestampType) -> Dict[str, Any]:
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
            "native_data_type": str(timestamp_type),
        }

    def visit_timestampz(self, timestamptz_type: TimestamptzType) -> Dict[str, Any]:
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
            "native_data_type": str(timestamptz_type),
        }

    def visit_string(self, string_type: StringType) -> Dict[str, Any]:
        return {
            "type": "string",
            "native_data_type": str(string_type),
        }

    def visit_uuid(self, uuid_type: UUIDType) -> Dict[str, Any]:
        return {
            "type": "string",
            "logicalType": "uuid",
            "native_data_type": str(uuid_type),
        }

    def visit_binary(self, binary_type: BinaryType) -> Dict[str, Any]:
        return {
            "type": "bytes",
            "native_data_type": str(binary_type),
        }
