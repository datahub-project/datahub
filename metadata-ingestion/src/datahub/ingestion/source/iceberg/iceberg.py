import json
import logging
import threading
import uuid
from typing import Any, Dict, Iterable, List, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    ServerError,
)
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
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

LOGGER = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


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
    "Automatically ingests ownership information from table properties based on `user_ownership_property` and `group_ownership_property`",
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
        namespaces = catalog.list_namespaces()
        LOGGER.debug(
            f"Retrieved {len(namespaces)} namespaces, first 10: {namespaces[:10]}"
        )
        self.report.report_no_listed_namespaces(len(namespaces))
        tables_count = 0
        for namespace in namespaces:
            namespace_repr = ".".join(namespace)
            if not self.config.namespace_pattern.allowed(namespace_repr):
                LOGGER.info(
                    f"Namespace {namespace_repr} is not allowed by config pattern, skipping"
                )
                self.report.report_dropped(f"{namespace_repr}.*")
                continue
            try:
                tables = catalog.list_tables(namespace)
                tables_count += len(tables)
                LOGGER.debug(
                    f"Retrieved {len(tables)} tables for namespace: {namespace}, in total retrieved {tables_count}, first 10: {tables[:10]}"
                )
                self.report.report_listed_tables_for_namespace(
                    ".".join(namespace), len(tables)
                )
                yield from tables
            except NoSuchNamespaceError:
                self.report.report_warning(
                    "no-such-namespace",
                    f"Couldn't list tables for namespace {namespace} due to NoSuchNamespaceError exception",
                )
                LOGGER.warning(
                    f"NoSuchNamespaceError exception while trying to get list of tables from namespace {namespace}, skipping it",
                )
            except Exception as e:
                self.report.report_failure(
                    "listing-tables-exception",
                    f"Couldn't list tables for namespace {namespace} due to {e}",
                )
                LOGGER.exception(
                    f"Unexpected exception while trying to get list of tables for namespace {namespace}, skipping it"
                )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        thread_local = threading.local()

        def _process_dataset(dataset_path: Identifier) -> Iterable[MetadataWorkUnit]:
            LOGGER.debug(f"Processing dataset for path {dataset_path}")
            dataset_name = ".".join(dataset_path)
            if not self.config.table_pattern.allowed(dataset_name):
                # Dataset name is rejected by pattern, report as dropped.
                self.report.report_dropped(dataset_name)
                LOGGER.debug(
                    f"Skipping table {dataset_name} due to not being allowed by the config pattern"
                )
                return
            try:
                if not hasattr(thread_local, "local_catalog"):
                    LOGGER.debug(
                        f"Didn't find local_catalog in thread_local ({thread_local}), initializing new catalog"
                    )
                    thread_local.local_catalog = self.config.get_catalog()

                with PerfTimer() as timer:
                    table = thread_local.local_catalog.load_table(dataset_path)
                    time_taken = timer.elapsed_seconds()
                    self.report.report_table_load_time(
                        time_taken, dataset_name, table.metadata_location
                    )
                LOGGER.debug(f"Loaded table: {table.name()}, time taken: {time_taken}")
                yield from self._create_iceberg_workunit(dataset_name, table)
            except NoSuchPropertyException as e:
                self.report.report_warning(
                    "table-property-missing",
                    f"Failed to create workunit for {dataset_name}. {e}",
                )
                LOGGER.warning(
                    f"NoSuchPropertyException while processing table {dataset_path}, skipping it.",
                )
            except NoSuchIcebergTableError as e:
                self.report.report_warning(
                    "not-an-iceberg-table",
                    f"Failed to create workunit for {dataset_name}. {e}",
                )
                LOGGER.warning(
                    f"NoSuchIcebergTableError while processing table {dataset_path}, skipping it.",
                )
            except NoSuchTableError as e:
                self.report.report_warning(
                    "no-such-table",
                    f"Failed to create workunit for {dataset_name}. {e}",
                )
                LOGGER.warning(
                    f"NoSuchTableError while processing table {dataset_path}, skipping it.",
                )
            except FileNotFoundError as e:
                self.report.report_warning(
                    "file-not-found",
                    f"Encountered FileNotFoundError when trying to read manifest file for {dataset_name}. {e}",
                )
                LOGGER.warning(
                    f"FileNotFoundError while processing table {dataset_path}, skipping it."
                )
            except ServerError as e:
                self.report.report_warning(
                    "iceberg-rest-server-error",
                    f"Iceberg Rest Catalog returned 500 status due to an unhandled exception for {dataset_name}. Exception: {e}",
                )
                LOGGER.warning(
                    f"Iceberg Rest Catalog server error (500 status) encountered when processing table {dataset_path}, skipping it."
                )
            except Exception as e:
                self.report.report_failure(
                    "general",
                    f"Failed to create workunit for dataset {dataset_name}: {e}",
                )
                LOGGER.exception(
                    f"Exception while processing table {dataset_path}, skipping it.",
                )

        try:
            catalog = self.config.get_catalog()
        except Exception as e:
            self.report.report_failure("get-catalog", f"Failed to get catalog: {e}")
            return

        for wu in ThreadedIteratorExecutor.process(
            worker_func=_process_dataset,
            args_list=[(dataset_path,) for dataset_path in self._get_datasets(catalog)],
            max_workers=self.config.processing_threads,
        ):
            yield wu

    def _create_iceberg_workunit(
        self, dataset_name: str, table: Table
    ) -> Iterable[MetadataWorkUnit]:
        with PerfTimer() as timer:
            self.report.report_table_scanned(dataset_name)
            LOGGER.debug(f"Processing table {dataset_name}")
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
            custom_properties["partition-spec"] = str(self._get_partition_aspect(table))
            if table.current_snapshot():
                custom_properties["snapshot-id"] = str(
                    table.current_snapshot().snapshot_id
                )
                custom_properties["manifest-list"] = (
                    table.current_snapshot().manifest_list
                )
            dataset_properties = DatasetPropertiesClass(
                name=table.name()[-1],
                description=table.metadata.properties.get("comment", None),
                customProperties=custom_properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)
            # Dataset ownership aspect.
            dataset_ownership = self._get_ownership_aspect(table)
            if dataset_ownership:
                LOGGER.debug(
                    f"Adding ownership: {dataset_ownership} to the dataset {dataset_name}"
                )
                dataset_snapshot.aspects.append(dataset_ownership)

            schema_metadata = self._create_schema_metadata(dataset_name, table)
            dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        self.report.report_table_processing_time(
            timer.elapsed_seconds(), dataset_name, table.metadata_location
        )
        yield MetadataWorkUnit(id=dataset_name, mce=mce)

        dpi_aspect = self._get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        if self.config.is_profiling_enabled():
            profiler = IcebergProfiler(self.report, self.config.profiling)
            yield from profiler.profile_table(dataset_name, dataset_urn, table)

    def _get_partition_aspect(self, table: Table) -> Optional[str]:
        """Extracts partition information from the provided table and returns a JSON array representing the [partition spec](https://iceberg.apache.org/spec/?#partition-specs) of the table.
        Each element of the returned array represents a field in the [partition spec](https://iceberg.apache.org/spec/?#partition-specs) that follows [Appendix-C](https://iceberg.apache.org/spec/?#appendix-c-json-serialization) of the Iceberg specification.
        Extra information has been added to this spec to make the information more user-friendly.

        Since Datahub does not have a place in its model to store this information, it is saved as a JSON string and displayed as a table property.

        Here is an example:
        ```json
        "partition-spec": "[{\"name\": \"timeperiod_loaded\", \"transform\": \"identity\", \"source\": \"timeperiod_loaded\", \"source-id\": 19, \"source-type\": \"date\", \"field-id\": 1000}]",
        ```

        Args:
            table (Table): The Iceberg table to extract partition spec from.

        Returns:
            str: JSON representation of the partition spec of the provided table (empty array if table is not partitioned) or `None` if an error occured.
        """
        try:
            return json.dumps(
                [
                    {
                        "name": partition.name,
                        "transform": str(partition.transform),
                        "source": str(
                            table.schema().find_column_name(partition.source_id)
                        ),
                        "source-id": partition.source_id,
                        "source-type": str(
                            table.schema().find_type(partition.source_id)
                        ),
                        "field-id": partition.field_id,
                    }
                    for partition in table.spec().fields
                ]
            )
        except Exception as e:
            self.report.report_warning(
                "extract-partition",
                f"Failed to extract partition spec from Iceberg table {table.name()} due to error: {str(e)}",
            )
            return None

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

    # visit_timestamptz() is required when using pyiceberg >= 0.5.0, which is essentially a duplicate
    # of visit_timestampz().  The function has been renamed from visit_timestampz().
    # Once Datahub can upgrade its pyiceberg dependency to >=0.5.0, the visit_timestampz() function can be safely removed.
    def visit_timestamptz(self, timestamptz_type: TimestamptzType) -> Dict[str, Any]:
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
