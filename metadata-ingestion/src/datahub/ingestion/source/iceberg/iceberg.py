import json
import logging
import threading
import uuid
from functools import partial
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dateutil import parser as dateutil_parser
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
from pyiceberg.typedef import Identifier, Properties
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
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import NamespaceKey
from datahub.ingestion.api.auto_work_units.auto_dataset_properties_aspect import (
    auto_patch_last_modified,
)
from datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size import (
    EnsureAspectSizeProcessor,
)
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
from datahub.ingestion.api.source_helpers import (
    AutoSystemMetadata,
    auto_fix_duplicate_schema_field_paths,
    auto_fix_empty_field_paths,
    auto_lowercase_urns,
    auto_materialize_referenced_tags_terms,
    auto_workunit_reporter,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
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
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TimeStampClass,
    _Aspect,
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

    platform: str = "iceberg"

    def __init__(self, config: IcebergSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.report: IcebergSourceReport = IcebergSourceReport()
        self.config: IcebergSourceConfig = config
        self.ctx: PipelineContext = ctx

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "IcebergSource":
        config = IcebergSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # This source needs to overwrite standard `get_workunit_processor`, because it is unique in terms of usage
        # of parallelism. Because of this, 2 processors won't work as expected:
        # 1. browse_path_processor - it needs aspects for a single entity to be continuous - which is not guaranteed
        #    in this source
        # 2. automatic stamping with systemMetadata - in current implementation of the Source class this processor
        #    would have been applied in a thread (single) shared between the source, processors and transformers.
        #    Since the metadata scraping happens in separate threads, this could lead to difference between
        #    time used by systemMetadata and actual time at which metadata was read
        auto_lowercase_dataset_urns: Optional[MetadataWorkUnitProcessor] = None
        if (
            self.ctx.pipeline_config
            and self.ctx.pipeline_config.source
            and self.ctx.pipeline_config.source.config
            and (
                (
                    hasattr(
                        self.ctx.pipeline_config.source.config,
                        "convert_urns_to_lowercase",
                    )
                    and self.ctx.pipeline_config.source.config.convert_urns_to_lowercase
                )
                or (
                    hasattr(self.ctx.pipeline_config.source.config, "get")
                    and self.ctx.pipeline_config.source.config.get(
                        "convert_urns_to_lowercase"
                    )
                )
            )
        ):
            auto_lowercase_dataset_urns = auto_lowercase_urns

        return [
            auto_lowercase_dataset_urns,
            auto_materialize_referenced_tags_terms,
            partial(
                auto_fix_duplicate_schema_field_paths, platform=self._infer_platform()
            ),
            partial(auto_fix_empty_field_paths, platform=self._infer_platform()),
            partial(auto_workunit_reporter, self.get_report()),
            auto_patch_last_modified,
            EnsureAspectSizeProcessor(self.get_report()).ensure_aspect_size,
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _get_namespaces(self, catalog: Catalog) -> Iterable[Identifier]:
        namespaces = catalog.list_namespaces()
        LOGGER.debug(
            f"Retrieved {len(namespaces)} namespaces, first 10: {namespaces[:10]}"
        )
        self.report.report_no_listed_namespaces(len(namespaces))
        for namespace in namespaces:
            namespace_repr = ".".join(namespace)
            if not self.config.namespace_pattern.allowed(namespace_repr):
                LOGGER.info(
                    f"Namespace {namespace_repr} is not allowed by config pattern, skipping"
                )
                self.report.report_dropped(f"{namespace_repr}.*")
                continue
            yield namespace

    def _get_datasets(
        self, catalog: Catalog, namespaces: Iterable[Tuple[Identifier, str]]
    ) -> Iterable[Tuple[Identifier, str]]:
        LOGGER.debug("Starting to retrieve tables")
        tables_count = 0
        for namespace, namespace_urn in namespaces:
            try:
                tables = catalog.list_tables(namespace)
                tables_count += len(tables)
                LOGGER.debug(
                    f"Retrieved {len(tables)} tables for namespace: {namespace}, in total retrieved {tables_count}, first 10: {tables[:10]}"
                )
                self.report.report_listed_tables_for_namespace(
                    ".".join(namespace), len(tables)
                )
                yield from [(table, namespace_urn) for table in tables]
            except NoSuchNamespaceError as e:
                self.report.warning(
                    title="No such namespace",
                    message="Skipping the missing namespace.",
                    context=str(namespace),
                    exc=e,
                )
            except Exception as e:
                self.report.report_failure(
                    title="Error when processing a namespace",
                    message="Skipping the namespace due to errors while processing it.",
                    context=str(namespace),
                    exc=e,
                )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        thread_local = threading.local()

        def _try_processing_dataset(
            dataset_path: Tuple[str, ...], dataset_name: str, namespace_urn: str
        ) -> Iterable[MetadataWorkUnit]:
            try:
                if not hasattr(thread_local, "local_catalog"):
                    LOGGER.debug(
                        f"Didn't find local_catalog in thread_local ({thread_local}), initializing new catalog"
                    )
                    thread_local.local_catalog = self.config.get_catalog()

                if not hasattr(thread_local, "stamping_processor"):
                    LOGGER.debug(
                        f"Didn't find stamping_processor in thread_local ({thread_local}), initializing new workunit processor"
                    )
                    thread_local.stamping_processor = AutoSystemMetadata(self.ctx)

                with PerfTimer() as timer:
                    table = thread_local.local_catalog.load_table(dataset_path)
                    time_taken = timer.elapsed_seconds()
                    self.report.report_table_load_time(
                        time_taken, dataset_name, table.metadata_location
                    )
                LOGGER.debug(f"Loaded table: {table.name()}, time taken: {time_taken}")
                dataset_urn: str = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                for aspect in self._create_iceberg_table_aspects(
                    dataset_name, table, namespace_urn
                ):
                    yield thread_local.stamping_processor.stamp_wu(
                        MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn, aspect=aspect
                        ).as_workunit()
                    )
            except NoSuchPropertyException as e:
                self.report.warning(
                    title="Unable to process table",
                    message="Table was not processed due to expected property missing (table is probably not an iceberg table).",
                    context=dataset_name,
                    exc=e,
                )
            except NoSuchIcebergTableError as e:
                self.report.warning(
                    title="Skipped non-iceberg table",
                    message="Table was recognized as non-iceberg and skipped.",
                    context=dataset_name,
                    exc=e,
                )
            except NoSuchTableError as e:
                self.report.warning(
                    title="Table not found",
                    message="Table was returned by the catalog in the list of table but catalog can't find its details, table was skipped.",
                    context=dataset_name,
                    exc=e,
                )
            except FileNotFoundError as e:
                self.report.warning(
                    title="Manifest file not found",
                    message="Couldn't find manifest file to read for the table, skipping it.",
                    context=dataset_name,
                    exc=e,
                )
            except ServerError as e:
                self.report.warning(
                    title="Iceberg REST Server Error",
                    message="Iceberg returned 500 HTTP status when trying to process a table, skipping it.",
                    context=dataset_name,
                    exc=e,
                )
            except ValueError as e:
                if "Could not initialize FileIO" not in str(e):
                    raise
                self.report.warning(
                    title="Could not initialize FileIO",
                    message="Could not initialize FileIO for a table (are you using custom FileIO?). Skipping the table.",
                    context=dataset_name,
                    exc=e,
                )

        def _process_dataset(
            dataset_path: Identifier, namespace_urn: str
        ) -> Iterable[MetadataWorkUnit]:
            try:
                LOGGER.debug(f"Processing dataset for path {dataset_path}")
                dataset_name = ".".join(dataset_path)
                if not self.config.table_pattern.allowed(dataset_name):
                    # Dataset name is rejected by pattern, report as dropped.
                    self.report.report_dropped(dataset_name)
                    LOGGER.debug(
                        f"Skipping table {dataset_name} due to not being allowed by the config pattern"
                    )
                    return

                yield from _try_processing_dataset(
                    dataset_path, dataset_name, namespace_urn
                )
            except Exception as e:
                self.report.report_failure(
                    title="Error when processing a table",
                    message="Skipping the table due to errors when processing it.",
                    context=str(dataset_path),
                    exc=e,
                )

        try:
            catalog = self.config.get_catalog()
        except Exception as e:
            self.report.report_failure(
                title="Failed to initialize catalog object",
                message="Couldn't start the ingestion due to failure to initialize catalog object.",
                exc=e,
            )
            return

        try:
            stamping_processor = AutoSystemMetadata(self.ctx)
            namespace_ids = self._get_namespaces(catalog)
            namespaces: List[Tuple[Identifier, str]] = []
            for namespace in namespace_ids:
                namespace_repr = ".".join(namespace)
                LOGGER.debug(f"Processing namespace {namespace_repr}")
                namespace_urn = make_container_urn(
                    NamespaceKey(
                        namespace=namespace_repr,
                        platform=self.platform,
                        instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                )
                namespace_properties: Properties = catalog.load_namespace_properties(
                    namespace
                )
                namespaces.append((namespace, namespace_urn))
                for aspect in self._create_iceberg_namespace_aspects(
                    namespace, namespace_properties
                ):
                    yield stamping_processor.stamp_wu(
                        MetadataChangeProposalWrapper(
                            entityUrn=namespace_urn, aspect=aspect
                        ).as_workunit()
                    )
            LOGGER.debug("Namespaces ingestion completed")
        except Exception as e:
            self.report.report_failure(
                title="Failed to list namespaces",
                message="Couldn't start the ingestion due to a failure to process the list of the namespaces",
                exc=e,
            )
            return

        for wu in ThreadedIteratorExecutor.process(
            worker_func=_process_dataset,
            args_list=[
                (dataset_path, namespace_urn)
                for dataset_path, namespace_urn in self._get_datasets(
                    catalog, namespaces
                )
            ],
            max_workers=self.config.processing_threads,
        ):
            yield wu

    def _create_iceberg_table_aspects(
        self, dataset_name: str, table: Table, namespace_urn: str
    ) -> Iterable[_Aspect]:
        with PerfTimer() as timer:
            self.report.report_table_scanned(dataset_name)
            LOGGER.debug(f"Processing table {dataset_name}")
            yield Status(removed=False)
            yield SubTypes(typeNames=[DatasetSubTypes.TABLE])

            yield self._get_dataset_properties_aspect(dataset_name, table)

            dataset_ownership = self._get_ownership_aspect(table)
            if dataset_ownership:
                LOGGER.debug(
                    f"Adding ownership: {dataset_ownership} to the dataset {dataset_name}"
                )
                yield dataset_ownership

            yield self._create_schema_metadata(dataset_name, table)
            dpi = self._get_dataplatform_instance_aspect()
            yield dpi
            yield self._create_browse_paths_aspect(dpi.instance, str(namespace_urn))
            yield ContainerClass(container=str(namespace_urn))

        self.report.report_table_processing_time(
            timer.elapsed_seconds(), dataset_name, table.metadata_location
        )

        if self.config.is_profiling_enabled():
            profiler = IcebergProfiler(self.report, self.config.profiling)
            yield from profiler.profile_table(dataset_name, table)

    def _create_browse_paths_aspect(
        self,
        platform_instance_urn: Optional[str] = None,
        container_urn: Optional[str] = None,
    ) -> BrowsePathsV2Class:
        path = []
        if platform_instance_urn:
            path.append(
                BrowsePathEntryClass(
                    id=platform_instance_urn, urn=platform_instance_urn
                )
            )
        if container_urn:
            path.append(BrowsePathEntryClass(id=container_urn, urn=container_urn))
        return BrowsePathsV2Class(path=path)

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
            self.report.warning(
                title="Failed to extract partition information",
                message="Failed to extract partition information for a table. Table metadata will be ingested without it.",
                context=str(table.name),
                exc=e,
            )
            return None

    def _get_dataset_properties_aspect(
        self, dataset_name: str, table: Table
    ) -> DatasetPropertiesClass:
        created: Optional[TimeStampClass] = None
        custom_properties = table.metadata.properties.copy()
        custom_properties["location"] = table.metadata.location
        custom_properties["format-version"] = str(table.metadata.format_version)
        custom_properties["partition-spec"] = str(self._get_partition_aspect(table))
        last_modified: Optional[int] = table.metadata.last_updated_ms
        if table.current_snapshot():
            custom_properties["snapshot-id"] = str(table.current_snapshot().snapshot_id)
            custom_properties["manifest-list"] = table.current_snapshot().manifest_list
            if not last_modified:
                last_modified = int(table.current_snapshot().timestamp_ms)
        if "created-at" in custom_properties:
            try:
                dt = dateutil_parser.isoparse(custom_properties["created-at"])
                created = TimeStampClass(int(dt.timestamp() * 1000))
            except Exception as ex:
                LOGGER.warning(
                    f"Exception while trying to parse creation date {custom_properties['created-at']}, ignoring: {ex}"
                )

        return DatasetPropertiesClass(
            name=table.name()[-1],
            description=table.metadata.properties.get("comment", None),
            customProperties=custom_properties,
            lastModified=TimeStampClass(last_modified)
            if last_modified is not None
            else None,
            created=created,
            qualifiedName=dataset_name,
        )

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

    def _get_dataplatform_instance_aspect(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            )
            if self.config.platform_instance
            else None,
        )

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

    def _create_iceberg_namespace_aspects(
        self, namespace: Identifier, properties: Properties
    ) -> Iterable[_Aspect]:
        namespace_repr = ".".join(namespace)
        custom_properties: Dict[str, str] = {}
        for k, v in properties.items():
            try:
                custom_properties[str(k)] = str(v)
            except Exception as e:
                LOGGER.warning(
                    f"Exception when trying to parse namespace properties for {namespace_repr}. Exception: {e}"
                )
        yield Status(removed=False)
        yield ContainerProperties(
            name=namespace_repr,
            qualifiedName=namespace_repr,
            env=self.config.env,
            customProperties=custom_properties,
        )
        yield SubTypes(typeNames=[DatasetContainerSubTypes.NAMESPACE])
        dpi = self._get_dataplatform_instance_aspect()
        yield dpi
        yield self._create_browse_paths_aspect(dpi.instance)


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
