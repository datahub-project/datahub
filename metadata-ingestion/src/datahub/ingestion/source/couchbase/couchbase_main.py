import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Type, Union, ValuesView

from couchbase.cluster import Cluster
from couchbase.management.buckets import BucketSettings
from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect
from datahub.ingestion.source.couchbase.couchbase_kv_schema import construct_schema
from datahub.ingestion.source.couchbase.couchbase_schema_reader import CouchbaseCollectionItemsReader
from datahub.ingestion.source.couchbase.couchbase_profiling import CouchbaseProfiler
from datahub.ingestion.source.couchbase.couchbase_common import (
    CouchbaseDBConfig,
    CouchbaseDBSourceReport,
)
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema_inference.object import (
    SchemaDescription,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadataClass as SchemaMetadata,
    StringTypeClass,
    UnionTypeClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    classification_workunit_processor,
)
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
)

logger = logging.getLogger(__name__)


# map Python types to DataHub classes
_field_type_mapping: Dict[str, Type] = {
    "array": ArrayTypeClass,
    "boolean": BooleanTypeClass,
    "null": NullTypeClass,
    "number": NumberTypeClass,
    "string": StringTypeClass,
    "object": RecordTypeClass,
    "mixed": UnionTypeClass,
}


class KeyspaceKey(ContainerKey):
    keyspace: str


@platform_name("Couchbase")
@config_class(CouchbaseDBConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "The platform_instance is derived from the configured cluster name")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(
    SourceCapability.CLASSIFICATION,
    "Optionally enabled via `classification.enabled`",
    supported=True,
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration `profiling.enabled`",
)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@dataclass
class CouchbaseDBSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Buckets (databases), scopes and collections
    - Schemas for each collection (via schema inference)

    The plugin will sample 10,000 documents by default. Use the setting `schema_sample_size` to define how many documents will be sampled per collection.

    """

    config: CouchbaseDBConfig
    report: CouchbaseDBSourceReport
    couchbase_cluster: Cluster
    platform: str = "couchbase"

    def __init__(self, ctx: PipelineContext, config: CouchbaseDBConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = CouchbaseDBSourceReport()
        self.classification_handler = ClassificationHandler(self.config, self.report)

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )

        query_timeout = float(self.config.query_timeout)
        kv_timeout = float(self.config.kv_timeout)

        self.couchbase_connect = CouchbaseConnect(self.config.connect_string, self.config.username, self.config.password, kv_timeout, query_timeout)
        self.couchbase_connect.cluster_init()
        self.couchbase_cluster = self.couchbase_connect.connect()

        self.profiler = CouchbaseProfiler(self.config, self.report, self.couchbase_connect)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "CouchbaseDBSource":
        config = CouchbaseDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_field_type(
        self, field_type: Union[Type, str], collection_name: str
    ) -> SchemaFieldDataType:
        type_class: Optional[Type] = _field_type_mapping.get(field_type)

        if type_class is None:
            self.report.warning(
                message="Unrecognized column type found",
                context=f"Collection: {collection_name}, field type {field_type}",
            )
            type_class = NullTypeClass

        return SchemaFieldDataType(type=type_class())

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        keyspaces: List[str] = []
        bucket_names: List[str] = self.couchbase_connect.bucket_list()

        for bucket_name in sorted(bucket_names):
            bucket_settings: BucketSettings = self.couchbase_connect.bucket_info(bucket_name)
            scope_names: List[str] = self.couchbase_connect.scope_list(bucket_name)

            for scope_name in sorted(scope_names):
                if scope_name == "_system":
                    continue
                collection_names: List[str] = self.couchbase_connect.collection_list(bucket_name, scope_name)

                for collection_name in sorted(collection_names):
                    dataset_name = f"{bucket_name}.{scope_name}.{collection_name}"
                    if not self.config.keyspace_pattern.allowed(dataset_name):
                        self.report.report_dropped(dataset_name)
                        continue

                    collection_count: int = self.couchbase_connect.collection_count(dataset_name)
                    if collection_count == 0:
                        self.report.report_dropped(dataset_name)
                        continue

                    yield from self._generate_keyspace_container(dataset_name, bucket_settings)

                    value_sample_size: int = self.config.classification.sample_size
                    schema_sample_size: int = self.config.schema_sample_size
                    schema: dict = self.couchbase_connect.collection_infer(schema_sample_size, value_sample_size, dataset_name)
                    keyspaces.append(dataset_name)

                    table_wu_generator = self.process_keyspace(dataset_name, schema, collection_count)

                    data_reader = CouchbaseCollectionItemsReader.create(schema)

                    yield from classification_workunit_processor(
                        table_wu_generator,
                        self.classification_handler,
                        data_reader,
                        [bucket_name, scope_name, collection_name],
                    )

        # Profiling
        if self.config.is_profiling_enabled():
            yield from self.profiler.get_workunits(keyspaces)

    def process_keyspace(self, dataset_name: str, schema: dict, doc_count: int) -> Iterable[MetadataWorkUnit]:
        platform_instance = self.config.cluster_name

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=platform_instance,
            name=dataset_name,
        )
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            customProperties={
                "collection.totalItems": str(doc_count),
            },
        )

        schema_metadata = self.construct_schema_metadata(
            keyspace=dataset_name,
            schema=schema,
            dataset_urn=dataset_urn,
            dataset_properties=dataset_properties,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
        )

        yield from add_dataset_to_container(
            container_key=self._generate_keyspace_container_key(dataset_name),
            dataset_urn=dataset_urn,
        )

        platform_instance_aspect = DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=make_dataplatform_instance_urn(self.platform, platform_instance),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=platform_instance_aspect,
        ).as_workunit()

    def construct_schema_metadata(
        self,
        keyspace: str,
        schema: dict,
        dataset_urn: str,
        dataset_properties: DatasetPropertiesClass,
    ) -> SchemaMetadata:
        collection_schema = construct_schema(schema)

        # initialize the schema for the collection
        canonical_schema: List[SchemaField] = []
        max_schema_size = self.config.maxSchemaSize
        collection_schema_size = len(collection_schema.values())
        collection_fields: Union[
            List[SchemaDescription], ValuesView[SchemaDescription]
        ] = collection_schema.values()
        assert max_schema_size is not None
        if collection_schema_size > max_schema_size:
            # downsample the schema, using frequency as the sort key
            self.report.report_warning(
                title="Too many schema fields",
                message=f"Downsampling the collection schema because it has too many schema fields. Configured threshold is {max_schema_size}",
                context=f"Schema Size: {collection_schema_size}, Collection: {dataset_urn}",
            )
            # Add this information to the custom properties so user can know they are looking at downsampled schema
            dataset_properties.customProperties["schema.downsampled"] = "True"
            dataset_properties.customProperties[
                "schema.totalFields"
            ] = f"{collection_schema_size}"

        logger.debug(f"Size of collection fields = {len(collection_fields)}")
        # append each schema field (sort so output is consistent)
        for schema_field in sorted(
            collection_fields,
            key=lambda x: (
                -x["count"],
                x["delimited_name"],
            ),  # Negate `count` for descending order, `delimited_name` stays the same for ascending
        )[0:max_schema_size]:
            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=schema_field["type"],
                type=self.get_field_type(schema_field["type"], dataset_urn),
                description=None,
                nullable=schema_field["nullable"],
                recursive=False,
            )
            canonical_schema.append(field)

        # create schema metadata object for collection
        return SchemaMetadata(
            schemaName=keyspace,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=canonical_schema,
        )

    def get_report(self) -> CouchbaseDBSourceReport:
        return self.report

    def _get_domain_wu(
            self, dataset_name: str, entity_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )
                break

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )

    def _generate_keyspace_container(
            self,
            keyspace: str,
            bucket_settings: BucketSettings,
    ) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._generate_keyspace_container_key(keyspace),
            name=keyspace,
            qualified_name=keyspace,
            extra_properties={
                "bucket_type": str(bucket_settings.bucket_type.name.lower()),
                "bucket_storage_backend": str(bucket_settings.storage_backend.name.lower()),
                "bucket_quota": str(bucket_settings.get('ram_quota_mb')),
                "bucket_max_expiry": str(bucket_settings.max_expiry.seconds),
                "bucket_num_replicas": str(bucket_settings.get('num_replicas')),
            },
            sub_types=[DatasetContainerSubTypes.KEYSPACE],
        )

    def _generate_keyspace_container_key(self, keyspace: str) -> ContainerKey:
        return KeyspaceKey(
            keyspace=keyspace,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def close(self):
        self.couchbase_cluster.close()
        super().close()
