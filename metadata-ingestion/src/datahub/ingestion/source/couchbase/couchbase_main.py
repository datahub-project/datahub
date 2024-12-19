import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, ValuesView

import bson.timestamp
from datetime import timedelta
from packaging import version
from pydantic import PositiveInt, validator
from pydantic.fields import Field
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, ClusterOptions, TLSVerifyMode
from couchbase.bucket import Bucket
from couchbase.collection import Collection
from couchbase.management.buckets import BucketManager, BucketSettings
from couchbase.management.collections import CollectionManager, ScopeSpec
from couchbase.cluster import Cluster

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    add_dataset_to_container,
    gen_containers,
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
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.schema_inference.object import (
    SchemaDescription,
    construct_schema,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
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
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.urns import DatasetUrn

logger = logging.getLogger(__name__)


class HostingEnvironment(Enum):
    SELF_HOSTED = "SELF_HOSTED"
    CAPELLA = "CAPELLA"


class CouchbaseDBConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    connect_string: str = Field(
        default="couchbases://localhost", description="Couchbase connection string."
    )
    username: Optional[str] = Field(default=None, description="Couchbase username.")
    password: Optional[str] = Field(default=None, description="Couchbase password.")
    kv_timeout: Optional[PositiveInt] = Field(default=5, description="KV timeout.")
    query_timeout: Optional[PositiveInt] = Field(default=60, description="Query timeout.")
    options: dict = Field(
        default={}, description="Additional options to pass to `ClusterOptions()`."
    )
    enableSchemaInference: bool = Field(
        default=True, description="Whether to infer schemas. "
    )
    schemaSamplingSize: Optional[PositiveInt] = Field(
        default=1000,
        description="Number of documents to use when inferring schema size. If set to `null`, all documents will be scanned.",
    )
    useRandomSampling: bool = Field(
        default=True,
        description="If documents for schema inference should be randomly selected. If `False`, documents will be selected from start.",
    )
    maxSchemaSize: Optional[PositiveInt] = Field(
        default=300, description="Maximum number of fields to include in the schema."
    )
    maxDocumentSize: Optional[PositiveInt] = Field(default=20971520, description="")

    hostingEnvironment: Optional[HostingEnvironment] = Field(
        default=HostingEnvironment.SELF_HOSTED,
        description="Hosting environment for Couchbase, default is SELF_HOSTED, currently supported is `SELF_HOSTED` or `CAPELLA`",
    )

    bucket_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for buckets to filter in ingestion.",
    )
    scope_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for scopes to filter in ingestion.",
    )
    keyspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for keyspace to filter in ingestion.",
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("maxDocumentSize")
    def check_max_doc_size_filter_is_valid(cls, doc_size_filter_value):
        if doc_size_filter_value > 20971520:
            raise ValueError("maxDocumentSize must be a positive value <= 20971520.")
        return doc_size_filter_value


@dataclass
class CouchbaseDBSourceReport(StaleEntityRemovalSourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map Python types to canonical strings
PYTHON_TYPE_TO_DATA_TYPE = {
    list: "array",
    dict: "object",
    type(None): "null",
    bool: "boolean",
    int: "integer",
    float: "float",
    str: "string",
    "mixed": "mixed",
}

# map Python types to DataHub classes
_field_type_mapping: Dict[Union[Type, str], Type] = {
    list: ArrayTypeClass,
    bool: BooleanTypeClass,
    type(None): NullTypeClass,
    int: NumberTypeClass,
    float: NumberTypeClass,
    str: StringTypeClass,
    dict: RecordTypeClass,
    "mixed": UnionTypeClass,
}


def construct_schema_couchbase(
    cluster: Cluster,
    keyspace: str,
    delimiter: str,
    use_random_sampling: bool,
    sample_size: Optional[int] = None,
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Calls construct_schema on a Couchbase collection.

    Returned schema is keyed by tuples of nested field names, with each
    value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.

    Parameters
    ----------
        cluster:
            Couchbase Cluster object
        keyspace:
            the Couchbase keyspace
        delimiter:
            string to concatenate field names by
        use_random_sampling:
            boolean to indicate if random sampling should be used
        sample_size:
            number of items in the collection to sample
            (reads entire collection if not provided)
    """

    document_id_list = []
    query = f"select meta().id from {keyspace}"
    if sample_size:
        query += f" limit {sample_size}"
    result = cluster.query(query)
    for item in result:
        document_id_list.append(item.get('id'))

    if use_random_sampling:
        pass

    keyspace_vector: List[str] = keyspace.split(".")
    if len(keyspace_vector) != 3:
        raise ValueError(
            f"Invalid keyspace format: {keyspace}. Expected format is <bucket>.<scope>.<collection>"
        )

    bucket_name = keyspace_vector[0]
    scope_name = keyspace_vector[1]
    collection_name = keyspace_vector[2]

    bucket: Bucket = cluster.bucket(bucket_name)
    collection: Collection = bucket.scope(scope_name).collection(collection_name)

    documents = []
    for document_id in document_id_list:
        document = collection.get(document_id)
        documents.append(document.content_as[dict])

    return construct_schema(documents, delimiter)


@platform_name("Couchbase")
@config_class(CouchbaseDBConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@dataclass
class CouchbaseDBSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Databases and associated metadata
    - Collections in each database and schemas for each collection (via schema inference)

    By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
    Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

    Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

    """

    config: CouchbaseDBConfig
    report: CouchbaseDBSourceReport
    couchbase_cluster: Cluster
    platform: str = "couchbase"

    def __init__(self, ctx: PipelineContext, config: CouchbaseDBConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = CouchbaseDBSourceReport()

        query_timeout = float(self.config.query_timeout)
        kv_timeout = float(self.config.kv_timeout)
        timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=query_timeout),
                                         kv_timeout=timedelta(seconds=kv_timeout),
                                         bootstrap_timeout=timedelta(seconds=kv_timeout * 2),
                                         resolve_timeout=timedelta(seconds=kv_timeout),
                                         connect_timeout=timedelta(seconds=kv_timeout),
                                         management_timeout=timedelta(seconds=kv_timeout * 2))

        cluster_options = ClusterOptions(PasswordAuthenticator(self.config.username, self.config.password),
                                         timeout_options=timeouts,
                                         tls_verify=TLSVerifyMode.NO_VERIFY,
                                         **self.config.options)

        self.couchbase_cluster = Cluster.connect(
            self.config.connect_string, cluster_options
        )

        self.couchbase_cluster.wait_until_ready(timedelta(seconds=10))

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

    def get_python_type_string(
        self, field_type: Union[Type, str], collection_name: str
    ) -> str:
        """
        Return Mongo type string from a Python type

        Parameters
        ----------
            field_type:
                type of Python object
            collection_name:
                name of collection (for logging)
        """
        try:
            type_string = PYTHON_TYPE_TO_DATA_TYPE[field_type]
        except KeyError:
            self.report.warning(
                message="Unrecognized column types found",
                context=f"Collection: {collection_name}, field type {field_type}",
            )
            PYTHON_TYPE_TO_DATA_TYPE[field_type] = "unknown"
            type_string = "unknown"

        return type_string

    def get_field_type(
        self, field_type: Union[Type, str], collection_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in PyMongo to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of Python object
            collection_name:
                name of collection (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.warning(
                message="Unrecognized column type found",
                context=f"Collection: {collection_name}, field type {field_type}",
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        bucket_manager: BucketManager = self.couchbase_cluster.buckets()
        buckets: List[BucketSettings] = bucket_manager.get_all_buckets()
        bucket_names: List[str] = [b.name for b in buckets]

        # traverse databases in sorted order so output is consistent
        for bucket_name in sorted(bucket_names):
            if not self.config.bucket_pattern.allowed(bucket_name):
                self.report.report_dropped(bucket_name)
                continue

            bucket: Bucket = self.couchbase_cluster.bucket(bucket_name)
            database_key = DatabaseKey(
                database=bucket_name,
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            )
            yield from gen_containers(
                container_key=database_key,
                name=bucket_name,
                sub_types=[DatasetContainerSubTypes.DATABASE],
            )

            collection_manager: CollectionManager = bucket.collections()
            scopes: Iterable[ScopeSpec] = collection_manager.get_all_scopes()
            for scope in scopes:
                if not self.config.scope_pattern.allowed(scope.name):
                    self.report.report_dropped(scope.name)
                    continue
                collection_names: List[str] = [c.name for c in scope.collections]
                # traverse collections in sorted order so output is consistent
                for collection_name in sorted(collection_names):
                    dataset_name = f"{bucket_name}.{scope.name}.{collection_name}"

                    if not self.config.keyspace_pattern.allowed(dataset_name):
                        self.report.report_dropped(dataset_name)
                        continue

                    dataset_urn = DatasetUrn.create_from_ids(
                        platform_id=self.platform,
                        table_name=dataset_name,
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )

                    # Initialize data_platform_instance with a default value
                    data_platform_instance = None
                    if self.config.platform_instance:
                        data_platform_instance = DataPlatformInstanceClass(
                            platform=make_data_platform_urn(self.platform),
                            instance=make_dataplatform_instance_urn(
                                self.platform, self.config.platform_instance
                            ),
                        )

                    dataset_properties = DatasetPropertiesClass(
                        name=dataset_name,
                        tags=[],
                        customProperties={},
                    )

                    schema_metadata: Optional[SchemaMetadata] = None
                    if self.config.enableSchemaInference:
                        schema_metadata = self._infer_schema_metadata(
                            keyspace=dataset_name,
                            dataset_urn=dataset_urn,
                            dataset_properties=dataset_properties,
                        )

                    yield from add_dataset_to_container(database_key, dataset_urn.urn())
                    yield from [
                        mcp.as_workunit()
                        for mcp in MetadataChangeProposalWrapper.construct_many(
                            entityUrn=dataset_urn.urn(),
                            aspects=[
                                schema_metadata,
                                dataset_properties,
                                data_platform_instance,
                            ],
                        )
                    ]

    def _infer_schema_metadata(
        self,
        keyspace: str,
        dataset_urn: DatasetUrn,
        dataset_properties: DatasetPropertiesClass,
    ) -> SchemaMetadata:
        assert self.config.maxDocumentSize is not None
        collection_schema = construct_schema_couchbase(
            self.couchbase_cluster,
            keyspace,
            delimiter=".",
            use_random_sampling=self.config.useRandomSampling,
            sample_size=self.config.schemaSamplingSize,
        )

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
                nativeDataType=self.get_pymongo_type_string(
                    schema_field["type"], dataset_urn.name
                ),
                type=self.get_field_type(schema_field["type"], dataset_urn.name),
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

    def close(self):
        self.couchbase_cluster.close()
        super().close()
