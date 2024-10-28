import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, ValuesView

import bson.timestamp
import pymongo.collection
from packaging import version
from pydantic import PositiveInt, validator
from pydantic.fields import Field
from pymongo.mongo_client import MongoClient

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
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn

logger = logging.getLogger(__name__)

# These are MongoDB-internal databases, which we want to skip.
# See https://docs.mongodb.com/manual/reference/local-database/ and
# https://docs.mongodb.com/manual/reference/config-database/ and
# https://stackoverflow.com/a/48273736/5004662.
DENY_DATABASE_LIST = {"admin", "config", "local"}


class HostingEnvironment(Enum):
    SELF_HOSTED = "SELF_HOSTED"
    ATLAS = "ATLAS"
    AWS_DOCUMENTDB = "AWS_DOCUMENTDB"


class MongoDBConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: str = Field(
        default="mongodb://localhost", description="MongoDB connection URI."
    )
    username: Optional[str] = Field(default=None, description="MongoDB username.")
    password: Optional[str] = Field(default=None, description="MongoDB password.")
    authMechanism: Optional[str] = Field(
        default=None, description="MongoDB authentication mechanism."
    )
    options: dict = Field(
        default={}, description="Additional options to pass to `pymongo.MongoClient()`."
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
    # mongodb only supports 16MB as max size for documents. However, if we try to retrieve a larger document it
    # errors out with "16793600" as the maximum size supported.
    maxDocumentSize: Optional[PositiveInt] = Field(default=16793600, description="")

    hostingEnvironment: Optional[HostingEnvironment] = Field(
        default=HostingEnvironment.SELF_HOSTED,
        description="Hosting environment of MongoDB, default is SELF_HOSTED, currently support `SELF_HOSTED`, `ATLAS`, `AWS_DOCUMENTDB`",
    )

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for databases to filter in ingestion.",
    )
    collection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for collections to filter in ingestion.",
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("maxDocumentSize")
    def check_max_doc_size_filter_is_valid(cls, doc_size_filter_value):
        if doc_size_filter_value > 16793600:
            raise ValueError("maxDocumentSize must be a positive value <= 16793600.")
        return doc_size_filter_value


@dataclass
class MongoDBSourceReport(StaleEntityRemovalSourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map PyMongo types to canonical MongoDB strings
PYMONGO_TYPE_TO_MONGO_TYPE = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",
    bool: "boolean",
    int: "integer",
    bson.int64.Int64: "biginteger",
    float: "float",
    str: "string",
    bson.datetime.datetime: "date",
    bson.timestamp.Timestamp: "timestamp",
    bson.dbref.DBRef: "dbref",
    bson.objectid.ObjectId: "oid",
    bson.Decimal128: "numberDecimal",
    "mixed": "mixed",
}

# map PyMongo types to DataHub classes
_field_type_mapping: Dict[Union[Type, str], Type] = {
    list: ArrayTypeClass,
    bool: BooleanTypeClass,
    type(None): NullTypeClass,
    int: NumberTypeClass,
    bson.int64.Int64: NumberTypeClass,
    float: NumberTypeClass,
    str: StringTypeClass,
    bson.datetime.datetime: TimeTypeClass,
    bson.timestamp.Timestamp: TimeTypeClass,
    bson.dbref.DBRef: BytesTypeClass,
    bson.objectid.ObjectId: BytesTypeClass,
    bson.Decimal128: NumberTypeClass,
    dict: RecordTypeClass,
    "mixed": UnionTypeClass,
}


def construct_schema_pymongo(
    collection: pymongo.collection.Collection,
    delimiter: str,
    use_random_sampling: bool,
    max_document_size: int,
    should_add_document_size_filter: bool,
    sample_size: Optional[int] = None,
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Calls construct_schema on a PyMongo collection.

    Returned schema is keyed by tuples of nested field names, with each
    value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.

    Parameters
    ----------
        collection:
            the PyMongo collection
        delimiter:
            string to concatenate field names by
        use_random_sampling:
            boolean to indicate if random sampling should be added to aggregation
        max_document_size:
            maximum size of the document that will be considered for generating the schema.
        should_add_document_size_filter:
            boolean to indicate if document size filter should be added to aggregation
        sample_size:
            number of items in the collection to sample
            (reads entire collection if not provided)
    """

    aggregations: List[Dict] = []
    if should_add_document_size_filter:
        doc_size_field = "temporary_doc_size_field"
        # create a temporary field to store the size of the document. filter on it and then remove it.
        aggregations = [
            {"$addFields": {doc_size_field: {"$bsonSize": "$$ROOT"}}},
            {"$match": {doc_size_field: {"$lt": max_document_size}}},
            {"$project": {doc_size_field: 0}},
        ]
    if use_random_sampling:
        # get sample documents in collection
        if sample_size:
            aggregations.append({"$sample": {"size": sample_size}})
        documents = collection.aggregate(
            aggregations,
            allowDiskUse=True,
        )
    else:
        if sample_size:
            aggregations.append({"$limit": sample_size})
        documents = collection.aggregate(aggregations, allowDiskUse=True)

    return construct_schema(list(documents), delimiter)


@platform_name("MongoDB")
@config_class(MongoDBConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@dataclass
class MongoDBSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Databases and associated metadata
    - Collections in each database and schemas for each collection (via schema inference)

    By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
    Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

    Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

    """

    config: MongoDBConfig
    report: MongoDBSourceReport
    mongo_client: MongoClient
    platform: str = "mongodb"

    def __init__(self, ctx: PipelineContext, config: MongoDBConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = MongoDBSourceReport()

        options = {}
        if self.config.username is not None:
            options["username"] = self.config.username
        if self.config.password is not None:
            options["password"] = self.config.password
        if self.config.authMechanism is not None:
            options["authMechanism"] = self.config.authMechanism
        options = {
            **options,
            **self.config.options,
        }

        # See https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes
        self.mongo_client = MongoClient(
            self.config.connect_uri, datetime_conversion="DATETIME_AUTO", **options  # type: ignore
        )

        # This cheaply tests the connection. For details, see
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        self.mongo_client.admin.command("ping")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MongoDBSource":
        config = MongoDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_pymongo_type_string(
        self, field_type: Union[Type, str], collection_name: str
    ) -> str:
        """
        Return Mongo type string from a Python type

        Parameters
        ----------
            field_type:
                type of a Python object
            collection_name:
                name of collection (for logging)
        """
        try:
            type_string = PYMONGO_TYPE_TO_MONGO_TYPE[field_type]
        except KeyError:
            self.report.warning(
                message="Unrecognized column types found",
                context=f"Collection: {collection_name}, field type {field_type}",
            )
            PYMONGO_TYPE_TO_MONGO_TYPE[field_type] = "unknown"
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
                type of a Python object
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
        database_names: List[str] = self.mongo_client.list_database_names()

        # traverse databases in sorted order so output is consistent
        for database_name in sorted(database_names):
            if database_name in DENY_DATABASE_LIST:
                continue
            if not self.config.database_pattern.allowed(database_name):
                self.report.report_dropped(database_name)
                continue

            database = self.mongo_client[database_name]
            database_key = DatabaseKey(
                database=database_name,
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            )
            yield from gen_containers(
                container_key=database_key,
                name=database_name,
                sub_types=[DatasetContainerSubTypes.DATABASE],
            )

            collection_names: List[str] = database.list_collection_names()
            # traverse collections in sorted order so output is consistent
            for collection_name in sorted(collection_names):
                dataset_name = f"{database_name}.{collection_name}"

                if not self.config.collection_pattern.allowed(dataset_name):
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
                    name=collection_name,
                    tags=[],
                    customProperties={},
                )

                schema_metadata: Optional[SchemaMetadata] = None
                if self.config.enableSchemaInference:
                    schema_metadata = self._infer_schema_metadata(
                        collection=database[collection_name],
                        dataset_urn=dataset_urn,
                        dataset_properties=dataset_properties,
                    )

                # TODO: use list_indexes() or index_information() to get index information
                # See https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes.

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
        collection: pymongo.collection.Collection,
        dataset_urn: DatasetUrn,
        dataset_properties: DatasetPropertiesClass,
    ) -> SchemaMetadata:
        assert self.config.maxDocumentSize is not None
        collection_schema = construct_schema_pymongo(
            collection,
            delimiter=".",
            use_random_sampling=self.config.useRandomSampling,
            max_document_size=self.config.maxDocumentSize,
            should_add_document_size_filter=self.should_add_document_size_filter(),
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
            schemaName=collection.name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=canonical_schema,
        )

    def is_server_version_gte_4_4(self) -> bool:
        try:
            server_version = self.mongo_client.server_info().get("versionArray")
            if server_version:
                logger.info(
                    f"Mongodb version for current connection - {server_version}"
                )
                server_version_str_list = [str(i) for i in server_version]
                required_version = "4.4"
                return version.parse(
                    ".".join(server_version_str_list)
                ) >= version.parse(required_version)
        except Exception as e:
            logger.error("Error while getting version of the mongodb server %s", e)

        return False

    def is_hosted_on_aws_documentdb(self) -> bool:
        return self.config.hostingEnvironment == HostingEnvironment.AWS_DOCUMENTDB

    def should_add_document_size_filter(self) -> bool:
        # the operation $bsonsize is only available in server version greater than 4.4
        # and is not supported by AWS DocumentDB, we should only add this operation to
        # aggregation for mongodb that doesn't run on AWS DocumentDB and version is greater than 4.4
        # https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html
        return (
            self.is_server_version_gte_4_4() and not self.is_hosted_on_aws_documentdb()
        )

    def get_report(self) -> MongoDBSourceReport:
        return self.report

    def close(self):
        self.mongo_client.close()
        super().close()
