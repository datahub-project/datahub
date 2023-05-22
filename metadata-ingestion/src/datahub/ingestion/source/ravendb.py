import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union, ValuesView

from ravendb import DocumentStore
from ravendb import GetDatabaseNamesOperation, GetCollectionStatisticsOperation
from datahub.ingestion.source.schema_inference.object import construct_schema

from pydantic import PositiveInt
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import make_data_platform_urn
from datahub.metadata.schema_classes import DataPlatformInstanceClass, DataPlatformInstancePropertiesClass
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema_inference.object import (
    SchemaDescription,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
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
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mce_builder import DEFAULT_ENV

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DENY_COLLECTION_LIST = set(["@hilo"])

# map RavenDB types to DataHub classes ??
_field_type_mapping: Dict[str, Type] = {
    "list": ArrayTypeClass,
    "bool": BooleanTypeClass,
    "None": NullTypeClass,
    "int": NumberTypeClass,
    "int64": NumberTypeClass,
    "float": NumberTypeClass,
    "str": StringTypeClass,
    "datetime.datetime": TimeTypeClass,
    "timestamp.Timestamp": TimeTypeClass,
    "dict": RecordTypeClass,
    "mixed": UnionTypeClass,
}


class UniversalEntity:
    """
    Universal class for querying RavenDB entries.
    If an object has an EntityType (captured in @metadata attribute), only the attributes defined 
    in the EntityType are returned and updated.  If this universal class is included in the query, 
    all attributes of the document are returned.
    """

    def __init__(self, *args, **kwargs):
        self.data = dict(*args, **kwargs)


class RavenDBConfig(EnvConfigMixin):

    connect_uri: str = Field(
        default=None, description="RavenDB connection URI.", required=True
    )
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    collection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire collection name in allowed databases.",
    )
    # schema inference
    enable_schema_inference: bool = Field(
        default=True, description="Whether to infer a schema."
    )
    schema_sampling_size: Optional[PositiveInt] = Field(
        default=1000,
        description="Number of documents to use when inferring schema size. If set to `0`, all documents will be scanned.",
    )
    remove_metadata_from_schema: bool = Field(
        default=True, description="Whether to remove @metadata field from schema."
    )
    max_schema_size: Optional[PositiveInt] = Field(
        default=300, description="Maximum number of fields to include in the schema."
    )

    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )

    # TODO add more config values like username/password, etc.


@dataclass
class RavenDBSourceReport(SourceReport):
    """
    Source reporter to report statistics, warnings, failures, and other information about the run. 
    """
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# 3. Implement the source itself
# The core for the source is the get_workunits method, which produces a stream of metadata events (typically MCP objects) wrapped up in a MetadataWorkUnit.
# The MetadataChangeEventClass is defined in the metadata models which are generated under metadata-ingestion/src/datahub/metadata/schema_classes.py.
# There are also some convenience methods for commonly used operations.
@platform_name("RavenDB", id="ravendb")
@config_class(RavenDBConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Schema is infered if config parameter 'enable_schema_inference' is true (Default: true).")
@dataclass
class RavenDBSource(Source):
    """
    This plugin extracts the following:

    - Databases and associated metadata
    - Collections in each database and schemas for each collection (via schema inference)

    By default, schema inference samples 1000 documents from each collection. 
    Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.
    """

    store: DocumentStore
    config: RavenDBConfig
    report: RavenDBSourceReport
    database_names: list[str]
    platform: str = "ravendb"

    def __init__(self, ctx: PipelineContext, config: RavenDBConfig):
        super().__init__(ctx)

        self.config = config

        # setup the RavenDB client
        logging.debug(f"Initialize store: {self.config.connect_uri}")
        self.store = DocumentStore(self.config.connect_uri)
        # get all databases and collections and filter them out, like in pymongo, l318
        self.store.initialize()

        self.report = RavenDBSourceReport()

        # ping the database to make sure the connection works
        self.database_names = [d for d in self.get_database_names_pagination()]
        logging.debug(
            f"Found {len(self.database_names)} databases matching allowed regex: {self.database_names}")

    def get_database_names_pagination(self, start=0, page_size=10):
        """
        Get all database names that are stored on the server.
        As the GetDatabaseNamesOperation returns only a page of database names, this function needs to be 
        called multiple times to get all database names.
         Parameters
        ----------
            start:
                start index of returned database names (default: 0)
            page_size:
                number of database names returned per page (default: 10)

        """
        result = []
        while True:
            operation = GetDatabaseNamesOperation(
                start=start, page_size=page_size)
            database_names = self.store.maintenance.server.send(
                operation=operation)
            result.extend(database_names)
            if start + page_size > len(database_names):
                return result
            start += page_size

    # helper methods to construct schema
    def construct_schema_ravendb(
            self,
            db_store,
            collection,
            sampling_size=1000,
            remove_metadata=True,
            ignore_entity_types=True) -> Dict[Tuple[str, ...], SchemaDescription]:
        """
        Create a schema from the RavenDB collection
        Returned schema is keyed by tuples of nested field names, with each
        value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.
        Parameters
        ----------
            collection:
                the RavenDB collection
            db_store:
                the RavenDB DocumentStore to open a session
            collection:
                the name of the collection
            sampling_size:
                number of items in the collection to sample (default: 1000)
            remove_metadata:
                whether or not to remove the '@metadata' field from the collection (default: True)
            ignore_entity_types:
                whether or not to get all columns no matter if they are mentioned in regarding EntityClass (default: True)
        Returns
        -------
            dict with infered schema

        """

        logging.debug(
            f"Conduct schema from collection: {collection}.\nIgnoring entity types: {ignore_entity_types}\nSampling size: {sampling_size}")
        items = []
        with db_store.open_session() as session:
            if ignore_entity_types:
                # get all columns no matter if they are mentioned in EntityClass
                result = session.query_collection(
                    collection, UniversalEntity).take(sampling_size)
                items = [item.__dict__["data"] for item in result.distinct()]
            else:
                result = session.query_collection(
                    collection).take(sampling_size)
                items = [item.__dict__ for item in result.distinct()]

            # Remove all keys that start with @
            if remove_metadata:
                items = [{k: v for k, v in item.items() if not k == "@metadata"}
                         for item in items]
            # key is tuple
            schema = {k[0]: v for k, v in construct_schema(items, ".").items()}
            # logging.debug(schema)
            return schema

    def get_additional_information(self, db_store):
        """
        Returns additional information about the given DocumentStore
        Parameters
        ----------
            db_store:
                the RavenDB DocumentStore to open a session
        Returns
        -------
            dict with statistics of DocumentStore
        """
        from ravendb.documents.operations.statistics import GetStatisticsOperation
        with db_store.open_session():
            stats = db_store.maintenance.send(GetStatisticsOperation())

            return {
                "nodeTag": stats["@metadata"]["NodeTag"],
                "indexes": stats["Indexes"],
                "SizeOnDisk": stats["SizeOnDisk"]["SizeInBytes"],
                "tempBuffersSizeOnDisk": stats["TempBuffersSizeOnDisk"]["SizeInBytes"],
                "sizeOnDisk": stats["SizeOnDisk"]["SizeInBytes"],
                "databaseChangeVector": stats["DatabaseChangeVector"],
                "lastIndexingTime": stats["LastIndexingTime"],
                "lastDocEtag": stats["LastDocEtag"],
                "lastDatabaseEtag": stats["LastDatabaseEtag"]
            }

    def get_datatypes(self, types: Dict, collection_name: str):
        """
        Determines the native datatype of a field and the mapped schema type
        Parameters
        ----------
            types:
                Counter with types of field values
            collection_name:
                name of collection (for logging)
        Returns
        -------
            Tuple of native and mapped datahub specific schema field data types 
        """

        # if "types" is dict with more than 1 entry, the datatype is mixed
        if len(types.items()) == 1:
            native_datatype = list(types.keys())[0].__name__
        else:
            native_datatype = "mixed"
        TypeClass: Optional[Type] = _field_type_mapping.get(native_datatype)
        if TypeClass is None:
            self.report.report_warning(
                collection_name, f"Type: Unable to map type {types} to metadata schema"
            )
            TypeClass = NullTypeClass

        # TODO if there are several different datatypes for a field:
        # how to determine the datahub datatype?
        return native_datatype, SchemaFieldDataType(type=TypeClass())

    def drop_collection(self, collection):
        """
        Determines whether the collection should be dropped because of the configured collection naming patterns
        Parameters
        ----------
            collection:
                Name of the collection to be checked
        Returns
        -------
            boolean whether the collection should be dropped 
        """
        return not self.config.collection_pattern.allowed(collection)

    def make_dataplatform_instance_urn(self, platform: str, instance: str) -> str:
        if instance.startswith("urn:li:dataPlatformInstance"):
            return instance
        else:
            return f"urn:li:dataPlatformInstance:({make_data_platform_urn(platform)},{instance})"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = RavenDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_report(self) -> RavenDBSourceReport:
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        # filter out not required dbs
        for database in sorted(self.database_names):
            if not self.config.database_pattern.allowed(database):
                self.report.report_dropped(database)
                logging.debug(f"Dropping database {database}")
                continue
            logging.debug(f"Working on database: {database}")
            db_store = DocumentStore(self.config.connect_uri, database)
            db_store.initialize()

            # further information (GB, indices, etc)
            db_stats = self.get_additional_information(db_store)
            db_stats["db_name"] = database
            # logging.debug(db_stats)

            collections_dict = {k for k in db_store.maintenance.send(
                GetCollectionStatisticsOperation()).collections.keys()}
            logging.debug(
                f"Found {len(collections_dict)} collections: {collections_dict}")

            # last index time of all indices of collection
            latest_time = ""
            try:
                latest_time = str(
                    max([i["lastIndexingTime"] for i in db_stats["indexes"]]))
            except:
                logging.debug(
                    f"Document Store '{database}': No indices available")
            platform_urn = make_data_platform_urn(self.platform)
            instance_urn = f"urn:li:dataPlatformInstance:({platform_urn},{database})"
            # saving additional custom ravendb database attributes as DataPlatformInstanceProperties
            platform_props = DataPlatformInstancePropertiesClass(
                name=database,
                externalUrl=self.config.connect_uri,
                customProperties={
                    "nodeTag": str(db_stats["nodeTag"]),
                    "indexes": str(db_stats["indexes"]),
                    "tempBuffersSizeOnDisk": str(db_stats["tempBuffersSizeOnDisk"]),
                    "sizeOnDisk": str(db_stats["sizeOnDisk"]),
                    "databaseChangeVector": str(db_stats["databaseChangeVector"]),
                    "lastCollectionIndexingTime": latest_time,
                    "lastDocEtag": str(db_stats["lastDocEtag"]),
                    "lastDatabaseEtag": str(db_stats["lastDatabaseEtag"]),
                }
            )

            # whole DocuementStore should be captured as a platform instance
            # even if there is no collections, the DocuementStore is still in DataHub
            mcp = MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=platform_props
            )
            wu = MetadataWorkUnit(
                id=f"{self.platform}.{database}.{mcp.aspectName}", mcp=mcp)
            self.report.report_workunit(wu)
            yield wu

            for collection in sorted(collections_dict):
                logging.debug(f"Processing collection {collection}")
                # TODO only collection or like this?
                dataset_name = f"{database}.{collection}"
                # filter out not required collections
                if collection in DENY_COLLECTION_LIST:
                    continue
                if self.drop_collection(collection):
                    self.report.report_dropped(dataset_name)
                    logging.debug(f"Dropping collection {collection}")
                    continue

                logging.debug(f"Working on collection: {collection}")
                dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})"

                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[],
                )
                # add db stats information to each dataset
                platform_instance = DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=instance_urn,
                )
                dataset_snapshot.aspects.append(platform_instance)

                # filter for index from collection
                collection_indexes = [
                    {"name": i["Name"], "stale": i["IsStale"],
                        "lastIndexingTime": i["LastIndexingTime"]}
                    for i in db_stats["indexes"]
                    if collection in i["Name"].split("/")
                ]
                # last index time of all indices of collection
                latest_collection_indexing_time = ""
                try:
                    latest_time = str(
                        max([i["lastIndexingTime"] for i in db_stats["indexes"]]))
                except:
                    logging.debug(
                        f"Collection '{collection}': No indices available")

                dataset_properties = DatasetPropertiesClass(
                    qualifiedName=dataset_name,
                    tags=[],
                    customProperties={
                        "indexes": str(collection_indexes),
                        "lastCollectionIndexingTime": latest_collection_indexing_time,
                    }
                )
                dataset_snapshot.aspects.append(dataset_properties)

                # conduct schema
                if self.config.enable_schema_inference:
                    collection_schema = self.construct_schema_ravendb(
                        db_store, collection,
                        sampling_size=self.config.schema_sampling_size,
                        remove_metadata=self.config.remove_metadata_from_schema)

                    # initialize the schema for the collection
                    canonical_schema: List[SchemaField] = []
                    max_schema_size = self.config.max_schema_size
                    collection_schema_size = len(collection_schema.values())
                    collection_fields: Union[
                        List[SchemaDescription], ValuesView[SchemaDescription]
                    ] = collection_schema.values()
                    assert max_schema_size is not None
                    if collection_schema_size > max_schema_size:
                        # downsample the schema, using frequency as the sort key
                        self.report.report_warning(
                            key=dataset_urn,
                            reason=f"Downsampling the collection schema of '{collection}' because it has {collection_schema_size} fields. Threshold is {max_schema_size}",
                        )
                        collection_fields = sorted(
                            collection_schema.values(),
                            key=lambda x: x["count"],
                            reverse=True,
                        )[0:max_schema_size]
                        # Add this information to the custom properties so user can know they are looking at downsampled schema
                        dataset_properties.customProperties[
                            "schema.downsampled"
                        ] = "True"
                        dataset_properties.customProperties[
                            "schema.totalFields"
                        ] = f"{collection_schema_size}"

                    # TODO downsampling schema
                    collection_fields = sorted(
                        collection_schema.values(),
                        key=lambda x: x["count"],
                        reverse=True,
                    )
                    logging.debug(
                        f"Collection '{collection}': Size of collection fields = {len(collection_fields)}"
                    )

                    # append each schema field (sort so output is consistent)
                    for schema_field in sorted(
                        collection_fields, key=lambda x: x["delimited_name"]
                    ):
                        native_datatype, mapped_type = self.get_datatypes(
                            dict(schema_field['types']), collection)
                        field = SchemaField(
                            fieldPath=schema_field["delimited_name"],
                            nativeDataType=native_datatype,
                            type=mapped_type,
                            description=None,
                            nullable=schema_field["nullable"],
                            recursive=False
                        )
                        canonical_schema.append(field)

                    # create schema metadata object for collection
                    # ravendb doesn't keep creation date on it's own
                    # every time the document is updated
                    schema_metadata = SchemaMetadata(
                        schemaName=database,
                        platform=f"urn:li:dataPlatform:{self.platform}",
                        platformSchema=SchemalessClass(),
                        fields=canonical_schema,
                        dataset=dataset_urn,
                        version=0,  # TODO db_stats["databaseChangeVector"],
                        hash="",
                    )
                    dataset_snapshot.aspects.append(schema_metadata)

                    mce = MetadataChangeEvent(
                        proposedSnapshot=dataset_snapshot)
                    wu = MetadataWorkUnit(id=dataset_name, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu
