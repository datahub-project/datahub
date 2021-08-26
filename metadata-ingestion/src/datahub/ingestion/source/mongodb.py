from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union

import bson
import pymongo
from mypy_extensions import TypedDict
from pydantic import PositiveInt
from pymongo.mongo_client import MongoClient

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
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
from datahub.metadata.schema_classes import DatasetPropertiesClass

# These are MongoDB-internal databases, which we want to skip.
# See https://docs.mongodb.com/manual/reference/local-database/ and
# https://docs.mongodb.com/manual/reference/config-database/ and
# https://stackoverflow.com/a/48273736/5004662.
DENY_DATABASE_LIST = set(["admin", "config", "local"])


class MongoDBConfig(ConfigModel):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: str = "mongodb://localhost"
    username: Optional[str] = None
    password: Optional[str] = None
    authMechanism: Optional[str] = None
    options: dict = {}
    enableSchemaInference: bool = True
    schemaSamplingSize: Optional[PositiveInt] = 1000
    useRandomSampling: bool = True
    env: str = DEFAULT_ENV

    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    collection_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class MongoDBSourceReport(SourceReport):
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
    dict: RecordTypeClass,
    "mixed": UnionTypeClass,
}


def is_nullable_doc(doc: Dict[str, Any], field_path: Tuple) -> bool:
    """
    Check if a nested field is nullable in a document from a collection.

    Parameters
    ----------
        doc:
            document to check nullability for
        field_path:
            path to nested field to check, ex. ('first_field', 'nested_child', '2nd_nested_child')
    """

    field = field_path[0]

    # if field is inside
    if field in doc:

        value = doc[field]

        if value is None:
            return True

        # if no fields left, must be non-nullable
        if len(field_path) == 1:
            return False

        # otherwise, keep checking the nested fields
        remaining_fields = field_path[1:]

        # if dictionary, check additional level of nesting
        if isinstance(value, dict):
            return is_nullable_doc(doc[field], remaining_fields)

        # if list, check if any member is missing field
        if isinstance(value, list):

            # count empty lists of nested objects as nullable
            if len(value) == 0:
                return True

            return any(is_nullable_doc(x, remaining_fields) for x in doc[field])

        # any other types to check?
        # raise ValueError("Nested type not 'list' or 'dict' encountered")
        return True

    return True


def is_nullable_collection(
    collection: Iterable[Dict[str, Any]], field_path: Tuple
) -> bool:
    """
    Check if a nested field is nullable in a collection.

    Parameters
    ----------
        collection:
            collection to check nullability for
        field_path:
            path to nested field to check, ex. ('first_field', 'nested_child', '2nd_nested_child')
    """

    return any(is_nullable_doc(doc, field_path) for doc in collection)


class BasicSchemaDescription(TypedDict):
    types: CounterType[type]  # field types and times seen
    count: int  # times the field was seen


class SchemaDescription(BasicSchemaDescription):
    delimited_name: str  # collapsed field name
    # we use 'mixed' to denote mixed types, so we need a str here
    type: Union[type, str]  # collapsed type
    nullable: bool  # if field is ever missing


def construct_schema(
    collection: Iterable[Dict[str, Any]], delimiter: str
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Construct (infer) a schema from a collection of documents.

    For each field (represented as a tuple to handle nested items), reports the following:
        - `types`: Python types of field values
        - `count`: Number of times the field was encountered
        - `type`: type of the field if `types` is just a single value, otherwise `mixed`
        - `nullable`: if field is ever null/missing
        - `delimited_name`: name of the field, joined by a given delimiter

    Parameters
    ----------
        collection:
            collection to construct schema over.
        delimiter:
            string to concatenate field names by
    """

    schema: Dict[Tuple[str, ...], BasicSchemaDescription] = {}

    def append_to_schema(doc: Dict[str, Any], parent_prefix: Tuple[str, ...]) -> None:
        """
        Recursively update the schema with a document, which may/may not contain nested fields.

        Parameters
        ----------
            doc:
                document to scan
            parent_prefix:
                prefix of fields that the document is under, pass an empty tuple when initializing
        """

        for key, value in doc.items():

            new_parent_prefix = parent_prefix + (key,)

            # if nested value, look at the types within
            if isinstance(value, dict):

                append_to_schema(value, new_parent_prefix)

            # if array of values, check what types are within
            if isinstance(value, list):

                for item in value:

                    # if dictionary, add it as a nested object
                    if isinstance(item, dict):
                        append_to_schema(item, new_parent_prefix)

            # don't record None values (counted towards nullable)
            if value is not None:

                if new_parent_prefix not in schema:

                    schema[new_parent_prefix] = {
                        "types": Counter([type(value)]),
                        "count": 1,
                    }

                else:

                    # update the type count
                    schema[new_parent_prefix]["types"].update({type(value): 1})
                    schema[new_parent_prefix]["count"] += 1

    for document in collection:
        append_to_schema(document, ())

    extended_schema: Dict[Tuple[str, ...], SchemaDescription] = {}

    for field_path in schema.keys():

        field_types = schema[field_path]["types"]

        field_type: Union[str, type] = "mixed"

        # if single type detected, mark that as the type to go with
        if len(field_types.keys()) == 1:
            field_type = next(iter(field_types))

        field_extended: SchemaDescription = {
            "types": schema[field_path]["types"],
            "count": schema[field_path]["count"],
            "nullable": is_nullable_collection(collection, field_path),
            "delimited_name": delimiter.join(field_path),
            "type": field_type,
        }

        extended_schema[field_path] = field_extended

    return extended_schema


def construct_schema_pymongo(
    collection: pymongo.collection.Collection,
    delimiter: str,
    use_random_sampling: bool,
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
        sample_size:
            number of items in the collection to sample
            (reads entire collection if not provided)
    """

    if sample_size:
        if use_random_sampling:
            # get sample documents in collection
            documents = collection.aggregate(
                [{"$sample": {"size": sample_size}}], allowDiskUse=True
            )
        else:
            documents = collection.aggregate(
                [{"$limit": sample_size}], allowDiskUse=True
            )
    else:
        # if sample_size is not provided, just take all items in the collection
        documents = collection.find({})

    return construct_schema(list(documents), delimiter)


@dataclass
class MongoDBSource(Source):
    config: MongoDBConfig
    report: MongoDBSourceReport
    mongo_client: MongoClient

    def __init__(self, ctx: PipelineContext, config: MongoDBConfig):
        super().__init__(ctx)
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

        self.mongo_client = pymongo.MongoClient(self.config.connect_uri, **options)

        # This cheaply tests the connection. For details, see
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        self.mongo_client.admin.command("ismaster")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MongoDBSource":
        config = MongoDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

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
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
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
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = "mongodb"

        database_names: List[str] = self.mongo_client.list_database_names()

        # traverse databases in sorted order so output is consistent
        for database_name in sorted(database_names):
            if database_name in DENY_DATABASE_LIST:
                continue
            if not self.config.database_pattern.allowed(database_name):
                self.report.report_dropped(database_name)
                continue

            database = self.mongo_client[database_name]
            collection_names: List[str] = database.list_collection_names()

            # traverse collections in sorted order so output is consistent
            for collection_name in sorted(collection_names):
                dataset_name = f"{database_name}.{collection_name}"

                if not self.config.collection_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                dataset_snapshot = DatasetSnapshot(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{self.config.env})",
                    aspects=[],
                )

                dataset_properties = DatasetPropertiesClass(
                    tags=[],
                    customProperties={},
                )
                dataset_snapshot.aspects.append(dataset_properties)

                if self.config.enableSchemaInference:

                    collection_schema = construct_schema_pymongo(
                        database[collection_name],
                        delimiter=".",
                        use_random_sampling=self.config.useRandomSampling,
                        sample_size=self.config.schemaSamplingSize,
                    )

                    # initialize the schema for the collection
                    canonical_schema: List[SchemaField] = []

                    # append each schema field (sort so output is consistent)
                    for schema_field in sorted(
                        collection_schema.values(), key=lambda x: x["delimited_name"]
                    ):
                        field = SchemaField(
                            fieldPath=schema_field["delimited_name"],
                            nativeDataType=self.get_pymongo_type_string(
                                schema_field["type"], dataset_name
                            ),
                            type=self.get_field_type(
                                schema_field["type"], dataset_name
                            ),
                            description=None,
                            nullable=schema_field["nullable"],
                            recursive=False,
                        )
                        canonical_schema.append(field)

                    # create schema metadata object for collection
                    schema_metadata = SchemaMetadata(
                        schemaName=collection_name,
                        platform=f"urn:li:dataPlatform:{platform}",
                        version=0,
                        hash="",
                        platformSchema=SchemalessClass(),
                        fields=canonical_schema,
                    )

                    dataset_snapshot.aspects.append(schema_metadata)

                # TODO: use list_indexes() or index_information() to get index information
                # See https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes.

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = MetadataWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> MongoDBSourceReport:
        return self.report

    def close(self):
        self.mongo_client.close()
