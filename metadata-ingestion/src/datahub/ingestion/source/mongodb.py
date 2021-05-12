import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import bson
import pymongo
from ete3 import Tree
from past.builtins import basestring
from pymongo.mongo_client import MongoClient

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass

# These are MongoDB-internal databases, which we want to skip.
# See https://docs.mongodb.com/manual/reference/local-database/ and
# https://docs.mongodb.com/manual/reference/config-database/ and
# https://stackoverflow.com/a/48273736/5004662.
DENY_DATABASE_LIST = set(["admin", "config", "local"])

logger = logging.getLogger(__name__)


class MongoDBConfig(ConfigModel):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: str = "mongodb://localhost"
    username: Optional[str] = None
    password: Optional[str] = None
    authMechanism: Optional[str] = None
    options: dict = {}

    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    collection_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class MongoDBSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


logger = logging.getLogger(__name__)

###
# Mapping from pymongo_type to type_string

PYMONGO_TYPE_TO_TYPE_STRING = {
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
}


def get_type_string(value: Any) -> str:
    """
    Return Mongo type string from a value

    Parameters
    ----------
        value: value to get type of
    """
    value_type = type(value)
    try:
        type_string = PYMONGO_TYPE_TO_TYPE_STRING[value_type]
    except KeyError:
        logger.warning(
            "Pymongo type %s is not mapped to a type_string. "
            "We define it as 'unknown' for current schema extraction",
            value_type,
        )
        PYMONGO_TYPE_TO_TYPE_STRING[value_type] = "unknown"
        type_string = "unknown"

    return type_string


###
# Define and use type_string_tree,
# to get the least common parent type_string from a list of type_string

NEWICK_TYPES_STRING_TREE = """
(
    (
        (
            float,
            ((boolean) integer) biginteger
        ) number,
        (
            oid,
            dbref
        ) string,
        date,
        timestamp,
        unknown
    ) general_scalar,
    OBJECT
) mixed_scalar_object
;"""

TYPES_STRING_TREE = Tree(NEWICK_TYPES_STRING_TREE, format=8)


def common_parent_type(list_of_type_string: List[str]) -> str:
    """
    Get the common parent type from a list of types.

    Parameters
    ----------
        list_of_type_string: list of Mongo type strings
    """
    if not list_of_type_string:
        return "null"
    # avoid duplicates as get_common_ancestor('integer', 'integer') -> 'number'
    list_of_type_string = list(set(list_of_type_string))
    if len(list_of_type_string) == 1:
        return list_of_type_string[0]
    return TYPES_STRING_TREE.get_common_ancestor(*list_of_type_string).name


def extract_collection_schema(
    pymongo_collection: pymongo.collection.Collection, sample_size=0
):
    """
    Iterate through all documents of a collection to create its schema:
        - Init collection schema
        - Add every document from MongoDB collection to the schema
        - Post-process schema

    Parameters
    ----------
        pymongo_collection: pymongo collection to iterate over
        sample_size: number of samples to check (if 0, checks all)
    """
    collection_schema = {"count": 0, "object": init_empty_object_schema()}

    n = pymongo_collection.count()
    collection_schema["count"] = n
    if sample_size:
        documents = pymongo_collection.aggregate(
            [{"$sample": {"size": sample_size}}], allowDiskUse=True
        )
    else:
        documents = pymongo_collection.find({})
    scan_count = sample_size or n
    for i, document in enumerate(documents, start=1):
        add_document_to_object_schema(document, collection_schema["object"])
        if i % 10 ** 5 == 0 or i == scan_count:
            logger.info(
                "   scanned %s documents out of %s (%.2f %%)",
                i,
                scan_count,
                (100.0 * i) / scan_count,
            )

    post_process_schema(collection_schema)
    collection_schema = recursive_default_to_regular_dict(collection_schema)
    return collection_schema


def recursive_default_to_regular_dict(value: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    If value is a dictionary, recursively replace defaultdict to regular dict.

    Note that defaultdict are instances of dict.

    Parameters
    ----------
        value: input defaultdict
    """
    if isinstance(value, dict):
        return {k: recursive_default_to_regular_dict(v) for k, v in value.items()}
    else:
        return value


def post_process_schema(object_count_schema: dict[str, Any]) -> Dict[str, Any]:
    """
    Clean and add information to schema once it has been built:
        - compute the main type for each field
        - compute the proportion of non null values in the parent object
        - recursively postprocess nested object schemas

    Parameters
    ----------
        object_count_schema: dict
    """
    object_count = object_count_schema["count"]
    object_schema = object_count_schema["object"]
    for field_schema in object_schema.values():

        summarize_types(field_schema)
        field_schema["prop_in_object"] = round(
            (field_schema["count"]) / float(object_count), 4
        )
        if "object" in field_schema:
            post_process_schema(field_schema)


def summarize_types(field_schema):
    """
    Summarize types information to one 'type' field.

    Add a 'type' field, compatible with all encountered types in 'types_count'.
    This is done by taking the least common parent type between types.
    If 'ARRAY' type count is not null, the main type is 'ARRAY'.
    An 'array_type' is defined, as the least common parent type between 'types' and 'array_types'

    Parameters
    ----------
        field_schema: input schema
    """

    type_list = list(field_schema["types_count"])
    # Only if 'ARRAY' in 'types_count':
    type_list += list(field_schema.get("array_types_count", {}))

    cleaned_type_list = [
        type_name for type_name in type_list if type_name not in ["ARRAY", "null"]
    ]

    common_type = common_parent_type(cleaned_type_list)

    if "ARRAY" in field_schema["types_count"]:
        field_schema["type"] = "ARRAY"
        field_schema["array_type"] = common_type
    else:
        field_schema["type"] = common_type


def init_empty_object_schema() -> defaultdict:
    """
    Generate an empty object schema.

    We use a defaultdict of empty fields schema. This avoid to test for the presence of fields.
    """

    def empty_field_schema():
        return {
            "types_count": defaultdict(int),
            "count": 0,
        }

    empty_object = defaultdict(
        empty_field_schema
    )  # type: defaultdict[str, dict[str, Any]]
    return empty_object


def add_document_to_object_schema(document: Dict[Any, Any], object_schema) -> None:
    """
    Add all fields of a document to a local object_schema.

    Parameters
    ----------
        document: Mongo document
        object_schema: object schema to update
    """
    for doc_field, doc_value in document.items():
        add_value_to_field_schema(doc_value, object_schema[doc_field])


def add_value_to_field_schema(value: Any, field_schema: Dict[str, Any]) -> None:
    """
    Add a value to a field_schema:
        - Update count or 'null_count' count.
        - Define or check the type of value.
        - Recursively add 'list' and 'dict' value to the schema.

    Parameters
    ----------
        value: value corresponding to a field in a MongoDB object
        field_schema: field schema to update
    """
    field_schema["count"] += 1
    add_value_type(value, field_schema)
    add_potential_list_to_field_schema(value, field_schema)
    add_potential_document_to_field_schema(value, field_schema)


def add_potential_document_to_field_schema(
    document: Any, field_schema: Dict[str, Any]
) -> None:
    """
    Add a document to a field_schema, exiting if document is not a dict.

    Parameters
    ----------
        document: input document
        field_schema: field schema to update
    """
    if isinstance(document, dict):
        if "object" not in field_schema:
            field_schema["object"] = init_empty_object_schema()
        add_document_to_object_schema(document, field_schema["object"])


def add_potential_list_to_field_schema(
    value_list: List[str], field_schema: Dict[str, Any]
) -> None:
    """
    Add a list of values to a field_schema:
        - Exit if value_list is not a list
        - Define or check the type of each value of the list.
        - Recursively add 'dict' values to the schema.

    Parameters
    ----------
        value_list: list of values
        field_schema: field schema to update
    """
    if isinstance(value_list, list):
        if "array_types_count" not in field_schema:
            field_schema["array_types_count"] = defaultdict(int)

        if not value_list:
            add_value_type(None, field_schema, type_str="array_types_count")

        for value in value_list:
            add_value_type(value, field_schema, type_str="array_types_count")
            add_potential_document_to_field_schema(value, field_schema)


def add_value_type(
    value: Any, field_schema: Dict[str, Any], type_str="types_count"
) -> None:
    """
    Define the type_str in field_schema, or check if it is equal to the one previously defined.

    Parameters
    ----------
        value: value to get type of
        field_schema: field schema to update
        type_str: type string (either 'types_count' or 'array_types_count')
    """
    value_type_str = get_type_string(value)
    field_schema[type_str][value_type_str] += 1


def flatten_schema(
    nested_schema: Dict[str, Any], parent: str, delimiter: str
) -> List[str]:
    """
    Flatten a nested schema by concatenating keys with a given delimiter.
    """

    flattened = []

    for key in list(nested_schema):
        initial = "" if parent == "" else delimiter

        if type(nested_schema[key]) == dict:

            flattened.extend(
                flatten_schema(
                    nested_schema[key], initial.join((parent, key)), delimiter
                )
            )
        else:
            flattened.extend([initial.join((parent, key))])
    return flattened


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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        env = "PROD"
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
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})",
                    aspects=[],
                )

                dataset_properties = DatasetPropertiesClass(
                    tags=[],
                    customProperties={},
                )
                dataset_snapshot.aspects.append(dataset_properties)

                # TODO: Guess the schema via sampling

                # code adapted from https://github.com/pajachiet/pymongo-schema
                collection_schema = extract_collection_schema(database[collection_name])
                import json
                from pprint import pprint

                print(json.dumps(collection_schema))
                pprint(collection_schema)

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
