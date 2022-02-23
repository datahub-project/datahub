from collections import Counter
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Sequence, Tuple, Union

from mypy_extensions import TypedDict


class BasicSchemaDescription(TypedDict):
    types: CounterType[type]  # field types and times seen
    count: int  # times the field was seen


class SchemaDescription(BasicSchemaDescription):
    delimited_name: str  # collapsed field name
    # we use 'mixed' to denote mixed types, so we need a str here
    type: Union[type, str]  # collapsed type
    nullable: bool  # if field is ever missing


def is_field_nullable(doc: Dict[str, Any], field_path: Tuple) -> bool:
    """
    Check if a nested field is nullable in a document from a collection.

    Parameters
    ----------
        doc:
            document to check nullability for
        field_path:
            path to nested field to check, ex. ('first_field', 'nested_child', '2nd_nested_child')
    """

    if not field_path:
        return True

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
            return is_field_nullable(doc[field], remaining_fields)
        # if list, check if any member is missing field
        if isinstance(value, list):
            # count empty lists of nested objects as nullable
            if len(value) == 0:
                return True
            return any(is_field_nullable(x, remaining_fields) for x in doc[field])

        # any other types to check?
        # raise ValueError("Nested type not 'list' or 'dict' encountered")
        return True

    return True


def is_nullable_collection(
    collection: Sequence[Dict[str, Any]], field_path: Tuple
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

    return any(is_field_nullable(doc, field_path) for doc in collection)


def construct_schema(
    collection: Sequence[Dict[str, Any]], delimiter: str
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
