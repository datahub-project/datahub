import itertools
import logging
from typing import IO, Dict, List, Type, Union

import ijson
import jsonlines as jsl
import ujson

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.schema_inference.object import construct_schema
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

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

logger = logging.getLogger(__name__)

_JsonContainer = Union[Dict[str, object], List[object]]


def _bounded_json_value(file: IO[bytes], max_rows: int) -> object:
    """Stream-rebuild a single JSON value, truncating every array to ``max_rows``
    elements so a large single object (e.g. ``{"data": [ ...millions... ]}``) is
    not fully loaded into memory just to infer its schema.

    Only arrays are bounded. A wide object (``{"user_1": {...}, "user_2": {...},
    ...}``) is still rebuilt in full, because dropping keys would drop the fields
    they describe - a column-oriented dump would silently lose columns.

    Array items beyond ``max_rows`` are dropped, so the inferred schema reflects
    only the first ``max_rows`` items of each array: a field that first appears in
    a later item is not reported, and nullability is decided from that sample.
    """
    root: List[object] = []
    stack: List[_JsonContainer] = []
    keys: List[str] = []  # pending key of each open object, innermost last
    skip_depth = 0  # >0 while skipping a subtree that is over the array limit

    def _at_array_limit() -> bool:
        if not stack:
            return False
        container = stack[-1]
        return isinstance(container, list) and len(container) >= max_rows

    def _attach(value: object) -> None:
        if not stack:
            root.append(value)
            return
        container = stack[-1]
        if isinstance(container, list):
            container.append(value)
        else:
            container[keys[-1]] = value

    # use_float keeps numbers as native int/float (not Decimal), matching json.load.
    for _prefix, event, value in ijson.parse(file, use_float=True):
        if skip_depth:
            if event in ("start_map", "start_array"):
                skip_depth += 1
            elif event in ("end_map", "end_array"):
                skip_depth -= 1
            continue

        if event == "map_key":
            keys[-1] = value
        elif event in ("start_map", "start_array"):
            if _at_array_limit():
                skip_depth = 1
                continue
            container: _JsonContainer = {} if event == "start_map" else []
            _attach(container)
            stack.append(container)
            if event == "start_map":
                keys.append("")
        elif event == "end_map":
            stack.pop()
            keys.pop()
        elif event == "end_array":
            stack.pop()
        else:  # scalar
            if _at_array_limit():
                continue
            _attach(value)

    if not root:
        # Any valid document emits at least one event, so this is a parse anomaly.
        # Raise rather than invent {}, letting the caller fall back to jsonlines.
        raise ijson.common.JSONError("no JSON value found")
    return root[0]


class JsonInferrer(SchemaInferenceBase):
    def __init__(self, max_rows: int = 100, format: str = "json"):
        self.max_rows = max_rows
        self.format = format

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        if self.format == "jsonl":
            file.seek(0)
            reader = jsl.Reader(file)
            datastore = [
                obj
                for obj in itertools.islice(
                    reader.iter(type=dict, skip_invalid=True), self.max_rows
                )
            ]
        else:
            try:
                # Stream-parse to avoid loading the entire file into memory.
                # ijson.items(file, 'item') lazily yields elements of a top-level JSON array.
                file.seek(0)
                datastore = list(
                    itertools.islice(ijson.items(file, "item"), self.max_rows)
                )
                if not datastore:
                    # Not a top-level array — likely a single JSON object.
                    file.seek(0)
                    root_value = _bounded_json_value(file, self.max_rows)
                    # A non-object root (bare scalar, empty array) has no fields.
                    datastore = [root_value] if isinstance(root_value, dict) else []
            except (
                ujson.JSONDecodeError,
                ijson.common.JSONError,
                UnicodeDecodeError,
            ) as e:
                logger.info(f"Failed to parse as JSON: {e}. Retry with jsonlines")
                file.seek(0)
                reader = jsl.Reader(file)
                datastore = [
                    obj
                    for obj in itertools.islice(
                        reader.iter(type=dict, skip_invalid=True), self.max_rows
                    )
                ]

        if not isinstance(datastore, list):
            datastore = [datastore]

        schema = construct_schema(datastore, delimiter=".")
        fields: List[SchemaField] = []

        for schema_field in schema.values():
            mapped_type = _field_type_mapping.get(schema_field["type"], NullTypeClass)

            native_type = schema_field["type"]

            if isinstance(native_type, type):
                native_type = native_type.__name__

            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=native_type,
                type=SchemaFieldDataType(type=mapped_type()),
                nullable=schema_field["nullable"],
                recursive=False,
            )
            fields.append(field)

        return fields
