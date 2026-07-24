import itertools
import logging
from typing import IO, Any, Dict, List, Optional, Tuple, Type, Union

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


def _bounded_json_value(file: IO[bytes], max_rows: int) -> Any:
    """Stream-rebuild a single JSON value, truncating each array to ``max_rows``
    elements so a huge single object (e.g. ``{"data": [ ...millions... ]}``) is
    not fully loaded into memory just to infer its schema.

    ponytail: array items beyond ``max_rows`` are dropped, so the inferred schema
    reflects only the first ``max_rows`` items of each array.
    """
    root: List[object] = []
    stack: List[Tuple[object, str]] = []  # (container, "map" | "array")
    keys: List[Optional[str]] = []
    skip_depth = 0  # >0 while skipping a subtree that is over the array limit

    def _at_array_limit() -> bool:
        return bool(stack) and stack[-1][1] == "array" and len(stack[-1][0]) >= max_rows  # type: ignore[arg-type]

    def _attach(value: object) -> None:
        if not stack:
            root.append(value)
            return
        container, kind = stack[-1]
        if kind == "map":
            container[keys[-1]] = value  # type: ignore[index]
        else:
            container.append(value)  # type: ignore[attr-defined]

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
            container: object = {} if event == "start_map" else []
            _attach(container)
            stack.append((container, "map" if event == "start_map" else "array"))
            if event == "start_map":
                keys.append(None)
        elif event == "end_map":
            stack.pop()
            keys.pop()
        elif event == "end_array":
            stack.pop()
        else:  # scalar
            if _at_array_limit():
                continue
            _attach(value)

    return root[0] if root else {}


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
                    datastore = [_bounded_json_value(file, self.max_rows)]
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
