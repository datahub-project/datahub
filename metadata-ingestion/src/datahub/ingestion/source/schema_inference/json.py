from collections import Counter
from os import PathLike
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Iterable, List, Tuple, Type, TypedDict, Union

import ujson

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.schema_inference.object import construct_schema
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
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
}


class JsonInferrer(SchemaInferenceBase):
    def infer_schema(file_path: Union[str, PathLike]) -> List[SchemaField]:

        with open(file_path, "r") as f:
            datastore = ujson.load(f)

            if not isinstance(datastore, list):
                datastore = [datastore]

            schema = construct_schema(datastore, delimiter=".")

            fields = []

            for schema_field in sorted(
                schema.values(), key=lambda x: x["delimited_name"]
            ):
                mapped_type = _field_type_mapping.get(
                    schema_field["type"], NullTypeClass
                )

                field = SchemaField(
                    fieldPath=schema_field["delimited_name"],
                    nativeDataType=str(mapped_type),
                    type=SchemaFieldDataType(type=mapped_type()),
                    nullable=schema_field["nullable"],
                    recursive=False,
                )
                fields.append(field)

            return fields
