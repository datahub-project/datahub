from io import TextIOWrapper
from os import PathLike
from typing import Dict, List, Type, Union

import ujson

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.schema_inference.object import construct_schema
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
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
    @staticmethod
    def infer_schema(file: TextIOWrapper) -> List[SchemaField]:

        datastore = ujson.load(file)

        if not isinstance(datastore, list):
            datastore = [datastore]

        schema = construct_schema(datastore, delimiter=".")

        fields = []

        for schema_field in sorted(schema.values(), key=lambda x: x["delimited_name"]):
            mapped_type = _field_type_mapping.get(schema_field["type"], NullTypeClass)

            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=str(mapped_type),
                type=SchemaFieldDataType(type=mapped_type()),
                nullable=schema_field["nullable"],
                recursive=False,
            )
            fields.append(field)

        return fields
