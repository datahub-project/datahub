import logging
from typing import IO, Dict, List, Type, Union

import jsonlines as jsl
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


class JsonInferrer(SchemaInferenceBase):
    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        try:
            datastore = ujson.load(file)
        except ujson.JSONDecodeError as e:
            logger.info(f"Got ValueError: {e}. Retry with jsonlines")
            file.seek(0)
            reader = jsl.Reader(file)
            datastore = [obj for obj in reader.iter(type=dict, skip_invalid=True)]

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
