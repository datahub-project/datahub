from io import TextIOWrapper
from os import PathLike
from typing import List, Type, Union

import pyarrow.parquet
import ujson
from avro.datafile import DataFileReader
from avro.io import DatumReader

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

# see https://avro.apache.org/docs/current/spec.html
avro_type_map = {
    "null": NullTypeClass,
    "boolean": BooleanTypeClass,
    "int": NumberTypeClass,
    "long": NumberTypeClass,
    "float": NumberTypeClass,
    "double": NumberTypeClass,
    "bytes": BytesTypeClass,
    "string": StringTypeClass,
    "record": RecordTypeClass,
    "enum": EnumTypeClass,
    "array": ArrayTypeClass,
    "map": MapTypeClass,
    "fixed": StringTypeClass,
}


class AvroInferrer(SchemaInferenceBase):
    @staticmethod
    def infer_schema(file: TextIOWrapper) -> List[SchemaField]:

        reader = DataFileReader(file, DatumReader())
        schema = ujson.loads(reader.schema)

        fields = []

        for field in schema["fields"]:
            name = field["name"]
            avro_type = field["type"]

            mapped_type: Type = NullTypeClass
            type_args = None
            nullable = False

            if isinstance(avro_type, str):
                mapped_type = avro_type_map.get(avro_type, NullTypeClass)

            elif isinstance(avro_type, list) and len(avro_type) > 1:

                if "null" in avro_type and len(avro_type) == 2:
                    avro_type.remove("null")
                    nullable = True
                    mapped_type = avro_type_map.get(avro_type[0], NullTypeClass)
                else:

                    mapped_type = UnionTypeClass
                    type_args = avro_type

            else:
                mapped_type = NullTypeClass
                # TODO: raise warning

            field = SchemaField(
                fieldPath=name,
                type=SchemaFieldDataType(mapped_type(type_args)),
                nativeDataType=str(avro_type),
                recursive=False,
                nullable=nullable,
            )

            fields.append(field)

        return fields
