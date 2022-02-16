from io import TextIOWrapper
from typing import List, Type

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

from datahub.ingestion.extractor import schema_util


class AvroInferrer(SchemaInferenceBase):
    def infer_schema(self, file: TextIOWrapper) -> List[SchemaField]:

        reader = DataFileReader(file, DatumReader())

        schema = schema_util.avro_schema_to_mce_fields(reader.schema)

        return schema
