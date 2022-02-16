from io import TextIOWrapper
from typing import List

from tableschema import Table

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
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

# see https://github.com/frictionlessdata/tableschema-py/blob/main/tableschema/schema.py#L545
tableschema_type_map = {
    "duration": TimeTypeClass,
    "geojson": RecordTypeClass,
    "geopoint": RecordTypeClass,
    "object": RecordTypeClass,
    "array": ArrayTypeClass,
    "datetime": TimeTypeClass,
    "time": TimeTypeClass,
    "date": DateTypeClass,
    "integer": NumberTypeClass,
    "number": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "string": StringTypeClass,
    "any": UnionTypeClass,
}

MAX_ROWS = 100


def infer_schema_general(table: Table) -> List[SchemaField]:
    table.infer(limit=MAX_ROWS)

    fields = []

    for raw_field in table.schema.fields:

        mapped_type = tableschema_type_map.get(raw_field.type, NullTypeClass)

        field = SchemaField(
            fieldPath=raw_field.name,
            type=SchemaFieldDataType(mapped_type()),
            nativeDataType=str(raw_field.type),
            recursive=False,
        )

        fields.append(field)

    return fields


class CsvInferrer(SchemaInferenceBase):
    @staticmethod
    def infer_schema(file: TextIOWrapper) -> List[SchemaField]:
        # infer schema of a csv file without reading the whole file
        table = Table(file, format="csv")

        return infer_schema_general(table)


class TsvInferrer(SchemaInferenceBase):
    @staticmethod
    def infer_schema(file: TextIOWrapper) -> List[SchemaField]:
        # infer schema of a tsv file without reading the whole file
        table = Table(file, format="tsv")

        return infer_schema_general(table)
