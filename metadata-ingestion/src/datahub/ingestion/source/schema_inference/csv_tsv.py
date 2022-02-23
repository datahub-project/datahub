from typing import IO, Dict, List, Type

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
tableschema_type_map: Dict[str, Type] = {
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


def get_table_schema_fields(table: Table, max_rows: int) -> List[SchemaField]:
    table.infer(limit=max_rows)

    fields: List[SchemaField] = []

    for raw_field in table.schema.fields:
        mapped_type: Type = tableschema_type_map.get(raw_field.type, NullTypeClass)

        field = SchemaField(
            fieldPath=raw_field.name,
            type=SchemaFieldDataType(mapped_type()),
            nativeDataType=str(raw_field.type),
            recursive=False,
        )
        fields.append(field)

    return fields


class CsvInferrer(SchemaInferenceBase):
    def __init__(self, max_rows: int):
        self.max_rows = max_rows

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        # infer schema of a csv file without reading the whole file
        table = Table(file, format="csv")
        return get_table_schema_fields(table, max_rows=self.max_rows)


class TsvInferrer(SchemaInferenceBase):
    def __init__(self, max_rows: int):
        self.max_rows = max_rows

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        # infer schema of a tsv file without reading the whole file
        table = Table(file, format="tsv")
        return get_table_schema_fields(table, max_rows=self.max_rows)
