from os import PathLike
from typing import List, Union
import pyarrow
import pyarrow.parquet
import ujson
from avro.datafile import DataFileReader
from avro.io import DatumReader
from genson import SchemaBuilder
from tableschema import Table

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MapTypeClass,
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


class CsvInferrer(SchemaInferenceBase):
    def infer_schema(file_path: Union[str, PathLike]) -> List[SchemaField]:
        # infer schema of a csv file without reading the whole file

        # read the first line of the file
        table = Table(file_path)

        table.read(keyed=True, limit=100)
        table.infer()

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
