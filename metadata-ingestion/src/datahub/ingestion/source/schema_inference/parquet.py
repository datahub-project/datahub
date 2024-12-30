from typing import IO, Any, Callable, Dict, List, Type

import pyarrow
import pyarrow.parquet

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)

# see https://arrow.apache.org/docs/python/api/datatypes.html#type-checking
pyarrow_type_map: Dict[Callable[[Any], bool], Type] = {
    pyarrow.types.is_boolean: BooleanTypeClass,
    pyarrow.types.is_integer: NumberTypeClass,
    pyarrow.types.is_signed_integer: NumberTypeClass,
    pyarrow.types.is_unsigned_integer: NumberTypeClass,
    pyarrow.types.is_int8: NumberTypeClass,
    pyarrow.types.is_int16: NumberTypeClass,
    pyarrow.types.is_int32: NumberTypeClass,
    pyarrow.types.is_int64: NumberTypeClass,
    pyarrow.types.is_uint8: NumberTypeClass,
    pyarrow.types.is_uint16: NumberTypeClass,
    pyarrow.types.is_uint32: NumberTypeClass,
    pyarrow.types.is_uint64: NumberTypeClass,
    pyarrow.types.is_floating: NumberTypeClass,
    pyarrow.types.is_float16: NumberTypeClass,
    pyarrow.types.is_float32: NumberTypeClass,
    pyarrow.types.is_float64: NumberTypeClass,
    pyarrow.types.is_decimal: NumberTypeClass,
    pyarrow.types.is_list: ArrayTypeClass,
    pyarrow.types.is_large_list: ArrayTypeClass,
    pyarrow.types.is_struct: RecordTypeClass,
    pyarrow.types.is_union: UnionTypeClass,
    pyarrow.types.is_nested: RecordTypeClass,
    pyarrow.types.is_temporal: TimeTypeClass,
    pyarrow.types.is_timestamp: TimeTypeClass,
    pyarrow.types.is_date: DateTypeClass,
    pyarrow.types.is_date32: DateTypeClass,
    pyarrow.types.is_date64: DateTypeClass,
    pyarrow.types.is_time: TimeTypeClass,
    pyarrow.types.is_time32: TimeTypeClass,
    pyarrow.types.is_time64: TimeTypeClass,
    pyarrow.types.is_null: NullTypeClass,
    pyarrow.types.is_binary: BytesTypeClass,
    pyarrow.types.is_unicode: StringTypeClass,
    pyarrow.types.is_string: StringTypeClass,
    pyarrow.types.is_large_binary: BytesTypeClass,
    pyarrow.types.is_large_unicode: StringTypeClass,
    pyarrow.types.is_large_string: StringTypeClass,
    pyarrow.types.is_fixed_size_binary: BytesTypeClass,
    pyarrow.types.is_map: RecordTypeClass,
    pyarrow.types.is_dictionary: RecordTypeClass,
}


def map_pyarrow_type(pyarrow_type: Type) -> Type:
    for checker, mapped_type in pyarrow_type_map.items():
        if checker(pyarrow_type):
            return mapped_type

    return NullTypeClass


class ParquetInferrer(SchemaInferenceBase):
    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        # infer schema of a parquet file without reading the whole file

        # read the first line of the file
        schema = pyarrow.parquet.read_schema(file, memory_map=True)

        fields: List[SchemaField] = []

        for name, pyarrow_type in zip(schema.names, schema.types):
            mapped_type = map_pyarrow_type(pyarrow_type)

            field = SchemaField(
                fieldPath=name,
                type=SchemaFieldDataType(mapped_type()),
                nativeDataType=str(pyarrow_type),
                recursive=False,
            )

            fields.append(field)

        return fields
