import pyarrow
import pyarrow.parquet
import ujson
from avro.datafile import DataFileReader
from avro.io import DatumReader
from genson import SchemaBuilder
from tableschema import Table

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


def infer_schema_csv(file_path):
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


# see https://arrow.apache.org/docs/python/api/datatypes.html#type-checking
pyarrow_type_map = {
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


def map_pyarrow_type(pyarrow_type):

    for checker, mapped_type in pyarrow_type_map.items():

        if checker(pyarrow_type):
            return mapped_type

    # TODO: raise warning
    return NullTypeClass


def infer_schema_parquet(file_path):
    # infer schema of a parquet file without reading the whole file

    # read the first line of the file
    schema = pyarrow.parquet.read_schema(file_path, memory_map=True)

    fields = []

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


def infer_schema_json(file_path):
    # infer schema of a json file without reading the whole file

    builder = SchemaBuilder()
    with open(file_path, "r") as f:
        datastore = ujson.load(f)
        builder.add_object(datastore)

    # return builder.to_schema()
    return []


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


def infer_schema_avro(file_path):

    with open(file_path, "rb") as f:

        reader = DataFileReader(f, DatumReader())
        schema = ujson.loads(reader.schema)

        fields = []

        for field in schema["fields"]:
            name = field["name"]
            avro_type = field["type"]

            mapped_type = None
            nullable = False

            if isinstance(avro_type, str):
                mapped_type = avro_type_map.get(avro_type)

            elif isinstance(avro_type, list) and len(avro_type) > 1:

                if "null" in avro_type and len(avro_type) == 2:
                    avro_type.remove("null")
                    nullable = True
                    mapped_type = avro_type_map.get(avro_type[0])
                else:

                    union_types = []

                    for subtype in avro_type:

                        mapped_subtype = avro_type_map.get(subtype)

                        if mapped_subtype is None:
                            # TODO: raise warning
                            mapped_subtype = NullTypeClass

                        union_types.append(mapped_subtype)

                    mapped_type = UnionTypeClass(union_types)

            else:
                mapped_type = NullTypeClass
                # TODO: raise warning

            field = SchemaField(
                fieldPath=name,
                type=SchemaFieldDataType(mapped_type()),
                nativeDataType=str(avro_type),
                recursive=False,
                nullable=nullable,
            )

            fields.append(field)

        return fields
