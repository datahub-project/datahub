import tempfile
from typing import List, Type

import avro.schema
import pandas as pd
import ujson
from avro import schema as avro_schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaField,
    StringTypeClass,
)
from tests.unit.test_schema_util import assert_field_paths_match

expected_field_paths = [
    "boolean_field",
    "integer_field",
    "string_field",
]

expected_field_paths_avro = [
    "[version=2.0].[type=test].[type=boolean].boolean_field",
    "[version=2.0].[type=test].[type=int].integer_field",
    "[version=2.0].[type=test].[type=string].string_field",
]

expected_field_types = [BooleanTypeClass, NumberTypeClass, StringTypeClass]

test_table = pd.DataFrame(
    {
        "boolean_field": [True, False, True],
        "integer_field": [1, 2, 3],
        "string_field": ["a", "b", "c"],
    }
)


def assert_field_types_match(
    fields: List[SchemaField], expected_field_types: List[Type]
) -> None:
    assert len(fields) == len(expected_field_types)
    for field, expected_type in zip(fields, expected_field_types):
        assert isinstance(field.type.type, expected_type)


def test_infer_schema_csv():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(bytes(test_table.to_csv(index=False, header=True), encoding="utf-8"))
        file.seek(0)

        fields = csv_tsv.CsvInferrer(max_rows=100).infer_schema(file)
        fields.sort(key=lambda x: x.fieldPath)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_tsv():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(
            bytes(
                test_table.to_csv(index=False, header=True, sep="\t"), encoding="utf-8"
            )
        )
        file.seek(0)

        fields = csv_tsv.TsvInferrer(max_rows=100).infer_schema(file)
        fields.sort(key=lambda x: x.fieldPath)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_json():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(bytes(test_table.to_json(orient="records"), encoding="utf-8"))
        file.seek(0)

        fields = json.JsonInferrer().infer_schema(file)
        fields.sort(key=lambda x: x.fieldPath)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_parquet():
    with tempfile.TemporaryFile(mode="w+b") as file:
        test_table.to_parquet(file)
        file.seek(0)

        fields = parquet.ParquetInferrer().infer_schema(file)
        fields.sort(key=lambda x: x.fieldPath)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_avro():
    with tempfile.TemporaryFile(mode="w+b") as file:
        schema = avro_schema.parse(
            ujson.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {"name": "boolean_field", "type": "boolean"},
                        {"name": "integer_field", "type": "int"},
                        {"name": "string_field", "type": "string"},
                    ],
                }
            )
        )
        writer = DataFileWriter(file, DatumWriter(), schema)
        records = test_table.to_dict(orient="records")
        for record in records:
            writer.append(record)
        writer.sync()

        file.seek(0)

        fields = avro.AvroInferrer().infer_schema(file)
        fields.sort(key=lambda x: x.fieldPath)

        assert_field_paths_match(fields, expected_field_paths_avro)
        assert_field_types_match(fields, expected_field_types)
