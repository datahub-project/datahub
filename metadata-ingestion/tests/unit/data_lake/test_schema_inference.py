import tempfile
from typing import List, Type

import pandas as pd
import ujson
from avro import schema as avro_schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from datahub.ingestion.source.schema_inference import csv_tsv, json, parquet
from datahub.ingestion.source.schema_inference.avro import AvroInferrer
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaField,
    StringTypeClass,
)
from tests.unit.test_schema_util import assert_field_paths_match
from datahub.ingestion.source.schema_inference.parquet import get_column_metadata

expected_field_paths = [
    "integer_field",
    "boolean_field",
    "string_field",
]

expected_field_paths_avro = [
    "[version=2.0].[type=test].[type=int].integer_field",
    "[version=2.0].[type=test].[type=boolean].boolean_field",
    "[version=2.0].[type=test].[type=string].string_field",
]

expected_field_types = [NumberTypeClass, BooleanTypeClass, StringTypeClass]

test_table = pd.DataFrame(
    {
        "integer_field": [1, 2, 3],
        "boolean_field": [True, False, True],
        "string_field": ["a", "b", "c"],
    }
)

# Add descriptions to columns
test_table["integer_field"].attrs["description"] = "A column containing integer values"
test_table["boolean_field"].attrs["description"] = "A column containing boolean values"
test_table["string_field"].attrs["description"] = "A column containing string values"


expected_field_descriptions = [
    "A column containing integer values",
    "A column containing boolean values",
    "A column containing string values",
]


test_column_metadata = [
    {"name": "integer_field", "metadata": { "integer_field" : "A column containing integer values"}},
    {"name": "boolean_field", "metadata": { "boolean_field" : "A column containing boolean values"}},
    {"name": "string_field", "metadata": { "string_field": "A column containing string values"}},
]

def test_get_column_metadata_column_exists():
    # Test when column exists with metadata
    schema_dict = {
        "col1": {
            "name": "age",
            "metadata": {"description": "Age of person"}
        },
        "col2": {
            "name": "name",
            "metadata": {"description": "Full name"}
        }
    }

    result = get_column_metadata(schema_dict, "age")
    assert result == {"description": "Age of person"}


def test_get_column_metadata_column_no_metadata():
    # Test when column exists but has no metadata
    schema_dict = {
        "col1": {
            "name": "age"
        }
    }

    result = get_column_metadata(schema_dict, "age")
    assert result == {}


def test_get_column_metadata_column_not_found():
    # Test when column doesn't exist
    schema_dict = {
        "col1": {
            "name": "age",
            "metadata": {"description": "Age of person"}
        }
    }

    result = get_column_metadata(schema_dict, "non_existent")
    assert result is None


def test_get_column_metadata_empty_dict():
    # Test with empty schema dictionary
    schema_dict = {}

    result = get_column_metadata(schema_dict, "any_column")
    assert result is None


def test_get_column_metadata_invalid_schema():
    # Test with invalid schema structure
    schema_dict = {
        "col1": "invalid_structure",
        "col2": None
    }

    result = get_column_metadata(schema_dict, "col1")
    assert result is None


def test_get_column_metadata_empty_column_name():
    # Test with empty column name
    schema_dict = {
        "col1": {
            "name": "",
            "metadata": {"description": "Empty name"}
        }
    }

    result = get_column_metadata(schema_dict, "")
    assert result == {"description": "Empty name"}


def test_get_column_metadata():
    assert "A column containing integer values" == get_column_metadata(test_column_metadata, "integer_field")
    assert "A column containing boolean values" == get_column_metadata(test_column_metadata, "boolean_field")
    assert "A column containing string values" == get_column_metadata(test_column_metadata, "string_field")


def assert_field_types_match(
    fields: List[SchemaField], expected_field_types: List[Type]
) -> None:
    assert len(fields) == len(expected_field_types)
    for field, expected_type in zip(fields, expected_field_types):
        assert isinstance(field.type.type, expected_type)


def assert_field_descriptions_match(
    fields: List[SchemaField], expected_field_descriptions: List[str]
) -> None:
    assert len(fields) == len(expected_field_descriptions)
    for field, expected_description in zip(fields, expected_field_descriptions):
        assert field.description == expected_description


def test_infer_schema_csv():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(bytes(test_table.to_csv(index=False, header=True), encoding="utf-8"))
        file.seek(0)

        fields = csv_tsv.CsvInferrer(max_rows=100).infer_schema(file)

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

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_jsonl():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(
            bytes(test_table.to_json(orient="records", lines=True), encoding="utf-8")
        )
        file.seek(0)

        fields = json.JsonInferrer(max_rows=100, format="jsonl").infer_schema(file)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_json():
    with tempfile.TemporaryFile(mode="w+b") as file:
        file.write(bytes(test_table.to_json(orient="records"), encoding="utf-8"))
        file.seek(0)

        fields = json.JsonInferrer().infer_schema(file)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)


def test_infer_schema_parquet():
    with tempfile.TemporaryFile(mode="w+b") as file:
        test_table.to_parquet(file)
        file.seek(0)
        fields = parquet.ParquetInferrer().infer_schema(file)

        assert_field_paths_match(fields, expected_field_paths)
        assert_field_types_match(fields, expected_field_types)
        assert_field_descriptions_match(fields, expected_field_descriptions)


def test_infer_schema_avro():
    with tempfile.TemporaryFile(mode="w+b") as file:
        schema = avro_schema.parse(
            ujson.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {"name": "integer_field", "type": "int"},
                        {"name": "boolean_field", "type": "boolean"},
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

        fields = AvroInferrer().infer_schema(file)

        assert_field_paths_match(fields, expected_field_paths_avro)
        assert_field_types_match(fields, expected_field_types)
