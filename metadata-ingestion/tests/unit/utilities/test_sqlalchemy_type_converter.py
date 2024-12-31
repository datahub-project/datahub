from typing import no_type_check
from unittest.mock import MagicMock

from sqlalchemy import types
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy_bigquery import STRUCT

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
)
from datahub.utilities.sqlalchemy_type_converter import (
    MapType,
    get_schema_fields_for_sqlalchemy_column,
)


def test_get_avro_schema_for_sqlalchemy_column():
    inspector_magic_mock = MagicMock()
    inspector_magic_mock.dialect = DefaultDialect()

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        column_name="test", column_type=types.INTEGER(), inspector=inspector_magic_mock
    )
    assert len(schema_fields) == 1
    assert schema_fields[0].fieldPath == "[version=2.0].[type=int].test"
    assert schema_fields[0].type.type == NumberTypeClass()
    assert schema_fields[0].nativeDataType == "INTEGER"
    assert schema_fields[0].nullable is True

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        column_name="test",
        column_type=types.String(),
        nullable=False,
        inspector=inspector_magic_mock,
    )
    assert len(schema_fields) == 1
    assert schema_fields[0].fieldPath == "[version=2.0].[type=string].test"
    assert schema_fields[0].type.type == NumberTypeClass()
    assert schema_fields[0].nativeDataType == "VARCHAR"
    assert schema_fields[0].nullable is False


def test_get_avro_schema_for_sqlalchemy_array_column():
    inspector_magic_mock = MagicMock()
    inspector_magic_mock.dialect = DefaultDialect()

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        column_name="test",
        column_type=types.ARRAY(types.FLOAT()),
        inspector=inspector_magic_mock,
    )
    assert len(schema_fields) == 1
    assert (
        schema_fields[0].fieldPath
        == "[version=2.0].[type=struct].[type=array].[type=float].test"
    )
    assert schema_fields[0].type.type == ArrayTypeClass(nestedType=["float"])
    assert schema_fields[0].nativeDataType == "array<FLOAT>"


def test_get_avro_schema_for_sqlalchemy_map_column():
    inspector_magic_mock = MagicMock()
    inspector_magic_mock.dialect = DefaultDialect()

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        column_name="test",
        column_type=MapType(types.String(), types.BOOLEAN()),
        inspector=inspector_magic_mock,
    )
    assert len(schema_fields) == 1
    assert (
        schema_fields[0].fieldPath
        == "[version=2.0].[type=struct].[type=map].[type=boolean].test"
    )
    assert schema_fields[0].type.type == MapTypeClass(
        keyType="string", valueType="boolean"
    )
    assert schema_fields[0].nativeDataType == "MapType(String(), BOOLEAN())"


def test_get_avro_schema_for_sqlalchemy_struct_column() -> None:
    inspector_magic_mock = MagicMock()
    inspector_magic_mock.dialect = DefaultDialect()

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        column_name="test",
        column_type=STRUCT(("test", types.INTEGER())),
        inspector=inspector_magic_mock,
    )
    assert len(schema_fields) == 2
    assert (
        schema_fields[0].fieldPath == "[version=2.0].[type=struct].[type=struct].test"
    )
    assert schema_fields[0].type.type == RecordTypeClass()
    assert schema_fields[0].nativeDataType == "STRUCT<test INT64>"

    assert (
        schema_fields[1].fieldPath
        == "[version=2.0].[type=struct].[type=struct].test.[type=int].test"
    )
    assert schema_fields[1].type.type == NumberTypeClass()
    assert schema_fields[1].nativeDataType == "INTEGER"


@no_type_check
def test_get_avro_schema_for_sqlalchemy_unknown_column():
    inspector_magic_mock = MagicMock()
    inspector_magic_mock.dialect = DefaultDialect()

    schema_fields = get_schema_fields_for_sqlalchemy_column(
        "invalid", "test", inspector=inspector_magic_mock
    )
    assert len(schema_fields) == 1
    assert schema_fields[0].type.type == NullTypeClass()
    assert schema_fields[0].fieldPath == "[version=2.0].[type=null]"
    assert schema_fields[0].nativeDataType == "test"
