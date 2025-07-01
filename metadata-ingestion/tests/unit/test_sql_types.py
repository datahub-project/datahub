import pytest

from datahub.ingestion.source.sql.sql_types import (
    ATHENA_SQL_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    TRINO_SQL_TYPES_MAP,
    resolve_athena_modified_type,
    resolve_snowflake_modified_type,
    resolve_sql_type,
    resolve_trino_modified_type,
)
from datahub.metadata.schema_classes import BooleanTypeClass, StringTypeClass


@pytest.mark.parametrize(
    "data_type, expected_data_type",
    [
        ("boolean", "boolean"),
        ("tinyint", "tinyint"),
        ("smallint", "smallint"),
        ("int", "int"),
        ("integer", "integer"),
        ("bigint", "bigint"),
        ("real", "real"),
        ("double", "double"),
        ("decimal(10,0)", "decimal"),
        ("varchar(20)", "varchar"),
        ("char", "char"),
        ("varbinary", "varbinary"),
        ("json", "json"),
        ("date", "date"),
        ("time", "time"),
        ("time(12)", "time"),
        ("timestamp", "timestamp"),
        ("timestamp(3)", "timestamp"),
        ("row(x bigint, y double)", "row"),
        ("array(row(x bigint, y double))", "array"),
        ("map(varchar, varchar)", "map"),
    ],
)
def test_resolve_trino_modified_type(data_type, expected_data_type):
    assert (
        resolve_trino_modified_type(data_type)
        == TRINO_SQL_TYPES_MAP[expected_data_type]
    )


@pytest.mark.parametrize(
    "data_type, expected_data_type",
    [
        ("boolean", "boolean"),
        ("tinyint", "tinyint"),
        ("smallint", "smallint"),
        ("int", "int"),
        ("integer", "integer"),
        ("bigint", "bigint"),
        ("float", "float"),
        ("double", "double"),
        ("decimal(10,0)", "decimal"),
        ("varchar(20)", "varchar"),
        ("char", "char"),
        ("binary", "binary"),
        ("date", "date"),
        ("timestamp", "timestamp"),
        ("timestamp(3)", "timestamp"),
        ("struct<x timestamp(3), y timestamp>", "struct"),
        ("array<struct<x bigint, y double>>", "array"),
        ("map<varchar, varchar>", "map"),
    ],
)
def test_resolve_athena_modified_type(data_type, expected_data_type):
    assert (
        resolve_athena_modified_type(data_type)
        == ATHENA_SQL_TYPES_MAP[expected_data_type]
    )


@pytest.mark.parametrize(
    "data_type, expected_data_type",
    [
        ("BOOLEAN", "BOOLEAN"),
        ("TINYINT", "TINYINT"),
        ("BYTEINT", "BYTEINT"),
        ("SMALLINT", "SMALLINT"),
        ("INT", "INT"),
        ("INTEGER", "INTEGER"),
        ("BIGINT", "BIGINT"),
        ("FLOAT", "FLOAT"),
        ("FLOAT4", "FLOAT4"),
        ("FLOAT8", "FLOAT8"),
        ("DOUBLE", "DOUBLE"),
        ("DOUBLE PRECISION", "DOUBLE PRECISION"),
        ("REAL", "REAL"),
        ("NUMBER(10,0)", "NUMBER"),
        ("DECIMAL(38,2)", "DECIMAL"),
        ("NUMERIC(15,4)", "NUMERIC"),
        ("VARCHAR(20)", "VARCHAR"),
        ("CHARACTER VARYING(50)", "CHARACTER VARYING"),
        ("CHAR(10)", "CHAR"),
        ("CHARACTER(5)", "CHARACTER"),
        ("STRING", "STRING"),
        ("TEXT", "TEXT"),
        ("BINARY", "BINARY"),
        ("VARBINARY", "VARBINARY"),
        ("DATE", "DATE"),
        ("DATETIME", "DATETIME"),
        ("TIME", "TIME"),
        ("TIME(3)", "TIME"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("TIMESTAMP(3)", "TIMESTAMP"),
        ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ"),
        ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
        ("TIMESTAMP_TZ", "TIMESTAMP_TZ"),
        ("VARIANT", "VARIANT"),
        ("OBJECT", "OBJECT"),
        ("ARRAY", "ARRAY"),
        ("GEOGRAPHY", "GEOGRAPHY"),
    ],
)
def test_resolve_snowflake_type(data_type, expected_data_type):
    assert (
        resolve_snowflake_modified_type(data_type)
        == SNOWFLAKE_TYPES_MAP[expected_data_type]
    )


def test_resolve_sql_type() -> None:
    assert resolve_sql_type("boolean") == BooleanTypeClass()
    assert resolve_sql_type("varchar") == StringTypeClass()
