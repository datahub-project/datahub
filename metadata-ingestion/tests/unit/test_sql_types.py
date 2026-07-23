import pytest

from datahub.ingestion.source.sql.sql_types import (
    ATHENA_SQL_TYPES_MAP,
    NEO4J_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    TRINO_SQL_TYPES_MAP,
    resolve_athena_modified_type,
    resolve_snowflake_modified_type,
    resolve_sql_type,
    resolve_trino_modified_type,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
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


@pytest.mark.parametrize(
    "neo4j_type, expected_class",
    [
        ("boolean", BooleanTypeClass),
        ("date", DateTypeClass),
        ("duration", TimeTypeClass),
        ("float", NumberTypeClass),
        ("integer", NumberTypeClass),
        ("list", ArrayTypeClass),
        ("local_date_time", TimeTypeClass),
        ("local_time", TimeTypeClass),
        ("point", StringTypeClass),
        ("string", StringTypeClass),
        ("zoned_date_time", TimeTypeClass),
        ("zoned_time", TimeTypeClass),
        ("node", StringTypeClass),
        ("relationship", StringTypeClass),
    ],
)
def test_neo4j_type_mappings(neo4j_type, expected_class):
    """Test that Neo4j types are correctly mapped to DataHub types."""
    assert NEO4J_TYPES_MAP[neo4j_type] == expected_class
    resolved_type = resolve_sql_type(neo4j_type)
    assert isinstance(resolved_type, expected_class)


def test_type_conflicts_across_platforms():
    """
    This test exposes that the comment 'However, the types don't ever conflict, so the merged mapping is fine.'
    is actually incorrect - there ARE conflicts. This test documents the known conflicts so we're aware of them.
    """
    from datahub.ingestion.source.sql.sql_types import (
        ATHENA_SQL_TYPES_MAP,
        BIGQUERY_TYPES_MAP,
        NEO4J_TYPES_MAP,
        POSTGRES_TYPES_MAP,
        SNOWFLAKE_TYPES_MAP,
        SPARK_SQL_TYPES_MAP,
        TRINO_SQL_TYPES_MAP,
        VERTICA_SQL_TYPES_MAP,
    )

    all_mappings = [
        ("POSTGRES", POSTGRES_TYPES_MAP),
        ("SNOWFLAKE", SNOWFLAKE_TYPES_MAP),
        ("BIGQUERY", BIGQUERY_TYPES_MAP),
        ("SPARK", SPARK_SQL_TYPES_MAP),
        ("TRINO", TRINO_SQL_TYPES_MAP),
        ("ATHENA", ATHENA_SQL_TYPES_MAP),
        ("VERTICA", VERTICA_SQL_TYPES_MAP),
        ("NEO4J", NEO4J_TYPES_MAP),
    ]

    conflicts = []
    all_type_mappings: dict[str, tuple[str, type]] = {}

    for platform_name, mapping in all_mappings:
        for type_name, type_class in mapping.items():
            if type_name in all_type_mappings:
                existing_platform, existing_class = all_type_mappings[type_name]
                if existing_class != type_class:
                    conflicts.append(
                        f"Type '{type_name}' conflicts: {existing_platform}={existing_class} vs {platform_name}={type_class}"
                    )
            else:
                all_type_mappings[type_name] = (platform_name, type_class)

    # Known conflicts as of the current implementation:
    expected_conflicts = {
        "Type 'map' conflicts: SPARK=<class 'datahub.metadata._internal_schema_classes.RecordTypeClass'> vs TRINO=<class 'datahub.metadata._internal_schema_classes.MapTypeClass'>",
        "Type 'map' conflicts: SPARK=<class 'datahub.metadata._internal_schema_classes.RecordTypeClass'> vs ATHENA=<class 'datahub.metadata._internal_schema_classes.MapTypeClass'>",
        "Type 'interval' conflicts: POSTGRES=None vs VERTICA=<class 'datahub.metadata._internal_schema_classes.TimeTypeClass'>",
        "Type 'point' conflicts: POSTGRES=None vs NEO4J=<class 'datahub.metadata._internal_schema_classes.StringTypeClass'>",
    }

    # Convert conflicts to set for comparison
    actual_conflicts = set(conflicts)

    # This test documents the known conflicts - if new conflicts are introduced or existing ones are resolved,
    # the test will fail and require updating
    assert actual_conflicts == expected_conflicts, (
        f"Type conflict changes detected!\n"
        f"Expected conflicts: {expected_conflicts}\n"
        f"Actual conflicts: {actual_conflicts}\n"
        f"New conflicts: {actual_conflicts - expected_conflicts}\n"
        f"Resolved conflicts: {expected_conflicts - actual_conflicts}"
    )
