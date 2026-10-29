"""AthenaPropertiesExtractor: parse failures, column-definition normalisation
and the partition-transform helpers.

The transform helpers are exercised with real sqlglot expression nodes rather
than through full CREATE TABLE statements: current sqlglot parses bucket()/
truncate() into dedicated node types, so the Anonymous/Identifier paths below
are only reached when sqlglot's parse output changes shape. The tests pin what
those paths must return if that happens.
"""

import pytest
from sqlglot.expressions import Anonymous, Column, Identifier, Literal, Year

from datahub.ingestion.source.sql.athena_properties_extractor import (
    AthenaPropertiesExtractionError,
    AthenaPropertiesExtractor,
)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t (id int",
        "this is not a create statement ((",
    ],
)
def test_unparseable_sql_raises_extraction_error(sql: str) -> None:
    with pytest.raises(AthenaPropertiesExtractionError):
        AthenaPropertiesExtractor.get_table_properties(sql)


def test_double_quoted_column_comment_is_normalised_to_single_quotes() -> None:
    line = AthenaPropertiesExtractor.format_column_definition(
        '  name string COMMENT "the user\'s name",'
    )
    assert line == " `name` string COMMENT 'the user''s name',"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "format_column_definition re-escapes every quote inside an already "
        "single-quoted comment, so the valid escape 'it''s' becomes 'it''''s' "
        "and the comment text gains a quote"
    ),
)
def test_already_escaped_single_quoted_comment_is_left_alone() -> None:
    line = AthenaPropertiesExtractor.format_column_definition(
        "  name string COMMENT 'it''s',"
    )
    assert line == " `name` string COMMENT 'it''s',"


def test_double_quoted_comment_survives_full_parse() -> None:
    """SHOW CREATE TABLE output with a double-quoted column comment must still
    parse, so partitions and the location are not lost for such tables."""
    sql = """CREATE TABLE t (
  id bigint COMMENT "primary id",
  name string
)
PARTITIONED BY (name)
LOCATION 's3://bucket/t/'"""
    info = AthenaPropertiesExtractor.get_table_properties(sql)
    assert [c.name for c in info.partition_info.simple_columns] == ["name"]
    assert info.table_properties.location == "s3://bucket/t/"


def test_transform_function_node_extracts_bucket_and_truncate_parameters() -> None:
    """bucket(n, col) and truncate(n, col) carry their parameter first; it must
    land on bucket_count / length respectively, never both."""
    bucket = Anonymous(
        this="bucket",
        expressions=[Literal.number(16), Column(this=Identifier(this="id"))],
    )
    column, transform = AthenaPropertiesExtractor._handle_transform_function(
        bucket, {"id": "BIGINT"}
    )
    assert (column.name, column.type) == ("id", "BIGINT")
    assert transform.type == "bucket"
    assert str(transform.bucket_count) == "16"
    assert transform.length is None

    truncate = Anonymous(
        this="truncate",
        expressions=[Literal.number(10), Column(this=Identifier(this="name"))],
    )
    _, transform = AthenaPropertiesExtractor._handle_transform_function(
        truncate, {"name": "TEXT"}
    )
    assert transform.type == "truncate"
    assert str(transform.length) == "10"
    assert transform.bucket_count is None


def test_transform_function_node_without_arguments_is_unknown() -> None:
    column, transform = AthenaPropertiesExtractor._handle_transform_function(
        Anonymous(this="bucket", expressions=[]), {}
    )
    assert column.name == "unknown"
    assert transform.type == "bucket"
    assert transform.bucket_count is None


def test_function_like_identifier_is_split_into_transform_and_column() -> None:
    column, transform = AthenaPropertiesExtractor._handle_function_expression(
        Identifier(this="day(`ts`)"), {"ts": "TIMESTAMP"}
    )
    assert (column.name, column.type) == ("ts", "TIMESTAMP")
    assert transform.type == "day"


def test_identifier_without_parentheses_is_kept_as_unknown_transform() -> None:
    column, transform = AthenaPropertiesExtractor._handle_function_expression(
        Identifier(this="dayts"), {}
    )
    assert column.name == "dayts"
    assert transform.type == "unknown"


def test_time_function_resolves_column_through_nested_or_flat_nodes() -> None:
    nested = Year(this=Column(this=Identifier(this="ts")))
    column, transform = AthenaPropertiesExtractor._handle_time_function(
        nested, {"ts": "TIMESTAMP"}
    )
    assert (column.name, column.type, transform.type) == ("ts", "TIMESTAMP", "year")

    flat = Year(this=Identifier(this="ts"))
    column, _ = AthenaPropertiesExtractor._handle_time_function(flat, {})
    assert column.name == "ts"
