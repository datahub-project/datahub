from typing import Dict

import pytest

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

# Regression tests for sqlglot's column resolver recursing on an unnest of an
# *unqualified* column when a referenced table is missing from the schema.
# sqlglot 30.12.0 guards the LATERAL FLATTEN path (Snowflake), so those shapes
# parse cleanly. The BigQuery UNNEST path is still unguarded, so DataHub skips
# column-level lineage for it before optimize(). The timeout turns a hang into
# a test failure instead of a stuck run.
pytestmark = pytest.mark.timeout(60, method="thread")

_SELECT = (
    "SELECT GET_PATH(f.value, 'id') AS obj_id, COUNT(*) AS cnt "
    "FROM my_db.raw_schema.events AS e, "
    "LATERAL FLATTEN(items) AS f "
    "WHERE CAST(GET_PATH(f.value, 'kind') AS VARCHAR) = 'X' "
    "GROUP BY obj_id"
)
_CREATE_VIEW = f"CREATE OR REPLACE VIEW my_db.analytics.usage_view AS {_SELECT}"


def test_risky_unnest_does_not_crash_when_base_table_unschemad() -> None:
    # CREATE VIEW shape that used to crash: the view target's schema is present
    # while the source table is absent. With the sqlglot 30.12.0 fix this parses
    # cleanly and keeps table-level lineage instead of segfaulting the process.
    resolver = SchemaResolver(platform="snowflake")
    resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.analytics.usage_view,PROD)",
        {"OBJ_ID": "VARIANT", "CNT": "NUMBER(18,0)"},
    )
    result = sqlglot_lineage(
        _CREATE_VIEW,
        schema_resolver=resolver,
        default_db="my_db",
        default_schema="analytics",
    )
    assert any("raw_schema.events" in t.lower() for t in result.in_tables), (
        result.in_tables
    )
    assert any("usage_view" in t.lower() for t in result.out_tables), result.out_tables
    assert result.debug_info.table_error is None
    assert result.debug_info.column_error is None


def test_risky_unnest_does_not_crash_when_flatten_column_missing() -> None:
    # The flatten's base table IS schema'd, but the flattened column (`items`) is
    # absent from that schema -- e.g. a semi-structured VARIANT/array column that
    # was not surfaced. This is the shape that made real Snowflake FLATTEN()
    # statements hard-crash even when the base table was otherwise schema'd. It
    # now parses cleanly and keeps table-level lineage.
    resolver = SchemaResolver(platform="snowflake")
    resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.raw_schema.events,PROD)",
        {"OTHER_COL": "STRING"},  # note: no `items` column
    )
    result = sqlglot_lineage(
        _SELECT,
        schema_resolver=resolver,
        default_db="my_db",
        default_schema="raw_schema",
    )
    assert any("raw_schema.events" in t.lower() for t in result.in_tables), (
        result.in_tables
    )
    assert result.debug_info.table_error is None
    assert result.debug_info.column_error is None


# BigQuery UNNEST of an unqualified column with a referenced table missing from
# the schema: column-level lineage must be skipped before optimize().
_BQ_UNNEST = (
    "CREATE OR REPLACE TABLE my_project.my_dataset.out_table AS "
    "SELECT k.col_a, item AS col_b "
    "FROM my_project.my_dataset.known_table AS k "
    "JOIN my_project.my_dataset.unknown_table AS u ON k.col_a = u.col_a, "
    "UNNEST(items) AS item"
)


def _bq_resolver(known_schema: Dict[str, str]) -> SchemaResolver:
    resolver = SchemaResolver(platform="bigquery")
    resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.known_table,PROD)",
        known_schema,
    )
    return resolver


def test_bigquery_unnest_over_unschemad_table_skips_column_lineage() -> None:
    result = sqlglot_lineage(
        _BQ_UNNEST,
        schema_resolver=_bq_resolver({"col_a": "INT64", "col_c": "STRING"}),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert any("unknown_table" in t for t in result.in_tables), result.in_tables
    assert any("out_table" in t for t in result.out_tables), result.out_tables
    assert result.debug_info.table_error is None
    assert result.debug_info.column_error is not None
    assert "UNNEST" in str(result.debug_info.column_error)


def test_bigquery_unnest_with_qualified_column_keeps_column_lineage() -> None:
    # Same unknown table as above, but the unnested column is qualified, so the
    # unguarded resolver path is not taken.
    result = sqlglot_lineage(
        "SELECT k.col_a, item "
        "FROM my_project.my_dataset.known_table AS k "
        "JOIN my_project.my_dataset.unknown_table AS u ON k.col_a = u.col_a, "
        "UNNEST(u.items) AS item",
        schema_resolver=_bq_resolver({"col_a": "INT64"}),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert any("unknown_table" in t for t in result.in_tables), result.in_tables
    assert result.debug_info.column_error is None


def test_bigquery_unnest_over_cte_keeps_column_lineage() -> None:
    # A CTE reference is not a table missing from the schema.
    result = sqlglot_lineage(
        "WITH c AS (SELECT col_a, items FROM my_project.my_dataset.known_table) "
        "SELECT col_a, item FROM c, UNNEST(items) AS item",
        schema_resolver=_bq_resolver({"col_a": "INT64", "items": "ARRAY<STRING>"}),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert result.debug_info.column_error is None
    assert result.column_lineage


def test_non_bigquery_unnest_keeps_column_lineage() -> None:
    resolver = SchemaResolver(platform="trino")
    resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:trino,my_db.my_schema.known_table,PROD)",
        {"col_a": "INTEGER", "items": "ARRAY(VARCHAR)"},
    )
    result = sqlglot_lineage(
        "SELECT k.col_a, t.item "
        "FROM my_db.my_schema.known_table AS k "
        "JOIN my_db.my_schema.unknown_table AS u ON k.col_a = u.col_a "
        "CROSS JOIN UNNEST(items) AS t(item)",
        schema_resolver=resolver,
        default_db="my_db",
        default_schema="my_schema",
    )
    assert result.debug_info.column_error is None
