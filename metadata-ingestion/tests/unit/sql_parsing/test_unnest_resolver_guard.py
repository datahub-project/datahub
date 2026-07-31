from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

# A LATERAL FLATTEN of an *unqualified* column (`items`) whose base table
# (`my_db.raw_schema.events`) is not fully described by the schema. On
# sqlglot >= 30.7.0 through 30.10.0 this triggered an infinite recursion in the
# column resolver that SIGSEGV'd the compiled sqlglot[c] build and killed the
# whole ingest process. sqlglot 30.12.0 fixes the resolver, so these shapes now
# parse cleanly (table-level lineage retained) instead of crashing. These are
# regression tests for that bump.
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
