import sqlglot

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    _statement_risks_unnest_resolver_recursion,
    sqlglot_lineage,
)

# A LATERAL FLATTEN of an *unqualified* column (`items`) whose base table
# (`my_db.raw_schema.events`) is not in the schema. With a schema otherwise
# present, this triggers an infinite recursion in sqlglot's resolver (>= 30.7.0)
# that SIGSEGVs the compiled sqlglot[c] build. See the guard for details.
_SELECT = (
    "SELECT GET_PATH(f.value, 'id') AS obj_id, COUNT(*) AS cnt "
    "FROM my_db.raw_schema.events AS e, "
    "LATERAL FLATTEN(items) AS f "
    "WHERE CAST(GET_PATH(f.value, 'kind') AS VARCHAR) = 'X' "
    "GROUP BY obj_id"
)
_CREATE_VIEW = f"CREATE OR REPLACE VIEW my_db.analytics.usage_view AS {_SELECT}"


def _parse(sql: str) -> sqlglot.exp.Expression:
    statement = sqlglot.parse_one(sql, dialect="snowflake")
    assert isinstance(statement, sqlglot.exp.Expression)
    return statement


def _schema(mapping: dict) -> sqlglot.MappingSchema:
    return sqlglot.MappingSchema(mapping, dialect="snowflake", normalize=False)


def test_detector_flags_unqualified_unnest_over_unschemad_table() -> None:
    schema = _schema({"MY_DB": {"ANALYTICS": {"USAGE_VIEW": {"OBJ_ID": "VARIANT"}}}})
    assert _statement_risks_unnest_resolver_recursion(_parse(_SELECT), schema) is True


def test_detector_flags_unqualified_unnest_when_flatten_column_missing() -> None:
    # Regression: the flatten's base table IS schema'd, but the
    # flattened column (`items`) is absent from that schema -- e.g. a
    # semi-structured VARIANT/array column that was not surfaced. The resolver
    # still cannot type the unqualified column, so it hits the same recursion.
    # The original guard only checked table presence and missed this.
    schema = _schema({"my_db": {"raw_schema": {"events": {"other_col": "STRING"}}}})
    assert _statement_risks_unnest_resolver_recursion(_parse(_SELECT), schema) is True


def test_detector_allows_safe_cases() -> None:
    schema = _schema({"MY_DB": {"ANALYTICS": {"USAGE_VIEW": {"OBJ_ID": "VARIANT"}}}})
    # qualified flatten column -> safe
    qualified = _parse(_SELECT.replace("FLATTEN(items)", "FLATTEN(e.items)"))
    assert _statement_risks_unnest_resolver_recursion(qualified, schema) is False

    # no schema at all -> safe
    assert (
        _statement_risks_unnest_resolver_recursion(_parse(_SELECT), _schema({}))
        is False
    )

    # base table fully schema'd (case matching the statement) -> safe
    base_schema = _schema({"my_db": {"raw_schema": {"events": {"items": "ARRAY"}}}})
    assert (
        _statement_risks_unnest_resolver_recursion(_parse(_SELECT), base_schema)
        is False
    )

    # ordinary query, no unnest -> safe
    plain = _parse("SELECT a, b FROM db.s.t")
    assert _statement_risks_unnest_resolver_recursion(plain, schema) is False


def test_risky_unnest_skips_cll_without_crashing() -> None:
    # End-to-end on the CREATE VIEW shape that crashes: the view target's schema
    # is present while the source table is not. The guard must keep table-level
    # lineage and report a column error instead of crashing the process (which it
    # otherwise would on the compiled sqlglot[c] build).
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
    assert "LATERAL FLATTEN" in str(result.debug_info.column_error)


def test_risky_unnest_skips_cll_when_flatten_column_missing() -> None:
    # Regression: end-to-end shape where the flatten's source table
    # is present in the schema but the flattened column itself is absent. Without
    # the fix this recurses -- a catchable RecursionError on pure-python
    # sqlglot, but an uncatchable SIGSEGV that kills ingestion on sqlglot[c].
    # The guard must skip CLL and keep table-level lineage instead.
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
    assert "LATERAL FLATTEN" in str(result.debug_info.column_error)
