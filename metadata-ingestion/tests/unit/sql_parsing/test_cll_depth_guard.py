import sqlglot

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    _statement_exceeds_depth_limit,
    sqlglot_lineage,
)


def test_depth_limit_shallow_statement_under_limit() -> None:
    expr = sqlglot.parse_one("SELECT a, b FROM t", dialect="snowflake")
    assert _statement_exceeds_depth_limit(expr, max_depth=100) is False


def test_depth_limit_detects_deep_nesting() -> None:
    sql = "SELECT a FROM (SELECT a FROM (SELECT a FROM (SELECT a FROM t)))"
    expr = sqlglot.parse_one(sql, dialect="snowflake")
    assert _statement_exceeds_depth_limit(expr, max_depth=2) is True
    assert _statement_exceeds_depth_limit(expr, max_depth=100) is False


def test_depth_check_is_iterative_on_pathologically_deep_ast() -> None:
    # Build an extremely deep AST *programmatically* (parsing a string this deep
    # could itself overflow sqlglot's native stack). The guard must measure depth
    # without recursing and without crashing - this is the exact scenario that
    # SIGSEGVs the ingestion process today.
    expr: sqlglot.exp.Expression = sqlglot.exp.column("a")
    for _ in range(20000):
        expr = sqlglot.exp.Paren(this=expr)

    assert _statement_exceeds_depth_limit(expr, max_depth=600) is True


def test_deep_statement_skips_cll_but_keeps_table_lineage(monkeypatch) -> None:
    # Force a tiny depth limit so a normal nested query trips the guard.
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.get_sql_cll_max_ast_depth",
        lambda: 2,
    )

    schema_resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    result = sqlglot_lineage(
        "SELECT a FROM (SELECT a FROM (SELECT a FROM db.public.t))",
        schema_resolver=schema_resolver,
        default_db="db",
        default_schema="public",
    )

    # The run must not crash: table-level lineage is preserved...
    assert result.in_tables == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)"
    ]
    # ...while column-level lineage is gracefully skipped and the reason recorded.
    assert result.column_lineage is None
    assert result.debug_info.column_error is not None
