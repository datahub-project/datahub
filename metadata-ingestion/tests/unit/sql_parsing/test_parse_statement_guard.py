import sqlglot

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage
from datahub.sql_parsing.sqlglot_utils import (
    StatementTooComplexError,
    _statement_exceeds_depth_limit,
    parse_statement,
)

_SNOWFLAKE = sqlglot.Dialect.get_or_raise("snowflake")


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
    # without recursing and without crashing - the exact scenario that SIGSEGVs
    # the ingestion process today.
    expr: sqlglot.exp.Expression = sqlglot.exp.column("a")
    for _ in range(20000):
        expr = sqlglot.exp.Paren(this=expr)

    assert _statement_exceeds_depth_limit(expr, max_depth=600) is True


def test_parse_statement_rejects_overlong_sql(monkeypatch) -> None:
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_utils.get_sql_parse_max_statement_length",
        lambda: 50,
    )
    long_sql = "SELECT " + ", ".join(f"col{i}" for i in range(100)) + " FROM t"
    try:
        parse_statement(long_sql, dialect=_SNOWFLAKE)
        raise AssertionError("expected StatementTooComplexError")
    except StatementTooComplexError:
        pass


def test_parse_statement_rejects_too_deep_sql(monkeypatch) -> None:
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_utils.get_sql_parse_max_ast_depth",
        lambda: 2,
    )
    sql = "SELECT a FROM (SELECT a FROM (SELECT a FROM (SELECT a FROM t)))"
    try:
        parse_statement(sql, dialect=_SNOWFLAKE)
        raise AssertionError("expected StatementTooComplexError")
    except StatementTooComplexError:
        pass


def test_deep_statement_skipped_gracefully_end_to_end(monkeypatch) -> None:
    # A deep statement must not crash the pipeline: the guard in parse_statement
    # makes sqlglot_lineage record a (recoverable) error instead of a SIGSEGV.
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_utils.get_sql_parse_max_ast_depth",
        lambda: 2,
    )

    schema_resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    result = sqlglot_lineage(
        "SELECT a FROM (SELECT a FROM (SELECT a FROM db.public.t))",
        schema_resolver=schema_resolver,
        default_db="db",
        default_schema="public",
    )

    assert result.debug_info.table_error is not None
