from typing import List, Set

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.m_query import native_sql_parser


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(run_id="test", pipeline_name="test")


def _in_tables(ctx: PipelineContext, query: str, platform: str = "mssql") -> Set[str]:
    result = native_sql_parser.parse_custom_sql(
        ctx=ctx,
        query=query,
        platform=platform,
        database="TestDB",
        schema="dbo",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    return {urn.lower() for urn in result.in_tables}


def test_join():
    query: str = "select A.name from GSL_TEST_DB.PUBLIC.SALES_ANALYST as A inner join GSL_TEST_DB.PUBLIC.SALES_FORECAST as B on A.name = B.name where startswith(A.name, 'mo')"
    tables: List[str] = native_sql_parser.get_tables(query)

    assert len(tables) == 2
    assert tables[0] == "GSL_TEST_DB.PUBLIC.SALES_ANALYST"
    assert tables[1] == "GSL_TEST_DB.PUBLIC.SALES_FORECAST"


def test_simple_from():
    query: str = "SELECT#(lf)concat((UPPER(REPLACE(SELLER,'-',''))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,'-',''))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"

    tables: List[str] = native_sql_parser.get_tables(query)

    assert len(tables) == 1
    assert tables[0] == "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"


def test_drop_statement():
    expected: str = "SELECT#(lf)concat((UPPER(REPLACE(SELLER,'-',''))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,'-',''))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"

    query: str = "DROP TABLE IF EXISTS #table1; DROP TABLE IF EXISTS #table1,#table2; DROP TABLE IF EXISTS table1; DROP TABLE IF EXISTS table1, #table2;SELECT#(lf)concat((UPPER(REPLACE(SELLER,'-',''))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,'-',''))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"

    actual: str = native_sql_parser.remove_drop_statement(query)

    assert actual == expected


def test_remove_tsql_control_statements_clean_query_unchanged():
    # A plain SELECT should pass through without modification — regression guard
    query = "SELECT col FROM dbo.MyTable WHERE col = 1"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert actual == query


def test_remove_tsql_control_statements_use_bracketed():
    # USE [Database] (bracketed identifier) is standard in production T-SQL and must also be stripped
    query = "USE [SampleDB]\n\nSELECT col FROM dbo.MyTable"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert "USE [SampleDB]" not in actual
    assert "SELECT col FROM dbo.MyTable" in actual


def test_remove_tsql_control_statements_set():
    # SET NOCOUNT ON / OFF are T-SQL session options that break sqlglot;
    # also verifies semicolon-terminated form is handled
    query = "SET NOCOUNT ON;\nSET QUOTED_IDENTIFIER OFF\nSELECT col FROM dbo.MyTable"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert "SET NOCOUNT" not in actual
    assert "SET QUOTED_IDENTIFIER" not in actual
    assert "SELECT col FROM dbo.MyTable" in actual


def test_remove_tsql_control_statements_go():
    # GO is a T-SQL batch separator; verify case-insensitivity and multi-occurrence
    query = "SELECT col FROM dbo.TableA\ngo\nSELECT col FROM dbo.TableB\nGO"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    # No line should consist solely of GO (case-insensitive)
    assert not any(line.strip().upper() == "GO" for line in actual.splitlines())
    assert "SELECT col FROM dbo.TableA" in actual
    assert "SELECT col FROM dbo.TableB" in actual
    # The batch boundary is preserved as a real ';' terminator, not deleted into a
    # blank line — so downstream splitting never has to guess where statements end.
    assert ";" in actual


def test_remove_tsql_control_statements_select_into():
    # SELECT … INTO #temp: stripped in both single-line and multi-line forms.
    # INSERT INTO #temp: must never be stripped (confirmed present in real log data).
    # SELECT in one statement must not reach INSERT INTO in the next statement.
    single_line = "SELECT col INTO #TempA FROM dbo.SourceTable"
    assert "#TempA" not in native_sql_parser.remove_tsql_control_statements(single_line)

    multi_line = "SELECT col\nINTO ##GlobalTemp\nFROM dbo.SourceTable"
    assert "##GlobalTemp" not in native_sql_parser.remove_tsql_control_statements(
        multi_line
    )

    insert_into = "INSERT INTO #Output SELECT col FROM dbo.SourceTable"
    assert "INSERT INTO #Output" in native_sql_parser.remove_tsql_control_statements(
        insert_into
    )

    cross_statement = (
        "SELECT col FROM dbo.TableA\n\nINSERT INTO #Output SELECT col FROM dbo.TableB"
    )
    assert "INSERT INTO #Output" in native_sql_parser.remove_tsql_control_statements(
        cross_statement
    )


def test_tsql_cleanup_requires_special_chars_expansion_first():
    # USE is line-anchored — it only matches after #(lf) is expanded to a real newline.
    # This documents the required call order: remove_special_characters before
    # remove_tsql_control_statements (the ordering bug that was the root cause).
    query = "USE SampleDB#(lf)SELECT col FROM dbo.MyTable"
    query = native_sql_parser.remove_special_characters(query)
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert "USE SampleDB" not in actual
    assert "SELECT col FROM dbo.MyTable" in actual


def test_remove_tsql_control_statements_combined():
    # T-SQL script with USE, DROP, multiple SELECT INTO temp tables — mirrors the
    # pattern that caused "Invalid expression / Unexpected token" parse errors
    query = """\
USE SampleDB

DROP TABLE IF EXISTS #TempA

SELECT col = COUNT(DISTINCT id)
INTO #TempA
FROM dbo.SourceTable
WHERE status = 'active'

SELECT col = COUNT(DISTINCT id)
INTO #TempB
FROM dbo.SourceTable
WHERE status = 'inactive'
"""
    actual = native_sql_parser.remove_tsql_control_statements(query)

    assert "USE SampleDB" not in actual
    assert "DROP TABLE" not in actual
    assert "#TempA" not in actual
    assert "#TempB" not in actual
    # Source table lineage must be preserved in both SELECT statements
    assert actual.count("dbo.SourceTable") == 2


def test_remove_tsql_control_statements_leading_semicolon_before_with():
    # Leading semicolon before CTE (;WITH ...) is a T-SQL defensive pattern to ensure
    # the previous statement is terminated. It must be stripped so sqlglot can parse the CTE.
    query = ";WITH cte AS (SELECT col FROM dbo.SourceTable)\nSELECT col FROM cte"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert not actual.startswith(";")
    assert "WITH cte AS" in actual
    assert "dbo.SourceTable" in actual


def test_is_single_statement_classification():
    # CTE/UNION/plain SELECT (incl. ';' or '--' inside comments and strings) are one
    # statement; only ';'- and blank-line-separated statements are multi.
    single = [
        "WITH c AS (\n  SELECT id FROM dbo.Src\n)\n\nSELECT * FROM c",
        "SELECT a FROM dbo.A\nUNION ALL\n\nSELECT b FROM dbo.B",
        "/* c */ WITH c AS (SELECT x FROM dbo.Src)\nSELECT * FROM c",
        "SELECT 'a; b -- c' AS s FROM dbo.A",
    ]
    multi = [
        "SELECT a FROM dbo.A;\nSELECT b FROM dbo.B",
        "SELECT a FROM dbo.A\n\nSELECT b FROM dbo.B",
    ]
    assert all(native_sql_parser._is_single_statement(q, "mssql") for q in single)
    assert not any(native_sql_parser._is_single_statement(q, "mssql") for q in multi)


def test_is_single_statement_platform_without_dialect():
    # 'odbc' has no sqlglot dialect; get_dialect raises and we fall back to the default
    # dialect instead of crashing.
    assert native_sql_parser._is_single_statement("SELECT a FROM dbo.A", "odbc")


def test_parse_custom_sql_multi_statement_extracts_all_sources(ctx):
    # Multi-statement SQL: lineage must come from every SELECT, not just the first/last.
    urns = _in_tables(
        ctx,
        "SELECT col FROM dbo.TableA WHERE x = 1;\nSELECT col FROM dbo.TableB WHERE y = 2",
    )
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)


def test_parse_custom_sql_select_then_union_extracts_all_sources(ctx):
    # ';' then a UNION query: all source tables across both statements are extracted.
    urns = _in_tables(
        ctx,
        "SELECT col FROM dbo.TableA WHERE x = 1;\n"
        "SELECT col FROM dbo.TableB\nUNION ALL\nSELECT col FROM dbo.TableC",
    )
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)
    assert any("tablec" in urn for urn in urns)


def test_single_cte_does_not_leak_alias(ctx):
    # Reported bug: a single nested CTE with blank lines and a ';' in a comment must stay
    # one statement, else the CTE alias resolves as a real table and leaks as an upstream.
    query = (
        "WITH outer_cte AS (\n"
        "    WITH inner_cte AS (\n"
        "\n"
        "        SELECT id FROM dbo.SrcA\n"
        "    )\n"
        "    -- join here; note the ';' in this comment\n"
        "    SELECT i.id FROM inner_cte i JOIN dbo.SrcB b ON b.id = i.id\n"
        ")\n"
        "\n\n"
        "SELECT * FROM outer_cte"
    )
    urns = _in_tables(ctx, query)
    assert any("srca" in u for u in urns) and any("srcb" in u for u in urns)
    assert not any("outer_cte" in u or "inner_cte" in u for u in urns)


def test_multi_statement_with_cte_does_not_leak_alias(ctx):
    # A CTE in a ';'-separated multi-statement query must keep its closing SELECT, so
    # the alias is not split off into a real-table upstream.
    query = (
        "WITH my_cte AS (SELECT id FROM dbo.Src)\n"
        "SELECT * FROM my_cte;\n"
        "SELECT x FROM dbo.Other"
    )
    urns = _in_tables(ctx, query)
    assert any("src" in u for u in urns) and any("other" in u for u in urns)
    assert not any("my_cte" in u for u in urns)


def test_go_separated_cte_does_not_leak_alias(ctx):
    # GO between a CTE and the next statement is preserved as a ';' by the cleanup, so the
    # CTE (despite its blank-line formatting) is split off without leaking its alias.
    raw = (
        "WITH my_cte AS (\n  SELECT id FROM dbo.Src\n)\n\nSELECT * FROM my_cte\n"
        "GO\n"
        "SELECT x FROM dbo.Other"
    )
    urns = _in_tables(ctx, native_sql_parser.remove_tsql_control_statements(raw))
    assert any("src" in u for u in urns) and any("other" in u for u in urns)
    assert not any("my_cte" in u for u in urns)


def test_parse_custom_sql_subquery_with_blank_lines_not_split(ctx):
    # The old blank-line heuristic inserted ';' before the inner SELECT, breaking the
    # subquery. The new _is_single_statement path leaves it intact.
    query = (
        "SELECT outer_q.col FROM (\n"
        "\n"
        "    SELECT id AS col FROM dbo.Source\n"
        "\n"
        ") AS outer_q"
    )
    urns = _in_tables(ctx, query)
    assert any("source" in urn for urn in urns)
    assert not any("outer_q" in urn for urn in urns)


def test_parse_custom_sql_union_all_with_blank_lines(ctx):
    # UNION ALL with blank lines between branches is a single statement; the old
    # heuristic would have inserted ';' before the second branch.
    query = "SELECT col FROM dbo.TableA\n\nUNION ALL\n\nSELECT col FROM dbo.TableB"
    urns = _in_tables(ctx, query)
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)


def test_parse_custom_sql_multiple_ctes(ctx):
    # Multiple named CTEs (WITH a AS (...), b AS (...)) — comma between definitions
    # and blank lines around them must not be mistaken for statement boundaries.
    query = (
        "WITH active AS (\n"
        "    SELECT id, name FROM dbo.Users WHERE active = 1\n"
        "),\n"
        "\n"
        "orders AS (\n"
        "    SELECT user_id, COUNT(*) AS cnt FROM dbo.Orders GROUP BY user_id\n"
        ")\n"
        "\n"
        "SELECT a.name, o.cnt FROM active a JOIN orders o ON o.user_id = a.id"
    )
    urns = _in_tables(ctx, query)
    assert any("users" in urn for urn in urns)
    assert any("orders" in urn for urn in urns)
    assert not any("active" in urn for urn in urns)


def test_parse_custom_sql_semicolon_in_string_literal(ctx):
    # ';' inside a string literal must not route the query to the multi-statement path.
    # The old code checked `";" in normalized` which fired on this.
    query = (
        "SELECT id, 'status; pending' AS label\n"
        "FROM dbo.Tasks\n"
        "WHERE category = 'type; A'"
    )
    urns = _in_tables(ctx, query)
    assert any("tasks" in urn for urn in urns)


def test_remove_tsql_control_statements_multiple_go_collapse():
    # Multiple consecutive GO separators must collapse to a single ';'.
    query = "SELECT col FROM dbo.A\nGO\nGO\nSELECT col FROM dbo.B"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert actual.count(";") == 1
    assert "dbo.A" in actual and "dbo.B" in actual


def test_is_single_statement_none_platform():
    # None platform must not crash — falls back gracefully to the default dialect.
    assert native_sql_parser._is_single_statement("SELECT a FROM dbo.A", None)  # type: ignore[arg-type]
