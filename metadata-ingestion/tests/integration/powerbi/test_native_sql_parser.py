from typing import List

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.m_query import native_sql_parser


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(run_id="test", pipeline_name="test")


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


def test_parse_custom_sql_multi_statement_extracts_all_sources(ctx):
    # Multi-statement SQL (Block contains N statements) should extract lineage from
    # all SELECT statements that reference real source tables — not just the first or last.
    # Semicolons separate statements as they appear in real M-Query SQL.
    query = "SELECT col FROM dbo.TableA WHERE x = 1;\nSELECT col FROM dbo.TableB WHERE y = 2"
    result = native_sql_parser.parse_custom_sql(
        ctx=ctx,
        query=query,
        platform="mssql",
        database="TestDB",
        schema="dbo",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    assert result.debug_info is None or result.debug_info.table_error is None
    urns = {urn.lower() for urn in result.in_tables}
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)


def test_remove_tsql_control_statements_leading_semicolon_before_with():
    # Leading semicolon before CTE (;WITH ...) is a T-SQL defensive pattern to ensure
    # the previous statement is terminated. It must be stripped so sqlglot can parse the CTE.
    query = ";WITH cte AS (SELECT col FROM dbo.SourceTable)\nSELECT col FROM cte"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert not actual.startswith(";")
    assert "WITH cte AS" in actual
    assert "dbo.SourceTable" in actual


def test_parse_custom_sql_blank_line_separated_selects_extracts_all_sources(ctx):
    # Multiple SELECTs separated only by blank lines — pattern that appears after
    # DROP TABLE IF EXISTS is stripped from between statements (no semicolons remain).
    # All source tables must still be extracted.
    query = (
        "SELECT col FROM dbo.TableA WHERE x = 1\n"
        "\n"
        "SELECT col FROM dbo.TableB WHERE y = 2"
    )
    result = native_sql_parser.parse_custom_sql(
        ctx=ctx,
        query=query,
        platform="mssql",
        database="TestDB",
        schema="dbo",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    assert result.debug_info is None or result.debug_info.table_error is None
    urns = {urn.lower() for urn in result.in_tables}
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)


def test_parse_custom_sql_select_then_union_extracts_all_sources(ctx):
    # SELECT ...;\nSELECT ... UNION SELECT ... produces ['Select', 'Union'] block error.
    # All source tables across both statements must be extracted.
    query = (
        "SELECT col FROM dbo.TableA WHERE x = 1;\n"
        "SELECT col FROM dbo.TableB\n"
        "UNION ALL\n"
        "SELECT col FROM dbo.TableC"
    )
    result = native_sql_parser.parse_custom_sql(
        ctx=ctx,
        query=query,
        platform="mssql",
        database="TestDB",
        schema="dbo",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    assert result.debug_info is None or result.debug_info.table_error is None
    urns = {urn.lower() for urn in result.in_tables}
    assert any("tablea" in urn for urn in urns)
    assert any("tableb" in urn for urn in urns)
    assert any("tablec" in urn for urn in urns)
