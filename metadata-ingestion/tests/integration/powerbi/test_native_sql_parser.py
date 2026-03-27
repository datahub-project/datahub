from typing import List

from datahub.ingestion.source.powerbi.m_query import native_sql_parser


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


def test_remove_tsql_control_statements_select_into_global_temp():
    # SELECT … INTO ##global_temp — double-hash global temp table must also be stripped
    query = "SELECT col\nINTO ##GlobalTemp\nFROM dbo.SourceTable"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert "##GlobalTemp" not in actual
    assert "FROM dbo.SourceTable" in actual


def test_remove_tsql_control_statements_only_noise_returns_empty():
    # A query consisting entirely of T-SQL noise with no SELECT should return empty string
    query = "USE SampleDB\nGO\nSET NOCOUNT ON\n"
    actual = native_sql_parser.remove_tsql_control_statements(query)
    assert actual == ""


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
