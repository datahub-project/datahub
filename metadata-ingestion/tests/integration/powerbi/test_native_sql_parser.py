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


def test_drop_statement_at_end_of_string():
    query = "SELECT col FROM t1 DROP TABLE IF EXISTS #temp"
    result = native_sql_parser.remove_drop_statement(query)
    assert "DROP" not in result
    assert "SELECT col FROM t1" in result


def test_remove_table_hints():
    query = (
        "SELECT * FROM t1 WITH (NOLOCK) JOIN t2 WITH (NOLOCK NOWAIT) ON t1.id = t2.id"
    )
    result = native_sql_parser.remove_table_hints(query)
    assert "NOLOCK" not in result
    assert "NOWAIT" not in result
    assert "t1" in result and "t2" in result


def test_remove_table_hints_preserves_cte():
    query = "WITH cte AS (SELECT id FROM t1) SELECT * FROM cte"
    result = native_sql_parser.remove_table_hints(query)
    assert result == query


def test_remove_declare_simple():
    query = "Declare @var Datetime = (select top 1 col from t1 order by col desc) select * from t2"
    result = native_sql_parser.remove_declare_statement(query)
    assert "@var" not in result
    assert "Declare" not in result.lower()
    assert "select * from t2" in result


def test_remove_declare_with_block_comment():
    query = "/* Author: Test */ Declare @x int = (select 1) select col from t1"
    result = native_sql_parser.remove_declare_statement(query)
    assert "Declare" not in result
    assert "select col from t1" in result


def test_remove_declare_non_parenthesized_value():
    """Non-parenthesized DECLARE followed by SELECT should keep the SELECT."""
    query = (
        "DECLARE @date DATETIME = '2024-01-01' SELECT * FROM table1 WHERE date = @date"
    )
    result = native_sql_parser.remove_declare_statement(query)
    assert "DECLARE" not in result
    assert "@date DATETIME" not in result
    assert "SELECT * FROM table1 WHERE date = @date" in result


def test_remove_declare_non_parenthesized_no_value():
    """DECLARE without assignment followed by SELECT."""
    query = "DECLARE @x INT SELECT * FROM t1"
    result = native_sql_parser.remove_declare_statement(query)
    assert "DECLARE" not in result
    assert "SELECT * FROM t1" in result


def test_remove_declare_non_parenthesized_with_semicolon():
    """Semicolon-terminated DECLARE should still work."""
    query = "DECLARE @x INT = 5; SELECT * FROM t1"
    result = native_sql_parser.remove_declare_statement(query)
    assert "DECLARE" not in result
    assert "SELECT * FROM t1" in result


def test_remove_declare_multiple_mixed():
    """Multiple DECLAREs with mixed styles: parenthesized and non-parenthesized."""
    query = (
        "DECLARE @a INT = 1 "
        "DECLARE @b TABLE = (SELECT id FROM t1) "
        "SELECT * FROM t2 WHERE a = @a"
    )
    result = native_sql_parser.remove_declare_statement(query)
    assert "DECLARE" not in result
    assert "SELECT * FROM t2 WHERE a = @a" in result


def test_remove_declare_string_containing_select():
    """DECLARE value with 'SELECT' inside a string literal should not break early."""
    query = "DECLARE @q VARCHAR(100) = 'SELECT is a keyword'; SELECT col FROM t1"
    result = native_sql_parser.remove_declare_statement(query)
    assert "DECLARE" not in result
    assert "SELECT col FROM t1" in result


def test_insert_semicolons_for_select_into():
    query = "SELECT col INTO #temp FROM t1 WHERE x = 1 SELECT col2 FROM #temp"
    result = native_sql_parser.insert_semicolons_after_select_into(query)
    assert ";" in result
    parts = result.split(";")
    assert len(parts) == 2
    assert "INTO #temp" in parts[0]
    assert "SELECT col2" in parts[1].strip()


def test_insert_semicolons_no_temp_table():
    query = "SELECT col FROM t1 WHERE x = 1"
    result = native_sql_parser.insert_semicolons_after_select_into(query)
    assert result == query


def test_insert_semicolons_preserves_subquery_select():
    query = "SELECT col INTO #temp FROM (SELECT id FROM t1) sub SELECT col FROM #temp"
    result = native_sql_parser.insert_semicolons_after_select_into(query)
    # The SELECT inside the subquery should NOT get a semicolon
    parts = [p.strip() for p in result.split(";") if p.strip()]
    assert len(parts) == 2
    assert "FROM (SELECT id FROM t1)" in parts[0]
