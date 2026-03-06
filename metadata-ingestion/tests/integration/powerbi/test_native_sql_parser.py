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


# --- preprocess_tsql tests ---
# preprocess_tsql inserts semicolons at statement boundaries.
# sqlglot's TSQL dialect natively handles DECLARE, WITH (NOLOCK), etc.


def test_preprocess_tsql_varchar_declare():
    """VARCHAR(100) type parens must not confuse the DECLARE boundary walker."""
    query = "DECLARE @q VARCHAR(100) = 'hello' SELECT col FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT col FROM t1" in result


def test_preprocess_tsql_nvarchar_max():
    query = "DECLARE @x NVARCHAR(MAX) = N'hello' SELECT col FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT col FROM t1" in result


def test_preprocess_tsql_decimal_type():
    query = "DECLARE @rate DECIMAL(10,2) = 0.05 SELECT col FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT col FROM t1" in result


def test_preprocess_tsql_declare_parenthesized_value():
    query = "DECLARE @d DATETIME = (SELECT TOP 1 col FROM t1 ORDER BY col DESC) SELECT * FROM t2"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM t2" in result


def test_preprocess_tsql_declare_no_value():
    query = "DECLARE @x INT SELECT * FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM t1" in result


def test_preprocess_tsql_declare_with_semicolon():
    """Already semicolon-terminated DECLARE should not get double semicolons."""
    query = "DECLARE @x INT = 5; SELECT * FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";;" not in result
    assert "SELECT * FROM t1" in result


def test_preprocess_tsql_declare_non_paren_value():
    query = (
        "DECLARE @date DATETIME = '2024-01-01' SELECT * FROM table1 WHERE date = @date"
    )
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM table1 WHERE date = @date" in result


def test_preprocess_tsql_multiple_declares():
    query = (
        "DECLARE @a INT = 1 "
        "DECLARE @b DATETIME = (SELECT TOP 1 col FROM t1) "
        "SELECT * FROM t2 WHERE a = @a"
    )
    result = native_sql_parser.preprocess_tsql(query)
    assert "SELECT * FROM t2 WHERE a = @a" in result
    # Each statement boundary should have a semicolon
    parts = [p.strip() for p in result.split(";") if p.strip()]
    assert len(parts) == 3


def test_preprocess_tsql_multi_variable_declare():
    query = "DECLARE @a INT = 1, @b VARCHAR(50) = 'test' SELECT * FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM t1" in result


def test_preprocess_tsql_declare_table_type():
    query = "DECLARE @t TABLE (id INT, name VARCHAR(50)) SELECT * FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM t1" in result


def test_preprocess_tsql_declare_then_merge():
    query = "DECLARE @x INT = 1 MERGE INTO t1 USING t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET t1.name = t2.name;"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "MERGE INTO t1" in result


def test_preprocess_tsql_declare_then_exec():
    query = "DECLARE @x INT = 1 EXEC sp_some_proc @x"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "EXEC sp_some_proc" in result


def test_preprocess_tsql_comment_with_keyword():
    """Comment containing SELECT between DECLARE and real SELECT should be handled."""
    query = "DECLARE @x INT = 1 -- SELECT fake\nSELECT * FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert "SELECT * FROM t1" in result


def test_preprocess_tsql_string_with_keyword():
    """String literal containing 'SELECT' in DECLARE value should not break early."""
    query = "DECLARE @q VARCHAR(100) = 'SELECT is a keyword'; SELECT col FROM t1"
    result = native_sql_parser.preprocess_tsql(query)
    assert "SELECT col FROM t1" in result


def test_preprocess_tsql_nolock_single_preserved():
    """Single WITH (NOLOCK) should be preserved unchanged."""
    query = "SELECT * FROM t1 WITH (NOLOCK)"
    result = native_sql_parser.preprocess_tsql(query)
    assert result == query


def test_preprocess_tsql_nolock_space_separated_gets_commas():
    """Space-separated hints get commas inserted for sqlglot compatibility."""
    query = "SELECT * FROM t1 WITH (NOLOCK NOWAIT) WHERE x = 1"
    result = native_sql_parser.preprocess_tsql(query)
    assert ", NOWAIT" in result


def test_preprocess_tsql_nolock_already_comma_separated():
    """Already comma-separated hints should not be doubled."""
    query = "SELECT * FROM t1 WITH (NOLOCK, NOWAIT) WHERE x = 1"
    result = native_sql_parser.preprocess_tsql(query)
    assert result == query


def test_preprocess_tsql_semicolon_insertion():
    query = "SELECT col INTO #temp FROM t1 WHERE x = 1 SELECT col2 FROM #temp"
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    parts = [p.strip() for p in result.split(";") if p.strip()]
    assert len(parts) == 2
    assert "INTO #temp" in parts[0]
    assert "SELECT col2" in parts[1]


def test_preprocess_tsql_semicolon_preserves_subquery():
    query = "SELECT col INTO #temp FROM (SELECT id FROM t1) sub SELECT col FROM #temp"
    result = native_sql_parser.preprocess_tsql(query)
    parts = [p.strip() for p in result.split(";") if p.strip()]
    assert len(parts) == 2
    assert "FROM (SELECT id FROM t1)" in parts[0]


def test_preprocess_tsql_combined():
    """DECLARE + hints + SELECT INTO all in one query."""
    query = (
        "DECLARE @x INT = 1 "
        "SELECT col INTO #tmp FROM t1 WITH (NOLOCK NOWAIT) WHERE id = @x "
        "SELECT col FROM #tmp"
    )
    result = native_sql_parser.preprocess_tsql(query)
    assert ";" in result
    assert ", NOWAIT" in result
    parts = [p.strip() for p in result.split(";") if p.strip()]
    assert len(parts) == 3


def test_preprocess_tsql_passthrough():
    """Simple single-statement query should pass through unchanged."""
    query = "SELECT * FROM t1 WHERE id = 1"
    result = native_sql_parser.preprocess_tsql(query)
    assert result == query


def test_preprocess_tsql_empty():
    assert native_sql_parser.preprocess_tsql("") == ""
    assert native_sql_parser.preprocess_tsql("   ") == ""
