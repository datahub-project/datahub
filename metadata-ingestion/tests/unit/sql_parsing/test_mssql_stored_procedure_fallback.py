"""
Tests for MSSQL stored procedure fallback parser with TRY/CATCH blocks.

These tests validate the fallback parsing mechanism that extracts lineage from
stored procedures containing unsupported control flow syntax (TRY/CATCH, etc.).
"""

import pathlib

import pytest

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage
from datahub.testing.check_sql_parser_result import assert_sql_result

RESOURCE_DIR = pathlib.Path(__file__).parent / "goldens"


@pytest.fixture(scope="function", autouse=True)
def _disable_cooperative_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable cooperative timeout for easier debugging."""
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.SQL_LINEAGE_TIMEOUT_ENABLED", False
    )


# ==============================================================================
# INTEGRATION TESTS WITH GOLDEN FILES (3 critical tests)
# ==============================================================================


def test_mssql_procedure_with_cte_in_try_catch() -> None:
    """Test CTE bracket bug fix - procedure with CTE inside TRY/CATCH.

    This is a critical regression test for the CTE bracket bug where
    split_statements incorrectly continued parsing after INSERT statements
    ending with ')' in WHERE clauses.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessDataWithCTE
AS
BEGIN
    BEGIN TRY
        WITH RankedData AS (
            SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn
            FROM source_table
        )
        INSERT INTO target_table (id, name)
        SELECT id, name
        FROM RankedData
        WHERE rn <= 100;
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_cte_in_try_catch.json",
    )


def test_mssql_procedure_complex_update_with_case() -> None:
    """Test complex column lineage extraction with CASE expressions.

    This verifies that column lineage correctly handles complex expressions
    with CASE statements and bitwise operations.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateReturns
AS
BEGIN
    UPDATE dst
    SET
        return_month_1 = CASE WHEN src.update_map & 1 = 1 THEN NULL ELSE dst.return_month_1 END,
        return_month_2 = CASE WHEN src.update_map & 2 = 2 THEN NULL ELSE dst.return_month_2 END,
        return_month_3 = CASE WHEN src.update_map & 4 = 4 THEN NULL ELSE dst.return_month_3 END,
        return_month_4 = CASE WHEN src.update_map & 8 = 8 THEN NULL ELSE dst.return_month_4 END,
        return_month_5 = CASE WHEN src.update_map & 16 = 16 THEN NULL ELSE dst.return_month_5 END,
        return_month_6 = CASE WHEN src.update_map & 32 = 32 THEN NULL ELSE dst.return_month_6 END
    FROM TargetDB.dbo.returns dst
    INNER JOIN SourceDB.dbo.staging_returns src
        ON dst.id = src.id;
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_complex_update_case.json",
    )


def test_mssql_procedure_with_try_catch_multiple_dml() -> None:
    """Test procedure with multiple DML statements and multiple output tables.

    This verifies that procedures with multiple INSERT/UPDATE/DELETE statements
    correctly generate lineage for all output tables.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessMetrics
@IsSnapshot VARCHAR(5) = 'NO'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Delete records marked for deletion
        DELETE dst
        FROM TargetDB.dbo.metrics dst
        INNER JOIN SourceDB.dbo.staging_metrics src
          ON dst.metric_id = src.metric_id
        WHERE  src.action_type = 'DELETE';

        -- Update existing records
        UPDATE dst
           SET  metric_date = src.metric_date,
                metric_value = src.metric_value
        FROM TargetDB.dbo.metrics dst
        INNER JOIN SourceDB.dbo.staging_metrics src
          ON dst.metric_id = src.metric_id
         WHERE  src.action_type IS NULL;

        -- Insert new records
        INSERT INTO TargetDB.dbo.metrics
            (metric_id, metric_date, metric_value)
        SELECT
            src.metric_id,
            src.metric_date,
            src.metric_value
        FROM SourceDB.dbo.staging_metrics src
        LEFT JOIN TargetDB.dbo.metrics dst
          ON dst.metric_id = src.metric_id
        WHERE dst.metric_id IS NULL;

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000);
        SELECT @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_try_catch_multiple_dml.json",
    )


# ==============================================================================
# UNIT TESTS (no golden files, simple assertions)
# ==============================================================================


def test_mssql_procedure_with_try_catch_basic() -> None:
    """Test basic stored procedure with TRY/CATCH block containing simple DML."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.UpdateCustomer
    @CustomerId INT,
    @CustomerName NVARCHAR(100)
AS
BEGIN
    BEGIN TRY
        UPDATE customers
        SET name = @CustomerName
        WHERE id = @CustomerId;
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.in_tables) == 1
    assert len(result.out_tables) == 1
    assert "customers" in result.out_tables[0]


def test_mssql_procedure_with_nested_control_flow() -> None:
    """Test nested control flow structures (TRY/CATCH inside IF)."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.ComplexFlow
    @Mode INT
AS
BEGIN
    IF @Mode = 1
    BEGIN
        BEGIN TRY
            INSERT INTO logs SELECT message FROM source;
        END TRY
        BEGIN CATCH
            INSERT INTO errors SELECT error_msg FROM error_source;
        END CATCH
    END
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.out_tables) >= 1  # At least one output table


def test_mssql_procedure_without_try_catch() -> None:
    """Test simple procedure without TRY/CATCH (still uses fallback due to BEGIN/END)."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.SimpleProc
AS
BEGIN
    INSERT INTO target SELECT * FROM source;
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.in_tables) == 1
    assert len(result.out_tables) == 1
    assert "source" in result.in_tables[0]
    assert "target" in result.out_tables[0]


def test_mssql_procedure_with_merge() -> None:
    """Test MERGE statement in stored procedure."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.MergeData
AS
BEGIN
    MERGE INTO target t
    USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.value = s.value
    WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.in_tables) == 1
    assert len(result.out_tables) == 1


def test_mssql_procedure_with_select_into() -> None:
    """Test SELECT INTO statement."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.CreateTemp
AS
BEGIN
    SELECT * INTO #temp FROM source WHERE id > 100;
    INSERT INTO target SELECT * FROM #temp;
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.in_tables) >= 1
    assert len(result.out_tables) >= 1


def test_mssql_procedure_with_temp_table() -> None:
    """Test procedure with explicit temp table creation."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.UseTempTable
AS
BEGIN
    CREATE TABLE #staging (id INT, name VARCHAR(100));
    INSERT INTO #staging SELECT id, name FROM source;
    INSERT INTO target SELECT * FROM #staging;
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    # Temp tables should be filtered out of final lineage
    assert all("#" not in table for table in result.out_tables)


def test_mssql_procedure_simple_without_begin_end() -> None:
    """Test simple procedure without BEGIN/END."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.SimpleInsert
AS
    INSERT INTO target SELECT * FROM source;
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )
    assert len(result.in_tables) >= 1
    assert len(result.out_tables) >= 1


# ==============================================================================
# UNIT TESTS FOR DETECTION FUNCTIONS
# ==============================================================================


def test_is_stored_procedure_with_unsupported_syntax() -> None:
    """Unit test for the detection function."""
    from datahub.sql_parsing.sqlglot_lineage import (
        _is_stored_procedure_with_unsupported_syntax,
    )
    from datahub.sql_parsing.sqlglot_utils import get_dialect

    dialect = get_dialect("mssql")

    # Should detect TRY/CATCH
    assert _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS BEGIN TRY SELECT 1 END TRY BEGIN CATCH END CATCH",
        dialect,
    )

    # Should detect BEGIN/END (control flow)
    assert _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS BEGIN SELECT 1 END", dialect
    )

    # Should detect IF statements
    assert _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS IF @x > 0 SELECT 1", dialect
    )

    # Should NOT detect non-procedure statements
    assert not _is_stored_procedure_with_unsupported_syntax(
        "SELECT * FROM table", dialect
    )

    # Should detect BEGIN TRY
    assert _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS BEGIN BEGIN TRY SELECT 1 END TRY END", dialect
    )

    # Should detect END CATCH
    assert _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS BEGIN BEGIN CATCH SELECT 1 END CATCH END", dialect
    )

    # Should NOT detect MSSQL-specific syntax for non-MSSQL dialects
    postgres_dialect = get_dialect("postgres")
    assert not _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test AS BEGIN SELECT 1 END", postgres_dialect
    )

    # Should NOT detect MSSQL-specific syntax for other dialects
    mysql_dialect = get_dialect("mysql")
    assert not _is_stored_procedure_with_unsupported_syntax(
        "CREATE PROCEDURE test BEGIN SELECT 1 END", mysql_dialect
    )


def test_is_control_flow_statement_word_boundary() -> None:
    """Test that control flow detection uses word boundaries to avoid false positives."""
    from datahub.sql_parsing.sqlglot_lineage import _is_tsql_control_flow_statement

    # Should match actual control flow keywords
    assert _is_tsql_control_flow_statement("SET @VAR = 1")
    assert _is_tsql_control_flow_statement("BEGIN")
    assert _is_tsql_control_flow_statement("END")
    assert _is_tsql_control_flow_statement("IF @X > 0")
    assert _is_tsql_control_flow_statement("GO")
    assert _is_tsql_control_flow_statement("RETURN")
    assert _is_tsql_control_flow_statement("DECLARE @X INT")

    # Should NOT match when keyword is part of identifier
    assert not _is_tsql_control_flow_statement("SETVAR = 1")  # SET is prefix
    assert not _is_tsql_control_flow_statement("BEGINX")  # BEGIN is prefix
    assert not _is_tsql_control_flow_statement("ENDPOINT")  # END is prefix
    assert not _is_tsql_control_flow_statement("IFFY")  # IF is prefix
    assert not _is_tsql_control_flow_statement("GOAL_TABLE")  # GO is prefix
    assert not _is_tsql_control_flow_statement("RETURNVALUE")  # RETURN is prefix

    # Should match with various delimiters after keyword
    assert _is_tsql_control_flow_statement("SET;")
    assert _is_tsql_control_flow_statement("BEGIN\n")
    assert _is_tsql_control_flow_statement("IF(")


def test_contains_control_flow_keyword_word_boundary() -> None:
    """Test that keyword detection in SQL uses word boundaries to avoid false positives."""
    from datahub.sql_parsing.sqlglot_lineage import _contains_control_flow_keyword

    # Should match actual control flow keywords in SQL
    assert _contains_control_flow_keyword("CREATE PROCEDURE X AS BEGIN SELECT 1 END")
    assert _contains_control_flow_keyword("CREATE PROCEDURE X AS SET @VAR = 1")
    assert _contains_control_flow_keyword("CREATE PROCEDURE X AS IF @X > 0 SELECT 1")

    # Should NOT match when keyword appears as substring in table/column names
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT * FROM TREND_TABLE"
    )  # TREND contains END
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT * FROM SETTINGS_TABLE"
    )  # SETTINGS contains SET
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT GOAL FROM GOALS"
    )  # GOAL contains GO
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT * FROM LEGEND"
    )  # LEGEND contains END
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT RETURNVALUE FROM T"
    )  # RETURNVALUE contains RETURN
    assert not _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT IFFY FROM T"
    )  # IFFY contains IF

    # Should match keywords at word boundaries
    assert _contains_control_flow_keyword(
        "CREATE PROCEDURE X AS SELECT * FROM T; END"
    )  # END is separate word


def test_fallback_parser_all_statements_fail() -> None:
    """Test fallback parser when all statements fail to parse."""
    from datahub.sql_parsing.sqlglot_lineage import _parse_stored_procedure_fallback
    from datahub.sql_parsing.sqlglot_utils import get_dialect

    schema_resolver = SchemaResolver(platform="mssql")
    dialect = get_dialect("mssql")

    # SQL with only unparseable statements
    result = _parse_stored_procedure_fallback(
        sql="CREATE PROCEDURE X AS INVALID SYNTAX THAT CANNOT PARSE",
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema=None,
        dialect=dialect,
    )

    # Should return empty result
    assert len(result.in_tables) == 0
    assert len(result.out_tables) == 0


def test_fallback_parser_empty_sql() -> None:
    """Test fallback parser with empty SQL."""
    from datahub.sql_parsing.sqlglot_lineage import _parse_stored_procedure_fallback
    from datahub.sql_parsing.sqlglot_utils import get_dialect

    schema_resolver = SchemaResolver(platform="mssql")
    dialect = get_dialect("mssql")

    result = _parse_stored_procedure_fallback(
        sql="",
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema=None,
        dialect=dialect,
    )

    # Should return empty result
    assert len(result.in_tables) == 0
    assert len(result.out_tables) == 0


# ==============================================================================
# TESTS FOR MULTIPLE OUTPUTS (already have assertions)
# ==============================================================================


def test_procedure_with_multiple_output_tables_all_registered() -> None:
    """Test that procedure with multiple outputs registers all tables (not just first).

    This test reproduces the bug where only the first output table was registered
    due to `parsed.out_tables[0]` in sql_parsing_aggregator.py.
    """
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.MultiOutput
AS
BEGIN
    -- Insert into multiple tables
    INSERT INTO inventory (product_id, quantity)
    SELECT product_id, SUM(quantity)
    FROM staging
    GROUP BY product_id;

    INSERT INTO sales (product_id, total_sales)
    SELECT product_id, SUM(price * quantity)
    FROM staging
    GROUP BY product_id;

    -- This uses VALUES without SELECT, so has no upstream lineage
    -- Production behavior: Does not appear in outputs
    INSERT INTO audit_log (message) VALUES ('Completed multi-output procedure');
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )

    # Production behavior: Only finds 2 output tables (inventory, sales)
    # The audit_log INSERT uses VALUES without SELECT, so has no upstream lineage
    assert len(result.out_tables) == 2
    out_table_str = " ".join(result.out_tables)
    assert "inventory" in out_table_str
    assert "sales" in out_table_str


def test_procedure_with_four_cross_database_outputs() -> None:
    """Test procedure with 4 output tables across different databases."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.CrossDbMultiOutput
AS
BEGIN
    INSERT INTO TimeSeries.dbo.metrics SELECT * FROM Staging.dbo.source;
    INSERT INTO CurrentData.dbo.metrics SELECT * FROM Staging.dbo.source;
    -- These use VALUES without SELECT, so have no upstream lineage
    INSERT INTO Audit.dbo.log VALUES ('msg1');
    INSERT INTO Temp.dbo.cache VALUES ('msg2');
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )

    # Production behavior: Only finds 2 output tables
    # The Audit and Temp INSERTs use VALUES without SELECT, so have no upstream lineage
    assert len(result.out_tables) == 2
    out_table_names = [table.lower() for table in result.out_tables]
    assert any("timeseries" in name for name in out_table_names)
    assert any("currentdata" in name for name in out_table_names)


def test_procedure_single_output_still_works() -> None:
    """Test that procedures with single output still work correctly."""
    result = sqlglot_lineage(
        """
CREATE PROCEDURE dbo.SingleOutput
AS
BEGIN
    INSERT INTO target SELECT * FROM source;
END
""",
        schema_resolver=SchemaResolver(platform="mssql"),
    )

    assert len(result.in_tables) == 1
    assert len(result.out_tables) == 1
    assert "source" in result.in_tables[0]
    assert "target" in result.out_tables[0]
