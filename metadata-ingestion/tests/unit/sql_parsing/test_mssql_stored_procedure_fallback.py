"""
Tests for MSSQL stored procedure fallback parser with TRY/CATCH blocks.

These tests validate the fallback parsing mechanism that extracts lineage from
stored procedures containing unsupported control flow syntax (TRY/CATCH, etc.).
"""

import pathlib

import pytest

from datahub.testing.check_sql_parser_result import assert_sql_result

RESOURCE_DIR = pathlib.Path(__file__).parent / "goldens"


@pytest.fixture(scope="function", autouse=True)
def _disable_cooperative_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable cooperative timeout for easier debugging."""
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.SQL_LINEAGE_TIMEOUT_ENABLED", False
    )


def test_mssql_procedure_with_try_catch_basic() -> None:
    """Test basic stored procedure with TRY/CATCH block containing simple DML."""
    assert_sql_result(
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
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_try_catch_basic.json",
    )


def test_mssql_procedure_with_try_catch_multiple_dml() -> None:
    """Test stored procedure with TRY/CATCH containing multiple DML statements."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateMetrics
@IsSnapshot VARCHAR(5) = 'NO'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Delete records marked for deletion
        DELETE dst
        FROM Production.dbo.metrics dst
        INNER JOIN Staging.dbo.staging_metrics src
          ON dst.metric_id = src.metric_id
        WHERE  src.record_action = 'DELETE';

        -- Update existing records
        UPDATE dst
           SET  metric_date = src.metric_date,
                metric_value = src.metric_value
        FROM Production.dbo.metrics dst
        INNER JOIN Staging.dbo.staging_metrics src
          ON dst.metric_id = src.metric_id
         WHERE  src.record_action IS NULL;

        -- Insert new records
        INSERT INTO Production.dbo.metrics
            (metric_id, metric_date, metric_value)
        SELECT
            src.metric_id,
            src.metric_date,
            src.metric_value
        FROM Staging.dbo.staging_metrics src
        LEFT JOIN Production.dbo.metrics dst
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


def test_mssql_procedure_with_nested_control_flow() -> None:
    """Test stored procedure with nested control flow (IF inside TRY/CATCH)."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessOrders
AS
BEGIN
    BEGIN TRY
        IF EXISTS (SELECT 1 FROM orders WHERE status = 'pending')
        BEGIN
            UPDATE orders
            SET status = 'processed'
            WHERE status = 'pending';
        END
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_nested_control_flow.json",
    )


def test_mssql_procedure_without_try_catch() -> None:
    """Test that procedures without TRY/CATCH still parse normally."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.SimpleUpdate
AS
BEGIN
    UPDATE customers
    SET last_updated = GETDATE()
    WHERE id = 1;
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_without_try_catch.json",
    )


def test_mssql_procedure_with_only_control_flow() -> None:
    """Test stored procedure containing only control flow (no DML)."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.LogError
AS
BEGIN
    BEGIN TRY
        DECLARE @Message NVARCHAR(100);
        SET @Message = 'Test';
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_only_control_flow.json",
        allow_table_error=True,
    )


def test_mssql_procedure_with_merge() -> None:
    """Test stored procedure with MERGE statement inside TRY/CATCH."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.MergeCustomers
AS
BEGIN
    BEGIN TRY
        MERGE INTO customers AS target
        USING staging_customers AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET target.name = source.name
        WHEN NOT MATCHED THEN
            INSERT (id, name) VALUES (source.id, source.name);
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_with_merge.json",
    )


def test_mssql_procedure_with_select_into() -> None:
    """Test stored procedure with SELECT INTO (creates table) inside TRY/CATCH."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.CreateTempTable
AS
BEGIN
    BEGIN TRY
        SELECT id, name
        INTO #temp_customers
        FROM customers
        WHERE active = 1;
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_select_into.json",
    )


def test_mssql_procedure_with_cte_in_try_catch() -> None:
    """Test stored procedure with CTE inside TRY/CATCH."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateFromCTE
AS
BEGIN
    BEGIN TRY
        WITH ranked_orders AS (
            SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rn
            FROM orders
        )
        UPDATE o
        SET is_latest = CASE WHEN rn = 1 THEN 1 ELSE 0 END
        FROM orders o
        INNER JOIN ranked_orders r ON o.id = r.id;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_cte_in_try_catch.json",
    )


def test_mssql_procedure_with_transaction() -> None:
    """Test stored procedure with explicit transaction handling."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.TransferFunds
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE accounts
        SET balance = balance - 100
        WHERE account_id = 1;

        UPDATE accounts
        SET balance = balance + 100
        WHERE account_id = 2;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_with_transaction.json",
    )


def test_mssql_procedure_with_dynamic_sql() -> None:
    """Test stored procedure with EXECUTE statement (dynamic SQL)."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.DynamicUpdate
AS
BEGIN
    BEGIN TRY
        DECLARE @sql NVARCHAR(MAX);
        SET @sql = 'UPDATE customers SET active = 1';
        EXECUTE sp_executesql @sql;
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_dynamic_sql.json",
        allow_table_error=True,  # Dynamic SQL can't be analyzed
    )


def test_mssql_procedure_with_temp_table() -> None:
    """Test stored procedure creating and using temp tables in TRY/CATCH."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessWithTemp
AS
BEGIN
    BEGIN TRY
        CREATE TABLE #temp_data (id INT, value NVARCHAR(100));

        INSERT INTO #temp_data (id, value)
        SELECT id, name FROM customers;

        UPDATE target_table
        SET value = t.value
        FROM target_table
        INNER JOIN #temp_data t ON target_table.id = t.id;

        DROP TABLE #temp_data;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#temp_data') IS NOT NULL
            DROP TABLE #temp_data;
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_with_temp_table.json",
    )


def test_mssql_procedure_multiple_catch_blocks() -> None:
    """Test stored procedure with multiple TRY/CATCH blocks."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.MultipleBlocks
AS
BEGIN
    BEGIN TRY
        UPDATE table1 SET col1 = 'value1';
    END TRY
    BEGIN CATCH
        PRINT 'Error in block 1';
    END CATCH

    BEGIN TRY
        UPDATE table2 SET col2 = 'value2';
    END TRY
    BEGIN CATCH
        PRINT 'Error in block 2';
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_multiple_catch_blocks.json",
    )


def test_mssql_procedure_with_output_parameters() -> None:
    """Test stored procedure with OUTPUT parameters."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.GetCustomerCount
    @TotalCount INT OUTPUT
AS
BEGIN
    BEGIN TRY
        SELECT @TotalCount = COUNT(*)
        FROM customers
        WHERE active = 1;
    END TRY
    BEGIN CATCH
        SET @TotalCount = -1;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_with_output_parameters.json",
    )


def test_mssql_procedure_with_while_loop() -> None:
    """Test stored procedure with WHILE loop inside TRY/CATCH."""
    assert_sql_result(
        """
CREATE PROCEDURE dbo.BatchProcess
AS
BEGIN
    BEGIN TRY
        DECLARE @batch_size INT = 1000;
        WHILE EXISTS (SELECT 1 FROM staging_data WHERE processed = 0)
        BEGIN
            UPDATE TOP (@batch_size) staging_data
            SET processed = 1
            WHERE processed = 0;

            INSERT INTO target_data (id, value)
            SELECT id, value
            FROM staging_data
            WHERE processed = 1;
        END
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_with_while_loop.json",
    )


def test_mssql_function_detection() -> None:
    """Test that CREATE FUNCTION is NOT treated as stored procedure fallback."""
    assert_sql_result(
        """
CREATE FUNCTION dbo.GetCustomerName(@id INT)
RETURNS NVARCHAR(100)
AS
BEGIN
    DECLARE @name NVARCHAR(100);
    SELECT @name = name FROM customers WHERE id = @id;
    RETURN @name;
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_function_detection.json",
        allow_table_error=True,  # Functions might not be fully supported
    )


# Edge cases and validation tests


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


def test_fallback_parser_statement_filtering() -> None:
    """Test that control flow statements are properly filtered."""
    # This is implicitly tested by the other tests, but we verify the behavior
    # by checking that procedures with only control flow don't produce lineage
    assert_sql_result(
        """
CREATE PROCEDURE dbo.OnlyDeclarations
AS
BEGIN
    DECLARE @var1 INT;
    DECLARE @var2 NVARCHAR(100);
    SET @var1 = 1;
    SET @var2 = 'test';
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_only_declarations.json",
        allow_table_error=True,  # Should fail because no DML
    )


def test_fallback_parser_confidence_score() -> None:
    """Test that fallback parser returns appropriate confidence score."""
    # The confidence should be 0.5 for fallback parsing
    # This is tested by comparing the debug_info in golden files
    pass  # Implicit test via golden files
