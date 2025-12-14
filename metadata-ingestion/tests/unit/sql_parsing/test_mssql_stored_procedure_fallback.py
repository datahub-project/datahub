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
    """Test stored procedure with TRY/CATCH containing multiple DML statements.

    Note: This tests the raw parser output before alias filtering. The golden file
    contains 'dst' aliases in the lineage, which is expected. These aliases are
    filtered out later by the MSSQL source's _filter_procedure_lineage() method.
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


@pytest.mark.xfail(
    reason="CTE splitting is not yet supported by split_statements - the WITH clause gets split from the UPDATE",
    strict=True,
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
    from datahub.sql_parsing.schema_resolver import SchemaResolver
    from datahub.sql_parsing.sqlglot_lineage import _parse_stored_procedure_fallback
    from datahub.sql_parsing.sqlglot_utils import get_dialect

    schema_resolver = SchemaResolver(platform="mssql")
    dialect = get_dialect("mssql")

    # SQL with only unparseable statements
    result = _parse_stored_procedure_fallback(
        sql="CREATE PROCEDURE X AS INVALID SYNTAX THAT CANNOT PARSE",
        schema_resolver=schema_resolver,
        dialect=dialect,
        default_db=None,
        default_schema=None,
    )

    # Fallback parser returns result with empty in_tables when no valid DML
    assert result.in_tables == []


def test_fallback_parser_empty_sql() -> None:
    """Test fallback parser with empty SQL."""
    from datahub.sql_parsing.schema_resolver import SchemaResolver
    from datahub.sql_parsing.sqlglot_lineage import _parse_stored_procedure_fallback
    from datahub.sql_parsing.sqlglot_utils import get_dialect

    schema_resolver = SchemaResolver(platform="mssql")
    dialect = get_dialect("mssql")

    result = _parse_stored_procedure_fallback(
        sql="",
        schema_resolver=schema_resolver,
        dialect=dialect,
        default_db=None,
        default_schema=None,
    )

    # Should handle empty SQL gracefully with empty result
    assert result.in_tables == []


# Real-world test cases based on customer issues


def test_mssql_procedure_update_with_alias_pattern() -> None:
    """Test UPDATE dst FROM table dst pattern that caused phantom table references.

    This pattern is common in MSSQL and was causing 'Resolved 2 of 3 table schemas' errors.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateFromStaging
@IsSnapshot VARCHAR(5) = 'NO'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Update existing records using MSSQL-specific UPDATE alias syntax
        UPDATE dst
        SET metric_value = src.metric_value,
            metric_date = src.metric_date,
            last_updated = GETDATE()
        FROM TargetDB.dbo.metrics dst
        INNER JOIN SourceDB.dbo.staging_metrics src
          ON dst.metric_id = src.metric_id
        WHERE src.action_type IS NULL;

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_update_alias_pattern.json",
    )


def test_mssql_procedure_delete_with_alias_pattern() -> None:
    """Test DELETE dst FROM table dst pattern.

    Similar to UPDATE, this MSSQL-specific syntax was causing schema resolution issues.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.DeleteMarkedRecords
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Delete records marked for deletion using MSSQL alias syntax
        DELETE dst
        FROM TargetDB.dbo.customer_data dst
        INNER JOIN SourceDB.dbo.source_customer_data src WITH (TABLOCK)
          ON src.customer_id = dst.customer_id
         AND src.effective_date = dst.effective_date
        WHERE src.action_type = 'DELETE';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000);
        SELECT @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_delete_alias_pattern.json",
    )


def test_mssql_procedure_complex_update_with_case() -> None:
    """Test complex UPDATE with multiple CASE expressions.

    This pattern with many CASE WHEN expressions was common in customer logs.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateMonthlyReturns
AS
BEGIN
    BEGIN TRY
        UPDATE dst
        SET return_month_1 = CASE WHEN src.update_map & 1 = 1 THEN NULL ELSE dst.return_month_1 END,
            return_month_2 = CASE WHEN src.update_map & 2 = 2 THEN NULL ELSE dst.return_month_2 END,
            return_month_3 = CASE WHEN src.update_map & 4 = 4 THEN NULL ELSE dst.return_month_3 END,
            return_month_4 = CASE WHEN src.update_map & 8 = 8 THEN NULL ELSE dst.return_month_4 END,
            return_month_5 = CASE WHEN src.update_map & 16 = 16 THEN NULL ELSE dst.return_month_5 END,
            return_month_6 = CASE WHEN src.update_map & 32 = 32 THEN NULL ELSE dst.return_month_6 END
        FROM TargetDB.dbo.monthly_returns dst
        INNER JOIN SourceDB.dbo.source_returns src
          ON dst.asset_id = src.asset_id
         AND dst.period_id = src.period_id;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_complex_update_case.json",
    )


def test_mssql_procedure_multi_dml_pattern() -> None:
    """Test procedure with DELETE, UPDATE, and INSERT in sequence.

    This represents the common ETL pattern seen in customer logs where a procedure
    performs multiple DML operations. Should produce both inputs AND outputs.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.SyncDimensionTable
@IsSnapshot VARCHAR(5) = 'NO'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Delete obsolete records
        DELETE dst
        FROM TargetDB.dbo.dim_product dst
        INNER JOIN SourceDB.dbo.source_dim_product src
          ON dst.product_key = src.product_key
        WHERE src.action_type = 'DELETE';

        -- Update changed records
        UPDATE dst
        SET product_name = src.product_name,
            category = src.category,
            last_modified = GETDATE()
        FROM TargetDB.dbo.dim_product dst
        INNER JOIN SourceDB.dbo.source_dim_product src
          ON dst.product_key = src.product_key
        WHERE src.action_type = 'UPDATE';

        -- Insert new records
        INSERT INTO TargetDB.dbo.dim_product
            (product_key, product_name, category, last_modified)
        SELECT
            src.product_key,
            src.product_name,
            src.category,
            GETDATE()
        FROM SourceDB.dbo.source_dim_product src
        LEFT JOIN TargetDB.dbo.dim_product dst
          ON dst.product_key = src.product_key
        WHERE dst.product_key IS NULL
          AND src.action_type = 'INSERT';

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_multi_dml_pattern.json",
    )


def test_mssql_procedure_snapshot_cleanup_pattern() -> None:
    """Test snapshot cleanup pattern with conditional DELETE.

    This pattern uses LEFT JOIN with NULL checks for cleanup operations.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.CleanupSnapshotData
@IsSnapshot VARCHAR(10) = 'YES'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Delete records not in current snapshot
        DELETE dst
        FROM TargetDB.dbo.financial_metrics dst
        LEFT JOIN SourceDB.dbo.source_financial_metrics src WITH (TABLOCK)
          ON src.metric_id = dst.metric_id
         AND src.snapshot_date = dst.snapshot_date
        WHERE src.snapshot_date IS NULL
          AND src.metric_id IS NULL
          AND @IsSnapshot = 'YES';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorNumber INT;
        DECLARE @ErrorMessage NVARCHAR(4000);
        SELECT @ErrorNumber = ERROR_NUMBER(), @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_snapshot_cleanup.json",
    )


# Tests for specific error cases from test-user.log


def test_mssql_procedure_with_inline_comments() -> None:
    """Test stored procedure with inline SQL comments.

    From test-user.log: 'ParseError: No expression was parsed from "--insert"'
    Comments should be handled gracefully and not parsed as statements.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.AddDocumentId
    @p_DocumentId INT,
    @p_ColId INT,
    @p_Status VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        --insert into DocumentId table
        INSERT INTO CurrentData.dbo.DocumentId
            (DocumentId, ColId, Status, CreatedOn)
        VALUES
            (@p_DocumentId, @p_ColId, @p_Status, GETDATE());

        -- Update related records
        UPDATE CurrentData.dbo.RelatedTable
        SET LastModified = GETDATE()
        WHERE DocumentId = @p_DocumentId;

    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_inline_comments.json",
    )


def test_mssql_procedure_update_same_table_twice() -> None:
    """Test UPDATE pattern where same table appears in both UPDATE and FROM clauses.

    From test-user.log: 'OptimizeError: Alias already used: fundleveloperation'
    This is a common MSSQL pattern: UPDATE table SET ... FROM table INNER JOIN ...
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.UpdateFundHomePage
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        UPDATE dbo.FundLevelOperation
        SET HomePage = o.HomePage
        FROM dbo.FundLevelOperation
        INNER JOIN (
            SELECT a.FundId, c.HomePage
            FROM DataIndex.dbo.vw_FundId a
            INNER JOIN DataIndex.dbo.vw_FundCompany b
              ON a.FundId = b.FundId AND a.HedgeFund = 0 AND b.CompanyType = 15
            LEFT JOIN CurrentData.dbo.CompanyOperationXOI c
              ON b.CompanyId = c.CompanyId
        ) o ON dbo.FundLevelOperation.FundId = o.FundId;

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR
        / "test_mssql_procedure_update_same_table_twice.json",
    )


def test_mssql_procedure_error_variable_assignments() -> None:
    """Test stored procedure with variable assignments from ERROR_* functions.

    From test-user.log: SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY()
    caused 'Invalid expression / Unexpected token' errors.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.SafeUpdateWithErrorHandling
AS
BEGIN
    BEGIN TRY
        UPDATE CurrentData.dbo.Metrics
        SET ProcessedDate = GETDATE()
        WHERE Status = 'pending';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorNumber INT;
        DECLARE @ErrorState INT;
        DECLARE @ErrorLine INT;
        DECLARE @ErrorProcedure NVARCHAR(200);

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorNumber = ERROR_NUMBER(),
               @ErrorState = ERROR_STATE(),
               @ErrorLine = ERROR_LINE(),
               @ErrorProcedure = ERROR_PROCEDURE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR
        / "test_mssql_procedure_error_variable_assignments.json",
    )


def test_mssql_procedure_raiserror_statement() -> None:
    """Test stored procedure with RAISERROR statement in CATCH block.

    From test-user.log: Complex RAISERROR with multiple parameters was appearing
    in lineage extraction attempts.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessDataWithLogging
AS
BEGIN
    BEGIN TRY
        INSERT INTO TargetDB.dbo.fact_table (MetricId, Value, ProcessedDate)
        SELECT MetricId, Value, GETDATE()
        FROM SourceDB.dbo.source_metrics
        WHERE Status = 'ready';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        -- Use RAISERROR inside the CATCH block to return error
        RAISERROR (@ErrorMessage,  -- Message text
                   @ErrorSeverity, -- Severity
                   @ErrorState     -- State
                  );
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_raiserror_statement.json",
    )


def test_mssql_procedure_complex_multiline_comments() -> None:
    """Test stored procedure with both single-line and multi-line comments.

    Ensures comment handling works correctly with various comment styles.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ComplexCommentHandling
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        /* Multi-line comment
           describing the insert operation
           across multiple lines */
        INSERT INTO CurrentData.dbo.AuditLog (EventType, EventDate)
        VALUES ('ProcessStart', GETDATE());

        -- Single line comment before update
        UPDATE CurrentData.dbo.ProcessStatus
        SET Status = 'running',
            StartTime = GETDATE()
        WHERE ProcessId = 1;
        -- Another single line comment after

        /* Inline */ SELECT * FROM CurrentData.dbo.ProcessStatus /* comment */ WHERE ProcessId = 1;

    END TRY
    BEGIN CATCH
        -- Error handling comment
        PRINT ERROR_MESSAGE();
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_complex_comments.json",
    )


def test_mssql_procedure_exec_sp_with_error_handling() -> None:
    """Test stored procedure that executes another stored procedure in CATCH block.

    From test-user.log: EXEC master.dbo.sp_LogError with multiple parameters
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.ProcessWithErrorLogging
AS
BEGIN
    BEGIN TRY
        UPDATE TargetDB.dbo.orders
        SET Status = 'processed',
            ProcessedDate = GETDATE()
        FROM TargetDB.dbo.orders
        WHERE Status = 'pending';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorNumber INT;
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorLogId INT;

        SELECT @ErrorNumber = ERROR_NUMBER(),
               @ErrorMessage = ERROR_MESSAGE();

        EXEC master.dbo.sp_LogError
            @ErrorNumber,
            @ErrorMessage,
            @ErrorLogId OUTPUT;

        RAISERROR (@ErrorMessage, 16, 1);
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_exec_sp_error_handling.json",
    )


def test_mssql_procedure_set_nocount_off_with_return() -> None:
    """Test stored procedure with SET NOCOUNT OFF and RETURN statement.

    From test-user.log: 'Can only generate column-level lineage for select-like
    inner statements, not <class 'sqlglot.expressions.Set'>'
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.QuickInsert
    @DocumentId INT,
    @Title VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        INSERT INTO CurrentData.dbo.Documents (DocumentId, Title, CreatedOn)
        VALUES (@DocumentId, @Title, GETDATE());

    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
        SET NOCOUNT OFF;
        RETURN -1;
    END CATCH

    SET NOCOUNT OFF;
    RETURN 0;
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_nocount_return.json",
    )


# Tests for advanced MSSQL syntax patterns


def test_mssql_procedure_with_for_xml_path_and_stuff() -> None:
    """Test stored procedure with FOR XML PATH and STUFF function.

    Tests complex MSSQL-specific syntax including:
    - STUFF function for string manipulation
    - FOR XML PATH for concatenating rows
    - @@ERROR global variable
    - Table-valued function calls
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.MDS_getFundManagerHistory
(@p_SecIdList VARCHAR(4000))
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ErrorLine INT;
    DECLARE @ErrorProcedure NVARCHAR(255);
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorLogId VARCHAR(255);
    DECLARE @ErrorLogDBName VARCHAR(100);

    SET @ErrorLogDBName = DB_NAME();
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        SELECT
            FundId as UniqueId,
            STUFF(
                (SELECT '|' + ManagerHistory
                 FROM DataIndex.dbo.xv_FundManagerHistory e2
                 WHERE e2.FundId = e1.FundId
                 FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),
                 1, 1, '') as ManagerHistory
        FROM DataIndex.dbo.xv_FundManagerHistory e1
        INNER JOIN DataIndex.dbo.tf_IdListLong(',', @p_SecIdList) t0 ON t0.Id = e1.FundId
        GROUP BY FundId;

        SET NOCOUNT OFF;
        RETURN @@ERROR;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorNumber = ERROR_NUMBER(),
               @ErrorState = ERROR_STATE(),
               @ErrorLine = ERROR_LINE(),
               @ErrorProcedure = ERROR_PROCEDURE();

        EXEC master.dbo.sp_LogError @ErrorNumber, @ErrorMessage, @ErrorLogId,
             @ErrorProcedure, @ErrorLogDBName, @ErrorLine;

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine, @ErrorProcedure);
        RETURN;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_for_xml_path_stuff.json",
    )


def test_mssql_procedure_with_table_variable() -> None:
    """Test stored procedure with table variable declaration.

    Tests MSSQL table variable patterns including:
    - DECLARE @variable TABLE syntax
    - SELECT with CASE expressions
    - Functions in FROM clause (string_split)
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.GetSecurityDetailBySecIds
    @p_SecIdList VARCHAR(MAX)
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ErrorLine INT;
    DECLARE @ErrorProcedure NVARCHAR(255);
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorLogId VARCHAR(255);
    DECLARE @ErrorLogDBName VARCHAR(100);

    SET @ErrorLogDBName = DB_NAME();
    SET NOCOUNT ON;

    DECLARE @SecId TABLE (SecId VARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT si.SecId, TradingSymbol, Name,
            (CASE
                WHEN SecurityType = 'FO' THEN 'Open End'
                WHEN SecurityType = 'FV' THEN 'Insurance/Life Product'
                WHEN SecurityType = 'FC' THEN 'Closed End'
                WHEN SecurityType = 'FE' THEN 'ETF'
                WHEN SecurityType = 'ST' THEN 'Stock'
                WHEN SecurityType = 'XI' THEN 'Index'
                WHEN SecurityType = 'SA' THEN 'Separate Account'
                ELSE SecurityType
            END) AS SecurityType
        FROM CurrentData.dbo.ParseSecIdListToTableByOrder(@p_SecIdList) sls
        LEFT JOIN DataIndex.dbo.vw_SecurityId si WITH(NOLOCK) ON si.SecId = sls.SecId
        ORDER BY sls.AutoId ASC;

        SET NOCOUNT OFF;
        RETURN;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorNumber = ERROR_NUMBER(),
               @ErrorState = ERROR_STATE(),
               @ErrorLine = ERROR_LINE(),
               @ErrorProcedure = ERROR_PROCEDURE();

        EXEC master.dbo.sp_LogError @ErrorNumber, @ErrorMessage, @ErrorLogId,
             @ErrorProcedure, @ErrorLogDBName, @ErrorLine;

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine, @ErrorProcedure);
        SET NOCOUNT OFF;
        RETURN;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_table_variable.json",
    )


def test_mssql_procedure_with_set_rowcount() -> None:
    """Test stored procedure with SET ROWCOUNT statements.

    Tests MSSQL-specific SET ROWCOUNT syntax:
    - SET ROWCOUNT @variable (dynamic row limit)
    - SET ROWCOUNT 0 (reset to unlimited)
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.findBondMasterByName_V3
    @p_NameLike VARCHAR(75),
    @p_ResultLimit INT = 5001
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ErrorMessage NVARCHAR(2000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ErrorLine INT;
    DECLARE @ErrorProcedure NVARCHAR(255);
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorLogId VARCHAR(255);
    DECLARE @ErrorLogDBName VARCHAR(100);

    SET @ErrorLogDBName = DB_NAME();

    BEGIN TRY
        SET ROWCOUNT @p_ResultLimit;

        SELECT LTRIM(RTRIM(BondId)) AS CUSIP,
               Name AS StandardName,
               CountryId,
               CurrencyId,
               MaturityDate
        FROM dbo.vw_BondMasterId WITH(NOLOCK, INDEX(ix_BondMasterId_Name))
        WHERE Name LIKE @p_NameLike
        ORDER BY StandardName;

        SET ROWCOUNT 0;

        SET NOCOUNT OFF;
        RETURN;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorNumber = ERROR_NUMBER(),
               @ErrorState = ERROR_STATE(),
               @ErrorLine = ERROR_LINE(),
               @ErrorProcedure = ERROR_PROCEDURE();

        EXEC master.dbo.sp_LogError @ErrorNumber, @ErrorMessage, @ErrorLogId,
             @ErrorProcedure, @ErrorLogDBName, @ErrorLine;

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine, @ErrorProcedure);
        RETURN;
    END CATCH
END
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_set_rowcount.json",
    )


@pytest.mark.xfail(
    reason="Procedures without explicit BEGIN/END are not handled correctly by split_statements - "
    "the entire procedure is treated as one CREATE PROCEDURE statement and gets skipped",
    strict=True,
)
def test_mssql_procedure_simple_without_begin_end() -> None:
    """Test simple stored procedure without explicit BEGIN/END around body.

    This pattern is valid MSSQL syntax where BEGIN/END is optional for single-statement procedures.
    However, split_statements does not handle this correctly - it treats the entire procedure
    as one CREATE PROCEDURE statement which gets filtered out, resulting in no lineage extraction.
    """
    assert_sql_result(
        """
CREATE PROCEDURE dbo.getManagerInformationBySecId (
    @p_SecId CHAR(10))
AS
    SELECT b.PersonId AS ManagerId,
           c.GivenName + ' ' + c.FamilyName AS ManagerName,
           b.StartDate,
           b.EndDate
    FROM DataIndex.dbo.vw_FundShareClassId a WITH (NOLOCK)
    INNER JOIN DataIndex.dbo.xv_FundManager b WITH (NOLOCK) ON a.FundId = b.FundId
    INNER JOIN DataIndex.dbo.vw_PersonId c WITH (NOLOCK) ON b.PersonId = c.PersonId
    WHERE FundShareClassId = @p_SecId AND b.EndDate IS NULL
    ORDER BY b.PersonId
""",
        dialect="mssql",
        expected_file=RESOURCE_DIR / "test_mssql_procedure_simple_no_begin_end.json",
    )


# =============================================================================
# Multiple Output Tables Tests
# =============================================================================


def test_procedure_with_multiple_output_tables_all_registered() -> None:
    """Test that procedures with multiple output tables register ALL outputs."""
    from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
    from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

    aggregator = SqlParsingAggregator(
        platform="mssql",
        platform_instance=None,
        env="PROD",
        graph=None,
    )

    # Procedure that modifies 3 different tables
    procedure_sql = """
    CREATE PROCEDURE dbo.update_inventory_and_sales AS
    BEGIN
        -- Update inventory table
        UPDATE inventory
        SET quantity = quantity - 1
        WHERE product_id = 100;

        -- Insert into sales table
        INSERT INTO sales (product_id, quantity, sale_date)
        SELECT product_id, 1, GETDATE()
        FROM inventory
        WHERE product_id = 100;

        -- Update audit log
        INSERT INTO audit_log (action, timestamp)
        VALUES ('inventory_update', GETDATE());
    END
    """

    # Parse the procedure
    result = sqlglot_lineage(
        procedure_sql,
        schema_resolver=aggregator._schema_resolver,
        default_db="testdb",
        default_schema="dbo",
    )

    # Should find all 3 output tables
    assert len(result.out_tables) == 3, (
        f"Expected 3 output tables, got {len(result.out_tables)}: {result.out_tables}"
    )

    # Verify each table is present
    output_table_names = {urn.split(".")[-1].split(",")[0] for urn in result.out_tables}
    assert "inventory" in output_table_names
    assert "sales" in output_table_names
    assert "audit_log" in output_table_names


def test_procedure_with_four_cross_database_outputs() -> None:
    """Test procedure modifying tables across multiple databases."""
    from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
    from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

    aggregator = SqlParsingAggregator(
        platform="mssql",
        platform_instance=None,
        env="PROD",
        graph=None,
    )

    procedure_sql = """
    CREATE PROCEDURE dbo.sync_data_across_systems AS
    BEGIN
        -- Update staging database
        DELETE FROM staging_db.dbo.temp_data WHERE age > 30;

        -- Insert into production database
        INSERT INTO prod_db.dbo.customers (id, name)
        SELECT id, name FROM staging_db.dbo.temp_data;

        -- Update reporting database
        UPDATE reporting_db.dbo.customer_summary
        SET total_count = (SELECT COUNT(*) FROM prod_db.dbo.customers);

        -- Log to audit database
        INSERT INTO audit_db.dbo.sync_log (sync_time, record_count)
        VALUES (GETDATE(), @@ROWCOUNT);
    END
    """

    result = sqlglot_lineage(
        procedure_sql,
        schema_resolver=aggregator._schema_resolver,
        default_db="staging_db",
        default_schema="dbo",
    )

    # Should capture all 4 output tables across different databases
    assert len(result.out_tables) == 4, (
        f"Expected 4 cross-database output tables, got {len(result.out_tables)}"
    )

    # Verify we have tables from all 4 databases
    all_outputs = " ".join(result.out_tables)

    assert "staging_db" in all_outputs
    assert "prod_db" in all_outputs
    assert "reporting_db" in all_outputs
    assert "audit_db" in all_outputs


def test_procedure_single_output_still_works() -> None:
    """Ensure single-output procedures still work (regression test)."""
    from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
    from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

    aggregator = SqlParsingAggregator(
        platform="mssql",
        platform_instance=None,
        env="PROD",
        graph=None,
    )

    procedure_sql = """
    CREATE PROCEDURE dbo.simple_update AS
    BEGIN
        UPDATE customers SET status = 'active' WHERE id = 1;
    END
    """

    result = sqlglot_lineage(
        procedure_sql,
        schema_resolver=aggregator._schema_resolver,
        default_db="testdb",
        default_schema="dbo",
    )

    # Should still work for single output
    assert len(result.out_tables) == 1
    assert "customers" in result.out_tables[0]
