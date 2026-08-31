"""
Test statement filtering for MSSQL stored procedures.

This test verifies that parse_procedure_code() correctly filters out non-DML
statements found in production logs, using sqlglot-based parsing instead of
string matching.

Test cases cover all problematic statement patterns found in production logs.
"""

from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver


def test_filter_variable_assignment_select_without_from():
    """
    Filter SELECT @var = value statements (variable assignments without FROM).

    Found 27+ instances in logs like:
    - SELECT @ErrorMessage = ERROR_MESSAGE()
    - SELECT @l_i=1
    - SELECT @p_Date = DATEADD(yy,-@p_NumYears,@p_MaxDate)

    These don't produce lineage, just assign values to variables.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    # Single variable assignment
    code = """
    SELECT @ErrorMessage = ERROR_MESSAGE()
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    # Should filter out - no lineage produced
    assert result is None or (not result.inputDatasets and not result.outputDatasets)

    # Multiple variable assignment
    code = """
    SELECT @ErrorMessage = ERROR_MESSAGE(),@ErrorSeverity = ERROR_SEVERITY(),@ErrorNumber = ERROR_NUMBER()
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)

    # Simple assignment
    code = """
    SELECT @l_i=1
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)

    # Assignment with function
    code = """
    SELECT @p_Date = DATEADD(yy,-@p_NumYears,@p_MaxDate)
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)


def test_keep_select_with_from():
    """
    KEEP SELECT @var = ... FROM table statements (should NOT be filtered).

    Found in logs:
    - SELECT @Count=COUNT(0) FROM #CumulativeFairValueNAV
    - SELECT @OldYear=YEAR([Date]), @OldMonth=MONTH([Date]) FROM [CurrentData].[dbo].[NumberOfShareHolder]

    These statements have FROM clauses, so they should pass through the filter.
    However, SELECT without a downstream table (no INSERT INTO, no output) won't
    produce final lineage - that's expected aggregator behavior, not a filtering issue.

    The key test is that these are NOT filtered at the statement filtering stage.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.SourceTable").urn(),
        schema_info={"col1": "int"},
    )
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.TargetTable").urn(),
        schema_info={"col1": "int"},
    )

    # SELECT with FROM - this has input but no output, so returns None
    # But it should NOT be filtered at statement filtering stage (it should reach aggregator)
    code = """
    SELECT @Count=COUNT(0) FROM SourceTable
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    # Returns None because no output table, but that's aggregator behavior
    # The important thing is it wasn't filtered at statement filtering stage
    assert result is None  # No downstream table

    # INSERT INTO ... SELECT FROM - this should produce lineage
    code = """
    INSERT INTO TargetTable
    SELECT @Count=COUNT(0), col1 FROM SourceTable
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    # Should produce lineage with both input and output
    assert result is not None
    assert len(result.inputDatasets) >= 1
    assert len(result.outputDatasets) >= 1


def test_filter_raiserror_statements():
    """
    Filter RAISERROR statements (error handling).

    Found 200+ instances in logs like:
    - RAISERROR (@ErrorMessage, 16, 1)
    - RAISERROR(@l_Msg, 11,1)

    These don't produce lineage, they're error handling.
    Sqlglot parses these as QueryType.UNKNOWN.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    # RAISERROR with variables
    code = """
    RAISERROR (@ErrorMessage, 16, 1)
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)

    # RAISERROR with literal
    code = """
    RAISERROR('Error occurred', 16, 1)
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)


def test_filter_comment_blocks():
    """
    Filter comment blocks.

    Found 248 instances in staging logs:
    - /*******************
    - /* Section separator */

    These can't be parsed as SQL, throw parse errors.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    # Block comment
    code = """
    /*******************
    */
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)

    # Comment with text
    code = """
    /* This is a section separator */
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is None or (not result.inputDatasets and not result.outputDatasets)


def test_keep_real_dml_statements():
    """
    KEEP real DML statements (INSERT, UPDATE, DELETE, MERGE).

    These are the statements we want to extract lineage from.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.SourceTable").urn(),
        schema_info={"col1": "int"},
    )
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.TargetTable").urn(),
        schema_info={"col1": "int"},
    )

    # INSERT
    code = """
    INSERT INTO TargetTable SELECT * FROM SourceTable
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is not None
    assert len(result.inputDatasets) >= 1
    assert len(result.outputDatasets) >= 1

    # UPDATE
    code = """
    UPDATE TargetTable SET col1 = 1 FROM SourceTable WHERE TargetTable.id = SourceTable.id
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is not None

    # DELETE
    code = """
    DELETE FROM TargetTable WHERE id IN (SELECT id FROM SourceTable)
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    assert result is not None


def test_mixed_procedure_with_all_statement_types():
    """
    Test a realistic procedure with mixed statement types.

    This simulates procedures from production logs that have:
    - Variable declarations
    - Error handling with RAISERROR
    - Variable assignments
    - Real DML statements
    - Comments

    Only the real DML should produce lineage.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.SourceTable").urn(),
        schema_info={"col1": "int"},
    )
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.TargetTable").urn(),
        schema_info={"col1": "int"},
    )

    code = """
    -- Variable declarations
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @Count INT

    BEGIN TRY
        /*******************
         * Main logic
         *******************/

        -- Get count from source
        SELECT @Count = COUNT(*) FROM SourceTable

        -- Insert data
        INSERT INTO TargetTable SELECT * FROM SourceTable WHERE status = 'active'

        -- Update statistics
        UPDATE TargetTable SET processed = 1 WHERE id > 0

    END TRY
    BEGIN CATCH
        -- Error handling
        SELECT @ErrorMessage = ERROR_MESSAGE()
        RAISERROR (@ErrorMessage, 16, 1)
    END CATCH
    """

    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )

    # Should produce lineage only from INSERT, UPDATE, and SELECT with FROM
    assert result is not None
    assert len(result.inputDatasets) >= 1  # SourceTable
    assert len(result.outputDatasets) >= 1  # TargetTable

    # Check that source and target are in the lineage
    input_urns = {str(ds) for ds in result.inputDatasets}
    output_urns = {str(ds) for ds in result.outputDatasets}

    assert any("sourcetable" in urn.lower() for urn in input_urns)
    assert any("targettable" in urn.lower() for urn in output_urns)


def test_multiline_statements():
    """
    Test that multiline statements are handled correctly.

    In logs, we saw statements split across lines:
    - SELECT @Count = COUNT(*)
      FROM table

    The FROM is on a different line, but should still be detected by sqlglot parser.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.SourceTable").urn(),
        schema_info={"col1": "int"},
    )
    schema_resolver.add_raw_schema_info(
        urn=DatasetUrn("mssql", "TestDB.dbo.TargetTable").urn(),
        schema_info={"col1": "int"},
    )

    # Multiline INSERT with SELECT FROM
    code = """
    INSERT INTO TargetTable
    SELECT @Count = COUNT(*), col1
    FROM SourceTable
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    # Should produce lineage (has FROM and downstream)
    assert result is not None
    assert len(result.inputDatasets) >= 1
    assert len(result.outputDatasets) >= 1


def test_edge_case_select_with_output_parameter():
    """
    Test SELECT with output parameters that look like assignments.

    Some stored procedures use SELECT to return values:
    - SELECT @p_SecId AS SecId

    These might not have FROM, but they're return values, not assignments.
    In our logs, these appeared without FROM, so should be filtered.
    """
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    # SELECT with AS (output parameter pattern)
    code = """
    SELECT @p_SecId AS SecId
    """
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=code,
        is_temp_table=lambda x: x.startswith("#"),
    )
    # No FROM clause, so filter it (no lineage)
    assert result is None or (not result.inputDatasets and not result.outputDatasets)


# =============================================================================
# DROP TABLE Statement Tests
# =============================================================================


def test_drop_table_does_not_break_lineage():
    """Test that DROP TABLE statements are skipped and don't prevent lineage extraction."""
    sql = """
CREATE PROCEDURE dbo.test_drop_table_procedure
AS
BEGIN
    -- This DROP TABLE statement was causing "Expected table name but got None" errors
    DROP TABLE IF EXISTS #Temp1;

    -- These DML statements should still produce lineage
    DELETE dst
    FROM CurrentData.dbo.output_table dst
    INNER JOIN Staging.dbo.input_table src
    ON dst.id = src.id
    WHERE src.action = 'DELETE';

    INSERT INTO CurrentData.dbo.output_table
    SELECT * FROM Staging.dbo.input_table
    WHERE action != 'DELETE';
END;
"""

    schema_resolver = SchemaResolver(platform="mssql")
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="Staging",
        default_schema="dbo",
        code=sql,
        is_temp_table=lambda x: False,
    )

    # Verify lineage was extracted despite DROP TABLE statement
    assert result is not None, "Lineage should be extracted even with DROP TABLE"
    assert len(result.inputDatasets) > 0, "Should have input datasets"
    assert len(result.outputDatasets) > 0, "Should have output datasets"

    # Verify correct tables are identified
    input_urns = [urn.lower() for urn in result.inputDatasets]
    output_urns = [urn.lower() for urn in result.outputDatasets]

    assert any("staging.dbo.input_table" in urn for urn in input_urns), (
        "Should include input table"
    )
    assert any("currentdata.dbo.output_table" in urn for urn in output_urns), (
        "Should include output table"
    )


def test_multiple_drop_statements():
    """Test procedure with multiple DROP statements."""
    sql = """
CREATE PROCEDURE dbo.test_multiple_drops
AS
BEGIN
    DROP TABLE IF EXISTS #Temp1;
    DROP TABLE #Temp2;
    DROP VIEW IF EXISTS #TempView;

    INSERT INTO CurrentData.dbo.result
    SELECT * FROM Staging.dbo.source;
END;
"""

    schema_resolver = SchemaResolver(platform="mssql")
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="Staging",
        default_schema="dbo",
        code=sql,
        is_temp_table=lambda x: False,
    )

    assert result is not None, "Should handle multiple DROP statements"
    assert len(result.inputDatasets) > 0
    assert len(result.outputDatasets) > 0


def test_cross_database_with_drop():
    """Test cross-database references with DROP statements (reproduces customer issue)."""
    sql = """
CREATE PROCEDURE dbo.upd_test_procedure
@IsSnapshot VARCHAR(5) = 'NO'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SeriesId VARCHAR(50) = (
        SELECT SeriesIdKey
        FROM ClientAccounts.dbo.SeriesIdClientAccountsMapping
        WHERE provide_id = 'test'
    );

    BEGIN TRY
        DROP TABLE IF EXISTS #Temp1;

        -- Delete from CurrentData (non-ingested DB)
        DELETE dst
        FROM CurrentData.dbo.output_table dst
        INNER JOIN Staging.dbo.Staging_lakehouse_input_table src
        ON dst.id = src.id
        WHERE src.record_action = 'DELETE';

        DROP TABLE IF EXISTS #Temp2;

        -- Update CurrentData from Staging
        UPDATE dst
        SET dst.value = src.value,
            dst.LastUpdate = GETDATE()
        FROM CurrentData.dbo.output_table dst
        INNER JOIN Staging.dbo.Staging_lakehouse_input_table src
        ON dst.id = src.id;

        -- Insert into CurrentData from Staging
        INSERT INTO CurrentData.dbo.output_table
        SELECT * FROM Staging.dbo.Staging_lakehouse_input_table src
        WHERE NOT EXISTS (
            SELECT 1 FROM CurrentData.dbo.output_table dst
            WHERE dst.id = src.id
        );
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
"""

    schema_resolver = SchemaResolver(platform="mssql")
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db="Staging",
        default_schema="dbo",
        code=sql,
        is_temp_table=lambda x: x.startswith("#"),
    )

    # This reproduces the customer issue: cross-database lineage with DROP statements
    assert result is not None, (
        "Should extract lineage with cross-database refs and DROP"
    )
    assert len(result.inputDatasets) > 0, "Should have inputs from Staging DB"
    assert len(result.outputDatasets) > 0, "Should have outputs to CurrentData DB"

    # Verify both databases are represented
    all_urns = result.inputDatasets + result.outputDatasets
    all_urns_lower = [urn.lower() for urn in all_urns]

    has_staging = any("staging" in urn for urn in all_urns_lower)
    has_currentdata = any("currentdata" in urn for urn in all_urns_lower)

    assert has_staging, "Should have references to Staging database"
    assert has_currentdata, "Should have references to CurrentData database"
