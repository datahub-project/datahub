"""
Test that DROP TABLE statements don't break stored procedure lineage extraction.

This test verifies the fix for the issue where "DROP TABLE IF EXISTS" statements
were being split incorrectly, causing parsing failures that prevented lineage
from being extracted from otherwise valid stored procedures.
"""

from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.sql_parsing.schema_resolver import SchemaResolver


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
