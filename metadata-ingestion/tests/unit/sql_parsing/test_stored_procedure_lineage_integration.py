"""
Integration tests for stored procedure lineage extraction through parse_procedure_code.

These tests validate the full pipeline:
1. Skip splitting for CREATE PROCEDURE statements
2. Fallback parser strips CREATE PROCEDURE wrapper
3. Statements are parsed and lineage is extracted
4. Aggregator generates MCPs with UNKNOWN query type
5. DataJobInputOutput is created with inputs and outputs
"""

import pytest

from datahub.ingestion.source.sql.stored_procedures.lineage import (
    parse_procedure_code,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver


@pytest.fixture
def mssql_schema_resolver() -> SchemaResolver:
    """Create a basic MSSQL schema resolver for testing."""
    return SchemaResolver(
        platform="mssql",
        platform_instance="test-instance",
        env="PROD",
    )


def test_create_procedure_with_try_catch_generates_lineage(
    mssql_schema_resolver: SchemaResolver,
) -> None:
    """
    Test that CREATE PROCEDURE with TRY/CATCH generates lineage.

    This validates the full fix:
    - CREATE PROCEDURE is not pre-split
    - Fallback parser strips wrapper and parses statements
    - UNKNOWN query type allows aggregator to generate MCPs
    - DataJobInputOutput is created with lineage
    """
    procedure_code = """
CREATE PROCEDURE dbo.UpdateCustomerOrders
    @CustomerId INT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Update customer total
        UPDATE customers
        SET total_orders = (
            SELECT COUNT(*)
            FROM orders
            WHERE customer_id = @CustomerId
        )
        WHERE id = @CustomerId;

        -- Delete old orders
        DELETE FROM orders
        WHERE customer_id = @CustomerId
        AND order_date < DATEADD(year, -2, GETDATE());
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
        THROW;
    END CATCH
END
"""

    result = parse_procedure_code(
        schema_resolver=mssql_schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=procedure_code,
        is_temp_table=lambda x: x.startswith("#"),
        procedure_name="UpdateCustomerOrders",
    )

    # Verify lineage was generated
    assert result is not None, "parse_procedure_code should return DataJobInputOutput"

    # Should have input tables (orders for SELECT and DELETE)
    assert result.inputDatasets is not None
    assert len(result.inputDatasets) > 0

    # Should have output tables (customers UPDATE, orders DELETE)
    assert result.outputDatasets is not None
    assert len(result.outputDatasets) > 0

    # Verify specific tables are present (platform instance will be prefixed)
    input_urns = [urn for urn in result.inputDatasets]
    output_urns = [urn for urn in result.outputDatasets]

    # Check that orders table appears (as input for SELECT and DELETE)
    assert any("orders" in urn.lower() for urn in input_urns)

    # Check that customers table appears (as output for UPDATE)
    assert any("customers" in urn.lower() for urn in output_urns)


def test_create_procedure_without_control_flow_generates_lineage(
    mssql_schema_resolver: SchemaResolver,
) -> None:
    """
    Test that CREATE PROCEDURE without control flow also generates lineage.

    This ensures the fix doesn't break procedures without TRY/CATCH.
    """
    procedure_code = """
CREATE PROCEDURE dbo.SimpleUpdate
AS
BEGIN
    UPDATE products
    SET last_updated = GETDATE()
    FROM products p
    INNER JOIN inventory i ON p.id = i.product_id
    WHERE i.quantity > 0;
END
"""

    result = parse_procedure_code(
        schema_resolver=mssql_schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=procedure_code,
        is_temp_table=lambda x: x.startswith("#"),
        procedure_name="SimpleUpdate",
    )

    assert result is not None
    assert result.inputDatasets is not None
    assert len(result.inputDatasets) > 0
    assert result.outputDatasets is not None
    assert len(result.outputDatasets) > 0


def test_create_procedure_multiple_statements_aggregates_lineage(
    mssql_schema_resolver: SchemaResolver,
) -> None:
    """
    Test that multiple statements in a procedure aggregate lineage correctly.
    """
    procedure_code = """
CREATE PROCEDURE dbo.ComplexETL
AS
BEGIN
    BEGIN TRY
        -- Insert from source
        INSERT INTO staging_table (id, name)
        SELECT id, name FROM source_table;

        -- Update target
        UPDATE target_table
        SET name = s.name
        FROM target_table t
        INNER JOIN staging_table s ON t.id = s.id;

        -- Clean up
        DELETE FROM staging_table;
    END TRY
    BEGIN CATCH
        ROLLBACK;
    END CATCH
END
"""

    result = parse_procedure_code(
        schema_resolver=mssql_schema_resolver,
        default_db="TestDB",
        default_schema="dbo",
        code=procedure_code,
        is_temp_table=lambda x: x.startswith("#"),
        procedure_name="ComplexETL",
    )

    assert result is not None

    # Should have multiple input tables (source_table, staging_table, target_table)
    # Note: Without schema resolution, UPDATE target tables appear in inputs
    assert result.inputDatasets is not None
    assert len(result.inputDatasets) >= 2

    # Should have output table (staging_table from INSERT/DELETE)
    assert result.outputDatasets is not None
    assert len(result.outputDatasets) >= 1

    # Verify specific tables
    input_urns = [urn.lower() for urn in result.inputDatasets]
    output_urns = [urn.lower() for urn in result.outputDatasets]

    # Source and staging tables should be in inputs
    assert any("source_table" in urn for urn in input_urns)
    assert any("staging_table" in urn for urn in input_urns)

    # Staging table should be in outputs (from INSERT/DELETE)
    assert any("staging_table" in urn for urn in output_urns)
