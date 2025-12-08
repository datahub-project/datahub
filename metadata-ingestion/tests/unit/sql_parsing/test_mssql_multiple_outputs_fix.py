"""
Tests for multiple output tables from stored procedures.

Problem: Only the first output table was being registered in downstream lineage.
Fix: Loop over all out_tables in sql_parsing_aggregator.py instead of taking [0].
"""

import pytest

from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


@pytest.fixture
def aggregator():
    """Create a SQL parsing aggregator for testing."""
    return SqlParsingAggregator(
        platform="mssql",
        platform_instance=None,
        env="PROD",
        graph=None,
    )


def test_procedure_with_multiple_output_tables_all_registered(aggregator):
    """Test that procedures with multiple output tables register ALL outputs."""
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


def test_procedure_with_four_cross_database_outputs(aggregator):
    """Test procedure modifying tables across multiple databases."""
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


def test_procedure_single_output_still_works(aggregator):
    """Ensure single-output procedures still work (regression test)."""
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
