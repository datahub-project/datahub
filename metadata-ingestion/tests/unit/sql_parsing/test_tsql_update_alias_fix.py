"""Tests for TSQL UPDATE alias fix.

This test verifies that unqualified table aliases (like 'dst' in "UPDATE dst FROM table dst")
are correctly filtered from lineage, preventing invalid URNs that would cause the sink to
reject the entire aspect.

See: https://github.com/datahub-project/datahub/issues/XXXXX
"""

from datahub.ingestion.source.sql.stored_procedures.lineage import (
    _filter_qualified_table_urns,
    _is_qualified_table_urn,
)


def test_is_qualified_table_urn():
    """Test that _is_qualified_table_urn correctly identifies qualified tables."""
    # Qualified tables (have db.schema.table format)
    assert _is_qualified_table_urn(
        "urn:li:dataset:(urn:li:dataPlatform:mssql,database1.dbo.table1,PROD)"
    )
    assert _is_qualified_table_urn(
        "urn:li:dataset:(urn:li:dataPlatform:mssql,database2.dbo.table2,PROD)"
    )

    # Unqualified tables
    assert not _is_qualified_table_urn(
        "urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD)"
    )
    assert not _is_qualified_table_urn(
        "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
    )


def test_filter_qualified_table_urns():
    """Test that _filter_qualified_table_urns removes unqualified tables."""
    in_tables = [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,database1.dbo.source_table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,database2.dbo.target_table,PROD)",
    ]

    filtered = _filter_qualified_table_urns(in_tables)

    # Should filter out 'dst' and keep the 2 qualified tables
    assert len(filtered) == 2
    assert all(
        "dst" not in urn or "database1.dbo" in urn or "database2.dbo" in urn
        for urn in filtered
    )
    assert any("database1.dbo.source_table" in urn for urn in filtered)
    assert any("database2.dbo.target_table" in urn for urn in filtered)


def test_tsql_update_alias_lineage():
    """Test that stored procedure lineage correctly filters TSQL UPDATE aliases.

    This is the end-to-end test that verifies the fix works in the full pipeline.
    The filtering happens in parse_procedure_code() in lineage.py after aggregation.
    Uses a complete stored procedure with TRY/CATCH to match real-world usage.
    """
    from datahub.ingestion.source.sql.stored_procedures.lineage import (
        parse_procedure_code,
    )
    from datahub.sql_parsing.schema_resolver import SchemaResolver

    # Full stored procedure with multiple statements (matches real-world usage)
    # This produces 2 out_tables: dst and database2.dbo.target_table
    sql = """
CREATE PROCEDURE dbo.test_update_procedure
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DELETE dst
        FROM Database2.dbo.target_table dst
        INNER JOIN Database1.dbo.source_table src
          ON dst.id = src.id
        WHERE src.action = 'DELETE';

        UPDATE dst
        SET last_updated = src.last_updated,
            value = src.value
        FROM Database2.dbo.target_table dst
        INNER JOIN Database1.dbo.source_table src
          ON dst.id = src.id
        WHERE src.action IS NULL;

        INSERT INTO Database2.dbo.target_table (id, last_updated)
        SELECT src.id, src.last_updated
        FROM Database1.dbo.source_table src
        LEFT JOIN Database2.dbo.target_table dst
          ON dst.id = src.id
        WHERE dst.id IS NULL;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
    """

    # Create schema resolver with mock discovered tables
    schema_resolver = SchemaResolver(
        platform="mssql",
        env="PROD",
    )

    # Mock discovered datasets (real tables that exist in the database)
    discovered_tables = {
        "database2.dbo.target_table",
        "database1.dbo.source_table",
    }

    def is_temp_table(name: str) -> bool:
        """Check if table is temporary (not in discovered tables)."""
        normalized = name.lower()
        return normalized not in discovered_tables

    # Call parse_procedure_code which includes the filtering
    result = parse_procedure_code(
        schema_resolver=schema_resolver,
        default_db=None,
        default_schema=None,
        code=sql,
        is_temp_table=is_temp_table,
        procedure_name="test_update_procedure",
    )

    # Verify result exists and contains only qualified tables
    assert result is not None, "parse_procedure_code should return a result"

    # Verify no unqualified tables in inputs
    for urn in result.inputDatasets:
        name = urn.split(",")[1] if "," in urn else urn
        assert "." in name, f"Input dataset should be qualified: {name}"
        # Should not contain 'dst' without database.schema prefix
        assert name != "dst", f"Input should not contain unqualified 'dst': {name}"

    # Verify no unqualified tables in outputs
    for urn in result.outputDatasets:
        name = urn.split(",")[1] if "," in urn else urn
        assert "." in name, f"Output dataset should be qualified: {name}"
        # Should not contain 'dst' without database.schema prefix
        assert name != "dst", f"Output should not contain unqualified 'dst': {name}"

    # Verify we have the expected tables
    all_datasets = result.inputDatasets + result.outputDatasets
    assert any("database2.dbo.target_table" in urn for urn in all_datasets), (
        "Should contain database2.dbo.target_table"
    )
    assert any("database1.dbo.source_table" in urn for urn in all_datasets), (
        "Should contain database1.dbo.source_table"
    )


if __name__ == "__main__":
    # Run tests manually
    print("Testing _is_qualified_table_urn...")
    test_is_qualified_table_urn()
    print("PASSED\n")

    print("Testing _filter_qualified_table_urns...")
    test_filter_qualified_table_urns()
    print("PASSED\n")

    print("Testing end-to-end stored procedure lineage...")
    test_tsql_update_alias_lineage()
    print("PASSED\n")

    print("=" * 80)
    print("ALL TESTS PASSED!")
    print("=" * 80)
