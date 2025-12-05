"""
Test for Bug: Only first output table shows as downstream when procedure modifies multiple tables.

Root cause: sql_parsing_aggregator.py:871 only takes parsed.out_tables[0]
"""

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingDebugInfo,
    SqlParsingResult,
)


def test_multiple_output_tables_only_first_shows_as_downstream():
    """
    Test that demonstrates the bug where only the first output table is registered.

    This test directly creates a SqlParsingResult with multiple out_tables and
    verifies that the ObservedQuery -> PreparsedQuery conversion (line 871) only
    registers the first output table.
    """

    # Create schema resolver
    schema_resolver = SchemaResolver(platform="mssql", env="PROD")

    # Add schemas for test tables
    for table in ["DB_A.dbo.Table_A", "DB_B.dbo.Table_B", "DB_C.dbo.Table_C"]:
        urn = make_dataset_urn(platform="mssql", name=table, env="PROD")
        from datahub.metadata.schema_classes import SchemalessClass, SchemaMetadataClass

        schema_resolver.add_schema_metadata(
            urn,
            SchemaMetadataClass(
                schemaName=table,
                platform="urn:li:dataPlatform:mssql",
                version=0,
                hash="",
                platformSchema=SchemalessClass(),
                fields=[],
            ),
        )

    # Create aggregator with a custom parser that returns multiple outputs
    aggregator = SqlParsingAggregator(
        platform="mssql",
        env="PROD",
        schema_resolver=schema_resolver,
        generate_lineage=True,
        generate_queries=False,
        generate_usage_statistics=False,
    )

    # Create URNs for our test tables
    table_a_urn = make_dataset_urn(
        platform="mssql", name="DB_A.dbo.Table_A", env="PROD"
    )
    table_b_urn = make_dataset_urn(
        platform="mssql", name="DB_B.dbo.Table_B", env="PROD"
    )
    table_c_urn = make_dataset_urn(
        platform="mssql", name="DB_C.dbo.Table_C", env="PROD"
    )

    # Mock a SqlParsingResult with MULTIPLE output tables (as the stored procedure parser would return)
    mock_parsed_result = SqlParsingResult(
        query_type=QueryType.UNKNOWN,
        in_tables=[table_a_urn],  # Read from Table_A
        out_tables=[table_b_urn, table_c_urn],  # Modify Table_B and Table_C
        column_lineage=[],
        debug_info=SqlParsingDebugInfo(
            confidence=0.9,
            tables_discovered=3,
            table_schemas_resolved=3,
        ),
    )

    print("\n=== MOCK PARSER RESULT ===")
    print(
        f"Mock parser has {len(mock_parsed_result.in_tables)} inputs: {mock_parsed_result.in_tables}"
    )
    print(
        f"Mock parser has {len(mock_parsed_result.out_tables)} outputs: {mock_parsed_result.out_tables}"
    )

    # Monkey-patch the aggregator's _run_sql_parser to return our mock result
    def mock_parser(*args, **kwargs):  # type: ignore[no-untyped-def]
        return mock_parsed_result

    original_parser = aggregator._run_sql_parser
    aggregator._run_sql_parser = mock_parser  # type: ignore[method-assign]

    # Now add as ObservedQuery to trigger the bug at line 871
    aggregator.add(
        ObservedQuery(
            query="SELECT * FROM test",  # Doesn't matter, we're mocking the parser
            session_id="test_session",
            timestamp=None,
        )
    )

    # Restore original parser
    aggregator._run_sql_parser = original_parser  # type: ignore[method-assign]

    # Generate lineage
    result = list(aggregator.gen_metadata())

    # Extract output datasets from lineage MCPs
    output_datasets = set()
    for mcp in result:
        if mcp.aspect:
            # Check for UpstreamLineage aspect
            if hasattr(mcp.aspect, "upstreams") and mcp.entityUrn:
                # This entityUrn is a downstream table
                output_datasets.add(mcp.entityUrn)

    print("\n=== AGGREGATOR RESULT ===")
    print(f"Aggregator registered {len(output_datasets)} outputs: {output_datasets}")

    # Extract table names for easier comparison
    def extract_table_name(urn: str) -> str:
        parts = urn.split(",")
        if len(parts) >= 2:
            return parts[1].lower()
        return urn.lower()

    output_table_names = {extract_table_name(urn) for urn in output_datasets}

    print(f"Output table names: {sorted(output_table_names)}")
    print("\n=== BUG DEMONSTRATION ===")
    print(
        f"Parser found {len(mock_parsed_result.out_tables)} outputs: {mock_parsed_result.out_tables}"
    )
    print(f"Aggregator registered {len(output_datasets)} outputs: {output_datasets}")

    # BUG: Only the first output table should be registered
    # After fix: All output tables should be registered

    # This assertion will FAIL (demonstrating the bug)
    assert len(output_datasets) == len(mock_parsed_result.out_tables), (
        f"Bug! Parser found {len(mock_parsed_result.out_tables)} outputs "
        f"but aggregator only registered {len(output_datasets)}. "
        f"Missing outputs: {set(mock_parsed_result.out_tables) - output_datasets}"
    )

    # Verify both Table_B and Table_C are in outputs
    assert table_b_urn in output_datasets, (
        "Table_B should be in outputs (parser returned it as output)"
    )
    assert table_c_urn in output_datasets, (
        "Table_C should be in outputs (parser returned it as output)"
    )


def test_multiple_output_tables_bug_fixed():
    """
    This test verifies the fix for the bug where only the first output table was registered.

    Phase 1 Fix (Dec 2024): Loop over all parsed.out_tables instead of taking [0].
    This ensures all output tables from stored procedures show as downstream.
    """
    test_multiple_output_tables_only_first_shows_as_downstream()
