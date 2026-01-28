"""Unit tests for BatchSchemaFetcher with synchronous batch fetching."""

from unittest.mock import MagicMock

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import BatchSchemaFetcher, SchemaResolver


@pytest.fixture
def mock_graph():
    """Create a mock DataHubGraph."""
    graph = MagicMock(spec=DataHubGraph)
    return graph


@pytest.fixture
def schema_resolver(mock_graph):
    """Create a SchemaResolver with mock graph."""
    resolver = SchemaResolver(
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
        graph=mock_graph,
    )
    return resolver


@pytest.fixture
def batch_fetcher(mock_graph):
    """Create a BatchSchemaFetcher."""
    return BatchSchemaFetcher(graph=mock_graph, batch_size=50)


def create_mock_schema(columns: list[tuple[str, str]]) -> SchemaMetadataClass:
    """Helper to create a mock schema."""
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=MagicMock(),
        fields=[
            SchemaFieldClass(
                fieldPath=col_name,
                nativeDataType=col_type,
                type=MagicMock(),
            )
            for col_name, col_type in columns
        ],
    )


def test_batch_fetcher_single_batch(batch_fetcher, schema_resolver, mock_graph):
    """Test fetching schemas in a single batch."""
    # Setup
    urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table3,PROD)",
    ]

    # Mock the batch fetch response
    mock_graph.get_entities.return_value = {
        urns[0]: {
            "schemaMetadata": (
                create_mock_schema([("col1", "string"), ("col2", "int")]),
                {},
            )
        },
        urns[1]: {
            "schemaMetadata": (
                create_mock_schema([("colA", "string"), ("colB", "float")]),
                {},
            )
        },
        # urns[2] not in response (simulating not found)
    }

    # Execute
    results = batch_fetcher.fetch_schemas(urns, schema_resolver)

    # Verify
    assert len(results) == 3
    assert results[urns[0]] is not None
    assert "col1" in results[urns[0]]
    assert results[urns[0]]["col1"] == "string"
    assert "col2" in results[urns[0]]

    assert results[urns[1]] is not None
    assert "colA" in results[urns[1]]
    assert results[urns[1]]["colA"] == "string"

    assert results[urns[2]] is None  # Not found in DataHub

    # Verify cache was populated
    assert schema_resolver._schema_cache.get(urns[0]) is not None
    assert schema_resolver._schema_cache.get(urns[1]) is not None
    assert schema_resolver._schema_cache.get(urns[2]) is None

    # Verify stats
    stats = batch_fetcher.get_stats()
    assert stats["api_calls"] == 1  # Only one batch call
    assert stats["fetched"] == 2  # Two schemas found


def test_batch_fetcher_multiple_batches(batch_fetcher, schema_resolver, mock_graph):
    """Test fetching schemas across multiple batches."""
    # Create 100 URNs (will require 2 batches with batch_size=50)
    urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table{i},PROD)"
        for i in range(100)
    ]

    # Mock response for all URNs
    mock_graph.get_entities.return_value = {
        urn: {
            "schemaMetadata": (
                create_mock_schema([("id", "int")]),
                {},
            )
        }
        for urn in urns
    }

    # Execute
    results = batch_fetcher.fetch_schemas(urns, schema_resolver)

    # Verify
    assert len(results) == 100
    assert all(result is not None for result in results.values())

    # Verify two batch calls were made
    assert mock_graph.get_entities.call_count == 2
    stats = batch_fetcher.get_stats()
    assert stats["api_calls"] == 2
    assert stats["fetched"] == 100


def test_batch_fetcher_skip_cached(batch_fetcher, schema_resolver, mock_graph):
    """Test that cached URNs are skipped during batch fetch."""
    urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table3,PROD)",
    ]

    # Pre-populate cache with table1 and table2
    schema_resolver.add_schema_metadata(
        urns[0], create_mock_schema([("cached_col", "string")])
    )
    schema_resolver._save_to_cache(urns[1], None)  # Cached as not found

    # Mock only responds for table3
    mock_graph.get_entities.return_value = {
        urns[2]: {
            "schemaMetadata": (
                create_mock_schema([("new_col", "int")]),
                {},
            )
        }
    }

    # Execute
    results = batch_fetcher.fetch_schemas(urns, schema_resolver)

    # Verify
    assert len(results) == 1  # Only table3 was fetched
    assert urns[2] in results
    assert results[urns[2]] is not None
    assert "new_col" in results[urns[2]]

    # Verify get_entities was called only with table3
    mock_graph.get_entities.assert_called_once()
    call_args = mock_graph.get_entities.call_args
    assert call_args[1]["urns"] == [urns[2]]

    # Verify cached values still intact
    assert schema_resolver._schema_cache.get(urns[0]) is not None
    assert "cached_col" in schema_resolver._schema_cache[urns[0]]


def test_batch_fetcher_cache_precedence(batch_fetcher, schema_resolver, mock_graph):
    """Test that ingestion schemas take precedence over fetched schemas."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)"

    # Add schema from ingestion (fresh)
    ingestion_schema = create_mock_schema([("fresh_col", "string")])
    schema_resolver.add_schema_metadata(urn, ingestion_schema)

    # Try to fetch (should be skipped due to cache precedence)
    mock_graph.get_entities.return_value = {
        urn: {
            "schemaMetadata": (
                create_mock_schema([("stale_col", "int")]),
                {},
            )
        }
    }

    results = batch_fetcher.fetch_schemas([urn], schema_resolver)

    # Verify
    assert len(results) == 0  # Skipped due to cache
    mock_graph.get_entities.assert_not_called()

    # Verify ingestion schema is still in cache
    cached_schema = schema_resolver._schema_cache.get(urn)
    assert cached_schema is not None
    assert "fresh_col" in cached_schema
    assert "stale_col" not in cached_schema


def test_batch_fetcher_error_fallback(batch_fetcher, schema_resolver, mock_graph):
    """Test fallback to individual fetches on batch error."""
    urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
    ]

    # Mock batch fetch to fail
    mock_graph.get_entities.side_effect = Exception("Batch fetch failed")

    # Mock individual fetches to succeed
    mock_graph.get_aspect.side_effect = [
        create_mock_schema([("col1", "string")]),
        create_mock_schema([("col2", "int")]),
    ]

    # Execute
    results = batch_fetcher.fetch_schemas(urns, schema_resolver)

    # Verify fallback worked
    assert len(results) == 2
    assert results[urns[0]] is not None
    assert "col1" in results[urns[0]]
    assert results[urns[1]] is not None
    assert "col2" in results[urns[1]]

    # Verify both individual fetches were called
    assert mock_graph.get_aspect.call_count == 2

    # Verify stats reflect individual fetches
    stats = batch_fetcher.get_stats()
    assert stats["api_calls"] == 3  # 1 failed batch + 2 individual
    assert stats["fetched"] == 2


def test_resolve_table_with_batch_fetcher(schema_resolver, mock_graph):
    """Test resolve_table with batch fetcher (improved casing strategy)."""
    # Setup batch fetcher
    batch_fetcher = BatchSchemaFetcher(graph=mock_graph, batch_size=50)
    schema_resolver.batch_fetcher = batch_fetcher

    # Mock batch fetch to return lowercase version only
    def mock_get_entities(entity_name, urns, aspects, with_system_metadata):
        # Return schema for lowercase URN only
        return {
            urn: {
                "schemaMetadata": (
                    create_mock_schema([("id", "int"), ("name", "string")]),
                    {},
                )
            }
            for urn in urns
            if urn.endswith("table,PROD)")  # Lowercase version
        }

    mock_graph.get_entities.side_effect = mock_get_entities

    # Execute
    table = _TableName(database="db", db_schema="schema", table="TABLE")  # Mixed case
    resolved_urn, schema_info = schema_resolver.resolve_table(table)

    # Verify
    assert schema_info is not None
    assert "id" in schema_info
    assert "name" in schema_info

    # Verify batch fetch was called (not 3 individual calls)
    mock_graph.get_entities.assert_called_once()

    # Verify all URN variations were tried in one batch
    call_args = mock_graph.get_entities.call_args[1]
    urns_fetched = call_args["urns"]
    assert len(urns_fetched) >= 2  # At least primary and lowercase


def test_resolve_table_batch_optimization(schema_resolver, mock_graph):
    """Test that resolve_table fetches all URN variations in one batch API call."""
    batch_fetcher = BatchSchemaFetcher(graph=mock_graph, batch_size=50)
    schema_resolver.batch_fetcher = batch_fetcher

    # Mock response (no schema found for any variation)
    mock_graph.get_entities.return_value = {}

    # Execute
    table = _TableName(database="DB", db_schema="SCHEMA", table="TABLE")
    resolved_urn, schema_info = schema_resolver.resolve_table(table)

    # Verify
    assert schema_info is None  # No schema found

    # Verify only ONE batch call was made (not 3 individual calls)
    assert mock_graph.get_entities.call_count == 1

    # Verify all URN variations were tried in that one call
    call_args = mock_graph.get_entities.call_args[1]
    urns_tried = call_args["urns"]

    # Should have tried multiple casing variations
    assert len(urns_tried) >= 2  # At least primary and lowercase

    # Verify URNs are unique
    assert len(urns_tried) == len(set(urns_tried))


def test_batch_fetcher_empty_list(batch_fetcher, schema_resolver, mock_graph):
    """Test batch fetcher with empty URN list."""
    results = batch_fetcher.fetch_schemas([], schema_resolver)

    assert results == {}
    mock_graph.get_entities.assert_not_called()


def test_batch_fetcher_all_cached(batch_fetcher, schema_resolver, mock_graph):
    """Test batch fetcher when all URNs are already cached."""
    urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)",
    ]

    # Pre-populate cache
    for urn in urns:
        schema_resolver.add_schema_metadata(
            urn, create_mock_schema([("cached", "string")])
        )

    # Execute
    results = batch_fetcher.fetch_schemas(urns, schema_resolver)

    # Verify
    assert results == {}  # Nothing new to fetch
    mock_graph.get_entities.assert_not_called()
