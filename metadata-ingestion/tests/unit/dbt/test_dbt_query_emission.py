"""Tests for dbt meta.queries Query entity emission."""

from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    QueryPropertiesClass,
    QuerySubjectsClass,
)


def create_test_dbt_node_with_queries() -> DBTNode:
    """Create a test DBTNode with meta.queries."""
    return DBTNode(
        database="test_db",
        schema="test_schema",
        name="test_model",
        alias=None,
        comment="Test model comment",
        description="Test model description",
        language="sql",
        raw_code="SELECT * FROM source_table",
        dbt_adapter="postgres",
        dbt_name="model.test.test_model",
        dbt_file_path="models/test_model.sql",
        dbt_package_name="test",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
        meta={
            "queries": [
                {
                    "name": "Active customers (30d)",
                    "description": "Standard engagement pull",
                    "sql": "SELECT * FROM test_model WHERE active = true AND last_seen > CURRENT_DATE - INTERVAL '30 days'",
                    "tags": ["production", "analytics"],
                    "terms": ["CustomerData", "Engagement"],
                },
                {
                    "name": "Revenue by customer",
                    "sql": "SELECT customer_id, SUM(amount) as total_revenue FROM test_model GROUP BY customer_id",
                },
            ]
        },
        query_tag={},
        tags=[],
        compiled_code="SELECT * FROM source_table",
    )


def create_test_dbt_source() -> DBTCoreSource:
    """Create a test DBTCoreSource instance."""
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="postgres",
        enable_meta_mapping=False,
        write_semantics="OVERRIDE",
    )
    return DBTCoreSource(config, ctx)


@freeze_time("2024-01-01 00:00:00")
def test_query_entity_emission_from_meta_queries():
    """Test that Query entities are correctly emitted from meta.queries."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    # Generate query entity MCPs
    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # We should have 2 queries * 2 aspects each (properties, subjects)
    # Query 1: properties (with customProperties for tags/terms), subjects = 2 aspects
    # Query 2: properties, subjects = 2 aspects
    # Total = 4 aspects
    assert len(mcps) == 4, f"Expected 4 MCPs, got {len(mcps)}"

    # Check first query properties
    query_properties_mcp = mcps[0]
    assert isinstance(query_properties_mcp.aspect, QueryPropertiesClass)
    assert query_properties_mcp.aspect.name == "Active customers (30d)"
    assert query_properties_mcp.aspect.description == "Standard engagement pull"
    assert "WHERE active = true" in query_properties_mcp.aspect.statement.value
    assert query_properties_mcp.aspect.origin == node_urn

    # Check that tags and terms are in customProperties
    assert query_properties_mcp.aspect.customProperties is not None
    assert "tags" in query_properties_mcp.aspect.customProperties
    assert "terms" in query_properties_mcp.aspect.customProperties
    assert (
        query_properties_mcp.aspect.customProperties["tags"] == "production, analytics"
    )
    assert (
        query_properties_mcp.aspect.customProperties["terms"]
        == "CustomerData, Engagement"
    )

    # Check first query subjects
    query_subjects_mcp = mcps[1]
    assert isinstance(query_subjects_mcp.aspect, QuerySubjectsClass)
    assert len(query_subjects_mcp.aspect.subjects) == 1
    assert query_subjects_mcp.aspect.subjects[0].entity == node_urn

    # Check second query properties (no description, tags, or terms)
    second_query_properties_mcp = mcps[2]
    assert isinstance(second_query_properties_mcp.aspect, QueryPropertiesClass)
    assert second_query_properties_mcp.aspect.name == "Revenue by customer"
    # No customProperties for second query since it has no tags/terms
    assert (
        second_query_properties_mcp.aspect.customProperties is None
        or len(second_query_properties_mcp.aspect.customProperties) == 0
    )
    assert second_query_properties_mcp.aspect.description is None
    assert "SUM(amount)" in second_query_properties_mcp.aspect.statement.value

    # Check second query subjects
    second_query_subjects_mcp = mcps[3]
    assert isinstance(second_query_subjects_mcp.aspect, QuerySubjectsClass)


def test_query_entity_emission_with_no_queries():
    """Test that no Query entities are emitted when meta.queries is absent."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {}  # No queries
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))
    assert len(mcps) == 0


def test_query_entity_emission_with_invalid_query():
    """Test that invalid query definitions are skipped with warnings."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {"name": "Valid query", "sql": "SELECT 1"},
            {"name": "Missing SQL"},  # Invalid: no SQL
            {"sql": "SELECT 2"},  # Invalid: no name
            "not a dict",  # Invalid: not a dict
            {"name": "", "sql": "SELECT 3"},  # Invalid: empty name
            {"name": "Empty SQL", "sql": ""},  # Invalid: empty SQL
            {"name": "   ", "sql": "SELECT 4"},  # Invalid: whitespace-only name
            {"name": 123, "sql": "SELECT 5"},  # Invalid: non-string name
            {"name": "Non-string SQL", "sql": 456},  # Invalid: non-string SQL
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))
    # Only the valid query should produce MCPs (properties + subjects = 2)
    assert len(mcps) == 2
    # Check that report tracks failures
    assert source.report.num_queries_emitted == 1
    assert source.report.num_queries_failed == 8


def test_query_urn_generation():
    """Test that query URNs are correctly generated and sanitized."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Check that URNs are properly formatted
    first_query_urn = mcps[0].entityUrn
    assert first_query_urn.startswith("urn:li:query:")
    # URN should have special characters replaced with underscores
    assert "model_test_test_model" in first_query_urn


def test_query_entity_with_invalid_tags_and_terms():
    """Test that tags and terms are converted to comma-separated strings in customProperties."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Test query",
                "sql": "SELECT 1",
                "tags": [
                    "valid_tag",
                    "",
                    "  ",
                    123,
                    None,
                    "another_valid",
                ],  # Mix of valid and invalid
                "terms": [
                    "ValidTerm",
                    "",
                    None,
                    456,
                    "AnotherTerm",
                ],  # Mix of valid and invalid
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Should have: properties (with customProperties), subjects = 2 MCPs
    assert len(mcps) == 2

    # Check that tags and terms are in customProperties as comma-separated strings
    properties_mcp = mcps[0]
    assert isinstance(properties_mcp.aspect, QueryPropertiesClass)
    assert properties_mcp.aspect.customProperties is not None

    # All items are converted to strings and joined
    assert "tags" in properties_mcp.aspect.customProperties
    assert "terms" in properties_mcp.aspect.customProperties
    # Should include all items (even empty strings and numbers converted to strings)
    assert "valid_tag" in properties_mcp.aspect.customProperties["tags"]
    assert "another_valid" in properties_mcp.aspect.customProperties["tags"]
    assert "ValidTerm" in properties_mcp.aspect.customProperties["terms"]
    assert "AnotherTerm" in properties_mcp.aspect.customProperties["terms"]


def test_query_entity_with_non_list_tags_and_terms():
    """Test that non-list tags and terms are handled gracefully."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Test query",
                "sql": "SELECT 1",
                "tags": "not_a_list",  # Invalid: should be list
                "terms": {"not": "a list"},  # Invalid: should be list
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Should only have properties and subjects (no tags or terms)
    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1


def test_query_entity_with_invalid_description():
    """Test that invalid description types are handled."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Test query",
                "sql": "SELECT 1",
                "description": 12345,  # Invalid: should be string
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Should still emit query but without description
    assert len(mcps) == 2
    query_properties = mcps[0].aspect
    assert query_properties.description is None
    assert source.report.num_queries_emitted == 1


def test_query_entity_reporting():
    """Test that reporting correctly tracks successes and failures."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {"name": "Valid 1", "sql": "SELECT 1"},
            {"name": "Valid 2", "sql": "SELECT 2"},
            {"name": "Missing SQL"},  # Invalid
            {"name": "Valid 3", "sql": "SELECT 3"},
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    list(source._create_query_entity_mcps(node, node_urn))

    assert source.report.num_queries_emitted == 3
    assert source.report.num_queries_failed == 1
    assert len(source.report.queries_failed_list) == 1
