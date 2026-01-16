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
def test_emits_query_properties_and_subjects_for_each_meta_query():
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


def test_skips_emission_when_no_queries_defined():
    """Test that no Query entities are emitted when meta.queries is absent."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {}  # No queries
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))
    assert len(mcps) == 0


def test_skips_query_with_missing_required_fields():
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


def test_generates_urn_from_model_and_query_name():
    """Test that query URNs are correctly generated and sanitized."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Check that URNs are properly formatted
    first_query_urn = mcps[0].entityUrn
    assert first_query_urn is not None
    assert isinstance(first_query_urn, str)
    assert first_query_urn.startswith("urn:li:query:")
    # URN should preserve dots for uniqueness (model.test.test_model stays as-is)
    assert "model.test.test_model" in first_query_urn


def test_stores_tags_and_terms_in_custom_properties():
    """Test that tags and terms lists are converted to comma-separated strings."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Test query",
                "sql": "SELECT 1",
                "tags": ["production", "analytics", "team-data"],
                "terms": ["CustomerData", "PII"],
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    properties_mcp = mcps[0]
    assert isinstance(properties_mcp.aspect, QueryPropertiesClass)
    assert properties_mcp.aspect.customProperties is not None

    # Tags and terms should be comma-separated strings
    assert (
        properties_mcp.aspect.customProperties["tags"]
        == "production, analytics, team-data"
    )
    assert properties_mcp.aspect.customProperties["terms"] == "CustomerData, PII"


def test_ignores_non_list_tags_and_terms():
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


def test_ignores_non_string_description():
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
    assert query_properties is not None
    assert isinstance(query_properties, QueryPropertiesClass)
    assert query_properties.description is None
    assert source.report.num_queries_emitted == 1


def test_increments_report_counters_on_emit_and_fail():
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


def test_node_meta_is_none():
    """Test that None meta is handled gracefully without errors."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = None  # type: ignore
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 0
    assert source.report.num_queries_emitted == 0
    assert source.report.num_queries_failed == 0


def test_meta_queries_not_a_list():
    """Test that non-list meta.queries (string, dict) is gracefully skipped."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    # Test with string
    node.meta = {"queries": "not a list"}
    mcps = list(source._create_query_entity_mcps(node, node_urn))
    assert len(mcps) == 0

    # Test with dict
    node.meta = {"queries": {"name": "query", "sql": "SELECT 1"}}
    mcps = list(source._create_query_entity_mcps(node, node_urn))
    assert len(mcps) == 0

    # Test with number
    node.meta = {"queries": 123}
    mcps = list(source._create_query_entity_mcps(node, node_urn))
    assert len(mcps) == 0

    assert source.report.num_queries_emitted == 0


def test_empty_queries_list():
    """Test that empty queries list emits nothing."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {"queries": []}
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 0
    assert source.report.num_queries_emitted == 0
    assert source.report.num_queries_failed == 0


def test_warns_on_duplicate_query_names_same_urn(caplog):
    """Test that duplicate query names emit a warning and create same URN (last wins)."""
    import logging

    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {"name": "Same Name", "sql": "SELECT 1"},
            {"name": "Same Name", "sql": "SELECT 2"},  # Duplicate name
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Both queries are emitted (4 MCPs = 2 queries * 2 aspects)
    assert len(mcps) == 4
    assert source.report.num_queries_emitted == 2

    # Both have the same URN (last one wins in DataHub)
    first_urn = mcps[0].entityUrn
    third_urn = mcps[2].entityUrn  # Second query's properties
    assert first_urn == third_urn

    # Verify warning was logged about duplicate
    assert any("Duplicate query" in record.message for record in caplog.records)


def test_sanitizes_special_characters_in_urn():
    """Test that special characters in query names are sanitized in URN."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Query with spaces & special (chars)!",
                "sql": "SELECT 1",
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    query_urn = mcps[0].entityUrn
    assert query_urn is not None
    # Special characters should be replaced with underscores
    assert "urn:li:query:" in query_urn
    assert "&" not in query_urn
    assert "(" not in query_urn
    assert ")" not in query_urn
    assert "!" not in query_urn


def test_handles_unicode_in_name_and_sql():
    """Test that Unicode characters in query names are handled."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "客户查询 (Customer Query)",
                "description": "Requête des données clients",
                "sql": "SELECT * FROM customers WHERE région = 'Île-de-France'",
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1

    # Check that the name and description are preserved
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    assert properties.name == "客户查询 (Customer Query)"
    assert properties.description == "Requête des données clients"
    # SQL should be preserved as-is
    assert "région" in properties.statement.value


def test_accepts_very_long_query_names():
    """Test that very long query names are handled."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    long_name = "A" * 500  # 500 character name
    node.meta = {
        "queries": [
            {
                "name": long_name,
                "sql": "SELECT 1",
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1

    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    assert properties.name == long_name
    assert len(properties.name) == 500


def test_respects_enable_query_entity_emission_config_flag():
    """Test that query emission can be disabled via config."""
    source = create_test_dbt_source()
    source.config.enable_query_entity_emission = False
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 0
    assert source.report.num_queries_emitted == 0


def test_filters_empty_and_none_values_from_tags_list():
    """Test that empty/None values in tags list are filtered out."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {
                "name": "Test Query",
                "sql": "SELECT 1",
                "tags": ["valid", "", None, "  ", "another_valid"],
                "terms": ["Term1", None, "", "Term2"],
            }
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    # Empty/None values should be filtered out
    assert properties.customProperties is not None
    assert properties.customProperties["tags"] == "valid, another_valid"
    assert properties.customProperties["terms"] == "Term1, Term2"
