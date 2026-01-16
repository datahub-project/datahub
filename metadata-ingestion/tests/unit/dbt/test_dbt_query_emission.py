"""Tests for dbt meta.queries Query entity emission."""

import logging

import dateutil.parser
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode, EmitDirective
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    QueryPropertiesClass,
    QuerySubjectsClass,
)
from datahub.utilities.time import datetime_to_ts_millis


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


def test_skips_duplicate_query_names_to_prevent_data_loss(caplog):
    """Test that duplicate query names are skipped (first one wins)."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node.meta = {
        "queries": [
            {"name": "Same Name", "sql": "SELECT 1"},
            {"name": "Same Name", "sql": "SELECT 2"},  # Duplicate - will be skipped
        ]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Only first query is emitted (2 MCPs = 1 query * 2 aspects)
    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1
    assert source.report.num_queries_failed == 1

    # Verify it's the first query that was kept (SELECT 1)
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    assert properties.statement.value == "SELECT 1"

    # Verify warning was logged about duplicate being skipped
    assert any(
        "Duplicate query" in record.message and "skipped" in record.message
        for record in caplog.records
    )


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


def test_disabling_query_emission_skips_all_queries_including_valid_ones():
    """Test that disabling query emission completely bypasses query processing."""
    source = create_test_dbt_source()
    source.config.entities_enabled.queries = EmitDirective.NO
    node = create_test_dbt_node_with_queries()
    # Node has 2 valid queries defined
    assert len(node.meta["queries"]) == 2
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # No MCPs emitted even though queries are valid
    assert len(mcps) == 0
    # Report counters should remain at zero (not even attempted)
    assert source.report.num_queries_emitted == 0
    assert source.report.num_queries_failed == 0


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


def test_exactly_max_queries_processes_all():
    """Test that exactly max_queries_per_model queries are all processed."""
    source = create_test_dbt_source()
    source.config.max_queries_per_model = 10  # Set a smaller limit for testing
    node = create_test_dbt_node_with_queries()
    # Create exactly 10 queries
    node.meta = {
        "queries": [{"name": f"query_{i}", "sql": f"SELECT {i}"} for i in range(10)]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # All 10 queries should be processed (2 MCPs each: properties + subjects)
    assert len(mcps) == 20
    assert source.report.num_queries_emitted == 10


def test_exceeds_max_queries_truncates_with_warning():
    """Test that exceeding max_queries_per_model truncates the list."""
    source = create_test_dbt_source()
    source.config.max_queries_per_model = 5  # Set a smaller limit for testing
    node = create_test_dbt_node_with_queries()
    # Create 10 queries (exceeds limit of 5)
    node.meta = {
        "queries": [{"name": f"query_{i}", "sql": f"SELECT {i}"} for i in range(10)]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Only first 5 queries should be processed (2 MCPs each)
    assert len(mcps) == 10
    assert source.report.num_queries_emitted == 5


def test_unlimited_queries_when_max_is_zero():
    """Test that setting max_queries_per_model=0 allows unlimited queries."""
    source = create_test_dbt_source()
    source.config.max_queries_per_model = 0  # Unlimited
    node = create_test_dbt_node_with_queries()
    # Create 150 queries (more than default 100)
    node.meta = {
        "queries": [{"name": f"query_{i}", "sql": f"SELECT {i}"} for i in range(150)]
    }
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    # All 150 queries should be processed (2 MCPs each)
    assert len(mcps) == 300
    assert source.report.num_queries_emitted == 150


def test_sql_at_max_length_not_truncated():
    """Test that SQL exactly at 1MB limit is not truncated."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    # Create SQL exactly at 1MB (1,048,576 bytes)
    max_length = 1 * 1024 * 1024
    long_sql = "X" * max_length
    node.meta = {"queries": [{"name": "long_query", "sql": long_sql}]}
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    # Should NOT be truncated (no "..." suffix)
    assert len(properties.statement.value) == max_length
    assert not properties.statement.value.endswith("...")


def test_sql_exceeding_max_length_is_truncated():
    """Test that SQL exceeding 1MB limit is truncated with '...' suffix."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    # Create SQL just over 1MB
    max_length = 1 * 1024 * 1024
    oversized_sql = "Y" * (max_length + 100)
    node.meta = {"queries": [{"name": "oversized_query", "sql": oversized_sql}]}
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 2
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    # Should be truncated with "..." suffix
    assert len(properties.statement.value) == max_length + 3  # +3 for "..."
    assert properties.statement.value.endswith("...")


def test_shows_all_validation_errors_not_just_first():
    """Test that when a query has multiple validation errors, all are reported."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    # Create query missing both name and sql (should report both errors)
    node.meta = {"queries": [{"description": "Query with no name or sql"}]}
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 0
    assert source.report.num_queries_failed == 1
    # Check that failure list contains both errors
    # LossyList yields values when iterated
    failure_msgs = list(source.report.queries_failed_list)
    assert len(failure_msgs) == 1
    failure_msg = failure_msgs[0]
    # Both 'name' and 'sql' errors should be present
    assert "name" in failure_msg.lower()
    assert "sql" in failure_msg.lower()


def test_uses_manifest_timestamp_when_available():
    """Test that manifest generated_at timestamp is used for Query entities."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    manifest_timestamp = "2024-06-15T10:30:00Z"
    # Calculate expected timestamp using same method as source
    expected_ts = datetime_to_ts_millis(dateutil.parser.parse(manifest_timestamp))

    # Set manifest_info with the timestamp
    source.report.manifest_info = {
        "generated_at": manifest_timestamp,
        "dbt_version": "1.5.0",
        "project_name": "test_project",
    }

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 4  # 2 queries * 2 aspects
    # Verify the timestamp matches what we expect from manifest
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    assert properties.created.time == expected_ts
    assert properties.lastModified.time == expected_ts
    # Should not increment fallback counter
    assert source.report.queries_using_fallback_timestamp == 0


def test_uses_fallback_timestamp_when_manifest_unavailable():
    """Test that current time is used when manifest timestamp is unavailable."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    # No manifest_info set (simulates missing manifest metadata)
    source.report.manifest_info = None

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 4
    # Verify fallback counter was incremented
    assert source.report.queries_using_fallback_timestamp == 1


def test_uses_fallback_timestamp_when_generated_at_is_unknown():
    """Test that current time is used when generated_at is 'unknown'."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    # Set manifest_info with unknown timestamp
    source.report.manifest_info = {
        "generated_at": "unknown",
        "dbt_version": "1.5.0",
    }

    mcps = list(source._create_query_entity_mcps(node, node_urn))

    assert len(mcps) == 4
    # Should use fallback
    assert source.report.queries_using_fallback_timestamp == 1


def test_timestamp_cached_across_nodes():
    """Test that timestamp is computed once and cached across multiple nodes."""
    source = create_test_dbt_source()
    node1 = create_test_dbt_node_with_queries()
    node2 = create_test_dbt_node_with_queries()
    node2.dbt_name = "model.test.another_model"
    node_urn1 = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"
    node_urn2 = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.another_model,PROD)"

    # No manifest timestamp - will use fallback
    source.report.manifest_info = None

    # Process first node
    mcps1 = list(source._create_query_entity_mcps(node1, node_urn1))
    first_timestamp = mcps1[0].aspect.created.time

    # Process second node
    mcps2 = list(source._create_query_entity_mcps(node2, node_urn2))
    second_timestamp = mcps2[0].aspect.created.time

    # Timestamps should be identical (cached)
    assert first_timestamp == second_timestamp
    # Fallback counter should only be incremented once (not per-node)
    assert source.report.queries_using_fallback_timestamp == 1


def test_warns_and_uses_fallback_on_malformed_generated_at(caplog):
    """Test that malformed generated_at logs warning and uses fallback timestamp."""
    source = create_test_dbt_source()
    node = create_test_dbt_node_with_queries()
    node_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"

    # Set manifest_info with malformed timestamp
    source.report.manifest_info = {
        "generated_at": "not-a-valid-date-format",
        "dbt_version": "1.5.0",
    }

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, node_urn))

    # Should still emit queries using fallback
    assert len(mcps) == 4
    assert source.report.queries_using_fallback_timestamp == 1

    # Verify warning was logged about parse failure
    assert any(
        "Failed to parse manifest timestamp" in record.message
        and "not-a-valid-date-format" in record.message
        for record in caplog.records
    )
