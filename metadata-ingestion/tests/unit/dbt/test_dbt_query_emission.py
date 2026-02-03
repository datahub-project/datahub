"""Tests for dbt meta.queries Query entity emission."""

import logging
from typing import Dict, Optional

import dateutil.parser
import pytest
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode, EmitDirective
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    QueryPropertiesClass,
    QuerySubjectsClass,
)
from datahub.utilities.time import datetime_to_ts_millis

# Standard URN used across tests
TEST_NODE_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"


def _create_dbt_source() -> DBTCoreSource:
    """Create a DBTCoreSource for tests needing custom setup."""
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="postgres",
        enable_meta_mapping=False,
        write_semantics="OVERRIDE",
    )
    return DBTCoreSource(config, ctx)


@pytest.fixture
def dbt_source() -> DBTCoreSource:
    """Fixture providing a test DBTCoreSource instance."""
    return _create_dbt_source()


def _create_dbt_node() -> DBTNode:
    """Create a DBTNode with default meta.queries for tests needing custom setup."""
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


@pytest.fixture
def dbt_node() -> DBTNode:
    """Fixture providing a test DBTNode with meta.queries."""
    return _create_dbt_node()


@freeze_time("2024-01-01 00:00:00")
def test_emits_query_properties_and_subjects_for_each_meta_query():
    """Test that Query entities are correctly emitted from meta.queries."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Generate query entity MCPs
    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

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
    assert query_properties_mcp.aspect.origin == TEST_NODE_URN

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
    assert query_subjects_mcp.aspect.subjects[0].entity == TEST_NODE_URN

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


def test_skips_query_with_missing_required_fields():
    """Test that invalid query definitions are skipped with warnings."""
    source = _create_dbt_source()
    node = _create_dbt_node()
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

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))
    # Only the valid query should produce MCPs (properties + subjects = 2)
    assert len(mcps) == 2
    # Check that report tracks failures
    assert source.report.num_queries_emitted == 1
    assert source.report.num_queries_failed == 8


def test_generates_urn_from_model_and_query_name():
    """Test that query URNs are correctly generated and sanitized."""
    source = _create_dbt_source()
    node = _create_dbt_node()

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # Check that URNs are properly formatted
    first_query_urn = mcps[0].entityUrn
    assert first_query_urn is not None
    assert isinstance(first_query_urn, str)
    assert first_query_urn.startswith("urn:li:query:")
    # URN should preserve dots for uniqueness (model.test.test_model stays as-is)
    assert "model.test.test_model" in first_query_urn


def test_increments_report_counters_on_emit_and_fail():
    """Test that reporting correctly tracks successes and failures."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    node.meta = {
        "queries": [
            {"name": "Valid 1", "sql": "SELECT 1"},
            {"name": "Valid 2", "sql": "SELECT 2"},
            {"name": "Missing SQL"},  # Invalid
            {"name": "Valid 3", "sql": "SELECT 3"},
        ]
    }

    list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    assert source.report.num_queries_emitted == 3
    assert source.report.num_queries_failed == 1
    assert len(source.report.queries_failed_list) == 1


def test_skips_duplicate_query_names(caplog):
    """Test that duplicate query names are skipped (first one wins)."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    node.meta = {
        "queries": [
            {"name": "Same Name", "sql": "SELECT 1"},
            {"name": "Same Name", "sql": "SELECT 2"},  # Duplicate - will be skipped
        ]
    }

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # Only first query is emitted (2 MCPs = 1 query * 2 aspects)
    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1
    assert source.report.num_queries_failed == 1

    # Verify it's the first query that was kept (SELECT 1)
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    assert properties.statement.value == "SELECT 1"

    # Verify warning was logged about URN collision
    assert any("URN collision" in record.message for record in caplog.records)


def test_query_names_collide_after_sanitization(caplog):
    """Different query names that sanitize to same URN should be handled."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    node.meta = {
        "queries": [
            {"name": "Revenue (USD)", "sql": "SELECT 1"},
            {"name": "Revenue [USD]", "sql": "SELECT 2"},  # Collides after sanitization
        ]
    }

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # Only first query emitted (2 MCPs), second skipped due to URN collision
    assert len(mcps) == 2
    assert source.report.num_queries_emitted == 1
    assert source.report.num_queries_failed == 1

    # Verify it's the first query that was kept
    props = mcps[0].aspect
    assert isinstance(props, QueryPropertiesClass)
    assert props.statement.value == "SELECT 1"

    # Verify warning about URN collision
    assert any("URN collision" in record.message for record in caplog.records)


def test_skips_non_list_meta_queries_with_warning(caplog):
    """Test that non-list meta.queries is skipped with a warning and report entry."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Set meta.queries to a string instead of a list
    node.meta = {"queries": "this should be a list, not a string"}

    with caplog.at_level(logging.WARNING):
        mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # No MCPs emitted
    assert len(mcps) == 0
    assert source.report.num_queries_emitted == 0
    # Not counted as failed (it's a structural issue, not validation failure)
    assert source.report.num_queries_failed == 0

    # Warning should be logged
    assert any(
        "expected list" in record.message.lower() and "str" in record.message.lower()
        for record in caplog.records
    )


def test_disabling_query_emission_skips_all_queries_including_valid_ones():
    """Test that disabling query emission completely bypasses query processing."""
    source = _create_dbt_source()
    source.config.entities_enabled.queries = EmitDirective.NO
    node = _create_dbt_node()
    # Node has 2 valid queries defined
    assert len(node.meta["queries"]) == 2

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # No MCPs emitted even though queries are valid
    assert len(mcps) == 0
    # Report counters should remain at zero (not even attempted)
    assert source.report.num_queries_emitted == 0
    assert source.report.num_queries_failed == 0


def test_sql_at_max_length_not_truncated():
    """Test that SQL exactly at 1MB limit is not truncated."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Create SQL exactly at 1MB (1,048,576 bytes)
    max_length = 1 * 1024 * 1024
    long_sql = "X" * max_length
    node.meta = {"queries": [{"name": "long_query", "sql": long_sql}]}

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    assert len(mcps) == 2
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    # Should NOT be truncated (no "..." suffix)
    assert len(properties.statement.value) == max_length
    assert not properties.statement.value.endswith("...")


def test_sql_exceeding_max_length_is_truncated():
    """Test that SQL exceeding 1MB limit is truncated with '...' suffix and marker."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Create SQL just over 1MB
    max_length = 1 * 1024 * 1024
    oversized_sql = "Y" * (max_length + 100)
    node.meta = {"queries": [{"name": "oversized_query", "sql": oversized_sql}]}

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    assert len(mcps) == 2
    properties = mcps[0].aspect
    assert isinstance(properties, QueryPropertiesClass)
    # Should be truncated with "..." suffix
    assert len(properties.statement.value) == max_length + 3  # +3 for "..."
    assert properties.statement.value.endswith("...")
    # Should have sql_truncated marker in customProperties
    assert properties.customProperties is not None
    assert properties.customProperties.get("sql_truncated") == "true"


def test_shows_all_validation_errors_not_just_first():
    """Test that when a query has multiple validation errors, all are reported."""
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Create query missing both name and sql (should report both errors)
    node.meta = {"queries": [{"description": "Query with no name or sql"}]}

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

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


def test_timestamp_cached_across_nodes():
    """Test that timestamp is computed once and cached across multiple nodes."""
    source = _create_dbt_source()
    node1 = _create_dbt_node()
    node2 = _create_dbt_node()
    node2.dbt_name = "model.test.another_model"
    node_urn1 = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.test_model,PROD)"
    node_urn2 = "urn:li:dataset:(urn:li:dataPlatform:dbt,test.another_model,PROD)"

    # No manifest timestamp - will use fallback
    source.report.manifest_info = None

    # Process first node
    mcps1 = list(source._create_query_entity_mcps(node1, node_urn1))
    props1 = mcps1[0].aspect
    assert isinstance(props1, QueryPropertiesClass)
    first_timestamp = props1.created.time

    # Process second node
    mcps2 = list(source._create_query_entity_mcps(node2, node_urn2))
    props2 = mcps2[0].aspect
    assert isinstance(props2, QueryPropertiesClass)
    second_timestamp = props2.created.time

    # Timestamps should be identical (cached)
    assert first_timestamp == second_timestamp
    # Fallback counter should only be incremented once (not per-node)
    assert source.report.query_timestamps_fallback_used is True


def test_rejects_unknown_fields_with_helpful_error():
    """Test that extra="forbid" catches typos like 'query' instead of 'sql'.

    The DBTQueryDefinition model uses extra="forbid" to help users catch typos
    in their dbt schema.yml files. This test verifies that unknown fields are
    rejected with a clear error message.
    """
    source = _create_dbt_source()
    node = _create_dbt_node()
    # Common typo: using 'query' instead of 'sql'
    node.meta = {
        "queries": [
            {
                "name": "Valid query name",
                "query": "SELECT * FROM table",  # Wrong field name - should be 'sql'
            }
        ]
    }

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # Query should be rejected
    assert len(mcps) == 0
    assert source.report.num_queries_failed == 1
    # Error message should mention the unknown field
    failure_msgs = list(source.report.queries_failed_list)
    assert len(failure_msgs) == 1
    # Should mention both the unknown field 'query' and missing required field 'sql'
    failure_msg = failure_msgs[0].lower()
    assert "query" in failure_msg or "extra" in failure_msg
    assert "sql" in failure_msg


@pytest.mark.parametrize(
    "entities_enabled,expected_queries_directive,expected_models_directive",
    [
        # queries=ONLY sets all others to NO, but queries should still emit
        ({"queries": "ONLY"}, EmitDirective.YES, EmitDirective.NO),
        # models=NO but queries=YES should still emit queries
        ({"models": "NO", "queries": "YES"}, EmitDirective.YES, EmitDirective.NO),
    ],
    ids=["queries_only_mode", "models_no_queries_yes"],
)
def test_queries_emit_independently_of_node_type_settings(
    entities_enabled: dict,
    expected_queries_directive: EmitDirective,
    expected_models_directive: EmitDirective,
) -> None:
    """Test that Query entities emit regardless of node type emission settings.

    This verifies the fix for https://github.com/datahub-project/datahub/issues/15150
    where query emission happened after the can_emit_node_type check, causing queries
    to be skipped when node types were disabled.
    """
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="postgres",
        enable_meta_mapping=False,
        write_semantics="OVERRIDE",
        entities_enabled=entities_enabled,
    )
    source = DBTCoreSource(config, ctx)

    # Verify configuration is as expected
    assert source.config.entities_enabled.queries == expected_queries_directive
    assert source.config.entities_enabled.models == expected_models_directive
    assert source.config.entities_enabled.can_emit_queries is True
    assert source.config.entities_enabled.can_emit_node_type("model") is False

    # Test that queries ARE actually emitted
    node = _create_dbt_node()

    mcps = list(source._create_query_entity_mcps(node, TEST_NODE_URN))

    # Should emit queries (2 queries * 2 aspects = 4 MCPs)
    assert len(mcps) == 4
    assert source.report.num_queries_emitted == 2


def test_skips_queries_on_ephemeral_model_with_warning(
    dbt_source: DBTCoreSource, dbt_node: DBTNode, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that queries on ephemeral models are skipped with a warning.

    Ephemeral models don't exist in the target platform, so there's no dataset
    to link queries to. This should be clearly communicated via warning.
    """
    dbt_node.materialization = "ephemeral"

    with caplog.at_level(logging.WARNING):
        mcps = list(dbt_source._create_query_entity_mcps(dbt_node, TEST_NODE_URN))

    # No queries emitted for ephemeral models
    assert len(mcps) == 0
    assert dbt_source.report.num_queries_emitted == 0
    assert dbt_source.report.num_queries_failed == 0

    # Warning should be logged explaining why
    assert any(
        "ephemeral" in record.message.lower() and dbt_node.dbt_name in record.message
        for record in caplog.records
    )


@pytest.mark.parametrize(
    "max_queries,query_count,expected_emitted",
    [
        (10, 10, 10),  # At limit - all processed
        (5, 10, 5),  # Over limit - truncated
        (0, 150, 150),  # Unlimited (0 means no limit)
    ],
    ids=["at_limit", "over_limit", "unlimited"],
)
def test_max_queries_limit_behavior(
    dbt_source: DBTCoreSource,
    dbt_node: DBTNode,
    max_queries: int,
    query_count: int,
    expected_emitted: int,
) -> None:
    """Test max_queries_per_model config limits query processing."""
    dbt_source.config.max_queries_per_model = max_queries
    dbt_node.meta = {
        "queries": [
            {"name": f"query_{i}", "sql": f"SELECT {i}"} for i in range(query_count)
        ]
    }

    list(dbt_source._create_query_entity_mcps(dbt_node, TEST_NODE_URN))

    assert dbt_source.report.num_queries_emitted == expected_emitted


@pytest.mark.parametrize(
    "manifest_info,expect_fallback",
    [
        ({"generated_at": "2024-06-15T10:30:00Z"}, False),  # Valid timestamp
        (None, True),  # No manifest info
        ({"generated_at": "unknown"}, True),  # Unknown value
        ({"generated_at": "not-a-date"}, True),  # Unparseable
    ],
    ids=["valid_timestamp", "no_manifest", "unknown_value", "unparseable"],
)
def test_timestamp_fallback_behavior(
    dbt_source: DBTCoreSource,
    dbt_node: DBTNode,
    manifest_info: Optional[Dict[str, str]],
    expect_fallback: bool,
) -> None:
    """Test timestamp handling with various manifest_info states."""
    dbt_source.report.manifest_info = manifest_info

    mcps = list(dbt_source._create_query_entity_mcps(dbt_node, TEST_NODE_URN))

    assert dbt_source.report.query_timestamps_fallback_used is expect_fallback

    # For valid timestamp, also verify the actual value is used correctly
    if not expect_fallback and manifest_info:
        expected_ts = datetime_to_ts_millis(
            dateutil.parser.parse(manifest_info["generated_at"])
        )
        properties = mcps[0].aspect
        assert isinstance(properties, QueryPropertiesClass)
        assert properties.created.time == expected_ts
        assert properties.lastModified.time == expected_ts
