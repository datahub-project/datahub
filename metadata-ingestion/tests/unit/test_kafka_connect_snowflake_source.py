"""
Tests for Snowflake Source Connector lineage extraction.

This module tests the SnowflakeSourceConnector class which handles
lineage extraction for Confluent Cloud Snowflake Source connectors.
"""

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    SNOWFLAKE_SOURCE_CLOUD,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import (
    ConnectorRegistry,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    SnowflakeSourceConnector,
)


@pytest.fixture
def config():
    """Create test configuration."""
    return KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )


@pytest.fixture
def report():
    """Create test report."""
    return KafkaConnectSourceReport()


def test_snowflake_source_connector_supports_class():
    """Test that SnowflakeSourceConnector recognizes correct connector class."""
    assert SnowflakeSourceConnector.supports_connector_class(SNOWFLAKE_SOURCE_CLOUD)
    assert SnowflakeSourceConnector.supports_connector_class("SnowflakeSource")
    assert not SnowflakeSourceConnector.supports_connector_class("SnowflakeSink")
    assert not SnowflakeSourceConnector.supports_connector_class("PostgresCdcSource")


def test_snowflake_source_connector_platform(config, report):
    """Test that SnowflakeSourceConnector returns correct platform."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={"connector.class": SNOWFLAKE_SOURCE_CLOUD},
        tasks=[],
    )
    connector = SnowflakeSourceConnector(manifest, config, report)
    platform = connector.get_platform()
    assert platform == "snowflake"


def test_snowflake_source_parser_basic(config, report):
    """Test basic parsing of Snowflake Source connector configuration."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    parser = connector.get_parser(manifest)

    assert parser.source_platform == "snowflake"
    assert parser.database_name == "ANALYTICS"
    assert parser.topic_prefix == "snowflake_"
    assert len(parser.table_names) == 2
    assert "ANALYTICS.PUBLIC.USERS" in parser.table_names
    assert "ANALYTICS.PUBLIC.ORDERS" in parser.table_names


def test_snowflake_source_get_topics_from_config(config, report):
    """Test topic generation from Snowflake Source configuration."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    topics = connector.get_topics_from_config()

    assert len(topics) == 2
    # Topics are lowercased to match DataHub's normalization
    assert "snowflake_analytics.public.users" in topics
    assert "snowflake_analytics.public.orders" in topics


def test_snowflake_source_get_topics_without_prefix(config, report):
    """Test topic generation without topic prefix."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    topics = connector.get_topics_from_config()

    assert len(topics) == 2
    # Topics are lowercased to match DataHub's normalization
    assert "analytics.public.users" in topics
    assert "analytics.public.orders" in topics


def test_snowflake_source_lineage_extraction(config, report):
    """Test complete lineage extraction for Snowflake Source connector."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[
            # Topics are lowercased to match DataHub's normalization
            "snowflake_analytics.public.users",
            "snowflake_analytics.public.orders",
        ],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    lineages = connector.extract_lineages()

    assert len(lineages) == 2

    # Verify first lineage (table names and topics are lowercase)
    users_lineage = next(
        (lineage for lineage in lineages if "users" in lineage.target_dataset), None
    )
    assert users_lineage is not None
    assert users_lineage.source_dataset == "analytics.public.users"
    assert users_lineage.source_platform == "snowflake"
    assert users_lineage.target_dataset == "snowflake_analytics.public.users"
    assert users_lineage.target_platform == "kafka"

    # Verify second lineage (table names and topics are lowercase)
    orders_lineage = next(
        (lineage for lineage in lineages if "orders" in lineage.target_dataset), None
    )
    assert orders_lineage is not None
    assert orders_lineage.source_dataset == "analytics.public.orders"
    assert orders_lineage.source_platform == "snowflake"
    assert orders_lineage.target_dataset == "snowflake_analytics.public.orders"
    assert orders_lineage.target_platform == "kafka"


def test_snowflake_source_lineage_no_matching_topics(config, report):
    """Test lineage extraction when topics don't match configuration."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=["other_topic"],  # Topic doesn't match expected pattern
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    lineages = connector.extract_lineages()

    # Should return empty list when no topics match
    assert len(lineages) == 0


def test_snowflake_source_flow_property_bag(config, report):
    """Test that sensitive fields are excluded from flow property bag."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "connection.user": "admin",
            "connection.password": "secret123",
            "snowflake.private.key": "private_key_data",
            "snowflake.private.key.passphrase": "passphrase123",
            "table.include.list": "ANALYTICS.PUBLIC.USERS",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    flow_props = connector.extract_flow_property_bag()

    # Verify sensitive fields are excluded
    assert "connection.password" not in flow_props
    assert "connection.user" not in flow_props
    assert "snowflake.private.key" not in flow_props
    assert "snowflake.private.key.passphrase" not in flow_props

    # Verify non-sensitive fields are included
    assert "connector.class" in flow_props
    assert "snowflake.database.name" in flow_props
    assert "table.include.list" in flow_props


def test_connector_registry_recognizes_snowflake_source(config, report):
    """Test that ConnectorRegistry creates SnowflakeSourceConnector for Snowflake Source."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = ConnectorRegistry.get_connector_for_manifest(manifest, config, report)

    assert connector is not None
    assert isinstance(connector, SnowflakeSourceConnector)


def test_snowflake_source_with_database_name_fallback(config, report):
    """Test parsing with database.name fallback."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "database.name": "ANALYTICS",  # Fallback field
            "table.include.list": "ANALYTICS.PUBLIC.USERS",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    parser = connector.get_parser(manifest)

    assert parser.database_name == "ANALYTICS"


def test_snowflake_source_with_table_whitelist(config, report):
    """Test parsing with deprecated table.whitelist field."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.whitelist": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    parser = connector.get_parser(manifest)

    assert len(parser.table_names) == 2
    assert "ANALYTICS.PUBLIC.USERS" in parser.table_names
    assert "ANALYTICS.PUBLIC.ORDERS" in parser.table_names


def test_snowflake_source_with_patterns_no_schema_resolver(config, report):
    """Test that patterns without schema resolver are skipped with warning."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.*,ANALYTICS.PRIVATE.USER_*",
        },
        tasks=[],
        topic_names=["some_topic"],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    # schema_resolver is None by default
    assert connector.schema_resolver is None

    lineages = connector.extract_lineages()

    # Should return empty lineages when patterns exist but no schema resolver
    assert len(lineages) == 0

    # Should have warning in report
    assert len(report.warnings) > 0
    warning_messages = [w.message for w in report.warnings if hasattr(w, "message")]
    assert any("table patterns" in msg.lower() for msg in warning_messages)
    assert any(
        "schema resolver is not available" in msg.lower() for msg in warning_messages
    )


def test_snowflake_source_static_tables_without_patterns(config, report):
    """Test that static table lists work without schema resolver."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS,ANALYTICS.PUBLIC.ORDERS",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[
            # Topics are lowercased to match DataHub's normalization
            "snowflake_analytics.public.users",
            "snowflake_analytics.public.orders",
        ],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    # schema_resolver is None by default
    assert connector.schema_resolver is None

    lineages = connector.extract_lineages()

    # Should work fine with static table lists
    assert len(lineages) == 2

    # Should have no warnings
    assert len(report.warnings) == 0


def test_snowflake_source_pattern_expansion_with_schema_resolver(config, report):
    """Test successful pattern expansion when schema resolver is available."""
    from unittest.mock import Mock

    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.*",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[
            "snowflake_analytics.public.users",
            "snowflake_analytics.public.orders",
            "snowflake_analytics.public.products",
        ],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)

    # Mock schema resolver with test URNs
    # Note: DataHub normalizes table names to lowercase in URNs
    mock_resolver = Mock()
    mock_graph = Mock()
    # Mock graph.get_urns_by_filter to return test URNs
    mock_graph.get_urns_by_filter.return_value = iter(
        [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.products,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.private.secrets,PROD)",
        ]
    )
    mock_resolver.graph = mock_graph
    mock_resolver.env = "PROD"
    connector.schema_resolver = mock_resolver

    lineages = connector.extract_lineages()

    # Should expand pattern to 3 matching tables
    assert len(lineages) == 3

    # Verify lineages (table names are lowercase as returned by DataHub)
    source_datasets = {lin.source_dataset for lin in lineages}
    assert "analytics.public.users" in source_datasets
    assert "analytics.public.orders" in source_datasets
    assert "analytics.public.products" in source_datasets

    # Verify no warnings
    assert len(report.warnings) == 0


def test_snowflake_source_pattern_expansion_mixed_patterns_and_explicit(config, report):
    """Test pattern expansion with mix of patterns and explicit table names."""
    from unittest.mock import Mock

    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.*,ANALYTICS.PRIVATE.SPECIFIC_TABLE",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[
            "snowflake_analytics.public.users",
            "snowflake_analytics.public.orders",
            "snowflake_analytics.private.specific_table",
        ],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)

    # Mock schema resolver (table names in lowercase as in real DataHub)
    mock_resolver = Mock()
    mock_graph = Mock()
    # Mock graph.get_urns_by_filter to return test URNs
    mock_graph.get_urns_by_filter.return_value = iter(
        [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.private.specific_table,PROD)",
        ]
    )
    mock_resolver.graph = mock_graph
    mock_resolver.env = "PROD"
    connector.schema_resolver = mock_resolver

    lineages = connector.extract_lineages()

    # Should have 3 lineages (2 from pattern + 1 explicit)
    assert len(lineages) == 3

    source_datasets = {lin.source_dataset for lin in lineages}
    assert "analytics.public.users" in source_datasets
    assert "analytics.public.orders" in source_datasets
    assert "analytics.private.specific_table" in source_datasets


def test_snowflake_source_pattern_expansion_no_matches(config, report):
    """Test pattern expansion when no tables match the pattern."""
    from unittest.mock import Mock

    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.*",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=["snowflake_ANALYTICS.PUBLIC.USERS"],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)

    # Mock schema resolver with no matching tables
    mock_resolver = Mock()
    mock_resolver.get_urns.return_value = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS.PRIVATE.USERS,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,ANALYTICS.PUBLIC.ORDERS,PROD)",
    ]
    connector.schema_resolver = mock_resolver

    lineages = connector.extract_lineages()

    # Should return empty list when no matches
    assert len(lineages) == 0


def test_snowflake_source_pattern_expansion_multiple_patterns(config, report):
    """Test expansion of multiple patterns."""
    from unittest.mock import Mock

    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USER_*,ANALYTICS.PUBLIC.ORDER_*",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[
            "snowflake_analytics.public.user_profiles",
            "snowflake_analytics.public.user_settings",
            "snowflake_analytics.public.order_items",
            "snowflake_analytics.public.order_history",
        ],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)

    # Mock schema resolver (table names in lowercase as in real DataHub)
    mock_resolver = Mock()
    mock_graph = Mock()
    # Mock graph.get_urns_by_filter to return test URNs
    # Use side_effect to return a fresh iterator each time (needed for multiple pattern expansions)
    mock_graph.get_urns_by_filter.side_effect = lambda **kwargs: iter(
        [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.user_profiles,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.user_settings,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.order_items,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.order_history,PROD)",
        ]
    )
    mock_resolver.graph = mock_graph
    mock_resolver.env = "PROD"
    connector.schema_resolver = mock_resolver

    lineages = connector.extract_lineages()

    # Should expand both patterns
    assert len(lineages) == 4

    source_datasets = {lin.source_dataset for lin in lineages}
    assert "analytics.public.user_profiles" in source_datasets
    assert "analytics.public.user_settings" in source_datasets
    assert "analytics.public.order_items" in source_datasets
    assert "analytics.public.order_history" in source_datasets


def test_snowflake_source_pattern_expansion_empty_datahub_response(config, report):
    """
    Test pattern expansion when DataHub returns no tables at all.

    This is a realistic scenario where:
    1. User configures a pattern (e.g., "ANALYTICS.PUBLIC.*")
    2. Schema resolver queries DataHub successfully
    3. DataHub returns empty result (no tables matching the pattern exist)
    4. Connector should handle this gracefully and log a warning
    """
    from unittest.mock import Mock

    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.NONEXISTENT.*",
            "topic.prefix": "snowflake_",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)

    # Mock schema resolver that returns empty results from DataHub
    mock_resolver = Mock()
    mock_graph = Mock()
    # DataHub returns empty iterator - no tables found
    mock_graph.get_urns_by_filter.return_value = iter([])
    mock_resolver.graph = mock_graph
    mock_resolver.env = "PROD"
    connector.schema_resolver = mock_resolver

    # Test get_topics_from_config - should return empty list
    topics = connector.get_topics_from_config()
    assert len(topics) == 0

    # Test extract_lineages - should also return empty list
    lineages = connector.extract_lineages()
    assert len(lineages) == 0

    # Verify that cached expanded tables is set to empty list
    assert connector._cached_expanded_tables == []


def test_snowflake_source_parser_extracts_transforms(config, report):
    """Test that parser correctly extracts transform configuration."""
    manifest = ConnectorManifest(
        name="snowflake-source-test",
        type="source",
        config={
            "connector.class": SNOWFLAKE_SOURCE_CLOUD,
            "snowflake.database.name": "ANALYTICS",
            "table.include.list": "ANALYTICS.PUBLIC.USERS",
            "transforms": "route,timestamp",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "snowflake_(.*)",
            "transforms.route.replacement": "prod_$1",
            "transforms.timestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter",
            "transforms.timestamp.field": "updated_at",
        },
        tasks=[],
        topic_names=[],
        lineages=[],
    )

    connector = SnowflakeSourceConnector(manifest, config, report)
    parser = connector.get_parser(manifest)

    # Verify transforms are parsed
    assert len(parser.transforms) == 2

    # Verify first transform (route)
    route_transform = parser.transforms[0]
    assert route_transform["name"] == "route"
    assert route_transform["type"] == "org.apache.kafka.connect.transforms.RegexRouter"
    assert route_transform["regex"] == "snowflake_(.*)"
    assert route_transform["replacement"] == "prod_$1"

    # Verify second transform (timestamp)
    timestamp_transform = parser.transforms[1]
    assert timestamp_transform["name"] == "timestamp"
    assert (
        timestamp_transform["type"]
        == "org.apache.kafka.connect.transforms.TimestampConverter"
    )
    assert timestamp_transform["field"] == "updated_at"
