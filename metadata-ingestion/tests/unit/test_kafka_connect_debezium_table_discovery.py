"""Tests for Debezium connector table discovery and filtering functionality."""

from typing import Optional
from unittest.mock import Mock, patch

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started():
    """Ensure JVM is started for all tests requiring Java regex."""
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())
    yield


def create_debezium_connector(
    connector_config: dict,
    schema_resolver: Optional[SchemaResolver] = None,
    use_schema_resolver: bool = False,
) -> DebeziumSourceConnector:
    """Helper to create a DebeziumSourceConnector instance for testing."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config=connector_config,
        tasks=[],
        topic_names=[],
    )

    config = Mock(spec=KafkaConnectSourceConfig)
    config.use_schema_resolver = use_schema_resolver
    config.schema_resolver_expand_patterns = True
    config.env = "PROD"

    report = Mock(spec=KafkaConnectSourceReport)

    connector = DebeziumSourceConnector(manifest, config, report)
    connector.schema_resolver = schema_resolver

    return connector


class TestGetTableNamesFromConfigOrDiscovery:
    """Tests for _get_table_names_from_config_or_discovery method."""

    def test_no_schema_resolver_uses_table_include_list(self) -> None:
        """When SchemaResolver is not available, should use table.include.list from config."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.users,public.orders,private.data",
        }

        connector = create_debezium_connector(
            connector_config, use_schema_resolver=False
        )

        result = connector._get_table_names_from_config_or_discovery(
            connector_config, "testdb", "postgres"
        )

        assert result == ["public.users", "public.orders", "private.data"]

    def test_no_schema_resolver_no_table_config_returns_empty(self) -> None:
        """When SchemaResolver disabled and no table config, should return empty list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(
            connector_config, use_schema_resolver=False
        )

        result = connector._get_table_names_from_config_or_discovery(
            connector_config, "testdb", "postgres"
        )

        assert result == []

    def test_schema_resolver_no_database_name_falls_back_to_config(self) -> None:
        """When SchemaResolver enabled but no database name, should fall back to table.include.list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "table.include.list": "public.users",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        result = connector._get_table_names_from_config_or_discovery(
            connector_config, None, "postgres"
        )

        assert result == ["public.users"]

    def test_schema_resolver_discovers_tables_from_database(self) -> None:
        """When SchemaResolver enabled with database name, should discover tables."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        # Mock SchemaResolver to return discovered tables
        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        # Mock _discover_tables_from_database to return tables
        with patch.object(
            connector,
            "_discover_tables_from_database",
            return_value=["public.users", "public.orders", "public.products"],
        ):
            result = connector._get_table_names_from_config_or_discovery(
                connector_config, "testdb", "postgres"
            )

        assert result == ["public.users", "public.orders", "public.products"]

    def test_schema_resolver_no_tables_found_returns_empty(self) -> None:
        """When SchemaResolver finds no tables, should return empty list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        # Mock _discover_tables_from_database to return empty
        with patch.object(connector, "_discover_tables_from_database", return_value=[]):
            result = connector._get_table_names_from_config_or_discovery(
                connector_config, "testdb", "postgres"
            )

        assert result == []


class TestApplySchemaFilters:
    """Tests for _apply_schema_filters method."""

    def test_no_schema_filters_returns_all_tables(self) -> None:
        """When no schema filters configured, should return all input tables."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "private.data"]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == tables

    def test_schema_include_list_filters_by_schema_name(self) -> None:
        """schema.include.list should filter tables by schema name."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "public",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "private.data"]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == ["public.users", "public.orders"]

    def test_schema_include_list_with_regex_pattern(self) -> None:
        """schema.include.list should support Java regex patterns."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "schema_v[0-9]+",
        }

        connector = create_debezium_connector(connector_config)

        tables = [
            "schema_v1.orders",
            "schema_v2.orders",
            "schema_vX.orders",
            "public.users",
        ]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == ["schema_v1.orders", "schema_v2.orders"]

    def test_schema_include_list_multiple_patterns(self) -> None:
        """schema.include.list should support multiple comma-separated patterns."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "public,analytics",
        }

        connector = create_debezium_connector(connector_config)

        tables = [
            "public.users",
            "analytics.events",
            "private.data",
            "staging.temp",
        ]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == ["public.users", "analytics.events"]

    def test_schema_include_list_no_matches_returns_empty(self) -> None:
        """schema.include.list with no matches should return empty list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "nonexistent",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "private.data"]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == []

    def test_schema_exclude_list_filters_out_matching_schemas(self) -> None:
        """schema.exclude.list should filter out tables with matching schema names."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.exclude.list": "private",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "private.data", "private.secrets"]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == ["public.users", "public.orders"]

    def test_schema_exclude_list_with_regex_pattern(self) -> None:
        """schema.exclude.list should support Java regex patterns."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.exclude.list": "temp.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "temp_staging.data", "temp_test.data", "prod.users"]
        result = connector._apply_schema_filters(connector_config, tables)

        assert result == ["public.users", "prod.users"]

    def test_schema_include_and_exclude_combined(self) -> None:
        """schema.include.list and schema.exclude.list should work together."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "public,analytics",
            "schema.exclude.list": "analytics",
        }

        connector = create_debezium_connector(connector_config)

        tables = [
            "public.users",
            "public.orders",
            "analytics.events",
            "private.data",
        ]
        result = connector._apply_schema_filters(connector_config, tables)

        # Include public and analytics, then exclude analytics
        assert result == ["public.users", "public.orders"]

    def test_schema_filters_skip_tables_without_schema_separator(self) -> None:
        """Schema filters should handle tables without '.' separator."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "public",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "just_table_name", "private.data"]
        result = connector._apply_schema_filters(connector_config, tables)

        # Tables without '.' are skipped by include filter
        assert result == ["public.users"]


class TestApplyTableFilters:
    """Tests for _apply_table_filters method."""

    def test_no_table_filters_returns_all_tables(self) -> None:
        """When no table filters configured, should return all input tables."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "private.data"]
        result = connector._apply_table_filters(connector_config, tables)

        assert result == tables

    def test_table_include_list_exact_match(self) -> None:
        """table.include.list should match exact table names."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.users,public.orders",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "public.products", "private.data"]
        result = connector._apply_table_filters(connector_config, tables)

        assert sorted(result) == ["public.orders", "public.users"]

    def test_table_include_list_with_wildcard(self) -> None:
        """table.include.list should support wildcard patterns."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders", "private.data"]
        result = connector._apply_table_filters(connector_config, tables)

        assert sorted(result) == ["public.orders", "public.users"]

    def test_table_include_list_with_character_class(self) -> None:
        """table.include.list should support Java regex character classes."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.user[0-9]+",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.user1", "public.user2", "public.userX", "public.orders"]
        result = connector._apply_table_filters(connector_config, tables)

        assert sorted(result) == ["public.user1", "public.user2"]

    def test_table_include_list_no_matches_returns_empty(self) -> None:
        """table.include.list with no matches should return empty list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "nonexistent.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "private.data"]
        result = connector._apply_table_filters(connector_config, tables)

        assert result == []

    def test_table_exclude_list_filters_out_matching_tables(self) -> None:
        """table.exclude.list should filter out matching tables."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.exclude.list": "public.temp.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.temp_staging", "public.temp_test"]
        result = connector._apply_table_filters(connector_config, tables)

        assert result == ["public.users"]

    def test_table_include_and_exclude_combined(self) -> None:
        """table.include.list and table.exclude.list should work together."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.*",
            "table.exclude.list": "public.temp.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = [
            "public.users",
            "public.orders",
            "public.temp_staging",
            "private.data",
        ]
        result = connector._apply_table_filters(connector_config, tables)

        # Include public.*, then exclude public.temp.*
        assert sorted(result) == ["public.orders", "public.users"]

    def test_table_whitelist_legacy_config_name(self) -> None:
        """Should support legacy 'table.whitelist' config name."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.whitelist": "public.users",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.orders"]
        result = connector._apply_table_filters(connector_config, tables)

        assert result == ["public.users"]

    def test_table_blacklist_legacy_config_name(self) -> None:
        """Should support legacy 'table.blacklist' config name."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.blacklist": "public.temp.*",
        }

        connector = create_debezium_connector(connector_config)

        tables = ["public.users", "public.temp_staging"]
        result = connector._apply_table_filters(connector_config, tables)

        assert result == ["public.users"]


class TestDeriveTopicsFromTables:
    """Tests for _derive_topics_from_tables method."""

    def test_derive_topics_with_server_name(self) -> None:
        """Should derive topics in format {server_name}.{schema.table}."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(connector_config)

        table_names = ["public.users", "public.orders", "analytics.events"]
        result = connector._derive_topics_from_tables(table_names, "myserver")

        assert result == [
            "myserver.public.users",
            "myserver.public.orders",
            "myserver.analytics.events",
        ]

    def test_derive_topics_without_server_name(self) -> None:
        """Should use table name directly when no server name."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(connector_config)

        table_names = ["public.users", "public.orders"]
        result = connector._derive_topics_from_tables(table_names, None)

        assert result == ["public.users", "public.orders"]

    def test_derive_topics_preserves_schema_table_format(self) -> None:
        """Should preserve schema.table format in topic names."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        connector = create_debezium_connector(connector_config)

        table_names = ["schema1.table1", "schema2.table2"]
        result = connector._derive_topics_from_tables(table_names, "myserver")

        assert result == ["myserver.schema1.table1", "myserver.schema2.table2"]


class TestGetTopicsFromConfigIntegration:
    """Integration tests for the full get_topics_from_config flow."""

    def test_full_flow_without_schema_resolver(self) -> None:
        """Test complete flow using only table.include.list."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.users,public.orders",
        }

        connector = create_debezium_connector(
            connector_config, use_schema_resolver=False
        )

        result = connector.get_topics_from_config()

        assert sorted(result) == ["myserver.public.orders", "myserver.public.users"]

    def test_full_flow_with_schema_and_table_filters(self) -> None:
        """Test complete flow with both schema and table filters."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "public",
            "table.include.list": "public.*",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        # Mock table discovery to return multiple schemas
        with patch.object(
            connector,
            "_discover_tables_from_database",
            return_value=[
                "public.users",
                "public.orders",
                "private.data",
                "analytics.events",
            ],
        ):
            result = connector.get_topics_from_config()

        # Should only include public schema tables
        assert sorted(result) == ["myserver.public.orders", "myserver.public.users"]

    def test_full_flow_with_exclude_filters(self) -> None:
        """Test complete flow with exclude filters."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.exclude.list": "temp.*",
            "table.exclude.list": "public.test.*",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        # Mock table discovery
        with patch.object(
            connector,
            "_discover_tables_from_database",
            return_value=[
                "public.users",
                "public.test_data",
                "temp_staging.data",
                "analytics.events",
            ],
        ):
            result = connector.get_topics_from_config()

        # Should exclude temp_staging schema and public.test_* tables
        assert result == ["myserver.public.users", "myserver.analytics.events"]

    def test_full_flow_no_tables_found(self) -> None:
        """Test flow when no tables match filters."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "schema.include.list": "nonexistent",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        with patch.object(
            connector,
            "_discover_tables_from_database",
            return_value=["public.users", "public.orders"],
        ):
            result = connector.get_topics_from_config()

        assert result == []

    def test_full_flow_handles_errors_gracefully(self) -> None:
        """Test that errors in discovery are handled gracefully."""
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        schema_resolver = Mock(spec=SchemaResolver)
        connector = create_debezium_connector(
            connector_config, schema_resolver=schema_resolver, use_schema_resolver=True
        )

        # Mock discovery to raise exception
        with patch.object(
            connector,
            "_discover_tables_from_database",
            side_effect=Exception("Connection error"),
        ):
            result = connector.get_topics_from_config()

        # Should return empty list instead of crashing
        assert result == []
