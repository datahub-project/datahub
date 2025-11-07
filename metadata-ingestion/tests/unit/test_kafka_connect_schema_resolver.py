"""Tests for Kafka Connect schema resolver integration."""

import logging
from typing import Dict, List, Optional
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)

logger = logging.getLogger(__name__)


class MockSchemaResolver:
    """Mock SchemaResolver for testing."""

    def __init__(self, platform: str, mock_urns: Optional[List[str]] = None):
        self.platform = platform
        self._mock_urns = mock_urns or []
        self._schemas: Dict[str, Dict[str, str]] = {}

    def get_urns(self) -> List[str]:
        """Return mock URNs."""
        return self._mock_urns

    def resolve_table(self, table):
        """Mock table resolution."""
        table_name = table.table
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{table_name},PROD)"
        schema = self._schemas.get(table_name)
        return urn, schema

    def get_urn_for_table(self, table):
        """Mock URN generation for table."""
        table_name = table.table
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{table_name},PROD)"

    def add_schema(self, table_name: str, schema: Dict[str, str]):
        """Add a schema for testing."""
        self._schemas[table_name] = schema


class TestSchemaResolverTableExpansion:
    """Tests for table pattern expansion using SchemaResolver."""

    def test_pattern_expansion_disabled_by_default(self):
        """Test that pattern expansion is disabled by default."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=None,
        )

        # Pattern should not be expanded
        result = connector._expand_table_patterns(
            "public.*", "postgres", "testdb"
        )

        # Should return the pattern as-is (not expanded)
        assert result == ["public.*"]

    def test_pattern_expansion_with_wildcard(self):
        """Test expanding pattern with wildcard using SchemaResolver."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver with matching URNs
        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.private.secrets,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Pattern should be expanded
        result = connector._expand_table_patterns(
            "public.*", "postgres", "testdb"
        )

        # Should match only public schema tables
        assert len(result) == 2
        assert "testdb.public.users" in result
        assert "testdb.public.orders" in result
        assert "testdb.private.secrets" not in result

    def test_pattern_expansion_no_matches(self):
        """Test pattern expansion when no tables match."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "nonexistent.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver with URNs that don't match the pattern
        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.orders,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Pattern should not match anything, return as-is
        result = connector._expand_table_patterns(
            "nonexistent.*", "postgres", "testdb"
        )

        # Should keep the pattern as-is since no matches found
        assert result == ["nonexistent.*"]

    def test_pattern_expansion_mixed_patterns_and_explicit(self):
        """Test expanding a mix of patterns and explicit table names."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.*,private.accounts",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver
        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.private.accounts,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Mixed pattern and explicit should work
        result = connector._expand_table_patterns(
            "public.*,private.accounts", "postgres", "testdb"
        )

        # Should expand pattern and keep explicit table name
        assert len(result) == 3
        assert "testdb.public.users" in result
        assert "testdb.public.orders" in result
        assert "private.accounts" in result

    def test_pattern_expansion_disabled_via_config(self):
        """Test that pattern expansion can be disabled via config."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=False,  # Disabled
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Pattern should not be expanded even though schema resolver is available
        result = connector._expand_table_patterns(
            "public.*", "postgres", "testdb"
        )

        assert result == ["public.*"]


class TestSchemaResolverFineGrainedLineage:
    """Tests for fine-grained lineage extraction using SchemaResolver."""

    def test_fine_grained_lineage_disabled_by_default(self):
        """Test that fine-grained lineage is disabled by default."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=None,
        )

        # Should return None (feature disabled)
        result = connector._extract_fine_grained_lineage(
            "testdb.public.users",
            "postgres",
            "testserver.public.users",
            "kafka",
        )

        assert result is None

    def test_fine_grained_lineage_generation(self):
        """Test generating fine-grained column-level lineage."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_finegrained_lineage=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver with schema metadata
        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {
                "id": "INTEGER",
                "username": "VARCHAR",
                "email": "VARCHAR",
                "created_at": "TIMESTAMP",
            },
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Should generate fine-grained lineage
        result = connector._extract_fine_grained_lineage(
            "testdb.public.users",
            "postgres",
            "testserver.public.users",
            "kafka",
        )

        # Should have 4 column lineages (one for each column)
        assert result is not None
        assert len(result) == 4

        # Check structure of first lineage
        first_lineage = result[0]
        assert first_lineage["upstreamType"] == "FIELD_SET"
        assert first_lineage["downstreamType"] == "FIELD"
        assert len(first_lineage["upstreams"]) == 1
        assert len(first_lineage["downstreams"]) == 1

        # Verify all columns are present
        columns = []
        for lineage in result:
            # Extract column name from URN
            upstream_urn = lineage["upstreams"][0]
            column_name = upstream_urn.split(",")[-1].rstrip(")")
            columns.append(column_name)

        assert "id" in columns
        assert "username" in columns
        assert "email" in columns
        assert "created_at" in columns

    def test_fine_grained_lineage_no_schema_metadata(self):
        """Test fine-grained lineage when schema metadata is unavailable."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_finegrained_lineage=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver without any schema metadata
        mock_resolver = MockSchemaResolver(platform="postgres")

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Should return None when schema metadata is unavailable
        result = connector._extract_fine_grained_lineage(
            "testdb.public.users",
            "postgres",
            "testserver.public.users",
            "kafka",
        )

        assert result is None

    def test_fine_grained_lineage_disabled_via_config(self):
        """Test that fine-grained lineage can be disabled via config."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_finegrained_lineage=False,  # Disabled
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {"id": "INTEGER", "username": "VARCHAR"},
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Should return None even though schema metadata is available
        result = connector._extract_fine_grained_lineage(
            "testdb.public.users",
            "postgres",
            "testserver.public.users",
            "kafka",
        )

        assert result is None


class TestSchemaResolverIntegration:
    """Integration tests for schema resolver with lineage extraction."""

    def test_lineage_extraction_with_fine_grained_lineage(self):
        """Test that extract_lineages includes fine-grained lineage."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_finegrained_lineage=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        # Create mock schema resolver with schema metadata
        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {"id": "INTEGER", "username": "VARCHAR", "email": "VARCHAR"},
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Extract lineages
        lineages = connector.extract_lineages()

        # Should have one lineage
        assert len(lineages) == 1

        # Check that fine-grained lineage is included
        lineage = lineages[0]
        assert lineage.fine_grained_lineages is not None
        assert len(lineage.fine_grained_lineages) == 3  # 3 columns
        assert lineage.source_dataset == "testdb.public.users"
        assert lineage.target_dataset == "testserver.public.users"

    def test_lineage_extraction_without_schema_resolver(self):
        """Test that lineage extraction works without schema resolver."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.users",
                "database.server.name": "testserver",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=None,  # No schema resolver
        )

        # Extract lineages
        lineages = connector.extract_lineages()

        # Should have one lineage without fine-grained lineage
        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.fine_grained_lineages is None
        assert lineage.source_dataset == "testdb.public.users"
        assert lineage.target_dataset == "testserver.public.users"


class TestSchemaResolverEdgeCases:
    """Test edge cases and error handling for schema resolver."""

    def test_extract_table_name_from_urn_valid(self):
        """Test extracting table name from valid URN."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="test",
            type="source",
            config={},
            tasks=[],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)"
        result = connector._extract_table_name_from_urn(urn)

        assert result == "testdb.public.users"

    def test_extract_table_name_from_urn_invalid(self):
        """Test extracting table name from invalid URN."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="test",
            type="source",
            config={},
            tasks=[],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        # Invalid URN format
        urn = "invalid-urn"
        result = connector._extract_table_name_from_urn(urn)

        assert result is None

    def test_pattern_expansion_empty_urns(self):
        """Test pattern expansion when schema resolver has no URNs."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname": "testdb",
                "table.include.list": "public.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        # Create mock schema resolver with no URNs
        mock_resolver = MockSchemaResolver(platform="postgres", mock_urns=[])

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        # Should return pattern as-is when no URNs available
        result = connector._expand_table_patterns(
            "public.*", "postgres", "testdb"
        )

        assert result == ["public.*"]