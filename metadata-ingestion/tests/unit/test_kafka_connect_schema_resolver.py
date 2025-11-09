"""Tests for Kafka Connect schema resolver integration."""

import logging
from typing import Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface

logger = logging.getLogger(__name__)


class MockSchemaResolver(SchemaResolverInterface):
    """Mock SchemaResolver for testing."""

    def __init__(self, platform: str, mock_urns: Optional[List[str]] = None):
        self._platform = platform
        self._mock_urns = set(mock_urns or [])
        self._schemas: Dict[str, Dict[str, str]] = {}
        # Additional attributes that production code may access
        self.graph = None
        self.env = "PROD"

    @property
    def platform(self) -> str:
        """Return the platform."""
        return self._platform

    def includes_temp_tables(self) -> bool:
        """Return whether temp tables are included."""
        return False

    def get_urns(self):
        """Return mock URNs."""
        return self._mock_urns

    def resolve_table(self, table: Any) -> Tuple[str, Optional[Dict[str, str]]]:
        """Mock table resolution."""
        table_name = table.table
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{table_name},PROD)"
        schema = self._schemas.get(table_name)
        return urn, schema

    def get_urn_for_table(self, table: Any) -> str:
        """Mock URN generation for table."""
        table_name = table.table
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{table_name},PROD)"

    def add_schema(self, table_name: str, schema: Dict[str, str]) -> None:
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
        result = connector._expand_table_patterns("public.*", "postgres", "testdb")

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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        # Pattern should be expanded
        result = connector._expand_table_patterns("public.*", "postgres", "testdb")

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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        # Pattern should not match anything, return as-is
        result = connector._expand_table_patterns("nonexistent.*", "postgres", "testdb")

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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        # Pattern should not be expanded even though schema resolver is available
        result = connector._expand_table_patterns("public.*", "postgres", "testdb")

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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        # Should return pattern as-is when no URNs available
        result = connector._expand_table_patterns("public.*", "postgres", "testdb")

        assert result == ["public.*"]


class TestJavaRegexPatternMatching:
    """Tests for Java regex pattern matching in table expansion."""

    def test_alternation_pattern(self):
        """Test alternation pattern: public\\.(bg|cp)_.*"""
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
                "table.include.list": "public\\.(bg|cp)_.*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.bg_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.cp_orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.fg_data,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns(
            "public\\.(bg|cp)_.*", "postgres", "testdb"
        )

        # Should match only tables starting with bg_ or cp_ in public schema
        assert len(result) == 2
        assert "testdb.public.bg_users" in result
        assert "testdb.public.cp_orders" in result
        assert "testdb.public.fg_data" not in result
        assert "testdb.public.users" not in result

    def test_character_class_pattern(self):
        """Test character class pattern: public\\.test[0-9]+"""
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
                "table.include.list": "public\\.test[0-9]+",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test23,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.testA,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns(
            "public\\.test[0-9]+", "postgres", "testdb"
        )

        # Should match only test followed by one or more digits
        assert len(result) == 2
        assert "testdb.public.test1" in result
        assert "testdb.public.test23" in result
        assert "testdb.public.test" not in result
        assert "testdb.public.testA" not in result

    def test_complex_grouping_pattern(self):
        """Test complex grouping: (public|private)\\.(users|orders)"""
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
                "table.include.list": "(public|private)\\.(users|orders)",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.private.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.private.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.products,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.admin.users,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns(
            "(public|private)\\.(users|orders)", "postgres", "testdb"
        )

        # Should match exactly: public.users, public.orders, private.users, private.orders
        assert len(result) == 4
        assert "testdb.public.users" in result
        assert "testdb.public.orders" in result
        assert "testdb.private.users" in result
        assert "testdb.private.orders" in result
        assert "testdb.public.products" not in result
        assert "testdb.admin.users" not in result

    def test_mysql_two_tier_pattern(self):
        """Test MySQL 2-tier pattern: mydb\\.user.*"""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_expand_patterns=True,
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="mysql-source",
            type="source",
            config={
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.dbname": "mydb",
                "table.include.list": "mydb\\.user.*",
                "database.server.name": "mysqlserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="mysql",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.user_roles,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.user_permissions,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,otherdb.users,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns("mydb\\.user.*", "mysql", "mydb")

        # Should match mydb tables starting with "user"
        assert len(result) == 3
        assert "mydb.users" in result
        assert "mydb.user_roles" in result
        assert "mydb.user_permissions" in result
        assert "mydb.orders" not in result
        assert "otherdb.users" not in result

    def test_escaped_dots_vs_any_char(self):
        """Test that escaped dots (\\.) match literal dots, not any character."""
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
                "table.include.list": "public\\.user",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.publicXuser,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns("public\\.user", "postgres", "testdb")

        # Escaped dot should match only literal dot, not any character
        assert len(result) == 1
        assert "testdb.public.user" in result
        assert "testdb.publicXuser" not in result

    def test_postgres_schema_without_database_prefix(self):
        """Test PostgreSQL pattern without database prefix: public\\..*"""
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
                "table.include.list": "public\\..*",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

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
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns("public\\..*", "postgres", "testdb")

        # Should match all tables in public schema (without database in pattern)
        assert len(result) == 2
        assert "testdb.public.users" in result
        assert "testdb.public.orders" in result
        assert "testdb.private.secrets" not in result

    def test_quantifier_patterns(self):
        """Test various quantifiers: +, *"""
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
                "table.include.list": "public\\.user_[a-z]+",
                "database.server.name": "testserver",
            },
            tasks=[],
        )

        mock_resolver = MockSchemaResolver(
            platform="postgres",
            mock_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user_ab,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user_abc,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user_abcd,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user_,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.user_123,PROD)",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        result = connector._expand_table_patterns(
            "public\\.user_[a-z]+", "postgres", "testdb"
        )

        # Should match only tables with one or more lowercase letters after user_
        assert len(result) == 3
        assert "testdb.public.user_ab" in result
        assert "testdb.public.user_abc" in result
        assert "testdb.public.user_abcd" in result
        assert "testdb.public.user_" not in result
        assert "testdb.public.user_123" not in result
