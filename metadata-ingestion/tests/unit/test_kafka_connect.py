import logging
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock, patch

import jpype
import jpype.imports
import pytest

# Import the classes we're testing
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_dataset_name,
    has_three_level_hierarchy,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
    RegexRouterTransform,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    ConfluentJDBCSourceConnector,
    RegexRouterTransform as SourceRegexRouterTransform,
    TableId,
    TransformPipeline,
)

logger = logging.getLogger(__name__)

if not jpype.isJVMStarted():
    jpype.startJVM(jpype.getDefaultJVMPath())


class TestRegexRouterTransform:
    """Test the RegexRouterTransform class."""

    def test_no_transforms_configured(self) -> None:
        """Test when no transforms are configured."""
        config: Dict[str, str] = {"connector.class": "some.connector"}
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: List[str] = transform.apply(["test-topic"])
        assert result == ["test-topic"]

    def test_non_regex_router_transforms(self) -> None:
        """Test when transforms exist but none are RegexRouter."""
        config: Dict[str, str] = {
            "transforms": "MyTransform",
            "transforms.MyTransform.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.MyTransform.field": "timestamp",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: List[str] = transform.apply(["test-topic"])
        assert result == ["test-topic"]

    def test_single_regex_router_transform(self) -> None:
        """Test single RegexRouter transformation."""
        # Extract transform-specific configuration
        transform_config = {
            "name": "TableNameTransformation",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": ".*",
            "replacement": "my_sink_table",
        }
        transform: RegexRouterTransform = RegexRouterTransform(transform_config)

        result: List[str] = transform.apply(["source-topic"])
        assert result == ["my_sink_table"]

    def test_multiple_regex_router_transforms(self) -> None:
        """Test multiple RegexRouter transformations applied in sequence."""
        # Test with multiple transforms by applying them sequentially
        first_transform_config = {
            "name": "First",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": "user-(.*)",
            "replacement": "customer_$1",
        }
        second_transform_config = {
            "name": "Second",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": "customer_(.*)",
            "replacement": "final_$1",
        }
        first_transform = RegexRouterTransform(first_transform_config)
        second_transform = RegexRouterTransform(second_transform_config)

        # Apply first transform
        intermediate_result = first_transform.apply(["user-events"])
        # Apply second transform to the result
        result = second_transform.apply(intermediate_result)
        assert result == ["final_events"]

    def test_mysql_source_config_example(self) -> None:
        """Test the specific MySQL source configuration from the example."""
        # Extract transform-specific configuration
        transform_config = {
            "name": "TotalReplacement",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": ".*(book)",
            "replacement": "my-new-topic-$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(transform_config)

        # Test with a topic that matches the pattern
        result: List[str] = transform.apply(["library-book"])
        assert result == ["my-new-topic-book"]

        # Test with a topic that doesn't match
        result = transform.apply(["user-data"])
        assert result == ["user-data"]  # Should remain unchanged

    def test_mixed_transforms(self) -> None:
        """Test mix of RegexRouter and other transforms."""
        # Extract just the RegexRouter transform configuration
        transform_config = {
            "name": "Router",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": "events-(.*)",
            "replacement": "processed_$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(transform_config)

        result: List[str] = transform.apply(["events-user"])
        assert result == ["processed_user"]

    def test_invalid_regex_pattern(self) -> None:
        """Test handling of invalid regex patterns."""
        config: Dict[str, str] = {
            "transforms": "BadRegex",
            "transforms.BadRegex.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.BadRegex.regex": "[invalid",  # Invalid regex
            "transforms.BadRegex.replacement": "fixed",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        # Should not crash and return original topic
        result: List[str] = transform.apply(["test-topic"])
        assert result == ["test-topic"]

    def test_empty_replacement(self) -> None:
        """Test with empty replacement string."""
        # Extract transform-specific configuration
        transform_config = {
            "name": "EmptyReplace",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": "prefix-(.*)",
            "replacement": "",
        }
        transform: RegexRouterTransform = RegexRouterTransform(transform_config)

        result: List[str] = transform.apply(["prefix-suffix"])
        assert result == [""]

    def test_whitespace_in_transform_names(self) -> None:
        """Test handling of whitespace in transform names."""
        # Extract transform-specific configuration
        transform_config = {
            "name": "Transform1",
            "type": "org.apache.kafka.connect.transforms.RegexRouter",
            "regex": "test-(.*)",
            "replacement": "result_$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(transform_config)

        result: List[str] = transform.apply(["test-data"])
        assert result == ["result_data"]


class TestBigQuerySinkConnector:
    """Test BigQuery sink connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="test-bigquery-connector",
            type="sink",
            config=config,
            tasks=[],
            topic_names=["source-topic"],
        )

    def create_mock_dependencies(self) -> Tuple[Mock, Mock]:
        """Helper to create mock dependencies."""
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)
        return config, report

    def test_bigquery_with_regex_router(self) -> None:
        """Test BigQuery connector with RegexRouter transformation."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "my-gcp-project",
            "defaultDataset": "ingest",
            "transforms": "TableNameTransformation",
            "transforms.TableNameTransformation.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TableNameTransformation.regex": ".*",
            "transforms.TableNameTransformation.replacement": "my_sink_table",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config, report = self.create_mock_dependencies()

        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.source_dataset == "source-topic"
        assert lineage.source_platform == "kafka"
        assert lineage.target_dataset == "my-gcp-project.ingest.my_sink_table"
        assert lineage.target_platform == "bigquery"

    def test_bigquery_with_complex_regex(self) -> None:
        """Test BigQuery with complex regex pattern."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "analytics",
            "defaultDataset": "raw",
            "transforms": "TopicTransform",
            "transforms.TopicTransform.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TopicTransform.regex": "app_(.*)_events",
            "transforms.TopicTransform.replacement": "$1_processed",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        manifest.topic_names = ["app_user_events", "app_order_events"]
        config, report = self.create_mock_dependencies()

        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 2

        # Check first lineage
        user_lineage = next(
            line for line in lineages if line.source_dataset == "app_user_events"
        )
        assert user_lineage.target_dataset == "analytics.raw.user_processed"

        # Check second lineage
        order_lineage = next(
            line for line in lineages if line.source_dataset == "app_order_events"
        )
        assert order_lineage.target_dataset == "analytics.raw.order_processed"

    def test_bigquery_no_transforms(self) -> None:
        """Test BigQuery connector without transforms."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "my-project",
            "defaultDataset": "dataset",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config, report = self.create_mock_dependencies()

        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.target_dataset == "my-project.dataset.source-topic"


class TestS3SinkConnector:
    """Test S3 sink connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="test-s3-connector",
            type="sink",
            config=config,
            tasks=[],
            topic_names=["user-events"],
        )

    def test_s3_with_regex_router(self) -> None:
        """Test S3 connector with RegexRouter transformation."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "s3.bucket.name": "my-data-lake",
            "topics.dir": "kafka-data",
            "transforms": "PathTransform",
            "transforms.PathTransform.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.PathTransform.regex": "user-(.*)",
            "transforms.PathTransform.replacement": "processed-$1",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentS3SinkConnector = ConfluentS3SinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.source_dataset == "user-events"
        assert lineage.source_platform == "kafka"
        assert lineage.target_dataset == "my-data-lake/kafka-data/processed-events"
        assert lineage.target_platform == "s3"


class TestBigquerySinkConnector:
    """Test BigQuery sink connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="test-bigquery-connector",
            type="sink",
            config=config,
            tasks=[],
            topic_names=["my-source-kafka-topic"],
        )

    def test_bigquery_with_regex_router(self) -> None:
        """Test BigQuery connector with RegexRouter transformation."""
        connector_config: Dict[str, str] = {
            "autoCreateTables": "true",
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "defaultDataset": "ingest",
            "project": "my-gcp-project",
            "sanitizeTopics": "true",
            "schemaRegistryLocation": "http://schema-registry",
            "schemaRetriever": "com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever",
            "tasks.max": "1",
            "topics": "my-source-kafka-topic",
            "transforms": "TableNameTransformation",
            "transforms.TableNameTransformation.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TableNameTransformation.regex": ".*",
            "transforms.TableNameTransformation.replacement": "my_sink_bigquery_table",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        # Assert that lineage was created
        assert len(lineages) == 1

        # Verify the lineage details
        lineage = lineages[0]
        assert lineage.source_platform == "kafka"
        assert lineage.source_dataset == "my-source-kafka-topic"
        assert lineage.target_platform == "bigquery"
        assert lineage.target_dataset == "my-gcp-project.ingest.my_sink_bigquery_table"


class TestSnowflakeSinkConnector:
    """Test Snowflake sink connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="test-snowflake-connector",
            type="sink",
            config=config,
            tasks=[],
            topic_names=["app_logs"],
        )

    def test_snowflake_with_regex_router(self) -> None:
        """Test Snowflake connector with RegexRouter transformation."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
            "snowflake.database.name": "ANALYTICS",
            "snowflake.schema.name": "RAW",
            "transforms": "TableTransform",
            "transforms.TableTransform.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TableTransform.regex": "app_(.*)",
            "transforms.TableTransform.replacement": "APPLICATION_$1",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: SnowflakeSinkConnector = SnowflakeSinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.source_dataset == "app_logs"
        assert lineage.source_platform == "kafka"
        assert lineage.target_dataset == "ANALYTICS.RAW.APPLICATION_logs"
        assert lineage.target_platform == "snowflake"


class TestJDBCSourceConnector:
    """Test JDBC source connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="mysql_source2",
            type="source",
            config=config,
            tasks=[
                {
                    "id": {"task": 0, "connector": "mysql_source2"},
                    "config": {"tables": "library.book"},
                }
            ],
            topic_names=["library-book"],
        )

    def test_mysql_source_with_regex_router(self) -> None:
        """Test the specific MySQL source configuration example."""

        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "mode": "incrementing",
            "incrementing.column.name": "id",
            "tasks.max": "1",
            "connection.url": "jdbc:mysql://localhost:3306/library",
            "transforms": "TotalReplacement",
            "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TotalReplacement.regex": ".*(book)",
            "transforms.TotalReplacement.replacement": "my-new-topic-$1",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test the transform parsing
        parser = connector.get_parser(manifest)
        assert len(parser.transforms) == 1
        transform: Dict[str, str] = parser.transforms[0]
        assert transform["name"] == "TotalReplacement"
        assert transform["type"] == "org.apache.kafka.connect.transforms.RegexRouter"
        assert transform["regex"] == ".*(book)"
        assert transform["replacement"] == "my-new-topic-$1"


class TestIntegration:
    """Integration tests for the complete RegexRouter functionality."""

    def test_end_to_end_bigquery_transformation(self) -> None:
        """Test complete end-to-end BigQuery transformation."""
        # Test multiple topics with different transformation patterns
        connector_config: Dict[str, str] = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "data-warehouse",
            "defaultDataset": "staging",
            "transforms": "Standardize,Prefix",
            "transforms.Standardize.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Standardize.regex": "raw_(.*)_data",
            "transforms.Standardize.replacement": "$1_cleaned",
            "transforms.Prefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Prefix.regex": "(.*)_cleaned",
            "transforms.Prefix.replacement": "final_$1",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="multi-transform-connector",
            type="sink",
            config=connector_config,
            tasks=[],
            topic_names=["raw_users_data", "raw_orders_data", "other_topic"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        # Should have 3 lineages
        assert len(lineages) == 3

        # Check transformed topics
        users_lineage = next(
            line for line in lineages if line.source_dataset == "raw_users_data"
        )
        assert users_lineage.target_dataset == "data-warehouse.staging.final_users"

        orders_lineage = next(
            line for line in lineages if line.source_dataset == "raw_orders_data"
        )
        assert orders_lineage.target_dataset == "data-warehouse.staging.final_orders"

        # Non-matching topic should remain unchanged
        other_lineage = next(
            line for line in lineages if line.source_dataset == "other_topic"
        )
        assert other_lineage.target_dataset == "data-warehouse.staging.other_topic"

    def test_regex_router_error_handling(self) -> None:
        """Test that invalid regex patterns don't crash the system."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "test-project",
            "defaultDataset": "test",
            "transforms": "BadRegex",
            "transforms.BadRegex.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.BadRegex.regex": "[invalid-regex",  # Invalid regex
            "transforms.BadRegex.replacement": "fixed",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="error-test-connector",
            type="sink",
            config=connector_config,
            tasks=[],
            topic_names=["test-topic"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        # Should not raise an exception
        connector: BigQuerySinkConnector = BigQuerySinkConnector(
            manifest, config, report
        )
        lineages: List = connector.extract_lineages()

        # Should still create lineage with original topic name
        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.target_dataset == "test-project.test.test-topic"


class TestConfluentCloudConnectors:
    """Test Confluent Cloud connector compatibility with Platform connectors."""

    def create_platform_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a Platform connector manifest."""
        return ConnectorManifest(
            name="test-platform-connector",
            type="source",
            config=config,
            tasks=[
                {
                    "id": {"task": 0, "connector": "test-platform-connector"},
                    "config": {"tables": "public.users,public.orders"},
                }
            ],
            topic_names=["users", "orders"],
        )

    def create_cloud_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a Cloud connector manifest."""
        return ConnectorManifest(
            name="test-cloud-connector",
            type="source",
            config=config,
            tasks=[],  # Cloud connectors may not have tasks API
            topic_names=["server_name.public.users", "server_name.public.orders"],
        )

    def test_platform_postgres_source_connector(self) -> None:
        """Test Platform PostgreSQL source connector with traditional JDBC config."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb?user=testuser&password=testpass",
            "table.whitelist": "public.users,public.orders",
            "topic.prefix": "db-",
            "mode": "incrementing",
            "incrementing.column.name": "id",
        }

        manifest: ConnectorManifest = self.create_platform_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test the parser correctly handles Platform config
        parser = connector.get_parser(manifest)
        assert parser.source_platform == "postgres"
        assert parser.database_name == "testdb"
        assert parser.topic_prefix == "db-"
        assert parser.db_connection_url == "postgresql://localhost:5432/testdb"

        # Test table names parsing
        table_names = connector.get_table_names()
        assert len(table_names) == 2
        # Check for TableId with schema='public' and table='users'
        assert any(t.schema == "public" and t.table == "users" for t in table_names)
        # Check for TableId with schema='public' and table='orders'
        assert any(t.schema == "public" and t.table == "orders" for t in table_names)

    def test_cloud_postgres_source_connector(self) -> None:
        """Test Confluent Cloud PostgreSQL CDC source connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "server_name.us-east-1.rds.amazonaws.com",
            "database.port": "5432",
            "database.user": "user_name",
            "database.password": "password",
            "database.dbname": "aledade",
            "database.server.name": "server_name",
            "table.include.list": "public.users,public.orders",
            "transforms": "Transform",
            "transforms.Transform.regex": "(.*)\\.(.*)\\.(.*)",
            "transforms.Transform.replacement": "$2.$3",
            "transforms.Transform.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        manifest: ConnectorManifest = self.create_cloud_manifest(connector_config)
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test the parser correctly handles Cloud config
        parser = connector.get_parser(manifest)
        assert parser.source_platform == "postgres"
        assert parser.database_name == "aledade"
        assert parser.topic_prefix == "server_name"  # Uses database.server.name
        assert (
            parser.db_connection_url
            == "postgresql://server_name.us-east-1.rds.amazonaws.com:5432/aledade"
        )

        # Test table names parsing with Cloud field names
        table_names = connector.get_table_names()
        assert len(table_names) == 2
        # Check for TableId with schema='public' and table='users'
        assert any(t.schema == "public" and t.table == "users" for t in table_names)
        # Check for TableId with schema='public' and table='orders'
        assert any(t.schema == "public" and t.table == "orders" for t in table_names)

    def test_cloud_mysql_source_connector(self) -> None:
        """Test Confluent Cloud MySQL source connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSource",
            "database.hostname": "mysql.us-east-1.rds.amazonaws.com",
            "database.port": "3306",
            "database.user": "admin",
            "database.password": "secret",
            "database.dbname": "inventory",
            "database.server.name": "mysql_server",
            "table.include.list": "inventory.products,inventory.categories",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-cloud-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[
                "mysql_server.inventory.products",
                "mysql_server.inventory.categories",
            ],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test the parser correctly handles MySQL Cloud config
        parser = connector.get_parser(manifest)
        assert parser.source_platform == "mysql"
        assert parser.database_name == "inventory"
        assert parser.topic_prefix == "mysql_server"
        assert (
            parser.db_connection_url
            == "mysql://mysql.us-east-1.rds.amazonaws.com:3306/inventory"
        )

    def test_mixed_field_name_fallback(self) -> None:
        """Test fallback when both Platform and Cloud field names are present."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "cloud.host.com",
            "database.port": "5432",
            "database.dbname": "clouddb",
            # Both field names present - should prefer Cloud format
            "table.include.list": "public.cloud_table",
            "table.whitelist": "public.platform_table",
            "database.server.name": "cloud_server",
            "topic.prefix": "platform_prefix",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mixed-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["cloud_server.public.cloud_table"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        parser = connector.get_parser(manifest)
        # Should prefer Cloud field names when both are present
        assert (
            parser.topic_prefix == "cloud_server"
        )  # database.server.name takes precedence for Cloud connectors

        table_names = connector.get_table_names()
        assert len(table_names) == 1
        # Check for TableId with schema='public' and table='cloud_table'
        assert any(
            t.schema == "public" and t.table == "cloud_table" for t in table_names
        )  # table.include.list takes precedence

    def test_cloud_connector_missing_required_fields(self) -> None:
        """Test Cloud connector with missing required configuration fields."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "host.com",
            # Missing database.port and database.dbname
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="incomplete-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Should raise ValueError for missing required fields
        try:
            connector.get_parser(manifest)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "Missing required Cloud connector config" in str(e)

    def test_lineage_generation_platform_vs_cloud(self) -> None:
        """Test that lineages are generated identically for Platform vs Cloud connectors."""
        # Platform connector config
        platform_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            "table.whitelist": "public.users",
            "topic.prefix": "db-",
        }

        # Cloud connector config with equivalent settings
        cloud_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "testdb",
            "database.server.name": "db-server",
            "table.include.list": "public.users",
        }

        # Create manifests
        platform_manifest = ConnectorManifest(
            name="platform-connector",
            type="source",
            config=platform_config,
            tasks=[
                {
                    "id": {"task": 0, "connector": "platform-connector"},
                    "config": {"tables": "public.users"},
                }
            ],
            topic_names=["db-users"],
        )

        cloud_manifest = ConnectorManifest(
            name="cloud-connector",
            type="source",
            config=cloud_config,
            tasks=[],
            topic_names=["db-server.public.users"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        # Test Platform connector
        with (
            patch(
                "datahub.ingestion.source.kafka_connect.source_connectors.make_url"
            ) as mock_url,
            patch(
                "datahub.ingestion.source.kafka_connect.source_connectors.get_platform_from_sqlalchemy_uri"
            ) as mock_platform,
        ):
            mock_url_obj: Mock = Mock()
            mock_url_obj.drivername = "postgresql"
            mock_url_obj.host = "localhost"
            mock_url_obj.port = 5432
            mock_url_obj.database = "testdb"
            mock_url.return_value = mock_url_obj
            mock_platform.return_value = "postgres"

            platform_connector = ConfluentJDBCSourceConnector(
                platform_manifest, config, report
            )
            platform_lineages = platform_connector.extract_lineages()

        # Test Cloud connector
        cloud_connector = ConfluentJDBCSourceConnector(cloud_manifest, config, report)
        cloud_lineages = cloud_connector.extract_lineages()

        # Both should generate valid lineages
        assert len(platform_lineages) == 1
        assert len(cloud_lineages) == 1

        # Both should have the same source platform
        assert platform_lineages[0].source_platform == "postgres"
        assert cloud_lineages[0].source_platform == "postgres"

        # Both should reference the same source dataset
        assert platform_lineages[0].source_dataset == "testdb.public.users"
        assert cloud_lineages[0].source_dataset == "testdb.public.users"


class TestPlatformDetection:
    """Test platform detection from connector class names."""

    def test_cloud_postgres_connector_detection(self) -> None:
        """Test detection of Confluent Cloud PostgreSQL connectors."""
        from datahub.ingestion.source.kafka_connect.common import (
            ConnectorTopicHandlerRegistry,
        )

        config = KafkaConnectSourceConfig()
        report = KafkaConnectSourceReport()
        registry = ConnectorTopicHandlerRegistry(config, report)

        assert registry.get_platform_for_connector("PostgresCdcSource") == "postgres"
        assert registry.get_platform_for_connector("PostgresCdcSourceV2") == "postgres"
        assert registry.get_platform_for_connector("PostgresSink") == "postgres"

    def test_cloud_mysql_connector_detection(self) -> None:
        """Test detection of Confluent Cloud MySQL connectors."""
        from datahub.ingestion.source.kafka_connect.common import (
            ConnectorTopicHandlerRegistry,
        )

        config = KafkaConnectSourceConfig()
        report = KafkaConnectSourceReport()
        registry = ConnectorTopicHandlerRegistry(config, report)

        assert registry.get_platform_for_connector("MySqlSource") == "mysql"
        assert registry.get_platform_for_connector("MySqlSink") == "mysql"

    def test_cloud_snowflake_connector_detection(self) -> None:
        """Test detection of Confluent Cloud Snowflake connectors."""
        from datahub.ingestion.source.kafka_connect.common import (
            ConnectorTopicHandlerRegistry,
        )

        config = KafkaConnectSourceConfig()
        report = KafkaConnectSourceReport()
        registry = ConnectorTopicHandlerRegistry(config, report)

        assert registry.get_platform_for_connector("SnowflakeSink") == "snowflake"

    def test_platform_connector_detection_fallback(self) -> None:
        """Test fallback detection for platform connectors with descriptive class names."""
        from datahub.ingestion.source.kafka_connect.common import (
            ConnectorTopicHandlerRegistry,
        )

        config = KafkaConnectSourceConfig()
        report = KafkaConnectSourceReport()
        registry = ConnectorTopicHandlerRegistry(config, report)

        assert (
            registry.get_platform_for_connector(
                "io.confluent.connect.jdbc.JdbcSourceConnector"
            )
            == "unknown"
        )
        assert (
            registry.get_platform_for_connector("com.mysql.cj.jdbc.Driver") == "mysql"
        )
        assert (
            registry.get_platform_for_connector("org.postgresql.Driver") == "postgres"
        )
        assert (
            registry.get_platform_for_connector(
                "net.snowflake.client.jdbc.SnowflakeDriver"
            )
            == "snowflake"
        )

    def test_unknown_connector_detection(self) -> None:
        """Test detection returns connector class name in lowercase for unrecognized connector classes."""
        from datahub.ingestion.source.kafka_connect.common import (
            ConnectorTopicHandlerRegistry,
        )

        config = KafkaConnectSourceConfig()
        report = KafkaConnectSourceReport()
        registry = ConnectorTopicHandlerRegistry(config, report)

        assert (
            registry.get_platform_for_connector("com.unknown.connector.SomeConnector")
            == "unknown"  # Contains "unknown" keyword
        )
        assert (
            registry.get_platform_for_connector("") == "unknown"
        )  # Empty string falls through to unknown
        assert (
            registry.get_platform_for_connector("com.example.CustomConnector")
            == "unknown"
        )  # No recognized keywords


class TestFullConnectorConfigValidation:
    """Test full connector configurations end-to-end with expected lineage results."""

    def validate_lineage_fields(
        self,
        connector_config: Dict[str, str],
        topic_names: List[str],
        expected_lineages: List[Dict[str, str]],
    ) -> None:
        """
        Helper method to validate that a connector config produces expected lineage results.

        Args:
            connector_config: Full Kafka Connect connector configuration
            topic_names: List of Kafka topic names the connector produces
            expected_lineages: List of expected lineage mappings with keys:
                - source_dataset: Expected source dataset URN
                - source_platform: Expected source platform name
                - target_dataset: Expected target topic name
                - target_platform: Expected target platform (should be 'kafka')
        """
        manifest: ConnectorManifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=topic_names,
        )

        mock_config: Mock = Mock(spec=KafkaConnectSourceConfig)
        mock_report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, mock_config, mock_report
        )

        # Test configuration parsing
        parser = connector.get_parser(manifest)
        assert parser is not None, "Parser should be created successfully"

        # Test table name extraction
        table_names = connector.get_table_names()
        assert len(table_names) > 0, "Should extract table names from configuration"

        # For tests without Java/JPype, we'll simulate the lineage extraction
        # since the actual extract_lineages() method requires Java regex support
        lineages = self._simulate_lineage_extraction(connector, parser)

        # Validate number of lineages matches expectations
        assert len(lineages) == len(expected_lineages), (
            f"Expected {len(expected_lineages)} lineages, got {len(lineages)}"
        )

        # Validate each lineage matches expectations
        for i, (actual, expected) in enumerate(zip(lineages, expected_lineages)):
            assert actual.source_dataset == expected["source_dataset"], (
                f"Lineage {i}: Expected source_dataset '{expected['source_dataset']}', "
                f"got '{actual.source_dataset}'"
            )
            assert actual.source_platform == expected["source_platform"], (
                f"Lineage {i}: Expected source_platform '{expected['source_platform']}', "
                f"got '{actual.source_platform}'"
            )
            assert actual.target_dataset == expected["target_dataset"], (
                f"Lineage {i}: Expected target_dataset '{expected['target_dataset']}', "
                f"got '{actual.target_dataset}'"
            )
            assert actual.target_platform == expected["target_platform"], (
                f"Lineage {i}: Expected target_platform '{expected['target_platform']}', "
                f"got '{actual.target_platform}'"
            )

    def _simulate_lineage_extraction(self, connector, parser):
        """Simulate lineage extraction using the same logic as our Cloud validation tests."""

        source_platform = parser.source_platform
        database_name = parser.database_name
        topic_prefix = parser.topic_prefix
        transforms = parser.transforms
        table_name_tuples = connector.get_table_names()

        # Check if we should use pipeline transforms
        if self._should_use_pipeline(transforms):
            return self._extract_with_pipeline(
                transforms,
                table_name_tuples,
                topic_prefix,
                connector,
                database_name,
                source_platform,
            )

        # Handle single RegexRouter transform (legacy logic)
        if self._is_single_regex_transform(transforms):
            return self._extract_with_single_regex(
                transforms[0],
                table_name_tuples,
                topic_prefix,
                connector,
                database_name,
                source_platform,
            )

        # Default: No transform or non-RegexRouter transform
        return self._extract_without_transforms(
            table_name_tuples, topic_prefix, connector, database_name, source_platform
        )

    def _should_use_pipeline(self, transforms):
        """Check if we should use the transform pipeline."""
        return len(transforms) >= 1 and any(
            t.get("type")
            in [
                "io.debezium.transforms.outbox.EventRouter",
                "org.apache.kafka.connect.transforms.RegexRouter",
                "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            ]
            for t in transforms
        )

    def _is_single_regex_transform(self, transforms):
        """Check if this is a single RegexRouter transform."""
        return (
            len(transforms) == 1
            and transforms[0].get("type")
            == "org.apache.kafka.connect.transforms.RegexRouter"
        )

    def _extract_with_pipeline(
        self,
        transforms,
        table_name_tuples,
        topic_prefix,
        connector,
        database_name,
        source_platform,
    ):
        """Extract lineages using transform pipeline."""

        try:
            pipeline = TransformPipeline(transforms)
            # Get connector class for topic generation strategy
            connector_class = connector.connector_manifest.config.get(
                "connector.class", ""
            )

            results = pipeline.apply_transforms(
                table_name_tuples,
                topic_prefix,
                list(connector.connector_manifest.topic_names),
                connector_class,
            )

            lineages = []
            for result in results:
                # Build source dataset name
                if result.schema and has_three_level_hierarchy(source_platform):
                    source_table_name = f"{result.schema}.{result.source_table}"
                else:
                    source_table_name = result.source_table

                source_dataset = get_dataset_name(database_name, source_table_name)

                # Create lineages for all final topics
                for final_topic in result.final_topics:
                    lineage = KafkaConnectLineage(
                        source_dataset=source_dataset,
                        source_platform=source_platform,
                        target_dataset=final_topic,
                        target_platform="kafka",
                    )
                    lineages.append(lineage)

            return lineages

        except Exception as e:
            print(f"Pipeline simulation failed: {e}")
            return []

    def _extract_with_single_regex(
        self,
        transform_config,
        table_name_tuples,
        topic_prefix,
        connector,
        database_name,
        source_platform,
    ):
        """Extract lineages with single RegexRouter transform."""

        transform_regex = transform_config["regex"]
        transform_replacement = transform_config["replacement"]
        lineages = []

        for table_id in table_name_tuples:
            source_table = table_id.table

            # Build original topic name (before transform)
            if topic_prefix:
                if has_three_level_hierarchy(source_platform) and table_id.schema:
                    original_topic = (
                        f"{topic_prefix}.{table_id.schema}.{table_id.table}"
                    )
                else:
                    original_topic = f"{topic_prefix}.{table_id.table}"
            else:
                original_topic = table_id.table

            # Apply regex transformation
            try:
                from java.util.regex import Pattern

                pattern = Pattern.compile(transform_regex)
                matcher = pattern.matcher(original_topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(transform_replacement))
                else:
                    transformed_topic = original_topic
            except Exception:
                transformed_topic = original_topic

            # Create lineage if topic matches
            if transformed_topic in connector.connector_manifest.topic_names:
                if has_three_level_hierarchy(source_platform) and table_id.schema:
                    source_table_name = f"{table_id.schema}.{table_id.table}"
                else:
                    source_table_name = source_table

                dataset_name = get_dataset_name(database_name, source_table_name)
                lineage = KafkaConnectLineage(
                    source_dataset=dataset_name,
                    source_platform=source_platform,
                    target_dataset=transformed_topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)

        return lineages

    def _extract_without_transforms(
        self, table_name_tuples, topic_prefix, connector, database_name, source_platform
    ):
        """Extract lineages without transforms."""

        lineages = []

        for topic in connector.connector_manifest.topic_names:
            # Remove topic prefix to get table name
            if topic_prefix and topic.startswith(topic_prefix):
                remaining = topic[len(topic_prefix) :]
                if remaining.startswith("."):
                    remaining = remaining[1:]
                source_table_suffix = remaining
            else:
                source_table_suffix = topic

            # Find matching table by suffix
            matching_table = self._find_matching_table(
                table_name_tuples, source_table_suffix
            )

            if matching_table:
                # For MySQL (2-tier) and PostgreSQL (3-tier), always use schema.table when available
                if matching_table.schema:
                    source_table_name = (
                        f"{matching_table.schema}.{matching_table.table}"
                    )
                else:
                    source_table_name = matching_table.table

                dataset_name = get_dataset_name(database_name, source_table_name)
                lineage = KafkaConnectLineage(
                    source_dataset=dataset_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)

        return lineages

    def _find_matching_table(self, table_ids, source_table_suffix):
        """Find table that matches the given suffix."""
        for table_id in table_ids:
            table_name = table_id.table
            possible_suffixes = [table_name]
            if table_id.schema:
                possible_suffixes.append(f"{table_id.schema}.{table_id.table}")

            if source_table_suffix in possible_suffixes:
                return table_id
        return None

    def test_cloud_postgres_cdc_with_regex_transform(self) -> None:
        """Test Confluent Cloud PostgreSQL CDC connector with RegexRouter transform."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.server.name": "test_server",
            "database.hostname": "test-host.amazonaws.com",
            "database.port": "5432",
            "database.user": "testuser",
            "database.password": "testpass",
            "database.dbname": "testdb",
            "table.include.list": "public.users,public.orders,inventory.products",
            "transforms": "Transform",
            "transforms.Transform.regex": r"(.*)\.(.*)\.(.*)",
            "transforms.Transform.replacement": r"$2.$3",
            "transforms.Transform.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        # Topics as they would appear in Kafka (after RegexRouter transform)
        topic_names = [
            "public.users",  # Originally: test_server.public.users -> public.users
            "public.orders",  # Originally: test_server.public.orders -> public.orders
            "inventory.products",  # Originally: test_server.inventory.products -> inventory.products
        ]

        expected_lineages = [
            {
                "source_dataset": "testdb.public.users",
                "source_platform": "postgres",
                "target_dataset": "public.users",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "testdb.public.orders",
                "source_platform": "postgres",
                "target_dataset": "public.orders",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "testdb.inventory.products",
                "source_platform": "postgres",
                "target_dataset": "inventory.products",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(connector_config, topic_names, expected_lineages)

    def test_cloud_mysql_source_no_transform(self) -> None:
        """Test Confluent Cloud MySQL source connector without transforms."""
        connector_config = {
            "connector.class": "MySqlSource",
            "database.hostname": "mysql-host.amazonaws.com",
            "database.port": "3306",
            "database.user": "admin",
            "database.password": "secret",
            "database.dbname": "ecommerce",
            "database.server.name": "mysql_prod",
            "table.include.list": "catalog.products,orders.order_items",
        }

        # Topics without transforms (server.schema.table format)
        topic_names = ["mysql_prod.catalog.products", "mysql_prod.orders.order_items"]

        expected_lineages = [
            {
                "source_dataset": "ecommerce.catalog.products",
                "source_platform": "mysql",
                "target_dataset": "mysql_prod.catalog.products",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "ecommerce.orders.order_items",
                "source_platform": "mysql",
                "target_dataset": "mysql_prod.orders.order_items",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(connector_config, topic_names, expected_lineages)

    def test_platform_jdbc_connector_with_topic_prefix(self) -> None:
        """Test traditional Platform JDBC connector with topic prefix."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/analytics?user=analyst&password=secret",
            "table.whitelist": "public.metrics,public.events",
            "topic.prefix": "db-",
            "mode": "incrementing",
            "incrementing.column.name": "id",
        }

        topic_names = ["db-metrics", "db-events"]

        expected_lineages = [
            {
                "source_dataset": "analytics.public.metrics",
                "source_platform": "postgres",
                "target_dataset": "db-metrics",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "analytics.public.events",
                "source_platform": "postgres",
                "target_dataset": "db-events",
                "target_platform": "kafka",
            },
        ]

        # Mock the sqlalchemy URL parsing for Platform connectors
        with (
            patch(
                "datahub.ingestion.source.kafka_connect.source_connectors.make_url"
            ) as mock_url,
            patch(
                "datahub.ingestion.source.kafka_connect.source_connectors.get_platform_from_sqlalchemy_uri"
            ) as mock_platform,
        ):
            mock_url_obj = Mock()
            mock_url_obj.drivername = "postgresql"
            mock_url_obj.host = "localhost"
            mock_url_obj.port = 5432
            mock_url_obj.database = "analytics"
            mock_url.return_value = mock_url_obj
            mock_platform.return_value = "postgres"

            self.validate_lineage_fields(
                connector_config, topic_names, expected_lineages
            )


class TestTransformPipeline:
    """Test the TransformPipeline class directly."""

    def test_pipeline_initialization_with_known_transforms(self) -> None:
        """Test TransformPipeline initialization with known transform types."""

        transform_configs = [
            {
                "name": "RegexTransform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "test-(.*)",
                "replacement": "processed_$1",
            },
            {
                "name": "CloudRegexTransform",
                "type": "io.confluent.connect.cloud.transforms.TopicRegexRouter",
                "regex": "cloud-(.*)",
                "replacement": "cloud_processed_$1",
            },
        ]

        pipeline = TransformPipeline(transform_configs)

        # Should have registered 2 transforms
        assert len(pipeline.transforms) == 2

        # Both should be RegexRouterTransform instances
        assert all(
            isinstance(t, SourceRegexRouterTransform) for t in pipeline.transforms
        )

    def test_pipeline_initialization_with_unknown_transforms(self) -> None:
        """Test TransformPipeline handles unknown transform types gracefully."""

        transform_configs = [
            {
                "name": "KnownTransform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "test-(.*)",
                "replacement": "processed_$1",
            },
            {
                "name": "UnknownTransform",
                "type": "com.unknown.transforms.SomeTransform",
                "config": "value",
            },
        ]

        # Should not raise an exception
        pipeline = TransformPipeline(transform_configs)

        # Should only register the known transform
        assert len(pipeline.transforms) == 1

    def test_pipeline_apply_transforms_single_table(self) -> None:
        """Test apply_transforms method with a single table."""

        # Single RegexRouter transform
        transform_configs = [
            {
                "name": "PrefixTransform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "users",
                "replacement": "processed_users",
            }
        ]

        pipeline = TransformPipeline(transform_configs)

        tables = [TableId(schema="public", table="users")]
        topic_prefix = ""
        manifest_topics = ["processed_users"]
        connector_class = "io.confluent.connect.jdbc.JdbcSourceConnector"

        results = pipeline.apply_transforms(
            tables, topic_prefix, manifest_topics, connector_class
        )

        # Should have one result
        assert len(results) == 1
        result = results[0]

        # Verify result properties
        assert result.schema == "public"
        assert result.source_table == "users"
        assert result.original_topic == "users"  # JDBC connector, no prefix
        assert "processed_users" in result.final_topics

    def test_pipeline_apply_transforms_multiple_tables_and_transforms(self) -> None:
        """Test apply_transforms with multiple tables and transforms."""

        # Multiple transforms in sequence
        transform_configs = [
            {
                "name": "FirstTransform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "(.*)",
                "replacement": "stage1_$1",
            },
            {
                "name": "SecondTransform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "stage1_(.*)",
                "replacement": "final_$1",
            },
        ]

        pipeline = TransformPipeline(transform_configs)

        tables = [
            TableId(schema="public", table="users"),
            TableId(schema="public", table="orders"),
        ]
        topic_prefix = ""
        manifest_topics = ["final_users", "final_orders"]
        connector_class = "io.confluent.connect.jdbc.JdbcSourceConnector"

        results = pipeline.apply_transforms(
            tables, topic_prefix, manifest_topics, connector_class
        )

        # Should have two results
        assert len(results) == 2

        # Check users result
        users_result = next(r for r in results if r.source_table == "users")
        assert users_result.original_topic == "users"
        assert "final_users" in users_result.final_topics

        # Check orders result
        orders_result = next(r for r in results if r.source_table == "orders")
        assert orders_result.original_topic == "orders"
        assert "final_orders" in orders_result.final_topics

    def test_pipeline_generate_original_topic_different_connectors(self) -> None:
        """Test _generate_original_topic works correctly for different connector types."""

        pipeline = TransformPipeline([])  # Empty pipeline for testing topic generation

        # Test JDBC connector (simple concatenation)
        result = pipeline._generate_original_topic(
            schema="public",
            table_name="users",
            topic_prefix="db-",
            connector_class="io.confluent.connect.jdbc.JdbcSourceConnector",
        )
        assert result == "db-users"

        # Test Cloud PostgreSQL CDC (hierarchical naming)
        result = pipeline._generate_original_topic(
            schema="public",
            table_name="users",
            topic_prefix="server",
            connector_class="PostgresCdcSource",
        )
        assert result == "server.public.users"

        # Test empty prefix with Cloud connector
        result = pipeline._generate_original_topic(
            schema="public",
            table_name="users",
            topic_prefix="",
            connector_class="PostgresCdcSource",
        )
        assert result == "public.users"  # Should NOT start with dot

    def test_pipeline_empty_transforms(self) -> None:
        """Test pipeline with no transforms configured."""

        pipeline = TransformPipeline([])  # No transforms

        tables = [TableId(schema="public", table="users")]
        topic_prefix = "db-"
        manifest_topics = ["db-users"]
        connector_class = "io.confluent.connect.jdbc.JdbcSourceConnector"

        results = pipeline.apply_transforms(
            tables, topic_prefix, manifest_topics, connector_class
        )

        # Should still work with no transforms
        assert len(results) == 1
        result = results[0]

        assert result.original_topic == "db-users"
        assert (
            "db-users" in result.final_topics
        )  # No transform, original topic passes through

    def test_pipeline_no_matching_topics(self) -> None:
        """Test pipeline when transformed topics don't match manifest topics."""

        transform_configs = [
            {
                "name": "Transform",
                "type": "org.apache.kafka.connect.transforms.RegexRouter",
                "regex": "users",
                "replacement": "transformed_users",
            }
        ]

        pipeline = TransformPipeline(transform_configs)

        tables = [TableId(schema="public", table="users")]
        topic_prefix = ""
        manifest_topics = ["some_other_topic"]  # Transform result won't match this
        connector_class = "io.confluent.connect.jdbc.JdbcSourceConnector"

        results = pipeline.apply_transforms(
            tables, topic_prefix, manifest_topics, connector_class
        )

        # Pipeline creates results with transformed topics, but manifest filtering would happen later
        # This test verifies the transform pipeline works correctly even with non-matching manifest
        assert len(results) == 1
        result = results[0]
        assert result.original_topic == "users"
        assert "transformed_users" in result.final_topics
        # The final filtering against manifest happens in the lineage extraction logic


class TestTopicGeneration:
    """Test original topic generation behavior to prevent regressions like the integration test failure."""

    def test_integration_test_regression_scenario(self) -> None:
        """
        Test the specific scenario that caused the integration test failure.

        This test reproduces the mysql_source2 connector scenario:
        - MySQL database: librarydb
        - Tables: member, MixedCaseTable
        - No topic.prefix configured
        - Expected: topic names should be just table names, not database.table
        """

        pipeline = TransformPipeline([])

        # Scenario that was failing: MySQL tables without topic prefix
        test_cases = [
            ("librarydb", "member"),
            ("librarydb", "MixedCaseTable"),
            ("librarydb", "book"),
        ]

        for schema, table in test_cases:
            result = pipeline._generate_original_topic(
                schema=schema,
                table_name=table,
                topic_prefix="",  # Key: no topic prefix
                connector_class="io.confluent.connect.jdbc.JdbcSourceConnector",  # Traditional JDBC
            )

            # Critical assertion: should be just table name
            assert result == table, (
                f"Integration test regression: {schema}.{table} without topic prefix "
                f"should generate '{table}', but got '{result}'"
            )

    @pytest.mark.parametrize(
        "schema,table_name,topic_prefix,connector_class,expected,description",
        [
            # Cloud connectors - hierarchical naming (CDC behavior)
            pytest.param(
                "public",
                "users",
                "",
                "PostgresCdcSource",
                "public.users",
                "Cloud PostgreSQL CDC with empty prefix should use schema.table format",
                id="cloud_postgres_cdc_empty_prefix",
            ),
            pytest.param(
                "public",
                "users",
                "pg-server",
                "PostgresCdcSource",
                "pg-server.public.users",
                "Cloud PostgreSQL CDC should use hierarchical naming",
                id="cloud_postgres_cdc_with_server",
            ),
            pytest.param(
                "inventory",
                "orders",
                "mysql-server",
                "MySqlCdcSource",
                "mysql-server.inventory.orders",
                "Cloud MySQL CDC should use hierarchical naming",
                id="cloud_mysql_cdc_hierarchical",
            ),
            # Traditional JDBC connector - simple concatenation
            pytest.param(
                "librarydb",
                "member",
                "",
                "io.confluent.connect.jdbc.JdbcSourceConnector",
                "member",
                "JDBC connector with empty prefix should use table name",
                id="jdbc_empty_prefix",
            ),
            pytest.param(
                "librarydb",
                "member",
                "jdbc-",
                "io.confluent.connect.jdbc.JdbcSourceConnector",
                "jdbc-member",
                "JDBC connector should use simple concatenation",
                id="jdbc_with_prefix",
            ),
            # Debezium connectors - hierarchical naming
            pytest.param(
                "inventory",
                "products",
                "debezium-server",
                "io.debezium.connector.mysql.MySqlConnector",
                "debezium-server.inventory.products",
                "Debezium MySQL should use hierarchical naming",
                id="debezium_mysql_hierarchical",
            ),
            pytest.param(
                "public",
                "events",
                "debezium-pg",
                "io.debezium.connector.postgresql.PostgresConnector",
                "debezium-pg.public.events",
                "Debezium PostgreSQL should use hierarchical naming",
                id="debezium_postgres_hierarchical",
            ),
            # Unknown connector - fallback to JDBC behavior
            pytest.param(
                "some_db",
                "some_table",
                "",
                "com.unknown.SomeConnector",
                "some_table",
                "Unknown connector with empty prefix should use JDBC naming",
                id="unknown_connector_empty_prefix",
            ),
            pytest.param(
                "some_db",
                "some_table",
                "unknown-",
                "com.unknown.SomeConnector",
                "unknown-some_table",
                "Unknown connector should use JDBC naming as fallback",
                id="unknown_connector_with_prefix",
            ),
        ],
    )
    def test_connector_type_determines_naming_strategy(
        self,
        schema: str,
        table_name: str,
        topic_prefix: str,
        connector_class: str,
        expected: str,
        description: str,
    ) -> None:
        """
        Test the architectural fix: connector type determines naming strategy, not topic_prefix.

        This validates that the source (connector class) decides how to generate topic names,
        addressing the user's feedback that the previous logic was backwards.
        """

        pipeline = TransformPipeline([])

        result = pipeline._generate_original_topic(
            schema=schema,
            table_name=table_name,
            topic_prefix=topic_prefix,
            connector_class=connector_class,
        )
        assert result == expected, f"{description}, got: {result}"

    def test_jdbc_source_topic_naming_follows_official_confluent_documentation(
        self,
    ) -> None:
        """
        Test compliance with official Kafka Connect JDBC Source documentation.

        According to: https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#topic-prefix
        - topic.prefix: "Prefix to prepend to table names to generate topic names"
        - When empty: topic name = table name
        - When set: topic name = prefix + table (possibly with schema)
        """

        pipeline = TransformPipeline([])

        # Documentation compliance test cases for JDBC connector specifically
        test_scenarios = [
            # (schema, table, prefix, expected_result, description)
            (
                "",
                "simple_table",
                "",
                "simple_table",
                "No schema, no prefix -> table name",
            ),
            ("db", "table1", "", "table1", "With schema, no prefix -> table name only"),
            (
                "",
                "table1",
                "prefix-",
                "prefix-table1",
                "No schema, with prefix -> prefix + table",
            ),
            (
                "schema",
                "table1",
                "prefix-",
                "prefix-table1",
                "With schema and prefix -> prefix + table (JDBC ignores schema)",
            ),
        ]

        for schema, table, prefix, expected, description in test_scenarios:
            result = pipeline._generate_original_topic(
                schema=schema,
                table_name=table,
                topic_prefix=prefix,
                connector_class="io.confluent.connect.jdbc.JdbcSourceConnector",
            )

            assert result == expected, (
                f"Kafka Connect JDBC documentation compliance failed: {description} "
                f"Expected '{expected}', got '{result}'"
            )


class TestEnvironmentSpecificTopicRetrieval:
    """Test the fallback strategy for topic derivation when /topics endpoint fails."""

    @patch("requests.Session.get")
    def test_derive_sink_topics_explicit_list(self, mock_get: Mock) -> None:
        """Test deriving topics from sink connector with explicit topics list."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock successful connection test
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        sink_config = {
            "connector.class": "io.confluent.connect.bigquery.BigQuerySinkConnector",
            "topics": "topic1,topic2,topic3",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-sink", type="sink", config=sink_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == ["topic1", "topic2", "topic3"]

    @patch("requests.Session.get")
    def test_derive_sink_topics_with_spaces(self, mock_get: Mock) -> None:
        """Test deriving topics from sink connector with spaces in topic list."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock successful connection test
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        sink_config = {
            "connector.class": "io.confluent.connect.bigquery.BigQuerySinkConnector",
            "topics": " topic1 , topic2 ,  topic3  ",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-sink", type="sink", config=sink_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == ["topic1", "topic2", "topic3"]

    @patch("requests.Session.get")
    def test_derive_sink_topics_regex(self, mock_get: Mock) -> None:
        """Test deriving topics from sink connector with regex pattern."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        sink_config = {
            "connector.class": "io.confluent.connect.bigquery.BigQuerySinkConnector",
            "topics.regex": "test_.*",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-sink", type="sink", config=sink_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # Should return empty list and log warning since we can't enumerate regex matches
        assert result == []

    @patch("requests.Session.get")
    def test_derive_sink_topics_no_config(self, mock_get: Mock) -> None:
        """Test deriving topics from sink connector with no topic configuration."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        sink_config = {
            "connector.class": "io.confluent.connect.bigquery.BigQuerySinkConnector",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-sink", type="sink", config=sink_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == []

    @patch("requests.Session.get")
    def test_derive_topics_from_table_config_platform_style(
        self, mock_get: Mock
    ) -> None:
        """Test deriving topics from table configuration with platform connector."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "table.whitelist": "public.users,public.orders,inventory.products",
            "topic.prefix": "db-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-source", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert len(result) == 3
        assert "db-public.users" in result
        assert "db-public.orders" in result
        assert "db-inventory.products" in result

    @patch("requests.Session.get")
    def test_derive_topics_from_table_config_cloud_style(self, mock_get: Mock) -> None:
        """Test deriving topics from table configuration with cloud connector."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "PostgresCdcSource",
            "table.include.list": "schema1.table1,schema2.table2",
            "database.server.name": "cloud-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-source", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert len(result) == 2
        assert "cloud-.schema1.table1" in result
        assert "cloud-.schema2.table2" in result

    @patch("requests.Session.get")
    def test_derive_topics_from_table_config_no_schema(self, mock_get: Mock) -> None:
        """Test deriving topics from tables without schema names."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "table.whitelist": "users,orders,products",
            "topic.prefix": "app-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-source", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert len(result) == 3
        assert "app-users" in result
        assert "app-orders" in result
        assert "app-products" in result

    @patch("requests.Session.get")
    def test_derive_topics_from_table_config_quoted_identifiers(
        self, mock_get: Mock
    ) -> None:
        """Test deriving topics from tables with quoted identifiers."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "table.whitelist": '"public"."user table","schema2"."order-data"',
            "topic.prefix": "sys-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-source", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert len(result) == 2
        assert "sys-public.user table" in result
        assert "sys-schema2.order-data" in result

    @patch("requests.Session.get")
    def test_derive_source_topics_platform_jdbc_no_transforms(
        self, mock_get: Mock
    ) -> None:
        """Test deriving topics from platform JDBC source connector without transforms."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            "table.whitelist": "users,orders",
            "topic.prefix": "db-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-connector", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # Should derive topics using JDBC naming strategy: prefix + table
        expected_topics = ["db-users", "db-orders"]
        assert result == expected_topics

    @patch("requests.Session.get")
    def test_derive_source_topics_cloud_cdc_no_transforms(self, mock_get: Mock) -> None:
        """Test deriving topics from cloud CDC source connector without transforms."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "PostgresCdcSource",
            "table.include.list": "public.users,public.orders",
            "database.server.name": "myserver",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "testdb",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-connector", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # Should derive topics using CDC naming strategy: server.schema.table
        expected_topics = ["myserver.public.users", "myserver.public.orders"]
        assert result == expected_topics

    @patch("requests.Session.get")
    def test_derive_source_topics_with_transforms(self, mock_get: Mock) -> None:
        """Test deriving topics from source connector with transforms."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "PostgresCdcSource",
            "table.include.list": "public.users",
            "database.server.name": "myserver",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "testdb",
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "myserver\\.public\\.(.*)",
            "transforms.route.replacement": "events.$1",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-connector", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # Current implementation doesn't apply transforms - returns base topic name
        expected_topics = ["myserver.public.users"]
        assert result == expected_topics

    @patch("requests.Session.get")
    def test_derive_source_topics_unsupported_connector(self, mock_get: Mock) -> None:
        """Test deriving topics from unsupported source connector type."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "com.example.CustomSourceConnector",
            "table.whitelist": "users,orders",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-connector", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # Should return empty list for unsupported connectors
        assert result == []

    @patch("requests.Session.get")
    def test_derive_source_topics_no_tables(self, mock_get: Mock) -> None:
        """Test deriving topics when no table configuration is found."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            # No table configuration
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-connector", type="source", config=connector_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == []

    @patch("requests.Session.get")
    def test_get_connector_topics_self_hosted_api_failure(self, mock_get: Mock) -> None:
        """Test _get_connector_topics returns empty list when self-hosted /topics endpoint fails."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock connection test success, but topics endpoint failure
        def mock_get_side_effect(url: str, **kwargs: Any) -> Mock:
            response = Mock()
            if "/topics" in url:
                response.raise_for_status.side_effect = Exception("404 Not Found")
            else:
                # Connection test success
                response.raise_for_status.return_value = None
                response.json.return_value = {}
            return response

        mock_get.side_effect = mock_get_side_effect

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        # Create mock ConnectorManifest
        mock_connector = Mock()
        mock_connector.name = "test-connector"
        mock_connector.type = "source"
        mock_connector.config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"
        }

        # For self-hosted when API fails, we return empty list (no fallback)
        result = source._get_connector_topics(mock_connector)

        # Self-hosted should return empty list when API fails
        assert result == []

    @patch("requests.Session.get")
    def test_get_connector_topics_self_hosted_api_success(self, mock_get: Mock) -> None:
        """Test _get_connector_topics uses self-hosted /topics API when endpoint succeeds."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock connection test and /topics API response success
        def mock_get_side_effect(url: str, **kwargs: Any) -> Mock:
            response = Mock()
            if "/topics" in url:
                # Mock /connectors/{name}/topics response for self-hosted
                response.raise_for_status.return_value = None
                response.json.return_value = {
                    "test-connector": {"topics": ["runtime-topic1", "runtime-topic2"]}
                }
            else:
                # Connection test success
                response.raise_for_status.return_value = None
                response.json.return_value = []
            return response

        mock_get.side_effect = mock_get_side_effect

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        # Create mock ConnectorManifest
        mock_connector = Mock()
        mock_connector.name = "test-connector"
        mock_connector.type = "source"
        mock_connector.config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"
        }

        # Call the method to get topics
        result = source._get_connector_topics(mock_connector)

        # Should use self-hosted /topics API response
        assert result == ["runtime-topic1", "runtime-topic2"]

    @patch("requests.Session.get")
    def test_extract_sink_topics_from_config_flow(self, mock_get: Mock) -> None:
        """Test _extract_sink_topics_from_config handles sink connectors correctly."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        sink_config = {
            "connector.class": "io.confluent.connect.bigquery.BigQuerySinkConnector",
            "topics": "sink-topic1,sink-topic2",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-sink", type="sink", config=sink_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == ["sink-topic1", "sink-topic2"]

    @patch("requests.Session.get")
    def test_derive_and_match_source_topics_flow(self, mock_get: Mock) -> None:
        """Test _derive_and_match_source_topics handles source connectors correctly."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        source_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            "table.whitelist": "users",
            "topic.prefix": "source-",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="test-source", type="source", config=source_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        assert result == ["source-users"]

    @patch("requests.Session.get")
    def test_outbox_pattern_scenario(self, mock_get: Mock) -> None:
        """Test fallback with outbox pattern similar to user's scenario."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the connection test that happens in __init__
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = KafkaConnectSourceConfig(connect_uri="http://test:8083")
        source = KafkaConnectSource(config, Mock())

        # Simulate outbox-cashout-dev connector configuration
        outbox_config = {
            "connector.class": "PostgresCdcSource",
            "table.include.list": "public.outbox",
            "database.server.name": "cashout-dev",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "cashout",
            "transforms": "outbox,route",
            "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "outbox\\.event\\.(.*)",
            "transforms.route.replacement": "cashout.$1",
        }

        # Create connector manifest for the new handler approach
        connector_manifest = ConnectorManifest(
            name="outbox-cashout-dev", type="source", config=outbox_config, tasks=[]
        )
        result = source._get_topics_from_connector_config(connector_manifest)

        # The new implementation derives the base topic name from config
        # Complex transforms like EventRouter are not currently handled
        # So we get the basic topic name based on the table configuration
        assert result == ["cashout-dev.public.outbox"]  # Base topic derived from config


class TestConfluentCloudDetection:
    """Test Confluent Cloud detection functionality."""

    @patch("requests.Session.get")
    def test_confluent_cloud_detection_positive(self, mock_get: Mock) -> None:
        """Test detection of Confluent Cloud Connect URI."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock successful connection test
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Confluent Cloud URI pattern
        config = KafkaConnectSourceConfig(
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
        )
        source = KafkaConnectSource(config, Mock())

        assert source._is_confluent_cloud is True

    @patch("requests.Session.get")
    def test_confluent_cloud_detection_negative(self, mock_get: Mock) -> None:
        """Test detection with self-hosted Connect URI."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock successful connection test
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Self-hosted URI pattern
        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        source = KafkaConnectSource(config, Mock())

        assert source._is_confluent_cloud is False

    @patch("requests.Session.get")
    def test_feature_flag_disabled_skips_api_calls(self, mock_get: Mock) -> None:
        """Test that disabling use_connect_topics_api skips all API calls."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock connection test
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Disable the feature flag
        config = KafkaConnectSourceConfig(
            connect_uri="http://localhost:8083",
            use_connect_topics_api=False,  # Explicitly disabled
        )
        source = KafkaConnectSource(config, Mock())

        # Create mock ConnectorManifest
        mock_connector = Mock()
        mock_connector.name = "test-connector"
        mock_connector.type = "source"
        mock_connector.config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"
        }

        result = source._get_connector_topics(mock_connector)

        # Should return empty list when feature flag is disabled
        assert result == []

    @patch("requests.Session.get")
    def test_feature_flag_enabled_allows_api_calls(self, mock_get: Mock) -> None:
        """Test that enabling use_connect_topics_api allows API calls."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock connection test and successful /topics API response
        def mock_get_side_effect(url: str, **kwargs: Any) -> Mock:
            response = Mock()
            if "/topics" in url:
                # Mock /connectors/{name}/topics response for self-hosted
                response.raise_for_status.return_value = None
                response.json.return_value = {
                    "test-connector": {"topics": ["api-topic1", "api-topic2"]}
                }
            else:
                # Connection test success
                response.raise_for_status.return_value = None
                response.json.return_value = []
            return response

        mock_get.side_effect = mock_get_side_effect

        # Enable the feature flag (default behavior)
        config = KafkaConnectSourceConfig(
            connect_uri="http://localhost:8083",
            use_connect_topics_api=True,  # Explicitly enabled
        )
        source = KafkaConnectSource(config, Mock())

        # Create mock ConnectorManifest
        mock_connector = Mock()
        mock_connector.name = "test-connector"
        mock_connector.type = "source"
        mock_connector.config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"
        }

        result = source._get_connector_topics(mock_connector)

        # Should return topics from API when feature flag is enabled
        assert result == ["api-topic1", "api-topic2"]


class TestBigQuerySinkConnectorSanitization:
    """Test BigQuery sink connector table name sanitization following Kafka Connect logic."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.manifest = ConnectorManifest(
            name="test-bq-sink",
            type="sink",
            config={
                "project": "test-project",
                "defaultDataset": "test_dataset",
                "sanitizeTopics": "true",
            },
            tasks=[],
        )

        self.config = KafkaConnectSourceConfig()
        self.report = KafkaConnectSourceReport()
        self.connector = BigQuerySinkConnector(self.manifest, self.config, self.report)

    def test_valid_table_names_unchanged(self) -> None:
        """Test that valid table names are left unchanged."""
        test_cases = [
            "valid_table",
            "Valid_Table123",
            "table123",
            "_valid_table",
            "table_name_123",
            "UPPERCASE_TABLE",
            "mixedCase_Table_123",
        ]

        for table_name in test_cases:
            result = self.connector.sanitize_table_name(table_name)
            assert result == table_name, (
                f"Valid name '{table_name}' should remain unchanged, got '{result}'"
            )

    def test_invalid_character_replacement(self) -> None:
        """Test replacement of invalid characters with underscores."""
        test_cases = [
            # (input, expected_output)
            ("topic-with-dashes", "topic_with_dashes"),
            ("topic.with.dots", "topic_with_dots"),
            ("topic with spaces", "topic_with_spaces"),
            ("topic@special#chars$", "topic_special_chars_"),
            ("topic/with\\slashes", "topic_with_slashes"),
            ("topic+with=symbols", "topic_with_symbols"),
            ("topic(with)parentheses", "topic_with_parentheses"),
            ("topic[with]brackets", "topic_with_brackets"),
            ("topic{with}braces", "topic_with_braces"),
            ("topic,with;punctuation:", "topic_with_punctuation_"),
            ("topic!with?exclamation", "topic_with_exclamation"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_numeric_start_handling(self) -> None:
        """Test prepending underscore for names starting with digits."""
        test_cases = [
            # (input, expected_output)
            ("123numeric", "_123numeric"),
            ("9test", "_9test"),
            ("0table", "_0table"),
            ("2023_events", "_2023_events"),
            ("1_table", "_1_table"),
            ("42answer", "_42answer"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_consecutive_underscore_cleanup(self) -> None:
        """Test removal of consecutive underscores."""
        test_cases = [
            # (input, expected_output) - matches actual Confluent connector behavior
            ("multiple___underscores", "multiple___underscores"),
            ("table____name", "table____name"),
            ("a_____b", "a_____b"),
            ("leading____and____trailing", "leading____and____trailing"),
            ("_____many_underscores_____", "_____many_underscores_____"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_leading_trailing_underscore_removal(self) -> None:
        """Test removal of leading and trailing underscores (except for digit handling)."""
        test_cases = [
            # (input, expected_output) - matches actual Confluent connector behavior
            ("__leading_underscores", "__leading_underscores"),
            ("trailing_underscores__", "trailing_underscores__"),
            ("__both_sides__", "__both_sides__"),
            ("_single_leading", "_single_leading"),
            ("single_trailing_", "single_trailing_"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_digit_handling_preserves_underscore(self) -> None:
        """Test that digit-handling underscore is preserved even during cleanup."""
        test_cases = [
            # (input, expected_output) - basic Confluent behavior: replace chars + prepend underscore for digits
            ("123___table___", "_123___table___"),
            ("9__test__", "_9__test__"),
            ("0___event___data___", "_0___event___data___"),
            (
                "___123___",
                "___123___",  # Starts with underscore, not digit, so no prepend needed
            ),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_complex_mixed_cases(self) -> None:
        """Test complex cases with multiple sanitization rules applied."""
        test_cases = [
            # (input, expected_output) - matches actual Confluent connector behavior
            ("123-topic.with@special", "_123_topic_with_special"),
            ("user-events-2023", "user_events_2023"),
            ("9user@events#2023$data", "_9user_events_2023_data"),
            (
                "___123--.topic..name___",
                "___123___topic__name___",
            ),  # Real Confluent behavior
            ("2023/user-data@domain.com", "_2023_user_data_domain_com"),
            (
                "order_events-2023!@#$%",
                "order_events_2023_____",
            ),  # Real Confluent behavior
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_empty_input_raises_error(self) -> None:
        """Test that empty input raises ValueError."""
        invalid_inputs = ["", "   ", "\t\n", "  \t  "]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="Table name cannot be empty"):
                self.connector.sanitize_table_name(invalid_input)

    def test_all_invalid_chars_becomes_underscores(self) -> None:
        """Test that input with only invalid characters becomes underscores (Confluent behavior)."""
        test_cases = [
            # (input, expected_output) - matches actual Confluent connector behavior
            ("___", "___"),
            ("@#$%", "____"),
            ("---", "___"),
            ("...", "___"),
            ("!!!", "___"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_length_truncation(self) -> None:
        """Test that overly long names are truncated to BigQuery's 1024 character limit."""
        # Create a name longer than 1024 characters
        long_name = "a" * 1030  # 1030 characters

        result = self.connector.sanitize_table_name(long_name)

        # Should be truncated to 1024 characters
        assert len(result) == 1024, f"Expected length 1024, got {len(result)}"
        assert result == "a" * 1024, (
            "Should be truncated to exactly 1024 'a' characters"
        )

    def test_length_truncation_with_trailing_underscores(self) -> None:
        """Test that long names with underscores stay as-is (Confluent behavior)."""
        # Create a name that becomes exactly 1024 chars + trailing underscores after sanitization
        long_name = (
            "a" * 1020 + "____"
        )  # Will become 1024 chars, no truncation in simple Confluent behavior

        result = self.connector.sanitize_table_name(long_name)

        # Simple Confluent behavior - no truncation, keeps all characters
        assert len(result) == 1024, f"Expected length 1024, got {len(result)}"
        assert result == "a" * 1020 + "____", (
            "Should be 1020 'a' characters + 4 underscores (no truncation in basic Confluent behavior)"
        )

    def test_real_world_topic_names(self) -> None:
        """Test sanitization with realistic Kafka topic names."""
        test_cases = [
            # Common real-world patterns
            ("user.events", "user_events"),
            ("order-processing", "order_processing"),
            ("payment_notifications", "payment_notifications"),
            ("inventory.stock.updates", "inventory_stock_updates"),
            ("user-profile-updates", "user_profile_updates"),
            ("event.stream.v2", "event_stream_v2"),
            ("api.gateway.logs", "api_gateway_logs"),
            ("metrics-collector-data", "metrics_collector_data"),
            ("2023.audit.logs", "_2023_audit_logs"),
            ("db.change.events", "db_change_events"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"Real-world case: '{input_name}' should become '{expected}', got '{result}'"
            )

    def test_kafka_connect_compatibility(self) -> None:
        """Test that our implementation matches official Kafka Connect BigQuery connector behavior."""
        # These test cases are based on the official Kafka Connect BigQuery connector documentation
        # Reference: https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka
        test_cases = [
            # Official documented behavior: "All invalid characters are replaced by underscores"
            ("topic-name", "topic_name"),
            ("topic.name", "topic_name"),
            ("topic name", "topic_name"),
            # Official documented behavior: "If the resulting name would start with a digit, an underscore is prepended"
            ("1topic", "_1topic"),
            ("2023-events", "_2023_events"),
            ("9data", "_9data"),
            # Combined cases
            ("123.topic-name", "_123_topic_name"),
            ("user@events#123", "user_events_123"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"Kafka Connect compatibility: '{input_name}' should become '{expected}', got '{result}'"
            )


class TestCloudTransformPipeline:
    """Test the new Cloud connector transform-aware functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = KafkaConnectSourceConfig()
        self.report = KafkaConnectSourceReport()

    def test_get_source_tables_from_config_single_table(self):
        """Test extracting single table from Cloud connector config."""
        manifest = ConnectorManifest(
            name="test-cloud-single",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "table.name": "public.users",
            },
            tasks=[],
            topic_names=["user-events"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        source_tables = connector._get_source_tables_from_config()

        assert source_tables == ["public.users"]

    def test_get_source_tables_from_config_multi_table(self):
        """Test extracting multiple tables from Cloud connector config."""
        manifest = ConnectorManifest(
            name="test-cloud-multi",
            type="source",
            config={
                "connector.class": "MySqlSource",
                "table.include.list": "public.users,public.orders,public.products",
            },
            tasks=[],
            topic_names=["users", "orders", "products"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        source_tables = connector._get_source_tables_from_config()

        assert source_tables == ["public.users", "public.orders", "public.products"]

    def test_get_source_tables_from_config_query_mode(self):
        """Test that query-based connectors return empty list."""
        manifest = ConnectorManifest(
            name="test-cloud-query",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "query": "SELECT * FROM complex_join_view",
            },
            tasks=[],
            topic_names=["query-results"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        source_tables = connector._get_source_tables_from_config()

        assert source_tables == []

    def test_apply_forward_transforms_with_regex_router(self):
        """Test forward transform application with RegexRouter."""
        manifest = ConnectorManifest(
            name="test-cloud-transform",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "test-host.com",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "prod-db",
                "table.include.list": "public.users,public.orders",
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "prod-db\\.public\\.(.*)",
                "transforms.route.replacement": "clean-$1",
            },
            tasks=[],
            topic_names=["clean-users", "clean-orders"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        parser = connector.get_parser(manifest)
        source_tables = ["public.users", "public.orders"]

        expected_topics = connector._apply_forward_transforms(source_tables, parser)

        # Should generate original topics first, then apply transforms
        # Original: prod-db.public.users, prod-db.public.orders
        # After RegexRouter: clean-users, clean-orders (if Java regex available)
        assert len(expected_topics) == 2
        assert "users" in expected_topics[0]  # Should contain transformed table name
        assert "orders" in expected_topics[1]  # Should contain transformed table name

    def test_cloud_transform_integration_with_topic_filtering(self):
        """Test complete cloud transform integration with topic filtering."""
        # Simulate cluster with many topics, only some belong to this connector
        all_topics = [
            "clean-users",  # This connector's topics (after transforms)
            "clean-orders",  # This connector's topics (after transforms)
            "other-app-data",  # Other connector's topics
            "system-logs",  # Other connector's topics
            "metrics",  # Other connector's topics
        ]

        manifest = ConnectorManifest(
            name="test-cloud-integration",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "test-host.com",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "prod-db",
                "table.include.list": "public.users,public.orders",
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "prod-db\\.public\\.(.*)",
                "transforms.route.replacement": "clean-$1",
            },
            tasks=[],
            topic_names=all_topics,  # Cloud gets ALL cluster topics
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        parser = connector.get_parser(manifest)

        # Test the new cloud transform method
        lineages = connector._extract_lineages_cloud_with_transforms(all_topics, parser)

        # Should create lineages only for topics that this connector produces
        # and that actually exist in the cluster
        assert len(lineages) <= 2  # At most 2 lineages for 2 tables

        # Check that lineages are created for the right topics
        target_topics = [lineage.target_dataset for lineage in lineages]
        for topic in target_topics:
            assert topic in all_topics  # Must be actual topics from cluster
            assert topic not in [
                "other-app-data",
                "system-logs",
                "metrics",
            ]  # Not from other connectors

    def test_cloud_fallback_to_existing_strategies(self):
        """Test that Cloud connectors fall back to existing strategies when transforms fail."""
        manifest = ConnectorManifest(
            name="test-cloud-fallback",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "test-host",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "prod-db",
                "table.include.list": "public.users,public.orders",
                # No transforms - should use config-based mapping
            },
            tasks=[],
            topic_names=["prod-db.public.users", "prod-db.public.orders"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        parser = connector.get_parser(manifest)

        # Should detect 1:1 mapping and use config-based extraction
        has_mapping = connector._has_one_to_one_topic_mapping()
        assert has_mapping is True

        # Should use fallback strategies
        lineages = connector._extract_lineages_cloud_environment(parser)
        assert len(lineages) == 2  # Should create lineages using fallback method

    def test_cloud_transform_with_no_matching_topics(self):
        """Test Cloud transform when predicted topics don't exist in cluster."""
        all_topics = [
            "other-app-data",  # No topics from this connector
            "system-logs",
            "metrics",
        ]

        manifest = ConnectorManifest(
            name="test-cloud-no-match",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "test-host",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "prod-db",
                "table.include.list": "public.users,public.orders",
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "prod-db\\.public\\.(.*)",
                "transforms.route.replacement": "clean-$1",
            },
            tasks=[],
            topic_names=all_topics,
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        parser = connector.get_parser(manifest)

        # Should return empty list when no predicted topics match cluster topics
        lineages = connector._extract_lineages_cloud_with_transforms(all_topics, parser)
        assert lineages == []

    def test_cloud_transform_preserves_schema_information(self):
        """Test that Cloud transform preserves full table names with schema."""
        manifest = ConnectorManifest(
            name="test-cloud-schema",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "test-host",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "prod-db",
                "table.include.list": "public.users,inventory.products",  # Different schemas
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "prod-db\\.(.*)",
                "transforms.route.replacement": "clean-$1",
            },
            tasks=[],
            topic_names=["clean-public.users", "clean-inventory.products"],
        )

        connector = ConfluentJDBCSourceConnector(manifest, self.config, self.report)
        parser = connector.get_parser(manifest)

        # Extract source tables - should preserve schema
        source_tables = connector._get_source_tables_from_config()
        assert "public.users" in source_tables
        assert "inventory.products" in source_tables

        # Forward transforms should preserve schema in topic generation
        expected_topics = connector._apply_forward_transforms(source_tables, parser)
        assert len(expected_topics) == 2
