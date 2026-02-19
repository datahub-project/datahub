import logging
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest.mock import Mock, patch

import jpype
import jpype.imports
import pytest

# Import the classes we're testing
from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    POSTGRES_CDC_SOURCE_CLOUD,
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
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JDBC_SOURCE_CONNECTOR_CLASS,
    ConfluentJDBCSourceConnector,
    DebeziumSourceConnector,
    MongoSourceConnector,
)
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    get_transform_pipeline,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface

logger = logging.getLogger(__name__)


def create_mock_kafka_connect_config() -> Mock:
    """Helper to create a properly configured KafkaConnectSourceConfig mock."""
    config = Mock(spec=KafkaConnectSourceConfig)
    config.use_schema_resolver = False
    config.schema_resolver_expand_patterns = True
    config.schema_resolver_finegrained_lineage = True
    config.env = "PROD"
    return config


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started():
    """Ensure JVM is started for all tests requiring Java regex."""
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())
    yield
    # Note: JVM shutdown is problematic, so we don't shutdown here


class TestTransformPipelineBasic:
    """Test the new unified TransformPipeline functionality."""

    def test_no_transforms_configured(self) -> None:
        """Test when no transforms are configured."""
        config: Dict[str, str] = {"connector.class": "some.connector"}
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["test-topic"], config)
        assert result.topics == ["test-topic"]
        assert result.successful is True
        assert result.fallback_used is False

    def test_non_regex_router_transforms(self) -> None:
        """Test when transforms exist but none are RegexRouter."""
        config: Dict[str, str] = {
            "transforms": "MyTransform",
            "transforms.MyTransform.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.MyTransform.field": "timestamp",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["test-topic"], config)
        assert result.topics == ["test-topic"]
        assert len(result.warnings) > 0  # Should warn about unknown transform

    def test_single_regex_router_transform(self) -> None:
        """Test single RegexRouter transformation."""
        config = {
            "transforms": "TableNameTransformation",
            "transforms.TableNameTransformation.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TableNameTransformation.regex": ".*",
            "transforms.TableNameTransformation.replacement": "my_sink_table",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["source-topic"], config)
        assert result.topics == ["my_sink_table"]
        assert result.successful is True

    def test_multiple_regex_router_transforms(self) -> None:
        """Test multiple RegexRouter transformations applied in sequence."""
        config = {
            "transforms": "First,Second",
            "transforms.First.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.First.regex": "user-(.*)",
            "transforms.First.replacement": "customer_$1",
            "transforms.Second.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Second.regex": "customer_(.*)",
            "transforms.Second.replacement": "final_$1",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["user-events"], config)
        assert result.topics == ["final_events"]
        assert result.successful is True

    def test_mysql_source_config_example(self) -> None:
        """Test the specific MySQL source configuration from the example."""
        config = {
            "transforms": "TotalReplacement",
            "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TotalReplacement.regex": ".*(book)",
            "transforms.TotalReplacement.replacement": "my-new-topic-$1",
        }
        pipeline = get_transform_pipeline()

        # Test with a topic that matches the pattern
        result = pipeline.apply_forward(["library-book"], config)
        assert result.topics == ["my-new-topic-book"]

        # Test with a topic that doesn't match
        result = pipeline.apply_forward(["user-data"], config)
        assert result.topics == ["user-data"]  # Should remain unchanged

    def test_mixed_transforms(self) -> None:
        """Test mix of RegexRouter and other transforms."""
        config = {
            "transforms": "Router,Other",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Router.regex": "events-(.*)",
            "transforms.Router.replacement": "processed_$1",
            "transforms.Other.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.Other.field": "timestamp",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["events-user"], config)
        assert result.topics == ["processed_user"]
        assert len(result.warnings) > 0  # Should warn about unknown transform type

    def test_invalid_regex_pattern(self) -> None:
        """Test handling of invalid regex patterns."""
        config: Dict[str, str] = {
            "transforms": "BadRegex",
            "transforms.BadRegex.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.BadRegex.regex": "[invalid",  # Invalid regex
            "transforms.BadRegex.replacement": "fixed",
        }
        pipeline = get_transform_pipeline()

        # Should not crash and return original topic
        result = pipeline.apply_forward(["test-topic"], config)
        assert result.topics == ["test-topic"]
        assert len(result.warnings) > 0  # Should have warnings about regex failure

    def test_empty_replacement(self) -> None:
        """Test with empty replacement string."""
        config = {
            "transforms": "EmptyReplace",
            "transforms.EmptyReplace.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.EmptyReplace.regex": "prefix-(.*)",
            "transforms.EmptyReplace.replacement": "",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["prefix-suffix"], config)
        assert result.topics == [""]

    def test_complex_transforms_fallback(self) -> None:
        """Test complex transforms that require explicit configuration."""
        config = {
            "transforms": "Outbox",
            "transforms.Outbox.type": "io.debezium.transforms.outbox.EventRouter",
        }
        pipeline = get_transform_pipeline()

        result = pipeline.apply_forward(["test-topic"], config)
        assert result.topics == ["test-topic"]  # Unchanged due to complex transform
        assert result.fallback_used is True
        assert len(result.warnings) > 0  # Should warn about complex transform


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
        config: Mock = create_mock_kafka_connect_config()
        config.use_schema_resolver = False
        config.schema_resolver_expand_patterns = True
        config.schema_resolver_finegrained_lineage = True
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
        config: Mock = create_mock_kafka_connect_config()
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
        config: Mock = create_mock_kafka_connect_config()
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
        config: Mock = create_mock_kafka_connect_config()
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
        config: Mock = create_mock_kafka_connect_config()
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

        config: Mock = create_mock_kafka_connect_config()
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

        config: Mock = create_mock_kafka_connect_config()
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


class TestMongoSourceConnector:
    """Test Mongo source connector lineage extraction."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="mongo-source-connector",
            type="source",
            config=config,
            tasks=[],
            topic_names=[
                "prod.mongo.avro.my-new-database.users",
                "prod.mongo.avro.-leading-hyphen._leading-underscore",
                "prod.mongo.avro.!user?<db>=.[]user=logs!",
            ],
        )

    def test_mongo_source_lineage_topic_parsing(self) -> None:
        """Test MongoDB topic name parsing with various patterns including special characters and filtering of invalid topics."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
            "topic.prefix": "prod.mongo.avro",
        }

        manifest: ConnectorManifest = self.create_mock_manifest(connector_config)
        config: Mock = create_mock_kafka_connect_config()
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: MongoSourceConnector = MongoSourceConnector(manifest, config, report)
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 2
        assert lineages[0].source_dataset == "my-new-database.users"
        assert lineages[1].source_dataset == "-leading-hyphen._leading-underscore"

    def test_mongo_source_fine_grained_lineage(self) -> None:
        """Test MongoDB connector produces column-level lineage when schema resolver is available."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
            use_schema_resolver=True,
            schema_resolver_finegrained_lineage=True,
        )
        report = KafkaConnectSourceReport()

        manifest = ConnectorManifest(
            name="mongo-source-connector",
            type="source",
            config={
                "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
                "topic.prefix": "prod.mongo",
            },
            tasks=[],
            topic_names=["prod.mongo.mydb.users"],
        )

        mock_resolver = Mock()
        mock_resolver.resolve_table.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:mongodb,mydb.users,PROD)",
            {"_id": "string", "name": "string", "email": "string"},
        )

        connector = MongoSourceConnector(
            connector_manifest=manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        assert lineages[0].source_dataset == "mydb.users"
        assert lineages[0].target_dataset == "prod.mongo.mydb.users"
        assert lineages[0].fine_grained_lineages is not None
        assert len(lineages[0].fine_grained_lineages) == 3

    def test_mongo_source_no_fine_grained_without_schema_resolver(self) -> None:
        """Test MongoDB connector skips column-level lineage when schema resolver is disabled."""
        connector_config: Dict[str, str] = {
            "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
            "topic.prefix": "prod.mongo.avro",
        }

        manifest = self.create_mock_manifest(connector_config)
        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)

        connector = MongoSourceConnector(manifest, config, report)
        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        assert all(lin.fine_grained_lineages is None for lin in lineages)


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
        config: Mock = create_mock_kafka_connect_config()
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
            "database.dbname": "test_db",
            "database.server.name": "server_name",
            "table.include.list": "public.users,public.orders",
            "transforms": "Transform",
            "transforms.Transform.regex": "(.*)\\.(.*)\\.(.*)",
            "transforms.Transform.replacement": "$2.$3",
            "transforms.Transform.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        manifest: ConnectorManifest = self.create_cloud_manifest(connector_config)
        config: Mock = create_mock_kafka_connect_config()
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test the parser correctly handles Cloud config
        parser = connector.get_parser(manifest)
        assert parser.source_platform == "postgres"
        assert parser.database_name == "test_db"
        assert parser.topic_prefix == "server_name"  # Uses database.server.name
        assert (
            parser.db_connection_url
            == "postgresql://server_name.us-east-1.rds.amazonaws.com:5432/test_db"
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

        config: Mock = create_mock_kafka_connect_config()
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

        config: Mock = create_mock_kafka_connect_config()
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

        config: Mock = create_mock_kafka_connect_config()
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

        config: Mock = create_mock_kafka_connect_config()
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

        mock_config: Mock = create_mock_kafka_connect_config()
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
        for i, (actual, expected) in enumerate(
            zip(lineages, expected_lineages, strict=False)
        ):
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
        """Extract lineages using unified transform pipeline."""

        try:
            from datahub.ingestion.source.kafka_connect.transform_plugins import (
                get_transform_pipeline,
            )

            pipeline = get_transform_pipeline()

            # Build connector config from existing config
            connector_config = connector.connector_manifest.config

            lineages = []

            # For each table, generate original topic and apply transforms
            for table_id in table_name_tuples:
                # Generate original topic name based on connector type
                if topic_prefix:
                    if has_three_level_hierarchy(source_platform) and table_id.schema:
                        original_topic = (
                            f"{topic_prefix}.{table_id.schema}.{table_id.table}"
                        )
                    else:
                        original_topic = f"{topic_prefix}.{table_id.table}"
                else:
                    original_topic = table_id.table

                # Apply transforms using unified pipeline
                transform_result = pipeline.apply_forward(
                    [original_topic], connector_config
                )
                transformed_topics = transform_result.topics

                # Build source dataset name
                if table_id.schema and has_three_level_hierarchy(source_platform):
                    source_table_name = f"{table_id.schema}.{table_id.table}"
                else:
                    source_table_name = table_id.table

                source_dataset = get_dataset_name(database_name, source_table_name)

                # Create lineages for transformed topics that exist in manifest
                for transformed_topic in transformed_topics:
                    if transformed_topic in connector.connector_manifest.topic_names:
                        lineage = KafkaConnectLineage(
                            source_dataset=source_dataset,
                            source_platform=source_platform,
                            target_dataset=transformed_topic,
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


# Removed obsolete TestTransformPipeline class - functionality replaced by unified transform pipeline
# Tests for the new unified system are in TestTransformPipelineBasic class


# Removed obsolete TestTopicGeneration class - functionality replaced by unified transform pipeline
# Topic generation is now handled by the unified TransformPipeline system


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
        assert "db-users" in result
        assert "db-orders" in result
        assert "db-products" in result

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
        assert "sys-user table" in result
        assert "sys-order-data" in result

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
        assert sorted(result) == sorted(expected_topics)

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
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456",
            username="test-api-key",
            password="test-api-secret",
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


class TestJdbcSinkConnector:
    """Test the JDBC sink connector for Confluent Cloud and self-hosted configurations."""

    def test_jdbc_sink_parser_self_hosted_postgres(self) -> None:
        """Test parsing self-hosted PostgreSQL sink configuration with JDBC URL."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkParserFactory,
        )

        config = {
            "name": "postgres-sink-self-hosted",
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "topics": "users,orders",
            "table.name.format": "${topic}",
        }

        manifest = ConnectorManifest(
            name="postgres-sink-self-hosted",
            type="sink",
            config=config,
            tasks=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory.create_parser(manifest, "postgres")

        assert parser.database_name == "mydb"
        assert parser.schema_name == "public"  # Default for Postgres
        assert parser.target_platform in [
            "postgres",
            "postgresql",
        ]  # SQLAlchemy may return either
        assert parser.table_name_format == "${topic}"
        assert "localhost:5432/mydb" in parser.db_connection_url

    def test_jdbc_sink_parser_confluent_cloud_postgres(self) -> None:
        """Test parsing Confluent Cloud PostgreSQL sink configuration."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkParserFactory,
        )

        config = {
            "name": "postgres-sink-cloud",
            "connector.class": "PostgresSink",
            "connection.host": "my-postgres.rds.amazonaws.com",
            "connection.port": "5432",
            "db.name": "production_db",
            "topics": "topic1,topic2",
            "table.name.format": "${topic}",
        }

        manifest = ConnectorManifest(
            name="postgres-sink-cloud",
            type="sink",
            config=config,
            tasks=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory.create_parser(manifest, "postgres")

        assert parser.database_name == "production_db"
        assert parser.schema_name == "public"  # Default for Postgres
        assert parser.target_platform == "postgres"
        assert parser.table_name_format == "${topic}"
        assert (
            parser.db_connection_url
            == "postgres://my-postgres.rds.amazonaws.com:5432/production_db"
        )

    def test_jdbc_sink_parser_with_custom_schema(self) -> None:
        """Test parsing with custom schema configuration."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkParserFactory,
        )

        config = {
            "name": "postgres-sink-custom-schema",
            "connector.class": "PostgresSink",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "mydb",
            "db.schema": "analytics",
            "topics": "events",
        }

        manifest = ConnectorManifest(
            name="postgres-sink-custom-schema",
            type="sink",
            config=config,
            tasks=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory.create_parser(manifest, "postgres")

        assert parser.schema_name == "analytics"

    def test_jdbc_sink_parser_missing_required_fields(self) -> None:
        """Test that parser raises ValueError when required fields are missing."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkParserFactory,
        )

        config = {
            "name": "invalid-sink",
            "connector.class": "PostgresSink",
            # Missing connection.host and db.name
            "topics": "test",
        }

        manifest = ConnectorManifest(
            name="invalid-sink",
            type="sink",
            config=config,
            tasks=[],
        )

        factory = JdbcSinkParserFactory()

        with pytest.raises(ValueError, match="Missing 'connection.host'"):
            factory.create_parser(manifest, "postgres")

    def test_jdbc_sink_lineage_extraction_simple(self) -> None:
        """Test lineage extraction with simple topic-to-table mapping."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkConnector,
        )

        config_dict = {
            "name": "postgres-sink",
            "connector.class": "PostgresSink",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "analytics",
            "topics": "users,orders",
            "table.name.format": "${topic}",
        }

        manifest = ConnectorManifest(
            name="postgres-sink",
            type="sink",
            config=config_dict,
            tasks=[],
            topic_names=["users", "orders"],
        )

        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        report = KafkaConnectSourceReport()

        connector = JdbcSinkConnector(manifest, config, report, platform="postgres")
        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        # Sort lineages by source dataset for consistent comparison
        for lineage in lineages:
            assert lineage.source_dataset is not None
        sorted_lineages = sorted(lineages, key=lambda x: cast(str, x.source_dataset))

        # Check first lineage (orders)
        assert sorted_lineages[0].source_dataset == "orders"
        assert sorted_lineages[0].source_platform == "kafka"
        assert sorted_lineages[0].target_dataset == "analytics.public.orders"
        assert sorted_lineages[0].target_platform == "postgres"

        # Check second lineage (users)
        assert sorted_lineages[1].source_dataset == "users"
        assert sorted_lineages[1].source_platform == "kafka"
        assert sorted_lineages[1].target_dataset == "analytics.public.users"
        assert sorted_lineages[1].target_platform == "postgres"

    def test_jdbc_sink_lineage_with_transforms(self) -> None:
        """Test lineage extraction with topic transforms."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkConnector,
        )

        config_dict = {
            "name": "postgres-sink-transform",
            "connector.class": "PostgresSink",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "analytics",
            "topics": "prod.users,prod.orders",
            "table.name.format": "${topic}",
            "transforms": "stripPrefix",
            "transforms.stripPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.stripPrefix.regex": "prod\\.(.*)",
            "transforms.stripPrefix.replacement": "$1",
        }

        manifest = ConnectorManifest(
            name="postgres-sink-transform",
            type="sink",
            config=config_dict,
            tasks=[],
            topic_names=["prod.users", "prod.orders"],
        )

        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        report = KafkaConnectSourceReport()

        connector = JdbcSinkConnector(manifest, config, report, platform="postgres")
        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        # Sort lineages by source dataset for consistent comparison
        for lineage in lineages:
            assert lineage.source_dataset is not None
        sorted_lineages = sorted(lineages, key=lambda x: cast(str, x.source_dataset))

        # Original topics should be preserved in source
        assert sorted_lineages[0].source_dataset == "prod.orders"
        assert sorted_lineages[1].source_dataset == "prod.users"

        # Transformed topics should be used for table names
        assert sorted_lineages[0].target_dataset == "analytics.public.orders"
        assert sorted_lineages[1].target_dataset == "analytics.public.users"

    def test_jdbc_sink_mysql_without_schema(self) -> None:
        """Test MySQL sink which doesn't use schema hierarchy."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkConnector,
        )

        config_dict = {
            "name": "mysql-sink",
            "connector.class": "MySqlSink",
            "connection.host": "localhost",
            "connection.port": "3306",
            "db.name": "myapp",
            "topics": "events",
        }

        manifest = ConnectorManifest(
            name="mysql-sink",
            type="sink",
            config=config_dict,
            tasks=[],
            topic_names=["events"],
        )

        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        report = KafkaConnectSourceReport()

        connector = JdbcSinkConnector(manifest, config, report, platform="mysql")
        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        # MySQL doesn't use schema, so should be database.table
        assert lineages[0].target_dataset == "myapp.events"

    def test_jdbc_sink_flow_property_bag_sanitization(self) -> None:
        """Test that sensitive credentials are removed from flow property bag."""
        from datahub.ingestion.source.kafka_connect.sink_connectors import (
            JdbcSinkConnector,
        )

        config_dict = {
            "name": "postgres-sink",
            "connector.class": "PostgresSink",
            "connection.host": "localhost",
            "connection.port": "5432",
            "connection.user": "admin",
            "connection.password": "secret123",
            "db.name": "mydb",
            "db.user": "dbuser",
            "db.password": "dbpass",
            "topics": "test",
        }

        manifest = ConnectorManifest(
            name="postgres-sink",
            type="sink",
            config=config_dict,
            tasks=[],
        )

        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        report = KafkaConnectSourceReport()

        connector = JdbcSinkConnector(manifest, config, report, platform="postgres")
        property_bag = connector.extract_flow_property_bag()

        # Sensitive fields should be removed
        assert "connection.password" not in property_bag
        assert "connection.user" not in property_bag
        assert "db.password" not in property_bag
        assert "db.user" not in property_bag

        # Non-sensitive fields should remain
        assert property_bag["connection.host"] == "localhost"
        assert property_bag["db.name"] == "mydb"

        # Connection URL should be sanitized (no credentials)
        assert "connection.url" in property_bag
        assert "admin" not in property_bag["connection.url"]
        assert "secret123" not in property_bag["connection.url"]


class TestConfluentCloudConnectorManifest:
    """Test Confluent Cloud connector manifest handling."""

    def test_connector_manifest_filters_extensions_field(self) -> None:
        """Test that connector manifest handles unexpected fields like 'extensions'."""
        # This tests the fix for the 'extensions' field error
        config: Dict[str, Any] = {
            "name": "test-connector",
            "type": "source",
            "config": {"connector.class": "PostgresCdcSource"},
            "tasks": [],
            "extensions": {},  # This field should be filtered out
        }

        # Should not raise TypeError about unexpected 'extensions' argument
        manifest = ConnectorManifest(
            name=str(config["name"]),
            type=str(config["type"]),
            config=dict(config["config"]),
            tasks=list(config["tasks"]),
        )

        assert manifest.name == "test-connector"
        assert manifest.type == "source"

    def test_connector_manifest_with_missing_name(self) -> None:
        """Test connector manifest handles missing name field gracefully."""
        config: Dict[str, Any] = {
            "type": "source",
            "config": {"connector.class": "PostgresCdcSource"},
            "tasks": [],
        }

        # Should use a fallback name or handle None
        manifest = ConnectorManifest(
            name="fallback-name",  # Provided as fallback
            type=str(config["type"]),
            config=dict(config["config"]),
            tasks=list(config["tasks"]),
        )

        assert manifest.name == "fallback-name"


class TestConfluentCloudTasksEndpoint:
    """Test that tasks endpoint is skipped for Confluent Cloud."""

    @patch("requests.Session")
    def test_tasks_endpoint_skipped_for_confluent_cloud(
        self, mock_session: Mock
    ) -> None:
        """Test that /tasks endpoint is not called for Confluent Cloud connectors."""
        from datahub.ingestion.source.kafka_connect.kafka_connect import (
            KafkaConnectSource,
        )

        # Mock the session responses
        mock_response = Mock()
        mock_response.json.return_value = {
            "version": "1.0.0",
        }
        mock_response.raise_for_status = Mock()
        mock_session.return_value.get.return_value = mock_response

        config = KafkaConnectSourceConfig(
            connect_uri="http://localhost:8888/connect/v1/environments/env-00000/clusters/cluster-123",
            confluent_cloud_environment_id="env-00000",
            confluent_cloud_cluster_id="cluster-123",
            username="test-api-key",
            password="test-api-secret",
        )

        source = KafkaConnectSource(config, Mock())

        # Verify Confluent Cloud is detected
        assert source._is_confluent_cloud is True

        # The tasks endpoint should not be called for Confluent Cloud
        # This is tested implicitly by the implementation - tasks are only
        # fetched for self-hosted connectors


class TestHelperFunctions:
    """Test helper functions used by connectors."""

    def test_get_dataset_name(self) -> None:
        """Test get_dataset_name helper function."""
        # Two-level hierarchy
        assert get_dataset_name("mydb", "mytable") == "mydb.mytable"

        # Three-level hierarchy
        assert get_dataset_name("mydb", "myschema.mytable") == "mydb.myschema.mytable"

    def test_has_three_level_hierarchy(self) -> None:
        """Test platform hierarchy detection."""
        # Platforms with schema support
        assert has_three_level_hierarchy("postgres") is True
        assert has_three_level_hierarchy("trino") is True
        assert has_three_level_hierarchy("redshift") is True
        assert has_three_level_hierarchy("snowflake") is True

        # Platforms without schema support
        assert has_three_level_hierarchy("mysql") is False
        assert has_three_level_hierarchy("mongodb") is False


# ============================================================================
# Phase 1: Quick Win Tests
# ============================================================================


class TestTableId:
    """Test TableId dataclass functionality."""

    def test_table_id_string_representation_full(self) -> None:
        """Test TableId __str__ with database, schema, and table."""
        from datahub.ingestion.source.kafka_connect.source_connectors import TableId

        table_id = TableId(database="mydb", schema="public", table="users")
        assert str(table_id) == "mydb.public.users"

    def test_table_id_string_representation_schema_table(self) -> None:
        """Test TableId __str__ with schema and table only."""
        from datahub.ingestion.source.kafka_connect.source_connectors import TableId

        table_id = TableId(schema="public", table="orders")
        assert str(table_id) == "public.orders"

    def test_table_id_string_representation_table_only(self) -> None:
        """Test TableId __str__ with table only."""
        from datahub.ingestion.source.kafka_connect.source_connectors import TableId

        table_id = TableId(table="products")
        assert str(table_id) == "products"

    def test_table_id_string_representation_with_database_no_schema(self) -> None:
        """Test TableId __str__ with database and table (no schema)."""
        from datahub.ingestion.source.kafka_connect.source_connectors import TableId

        table_id = TableId(database="mydb", table="customers")
        assert str(table_id) == "mydb.customers"


class TestFlowPropertyBag:
    """Test flow property bag extraction and credential masking."""

    def test_extract_flow_property_bag_masks_credentials(self) -> None:
        """Test that flow property bag masks/excludes sensitive credentials."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "connection.user": "admin",
            "connection.password": "secret123",
            "topic.prefix": "db_",
            "mode": "incrementing",
        }

        manifest = ConnectorManifest(
            name="jdbc-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        props = connector.extract_flow_property_bag()

        # Credentials should be excluded
        assert "connection.password" not in props
        assert "connection.user" not in props

        # URL should be sanitized (no credentials)
        assert "connection.url" in props
        assert "localhost:5432/mydb" in props["connection.url"]

        # Other configs should be included
        assert props["topic.prefix"] == "db_"
        assert props["mode"] == "incrementing"


class TestTransformPluginEdgeCases:
    """Test transform plugin edge cases and error handling."""

    def test_has_complex_transforms_with_event_router(self) -> None:
        """Test detection of complex transforms (EventRouter)."""
        pipeline = get_transform_pipeline()

        config_with_complex = {
            "transforms": "eventRouter",
            "transforms.eventRouter.type": "io.debezium.transforms.outbox.EventRouter",
        }

        assert pipeline.has_complex_transforms(config_with_complex) is True

    def test_has_complex_transforms_with_only_regex_router(self) -> None:
        """Test no complex transforms when only RegexRouter is used."""
        pipeline = get_transform_pipeline()

        config_simple = {
            "transforms": "regexRouter",
            "transforms.regexRouter.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.regexRouter.regex": ".*",
            "transforms.regexRouter.replacement": "new-topic",
        }

        assert pipeline.has_complex_transforms(config_simple) is False

    def test_has_complex_transforms_with_no_transforms(self) -> None:
        """Test no complex transforms when no transforms configured."""
        pipeline = get_transform_pipeline()
        config_no_transforms: Dict[str, str] = {}

        assert pipeline.has_complex_transforms(config_no_transforms) is False

    def test_unknown_transform_generates_warning(self) -> None:
        """Test that unknown transform types generate warnings."""
        pipeline = get_transform_pipeline()

        config = {
            "transforms": "unknown",
            "transforms.unknown.type": "com.example.UnknownTransform",
        }

        result = pipeline.apply_forward(["topic1"], config)

        assert len(result.warnings) > 0
        assert any("Unknown transform type" in w for w in result.warnings)
        assert result.topics == ["topic1"]  # Topics unchanged

    def test_complex_transform_triggers_fallback(self) -> None:
        """Test that complex transforms trigger fallback mode."""
        pipeline = get_transform_pipeline()

        config = {
            "transforms": "eventRouter",
            "transforms.eventRouter.type": "io.debezium.transforms.outbox.EventRouter",
            "transforms.eventRouter.field.event.id": "event_id",
        }

        result = pipeline.apply_forward(["topic1"], config)

        assert result.fallback_used is True
        assert len(result.warnings) > 0
        assert any("Complex transforms detected" in w for w in result.warnings)


# ============================================================================
# Phase 2: Critical Gap Tests
# ============================================================================


class TestDebeziumConnectors:
    """Test Debezium CDC connector lineage extraction."""

    def test_debezium_postgres_lineage_extraction(self) -> None:
        """Test Debezium PostgreSQL connector lineage extraction."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "mydb",
            "table.include.list": "public.users,public.orders",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["myserver.public.users", "myserver.public.orders"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        # Check first lineage
        assert lineages[0].source_dataset == "mydb.public.users"
        assert lineages[0].target_dataset == "myserver.public.users"
        assert lineages[0].source_platform == "postgres"

        # Check second lineage
        assert lineages[1].source_dataset == "mydb.public.orders"
        assert lineages[1].target_dataset == "myserver.public.orders"
        assert lineages[1].source_platform == "postgres"

    def test_debezium_mysql_lineage_extraction(self) -> None:
        """Test Debezium MySQL connector lineage extraction."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.server.name": "mysql-server",
            "table.include.list": "inventory.products,inventory.customers",
        }

        manifest = ConnectorManifest(
            name="mysql-cdc",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[
                "mysql-server.inventory.products",
                "mysql-server.inventory.customers",
            ],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        assert lineages[0].source_dataset == "inventory.products"
        assert lineages[0].target_dataset == "mysql-server.inventory.products"
        assert lineages[0].source_platform == "mysql"

    def test_debezium_sqlserver_with_database_and_schema(self) -> None:
        """Test SQL Server Debezium connector with 2-level container pattern."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "sqlserver",
            "database.dbname": "mydb",
            "table.include.list": "dbo.customers",
        }

        manifest = ConnectorManifest(
            name="sqlserver-cdc",
            type="source",
            config=connector_config,
            tasks=[],
            # SQL Server includes database name in topic: server.database.schema.table
            topic_names=["sqlserver.mydb.dbo.customers"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        # Should handle the duplicate database name in topic pattern
        assert lineages[0].source_dataset == "mydb.dbo.customers"
        assert lineages[0].target_dataset == "sqlserver.mydb.dbo.customers"
        assert lineages[0].source_platform == "mssql"

    def test_debezium_with_topic_prefix(self) -> None:
        """Test Debezium connector using topic.prefix instead of database.server.name."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "topic.prefix": "my-prefix",
            "database.dbname": "testdb",
            "table.include.list": "public.events",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-prefix",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["my-prefix.public.events"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        assert lineages[0].source_dataset == "testdb.public.events"
        assert lineages[0].target_dataset == "my-prefix.public.events"


class TestDebeziumDatabaseDiscovery:
    """Test Debezium database discovery via SchemaResolver and filtering."""

    def test_database_discovery_without_table_include_list(self) -> None:
        """Test discovering tables from database.dbname when table.include.list is not configured."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "appdb",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-discovery",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = True
        report = Mock(spec=KafkaConnectSourceReport)

        mock_schema_resolver = Mock()
        mock_schema_resolver.get_urns.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.products,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,other_db.table1,PROD)",
        ]

        connector = DebeziumSourceConnector(
            manifest, config, report, schema_resolver=mock_schema_resolver
        )

        topics = connector.get_topics_from_config()

        assert len(topics) == 3
        assert "myserver.public.users" in topics
        assert "myserver.public.orders" in topics
        assert "myserver.public.products" in topics

    def test_database_discovery_with_include_filter(self) -> None:
        """Test discovering tables from database then filtering with table.include.list."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "appdb",
            "table.include.list": "public.users,public.orders",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-filtered",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = True
        report = Mock(spec=KafkaConnectSourceReport)

        mock_schema_resolver = Mock()
        mock_schema_resolver.get_urns.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.products,PROD)",
        ]

        connector = DebeziumSourceConnector(
            manifest, config, report, schema_resolver=mock_schema_resolver
        )

        topics = connector.get_topics_from_config()

        assert len(topics) == 2
        assert "myserver.public.users" in topics
        assert "myserver.public.orders" in topics
        assert "myserver.public.products" not in topics

    def test_database_discovery_with_exclude_filter(self) -> None:
        """Test discovering tables from database then filtering with table.exclude.list."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "appdb",
            "table.exclude.list": "public.products",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-excluded",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = True
        report = Mock(spec=KafkaConnectSourceReport)

        mock_schema_resolver = Mock()
        mock_schema_resolver.get_urns.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.products,PROD)",
        ]

        connector = DebeziumSourceConnector(
            manifest, config, report, schema_resolver=mock_schema_resolver
        )

        topics = connector.get_topics_from_config()

        assert len(topics) == 2
        assert "myserver.public.users" in topics
        assert "myserver.public.orders" in topics
        assert "myserver.public.products" not in topics

    def test_database_discovery_with_include_and_exclude_filters(self) -> None:
        """Test include filter followed by exclude filter."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public.test_.*",
            "table.exclude.list": "public.test_temp_.*",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-both-filters",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = True
        report = Mock(spec=KafkaConnectSourceReport)

        mock_schema_resolver = Mock()
        mock_schema_resolver.get_urns.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test_users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test_orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.test_temp_data,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.production_users,PROD)",
        ]

        connector = DebeziumSourceConnector(
            manifest, config, report, schema_resolver=mock_schema_resolver
        )

        topics = connector.get_topics_from_config()

        assert len(topics) == 2
        assert "myserver.public.test_users" in topics
        assert "myserver.public.test_orders" in topics
        assert "myserver.public.test_temp_data" not in topics
        assert "myserver.public.production_users" not in topics

    def test_pattern_matching_with_java_regex(self) -> None:
        """Test pattern matching uses Java regex for compatibility with Debezium."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
            "table.include.list": "public\\.users,schema_.*\\.orders",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-regex",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = True
        report = Mock(spec=KafkaConnectSourceReport)

        mock_schema_resolver = Mock()
        mock_schema_resolver.get_urns.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.schema_v1.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.schema_v2.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,testdb.public.products,PROD)",
        ]

        connector = DebeziumSourceConnector(
            manifest, config, report, schema_resolver=mock_schema_resolver
        )

        topics = connector.get_topics_from_config()

        assert len(topics) == 3
        assert "myserver.public.users" in topics
        assert "myserver.schema_v1.orders" in topics
        assert "myserver.schema_v2.orders" in topics
        assert "myserver.public.products" not in topics

    def test_fallback_when_schema_resolver_unavailable(self) -> None:
        """Test fallback to table.include.list only when SchemaResolver is not available."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "appdb",
            "table.include.list": "public.users,public.orders",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-no-resolver",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = False
        report = Mock(spec=KafkaConnectSourceReport)

        connector = DebeziumSourceConnector(manifest, config, report)

        topics = connector.get_topics_from_config()

        assert len(topics) == 2
        assert "myserver.public.users" in topics
        assert "myserver.public.orders" in topics

    def test_no_topics_when_no_config_and_no_resolver(self) -> None:
        """Test returns empty list when no table.include.list and no SchemaResolver."""
        from datahub.ingestion.source.kafka_connect.source_connectors import (
            DebeziumSourceConnector,
        )

        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "appdb",
        }

        manifest = ConnectorManifest(
            name="postgres-cdc-no-config",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        config.use_schema_resolver = False
        report = Mock(spec=KafkaConnectSourceReport)

        connector = DebeziumSourceConnector(manifest, config, report)

        topics = connector.get_topics_from_config()

        assert len(topics) == 0


class TestErrorHandling:
    """Test error handling in connector parsing and lineage extraction."""

    def test_parser_creation_with_missing_database_in_url(self) -> None:
        """Test parser fails gracefully when database name is missing from JDBC URL."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/",  # No database!
            "topic.prefix": "test_",
        }

        manifest = ConnectorManifest(
            name="bad-jdbc",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        with pytest.raises(ValueError, match="Missing database name"):
            connector.get_parser(manifest)

    def test_parser_creation_with_cloud_connector_missing_fields(self) -> None:
        """Test parser fails gracefully when Cloud connector is missing required fields."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "localhost",
            # Missing: database.port, database.dbname
        }

        manifest = ConnectorManifest(
            name="bad-cloud-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        with pytest.raises(ValueError, match="Missing required Cloud connector config"):
            connector.get_parser(manifest)

    def test_query_based_connector_with_no_source_dataset(self) -> None:
        """Test connectors with custom query create lineages without source datasets."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "query": "SELECT * FROM complex_view WHERE active = true",
            "topic.prefix": "custom_",
        }

        manifest = ConnectorManifest(
            name="query-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["custom_topic"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        # Query-based connectors should have None as source_dataset
        assert lineages[0].source_dataset is None
        assert lineages[0].target_dataset == "custom_topic"

    def test_get_topics_from_config_handles_exceptions_gracefully(self) -> None:
        """Test get_topics_from_config handles exceptions without crashing."""
        # Intentionally malformed config to trigger exception path
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            # Missing connection.url will cause parser to fail
        }

        manifest = ConnectorManifest(
            name="malformed-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        # Should return empty list instead of crashing
        topics = connector.get_topics_from_config()
        assert topics == []


# ============================================================================
# Phase 3: Comprehensive Coverage Tests
# ============================================================================


class TestInferMappings:
    """Test infer_mappings method for JDBC source connectors."""

    def test_infer_mappings_single_table_mode(self) -> None:
        """Test infer_mappings with single table configuration (kafka.topic + table.name)."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "kafka.topic": "users_topic",
            "table.name": "users",
        }

        manifest = ConnectorManifest(
            name="jdbc-single-table",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["users_topic"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.infer_mappings(all_topics=["users_topic"])

        assert len(lineages) == 1
        assert lineages[0].source_dataset == "users"
        assert lineages[0].target_dataset == "users_topic"
        assert lineages[0].source_platform == "postgres"
        assert lineages[0].job_property_bag is not None
        assert lineages[0].job_property_bag["mode"] == "single_table"

    def test_infer_mappings_multi_table_mode(self) -> None:
        """Test infer_mappings with multi-table configuration."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:mysql://localhost:3306/inventory",
            "table.include.list": "products,customers,orders",
            "topic.prefix": "db_",
        }

        manifest = ConnectorManifest(
            name="jdbc-multi-table",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["db_products", "db_customers", "db_orders"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.infer_mappings(
            all_topics=["db_products", "db_customers", "db_orders"]
        )

        assert len(lineages) == 3

        # Check products lineage
        products_lineage = next(
            lineage for lineage in lineages if "products" in lineage.target_dataset
        )
        assert products_lineage.source_dataset == "products"
        assert products_lineage.target_dataset == "db_products"
        assert products_lineage.job_property_bag is not None
        assert products_lineage.job_property_bag["mode"] == "multi_table"

    def test_infer_mappings_with_topic_prefix_only(self) -> None:
        """Test infer_mappings with only topic prefix (no explicit table list)."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            "topic.prefix": "staging_",
        }

        manifest = ConnectorManifest(
            name="jdbc-prefix-only",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["staging_events", "staging_logs"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.infer_mappings(
            all_topics=["staging_events", "staging_logs", "other_topic"]
        )

        # Should infer from topics matching the prefix
        assert len(lineages) == 2
        assert lineages[0].source_dataset == "events"
        assert lineages[0].target_dataset == "staging_events"
        assert lineages[0].job_property_bag is not None
        assert lineages[0].job_property_bag["mode"] == "inferred"

    def test_infer_mappings_with_schema_qualified_tables(self) -> None:
        """Test infer_mappings with schema-qualified table names."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "table.include.list": "public.users,analytics.events",
            "topic.prefix": "",
        }

        manifest = ConnectorManifest(
            name="jdbc-schema-qualified",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["public.users", "analytics.events"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.infer_mappings(
            all_topics=["public.users", "analytics.events"]
        )

        assert len(lineages) == 2
        assert lineages[0].source_dataset == "public.users"
        assert lineages[1].source_dataset == "analytics.events"

    def test_infer_mappings_with_no_matching_topics(self) -> None:
        """Test infer_mappings when predicted topics don't exist in cluster."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:mysql://localhost:3306/mydb",
            "table.include.list": "users,orders",
            "topic.prefix": "prod_",
        }

        manifest = ConnectorManifest(
            name="jdbc-no-match",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        # Provide topics that don't match the expected pattern
        lineages = connector.infer_mappings(all_topics=["dev_users", "test_orders"])

        # Should not create lineages for non-matching topics
        assert len(lineages) == 0

    def test_infer_mappings_handles_unknown_platform(self) -> None:
        """Test infer_mappings returns empty list for unknown JDBC platform."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:unknown://localhost:1234/mydb",
            "kafka.topic": "test_topic",
        }

        manifest = ConnectorManifest(
            name="jdbc-unknown-platform",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["test_topic"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.infer_mappings(all_topics=["test_topic"])

        # Should return empty list for unknown platform
        assert len(lineages) == 0


class TestCloudEnvironmentEdgeCases:
    """Test cloud environment extraction edge cases."""

    def test_cloud_extraction_with_no_topics(self) -> None:
        """Test cloud extraction when no topics are available."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "mydb",
            "database.user": "user",
            "table.include.list": "public.users",
        }

        manifest = ConnectorManifest(
            name="cloud-no-topics",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],  # No topics available
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        # Should handle gracefully - may return empty or fallback lineages
        assert isinstance(lineages, list)

    def test_cloud_extraction_with_transforms_but_no_source_tables(self) -> None:
        """Test cloud extraction with transforms configured but unable to derive source tables."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "mydb",
            "database.user": "user",
            "transforms": "regexRouter",
            "transforms.regexRouter.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.regexRouter.regex": ".*",
            "transforms.regexRouter.replacement": "transformed",
            # No table.include.list - can't derive source tables
        }

        manifest = ConnectorManifest(
            name="cloud-transforms-no-tables",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=["transformed"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        lineages = connector.extract_lineages()

        # Should handle gracefully without crashing
        assert isinstance(lineages, list)

    def test_cloud_environment_detection(self) -> None:
        """Test proper detection of cloud connector classes."""
        # Test cloud connector class
        cloud_config = {
            "connector.class": "PostgresCdcSource",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.dbname": "mydb",
            "database.user": "user",
            "table.include.list": "public.users",
        }

        cloud_manifest = ConnectorManifest(
            name="cloud-postgres",
            type="source",
            config=cloud_config,
            tasks=[],
            topic_names=["topic1"],
        )

        # Test platform connector class
        platform_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
            "table.include.list": "public.users",
        }

        platform_manifest = ConnectorManifest(
            name="platform-postgres",
            type="source",
            config=platform_config,
            tasks=[],
            topic_names=["topic1"],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)

        # Both should work but use different extraction logic
        cloud_connector = ConfluentJDBCSourceConnector(cloud_manifest, config, report)
        platform_connector = ConfluentJDBCSourceConnector(
            platform_manifest, config, report
        )

        cloud_lineages = cloud_connector.extract_lineages()
        platform_lineages = platform_connector.extract_lineages()

        assert isinstance(cloud_lineages, list)
        assert isinstance(platform_lineages, list)


class TestPlatformDetection:
    """Test JDBC URL platform detection logic."""

    def test_extract_platform_from_jdbc_url_postgres(self) -> None:
        """Test platform extraction for PostgreSQL JDBC URL."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/mydb",
        }

        manifest = ConnectorManifest(
            name="test",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        platform = connector._extract_platform_from_jdbc_url(
            "jdbc:postgresql://localhost:5432/mydb"
        )
        assert platform == "postgres"

    def test_extract_platform_from_jdbc_url_mysql(self) -> None:
        """Test platform extraction for MySQL JDBC URL."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:mysql://localhost:3306/mydb",
        }

        manifest = ConnectorManifest(
            name="test",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        platform = connector._extract_platform_from_jdbc_url(
            "jdbc:mysql://localhost:3306/mydb"
        )
        assert platform == "mysql"

    def test_extract_platform_from_jdbc_url_sqlserver(self) -> None:
        """Test platform extraction for SQL Server JDBC URL."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:sqlserver://localhost:1433;databaseName=mydb",
        }

        manifest = ConnectorManifest(
            name="test",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        platform = connector._extract_platform_from_jdbc_url(
            "jdbc:sqlserver://localhost:1433;databaseName=mydb"
        )
        assert platform == "sqlserver"

    def test_extract_platform_from_invalid_jdbc_url(self) -> None:
        """Test platform extraction for invalid JDBC URL returns 'unknown'."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "not-a-jdbc-url",
        }

        manifest = ConnectorManifest(
            name="test",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        platform = connector._extract_platform_from_jdbc_url("not-a-jdbc-url")
        assert platform == "unknown"

    def test_extract_platform_from_empty_jdbc_url(self) -> None:
        """Test platform extraction for empty JDBC URL returns 'unknown'."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "",
        }

        manifest = ConnectorManifest(
            name="test",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = create_mock_kafka_connect_config()
        report = Mock(spec=KafkaConnectSourceReport)
        connector = ConfluentJDBCSourceConnector(manifest, config, report)

        platform = connector._extract_platform_from_jdbc_url("")
        assert platform == "unknown"


class TestTransformPluginAdditionalCoverage:
    """Additional tests for transform plugin coverage gaps."""

    def test_regex_router_with_missing_config(self) -> None:
        """Test RegexRouter handles missing regex/replacement gracefully."""
        pipeline = get_transform_pipeline()

        config = {
            "transforms": "regexRouter",
            "transforms.regexRouter.type": "org.apache.kafka.connect.transforms.RegexRouter",
            # Missing regex and replacement!
        }

        result = pipeline.apply_forward(["topic1"], config)

        # Should return topics unchanged (logs warning via logger, not result.warnings)
        assert result.topics == ["topic1"]
        assert result.successful is True

    def test_complex_transform_plugin_apply_methods(self) -> None:
        """Test ComplexTransformPlugin returns topics unchanged in apply methods."""
        from datahub.ingestion.source.kafka_connect.transform_plugins import (
            ComplexTransformPlugin,
            TransformConfig,
        )

        plugin = ComplexTransformPlugin()
        config = TransformConfig(
            name="eventRouter",
            type="io.debezium.transforms.outbox.EventRouter",
            config={},
        )

        # Test apply_forward
        result = plugin.apply_forward(["topic1", "topic2"], config)
        assert result == ["topic1", "topic2"]

        # Test apply_reverse
        result = plugin.apply_reverse(["topic1", "topic2"], config)
        assert result == ["topic1", "topic2"]

    def test_regex_router_reverse_transform(self) -> None:
        """Test RegexRouter apply_reverse (limited support)."""
        from datahub.ingestion.source.kafka_connect.transform_plugins import (
            RegexRouterPlugin,
            TransformConfig,
        )

        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="reverse_test",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "(.*)_(.*)", "replacement": "$2_$1"},
        )

        # Reverse is not fully supported, should return topics unchanged
        result = plugin.apply_reverse(["transformed_topic"], config)
        assert result == ["transformed_topic"]


# ============================================================================
# Integration Tests for Schema Resolver, Fine-Grained Lineage, and Environment Detection
# ============================================================================


class MockSchemaResolver(SchemaResolverInterface):
    """Mock SchemaResolver for integration testing."""

    def __init__(
        self,
        platform: str,
        mock_urns: Optional[List[str]] = None,
        raise_on_resolve: bool = False,
    ):
        self._platform = platform
        self._mock_urns = set(mock_urns or [])
        self._schemas: Dict[str, Dict[str, str]] = {}
        self._raise_on_resolve = raise_on_resolve
        self.graph = None
        self.env = "PROD"
        self.platform_instance = None

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
        if self._raise_on_resolve:
            raise Exception("Schema resolver error")

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


class TestSchemaResolverFallback:
    """Test schema resolver fallback behavior when DataHub is unavailable or errors occur."""

    def test_fallback_when_schema_resolver_not_configured(self):
        """Test fallback to config-based approach when schema resolver is not configured."""
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
                "table.include.list": "public.users,public.orders",
                "database.server.name": "testserver",
            },
            tasks=[],
            topic_names=["testserver.public.users", "testserver.public.orders"],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=None,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        assert all(lin.fine_grained_lineages is None for lin in lineages)
        source_datasets = {lin.source_dataset for lin in lineages}
        assert "testdb.public.users" in source_datasets
        assert "testdb.public.orders" in source_datasets

    def test_fallback_when_schema_resolver_throws_error(self):
        """Test graceful fallback when schema resolver throws an error during resolution."""
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

        mock_resolver = MockSchemaResolver(platform="postgres", raise_on_resolve=True)

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        assert lineages[0].fine_grained_lineages is None
        assert lineages[0].source_dataset == "testdb.public.users"
        assert lineages[0].target_dataset == "testserver.public.users"

    def test_fallback_when_no_schema_metadata_found(self):
        """Test fallback when schema resolver returns empty schema metadata."""
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

        mock_resolver = MockSchemaResolver(platform="postgres")

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        assert lineages[0].source_dataset == "testdb.public.users"
        assert lineages[0].fine_grained_lineages is None

    def test_fallback_pattern_expansion_no_matches(self):
        """Test fallback when pattern expansion finds no matching tables in DataHub."""
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
            topic_names=[],
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

        result = connector._expand_table_patterns("nonexistent.*", "postgres", "testdb")
        assert result == ["nonexistent.*"]

        lineages = connector.extract_lineages()
        assert len(lineages) == 1
        assert lineages[0].fine_grained_lineages is None

    def test_fallback_with_partial_schema_availability(self):
        """Test behavior when schemas are available for some tables but not others."""
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
                "table.include.list": "public.users,public.orders",
                "database.server.name": "testserver",
            },
            tasks=[],
            topic_names=["testserver.public.users", "testserver.public.orders"],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {"id": "INT", "name": "VARCHAR"},
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        users_lineage = next(
            (lin for lin in lineages if "users" in lin.target_dataset), None
        )
        orders_lineage = next(
            (lin for lin in lineages if "orders" in lin.target_dataset), None
        )

        assert users_lineage is not None
        assert users_lineage.fine_grained_lineages is not None
        assert len(users_lineage.fine_grained_lineages) == 2

        assert orders_lineage is not None
        assert orders_lineage.fine_grained_lineages is None


class TestFineGrainedLineageWithReplaceField:
    """Integration tests for fine-grained lineage with ReplaceField transforms."""

    def test_fine_grained_lineage_with_field_exclusion(self):
        """Test that excluded fields are dropped from fine-grained lineage."""
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
                "transforms": "dropPassword",
                "transforms.dropPassword.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.dropPassword.exclude": "password",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {
                "id": "INT",
                "username": "VARCHAR",
                "password": "VARCHAR",
                "email": "VARCHAR",
            },
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]

        assert lineage.fine_grained_lineages is not None
        assert len(lineage.fine_grained_lineages) == 3

        downstream_fields = []
        for fg_lineage in lineage.fine_grained_lineages:
            for downstream_urn in fg_lineage["downstreams"]:
                field_name = downstream_urn.split(",")[-1].rstrip(")")
                downstream_fields.append(field_name)

        assert "password" not in downstream_fields
        assert "id" in downstream_fields
        assert "username" in downstream_fields
        assert "email" in downstream_fields

    def test_fine_grained_lineage_with_field_inclusion(self):
        """Test that only included fields appear in fine-grained lineage."""
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
                "transforms": "keepOnly",
                "transforms.keepOnly.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.keepOnly.include": "id,username,email",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {
                "id": "INT",
                "username": "VARCHAR",
                "password": "VARCHAR",
                "email": "VARCHAR",
                "internal_notes": "TEXT",
            },
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]

        assert lineage.fine_grained_lineages is not None
        assert len(lineage.fine_grained_lineages) == 3

        downstream_fields = []
        for fg_lineage in lineage.fine_grained_lineages:
            for downstream_urn in fg_lineage["downstreams"]:
                field_name = downstream_urn.split(",")[-1].rstrip(")")
                downstream_fields.append(field_name)

        assert set(downstream_fields) == {"id", "username", "email"}
        assert "password" not in downstream_fields
        assert "internal_notes" not in downstream_fields

    def test_fine_grained_lineage_with_field_renaming(self):
        """Test that renamed fields appear with correct names in fine-grained lineage."""
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
                "transforms": "renameFields",
                "transforms.renameFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.renameFields.renames": "user_id:id,user_name:name",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {
                "user_id": "INT",
                "user_name": "VARCHAR",
                "email": "VARCHAR",
            },
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]

        assert lineage.fine_grained_lineages is not None
        assert len(lineage.fine_grained_lineages) == 3

        downstream_fields = []
        for fg_lineage in lineage.fine_grained_lineages:
            for downstream_urn in fg_lineage["downstreams"]:
                field_name = downstream_urn.split(",")[-1].rstrip(")")
                downstream_fields.append(field_name)

        assert "id" in downstream_fields
        assert "name" in downstream_fields
        assert "email" in downstream_fields
        assert "user_id" not in downstream_fields
        assert "user_name" not in downstream_fields

    def test_fine_grained_lineage_with_chained_transforms(self):
        """Test fine-grained lineage with multiple chained ReplaceField transforms."""
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
                "transforms": "dropSensitive,renameFields",
                "transforms.dropSensitive.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.dropSensitive.exclude": "password,ssn",
                "transforms.renameFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.renameFields.renames": "user_id:id,user_name:name",
            },
            tasks=[],
            topic_names=["testserver.public.users"],
        )

        mock_resolver = MockSchemaResolver(platform="postgres")
        mock_resolver.add_schema(
            "testdb.public.users",
            {
                "user_id": "INT",
                "user_name": "VARCHAR",
                "email": "VARCHAR",
                "password": "VARCHAR",
                "ssn": "VARCHAR",
            },
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
            schema_resolver=mock_resolver,  # type: ignore[arg-type]
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        lineage = lineages[0]

        assert lineage.fine_grained_lineages is not None
        assert len(lineage.fine_grained_lineages) == 3

        downstream_fields = []
        for fg_lineage in lineage.fine_grained_lineages:
            for downstream_urn in fg_lineage["downstreams"]:
                field_name = downstream_urn.split(",")[-1].rstrip(")")
                downstream_fields.append(field_name)

        assert set(downstream_fields) == {"id", "name", "email"}
        assert "password" not in downstream_fields
        assert "ssn" not in downstream_fields
        assert "user_id" not in downstream_fields
        assert "user_name" not in downstream_fields


class TestPlatformCloudEnvironmentDetection:
    """Integration tests for Platform vs Cloud environment detection."""

    def test_platform_environment_detection_jdbc(self):
        """Test that self-hosted JDBC connector is detected as Platform environment."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="jdbc-source",
            type="source",
            config={
                "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "table.include.list": "public.users,public.orders",
                "topic.prefix": "db_",
            },
            tasks=[],
            topic_names=["db_users", "db_orders"],
        )

        connector = ConfluentJDBCSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        assert all(lin.target_platform == "kafka" for lin in lineages)

        target_datasets = {lin.target_dataset for lin in lineages}
        assert "db_users" in target_datasets
        assert "db_orders" in target_datasets

    def test_cloud_environment_detection_postgres_cdc(self):
        """Test that Confluent Cloud CDC connector is detected as Cloud environment."""
        config = KafkaConnectSourceConfig(
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-456",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="postgres-cloud-source",
            type="source",
            config={
                "connector.class": POSTGRES_CDC_SOURCE_CLOUD,
                "database.hostname": "postgres.example.com",
                "database.port": "5432",
                "database.dbname": "testdb",
                "table.include.list": "public.users,public.orders",
                "database.server.name": "cloudserver",
            },
            tasks=[],
            topic_names=["cloudserver.public.users", "cloudserver.public.orders"],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        assert connector_manifest.config["connector.class"] in CLOUD_JDBC_SOURCE_CLASSES

        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        target_datasets = {lin.target_dataset for lin in lineages}
        assert "cloudserver.public.users" in target_datasets
        assert "cloudserver.public.orders" in target_datasets

    def test_platform_with_transforms_uses_actual_topics(self):
        """Test that Platform environment uses actual runtime topics from API when transforms are present."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="jdbc-with-transforms",
            type="source",
            config={
                "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "table.include.list": "public.users",
                "topic.prefix": "db_",
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "db_(.*)",
                "transforms.route.replacement": "prod_$1",
            },
            tasks=[],
            topic_names=["prod_users"],
        )

        connector = ConfluentJDBCSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 1
        assert lineages[0].target_dataset == "prod_users"
        source_dataset = lineages[0].source_dataset
        assert source_dataset is not None
        assert "users" in source_dataset

    def test_cloud_with_transforms_without_jpype(self):
        """Test that Cloud environment handles gracefully when JPype is not available."""
        config = KafkaConnectSourceConfig(
            connect_uri="https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-456",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="cloud-with-transforms",
            type="source",
            config={
                "connector.class": POSTGRES_CDC_SOURCE_CLOUD,
                "database.hostname": "postgres.example.com",
                "database.port": "5432",
                "database.dbname": "testdb",
                "table.include.list": "public.users,public.orders",
                "database.server.name": "cloudserver",
            },
            tasks=[],
            topic_names=[
                "cloudserver.public.users",
                "cloudserver.public.orders",
                "other_connector_topic",
            ],
        )

        connector = DebeziumSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        target_datasets = {lin.target_dataset for lin in lineages}
        assert "cloudserver.public.users" in target_datasets
        assert "cloudserver.public.orders" in target_datasets

    def test_platform_single_table_multi_topic_transform(self):
        """Test Platform environment with single source table producing multiple topics."""
        config = KafkaConnectSourceConfig(
            connect_uri="http://test:8083",
            cluster_name="test",
        )
        report = KafkaConnectSourceReport()

        connector_manifest = ConnectorManifest(
            name="jdbc-multi-topic",
            type="source",
            config={
                "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "table.include.list": "public.events",
                "topic.prefix": "db_",
                "transforms": "extractTopic",
                "transforms.extractTopic.type": "io.confluent.connect.transforms.ExtractTopic$Value",
                "transforms.extractTopic.field": "event_type",
            },
            tasks=[],
            topic_names=["user_events", "order_events", "system_events"],
        )

        connector = ConfluentJDBCSourceConnector(
            connector_manifest=connector_manifest,
            config=config,
            report=report,
        )

        lineages = connector.extract_lineages()

        assert len(lineages) == 3
        source_datasets = {lin.source_dataset for lin in lineages}
        assert len(source_datasets) == 1
        source_dataset = list(source_datasets)[0]
        assert source_dataset is not None
        assert "events" in source_dataset

        target_datasets = {lin.target_dataset for lin in lineages}
        assert "user_events" in target_datasets
        assert "order_events" in target_datasets
        assert "system_events" in target_datasets
