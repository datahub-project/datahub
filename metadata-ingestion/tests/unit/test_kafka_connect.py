import logging
from typing import Dict, List, Tuple
from unittest.mock import Mock, patch

import jpype
import jpype.imports

# Import the classes we're testing
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
    RegexRouterTransform,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    ConfluentJDBCSourceConnector,
)

logger = logging.getLogger(__name__)

if not jpype.isJVMStarted():
    jpype.startJVM()


class TestRegexRouterTransform:
    """Test the RegexRouterTransform class."""

    def test_no_transforms_configured(self) -> None:
        """Test when no transforms are configured."""
        config: Dict[str, str] = {"connector.class": "some.connector"}
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("test-topic")
        assert result == "test-topic"

    def test_non_regex_router_transforms(self) -> None:
        """Test when transforms exist but none are RegexRouter."""
        config: Dict[str, str] = {
            "transforms": "MyTransform",
            "transforms.MyTransform.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.MyTransform.field": "timestamp",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("test-topic")
        assert result == "test-topic"

    def test_single_regex_router_transform(self) -> None:
        """Test single RegexRouter transformation."""
        config: Dict[str, str] = {
            "transforms": "TableNameTransformation",
            "transforms.TableNameTransformation.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TableNameTransformation.regex": ".*",
            "transforms.TableNameTransformation.replacement": "my_sink_table",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("source-topic")
        assert result == "my_sink_table"

    def test_multiple_regex_router_transforms(self) -> None:
        """Test multiple RegexRouter transformations applied in sequence."""
        config: Dict[str, str] = {
            "transforms": "First,Second",
            "transforms.First.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.First.regex": "user-(.*)",
            "transforms.First.replacement": "customer_$1",
            "transforms.Second.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Second.regex": "customer_(.*)",
            "transforms.Second.replacement": "final_$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("user-events")
        assert result == "final_events"

    def test_mysql_source_config_example(self) -> None:
        """Test the specific MySQL source configuration from the example."""
        config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "mode": "incrementing",
            "incrementing.column.name": "id",
            "tasks.max": "1",
            "connection.url": "${env:MYSQL_CONNECTION_URL}",
            "transforms": "TotalReplacement",
            "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TotalReplacement.regex": ".*(book)",
            "transforms.TotalReplacement.replacement": "my-new-topic-$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        # Test with a topic that matches the pattern
        result: str = transform.apply_transforms("library-book")
        assert result == "my-new-topic-book"

        # Test with a topic that doesn't match
        result = transform.apply_transforms("user-data")
        assert result == "user-data"  # Should remain unchanged

    def test_mixed_transforms(self) -> None:
        """Test mix of RegexRouter and other transforms."""
        config: Dict[str, str] = {
            "transforms": "NonRouter,Router,AnotherNonRouter",
            "transforms.NonRouter.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.NonRouter.field": "timestamp",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Router.regex": "events-(.*)",
            "transforms.Router.replacement": "processed_$1",
            "transforms.AnotherNonRouter.type": "org.apache.kafka.connect.transforms.MaskField",
            "transforms.AnotherNonRouter.fields": "sensitive",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("events-user")
        assert result == "processed_user"

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
        result: str = transform.apply_transforms("test-topic")
        assert result == "test-topic"

    def test_empty_replacement(self) -> None:
        """Test with empty replacement string."""
        config: Dict[str, str] = {
            "transforms": "EmptyReplace",
            "transforms.EmptyReplace.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.EmptyReplace.regex": "prefix-(.*)",
            "transforms.EmptyReplace.replacement": "",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("prefix-suffix")
        assert result == ""

    def test_whitespace_in_transform_names(self) -> None:
        """Test handling of whitespace in transform names."""
        config: Dict[str, str] = {
            "transforms": " Transform1 , Transform2 ",
            "transforms.Transform1.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Transform1.regex": "test-(.*)",
            "transforms.Transform1.replacement": "result_$1",
        }
        transform: RegexRouterTransform = RegexRouterTransform(config)

        result: str = transform.apply_transforms("test-data")
        assert result == "result_data"


class TestBigQuerySinkConnector:
    """Test BigQuery sink connector with RegexRouter support."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="test-bigquery-connector",
            type="sink",
            config=config,
            tasks={},
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
            tasks={},
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
            tasks={},
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
            tasks={},
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
            tasks={"config": {"tables": "library.book"}},
            topic_names=["library-book"],
        )

    @patch("datahub.ingestion.source.kafka_connect.source_connectors.make_url")
    @patch(
        "datahub.ingestion.source.kafka_connect.source_connectors.get_platform_from_sqlalchemy_uri"
    )
    def test_mysql_source_with_regex_router(
        self, mock_platform: Mock, mock_url: Mock
    ) -> None:
        """Test the specific MySQL source configuration example."""
        # Mock the database connection parsing
        mock_url_obj: Mock = Mock()
        mock_url_obj.drivername = "mysql+pymysql"
        mock_url_obj.host = "localhost"
        mock_url_obj.port = 3306
        mock_url_obj.database = "library"
        mock_url.return_value = mock_url_obj
        mock_platform.return_value = "mysql"

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
            tasks={},
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
            tasks={},
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
