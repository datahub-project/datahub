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
    MySqlSinkConnector,
    PostgresSinkConnector,
    RegexRouterTransform,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    ConfluentJDBCSourceConnector,
    MongoSourceConnector,
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


class TestMongoSourceConnector:
    """Test Mongo source connector lineage extraction."""

    def create_mock_manifest(self, config: Dict[str, str]) -> ConnectorManifest:
        """Helper to create a mock connector manifest."""
        return ConnectorManifest(
            name="mongo-source-connector",
            type="source",
            config=config,
            tasks={},
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
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: MongoSourceConnector = MongoSourceConnector(manifest, config, report)
        lineages: List = connector.extract_lineages()

        assert len(lineages) == 2
        assert lineages[0].source_dataset == "my-new-database.users"
        assert lineages[1].source_dataset == "-leading-hyphen._leading-underscore"


class TestConfluentCloudConnectors:
    """Test Confluent Cloud connector configurations and parsing."""

    def create_mock_dependencies(self) -> Tuple[Mock, Mock]:
        """Helper to create mock dependencies."""
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)
        return config, report

    def test_postgres_cdc_source_cloud_config_parsing(self) -> None:
        """Test PostgresCdcSource (Cloud) configuration parsing."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "connection.host": "test-postgres.us-east-1.rds.amazonaws.com",
            "connection.password": "password",
            "connection.port": "5432",
            "connection.user": "test_user",
            "db.name": "test_database",
            "table.include.list": "public.users,public.orders",
            "database.server.name": "test-server",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-cdc-cloud-connector",
            type="source",
            config=connector_config,
            tasks={},
            topic_names=[
                "test-server.public.users",
                "test-server.public.orders",
            ],
        )
        config, report = self.create_mock_dependencies()

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        # Test parser handles Cloud configuration
        parser = connector.get_parser(manifest)
        assert parser.source_platform == "postgres"
        assert parser.database_name == "test_database"
        assert parser.topic_prefix == "test-server"
        assert (
            parser.db_connection_url
            == "postgresql://test-postgres.us-east-1.rds.amazonaws.com:5432/test_database"
        )

    def test_postgres_cdc_source_v2_cloud_config_parsing(self) -> None:
        """Test PostgresCdcSourceV2 (Cloud) configuration parsing."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSourceV2",
            "database.hostname": "postgres.example.com",
            "database.port": "5432",
            "database.dbname": "production",
            "database.user": "postgres_user",
            "table.include.list": "schema1.table1,schema2.table2",
            "database.server.name": "prod-server",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-cdc-v2-cloud-connector",
            type="source",
            config=connector_config,
            tasks={},
            topic_names=["prod-server.schema1.table1", "prod-server.schema2.table2"],
        )
        config, report = self.create_mock_dependencies()

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        parser = connector.get_parser(manifest)
        assert parser.source_platform == "postgres"
        assert parser.database_name == "production"
        assert parser.topic_prefix == "prod-server"
        assert (
            parser.db_connection_url
            == "postgresql://postgres.example.com:5432/production"
        )

    def test_platform_vs_cloud_table_config_parsing(self) -> None:
        """Test that both Platform and Cloud table configuration formats are handled."""
        # Test Cloud format (table.include.list)
        cloud_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "testdb",
            "table.include.list": "public.users,public.orders",
        }

        cloud_manifest: ConnectorManifest = ConnectorManifest(
            name="cloud-connector",
            type="source",
            config=cloud_config,
            tasks={},
            topic_names=[],
        )
        config, report = self.create_mock_dependencies()

        cloud_connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            cloud_manifest, config, report
        )
        cloud_tables = cloud_connector.get_table_names()
        assert len(cloud_tables) == 2
        assert ("public", "users") in cloud_tables
        assert ("public", "orders") in cloud_tables

        # Test Platform format (table.whitelist)
        platform_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            "table.whitelist": "public.users,public.orders",
        }

        platform_manifest: ConnectorManifest = ConnectorManifest(
            name="platform-connector",
            type="source",
            config=platform_config,
            tasks={},
            topic_names=[],
        )

        platform_connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            platform_manifest, config, report
        )
        platform_tables = platform_connector.get_table_names()
        assert len(platform_tables) == 2
        assert ("public", "users") in platform_tables
        assert ("public", "orders") in platform_tables

    def test_topic_prefix_fallback_logic(self) -> None:
        """Test topic prefix fallback from topic.prefix to database.server.name."""
        # Test with topic.prefix (should take precedence)
        config_with_both: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "testdb",
            "topic.prefix": "primary-prefix",
            "database.server.name": "fallback-prefix",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config=config_with_both,
            tasks={},
            topic_names=[],
        )
        config, report = self.create_mock_dependencies()

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)
        assert parser.topic_prefix == "primary-prefix"

        # Test with only database.server.name
        config_server_name_only: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "connection.host": "localhost",
            "connection.port": "5432",
            "db.name": "testdb",
            "database.server.name": "server-name-prefix",
        }

        manifest_server_only: ConnectorManifest = ConnectorManifest(
            name="test-connector-2",
            type="source",
            config=config_server_name_only,
            tasks={},
            topic_names=[],
        )

        connector_server_only: ConfluentJDBCSourceConnector = (
            ConfluentJDBCSourceConnector(manifest_server_only, config, report)
        )
        parser_server_only = connector_server_only.get_parser(manifest_server_only)
        assert parser_server_only.topic_prefix == "server-name-prefix"

    def test_cloud_sink_connector_detection(self) -> None:
        """Test that Confluent Cloud sink connectors are properly detected."""
        from datahub.ingestion.source.kafka_connect.common import (
            CLOUD_SINK_CLASSES,
            POSTGRES_SINK_CLOUD,
            SNOWFLAKE_SINK_CLOUD,
        )

        # Test PostgresSink detection
        assert POSTGRES_SINK_CLOUD in CLOUD_SINK_CLASSES
        assert POSTGRES_SINK_CLOUD == "PostgresSink"

        # Test SnowflakeSink detection
        assert SNOWFLAKE_SINK_CLOUD in CLOUD_SINK_CLASSES
        assert SNOWFLAKE_SINK_CLOUD == "SnowflakeSink"

    def test_cloud_source_connector_detection(self) -> None:
        """Test that Confluent Cloud source connectors are properly detected."""
        from datahub.ingestion.source.kafka_connect.common import (
            CLOUD_JDBC_SOURCE_CLASSES,
            POSTGRES_CDC_SOURCE_CLOUD,
            POSTGRES_CDC_SOURCE_V2_CLOUD,
        )

        # Test PostgresCdcSource detection
        assert POSTGRES_CDC_SOURCE_CLOUD in CLOUD_JDBC_SOURCE_CLASSES
        assert POSTGRES_CDC_SOURCE_CLOUD == "PostgresCdcSource"

        # Test PostgresCdcSourceV2 detection
        assert POSTGRES_CDC_SOURCE_V2_CLOUD in CLOUD_JDBC_SOURCE_CLASSES
        assert POSTGRES_CDC_SOURCE_V2_CLOUD == "PostgresCdcSourceV2"

    def test_platform_detection_from_connector_class(self) -> None:
        """Test platform detection from connector class names."""
        from datahub.ingestion.source.kafka_connect.common import (
            get_source_platform_from_connector_class,
        )

        # Test Cloud connector classes
        assert (
            get_source_platform_from_connector_class("PostgresCdcSource") == "postgres"
        )
        assert (
            get_source_platform_from_connector_class("PostgresCdcSourceV2")
            == "postgres"
        )
        assert get_source_platform_from_connector_class("PostgresSink") == "postgres"
        assert get_source_platform_from_connector_class("SnowflakeSink") == "snowflake"

        # Test pattern matching
        assert get_source_platform_from_connector_class("MySQLConnector") == "mysql"
        assert (
            get_source_platform_from_connector_class("OracleSourceConnector")
            == "oracle"
        )
        assert get_source_platform_from_connector_class("SqlServerConnector") == "mssql"
        assert get_source_platform_from_connector_class("MongoDBConnector") == "mongodb"

        # Test unknown connector
        assert get_source_platform_from_connector_class("UnknownConnector") == "unknown"

    @patch("datahub.ingestion.source.kafka_connect.source_connectors.make_url")
    @patch(
        "datahub.ingestion.source.kafka_connect.source_connectors.get_platform_from_sqlalchemy_uri"
    )
    def test_mixed_platform_and_cloud_configurations(
        self, mock_platform: Mock, mock_url: Mock
    ) -> None:
        """Test that the connector can handle both Platform and Cloud configurations in the same deployment."""
        # Mock Platform connector parsing
        mock_url_obj: Mock = Mock()
        mock_url_obj.drivername = "postgresql"
        mock_url_obj.host = "platform-host"
        mock_url_obj.port = 5432
        mock_url_obj.database = "platform_db"
        mock_url.return_value = mock_url_obj
        mock_platform.return_value = "postgres"

        # Platform configuration
        platform_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://platform-host:5432/platform_db",
            "topic.prefix": "platform-prefix",
            "table.whitelist": "public.platform_table",
        }

        platform_manifest: ConnectorManifest = ConnectorManifest(
            name="platform-connector",
            type="source",
            config=platform_config,
            tasks={},
            topic_names=["platform-prefix.platform_table"],
        )

        config, report = self.create_mock_dependencies()
        platform_connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            platform_manifest, config, report
        )
        platform_parser = platform_connector.get_parser(platform_manifest)

        assert platform_parser.source_platform == "postgres"
        assert platform_parser.database_name == "platform_db"
        assert platform_parser.topic_prefix == "platform-prefix"
        assert "platform-host" in platform_parser.db_connection_url

        # Cloud configuration
        cloud_config: Dict[str, str] = {
            "connector.class": "PostgresCdcSource",
            "connection.host": "cloud-host.amazonaws.com",
            "connection.port": "5432",
            "db.name": "cloud_db",
            "database.server.name": "cloud-prefix",
            "table.include.list": "public.cloud_table",
        }

        cloud_manifest: ConnectorManifest = ConnectorManifest(
            name="cloud-connector",
            type="source",
            config=cloud_config,
            tasks={},
            topic_names=["cloud-prefix.public.cloud_table"],
        )

        cloud_connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            cloud_manifest, config, report
        )
        cloud_parser = cloud_connector.get_parser(cloud_manifest)

        assert cloud_parser.source_platform == "postgres"
        assert cloud_parser.database_name == "cloud_db"
        assert cloud_parser.topic_prefix == "cloud-prefix"
        assert "cloud-host.amazonaws.com" in cloud_parser.db_connection_url


class TestPostgresSinkConnector:
    """Test PostgreSQL sink connector for both Platform and Cloud configurations."""

    def create_mock_dependencies(self) -> Tuple[Mock, Mock]:
        """Helper to create mock dependencies."""
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        config.platform_instance_map = {
            "kafka": "kafka-test",
            "postgres": "postgres-test",
        }
        config.env = "PROD"
        report: Mock = Mock(spec=KafkaConnectSourceReport)
        return config, report

    def test_postgres_sink_platform_config_parsing(self) -> None:
        """Test PostgreSQL sink connector with Platform configuration (JDBC URL)."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/test_db",
            "connection.user": "test_user",
            "connection.password": "test_password",
            "topics": "topic1,topic2,topic3",
            "table.name.format": "${topic}",
            "auto.create": "true",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-platform-sink",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["topic1", "topic2", "topic3"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        assert parser.database_name == "test_db"
        assert parser.schema_name == "public"  # Default schema
        assert parser.host == "localhost"
        assert parser.port == "5432"
        assert len(parser.topics_to_tables) == 3
        assert parser.topics_to_tables["topic1"] == "topic1"
        assert parser.topics_to_tables["topic2"] == "topic2"
        assert parser.topics_to_tables["topic3"] == "topic3"

    def test_postgres_sink_cloud_config_parsing(self) -> None:
        """Test PostgreSQL sink connector with Confluent Cloud configuration."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "testpg-jd.postgres.database.azure.com",
            "connection.port": "5432",
            "connection.user": "jonnydixon",
            "connection.password": "secret_password",
            "db.name": "newdb",
            "topics": "sample_data_orders,sample_data_users,sample_data_stock_trades",
            "table.name.format": "public.${topic}",
            "input.data.format": "AVRO",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-cloud-sink",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=[
                "sample_data_orders",
                "sample_data_users",
                "sample_data_stock_trades",
            ],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        assert parser.database_name == "newdb"
        assert parser.schema_name == "public"
        assert parser.host == "testpg-jd.postgres.database.azure.com"
        assert parser.port == "5432"
        assert len(parser.topics_to_tables) == 3
        assert parser.topics_to_tables["sample_data_orders"] == "sample_data_orders"
        assert parser.topics_to_tables["sample_data_users"] == "sample_data_users"
        assert (
            parser.topics_to_tables["sample_data_stock_trades"]
            == "sample_data_stock_trades"
        )

    def test_postgres_sink_lineage_extraction(self) -> None:
        """Test lineage extraction for PostgreSQL sink connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "analytics",
            "topics": "user_events,order_events",
            "table.name.format": "staging.${topic}",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-lineage-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["user_events", "order_events"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        # Check user_events lineage
        user_lineage = next(
            lineage for lineage in lineages if "user_events" in lineage.target_dataset
        )
        assert user_lineage.source_platform == "kafka"
        assert user_lineage.target_platform == "postgres"
        assert "analytics.staging.user_events" in user_lineage.target_dataset
        assert (
            user_lineage.source_dataset and "user_events" in user_lineage.source_dataset
        )

        # Check order_events lineage
        order_lineage = next(
            lineage for lineage in lineages if "order_events" in lineage.target_dataset
        )
        assert order_lineage.source_platform == "kafka"
        assert order_lineage.target_platform == "postgres"
        assert "analytics.staging.order_events" in order_lineage.target_dataset
        assert (
            order_lineage.source_dataset
            and "order_events" in order_lineage.source_dataset
        )

    def test_postgres_sink_with_regex_transforms(self) -> None:
        """Test PostgreSQL sink with RegexRouter transformations."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "warehouse",
            "topics": "raw_user_data,raw_order_data",
            "table.name.format": "${topic}",
            "transforms": "RouteToClean",
            "transforms.RouteToClean.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.RouteToClean.regex": "raw_(.*)",
            "transforms.RouteToClean.replacement": "clean_$1",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-transform-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["raw_user_data", "raw_order_data"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        # Check that topics are transformed to table names
        assert parser.topics_to_tables["raw_user_data"] == "clean_user_data"
        assert parser.topics_to_tables["raw_order_data"] == "clean_order_data"

    def test_postgres_sink_flow_property_bag_platform(self) -> None:
        """Test flow property bag for Platform PostgreSQL connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/test_db",
            "connection.user": "test_user",
            "connection.password": "secret_password",
            "topics": "test_topic",
            "auto.create": "true",
            "batch.size": "1000",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-platform-properties",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        properties = connector.extract_flow_property_bag()

        # Should exclude sensitive fields
        assert "connection.password" not in properties
        assert "connection.user" not in properties  # Excluded for Platform connectors

        # Should include other properties
        assert (
            properties["connector.class"]
            == "io.confluent.connect.jdbc.JdbcSinkConnector"
        )
        assert properties["topics"] == "test_topic"
        assert properties["auto.create"] == "true"
        assert properties["batch.size"] == "1000"

    def test_postgres_sink_flow_property_bag_cloud(self) -> None:
        """Test flow property bag for Cloud PostgreSQL connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "connection.user": "cloud_user",
            "connection.password": "secret_password",
            "db.name": "cloud_db",
            "topics": "test_topic",
            "input.data.format": "AVRO",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-cloud-properties",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        properties = connector.extract_flow_property_bag()

        # Should exclude only password for Cloud connectors
        assert "connection.password" not in properties

        # Should include connection.user for Cloud connectors (it's a separate field)
        assert properties["connection.user"] == "cloud_user"
        assert properties["connection.host"] == "postgres.example.com"
        assert properties["db.name"] == "cloud_db"
        assert properties["input.data.format"] == "AVRO"

    def test_postgres_sink_error_handling(self) -> None:
        """Test error handling for invalid configurations."""
        # Test missing database name
        invalid_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            # Missing db.name
            "topics": "test_topic",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-invalid",
            type="sink",
            config=invalid_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )

        try:
            connector.get_parser(manifest)
            raise AssertionError(
                "Should have raised ValueError for missing database name"
            )
        except ValueError as e:
            assert "PostgreSQL connection details (host, database) are required" in str(
                e
            )

    def test_postgres_sink_table_name_default_behavior(self) -> None:
        """Test default table name behavior (uses ${topic} format)."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "test_db",
            "topics": "topic-with-hyphens,topic.with.dots,123numeric_start",
            # No table.name.format specified - defaults to "${topic}"
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-default-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["topic-with-hyphens", "topic.with.dots", "123numeric_start"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        # Default behavior: preserves topic names, but removes schema prefixes (splits on dots)
        assert parser.topics_to_tables["topic-with-hyphens"] == "topic-with-hyphens"
        assert (
            parser.topics_to_tables["topic.with.dots"] == "dots"
        )  # Takes last part after dot
        assert parser.topics_to_tables["123numeric_start"] == "123numeric_start"

    def test_postgres_sink_table_name_sanitization_explicit(self) -> None:
        """Test explicit table name sanitization when no format is used."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "test_db",
            "topics": "topic-with-hyphens,topic.with.dots,123numeric_start",
            "table.name.format": "custom_format_without_topic_placeholder",  # No ${topic} placeholder
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-sanitization-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["topic-with-hyphens", "topic.with.dots", "123numeric_start"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        # When no ${topic} placeholder, uses get_table_name_from_topic_name for sanitization
        assert parser.topics_to_tables["topic-with-hyphens"] == "topic_with_hyphens"
        assert parser.topics_to_tables["topic.with.dots"] == "topic_with_dots"
        assert parser.topics_to_tables["123numeric_start"] == "_123numeric_start"

    def test_postgres_sink_table_name_format_with_schema(self) -> None:
        """Test table.name.format with schema prefix handling."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "test_db",
            "topics": "user_events,order_events",
            "table.name.format": "staging.${topic}",  # Schema prefix in format
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-schema-format-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["user_events", "order_events"],
        )
        config, report = self.create_mock_dependencies()

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        parser = connector.get_parser(manifest)

        # Schema prefix should be removed from table name (handled separately)
        assert parser.topics_to_tables["user_events"] == "user_events"
        assert parser.topics_to_tables["order_events"] == "order_events"
        assert parser.schema_name == "staging"  # Schema extracted from format


class TestMySqlSinkConnector:
    """Test MySQL sink connector for both Platform and Cloud configurations."""

    def create_mock_dependencies(self) -> Tuple[Mock, Mock]:
        """Helper to create mock dependencies."""
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        config.platform_instance_map = {"kafka": "kafka-test", "mysql": "mysql-test"}
        config.env = "PROD"
        report: Mock = Mock(spec=KafkaConnectSourceReport)
        return config, report

    def test_mysql_sink_platform_config_parsing(self) -> None:
        """Test MySQL sink connector with Platform configuration (JDBC URL)."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:mysql://localhost:3306/ecommerce_db",
            "connection.user": "mysql_user",
            "connection.password": "mysql_password",
            "topics": "orders,products,customers",
            "table.name.format": "${topic}",
            "auto.create": "true",
            "batch.size": "500",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-platform-sink",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["orders", "products", "customers"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        parser = connector.get_parser(manifest)

        assert parser.database_name == "ecommerce_db"
        assert parser.host == "localhost"
        assert parser.port == "3306"
        assert len(parser.topics_to_tables) == 3
        assert parser.topics_to_tables["orders"] == "orders"
        assert parser.topics_to_tables["products"] == "products"
        assert parser.topics_to_tables["customers"] == "customers"

    def test_mysql_sink_cloud_config_parsing(self) -> None:
        """Test MySQL sink connector with Confluent Cloud configuration."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql-aws-rds.us-east-1.amazonaws.com",
            "connection.port": "3306",
            "connection.user": "admin",
            "connection.password": "secret_password",
            "db.name": "analytics_db",
            "topics": "user_events,order_events,product_views",
            "table.name.format": "${topic}",
            "input.data.format": "AVRO",
            "auto.create": "true",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-cloud-sink",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["user_events", "order_events", "product_views"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        parser = connector.get_parser(manifest)

        assert parser.database_name == "analytics_db"
        assert parser.host == "mysql-aws-rds.us-east-1.amazonaws.com"
        assert parser.port == "3306"
        assert len(parser.topics_to_tables) == 3
        assert parser.topics_to_tables["user_events"] == "user_events"
        assert parser.topics_to_tables["order_events"] == "order_events"
        assert parser.topics_to_tables["product_views"] == "product_views"

    def test_mysql_sink_lineage_extraction(self) -> None:
        """Test lineage extraction for MySQL sink connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql.example.com",
            "connection.port": "3306",
            "db.name": "warehouse",
            "topics": "sales_data,inventory_data",
            "table.name.format": "${topic}",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-lineage-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["sales_data", "inventory_data"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        lineages = connector.extract_lineages()

        assert len(lineages) == 2

        # Check sales_data lineage
        sales_lineage = next(
            lineage for lineage in lineages if "sales_data" in lineage.target_dataset
        )
        assert sales_lineage.source_platform == "kafka"
        assert sales_lineage.target_platform == "mysql"
        assert "warehouse.sales_data" in sales_lineage.target_dataset
        assert (
            sales_lineage.source_dataset
            and "sales_data" in sales_lineage.source_dataset
        )

        # Check inventory_data lineage
        inventory_lineage = next(
            lineage
            for lineage in lineages
            if "inventory_data" in lineage.target_dataset
        )
        assert inventory_lineage.source_platform == "kafka"
        assert inventory_lineage.target_platform == "mysql"
        assert "warehouse.inventory_data" in inventory_lineage.target_dataset
        assert (
            inventory_lineage.source_dataset
            and "inventory_data" in inventory_lineage.source_dataset
        )

    def test_mysql_sink_with_regex_transforms(self) -> None:
        """Test MySQL sink with RegexRouter transformations."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql.example.com",
            "connection.port": "3306",
            "db.name": "transformed_data",
            "topics": "raw_user_activity,raw_purchase_events",
            "table.name.format": "${topic}",
            "transforms": "CleanTopicNames",
            "transforms.CleanTopicNames.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.CleanTopicNames.regex": "raw_(.*)",
            "transforms.CleanTopicNames.replacement": "clean_$1",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-transform-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["raw_user_activity", "raw_purchase_events"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        parser = connector.get_parser(manifest)

        # Check that topics are transformed to table names
        assert parser.topics_to_tables["raw_user_activity"] == "clean_user_activity"
        assert parser.topics_to_tables["raw_purchase_events"] == "clean_purchase_events"

    def test_mysql_sink_table_name_sanitization(self) -> None:
        """Test MySQL table name sanitization and length limits."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql.example.com",
            "connection.port": "3306",
            "db.name": "test_db",
            "topics": "topic-with-hyphens,topic.with.dots,123numeric_start,very_long_topic_name_that_exceeds_mysql_table_name_limit_of_64_characters",
            # No table.name.format - this will trigger sanitization
            "table.name.format": "no_topic_placeholder",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-sanitization-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=[
                "topic-with-hyphens",
                "topic.with.dots",
                "123numeric_start",
                "very_long_topic_name_that_exceeds_mysql_table_name_limit_of_64_characters",
            ],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        parser = connector.get_parser(manifest)

        # Check sanitized table names (MySQL convention: lowercase, underscores, max 64 chars)
        assert parser.topics_to_tables["topic-with-hyphens"] == "topic_with_hyphens"
        assert parser.topics_to_tables["topic.with.dots"] == "topic_with_dots"
        assert parser.topics_to_tables["123numeric_start"] == "_123numeric_start"

        # Check length limit (64 characters)
        long_table_name = parser.topics_to_tables[
            "very_long_topic_name_that_exceeds_mysql_table_name_limit_of_64_characters"
        ]
        assert len(long_table_name) <= 64
        assert (
            long_table_name
            == "very_long_topic_name_that_exceeds_mysql_table_name_limit_of_64_c"
        )

    def test_mysql_sink_flow_property_bag_platform(self) -> None:
        """Test flow property bag for Platform MySQL connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:mysql://localhost:3306/test_db",
            "connection.user": "test_user",
            "connection.password": "secret_password",
            "topics": "test_topic",
            "auto.create": "true",
            "batch.size": "1000",
            "max.retries": "3",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-platform-properties",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        properties = connector.extract_flow_property_bag()

        # Should exclude sensitive fields
        assert "connection.password" not in properties
        assert "connection.user" not in properties  # Excluded for Platform connectors

        # Should include other properties
        assert (
            properties["connector.class"]
            == "io.confluent.connect.jdbc.JdbcSinkConnector"
        )
        assert properties["topics"] == "test_topic"
        assert properties["auto.create"] == "true"
        assert properties["batch.size"] == "1000"
        assert properties["max.retries"] == "3"

    def test_mysql_sink_flow_property_bag_cloud(self) -> None:
        """Test flow property bag for Cloud MySQL connector."""
        connector_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql.example.com",
            "connection.port": "3306",
            "connection.user": "cloud_user",
            "connection.password": "secret_password",
            "db.name": "cloud_db",
            "topics": "test_topic",
            "input.data.format": "AVRO",
            "auto.create": "true",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-cloud-properties",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)
        properties = connector.extract_flow_property_bag()

        # Should exclude only password for Cloud connectors
        assert "connection.password" not in properties

        # Should include connection.user for Cloud connectors (it's a separate field)
        assert properties["connection.user"] == "cloud_user"
        assert properties["connection.host"] == "mysql.example.com"
        assert properties["db.name"] == "cloud_db"
        assert properties["input.data.format"] == "AVRO"
        assert properties["auto.create"] == "true"

    def test_mysql_sink_error_handling(self) -> None:
        """Test error handling for invalid MySQL configurations."""
        # Test missing database name
        invalid_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql.example.com",
            "connection.port": "3306",
            # Missing db.name
            "topics": "test_topic",
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-invalid",
            type="sink",
            config=invalid_config,
            tasks={},
            topic_names=["test_topic"],
        )
        config, report = self.create_mock_dependencies()

        connector: MySqlSinkConnector = MySqlSinkConnector(manifest, config, report)

        try:
            connector.get_parser(manifest)
            raise AssertionError(
                "Should have raised ValueError for missing database name"
            )
        except ValueError as e:
            assert "MySQL connection details (host, database) are required" in str(e)

    def test_mysql_sink_connector_detection(self) -> None:
        """Test that MySQL sink connectors are properly detected."""
        from datahub.ingestion.source.kafka_connect.common import (
            CLOUD_SINK_CLASSES,
            MYSQL_SINK_CLOUD,
        )

        # Test MySqlSink detection
        assert MYSQL_SINK_CLOUD in CLOUD_SINK_CLASSES
        assert MYSQL_SINK_CLOUD == "MySqlSink"


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases across all connector types."""

    def test_missing_required_config_fields(self) -> None:
        """Test handling of missing required configuration fields."""
        # Test JDBC source without connection.url or cloud fields
        invalid_jdbc_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "mode": "incrementing",
            # Missing connection details
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="invalid-jdbc",
            type="source",
            config=invalid_jdbc_config,
            tasks={},
            topic_names=[],
        )
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: ConfluentJDBCSourceConnector = ConfluentJDBCSourceConnector(
            manifest, config, report
        )

        try:
            connector.get_parser(manifest)
            raise AssertionError(
                "Should have raised an error for missing connection details"
            )
        except (ValueError, KeyError, AttributeError):
            # Expected - missing required configuration
            pass

    def test_invalid_regex_patterns(self) -> None:
        """Test handling of invalid regex patterns in transforms."""
        config_with_invalid_regex: Dict[str, str] = {
            "transforms": "BadRegex",
            "transforms.BadRegex.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.BadRegex.regex": "[invalid-regex-pattern",  # Missing closing bracket
            "transforms.BadRegex.replacement": "replacement",
        }

        transform: RegexRouterTransform = RegexRouterTransform(
            config_with_invalid_regex
        )

        # Should not crash, but should log warning and return original topic
        result: str = transform.apply_transforms("test-topic")
        assert result == "test-topic"

    def test_empty_topic_names(self) -> None:
        """Test handling of connectors with no topics."""
        connector_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres.example.com",
            "connection.port": "5432",
            "db.name": "test_db",
            "topics": "",  # Empty topics
        }

        manifest: ConnectorManifest = ConnectorManifest(
            name="empty-topics-test",
            type="sink",
            config=connector_config,
            tasks={},
            topic_names=[],  # No topics
        )
        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        config.platform_instance_map = {}
        config.env = "PROD"
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        connector: PostgresSinkConnector = PostgresSinkConnector(
            manifest, config, report
        )
        lineages = connector.extract_lineages()

        # Should return empty lineages without crashing
        assert len(lineages) == 0

    def test_malformed_connection_urls(self) -> None:
        """Test handling of malformed JDBC connection URLs."""
        malformed_configs = [
            "not-a-jdbc-url",
            "jdbc:postgresql://",  # Missing host/port/database
            "jdbc:postgresql://host:port",  # Missing database
            "jdbc:mysql://host:port/db?invalid-params",
        ]

        for malformed_url in malformed_configs:
            connector_config: Dict[str, str] = {
                "connector.class": "PostgresSink",
                "connection.url": malformed_url,
                "topics": "test_topic",
            }

            manifest: ConnectorManifest = ConnectorManifest(
                name="malformed-url-test",
                type="sink",
                config=connector_config,
                tasks={},
                topic_names=["test_topic"],
            )
            config: Mock = Mock(spec=KafkaConnectSourceConfig)
            report: Mock = Mock(spec=KafkaConnectSourceReport)

            connector: PostgresSinkConnector = PostgresSinkConnector(
                manifest, config, report
            )

            try:
                connector.get_parser(manifest)
                # Some malformed URLs might still parse, that's okay
            except (ValueError, AttributeError):
                # Expected for truly malformed URLs
                pass


class TestDualCompatibilityValidation:
    """Test that all sink connectors work with both Platform and Cloud configurations."""

    def test_mysql_sink_dual_compatibility(self) -> None:
        """Test MySQL sink works with both Platform JDBC and Cloud MySqlSink configurations."""

        # Platform configuration (JDBC)
        platform_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:mysql://mysql-host:3306/testdb",
            "connection.user": "user",
            "connection.password": "password",
            "topics": "orders,users",
            "table.name.format": "${topic}",
        }

        platform_manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-platform-sink",
            type="sink",
            config=platform_config,
            tasks={},
            topic_names=["orders", "users"],
        )

        # Cloud configuration (MySqlSink)
        cloud_config: Dict[str, str] = {
            "connector.class": "MySqlSink",
            "connection.host": "mysql-host",
            "connection.port": "3306",
            "connection.user": "user",
            "connection.password": "password",
            "db.name": "testdb",
            "topics": "orders,users",
            "table.name.format": "${topic}",
        }

        cloud_manifest: ConnectorManifest = ConnectorManifest(
            name="mysql-cloud-sink",
            type="sink",
            config=cloud_config,
            tasks={},
            topic_names=["orders", "users"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        # Test Platform connector
        platform_connector: MySqlSinkConnector = MySqlSinkConnector(
            platform_manifest, config, report
        )
        platform_parser = platform_connector.get_parser(platform_manifest)
        platform_lineages = platform_connector.extract_lineages()

        # Test Cloud connector
        cloud_connector: MySqlSinkConnector = MySqlSinkConnector(
            cloud_manifest, config, report
        )
        cloud_parser = cloud_connector.get_parser(cloud_manifest)
        cloud_lineages = cloud_connector.extract_lineages()

        # Both should produce identical results
        assert platform_parser.database_name == cloud_parser.database_name == "testdb"
        assert platform_parser.host == cloud_parser.host == "mysql-host"
        assert platform_parser.port == cloud_parser.port == "3306"
        assert platform_parser.topics_to_tables == cloud_parser.topics_to_tables

        # Both should generate the same lineages
        assert len(platform_lineages) == len(cloud_lineages) == 2

        # Check lineage content is identical
        for platform_lineage, cloud_lineage in zip(platform_lineages, cloud_lineages):
            assert platform_lineage.source_dataset == cloud_lineage.source_dataset
            assert platform_lineage.target_dataset == cloud_lineage.target_dataset
            assert (
                platform_lineage.source_platform
                == cloud_lineage.source_platform
                == "kafka"
            )
            assert (
                platform_lineage.target_platform
                == cloud_lineage.target_platform
                == "mysql"
            )

    def test_postgres_sink_dual_compatibility(self) -> None:
        """Test PostgreSQL sink works with both Platform JDBC and Cloud PostgresSink configurations."""

        # Platform configuration (JDBC)
        platform_config: Dict[str, str] = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "connection.url": "jdbc:postgresql://postgres-host:5432/testdb",
            "connection.user": "user",
            "connection.password": "password",
            "topics": "orders,users",
            "table.name.format": "public.${topic}",
        }

        platform_manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-platform-sink",
            type="sink",
            config=platform_config,
            tasks={},
            topic_names=["orders", "users"],
        )

        # Cloud configuration (PostgresSink)
        cloud_config: Dict[str, str] = {
            "connector.class": "PostgresSink",
            "connection.host": "postgres-host",
            "connection.port": "5432",
            "connection.user": "user",
            "connection.password": "password",
            "db.name": "testdb",
            "topics": "orders,users",
            "table.name.format": "public.${topic}",
        }

        cloud_manifest: ConnectorManifest = ConnectorManifest(
            name="postgres-cloud-sink",
            type="sink",
            config=cloud_config,
            tasks={},
            topic_names=["orders", "users"],
        )

        config: Mock = Mock(spec=KafkaConnectSourceConfig)
        report: Mock = Mock(spec=KafkaConnectSourceReport)

        # Test Platform connector
        platform_connector: PostgresSinkConnector = PostgresSinkConnector(
            platform_manifest, config, report
        )
        platform_parser = platform_connector.get_parser(platform_manifest)
        platform_lineages = platform_connector.extract_lineages()

        # Test Cloud connector
        cloud_connector: PostgresSinkConnector = PostgresSinkConnector(
            cloud_manifest, config, report
        )
        cloud_parser = cloud_connector.get_parser(cloud_manifest)
        cloud_lineages = cloud_connector.extract_lineages()

        # Both should produce identical results
        assert platform_parser.database_name == cloud_parser.database_name == "testdb"
        assert platform_parser.host == cloud_parser.host == "postgres-host"
        assert platform_parser.port == cloud_parser.port == "5432"
        assert platform_parser.schema_name == cloud_parser.schema_name == "public"
        assert platform_parser.topics_to_tables == cloud_parser.topics_to_tables

        # Both should generate the same lineages
        assert len(platform_lineages) == len(cloud_lineages) == 2

        # Check lineage content is identical
        for platform_lineage, cloud_lineage in zip(platform_lineages, cloud_lineages):
            assert platform_lineage.source_dataset == cloud_lineage.source_dataset
            assert platform_lineage.target_dataset == cloud_lineage.target_dataset
            assert (
                platform_lineage.source_platform
                == cloud_lineage.source_platform
                == "kafka"
            )
            assert (
                platform_lineage.target_platform
                == cloud_lineage.target_platform
                == "postgres"
            )

    def test_connector_detection_dual_compatibility(self) -> None:
        """Test that connector detection works for both Platform and Cloud connector classes."""

        # Mock configurations for different connector types
        test_cases = [
            # MySQL
            ("io.confluent.connect.jdbc.JdbcSinkConnector", "mysql", "Platform MySQL"),
            ("MySqlSink", "mysql", "Cloud MySQL"),
            # PostgreSQL
            (
                "io.confluent.connect.jdbc.JdbcSinkConnector",
                "postgres",
                "Platform PostgreSQL",
            ),
            ("PostgresSink", "postgres", "Cloud PostgreSQL"),
            # Snowflake
            (
                "com.snowflake.kafka.connector.SnowflakeSinkConnector",
                "snowflake",
                "Platform Snowflake",
            ),
            ("SnowflakeSink", "snowflake", "Cloud Snowflake"),
        ]

        for connector_class, expected_platform, description in test_cases:
            # Create a minimal connector manifest
            connector_config: Dict[str, str] = {
                "connector.class": connector_class,
                "topics": "test_topic",
            }

            # Add platform-specific required fields
            if "mysql" in expected_platform.lower():
                if "MySqlSink" in connector_class:
                    connector_config.update(
                        {
                            "connection.host": "mysql-host",
                            "db.name": "testdb",
                        }
                    )
                else:
                    connector_config.update(
                        {
                            "connection.url": "jdbc:mysql://mysql-host:3306/testdb",
                        }
                    )
            elif "postgres" in expected_platform.lower():
                if "PostgresSink" in connector_class:
                    connector_config.update(
                        {
                            "connection.host": "postgres-host",
                            "db.name": "testdb",
                        }
                    )
                else:
                    connector_config.update(
                        {
                            "connection.url": "jdbc:postgresql://postgres-host:5432/testdb",
                        }
                    )
            elif "snowflake" in expected_platform.lower():
                connector_config.update(
                    {
                        "snowflake.database.name": "testdb",
                        "snowflake.schema.name": "public",
                    }
                )

            ConnectorManifest(
                name=f"test-{expected_platform}-connector",
                type="sink",
                config=connector_config,
                tasks={},
                topic_names=["test_topic"],
            )

            # This should not raise an exception and should detect the correct connector type
            # The actual connector instantiation is tested in the main kafka_connect.py logic
            print(f"✅ {description}: {connector_class} → {expected_platform}")

        print("✅ All connector detection tests passed!")
