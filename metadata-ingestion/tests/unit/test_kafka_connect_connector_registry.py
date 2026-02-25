"""Tests for connector_registry module."""

from unittest.mock import Mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka_connect.common import (
    MYSQL_SINK_CLOUD,
    POSTGRES_CDC_SOURCE_CLOUD,
    POSTGRES_SINK_CLOUD,
    SINK,
    SNOWFLAKE_SINK_CLOUD,
    SNOWFLAKE_SOURCE_CLOUD,
    SOURCE,
    ConnectorManifest,
    GenericConnectorConfig,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import (
    ConnectorRegistry,
    _GenericConnector,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BIGQUERY_SINK_CONNECTOR_CLASS,
    S3_SINK_CONNECTOR_CLASS,
    SNOWFLAKE_SINK_CONNECTOR_CLASS,
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
    JdbcSinkConnector,
    SnowflakeSinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JDBC_SOURCE_CONNECTOR_CLASS,
    MONGO_SOURCE_CONNECTOR_CLASS,
    ConfluentJDBCSourceConnector,
    DebeziumSourceConnector,
    MongoSourceConnector,
    SnowflakeSourceConnector,
)


def create_mock_config() -> KafkaConnectSourceConfig:
    """Create a mock KafkaConnectSourceConfig."""
    config = Mock(spec=KafkaConnectSourceConfig)
    config.use_schema_resolver = False
    config.env = "PROD"
    config.platform_instance_map = {}
    config.generic_connectors = []
    config.connect_to_platform_map = {}
    return config


def create_mock_report() -> KafkaConnectSourceReport:
    """Create a mock KafkaConnectSourceReport."""
    return Mock(spec=KafkaConnectSourceReport)


def create_manifest(
    connector_type: str, connector_class: str, name: str = "test-connector"
) -> ConnectorManifest:
    """Create a ConnectorManifest for testing."""
    return ConnectorManifest(
        name=name,
        type=connector_type,
        config={"connector.class": connector_class},
        tasks=[],
        topic_names=[],
    )


class TestConnectorRegistrySourceConnectors:
    """Test source connector routing logic."""

    def test_jdbc_source_connector(self) -> None:
        """Test routing for JDBC source connector."""
        manifest = create_manifest(SOURCE, JDBC_SOURCE_CONNECTOR_CLASS)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, ConfluentJDBCSourceConnector)
        assert connector.get_platform() == "unknown"

    def test_snowflake_source_connector(self) -> None:
        """Test routing for Snowflake source connector."""
        manifest = create_manifest(SOURCE, SNOWFLAKE_SOURCE_CLOUD)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, SnowflakeSourceConnector)
        assert connector.get_platform() == "snowflake"

    def test_debezium_postgres_connector(self) -> None:
        """Test routing for Debezium Postgres CDC connector."""
        manifest = create_manifest(SOURCE, POSTGRES_CDC_SOURCE_CLOUD)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, DebeziumSourceConnector)
        assert connector.get_platform() == "postgres"

    def test_debezium_connector_with_prefix(self) -> None:
        """Test routing for Debezium connector with standard prefix."""
        manifest = create_manifest(SOURCE, "io.debezium.connector.mysql.MySqlConnector")
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, DebeziumSourceConnector)

    def test_mongo_source_connector(self) -> None:
        """Test routing for MongoDB source connector."""
        manifest = create_manifest(SOURCE, MONGO_SOURCE_CONNECTOR_CLASS)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, MongoSourceConnector)
        assert connector.get_platform() == "mongodb"

    def test_unknown_source_connector(self) -> None:
        """Test routing for unknown source connector class."""
        manifest = create_manifest(SOURCE, "com.example.UnknownSourceConnector")
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is None


class TestConnectorRegistrySinkConnectors:
    """Test sink connector routing logic."""

    def test_bigquery_sink_connector(self) -> None:
        """Test routing for BigQuery sink connector."""
        manifest = create_manifest(SINK, BIGQUERY_SINK_CONNECTOR_CLASS)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, BigQuerySinkConnector)
        assert connector.get_platform() == "bigquery"

    def test_bigquery_sink_connector_alternate_class(self) -> None:
        """Test routing for BigQuery sink with alternate connector class."""
        manifest = create_manifest(
            SINK, "io.confluent.connect.bigquery.BigQuerySinkConnector"
        )
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, BigQuerySinkConnector)

    def test_s3_sink_connector(self) -> None:
        """Test routing for S3 sink connector."""
        manifest = create_manifest(SINK, S3_SINK_CONNECTOR_CLASS)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, ConfluentS3SinkConnector)
        assert connector.get_platform() == "s3"

    def test_snowflake_sink_connector_self_hosted(self) -> None:
        """Test routing for self-hosted Snowflake sink connector."""
        manifest = create_manifest(SINK, SNOWFLAKE_SINK_CONNECTOR_CLASS)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, SnowflakeSinkConnector)
        assert connector.get_platform() == "snowflake"

    def test_snowflake_sink_connector_cloud(self) -> None:
        """Test routing for Confluent Cloud Snowflake sink connector."""
        manifest = create_manifest(SINK, SNOWFLAKE_SINK_CLOUD)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, SnowflakeSinkConnector)

    def test_postgres_sink_cloud(self) -> None:
        """Test routing for Confluent Cloud Postgres sink connector."""
        manifest = create_manifest(SINK, POSTGRES_SINK_CLOUD)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, JdbcSinkConnector)
        assert connector.get_platform() == "postgres"

    def test_mysql_sink_cloud(self) -> None:
        """Test routing for Confluent Cloud MySQL sink connector."""
        manifest = create_manifest(SINK, MYSQL_SINK_CLOUD)
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, JdbcSinkConnector)
        assert connector.get_platform() == "mysql"

    def test_unknown_sink_connector(self) -> None:
        """Test routing for unknown sink connector class."""
        manifest = create_manifest(SINK, "com.example.UnknownSinkConnector")
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is None


class TestConnectorRegistryGenericConnectors:
    """Test generic connector fallback logic."""

    def test_generic_connector_by_name(self) -> None:
        """Test routing to generic connector by name match."""
        manifest = create_manifest(
            SOURCE, "com.custom.CustomConnector", name="my-custom-connector"
        )

        generic_config = GenericConnectorConfig(
            connector_name="my-custom-connector",
            source_platform="custom",
            source_dataset="custom_dataset",
        )

        config = create_mock_config()
        config.generic_connectors = [generic_config]
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert isinstance(connector, _GenericConnector)
        assert connector.generic_config == generic_config

    def test_generic_connector_no_match(self) -> None:
        """Test generic connector with no name match returns None."""
        manifest = create_manifest(
            SOURCE, "com.custom.CustomConnector", name="different-name"
        )

        generic_config = GenericConnectorConfig(
            connector_name="my-custom-connector",
            source_platform="custom",
            source_dataset="custom_dataset",
        )

        config = create_mock_config()
        config.generic_connectors = [generic_config]
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is None


class TestConnectorRegistryUnknownTypes:
    """Test handling of unknown connector types."""

    def test_unknown_connector_type(self) -> None:
        """Test handling of unknown connector type (not source or sink)."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="unknown",
            config={"connector.class": "some.class"},
            tasks=[],
            topic_names=[],
        )
        config = create_mock_config()
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is None


class TestConnectorRegistrySchemaResolver:
    """Test schema resolver creation and attachment."""

    def test_schema_resolver_disabled(self) -> None:
        """Test schema resolver not created when feature is disabled."""
        manifest = create_manifest(SOURCE, JDBC_SOURCE_CONNECTOR_CLASS)
        config = create_mock_config()
        config.use_schema_resolver = False
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report
        )

        assert connector is not None
        assert connector.schema_resolver is None

    def test_schema_resolver_no_context(self) -> None:
        """Test schema resolver not created when PipelineContext is None."""
        manifest = create_manifest(SOURCE, JDBC_SOURCE_CONNECTOR_CLASS)
        config = create_mock_config()
        config.use_schema_resolver = True
        report = create_mock_report()

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx=None
        )

        assert connector is not None
        assert connector.schema_resolver is None

    def test_schema_resolver_no_graph(self) -> None:
        """Test schema resolver not created when graph is not available."""
        manifest = create_manifest(SOURCE, JDBC_SOURCE_CONNECTOR_CLASS)
        config = create_mock_config()
        config.use_schema_resolver = True
        report = create_mock_report()

        ctx = Mock(spec=PipelineContext)
        ctx.graph = None

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx=ctx
        )

        assert connector is not None
        assert connector.schema_resolver is None

    def test_schema_resolver_created_successfully(self) -> None:
        """Test schema resolver created when all conditions are met."""
        manifest = create_manifest(SOURCE, POSTGRES_CDC_SOURCE_CLOUD)
        config = create_mock_config()
        config.use_schema_resolver = True
        report = create_mock_report()

        mock_schema_resolver = Mock()
        mock_graph = Mock()
        mock_graph.initialize_schema_resolver_from_datahub.return_value = (
            mock_schema_resolver
        )
        ctx = Mock(spec=PipelineContext)
        ctx.graph = mock_graph

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx=ctx
        )

        assert connector is not None
        assert connector.schema_resolver == mock_schema_resolver

        # Verify initialize_schema_resolver_from_datahub was called with correct parameters
        mock_graph.initialize_schema_resolver_from_datahub.assert_called_once_with(
            platform="postgres",
            platform_instance=None,
            env="PROD",
        )

    def test_schema_resolver_with_platform_instance(self) -> None:
        """Test schema resolver created with platform instance."""
        manifest = create_manifest(
            SOURCE, POSTGRES_CDC_SOURCE_CLOUD, name="my-connector"
        )
        config = create_mock_config()
        config.use_schema_resolver = True
        # Type annotation to satisfy mypy - nested dict structure
        config.connect_to_platform_map = {
            "my-connector": {"postgres": "my-instance"}  # type: ignore[dict-item]
        }
        report = create_mock_report()

        mock_schema_resolver = Mock()
        mock_graph = Mock()
        mock_graph.initialize_schema_resolver_from_datahub.return_value = (
            mock_schema_resolver
        )
        ctx = Mock(spec=PipelineContext)
        ctx.graph = mock_graph

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx=ctx
        )

        assert connector is not None

        # Verify initialize_schema_resolver_from_datahub was called with platform instance
        mock_graph.initialize_schema_resolver_from_datahub.assert_called_once_with(
            platform="postgres",
            platform_instance="my-instance",
            env="PROD",
        )

    def test_schema_resolver_creation_fails_gracefully(self) -> None:
        """Test that schema resolver creation failure doesn't break connector creation."""
        manifest = create_manifest(SOURCE, POSTGRES_CDC_SOURCE_CLOUD)
        config = create_mock_config()
        config.use_schema_resolver = True
        report = create_mock_report()

        mock_graph = Mock()
        mock_graph.initialize_schema_resolver_from_datahub.side_effect = Exception(
            "Schema resolver creation failed"
        )
        ctx = Mock(spec=PipelineContext)
        ctx.graph = mock_graph

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, config, report, ctx=ctx
        )

        # Connector should still be created
        assert connector is not None
        # But schema resolver should be None
        assert connector.schema_resolver is None


class TestConnectorRegistryTopicExtraction:
    """Test get_topics_from_config() method."""

    def test_get_topics_from_jdbc_source(self) -> None:
        """Test topic extraction from JDBC source connector."""
        manifest = create_manifest(SOURCE, JDBC_SOURCE_CONNECTOR_CLASS)
        manifest.config["topics"] = "topic1,topic2,topic3"
        config = create_mock_config()
        report = create_mock_report()

        topics = ConnectorRegistry.get_topics_from_config(manifest, config, report)

        # JDBC source doesn't have simple topic extraction from config
        # It needs database connection or inference
        assert isinstance(topics, list)

    def test_get_topics_no_handler(self) -> None:
        """Test topic extraction when no handler is found."""
        manifest = create_manifest(SOURCE, "com.example.UnknownConnector")
        config = create_mock_config()
        report = create_mock_report()

        topics = ConnectorRegistry.get_topics_from_config(manifest, config, report)

        assert topics == []


class TestGenericConnector:
    """Test _GenericConnector implementation."""

    def test_extract_lineages(self) -> None:
        """Test generic connector lineage extraction uses topic_names."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        manifest.topic_names = ["topic-a", "topic-b"]
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        lineages = connector.extract_lineages()

        assert len(lineages) == 2
        assert lineages[0].source_platform == "custom"
        assert lineages[0].source_dataset == "my_dataset"
        assert lineages[0].target_dataset == "topic-a"
        assert lineages[0].target_platform == "kafka"
        assert lineages[1].target_dataset == "topic-b"

    def test_extract_lineages_no_topics(self) -> None:
        """Test generic connector returns empty lineages when no topics available."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        lineages = connector.extract_lineages()

        assert len(lineages) == 0

    def test_get_topics_from_config_topics_field(self) -> None:
        """Test topic extraction from 'topics' config field."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        manifest.config["topics"] = "topic1,topic2,topic3"
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        topics = connector.get_topics_from_config()

        assert topics == ["topic1", "topic2", "topic3"]

    def test_get_topics_from_config_kafka_topic_field(self) -> None:
        """Test topic extraction from 'kafka.topic' config field."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        manifest.config["kafka.topic"] = "single-topic"
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        topics = connector.get_topics_from_config()

        assert topics == ["single-topic"]

    def test_get_topics_from_config_topic_field(self) -> None:
        """Test topic extraction from 'topic' config field."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        manifest.config["topic"] = "another-topic"
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        topics = connector.get_topics_from_config()

        assert topics == ["another-topic"]

    def test_get_topics_from_config_no_topics(self) -> None:
        """Test topic extraction when no topic config exists."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)
        topics = connector.get_topics_from_config()

        assert topics == []

    def test_supports_connector_class(self) -> None:
        """Test that generic connector supports any class."""
        assert _GenericConnector.supports_connector_class("any.connector.Class")
        assert _GenericConnector.supports_connector_class("")

    def test_get_platform(self) -> None:
        """Test get_platform returns 'unknown'."""
        manifest = create_manifest(SOURCE, "com.custom.CustomConnector")
        config = create_mock_config()
        report = create_mock_report()

        generic_config = GenericConnectorConfig(
            connector_name="test-connector",
            source_platform="custom",
            source_dataset="my_dataset",
        )

        connector = _GenericConnector(manifest, config, report, generic_config)

        assert connector.get_platform() == "unknown"
