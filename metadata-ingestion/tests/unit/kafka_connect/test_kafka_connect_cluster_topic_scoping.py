from typing import Dict, List, Optional
from unittest.mock import Mock, patch

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
)
from datahub.ingestion.source.kafka_connect.connector_registry import ConnectorRegistry
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    ConfluentS3SinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JDBC_SOURCE_CONNECTOR_CLASS,
    ConfluentJDBCSourceConnector,
    DebeziumSourceConnector,
)


def _make_cloud_source() -> KafkaConnectSource:
    with patch("requests.Session.get") as mock_get:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        mock_get.return_value = response

        config = KafkaConnectSourceConfig(
            confluent_cloud_environment_id="env-abc123",
            confluent_cloud_cluster_id="lkc-abc123",
            username="connect-key",
            password="connect-secret",
            use_schema_resolver=False,
            confluent_catalog={"enabled": False},
        )
        return KafkaConnectSource(config, Mock())


def _make_manifest(
    *,
    name: str,
    connector_type: str,
    config: Dict[str, str],
) -> ConnectorManifest:
    return ConnectorManifest(
        name=name,
        type=connector_type,
        config=config,
        tasks=[],
    )


def _registry_connector(
    manifest: ConnectorManifest,
) -> object:
    source = _make_cloud_source()
    connector = ConnectorRegistry.get_connector_for_manifest(
        manifest, source.config, source.report, None
    )
    assert connector is not None
    return connector


class TestClusterTopicScoping:
    def test_sinks_require_cluster_topics(self) -> None:
        manifest = _make_manifest(
            name="s3-sink",
            connector_type="sink",
            config={"connector.class": "io.confluent.connect.s3.S3SinkConnector"},
        )
        connector = _registry_connector(manifest)
        assert isinstance(connector, ConfluentS3SinkConnector)
        assert connector.requires_cluster_topics()

    def test_cloud_postgres_cdc_dispatches_to_debezium_without_cluster_topics(
        self,
    ) -> None:
        manifest = _make_manifest(
            name="source_postgres_cdc_01",
            connector_type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.dbname": "ecommerce",
                "database.server.name": "pg_cdc",
                "table.include.list": "public.orders",
            },
        )
        connector = _registry_connector(manifest)
        assert isinstance(connector, DebeziumSourceConnector)
        assert not isinstance(connector, ConfluentJDBCSourceConnector)
        assert not connector.requires_cluster_topics()

    def test_traditional_jdbc_source_requires_cluster_topics(self) -> None:
        manifest = _make_manifest(
            name="jdbc-source",
            connector_type="source",
            config={
                "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
                "connection.url": "jdbc:postgresql://localhost:5432/db",
                "table.whitelist": "public.orders",
            },
        )
        connector = _registry_connector(manifest)
        assert isinstance(connector, ConfluentJDBCSourceConnector)
        assert connector.requires_cluster_topics()

    def test_event_router_debezium_requires_cluster_topics(self) -> None:
        manifest = _make_manifest(
            name="outbox-cdc",
            connector_type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.outbox",
                "transforms": "outbox",
                "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
            },
        )
        connector = _registry_connector(manifest)
        assert isinstance(connector, DebeziumSourceConnector)
        assert connector.requires_cluster_topics()

    def test_plain_debezium_does_not_require_cluster_topics(self) -> None:
        manifest = _make_manifest(
            name="pg-cdc",
            connector_type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            },
        )
        connector = _registry_connector(manifest)
        assert isinstance(connector, DebeziumSourceConnector)
        assert not connector.requires_cluster_topics()

    def test_extract_lineages_does_not_assign_whole_cluster_to_plain_debezium(
        self,
    ) -> None:
        source = _make_cloud_source()
        manifest = _make_manifest(
            name="pg-cdc",
            connector_type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            },
        )

        assigned: List[Optional[List[str]]] = []

        class CapturingDebezium(DebeziumSourceConnector):
            def extract_lineages(self) -> List[KafkaConnectLineage]:
                assigned.append(self.all_cluster_topics)
                return []

            def extract_flow_property_bag(self) -> Dict[str, str]:
                return {}

        with (
            patch(
                "datahub.ingestion.source.kafka_connect.connector_registry."
                "ConnectorRegistry.get_connector_for_manifest",
                return_value=CapturingDebezium(manifest, source.config, source.report),
            ),
            patch.object(
                source,
                "_get_all_topics_from_kafka_api",
                return_value=["myserver.public.users", "unrelated.topic", "noise"],
            ),
        ):
            assert source.extract_connector_lineages(manifest)

        assert assigned == [None]

    def test_extract_lineages_assigns_cluster_topics_to_traditional_jdbc(self) -> None:
        source = _make_cloud_source()
        manifest = _make_manifest(
            name="jdbc-source",
            connector_type="source",
            config={
                "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
                "connection.url": "jdbc:postgresql://localhost:5432/db",
                "table.whitelist": "public.orders",
            },
        )
        manifest.topic_names = []

        cluster_topics = ["db.public.orders", "unrelated.topic"]
        assigned: List[Optional[List[str]]] = []

        class CapturingJdbc(ConfluentJDBCSourceConnector):
            def extract_lineages(self) -> List[KafkaConnectLineage]:
                assigned.append(
                    list(self.all_cluster_topics)
                    if self.all_cluster_topics is not None
                    else None
                )
                assert self.available_topics() == cluster_topics
                return []

            def extract_flow_property_bag(self) -> Dict[str, str]:
                return {}

        with (
            patch(
                "datahub.ingestion.source.kafka_connect.connector_registry."
                "ConnectorRegistry.get_connector_for_manifest",
                return_value=CapturingJdbc(manifest, source.config, source.report),
            ),
            patch.object(
                source,
                "_get_all_topics_from_kafka_api",
                return_value=cluster_topics,
            ),
        ):
            assert source.extract_connector_lineages(manifest)

        assert assigned == [cluster_topics]

    def test_cloud_cdc_emits_lineage_without_cluster_topics(self) -> None:
        source = _make_cloud_source()
        manifest = _make_manifest(
            name="source_postgres_cdc_01",
            connector_type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.dbname": "ecommerce",
                "database.server.name": "pg_cdc",
                "table.include.list": "public.orders",
                "topic.prefix": "pg_cdc",
            },
        )
        manifest.topic_names = []

        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, source.config, source.report, None
        )
        assert isinstance(connector, DebeziumSourceConnector)
        assert connector.all_cluster_topics is None

        lineages = connector.extract_lineages()
        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.source_dataset == "ecommerce.public.orders"
        assert lineage.target_dataset == "pg_cdc.public.orders"
        assert lineage.source_platform == "postgres"
