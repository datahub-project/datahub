from typing import Dict, List, Optional
from unittest.mock import Mock, patch

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.ingestion.source.kafka_connect.source_connectors import (
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


class TestClusterTopicScoping:
    def test_should_assign_for_sinks(self) -> None:
        manifest = _make_manifest(
            name="s3-sink",
            connector_type="sink",
            config={"connector.class": "io.confluent.connect.s3.S3SinkConnector"},
        )
        assert KafkaConnectSource._should_assign_cluster_topics(manifest, object())

    def test_should_assign_for_event_router_sources(self) -> None:
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
        connector = DebeziumSourceConnector(
            manifest, _make_cloud_source().config, KafkaConnectSourceReport()
        )
        assert KafkaConnectSource._should_assign_cluster_topics(manifest, connector)

    def test_should_not_assign_for_plain_debezium_sources(self) -> None:
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
        connector = DebeziumSourceConnector(
            manifest, _make_cloud_source().config, KafkaConnectSourceReport()
        )
        assert not KafkaConnectSource._should_assign_cluster_topics(manifest, connector)

    def test_extract_lineages_does_not_assign_whole_cluster_to_plain_source(
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
            def extract_lineages(self) -> List[object]:
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
