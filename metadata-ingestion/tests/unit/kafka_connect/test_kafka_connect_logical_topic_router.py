from datahub.ingestion.source.kafka_connect.common import (
    SOURCE,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import ConnectorRegistry


def test_debezium_logical_topic_router_emits_rerouted_topic_lineage() -> None:
    manifest = ConnectorManifest(
        name="sharded-postgres-cdc",
        type=SOURCE,
        config={
            "connector.class": ("io.debezium.connector.postgresql.PostgresConnector"),
            "database.dbname": "app_db",
            "table.include.list": "inventory_shard1.customers",
            "topic.prefix": "server",
            "transforms": "reroute",
            "transforms.reroute.type": ("io.debezium.transforms.ByLogicalTableRouter"),
            "transforms.reroute.topic.regex": r"^(.*)\..*_shard\d+\.(.*)$",
            "transforms.reroute.topic.replacement": "$1.$2",
        },
        tasks=[],
        topic_names=["server.customers"],
    )
    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083",
        env="PROD",
    )

    connector = ConnectorRegistry.get_connector_for_manifest(
        manifest,
        config,
        KafkaConnectSourceReport(),
    )
    assert connector is not None

    lineages = connector.extract_lineages()

    assert len(lineages) == 1
    lineage = lineages[0]
    assert lineage.source_dataset == "app_db.inventory_shard1.customers"
    assert lineage.target_dataset == "server.customers"
    assert lineage.source_platform == "postgres"
