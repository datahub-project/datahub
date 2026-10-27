"""Regression tests for kafka-connect Debezium table discovery when the source
platform datasets were ingested with a `platform_instance`.

DataHub encodes the platform_instance *inside* the dataset URN name, i.e.
`urn:li:dataset:(urn:li:dataPlatform:postgres,<instance>.<db>.<schema>.<table>,PROD)`.
When the Kafka Connect source builds a SchemaResolver for a platform that has a
platform_instance configured, every cached URN carries that prefix.

`_extract_table_name_from_urn` must strip that prefix so the rest of the
Debezium logic sees the logical `db.schema.table` name. Otherwise:

1. Table discovery filters on `db.` but the name starts with `instance.`, so
   `_discover_tables_from_database` returns nothing.
2. The resolver-derived `source_dataset` still carries the prefix and the
   emitter re-applies the instance, producing a doubled `instance.instance.`
   segment in the lineage URN.
"""

from unittest.mock import Mock

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)

PLATFORM_INSTANCE = "my_instance"
DATABASE = "mydb"


def _make_connector(platform_instance, urns):
    manifest = ConnectorManifest(
        name="test-postgres-cdc",
        type="source",
        config={
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.dbname": DATABASE,
            "topic.prefix": "server",
        },
        tasks=[],
        topic_names=[],
    )
    config = Mock(spec=KafkaConnectSourceConfig)
    config.use_schema_resolver = True
    config.env = "PROD"

    schema_resolver = Mock()
    schema_resolver.platform = "postgres"
    schema_resolver.platform_instance = platform_instance
    schema_resolver.env = "PROD"
    schema_resolver.get_urns.return_value = set(urns)

    return DebeziumSourceConnector(
        connector_manifest=manifest,
        config=config,
        report=Mock(spec=KafkaConnectSourceReport),
        schema_resolver=schema_resolver,
    )


def test_extract_table_name_strips_platform_instance_prefix() -> None:
    """The parsed name must be the logical db.schema.table, without the
    platform_instance prefix that DataHub bakes into the URN."""
    connector = _make_connector(PLATFORM_INSTANCE, urns=[])

    urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,"
        f"{PLATFORM_INSTANCE}.{DATABASE}.public.users,PROD)"
    )
    assert connector._extract_table_name_from_urn(urn) == f"{DATABASE}.public.users"


def test_extract_table_name_without_platform_instance_unchanged() -> None:
    """Sanity: with no platform_instance, the full URN name is returned as-is
    (guards against stripping a legitimate leading component)."""
    connector = _make_connector(None, urns=[])

    urn = f"urn:li:dataset:(urn:li:dataPlatform:postgres,{DATABASE}.public.users,PROD)"
    assert connector._extract_table_name_from_urn(urn) == f"{DATABASE}.public.users"


def test_discover_tables_from_database_with_platform_instance() -> None:
    """Discovery must find tables even when the SchemaResolver URNs carry a
    platform_instance prefix, returning schema.table names."""
    urns = [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,"
        f"{PLATFORM_INSTANCE}.{DATABASE}.public.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,"
        f"{PLATFORM_INSTANCE}.{DATABASE}.public.orders,PROD)",
    ]
    connector = _make_connector(PLATFORM_INSTANCE, urns=urns)

    discovered = connector._discover_tables_from_database(DATABASE, "postgres")

    assert sorted(discovered) == ["public.orders", "public.users"]
