"""Debezium lineage through ByLogicalTableRouter, down to the topic's stored field paths."""

import json
from typing import Dict, List, Optional
from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaMetadataClass,
)
from tests.unit.kafka_connect.conftest import (
    StubSchemaResolver,
    attach_resolver,
    dataset_urn,
    make_manifest,
    mock_graph,
)

TOPIC = "cdc-orders"
TOPIC_URN = dataset_urn("kafka", TOPIC)
SOURCE_TABLE = "inventory.orders"

# Every shard's orders table is merged into one logical topic.
CONNECTOR_CONFIG = {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "shard1",
    "database.include.list": "inventory",
    "table.include.list": SOURCE_TABLE,
    "transforms": "Reroute",
    "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
    "transforms.Reroute.topic.regex": "(shard1)\\.(.*)\\.(.*)",
    "transforms.Reroute.topic.replacement": "cdc-$3",
    "transforms.Reroute.key.field.regex": "(.*)",
    "transforms.Reroute.key.field.replacement": "$1",
}

VALUE_RECORD = {
    "type": "record",
    "name": "Value",
    "namespace": "cdc_orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer_name", "type": ["null", "string"], "default": None},
        {"name": "amount", "type": "double"},
    ],
}

ENVELOPE_SCHEMA = {
    "type": "record",
    "name": "Envelope",
    "namespace": "cdc_orders",
    "fields": [
        {"name": "before", "type": ["null", VALUE_RECORD], "default": None},
        {"name": "after", "type": ["null", "cdc_orders.Value"], "default": None},
        {"name": "op", "type": "string"},
    ],
}

KEY_SCHEMA = {
    "type": "record",
    "name": "Key",
    "namespace": "cdc_orders",
    "fields": [{"name": "id", "type": "long"}],
}


def _topic_schema(value_schema: Dict) -> SchemaMetadataClass:
    fields = avro_schema_to_mce_fields(
        json.dumps(KEY_SCHEMA), is_key_schema=True
    ) + avro_schema_to_mce_fields(json.dumps(value_schema))
    return SchemaMetadataClass(
        schemaName=TOPIC,
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )


def _stored_path(schema: SchemaMetadataClass, column: str) -> str:
    return next(
        f.fieldPath
        for f in schema.fields
        if "[key=True]" not in f.fieldPath
        and ".after." in f.fieldPath
        and f.fieldPath.endswith(f".{column}")
    )


def _extract(topic_schema: Optional[SchemaMetadataClass]) -> List[KafkaConnectLineage]:
    connector = DebeziumSourceConnector(
        make_manifest(CONNECTOR_CONFIG, name="orders-cdc"),
        KafkaConnectSourceConfig(use_schema_resolver=True),
        KafkaConnectSourceReport(),
    )
    graph: MagicMock = mock_graph()
    graph.get_aspect.return_value = topic_schema
    resolver = StubSchemaResolver(platform="mysql", graph=graph)
    resolver.add_schema(
        SOURCE_TABLE, {"id": "BIGINT", "customer_name": "VARCHAR", "amount": "DOUBLE"}
    )
    attach_resolver(connector, resolver)
    return connector.extract_lineages()


def _downstreams(lineage: KafkaConnectLineage) -> Dict[str, str]:
    assert lineage.fine_grained_lineages
    return {
        fgl.upstreams[0].rsplit(",", 1)[-1].rstrip(")"): fgl.downstreams[0]
        for fgl in lineage.fine_grained_lineages
        if fgl.upstreams and fgl.downstreams
    }


def test_by_logical_table_router_links_table_to_routed_topic() -> None:
    lineages = _extract(_topic_schema(ENVELOPE_SCHEMA))

    assert len(lineages) == 1
    assert lineages[0].source_dataset == SOURCE_TABLE
    assert lineages[0].target_dataset == TOPIC


def test_columns_link_to_stored_after_field_paths() -> None:
    schema = _topic_schema(ENVELOPE_SCHEMA)

    downstreams = _downstreams(_extract(schema)[0])

    assert downstreams == {
        column: make_schema_field_urn(TOPIC_URN, _stored_path(schema, column))
        for column in ("id", "customer_name", "amount")
    }


def test_topic_with_mismatched_schema_gets_no_column_lineage() -> None:
    # Another table's schema registered for this topic: only "id" lines up.
    unrelated_value = {
        **VALUE_RECORD,
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "body", "type": "string"},
        ],
    }
    lineages = _extract(
        _topic_schema(
            {
                **ENVELOPE_SCHEMA,
                "fields": [{"name": "after", "type": ["null", unrelated_value]}],
            }
        )
    )

    assert lineages[0].target_dataset == TOPIC
    assert lineages[0].fine_grained_lineages is None


def test_topic_without_schema_in_datahub_keeps_column_name_paths() -> None:
    downstreams = _downstreams(_extract(None)[0])

    assert downstreams["id"] == make_schema_field_urn(TOPIC_URN, "id")
