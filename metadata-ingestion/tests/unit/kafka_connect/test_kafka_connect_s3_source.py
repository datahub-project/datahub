"""Tests for the S3 source connector (S3 objects -> Kafka topics)."""

from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import Mock

from datahub.ingestion.source.kafka_connect.common import (
    SOURCE,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    S3_SOURCE_CONNECTOR_CLASS,
    ConfluentS3SourceConnector,
)


def _connector(
    config: Dict[str, str],
    topic_names: Optional[List[str]] = None,
    connector_class: str = "S3Source",
) -> ConfluentS3SourceConnector:
    manifest = ConnectorManifest(
        name="s3-source",
        type=SOURCE,
        config={"connector.class": connector_class, **config},
        tasks=[],
        topic_names=topic_names or [],
    )
    return ConfluentS3SourceConnector(
        manifest,
        Mock(spec=KafkaConnectSourceConfig),
        Mock(spec=KafkaConnectSourceReport),
    )


def _edges(connector: ConfluentS3SourceConnector) -> Set[Tuple[str, str]]:
    return {
        (str(lineage.source_dataset), lineage.target_dataset)
        for lineage in connector.extract_lineages()
    }


def test_regex_pinned_to_a_folder_maps_that_folder_to_the_topic() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": r"orders:raw/orders/.*\.json,payments:^raw/pay\-ments/.*",
        }
    )

    assert _edges(connector) == {
        ("my-bucket/raw/orders", "orders"),
        ("my-bucket/raw/pay-ments", "payments"),
    }


def test_regex_not_pinned_to_a_folder_is_skipped_and_reported() -> None:
    # `.*orders.*` can match objects anywhere in the bucket and `raw/orders.*` also
    # matches the sibling folder raw/orders_v2/, so neither names an S3 dataset.
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": "orders:.*orders.*,archive:raw/orders.*,ok:raw/ok/.*",
        }
    )

    assert _edges(connector) == {("my-bucket/raw/ok", "ok")}
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_restore_backup_mode_uses_the_sink_layout_for_reported_topics() -> None:
    connector = _connector(
        {"s3.bucket.name": "my-bucket", "topics.dir": "exports"},
        topic_names=["orders"],
        connector_class=S3_SOURCE_CONNECTOR_CLASS,
    )

    assert _edges(connector) == {("my-bucket/exports/orders", "orders")}


def test_generic_mode_on_self_hosted_ignores_reported_topics() -> None:
    connector = _connector(
        {"s3.bucket.name": "my-bucket", "mode": "GENERIC"},
        topic_names=["orders"],
        connector_class=S3_SOURCE_CONNECTOR_CLASS,
    )

    assert _edges(connector) == set()
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_topics_from_config_come_from_topic_regex_list() -> None:
    connector = _connector({"topic.regex.list": "orders:a/.*,orders:b/.*,pay:c/.*"})

    assert connector.get_topics_from_config() == ["orders", "pay"]


def test_missing_bucket_warns_and_returns_empty() -> None:
    connector = _connector({"topic.regex.list": "orders:raw/orders/.*"})

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_flow_property_bag_masks_credentials() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "aws.access.key.id": "AKIA",
            "aws.secret.access.key": "secret",
            "aws.session.token": "session",
            "s3.sse.customer.key": "sse-key",
            "s3.sse.customer.key.md5": "md5",
            "s3.proxy.password": "proxy-pw",
        }
    )

    bag = connector.extract_flow_property_bag()

    assert "aws.access.key.id" not in bag
    assert "aws.secret.access.key" not in bag
    assert "aws.session.token" not in bag
    assert "s3.sse.customer.key" not in bag
    assert "s3.sse.customer.key.md5" not in bag
    assert "s3.proxy.password" not in bag
    assert bag["s3.bucket.name"] == "my-bucket"
