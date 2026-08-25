"""Tests for the S3 source connector (S3 objects -> Kafka topics)."""

from typing import Dict, List, Optional
from unittest.mock import Mock

from datahub.ingestion.source.kafka_connect.common import (
    SOURCE,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    ConfluentS3SourceConnector,
)


def _connector(
    config: Dict[str, str],
    topic_names: Optional[List[str]] = None,
) -> ConfluentS3SourceConnector:
    manifest = ConnectorManifest(
        name="s3-source",
        type=SOURCE,
        config={"connector.class": "S3Source", **config},
        tasks=[],
        topic_names=topic_names or [],
    )
    return ConfluentS3SourceConnector(
        manifest,
        Mock(spec=KafkaConnectSourceConfig),
        Mock(spec=KafkaConnectSourceReport),
    )


def test_lineage_from_topic_regex_list() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": "orders:.*orders.*,payments:.*payments.*",
        }
    )

    lineages = connector.extract_lineages()

    edges = {(lineage.source_dataset, lineage.target_dataset) for lineage in lineages}
    assert edges == {
        ("my-bucket/topics/orders", "orders"),
        ("my-bucket/topics/payments", "payments"),
    }
    assert all(lineage.source_platform == "s3" for lineage in lineages)
    assert all(lineage.target_platform == "kafka" for lineage in lineages)


def test_topics_dir_override() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topics.dir": "exports",
            "topic.regex.list": "orders:.*",
        }
    )

    lineages = connector.extract_lineages()

    assert len(lineages) == 1
    assert lineages[0].source_dataset == "my-bucket/exports/orders"


def test_single_topic_field() -> None:
    connector = _connector({"s3.bucket.name": "my-bucket", "topic": "events"})

    topics = connector.get_topics_from_config()

    assert topics == ["events"]


def test_falls_back_to_reported_topics_not_cluster() -> None:
    # No topic config -> use the connector's own reported topics only, so a
    # source can't fabricate lineage for unrelated cluster topics.
    connector = _connector(
        {"s3.bucket.name": "my-bucket"}, topic_names=["produced-topic"]
    )

    lineages = connector.extract_lineages()

    assert len(lineages) == 1
    assert lineages[0].source_dataset == "my-bucket/topics/produced-topic"
    assert lineages[0].target_dataset == "produced-topic"


def test_missing_bucket_warns_and_returns_empty() -> None:
    connector = _connector({"topic.regex.list": "orders:.*"})

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_no_topics_warns_and_returns_empty() -> None:
    connector = _connector({"s3.bucket.name": "my-bucket"})

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_flow_property_bag_masks_credentials() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "aws.access.key.id": "AKIA",
            "aws.secret.access.key": "secret",
        }
    )

    bag = connector.extract_flow_property_bag()

    assert "aws.access.key.id" not in bag
    assert "aws.secret.access.key" not in bag
    assert bag["s3.bucket.name"] == "my-bucket"
