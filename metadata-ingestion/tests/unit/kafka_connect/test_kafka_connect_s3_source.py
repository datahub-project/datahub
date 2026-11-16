"""Tests for the S3 source connector (S3 objects -> Kafka topics)."""

from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import Mock

import pytest

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


def test_flow_property_bag_only_shows_known_non_secret_properties() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": "orders:raw/orders/.*",
            "aws.secret.access.key": "secret",
            "kafka.api.secret": "secret",
            "some.vendor.token": "secret",
        }
    )

    assert connector.extract_flow_property_bag() == {
        "connector.class": "S3Source",
        "s3.bucket.name": "my-bucket",
        "topic.regex.list": "orders:raw/orders/.*",
    }


def test_fully_literal_regex_is_one_object_and_skipped() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": "orders:raw/orders/part-0\\.json",
        }
    )

    assert _edges(connector) == set()
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({}, id="no topic.regex.list"),
        pytest.param({"topic.regex.list": "orders,:raw/.*"}, id="malformed pairs"),
        pytest.param({"mode": "SOMETHING_ELSE"}, id="unknown mode"),
    ],
)
def test_unusable_generic_config_warns_and_emits_nothing(
    config: Dict[str, str],
) -> None:
    connector = _connector({"s3.bucket.name": "my-bucket", **config})

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "config,topic_names",
    [
        pytest.param(
            {
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "(.*)",
                "transforms.route.replacement": "restored-$1",
            },
            ["restored-orders"],
            id="topic-routing transform",
        ),
        pytest.param({"topics.dir": " "}, ["orders"], id="blank topics.dir"),
        pytest.param({}, [], id="no reported topics"),
    ],
)
def test_restore_backup_without_a_known_folder_warns_and_emits_nothing(
    config: Dict[str, str], topic_names: List[str]
) -> None:
    connector = _connector(
        {"s3.bucket.name": "my-bucket", **config},
        topic_names=topic_names,
        connector_class=S3_SOURCE_CONNECTOR_CLASS,
    )

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]


def test_generic_mode_with_a_topic_routing_transform_warns_and_emits_nothing() -> None:
    connector = _connector(
        {
            "s3.bucket.name": "my-bucket",
            "topic.regex.list": "orders:raw/orders/.*",
            "transforms": "route",
            "transforms.route.type": "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            "transforms.route.regex": "(.*)",
            "transforms.route.replacement": "landing-$1",
        }
    )

    assert connector.extract_lineages() == []
    connector.report.warning.assert_called_once()  # type: ignore[attr-defined]
