"""Tests for :meth:`DatahubPgQueueConsumer.poll_route_keys`."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueConsumerConfig
from datahub.pgqueue.consumer import DatahubPgQueueConsumer


def test_poll_route_keys_delegates_to_poll_route_key() -> None:
    queue_cfg = PgQueueConnectionConfig(
        host_port="localhost:5432",
        database="db",
        username="u",
        password="secret",
    )
    cfg = PgQueueConsumerConfig(queue=queue_cfg)
    side_a = [MagicMock(name="a")]
    side_b = [MagicMock(name="b")]
    with (
        patch("datahub.pgqueue.consumer.SchemaRegistryClient"),
        patch("datahub.pgqueue.consumer.create_pgqueue_connection"),
        patch.object(
            DatahubPgQueueConsumer,
            "poll_route_key",
            side_effect=[side_a, side_b],
        ) as poll_mock,
    ):
        consumer = DatahubPgQueueConsumer(cfg)
        out = consumer.poll_route_keys(["mcp", "mce"], max_messages=7)

    assert out == side_a + side_b
    assert poll_mock.call_args_list == [
        call("mcp", max_messages=7, partition_ids=None),
        call("mce", max_messages=7, partition_ids=None),
    ]
