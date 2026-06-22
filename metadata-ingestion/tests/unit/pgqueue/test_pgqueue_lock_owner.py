"""Tests for lock_owner format alignment between Python and Java.

Java (PgQueuePollWorker) produces: ``{consumerGroupId}:{UUID}``
Python (consumer_lock_owner) must match that two-part format.
"""

from __future__ import annotations

import re
from unittest.mock import patch

from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueConsumerConfig
from datahub.pgqueue.connection import consumer_lock_owner
from datahub.pgqueue.consumer import DatahubPgQueueConsumer

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def test_lock_owner_two_part_format() -> None:
    """Format must be ``{group}:{uuid}`` — no extra segments."""
    owner = consumer_lock_owner("my-group")
    group, sep, uuid_part = owner.partition(":")
    assert sep == ":", f"Expected colon separator in {owner}"
    assert group == "my-group"
    assert UUID_RE.match(uuid_part), f"UUID portion is not valid: {uuid_part}"


def test_lock_owner_unique_per_call() -> None:
    a = consumer_lock_owner("g")
    b = consumer_lock_owner("g")
    assert a != b, "Each call must produce a distinct lock_owner"


def test_lock_owner_preserves_group_name() -> None:
    owner = consumer_lock_owner("generic-mae-consumer-job-client")
    assert owner.startswith("generic-mae-consumer-job-client:")


def test_consumer_config_no_lock_owner_suffix() -> None:
    """lock_owner_suffix was removed — config must not accept it."""
    cfg = PgQueueConsumerConfig(
        queue=PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="db",
            username="u",
            password="secret",
        ),
    )
    assert not hasattr(cfg, "lock_owner_suffix")


def test_consumer_lock_owner_matches_java_format() -> None:
    """Consumer instance must produce a lock_owner in Java-aligned format."""
    queue_cfg = PgQueueConnectionConfig(
        host_port="localhost:5432",
        database="db",
        username="u",
        password="secret",
    )
    cfg = PgQueueConsumerConfig(queue=queue_cfg, consumer_group="test-group")
    with (
        patch("datahub.pgqueue.consumer.SchemaRegistryClient"),
        patch("datahub.pgqueue.consumer.create_pgqueue_connection"),
    ):
        consumer = DatahubPgQueueConsumer(cfg)
        owner = consumer.lock_owner()

    assert owner.startswith("test-group:")
    uuid_str = owner[len("test-group:") :]
    assert UUID_RE.match(uuid_str), f"Expected UUID after group prefix, got: {uuid_str}"
