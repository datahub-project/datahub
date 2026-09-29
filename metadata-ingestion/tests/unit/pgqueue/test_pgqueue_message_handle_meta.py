"""Round-trip for :class:`~datahub.pgqueue.repository.PgQueueMessageHandle` metadata dicts."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from datahub.pgqueue.consumer import (
    PgQueueConsumedRecord,
    build_pg_queue_event_meta,
)
from datahub.pgqueue.repository import PgQueueMessageHandle, PgQueueReceivedMessage


def test_handle_meta_dict_round_trip() -> None:
    original = PgQueueMessageHandle(
        id=42,
        enqueued_at=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
        topic_id=7,
        partition_id=3,
        enqueue_seq=99,
    )
    restored = PgQueueMessageHandle.from_meta_dict(original.to_meta_dict())
    assert restored == original


def test_build_pg_queue_event_meta_shape() -> None:
    handle = PgQueueMessageHandle(
        id=1,
        enqueued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        topic_id=1,
        partition_id=0,
        enqueue_seq=5,
    )
    raw = PgQueueReceivedMessage(
        handle=handle,
        priority=0,
        payload=b"{}",
        content_type=None,
        payload_compression=0,
        headers=(),
        routing_key="urn:li:corpuser:test",
        lock_owner="test:0",
    )
    rec = PgQueueConsumedRecord(
        route_key="mcl",
        topic_name="MetadataChangeLog_Versioned_v1",
        raw=raw,
        record=MagicMock(),
    )
    meta = build_pg_queue_event_meta(rec)
    assert meta["pg_queue"]["topic"] == "MetadataChangeLog_Versioned_v1"
    assert meta["pg_queue"]["partition"] == 0
    assert meta["pg_queue"]["enqueue_seq"] == 5
    assert meta["pg_queue"]["routing_key"] == "urn:li:corpuser:test"
    round_handle = PgQueueMessageHandle.from_meta_dict(meta["pg_queue"]["handle"])
    assert round_handle == handle
