"""Batch ack behavior for pg_queue event source."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from datahub.pgqueue.repository import PgQueueMessageHandle
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.pgqueue.pg_queue_event_source import (
    PgQueueEventSource,
    PgQueueEventSourceConfig,
)


def _envelope_with_handle(handle_id: int) -> EventEnvelope:
    handle = PgQueueMessageHandle(
        id=handle_id,
        enqueued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        topic_id=1,
        partition_id=0,
        enqueue_seq=handle_id,
    )
    meta = {
        "pg_queue": {
            "topic": "MetadataChangeLog_Versioned_v1",
            "partition": 0,
            "enqueue_seq": handle_id,
            "handle": handle.to_meta_dict(),
        }
    }
    return EventEnvelope(event_type="test", event=MagicMock(), meta=meta)


def _make_source() -> PgQueueEventSource:
    cfg = PgQueueEventSourceConfig(
        queue={
            "host_port": "localhost:5432",
            "database": "datahub",
            "username": "datahub",
            "password": "datahub",
        }
    )
    ctx = PipelineContext(pipeline_name="test-pg-ack", graph=None)
    with (
        patch("datahub.pgqueue.consumer.SchemaRegistryClient"),
        patch("datahub.pgqueue.consumer.create_pgqueue_connection"),
    ):
        return PgQueueEventSource(cfg, ctx)


def test_ack_many_calls_consumer_ack_once_with_all_handles() -> None:
    source = _make_source()
    mock_consumer = MagicMock()
    source._consumer = mock_consumer

    ev1 = _envelope_with_handle(10)
    ev2 = _envelope_with_handle(11)
    source.ack_many([ev1, ev2], processed=True)

    mock_consumer.ack.assert_called_once()
    handles_arg = mock_consumer.ack.call_args[0][0]
    assert len(handles_arg) == 2
    assert handles_arg[0].id == 10
    assert handles_arg[1].id == 11


def test_ack_many_skipped_when_processed_false() -> None:
    source = _make_source()
    mock_consumer = MagicMock()
    source._consumer = mock_consumer

    source.ack_many([_envelope_with_handle(1)], processed=False)
    mock_consumer.ack.assert_not_called()


def test_ack_delegates_to_ack_many() -> None:
    source = _make_source()
    mock_consumer = MagicMock()
    source._consumer = mock_consumer

    ev = _envelope_with_handle(99)
    source.ack(ev, processed=True)
    mock_consumer.ack.assert_called_once()
    assert len(mock_consumer.ack.call_args[0][0]) == 1
