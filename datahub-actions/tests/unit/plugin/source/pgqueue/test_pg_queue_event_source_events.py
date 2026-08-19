"""Tests for PgQueueEventSource.events() and _records_to_envelopes()."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Union
from unittest.mock import MagicMock, patch

import pytest

from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeLogClass,
    MetadataChangeProposalClass,
)
from datahub.pgqueue.consumer import PgQueueConsumedRecord
from datahub.pgqueue.repository import PgQueueMessageHandle, PgQueueReceivedMessage
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    RELATIONSHIP_CHANGE_EVENT_V1_TYPE,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.pgqueue.pg_queue_event_source import (
    PgQueueEventSource,
    PgQueueEventSourceConfig,
)


def _make_source() -> PgQueueEventSource:
    cfg = PgQueueEventSourceConfig(
        queue={
            "host_port": "localhost:5432",
            "database": "datahub",
            "username": "datahub",
            "password": "datahub",
        }
    )
    ctx = PipelineContext(pipeline_name="test-events", graph=None)
    with (
        patch("datahub.pgqueue.consumer.SchemaRegistryClient"),
        patch("datahub.pgqueue.consumer.create_pgqueue_connection"),
    ):
        return PgQueueEventSource(cfg, ctx)


def _make_handle(msg_id: int = 1) -> PgQueueMessageHandle:
    return PgQueueMessageHandle(
        id=msg_id,
        enqueued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        topic_id=1,
        partition_id=0,
        enqueue_seq=msg_id,
    )


def _make_raw(handle: PgQueueMessageHandle) -> PgQueueReceivedMessage:
    return PgQueueReceivedMessage(
        handle=handle,
        priority=0,
        payload=b"",
        content_type=None,
        payload_compression=0,
        headers=(),
        routing_key="test",
        lock_owner="test-owner",
    )


RecordType = Union[
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    MetadataChangeLogClass,
    Dict[str, Any],
]


def _make_consumed_record(
    record: RecordType,
    topic_name: str,
    handle: PgQueueMessageHandle,
    route_key: str = "mcl",
) -> PgQueueConsumedRecord:
    return PgQueueConsumedRecord(
        route_key=route_key,
        topic_name=topic_name,
        raw=_make_raw(handle),
        record=record,
    )


def test_events_raises_when_consumer_is_none() -> None:
    source = _make_source()
    source._consumer = None

    with pytest.raises(RuntimeError, match="not initialized"):
        next(iter(source.events()))


def test_records_to_envelopes_mcl_yields_mcl_event() -> None:
    source = _make_source()
    mcl = MetadataChangeLogClass(
        entityType="dataset",
        changeType="UPSERT",
        aspectName="status",
    )
    rec = _make_consumed_record(
        record=mcl,
        topic_name="MetadataChangeLog_Versioned_v1",
        handle=_make_handle(),
    )

    envelopes = list(source._records_to_envelopes(rec))

    assert len(envelopes) == 1
    assert envelopes[0].event_type == METADATA_CHANGE_LOG_EVENT_V1_TYPE


def test_records_to_envelopes_entity_change_event() -> None:
    source = _make_source()

    ece_payload = {
        "value": (
            '{"entityUrn":"urn:li:dataset:1",'
            '"entityType":"dataset",'
            '"category":"TAG",'
            '"operation":"ADD",'
            '"modifier":"urn:li:tag:pii",'
            '"parameters":{},'
            '"auditStamp":{"time":1700000000000,"actor":"urn:li:corpuser:datahub"},'
            '"version":0}'
        ),
        "contentType": "JSON",
    }
    platform_event = {
        "name": "entityChangeEvent",
        "payload": ece_payload,
    }
    rec = _make_consumed_record(
        record=platform_event,
        topic_name="PlatformEvent_v1",
        handle=_make_handle(),
        route_key="pe",
    )

    envelopes = list(source._records_to_envelopes(rec))

    assert len(envelopes) == 1
    assert envelopes[0].event_type == ENTITY_CHANGE_EVENT_V1_TYPE


def test_records_to_envelopes_relationship_change_event() -> None:
    source = _make_source()

    rce_payload = {
        "value": (
            '{"sourceUrn":"urn:li:dataset:1",'
            '"destinationUrn":"urn:li:dataset:2",'
            '"relationshipType":"DownstreamOf",'
            '"category":"TAG",'
            '"operation":"ADD",'
            '"parameters":{},'
            '"auditStamp":{"time":1700000000000,"actor":"urn:li:corpuser:datahub"}}'
        ),
        "contentType": "JSON",
    }
    platform_event = {
        "name": "relationshipChangeEvent",
        "payload": rce_payload,
    }
    rec = _make_consumed_record(
        record=platform_event,
        topic_name="PlatformEvent_v1",
        handle=_make_handle(),
        route_key="pe",
    )

    envelopes = list(source._records_to_envelopes(rec))

    assert len(envelopes) == 1
    assert envelopes[0].event_type == RELATIONSHIP_CHANGE_EVENT_V1_TYPE


def test_records_to_envelopes_unexpected_type_yields_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _make_source()
    mcp = MetadataChangeProposalClass(entityType="dataset", changeType="UPSERT")
    rec = _make_consumed_record(
        record=mcp,
        topic_name="Unknown_v1",
        handle=_make_handle(),
    )

    envelopes = list(source._records_to_envelopes(rec))

    assert envelopes == []
    assert "Unexpected pgQueue payload type" in caplog.text


def test_close_sets_running_false_and_closes_consumer() -> None:
    source = _make_source()
    mock_consumer = MagicMock()
    source._consumer = mock_consumer
    source.running = True

    source.close()

    assert source.running is False
    mock_consumer.close.assert_called_once()
    assert source._consumer is None
