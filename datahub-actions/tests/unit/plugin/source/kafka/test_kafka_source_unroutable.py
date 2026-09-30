# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unroutable PlatformEvent messages must still advance committed offsets.

PlatformEvent_v1 is a multiplexed topic. handle_pe only routes
entityChangeEvent / relationshipChangeEvent; any other event name yields no
envelope, so the pipeline never acks the message. In async-commit mode
(enable.auto.offset.store=False) its offset is then never stored or committed,
and a partition whose tail is unroutable reports consumer lag forever -- which
the commit-progress health check flags as a zombie, recycling healthy pods
(ZD-8461).
"""

from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)

TOPIC = "PlatformEvent_v1"

ECE_VALUE = (
    b'{"entityUrn": "urn:li:dataset:abc","entityType": "dataset",'
    b'"category": "TAG","operation": "ADD","modifier": "urn:li:tag:PII",'
    b'"auditStamp": {"actor": "urn:li:corpuser:jdoe","time": 1649953100653},'
    b'"version":0}'
)


class _FakeMessage:
    """Minimal confluent-kafka Message stand-in (TestMessage lacks error())."""

    def __init__(self, value: Dict, offset: int, partition: int = 3):
        self._value = value
        self._offset = offset
        self._partition = partition

    def value(self) -> Dict:
        return self._value

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Optional[Any]:
        return None


def _pe_message(name: str, offset: int, value: bytes = b"{}") -> _FakeMessage:
    return _FakeMessage(
        {
            "name": name,
            "payload": {"contentType": "application/json", "value": value},
        },
        offset=offset,
    )


def _make_source(async_commit_enabled: bool) -> KafkaEventSource:
    config = KafkaEventSourceConfig(
        async_commit_enabled=async_commit_enabled,
        topic_routes={"pe": TOPIC},
    )
    ctx = PipelineContext(pipeline_name="test-unroutable", graph=None)
    with patch(
        "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
    ):
        with patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
        ):
            return KafkaEventSource(config, ctx)


def _drive_events(source: KafkaEventSource, *messages: _FakeMessage) -> EventEnvelope:
    """Feed messages through one pass of the events() loop; returns the first
    envelope it yields (the last message supplied must be routable)."""
    source.consumer.poll = MagicMock(side_effect=list(messages))
    return next(iter(source.events()))


def test_handle_pe_yields_nothing_for_unroutable_name() -> None:
    msg = _pe_message("notificationRequestEvent", offset=100)
    assert list(KafkaEventSource.handle_pe(msg)) == []


def test_unroutable_pe_offset_is_stored_in_async_mode() -> None:
    source = _make_source(async_commit_enabled=True)
    unroutable = _pe_message("notificationRequestEvent", offset=100)
    routable = _pe_message("entityChangeEvent", offset=101, value=ECE_VALUE)

    envelope = _drive_events(source, unroutable, routable)

    assert envelope.event_type == "EntityChangeEvent_v1"
    source.consumer.store_offsets.assert_called_once_with(
        offsets=[TopicPartition(TOPIC, 3, 101)]
    )


def test_routable_pe_is_not_stored_at_consume_time() -> None:
    """Routable events are acked (exactly-once) by the pipeline, never on consume."""
    source = _make_source(async_commit_enabled=True)
    routable = _pe_message("entityChangeEvent", offset=101, value=ECE_VALUE)

    envelope = _drive_events(source, routable)

    assert envelope.event_type == "EntityChangeEvent_v1"
    source.consumer.store_offsets.assert_not_called()


def test_unroutable_pe_is_not_stored_in_sync_mode() -> None:
    """Sync-commit mode keeps enable.auto.offset.store=true, so librdkafka
    already tracks consumed positions; storing again would be redundant."""
    source = _make_source(async_commit_enabled=False)
    unroutable = _pe_message("notificationRequestEvent", offset=100)
    routable = _pe_message("entityChangeEvent", offset=101, value=ECE_VALUE)

    _drive_events(source, unroutable, routable)

    source.consumer.store_offsets.assert_not_called()


def _stored_offsets(source: KafkaEventSource) -> list:
    return [
        call.kwargs["offsets"][0].offset
        for call in source.consumer.store_offsets.call_args_list
    ]


def test_unroutable_store_deferred_while_routable_in_flight() -> None:
    """Unroutable messages must not commit past an earlier routable event
    whose work is still in flight; their store lands with that event's ack.
    Uses a RUN of unroutable messages -- the release must trigger on acks
    passing the routable watermark below the run, not on reaching the run's
    own (never-acked) offsets."""
    source = _make_source(async_commit_enabled=True)
    routable_100 = _pe_message("entityChangeEvent", offset=100, value=ECE_VALUE)
    unroutable_101 = _pe_message("notificationRequestEvent", offset=101)
    unroutable_102 = _pe_message("notificationRequestEvent", offset=102)
    routable_103 = _pe_message("entityChangeEvent", offset=103, value=ECE_VALUE)

    source.consumer.poll = MagicMock(
        side_effect=[routable_100, unroutable_101, unroutable_102, routable_103]
    )
    gen = iter(source.events())
    envelope_100 = next(gen)  # offset 100 handed to the pipeline (in flight)
    envelope_103 = next(gen)  # consumes 101+102 (deferred), yields 103

    source.consumer.store_offsets.assert_not_called()

    source.ack(envelope_100, processed=True)  # 100 completes...
    # ...which releases the deferred skip straight through 102.
    assert _stored_offsets(source) == [103]

    source.ack(envelope_103, processed=True)
    assert _stored_offsets(source) == [103, 104]


def test_late_ack_does_not_rewind_unroutable_store() -> None:
    """A stale/duplicate ack below an already-stored offset must be dropped,
    or it would re-pin the partition the unroutable store just advanced."""
    source = _make_source(async_commit_enabled=True)
    unroutable_101 = _pe_message("notificationRequestEvent", offset=101)
    routable_102 = _pe_message("entityChangeEvent", offset=102, value=ECE_VALUE)

    _drive_events(source, unroutable_101, routable_102)  # idle -> stores 102

    stale = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=MagicMock(),
        meta={"kafka": {"topic": TOPIC, "partition": 3, "offset": 100}},
    )
    source.ack(stale, processed=True)

    assert _stored_offsets(source) == [102]
