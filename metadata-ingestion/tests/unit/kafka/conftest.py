"""Shared builders for the Kafka source unit tests.

The three network clients the source constructs (consumer, admin client, schema
registry) are replaced with autospec'd mocks via `kafka_clients`; sources are
built through `make_kafka_source` so every one is closed at teardown. Messages
and consumers are hand-written fakes rather than mocks so each test scripts
exactly what the broker hands back.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Protocol, Tuple, Type, TypeVar
from unittest.mock import MagicMock, patch

import confluent_kafka
import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass

KAFKA_MODULE = "datahub.ingestion.source.kafka.kafka"
REGISTRY_MODULE = "datahub.ingestion.source.confluent_schema_registry"
INFERENCE_MODULE = "datahub.ingestion.source.kafka.kafka_schema_inference"
BOOTSTRAP = "localhost:9092"

T = TypeVar("T")


class SourceFactory(Protocol):
    def __call__(self, overrides: Optional[Dict[str, Any]] = None) -> KafkaSource: ...


@dataclass
class KafkaClientMocks:
    """The patched client classes; `.return_value` on each is the instance."""

    consumer: MagicMock
    admin: MagicMock
    registry: MagicMock

    def set_topics(self, names: List[str]) -> None:
        metadata = MagicMock()
        metadata.topics = dict.fromkeys(names)
        self.consumer.return_value.list_topics.return_value = metadata


@pytest.fixture
def kafka_clients() -> Iterator[KafkaClientMocks]:
    """Patch every network client the source constructs: consumer, admin, registry."""
    with (
        patch(f"{KAFKA_MODULE}.confluent_kafka.Consumer", autospec=True) as consumer,
        patch(f"{KAFKA_MODULE}.AdminClient", autospec=True) as admin,
        patch(f"{REGISTRY_MODULE}.SchemaRegistryClient", autospec=True) as registry,
    ):
        registry.return_value.get_subjects.return_value = []
        yield KafkaClientMocks(consumer=consumer, admin=admin, registry=registry)


@pytest.fixture
def make_kafka_source(kafka_clients: KafkaClientMocks) -> Iterator[SourceFactory]:
    """Build a KafkaSource from recipe overrides; every source built is closed."""
    sources: List[KafkaSource] = []

    def _make(overrides: Optional[Dict[str, Any]] = None) -> KafkaSource:
        recipe: Dict[str, Any] = {"connection": {"bootstrap": BOOTSTRAP}}
        recipe.update(overrides or {})
        source = KafkaSource.create(recipe, PipelineContext(run_id="test"))
        sources.append(source)
        return source

    yield _make
    for source in sources:
        source.close()


@pytest.fixture
def kafka_source(make_kafka_source: SourceFactory) -> KafkaSource:
    """A source with profiling enabled and a 100-message sample/batch size."""
    return make_kafka_source(
        {"profiling": {"enabled": True, "sample_size": 100, "batch_size": 100}}
    )


def emitted_aspects(source: KafkaSource, aspect_type: Type[T]) -> List[T]:
    """Run get_workunits() and return the MCP aspects of one type, in emission order."""
    return [
        wu.metadata.aspect
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def warned_about(report: SourceReport, key: str) -> bool:
    """Whether any warning names `key` in its context.

    Matches on the stable context key (a topic name or "profiling") rather than
    warning wording, count, or order. Substring match because a warning raised
    with `exc=` carries the exception text in the same context entry.
    """
    return any(key in ctx for w in report.warnings for ctx in w.context)


def schemaless_metadata(topic: str) -> SchemaMetadataClass:
    """What the source builds for a topic whose fields were inferred from messages.

    Schema metadata exists, but there is no registered document schema to decode with.
    """
    return SchemaMetadataClass(
        schemaName=topic,
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=KafkaSchemaClass(documentSchema=""),
        fields=[],
    )


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message (which cannot be instantiated)."""

    def __init__(
        self,
        value: Optional[bytes],
        key: Optional[bytes] = None,
        offset: int = 0,
        error: Optional[str] = None,
        timestamp_ms: Optional[int] = 1_700_000_000_000,
    ) -> None:
        self._value = value
        self._key = key
        self._offset = offset
        self._error = error
        self._timestamp_ms = timestamp_ms

    def key(self) -> Optional[bytes]:
        return self._key

    def value(self) -> Optional[bytes]:
        return self._value

    def offset(self) -> int:
        return self._offset

    def error(self) -> Optional[str]:
        return self._error

    def timestamp(self) -> Optional[Tuple[int, int]]:
        if self._timestamp_ms is None:
            return None
        return (confluent_kafka.TIMESTAMP_CREATE_TIME, self._timestamp_ms)


class FakeConsumer:
    """Serves scripted batches from consume() and records what was requested."""

    def __init__(
        self,
        batches: List[List[FakeMessage]],
        partitions: Optional[Dict[int, Tuple[int, int]]] = None,
    ) -> None:
        self._batches = list(batches)
        self._watermarks = partitions or {}
        self.requested: List[int] = []
        self.unassigned = False

    def list_topics(self, topic: str, timeout: int) -> MagicMock:
        topic_meta = MagicMock()
        topic_meta.partitions = {p: MagicMock() for p in self._watermarks}
        metadata = MagicMock()
        metadata.topics = {topic: topic_meta}
        return metadata

    def get_watermark_offsets(
        self, partition: confluent_kafka.TopicPartition
    ) -> Tuple[int, int]:
        return self._watermarks[partition.partition]

    def assign(self, partitions: List[confluent_kafka.TopicPartition]) -> None:
        pass

    def consume(self, num_messages: int, timeout: float) -> List[FakeMessage]:
        self.requested.append(num_messages)
        return self._batches.pop(0)[:num_messages] if self._batches else []

    def unassign(self) -> None:
        self.unassigned = True


class ScriptedConsumer:
    """Yields a fixed sequence of poll() results (then None) and records its use.

    Covers both poll-driven loops in the source: the stratified sampler
    (assign/seek/poll) and schema inference (subscribe/poll/close).
    """

    def __init__(
        self, messages: List[object], subscribe_error: Optional[Exception] = None
    ) -> None:
        self._messages = list(messages)
        self._subscribe_error = subscribe_error
        self.poll_timeouts: List[float] = []
        self.closed = False

    def subscribe(self, topics: List[str]) -> None:
        if self._subscribe_error is not None:
            raise self._subscribe_error

    def assign(self, partitions: List[confluent_kafka.TopicPartition]) -> None:
        pass

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        pass

    def poll(self, timeout: float) -> object:
        self.poll_timeouts.append(timeout)
        return self._messages.pop(0) if self._messages else None

    def close(self) -> None:
        self.closed = True


class OffsetRecordingConsumer:
    """Records assign/seek offsets and yields no messages so read loops exit."""

    def __init__(self) -> None:
        self.assigned_offsets: List[int] = []
        self.seek_offsets: List[int] = []

    def assign(self, partitions: List[confluent_kafka.TopicPartition]) -> None:
        self.assigned_offsets = [p.offset for p in partitions]

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        self.seek_offsets.append(partition.offset)

    def consume(self, num_messages: int, timeout: float) -> List[FakeMessage]:
        return []

    def poll(self, timeout: float) -> None:
        return None
