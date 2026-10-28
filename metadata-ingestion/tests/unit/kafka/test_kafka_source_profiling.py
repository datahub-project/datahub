import io
import json
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import avro.io
import avro.schema
import confluent_kafka
import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    KafkaSchemaClass,
    SchemaMetadataClass,
)

KAFKA_MODULE = "datahub.ingestion.source.kafka.kafka"
REGISTRY_MODULE = "datahub.ingestion.source.confluent_schema_registry"
TOPIC = "orders"
CONFLUENT_HEADER = bytes([0, 0, 0, 0, 1])
RECORD_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "Order",
        "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}],
    }
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


def avro_encode(schema_str: str, datum: Any) -> bytes:
    buffer = io.BytesIO()
    avro.io.DatumWriter(avro.schema.parse(schema_str)).write(
        datum, avro.io.BinaryEncoder(buffer)
    )
    return CONFLUENT_HEADER + buffer.getvalue()


def avro_schema_metadata(schema_str: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName=TOPIC,
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=KafkaSchemaClass(
            documentSchema=schema_str, documentSchemaType="AVRO"
        ),
        fields=[],
    )


# What the source builds for a topic whose fields were inferred from messages:
# schema metadata exists, but there is no registered document schema to decode with.
SCHEMALESS_METADATA = SchemaMetadataClass(
    schemaName=TOPIC,
    platform="urn:li:dataPlatform:kafka",
    version=0,
    hash="",
    platformSchema=KafkaSchemaClass(documentSchema=""),
    fields=[],
)


@pytest.fixture
def registry() -> Iterator[MagicMock]:
    with patch(f"{REGISTRY_MODULE}.SchemaRegistryClient", autospec=True) as mock:
        mock.return_value.get_subjects.return_value = []
        yield mock


@pytest.fixture
def source(registry: MagicMock) -> Iterator[KafkaSource]:
    with (
        patch(f"{KAFKA_MODULE}.confluent_kafka.Consumer", autospec=True),
        patch(f"{KAFKA_MODULE}.AdminClient", autospec=True),
    ):
        config = KafkaSourceConfig.model_validate(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "profiling": {"enabled": True, "sample_size": 4, "batch_size": 10},
            }
        )
        kafka_source = KafkaSource(config, PipelineContext(run_id="test"))
        yield kafka_source
        kafka_source.close()


def test_get_sample_messages_reads_from_every_partition(source: KafkaSource) -> None:
    messages = [
        FakeMessage(json.dumps({"amount": i}).encode(), offset=i) for i in range(6, 10)
    ]
    consumer = FakeConsumer([messages], partitions={0: (0, 10), 1: (0, 10)})

    samples = source.get_sample_messages(
        TOPIC,
        consumer=consumer,  # type: ignore[arg-type]
        schema_metadata=SCHEMALESS_METADATA,
    )

    assert samples is not None
    assert [s["offset"] for s in samples] == [6, 7, 8, 9]
    assert samples[0]["amount"] == "6"
    assert "timestamp" in samples[0]
    # Never reads more than sample_size, and releases the assignment afterwards.
    assert consumer.requested == [4]
    assert consumer.unassigned


def test_get_sample_messages_with_no_partitions_warns(source: KafkaSource) -> None:
    consumer = FakeConsumer([], partitions={})

    samples = source.get_sample_messages(
        TOPIC,
        consumer=consumer,  # type: ignore[arg-type]
        schema_metadata=SCHEMALESS_METADATA,
    )

    assert samples == []
    assert source.report.warnings


def test_get_sample_messages_survives_broker_errors(source: KafkaSource) -> None:
    consumer = MagicMock()
    consumer.list_topics.side_effect = confluent_kafka.KafkaException("timed out")
    consumer.unassign.side_effect = RuntimeError("not assigned")

    samples = source.get_sample_messages(TOPIC, consumer=consumer)

    # A broker error degrades to "no samples" with warnings, never an exception
    # out of the profiling worker.
    assert samples == []
    assert len(source.report.warnings) >= 2


def test_batch_read_is_clamped_to_available_messages(source: KafkaSource) -> None:
    messages = [FakeMessage(b'{"a": 1}', offset=i) for i in range(3)]
    consumer = FakeConsumer([messages])
    samples: List[Dict[str, Any]] = []

    source._read_messages_in_batches(
        [confluent_kafka.TopicPartition(TOPIC, 0)],
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
        consumer=consumer,  # type: ignore[arg-type]
        max_available=2,
    )

    # Asking for more than the watermarks hold would block on consume().
    assert consumer.requested == [2]
    assert len(samples) == 2


def test_batch_read_skips_errored_and_undecodable_messages(source: KafkaSource) -> None:
    consumer = FakeConsumer(
        [
            [
                FakeMessage(None, error="broker error"),
                FakeMessage(b"\x8a\x8b\xff", offset=1),  # not UTF-8, no schema
                FakeMessage(b'{"a": 1}', offset=2),
            ]
        ]
    )
    samples: List[Dict[str, Any]] = []

    source._read_messages_in_batches(
        [confluent_kafka.TopicPartition(TOPIC, 0)],
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
        consumer=consumer,  # type: ignore[arg-type]
    )

    # The errored message is dropped and reported; the undecodable one keeps its
    # metadata but contributes no synthetic value field, and is counted as skipped.
    assert [s["offset"] for s in samples] == [1, 2]
    assert "value" not in samples[0]
    assert samples[1]["a"] == "1"
    assert source.report.profiling_samples_skipped == 1
    assert source.report.warnings


def test_complex_key_fields_are_prefixed(source: KafkaSource) -> None:
    samples: List[Dict[str, Any]] = []
    msg = FakeMessage(b'{"amount": 5}', key=b'{"id": 1, "region": "eu"}', offset=3)

    source._process_message_to_sample(
        msg,  # type: ignore[arg-type]
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
    )

    [sample] = samples
    assert sample["key.id"] == "1"
    assert sample["key.region"] == "eu"
    assert sample["amount"] == "5"
    assert sample["offset"] == 3


def test_scalar_key_and_value_use_fixed_field_names(source: KafkaSource) -> None:
    samples: List[Dict[str, Any]] = []
    msg = FakeMessage(b"42", key=b'"customer-1"')

    source._process_message_to_sample(
        msg,  # type: ignore[arg-type]
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
    )

    [sample] = samples
    assert sample["key"] == "customer-1"
    assert sample["value"] == 42


def test_malformed_message_metadata_is_skipped(source: KafkaSource) -> None:
    samples: List[Dict[str, Any]] = []
    msg = FakeMessage(b'{"a": 1}', timestamp_ms=None)  # timestamp() -> None

    source._process_message_to_sample(
        msg,  # type: ignore[arg-type]
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
    )

    assert samples == []
    assert source.report.warnings


def test_avro_record_is_decoded_with_schema(source: KafkaSource) -> None:
    data = avro_encode(RECORD_SCHEMA, {"id": 1, "name": "a"})

    decoded = source._process_message_part(
        data, "value", TOPIC, avro_schema_metadata(RECORD_SCHEMA)
    )

    assert decoded == {"id": "1", "name": "a"}


def test_avro_primitive_is_returned_as_is(source: KafkaSource) -> None:
    data = avro_encode('"string"', "abc")

    decoded = source._process_message_part(
        data, "value", TOPIC, avro_schema_metadata('"string"')
    )

    assert decoded == "abc"


def test_avro_message_without_wire_header_falls_back_to_text(
    source: KafkaSource,
) -> None:
    # Producers that bypass the Confluent serializer write plain JSON even on a
    # topic with a registered Avro schema; those must still be profiled.
    decoded = source._process_message_part(
        b'{"id": 7}', "value", TOPIC, avro_schema_metadata(RECORD_SCHEMA)
    )

    assert decoded == {"id": "7"}


def test_undecodable_binary_without_schema_is_skipped(source: KafkaSource) -> None:
    decoded = source._process_message_part(
        b"\x8a\x8b\xff", "value", TOPIC, SCHEMALESS_METADATA
    )

    assert decoded is None
    assert source.report.profiling_samples_skipped == 1


def test_unexpected_decode_error_is_reported_not_raised(source: KafkaSource) -> None:
    with patch(
        f"{KAFKA_MODULE}.decode_kafka_message_value", side_effect=RuntimeError("boom")
    ):
        decoded = source._process_message_part(
            b"{}", "value", TOPIC, SCHEMALESS_METADATA
        )

    assert decoded is None
    assert source.report.profiling_samples_skipped == 1
    assert source.report.warnings


def test_collect_and_profile_topic_returns_profile(source: KafkaSource) -> None:
    worker_consumer = MagicMock()
    worker_consumer.close.side_effect = RuntimeError("already closed")
    samples = [{"offset": i, "amount": i} for i in range(3)]

    with (
        patch(f"{KAFKA_MODULE}.get_kafka_consumer", return_value=worker_consumer),
        patch.object(source, "get_sample_messages", return_value=samples),
    ):
        result = source._collect_and_profile_topic("urn:li:dataset:x", TOPIC, None)

    assert result is not None
    urn, profile = result
    assert urn == "urn:li:dataset:x"
    assert isinstance(profile, DatasetProfileClass)
    assert profile.fieldProfiles is not None
    assert [f.fieldPath for f in profile.fieldProfiles] == ["amount"]
    assert source.report.profiling_topics_profiled == 1


def test_parallel_profiling_yields_a_workunit_per_profiled_topic(
    source: KafkaSource,
) -> None:
    profile = DatasetProfileClass(timestampMillis=0)
    tasks: List[Tuple[str, str, Optional[SchemaMetadataClass]]] = [
        ("urn:li:dataset:a", "a", None),
        ("urn:li:dataset:b", "b", None),
    ]

    def collect(urn: str, topic: str, schema: Optional[SchemaMetadataClass]):  # type: ignore[no-untyped-def]
        return (urn, profile) if topic == "a" else None

    with patch.object(source, "_collect_and_profile_topic", side_effect=collect):
        workunits = list(source.generate_profiles_in_parallel(tasks))

    assert len(workunits) == 1
    assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
    assert workunits[0].metadata.entityUrn == "urn:li:dataset:a"
    assert workunits[0].metadata.aspect is profile
    assert list(source.generate_profiles_in_parallel([])) == []


@patch(f"{KAFKA_MODULE}.AdminClient", autospec=True)
@patch(f"{KAFKA_MODULE}.confluent_kafka.Consumer", autospec=True)
def test_profiling_is_queued_only_for_topics_with_schema_info(
    consumer: MagicMock, admin: MagicMock, registry: MagicMock
) -> None:
    metadata = MagicMock()
    metadata.topics = {"with_schema": None, "schemaless": None}
    consumer.return_value.list_topics.return_value = metadata
    registry.return_value.get_subjects.return_value = ["with_schema-value"]
    registry.return_value.get_latest_version.return_value = RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=Schema(schema_str=RECORD_SCHEMA, schema_type="AVRO"),
        subject="with_schema-value",
        version=1,
    )
    kafka_source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "profiling": {"enabled": True},
        },
        PipelineContext(run_id="test"),
    )

    with patch.object(
        kafka_source, "generate_profiles_in_parallel", return_value=iter([])
    ) as profiles:
        list(kafka_source.get_workunits())

    profiles.assert_called_once()
    [tasks] = profiles.call_args.args
    # A topic with no schema has nothing to decode against, so it is not profiled.
    assert [topic for _, topic, _ in tasks] == ["with_schema"]
    assert isinstance(tasks[0][2], SchemaMetadataClass)
