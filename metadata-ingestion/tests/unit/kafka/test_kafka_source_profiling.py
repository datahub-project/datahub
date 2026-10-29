import io
import json
from typing import Any, Dict, List, Optional, Tuple
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
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    KafkaSchemaClass,
    SchemaMetadataClass,
)
from tests.unit.kafka.conftest import (
    KAFKA_MODULE,
    FakeConsumer,
    FakeMessage,
    KafkaClientMocks,
    SourceFactory,
    schemaless_metadata,
    warned_about,
)

TOPIC = "orders"
CONFLUENT_HEADER = bytes([0, 0, 0, 0, 1])
RECORD_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "Order",
        "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}],
    }
)
SCHEMALESS_METADATA = schemaless_metadata(TOPIC)


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


@pytest.fixture
def source(make_kafka_source: SourceFactory) -> KafkaSource:
    """Profiling source with a small sample size so batch clamping is observable."""
    return make_kafka_source(
        {"profiling": {"enabled": True, "sample_size": 4, "batch_size": 10}}
    )


def test_get_sample_messages_reads_from_every_partition(source: KafkaSource) -> None:
    """Never reads more than sample_size, and releases the assignment afterwards."""
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
    assert warned_about(source.report, TOPIC)


def test_get_sample_messages_survives_broker_errors(source: KafkaSource) -> None:
    """A broker error degrades to "no samples" plus warnings for both the
    collection and the failed unassign, never an exception out of the worker."""
    consumer = MagicMock()
    consumer.list_topics.side_effect = confluent_kafka.KafkaException("timed out")
    consumer.unassign.side_effect = RuntimeError("not assigned")

    samples = source.get_sample_messages(TOPIC, consumer=consumer)

    assert samples == []
    assert warned_about(source.report, TOPIC)
    assert warned_about(source.report, "profiling")


def test_batch_read_is_clamped_to_available_messages(source: KafkaSource) -> None:
    """Asking consume() for more than the watermarks hold would block on it."""
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

    assert consumer.requested == [2]
    assert len(samples) == 2


def test_batch_read_skips_errored_and_undecodable_messages(source: KafkaSource) -> None:
    """An errored message is dropped and reported; an undecodable value is
    counted as skipped and produces no sample at all (a metadata-only sample
    would inflate the row count without any field data)."""
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

    assert [s["offset"] for s in samples] == [2]
    assert samples[0]["a"] == "1"
    assert source.report.profiling_samples_skipped == 1
    assert warned_about(source.report, TOPIC)


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


def test_tombstone_keeps_its_key_fields(source: KafkaSource) -> None:
    """A null value is a tombstone, not a decode failure: the key is still sampled."""
    samples: List[Dict[str, Any]] = []
    msg = FakeMessage(None, key=b'{"id": 1}', offset=9)

    source._process_message_to_sample(
        msg,  # type: ignore[arg-type]
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
    )

    [sample] = samples
    assert sample["key.id"] == "1"
    assert "value" not in sample
    assert source.report.profiling_samples_skipped == 0


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
    assert warned_about(source.report, "profiling")


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
    """Producers that bypass the Confluent serializer write plain JSON even on a
    topic with a registered Avro schema; those must still be profiled."""
    decoded = source._process_message_part(
        b'{"id": 7}', "value", TOPIC, avro_schema_metadata(RECORD_SCHEMA)
    )

    assert decoded == {"id": "7"}


@pytest.mark.parametrize("schema_metadata", [None, SCHEMALESS_METADATA])
def test_bytes_without_schema_are_decoded_as_json(
    source: KafkaSource, schema_metadata: Optional[SchemaMetadataClass]
) -> None:
    """With no usable schema the payload is decoded as JSON/text rather than
    handed to the profiler as raw bytes."""
    decoded = source._process_message_part(
        b'{"id": 1}', "value", TOPIC, schema_metadata
    )

    assert decoded == {"id": "1"}


@pytest.mark.parametrize("schema_metadata", [None, SCHEMALESS_METADATA])
def test_undecodable_binary_without_schema_is_skipped(
    source: KafkaSource, schema_metadata: Optional[SchemaMetadataClass]
) -> None:
    decoded = source._process_message_part(
        b"\x8a\x8b\xff", "value", TOPIC, schema_metadata
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
    assert warned_about(source.report, TOPIC)


def test_collect_and_profile_topic_returns_profile(source: KafkaSource) -> None:
    """The per-topic worker builds its own consumer, tolerates a failing close(),
    and returns (urn, profile) for the topic."""
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


def test_profiling_is_queued_only_for_topics_with_schema_info(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    """A topic with no schema has nothing to decode against, so it is not profiled."""
    kafka_clients.set_topics(["with_schema", "schemaless"])
    kafka_clients.registry.return_value.get_subjects.return_value = [
        "with_schema-value"
    ]
    kafka_clients.registry.return_value.get_latest_version.return_value = (
        RegisteredSchema(
            schema_id=1,
            guid=None,
            schema=Schema(schema_str=RECORD_SCHEMA, schema_type="AVRO"),
            subject="with_schema-value",
            version=1,
        )
    )
    kafka_source = make_kafka_source({"profiling": {"enabled": True}})

    with patch.object(
        kafka_source, "generate_profiles_in_parallel", return_value=iter([])
    ) as profiles:
        list(kafka_source.get_workunits())

    profiles.assert_called_once()
    [tasks] = profiles.call_args.args
    assert [topic for _, topic, _ in tasks] == ["with_schema"]
    assert isinstance(tasks[0][2], SchemaMetadataClass)
