from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

import confluent_kafka

from datahub.ingestion.source.kafka.kafka import (
    _full_offset,
    _latest_offset,
    _random_offset,
)
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass
from tests.unit.kafka.conftest import (
    KAFKA_MODULE,
    FakeMessage,
    OffsetRecordingConsumer,
    ScriptedConsumer,
    SourceFactory,
    schemaless_metadata,
    warned_about,
)

TOPIC = "test-topic"
SCHEMALESS_METADATA = schemaless_metadata(TOPIC)


def _single_partition() -> Tuple[
    List[confluent_kafka.TopicPartition], Dict[int, Tuple[int, int]]
]:
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    watermarks = {0: (0, 1000)}  # low=0, high=1000
    return [partition], watermarks


def test_latest_strategy_seeks_near_end(kafka_source):
    partitions, watermarks = _single_partition()
    fake = OffsetRecordingConsumer()

    kafka_source._get_offset_based_samples(
        partitions,
        watermarks,
        200,
        [],
        TOPIC,
        None,
        consumer=fake,
        offset_fn=_latest_offset,
    )

    # latest: start_offset = max(low, high - partition_sample_size) = 1000 - 200
    assert fake.assigned_offsets == [800]


def test_full_strategy_seeks_to_low(kafka_source):
    partitions, watermarks = _single_partition()
    fake = OffsetRecordingConsumer()

    kafka_source._get_offset_based_samples(
        partitions,
        watermarks,
        200,
        [],
        TOPIC,
        None,
        consumer=fake,
        offset_fn=_full_offset,
    )

    assert fake.assigned_offsets == [0]


def test_random_strategy_seeks_within_valid_window(kafka_source):
    partitions, watermarks = _single_partition()
    fake = OffsetRecordingConsumer()

    # randint is called with (0, range_size - partition_sample_size) = (0, 800);
    # pin it so the assertion is deterministic.
    with patch(f"{KAFKA_MODULE}.random.randint", return_value=123):
        kafka_source._get_offset_based_samples(
            partitions,
            watermarks,
            200,
            [],
            TOPIC,
            None,
            consumer=fake,
            offset_fn=_random_offset,
        )

    # random_start = low + randint(...) = 0 + 123
    assert fake.assigned_offsets == [123]


def test_random_strategy_never_seeks_past_available_window(kafka_source):
    partitions, watermarks = _single_partition()
    fake = OffsetRecordingConsumer()

    # Largest legal randint result keeps start + sample_size within [low, high].
    with patch(f"{KAFKA_MODULE}.random.randint", return_value=800):
        kafka_source._get_offset_based_samples(
            partitions,
            watermarks,
            200,
            [],
            TOPIC,
            None,
            consumer=fake,
            offset_fn=_random_offset,
        )

    start = fake.assigned_offsets[0]
    assert start + 200 <= 1000


def test_stratified_strategy_seeks_evenly_across_range(kafka_source):
    partitions, watermarks = _single_partition()
    fake = OffsetRecordingConsumer()

    kafka_source._get_stratified_samples(
        partitions, watermarks, 100, [], TOPIC, None, consumer=fake
    )

    # num_samples = min(100, 1000) = 100; stride = 1000 / 100 = 10; batches of 10
    # start at low + int(i * stride) for i in {0, 10, 20, ...}.
    assert fake.seek_offsets == [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]


def test_strategy_skips_empty_partition(kafka_source):
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    watermarks = {0: (500, 500)}  # high == low -> nothing to read
    fake = OffsetRecordingConsumer()

    kafka_source._get_offset_based_samples(
        [partition],
        watermarks,
        200,
        [],
        TOPIC,
        None,
        consumer=fake,
        offset_fn=_latest_offset,
    )

    # No offset should be set; the read loop still assigns the (unchanged) partition.
    assert fake.assigned_offsets == [partition.offset]


def test_stratified_reports_errored_messages(kafka_source):
    """Errored messages are reported (like the batch path), not silently dropped."""
    partitions, watermarks = _single_partition()
    samples: List[Dict] = []
    errored = ScriptedConsumer([FakeMessage(None, error="simulated broker error")])

    kafka_source._get_stratified_samples(
        partitions, watermarks, 10, samples, TOPIC, None, consumer=errored
    )

    assert samples == []
    assert warned_about(kafka_source.report, TOPIC)


def test_parallel_profiling_worker_failure_is_dropped(kafka_source):
    tasks: List[Tuple[str, str, Optional[SchemaMetadataClass]]] = [
        ("urn:li:dataset:(urn:li:dataPlatform:kafka,test-topic,PROD)", TOPIC, None)
    ]
    with patch.object(
        kafka_source, "_collect_and_profile_topic", side_effect=RuntimeError("boom")
    ):
        workunits = list(kafka_source.generate_profiles_in_parallel(tasks))

    assert workunits == []
    assert kafka_source.report.profiling_topics_dropped == 1
    assert warned_about(kafka_source.report, TOPIC)


def test_empty_samples_drops_profile(kafka_source):
    with (
        patch(f"{KAFKA_MODULE}.get_kafka_consumer"),
        patch.object(kafka_source, "get_sample_messages", return_value=[]),
    ):
        result = kafka_source._collect_and_profile_topic("urn:test", TOPIC, None)

    assert result is None
    assert kafka_source.report.profiling_topics_dropped == 1


def test_empty_samples_reports_when_report_dropped_profiles_enabled(
    make_kafka_source: SourceFactory,
) -> None:
    source = make_kafka_source(
        {"profiling": {"enabled": True, "report_dropped_profiles": True}}
    )

    with (
        patch(f"{KAFKA_MODULE}.get_kafka_consumer"),
        patch.object(source, "get_sample_messages", return_value=[]),
    ):
        result = source._collect_and_profile_topic("urn:test", TOPIC, None)

    assert result is None
    assert source.report.profiling_topics_dropped == 1
    assert warned_about(source.report, TOPIC)


def test_avro_decode_failure_is_skipped_and_reported(kafka_source):
    # An AVRO-typed message that cannot be decoded must be skipped (return None),
    # not turned into a synthetic "avro_decode_error" field that pollutes the profile.
    schema_metadata = SchemaMetadataClass(
        schemaName=TOPIC,
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        # Intentionally invalid Avro schema so parsing/decoding fails.
        platformSchema=KafkaSchemaClass(
            documentSchema='{"type":"record"}',
            documentSchemaType="AVRO",
        ),
        fields=[],
    )
    # Valid Confluent header (magic byte 0 + 4-byte id) followed by junk payload.
    data = bytes([0, 0, 0, 0, 1, 9, 9, 9])

    result = kafka_source._process_message_part(
        data, "value", TOPIC, schema_metadata, is_key=False
    )

    assert result is None
    assert kafka_source.report.profiling_avro_decode_failures == 1


def test_random_offset_reads_whole_range_when_smaller_than_sample():
    assert _random_offset(10, 15, 100) == (10, 5)


def test_stratified_reads_small_partition_sequentially(kafka_source):
    """Fewer messages than requested: read from low in one batch, no seeking."""
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    fake = OffsetRecordingConsumer()

    kafka_source._get_stratified_samples(
        [partition], {0: (0, 5)}, 100, [], TOPIC, None, consumer=fake
    )

    assert fake.assigned_offsets == [0]
    assert fake.seek_offsets == []


def test_stratified_skips_empty_partition(kafka_source):
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    fake = OffsetRecordingConsumer()

    kafka_source._get_stratified_samples(
        [partition], {0: (7, 7)}, 10, [], TOPIC, None, consumer=fake
    )

    assert fake.assigned_offsets == []
    assert fake.seek_offsets == []


def test_stratified_collects_polled_messages(kafka_source):
    partitions, watermarks = _single_partition()
    samples: List[Dict] = []
    scripted = ScriptedConsumer([FakeMessage(b'{"a": 1}', offset=i) for i in range(3)])

    kafka_source._get_stratified_samples(
        partitions,
        watermarks,
        20,
        samples,
        TOPIC,
        SCHEMALESS_METADATA,
        consumer=scripted,
    )

    assert len(samples) == 3
    assert samples[0]["a"] == "1"
