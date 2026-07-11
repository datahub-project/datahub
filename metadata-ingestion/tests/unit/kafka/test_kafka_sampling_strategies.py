from typing import Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import confluent_kafka
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.metadata.schema_classes import (
    KafkaSchemaClass,
    SchemaMetadataClass,
)

TOPIC = "test-topic"


class FakeConsumer:
    """Records assign/seek offsets and yields no messages so the read loop exits."""

    def __init__(self) -> None:
        self.assigned_offsets: List[int] = []
        self.seek_offsets: List[int] = []

    def assign(self, partitions: List[confluent_kafka.TopicPartition]) -> None:
        self.assigned_offsets = [p.offset for p in partitions]

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        self.seek_offsets.append(partition.offset)

    def consume(self, num_messages: int, timeout: float) -> list:
        return []

    def poll(self, timeout: float) -> None:
        return None


@pytest.fixture
def kafka_source() -> Iterator[KafkaSource]:
    with (
        patch(
            "datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer",
            autospec=True,
        ),
        patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True),
    ):
        config = KafkaSourceConfig.model_validate(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "profiling": {
                    "enabled": True,
                    "sample_size": 100,
                    "batch_size": 100,
                },
            }
        )
        source = KafkaSource(config, PipelineContext(run_id="test"))
        yield source
        source.close()


def _single_partition() -> Tuple[
    List[confluent_kafka.TopicPartition], Dict[int, Tuple[int, int]]
]:
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    watermarks = {0: (0, 1000)}  # low=0, high=1000
    return [partition], watermarks


def test_latest_strategy_seeks_near_end(kafka_source):
    partitions, watermarks = _single_partition()
    fake = FakeConsumer()

    kafka_source._get_latest_samples(
        partitions, watermarks, 200, [], TOPIC, None, consumer=fake
    )

    # latest: start_offset = max(low, high - partition_sample_size) = 1000 - 200
    assert fake.assigned_offsets == [800]


def test_full_strategy_seeks_to_low(kafka_source):
    partitions, watermarks = _single_partition()
    fake = FakeConsumer()

    kafka_source._get_full_samples(
        partitions, watermarks, 200, [], TOPIC, None, consumer=fake
    )

    assert fake.assigned_offsets == [0]


def test_random_strategy_seeks_within_valid_window(kafka_source):
    partitions, watermarks = _single_partition()
    fake = FakeConsumer()

    # randint is called with (0, range_size - partition_sample_size) = (0, 800);
    # pin it so the assertion is deterministic.
    with patch("datahub.ingestion.source.kafka.kafka.random.randint", return_value=123):
        kafka_source._get_random_samples(
            partitions, watermarks, 200, [], TOPIC, None, consumer=fake
        )

    # random_start = low + randint(...) = 0 + 123
    assert fake.assigned_offsets == [123]


def test_random_strategy_never_seeks_past_available_window(kafka_source):
    partitions, watermarks = _single_partition()
    fake = FakeConsumer()

    # Largest legal randint result keeps start + sample_size within [low, high].
    with patch("datahub.ingestion.source.kafka.kafka.random.randint", return_value=800):
        kafka_source._get_random_samples(
            partitions, watermarks, 200, [], TOPIC, None, consumer=fake
        )

    start = fake.assigned_offsets[0]
    assert start + 200 <= 1000


def test_stratified_strategy_seeks_evenly_across_range(kafka_source):
    partitions, watermarks = _single_partition()
    fake = FakeConsumer()

    kafka_source._get_stratified_samples(
        partitions, watermarks, 100, [], TOPIC, None, consumer=fake
    )

    # num_samples = min(100, 1000) = 100; stride = 1000 / 100 = 10; batches of 10
    # start at low + int(i * stride) for i in {0, 10, 20, ...}.
    assert fake.seek_offsets == [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]


def test_strategy_skips_empty_partition(kafka_source):
    partition = confluent_kafka.TopicPartition(TOPIC, 0)
    watermarks = {0: (500, 500)}  # high == low -> nothing to read
    fake = FakeConsumer()

    kafka_source._get_latest_samples(
        [partition], watermarks, 200, [], TOPIC, None, consumer=fake
    )

    # No offset should be set; the read loop still assigns the (unchanged) partition.
    assert fake.assigned_offsets == [partition.offset]


class ErroringConsumer:
    """Yields messages whose .error() is truthy to exercise the error-report path."""

    def assign(self, partitions: List[confluent_kafka.TopicPartition]) -> None:
        pass

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        pass

    def poll(self, timeout: float) -> MagicMock:
        msg = MagicMock()
        msg.error.return_value = "simulated broker error"
        return msg


def test_stratified_reports_errored_messages(kafka_source):
    partitions, watermarks = _single_partition()
    samples: List[Dict] = []

    kafka_source._get_stratified_samples(
        partitions, watermarks, 10, samples, TOPIC, None, consumer=ErroringConsumer()
    )

    # Errored messages must be reported (like the batch path), not silently dropped.
    assert samples == []
    assert kafka_source.report.warnings


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
    assert kafka_source.report.warnings


def test_empty_samples_drops_profile(kafka_source):
    with (
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
        patch.object(kafka_source, "get_sample_messages", return_value=[]),
    ):
        result = kafka_source._collect_and_profile_topic("urn:test", TOPIC, None)

    assert result is None
    assert kafka_source.report.profiling_topics_dropped == 1


def test_empty_samples_reports_when_report_dropped_profiles_enabled():
    with (
        patch(
            "datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer",
            autospec=True,
        ),
        patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True),
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
    ):
        config = KafkaSourceConfig.model_validate(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "profiling": {"enabled": True, "report_dropped_profiles": True},
            }
        )
        source = KafkaSource(config, PipelineContext(run_id="test"))
        with patch.object(source, "get_sample_messages", return_value=[]):
            result = source._collect_and_profile_topic("urn:test", TOPIC, None)

        assert result is None
        assert source.report.profiling_topics_dropped == 1
        assert source.report.warnings
        source.close()


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
