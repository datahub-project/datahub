from unittest.mock import MagicMock, patch

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)


def _make_event_envelope() -> EventEnvelope:
    return EventEnvelope(
        event_type="TestEvent",
        event=MagicMock(),
        meta={"kafka": {"topic": "test-topic", "partition": 0, "offset": 42}},
    )


def _make_source(async_commit_enabled: bool) -> KafkaEventSource:
    """Build a KafkaEventSource with a mocked Kafka consumer."""
    config = KafkaEventSourceConfig(
        async_commit_enabled=async_commit_enabled,
    )
    ctx = PipelineContext(pipeline_name="test-ack", graph=None)

    with patch(
        "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
    ):
        with patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
        ):
            return KafkaEventSource(config, ctx)


class TestAckSyncMode:
    """When async_commit_enabled=False (default), ack always does sync commits."""

    def test_sync_mode_commits_on_processed_true(self) -> None:
        source = _make_source(async_commit_enabled=False)
        event = _make_event_envelope()

        with (
            patch.object(source, "_commit_offsets") as mock_commit,
            patch.object(source, "_store_offsets") as mock_store,
        ):
            source.ack(event, processed=True)

            mock_commit.assert_called_once_with(event)
            mock_store.assert_not_called()

    def test_sync_mode_commits_on_processed_false(self) -> None:
        source = _make_source(async_commit_enabled=False)
        event = _make_event_envelope()

        with (
            patch.object(source, "_commit_offsets") as mock_commit,
            patch.object(source, "_store_offsets") as mock_store,
        ):
            source.ack(event, processed=False)

            mock_commit.assert_called_once_with(event)
            mock_store.assert_not_called()


class TestAckAsyncMode:
    """When async_commit_enabled=True, ack should always store offsets for
    periodic autocommit — never do synchronous commits."""

    def test_async_mode_stores_offset_on_processed_true(self) -> None:
        source = _make_source(async_commit_enabled=True)
        event = _make_event_envelope()

        with (
            patch.object(source, "_commit_offsets") as mock_commit,
            patch.object(source, "_store_offsets") as mock_store,
        ):
            source.ack(event, processed=True)

            mock_store.assert_called_once_with(event)
            mock_commit.assert_not_called()

    def test_async_mode_stores_offset_on_processed_false(self) -> None:
        source = _make_source(async_commit_enabled=True)
        event = _make_event_envelope()

        with (
            patch.object(source, "_commit_offsets") as mock_commit,
            patch.object(source, "_store_offsets") as mock_store,
        ):
            source.ack(event, processed=False)

            mock_store.assert_called_once_with(event)
            mock_commit.assert_not_called()
