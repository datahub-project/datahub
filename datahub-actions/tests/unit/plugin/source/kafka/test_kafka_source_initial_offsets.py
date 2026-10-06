import logging
import os
from typing import Any, Dict, List, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, TopicPartition

from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)

_MODULE = "datahub_actions.plugin.source.kafka.kafka_event_source"
_TOPIC = "PlatformEvent_v1"


@pytest.fixture(autouse=True)
def _clean_kafka_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("KAFKA_PROPERTIES_"):
            monkeypatch.delenv(name)


def _source(config_dict: Dict[str, Any]) -> KafkaEventSource:
    ctx = PipelineContext(pipeline_name="p1", graph=None)
    with (
        patch(f"{_MODULE}.confluent_kafka.DeserializingConsumer"),
        patch(f"{_MODULE}.SchemaRegistryClient"),
    ):
        return KafkaEventSource(KafkaEventSourceConfig.model_validate(config_dict), ctx)


def _assign(
    source: KafkaEventSource,
    committed: Dict[int, int],
    watermarks: Dict[int, Tuple[int, int]],
) -> MagicMock:
    """Runs events() through one rebalance: the first poll after subscribe() invokes
    the registered on_assign callback, as librdkafka does, then stops the source."""
    consumer = cast(MagicMock, source.consumer)
    consumer.committed.return_value = [
        TopicPartition(_TOPIC, p, offset) for p, offset in committed.items()
    ]
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout: watermarks[
        tp.partition
    ]

    def poll(timeout: float) -> Any:
        if consumer.subscribe.called:
            on_assign = consumer.subscribe.call_args.kwargs.get("on_assign")
            assert on_assign is not None, "subscribe() registered no on_assign"
            on_assign(consumer, [TopicPartition(_TOPIC, p) for p in committed])
            source.running = False
        return None

    consumer.poll.side_effect = poll
    assert list(source.events()) == []
    return consumer


def _committed_offsets(consumer: MagicMock) -> List[Tuple[int, int]]:
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    return [
        (tp.partition, tp.offset) for tp in consumer.commit.call_args.kwargs["offsets"]
    ]


def test_uncommitted_partitions_get_their_starting_position_committed() -> None:
    consumer = _assign(
        _source({}),
        committed={0: OFFSET_INVALID, 1: 17},
        watermarks={0: (3, 42), 1: (0, 20)},
    )

    assert _committed_offsets(consumer) == [(0, 42)]


def test_earliest_reset_commits_the_low_watermark() -> None:
    source = _source(
        {"connection": {"consumer_config": {"auto.offset.reset": "earliest"}}}
    )
    consumer = _assign(source, committed={0: OFFSET_INVALID}, watermarks={0: (3, 42)})

    assert _committed_offsets(consumer) == [(0, 3)]


def test_group_with_committed_offsets_commits_nothing() -> None:
    consumer = _assign(_source({}), committed={0: 5}, watermarks={0: (0, 9)})

    consumer.commit.assert_not_called()


def test_failed_initial_commit_is_logged_and_consumption_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _source({})
    consumer = cast(MagicMock, source.consumer)
    consumer.commit.side_effect = KafkaException(
        KafkaError(KafkaError.COORDINATOR_NOT_AVAILABLE)
    )

    with caplog.at_level(logging.WARNING):
        _assign(source, committed={0: OFFSET_INVALID}, watermarks={0: (0, 9)})

    assert "starting position" in caplog.text
