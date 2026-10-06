import json
import os
from typing import Any, Dict, Iterator, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)
from tests.unit.test_helpers import metadata_change_log_event

_MODULE = "datahub_actions.plugin.source.kafka.kafka_event_source"


@pytest.fixture(autouse=True)
def _clean_kafka_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("KAFKA_PROPERTIES_"):
            monkeypatch.delenv(name)


@pytest.fixture
def producer_cls() -> Iterator[MagicMock]:
    with (
        patch(f"{_MODULE}.confluent_kafka.DeserializingConsumer"),
        patch(f"{_MODULE}.SchemaRegistryClient"),
        patch(f"{_MODULE}.confluent_kafka.Producer") as producer_cls,
    ):
        yield producer_cls


def _source(config_dict: Dict[str, Any]) -> KafkaEventSource:
    config = KafkaEventSourceConfig.model_validate(config_dict)
    return KafkaEventSource(config, PipelineContext(pipeline_name="p1", graph=None))


def _event() -> EventEnvelope:
    return EventEnvelope(
        "MetadataChangeLogEvent_v1",
        metadata_change_log_event,
        {
            "kafka": {
                "topic": "MetadataChangeLog_Versioned_v1",
                "partition": 2,
                "offset": 41,
            }
        },
    )


def _deliver(producer: MagicMock, error: Optional[KafkaError] = None) -> None:
    # Invoke the delivery callback when flush() runs, as librdkafka does.
    def flush(timeout: float) -> int:
        on_delivery = producer.produce.call_args.kwargs["on_delivery"]
        on_delivery(error, None)
        return 0

    producer.flush.side_effect = flush


def test_without_topic_no_producer_is_created(producer_cls: MagicMock) -> None:
    source = _source({})

    assert source.dead_letter(_event(), RuntimeError("boom")) is False
    producer_cls.assert_not_called()


def test_failed_event_is_produced_with_its_coordinates_and_error(
    producer_cls: MagicMock,
) -> None:
    source = _source(
        {
            "dead_letter_topic": "actions-dlq",
            "connection": {
                "bootstrap": "broker:9092",
                "consumer_config": {
                    "security.protocol": "SASL_SSL",
                    "auto.offset.reset": "earliest",
                },
            },
        }
    )
    producer = producer_cls.return_value
    _deliver(producer)

    assert source.dead_letter(_event(), RuntimeError("boom")) is True

    producer_config = producer_cls.call_args.args[0]
    assert producer_config["bootstrap.servers"] == "broker:9092"
    assert producer_config["security.protocol"] == "SASL_SSL"
    assert producer_config["enable.idempotence"] is True
    assert "auto.offset.reset" not in producer_config

    produced = producer.produce.call_args
    assert produced.args == ("actions-dlq",)
    assert produced.kwargs["key"] == "MetadataChangeLog_Versioned_v1:2:41"
    assert json.loads(produced.kwargs["value"])["event_type"] == (
        "MetadataChangeLogEvent_v1"
    )
    headers: Dict[str, Any] = dict(produced.kwargs["headers"])
    assert headers["datahub.pipeline"] == "p1"
    assert headers["datahub.source.partition"] == "2"
    assert headers["datahub.source.offset"] == "41"
    assert headers["datahub.error"] == "RuntimeError: boom"


@pytest.mark.parametrize(
    "delivery",
    [
        (KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED), 0),
        (None, 1),  # still undelivered when flush() timed out
    ],
)
def test_undelivered_dead_letter_raises(
    producer_cls: MagicMock, delivery: Tuple[Optional[KafkaError], int]
) -> None:
    error, remaining = delivery
    source = _source({"dead_letter_topic": "actions-dlq"})
    producer = producer_cls.return_value

    def flush(timeout: float) -> int:
        if error is not None:
            producer.produce.call_args.kwargs["on_delivery"](error, None)
        return remaining

    producer.flush.side_effect = flush

    with pytest.raises(KafkaException):
        source.dead_letter(_event(), RuntimeError("boom"))
