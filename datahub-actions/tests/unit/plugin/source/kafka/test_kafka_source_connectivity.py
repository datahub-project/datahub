import logging
from typing import Any, Callable, Iterator, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka import kafka_event_source
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    DEFAULT_TOPIC_ROUTES,
    KafkaEventSource,
    KafkaEventSourceConfig,
)

_MODULE = "datahub_actions.plugin.source.kafka.kafka_event_source"


def _make_source() -> Tuple[KafkaEventSource, dict]:
    """Returns the source and the config dict its consumer was built with."""
    ctx = PipelineContext(pipeline_name="test-connect", graph=None)
    with (
        patch(f"{_MODULE}.confluent_kafka.DeserializingConsumer") as consumer_cls,
        patch(f"{_MODULE}.SchemaRegistryClient"),
    ):
        source = KafkaEventSource(KafkaEventSourceConfig(), ctx)
    return source, consumer_cls.call_args.args[0]


def _consumer(source: KafkaEventSource) -> MagicMock:
    return cast(MagicMock, source.consumer)


def _transport_failure() -> KafkaException:
    return KafkaException(
        KafkaError(KafkaError._TRANSPORT, "Failed to get metadata: transport failure")
    )


@pytest.fixture
def clock() -> Iterator[Callable[[], float]]:
    """A monotonic clock that advances one second per reading, so the startup
    window elapses without the test sleeping."""
    now = [0.0]

    def tick() -> float:
        now[0] += 1.0
        return now[0]

    with patch(f"{_MODULE}.time.monotonic", side_effect=tick):
        yield tick


def _stop_on_first_poll_after_subscribe(source: KafkaEventSource) -> None:
    def poll(timeout: float) -> Any:
        if _consumer(source).subscribe.called:
            source.running = False
        return None

    _consumer(source).poll.side_effect = poll


def test_connected_source_logs_readiness_and_subscribes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source, _ = _make_source()
    _stop_on_first_poll_after_subscribe(source)

    with caplog.at_level(logging.INFO):
        assert list(source.events()) == []

    _consumer(source).subscribe.assert_called_once()
    assert [
        call.args[0] for call in _consumer(source).list_topics.call_args_list
    ] == list(DEFAULT_TOPIC_ROUTES.values())
    assert any(
        "connected to Kafka" in r.message and "test-connect" in r.message
        for r in caplog.records
        if r.levelno == logging.INFO
    )


def test_transient_failures_inside_the_window_are_retried(
    clock: Callable[[], float], caplog: pytest.LogCaptureFixture
) -> None:
    source, _ = _make_source()
    failures = {"remaining": 2}

    def list_topics(*_args: Any, **_kwargs: Any) -> Any:
        if failures["remaining"]:
            failures["remaining"] -= 1
            raise _transport_failure()
        return MagicMock()

    _consumer(source).list_topics.side_effect = list_topics
    _stop_on_first_poll_after_subscribe(source)

    assert list(source.events()) == []

    _consumer(source).subscribe.assert_called_once()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_unreachable_cluster_fails_the_source_after_the_window(
    clock: Callable[[], float], caplog: pytest.LogCaptureFixture
) -> None:
    source, _ = _make_source()
    _consumer(source).list_topics.side_effect = _transport_failure()

    with pytest.raises(KafkaException):
        list(source.events())

    _consumer(source).subscribe.assert_not_called()
    assert any(
        r.levelno == logging.ERROR and "Could not connect to Kafka" in r.message
        for r in caplog.records
    )
    # Bounded: it gave up after the window rather than retrying forever.
    assert (
        _consumer(source).list_topics.call_count
        <= kafka_event_source._STARTUP_CONNECT_TIMEOUT_SECONDS
    )


def test_close_during_startup_stops_waiting_without_failing(
    clock: Callable[[], float],
) -> None:
    source, _ = _make_source()

    def list_topics(*_args: Any, **_kwargs: Any) -> Any:
        source.close()
        raise _transport_failure()

    _consumer(source).list_topics.side_effect = list_topics

    assert list(source.events()) == []
    _consumer(source).subscribe.assert_not_called()


def test_stop_during_startup_does_not_restart_the_consumer() -> None:
    source, _ = _make_source()
    source.request_stop()

    assert list(source.events()) == []

    _consumer(source).list_topics.assert_not_called()
    _consumer(source).subscribe.assert_not_called()
    assert source.running is False


def test_closed_source_does_not_restart_the_consumer() -> None:
    source, _ = _make_source()
    source.close()

    assert list(source.events()) == []

    _consumer(source).list_topics.assert_not_called()
    _consumer(source).subscribe.assert_not_called()
    assert source.running is False


def test_startup_probes_each_configured_topic() -> None:
    source, _ = _make_source()
    _stop_on_first_poll_after_subscribe(source)

    assert list(source.events()) == []

    probed = [call.args[0] for call in _consumer(source).list_topics.call_args_list]
    assert probed == list(DEFAULT_TOPIC_ROUTES.values())


def test_unknown_topic_still_counts_as_connected() -> None:
    source, _ = _make_source()

    def list_topics(topic: str, timeout: float = 0) -> Any:
        topic_meta = MagicMock()
        topic_meta.error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "missing")
        metadata = MagicMock()
        metadata.topics = {topic: topic_meta}
        return metadata

    _consumer(source).list_topics.side_effect = list_topics
    _stop_on_first_poll_after_subscribe(source)

    assert list(source.events()) == []
    _consumer(source).subscribe.assert_called_once()


def test_topic_authorization_error_is_not_treated_as_connected(
    clock: Callable[[], float],
) -> None:
    source, _ = _make_source()

    def list_topics(topic: str, timeout: float = 0) -> Any:
        topic_meta = MagicMock()
        topic_meta.error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "denied")
        metadata = MagicMock()
        metadata.topics = {topic: topic_meta}
        return metadata

    _consumer(source).list_topics.side_effect = list_topics

    with pytest.raises(KafkaException):
        list(source.events())
    _consumer(source).subscribe.assert_not_called()


@pytest.mark.parametrize(
    "code, level",
    [
        (KafkaError._AUTHENTICATION, logging.ERROR),
        (KafkaError._ALL_BROKERS_DOWN, logging.ERROR),
        (KafkaError._TRANSPORT, logging.WARNING),
    ],
)
def test_client_errors_are_logged_with_their_cause(
    code: int, level: int, caplog: pytest.LogCaptureFixture
) -> None:
    _, consumer_config = _make_source()

    consumer_config["error_cb"](KafkaError(code, "SASL authentication error: nope"))

    assert [
        r.levelno
        for r in caplog.records
        if "SASL authentication error: nope" in r.message
        and "test-connect" in r.message
    ] == [level]
