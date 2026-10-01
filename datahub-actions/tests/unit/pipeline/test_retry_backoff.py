import threading
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pytest

from datahub_actions.action.action import Action
from datahub_actions.action.action_registry import action_registry
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry
from tests.unit.test_helpers import metadata_change_log_event


class RecordingEventSource(EventSource):
    """Yields one event and records the `processed` flag the pipeline acks with."""

    def __init__(self) -> None:
        self.acks: List[Tuple[str, bool]] = []

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        return cls()

    def events(self) -> Iterable[EventEnvelope]:
        return [
            EventEnvelope("MetadataChangeLogEvent_v1", metadata_change_log_event, {})
        ]

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        self.acks.append((event.event_type, processed))

    def close(self) -> None:
        pass


class FlakyAction(Action):
    """Fails the first `failures` calls, then succeeds, like a downstream 503."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0
        self.first_call = threading.Event()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        return cls(failures=(config_dict or {}).get("failures", 2))

    def act(self, event: EventEnvelope) -> None:
        self.calls += 1
        self.first_call.set()
        if self.calls <= self.failures:
            raise RuntimeError("downstream unavailable")

    def close(self) -> None:
        pass


event_source_registry.register("retry_recording_source", RecordingEventSource)
action_registry.register("retry_flaky_action", FlakyAction)


def _pipeline(
    tmp_path: Path,
    failures: int,
    retry_count: Optional[int] = None,
    retry_backoff_seconds: Optional[float] = 0,
) -> Pipeline:
    options: dict = {"failed_events_dir": str(tmp_path)}
    if retry_count is not None:
        options["retry_count"] = retry_count
    if retry_backoff_seconds is not None:
        options["retry_backoff_seconds"] = retry_backoff_seconds
    return Pipeline.create(
        {
            "name": "retry-pipeline",
            "source": {"type": "retry_recording_source", "config": {}},
            "action": {"type": "retry_flaky_action", "config": {"failures": failures}},
            "options": options,
        }
    )


def test_transient_failure_is_retried_by_default(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, failures=2)
    pipeline.run()

    source = pipeline.source
    assert isinstance(source, RecordingEventSource)
    assert source.acks == [("MetadataChangeLogEvent_v1", True)]
    assert pipeline.stats().failed_event_count == 0


def test_retries_back_off_exponentially(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = _pipeline(tmp_path, failures=3, retry_backoff_seconds=1.5)
    waits: List[float] = []

    def record_wait(timeout: float) -> bool:
        waits.append(timeout)
        return False

    monkeypatch.setattr(pipeline._stop_requested, "wait", record_wait)
    pipeline.run()

    assert waits == [1.5, 3.0, 6.0]
    assert pipeline.stats().failed_event_count == 0


def test_stop_during_backoff_leaves_event_unacked(tmp_path: Path) -> None:
    # A stopping pipeline must not ack an event it gave up on mid-retry: on a Kafka
    # source that would commit past it. Unacked, it is redelivered after restart.
    pipeline = _pipeline(
        tmp_path, failures=10, retry_count=10, retry_backoff_seconds=60
    )
    worker = threading.Thread(target=pipeline.run)
    worker.start()

    action = pipeline.action
    assert isinstance(action, FlakyAction)
    assert action.first_call.wait(timeout=5)
    pipeline.stop()
    worker.join(timeout=5)

    assert not worker.is_alive()
    source = pipeline.source
    assert isinstance(source, RecordingEventSource)
    assert source.acks == []
    assert action.calls == 1
