import json
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pytest

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline import Pipeline, PipelineException
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry
from tests.unit.test_helpers import metadata_change_log_event


class RecordingEventSource(EventSource):
    """Yields one event; records acks. No dead-letter destination."""

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


class DeadLetteringEventSource(RecordingEventSource):
    """Records dead-lettered events, or fails the write when `broken` is set."""

    broken = False

    def __init__(self) -> None:
        super().__init__()
        self.dead_lettered: List[Tuple[str, str]] = []

    def dead_letter(self, event: EventEnvelope, error: BaseException) -> bool:
        if self.broken:
            raise RuntimeError("dead-letter topic unavailable")
        self.dead_lettered.append((event.event_type, str(error)))
        return True


class BrokenDeadLetterEventSource(DeadLetteringEventSource):
    broken = True


event_source_registry.register("dlq_recording_source", RecordingEventSource)
event_source_registry.register("dlq_dead_lettering_source", DeadLetteringEventSource)
event_source_registry.register("dlq_broken_source", BrokenDeadLetterEventSource)


def _pipeline(
    tmp_path: Path, source_type: str, failure_mode: Optional[str] = None
) -> Pipeline:
    options: dict = {"retry_count": 0, "failed_events_dir": str(tmp_path)}
    if failure_mode is not None:
        options["failure_mode"] = failure_mode
    return Pipeline.create(
        {
            "name": "dlq-pipeline",
            "source": {"type": source_type, "config": {}},
            "action": {"type": "throwing_test_action", "config": {}},
            "options": options,
        }
    )


def test_failed_event_is_dead_lettered_before_it_is_acked(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, "dlq_dead_lettering_source")
    pipeline.run()

    source = pipeline.source
    assert isinstance(source, DeadLetteringEventSource)
    assert source.dead_lettered == [
        ("MetadataChangeLogEvent_v1", "Ouch! Action code threw an exception.")
    ]
    assert source.acks == [("MetadataChangeLogEvent_v1", True)]
    assert pipeline.stats().failed_event_count == 1


def test_event_is_not_acked_when_dead_lettering_fails(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, "dlq_broken_source")

    with pytest.raises(PipelineException):
        pipeline.run()

    source = pipeline.source
    assert isinstance(source, BrokenDeadLetterEventSource)
    assert source.acks == []


def test_throw_mode_stops_without_dead_lettering(tmp_path: Path) -> None:
    # THROW leaves the event unacked for redelivery; a dead-letter copy would
    # duplicate it on every restart.
    pipeline = _pipeline(tmp_path, "dlq_dead_lettering_source", failure_mode="THROW")

    with pytest.raises(PipelineException):
        pipeline.run()

    source = pipeline.source
    assert isinstance(source, DeadLetteringEventSource)
    assert source.dead_lettered == []
    assert source.acks == []


def test_without_dead_letter_destination_failed_event_is_acked(tmp_path: Path) -> None:
    # Sources without a dead-letter destination keep the previous behavior: the
    # failed event is written to the local failed-events file and acked.
    pipeline = _pipeline(tmp_path, "dlq_recording_source")
    pipeline.run()

    source = pipeline.source
    assert isinstance(source, RecordingEventSource)
    assert source.acks == [("MetadataChangeLogEvent_v1", True)]
    failed_events = tmp_path / "dlq-pipeline" / "failed_events.log"
    assert json.loads(failed_events.read_text().strip())
