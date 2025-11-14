import json
import pathlib
from typing import Iterable

from datahub.configuration.common import ConfigModel
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource


def read_events_file(filename: pathlib.Path) -> Iterable[EventEnvelope]:
    events_raw = filename.read_text()
    events_json = json.loads(events_raw)

    for event in events_json:
        # Serializing back to json because the EventEnvelope interface is kinda dumb and wants a string.
        event_raw = json.dumps(event)
        yield EventEnvelope.from_json(event_raw)


class ReplayEventSourceConfig(ConfigModel):
    filename: str


class ReplayEventSource(EventSource):
    def __init__(self, config: ReplayEventSourceConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        config = ReplayEventSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        yield from read_events_file(pathlib.Path(self.config.filename))

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        pass

    def close(self) -> None:
        return super().close()
