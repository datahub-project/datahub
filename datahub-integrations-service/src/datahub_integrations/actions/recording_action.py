import json

from datahub.configuration.common import ConfigModel
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class RecordingActionConfig(ConfigModel):
    filename: str


class RecordingAction(Action):
    def __init__(self, config: RecordingActionConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

        self.file = open(self.config.filename, "w")
        self._wrote_first = False

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "RecordingAction":
        config = RecordingActionConfig.model_validate(config_dict)
        return cls(config, ctx)

    def act(self, event: EventEnvelope) -> None:
        single_line = event.as_json()
        pretty = json.dumps(json.loads(single_line), indent=2)

        if self._wrote_first:
            self.file.write(",\n")
        else:
            self.file.write("[\n")
            self._wrote_first = True

        self.file.write(pretty)

        print(pretty)

    def close(self) -> None:
        self.file.write("\n]")
        self.file.close()

        return super().close()
