# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
from typing import Optional

from pydantic import BaseModel

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class HelloWorldConfig(BaseModel):
    # Whether to print the message in upper case.
    to_upper: Optional[bool] = None


# A basic example of a DataHub action that prints all
# events received to the console.
class HelloWorldAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = HelloWorldConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config: HelloWorldConfig, ctx: PipelineContext):
        self.config = config

    def act(self, event: EventEnvelope) -> None:
        print("Hello world! Received event:")
        message = json.dumps(json.loads(event.as_json()), indent=4)
        if self.config.to_upper:
            print(message.upper())
        else:
            print(message)

    def close(self) -> None:
        pass
