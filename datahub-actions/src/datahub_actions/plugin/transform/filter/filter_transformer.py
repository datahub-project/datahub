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
from typing import Any, Dict, List, Optional, Union

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.filter import match_util
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


class FilterTransformerConfig(ConfigModel):
    event_type: Union[str, List[str]]
    event: Optional[Dict[str, Any]] = Field(default=None)


class FilterTransformer(Transformer):
    def __init__(self, config: FilterTransformerConfig):
        self.config: FilterTransformerConfig = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        config = FilterTransformerConfig.model_validate(config_dict)
        return cls(config)

    def transform(self, env_event: EventEnvelope) -> Optional[EventEnvelope]:
        logger.debug(f"Preparing to filter event {env_event}")

        # Match Event Type.
        if not match_util.matches(self.config.event_type, env_event.event_type):
            return None

        # Match Event Body.
        if self.config.event is not None:
            body_as_json_dict = json.loads(env_event.event.as_json())
            for key, val in self.config.event.items():
                if not match_util.matches(val, body_as_json_dict.get(key)):
                    return None
        return env_event
