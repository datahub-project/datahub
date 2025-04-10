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

import pymsteams
from pydantic import SecretStr
from ratelimit import limits, sleep_and_retry

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.utils.datahub_util import DATAHUB_SYSTEM_ACTOR_URN
from datahub_actions.utils.social_util import (
    get_message_from_entity_change_event,
    get_welcome_message,
    pretty_any_text,
)

logger = logging.getLogger(__name__)


@sleep_and_retry
@limits(calls=1, period=1)  # 1 call per second
def post_message(message_card, message):
    message_card.text(message)
    message_card.send()


class TeamsNotificationConfig(ConfigModel):
    webhook_url: SecretStr
    base_url: str = "http://localhost:9002/"
    suppress_system_activity: bool = True


class TeamsNotificationAction(Action):
    def name(self):
        return "TeamsNotificationAction"

    def close(self) -> None:
        pass

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = TeamsNotificationConfig.parse_obj(config_dict or {})
        logger.info(f"Teams notification action configured with {action_config}")
        return cls(action_config, ctx)

    def _new_card(self):
        return pymsteams.connectorcard(
            self.action_config.webhook_url.get_secret_value()
        )

    def __init__(self, action_config: TeamsNotificationConfig, ctx: PipelineContext):
        self.action_config = action_config
        self.ctx = ctx
        welcome_card = self._new_card()
        structured_message = get_welcome_message(self.action_config.base_url)
        welcome_card.title(structured_message.title)
        message_section = pymsteams.cardsection()
        for k, v in structured_message.properties.items():
            message_section.addFact(k, pretty_any_text(v, channel="teams"))
        welcome_card.addSection(message_section)
        post_message(welcome_card, structured_message.text)

    def act(self, event: EventEnvelope) -> None:
        try:
            message = json.dumps(json.loads(event.as_json()), indent=4)
            logger.debug(f"Received event: {message}")
            if event.event_type == "EntityChangeEvent_v1":
                assert isinstance(event.event, EntityChangeEvent)
                if (
                    event.event.auditStamp.actor == DATAHUB_SYSTEM_ACTOR_URN
                    and self.action_config.suppress_system_activity
                ):
                    return None

                semantic_message = get_message_from_entity_change_event(
                    event.event,
                    self.action_config.base_url,
                    self.ctx.graph.graph if self.ctx.graph else None,
                    channel="teams",
                )
                message_card = self._new_card()
                post_message(message_card, semantic_message)
            else:
                logger.debug("Skipping message because it didn't match our filter")
        except Exception as e:
            logger.debug("Failed to process event", e)
