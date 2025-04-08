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
from dataclasses import dataclass
from typing import Dict, List

from pydantic import SecretStr
from ratelimit import limits, sleep_and_retry
from requests import sessions
from slack_bolt import App

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.utils.datahub_util import DATAHUB_SYSTEM_ACTOR_URN
from datahub_actions.utils.social_util import (
    StructuredMessage,
    get_message_from_entity_change_event,
    get_welcome_message,
    pretty_any_text,
)

logger = logging.getLogger(__name__)


@sleep_and_retry
@limits(calls=1, period=1)
def post_message(client, token, channel, text):
    client.chat_postMessage(
        token=token,
        channel=channel,
        text=text,
    )


@dataclass
class SlackNotification:
    @staticmethod
    def get_payload(message: StructuredMessage) -> List[Dict]:
        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message.title},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(
                        [
                            f"*{k}*: {pretty_any_text(v, channel='slack')}"
                            for k, v in message.properties.items()
                        ]
                    ),
                },
            },
            {"type": "divider"},
        ]


class SlackNotificationConfig(ConfigModel):
    # default webhook posts to #actions-dev-slack-notifications on Acryl Data Slack space
    bot_token: SecretStr
    signing_secret: SecretStr
    default_channel: str
    base_url: str = "http://localhost:9002/"
    suppress_system_activity: bool = True


class SlackNotificationAction(Action):
    def name(self):
        return "SlackNotificationAction"

    def close(self) -> None:
        pass

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = SlackNotificationConfig.parse_obj(config_dict or {})
        logger.info(f"Slack notification action configured with {action_config}")
        return cls(action_config, ctx)

    def __init__(self, action_config: SlackNotificationConfig, ctx: PipelineContext):
        self.action_config = action_config
        self.ctx = ctx
        self.session = sessions.Session()

        # Initializes your app with your bot token and signing secret
        self.app = App(
            token=self.action_config.bot_token.get_secret_value(),
            signing_secret=self.action_config.signing_secret.get_secret_value(),
        )

        self.app.client.chat_postMessage(
            token=self.action_config.bot_token.get_secret_value(),
            channel=self.action_config.default_channel,
            blocks=SlackNotification.get_payload(
                get_welcome_message(self.action_config.base_url)
            ),
        )

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
                    channel="slack",
                )
                if semantic_message:
                    post_message(
                        client=self.app.client,
                        token=self.action_config.bot_token.get_secret_value(),
                        channel=self.action_config.default_channel,
                        text=semantic_message,
                    )
            else:
                logger.debug("Skipping message because it didn't match our filter")
        except Exception as e:
            logger.debug("Failed to process event", e)
