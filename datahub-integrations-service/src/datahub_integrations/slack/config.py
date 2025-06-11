import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import pydantic
from datahub.configuration.common import ConnectionModel
from datahub.metadata.urns import DataPlatformUrn
from loguru import logger

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import (
    get_connection_json,
    save_connection_json,
)

_SLACK_CONFIG_ID = "__system_slack-0"
_SLACK_CONFIG_URN = f"urn:li:dataHubConnection:{_SLACK_CONFIG_ID}"
_SLACK_PLATFORM_URN: str = DataPlatformUrn("slack").urn()
SLACK_PROXY = os.environ.get("DATAHUB_SLACK_PROXY")


class _FrozenConnectionModel(ConnectionModel):
    model_config = pydantic.ConfigDict(frozen=True)


class SlackAppConfigCredentials(_FrozenConnectionModel):
    """Used for creating and updating the App Manifest"""

    access_token: Optional[str]
    refresh_token: Optional[str]

    # Default is to consider the token already expired.
    exp: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(tz=timezone.utc) - timedelta(days=1)
    )

    def is_expired(self):
        return datetime.now(tz=timezone.utc) > self.exp


class SlackAppDetails(_FrozenConnectionModel):
    app_id: Optional[str]
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    signing_secret: Optional[str] = None
    verification_token: Optional[str] = None


class SlackConnection(_FrozenConnectionModel):
    app_config_tokens: Optional[SlackAppConfigCredentials] = None

    app_details: Optional[SlackAppDetails] = None

    bot_token: Optional[str] = None

    # TODO: Maybe add a needs_reinstall flag here?
    # TODO: Add workspace_id here?


def _get_current_slack_config() -> SlackConnection:
    """Gets the current slack config from DataHub."""

    # For local testing, you can use this instead:
    # import pathlib
    # return SlackConnection.parse_obj(
    #     json.loads(pathlib.Path("slack_details.json").read_text())
    # )

    obj = get_connection_json(graph=graph, urn=_SLACK_CONFIG_URN)

    if not obj:
        logger.debug("No slack config found, returning an empty config")
        return SlackConnection()

    config = SlackConnection.model_validate(obj)

    return config


def _set_current_slack_config(config: SlackConnection) -> None:
    """Sets the current slack config in DataHub."""

    save_connection_json(
        graph=graph,
        urn=_SLACK_CONFIG_URN,
        platform_urn=_SLACK_PLATFORM_URN,
        config=config,
    )


@dataclass
class _SlackConfigManager:
    """A caching wrapper around the Slack config."""

    _config: Optional[SlackConnection] = None

    def get_config(self, force_refresh: bool = False) -> SlackConnection:
        if self._config is None or force_refresh:
            logger.info("Getting slack config")
            self._config = _get_current_slack_config()

        return self._config

    def reload(self) -> SlackConnection:
        logger.info("Reloading slack config")
        self._config = _get_current_slack_config()
        return self._config

    def save_config(self, config: SlackConnection) -> None:
        logger.info("Setting slack config")
        self._config = config
        _set_current_slack_config(config)


slack_config = _SlackConfigManager()
