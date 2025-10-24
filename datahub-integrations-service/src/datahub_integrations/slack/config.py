import dataclasses
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import cachetools
import pydantic
from datahub.configuration.common import ConnectionModel
from datahub.metadata.urns import DataPlatformUrn
from loguru import logger

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import (
    get_connection_json,
    save_connection_json,
)
from datahub_integrations.slack.slack_history import (
    SlackHistoryCache,
)

_SLACK_CONNECTION_ID = "__system_slack-0"
_SLACK_CONNECTION_URN = f"urn:li:dataHubConnection:{_SLACK_CONNECTION_ID}"
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
    # Custom validator to treat empty dict as None for app_config_tokens
    @pydantic.field_validator("app_config_tokens", mode="before")
    @classmethod
    def app_config_tokens_empty_dict_to_none(cls, v):
        if v == {}:
            return None
        return v

    # Custom validator to treat empty dict as None for app_details
    @pydantic.field_validator("app_details", mode="before")
    @classmethod
    def app_details_empty_dict_to_none(cls, v):
        if v == {}:
            return None
        return v

    app_config_tokens: Optional[SlackAppConfigCredentials] = None

    app_details: Optional[SlackAppDetails] = None

    bot_token: Optional[str] = None

    # TODO: Maybe add a needs_reinstall flag here?
    # TODO: Add workspace_id here?


def _get_current_slack_connection() -> SlackConnection:
    """Gets the current slack config from DataHub."""

    # For local testing, you can use this instead:
    # import pathlib
    # return SlackConnection.parse_obj(
    #     json.loads(pathlib.Path("slack_details.json").read_text())
    # )

    obj = get_connection_json(graph=graph, urn=_SLACK_CONNECTION_URN)

    if not obj:
        logger.debug("No slack config found, returning an empty config")
        return SlackConnection()

    config = SlackConnection.model_validate(obj)

    return config


def _set_current_slack_connection(config: SlackConnection) -> None:
    """Sets the current slack config in DataHub."""

    save_connection_json(
        graph=graph,
        urn=_SLACK_CONNECTION_URN,
        platform_urn=_SLACK_PLATFORM_URN,
        config=config,
    )


_SLACK_GET_GLOBAL_SETTINGS_QUERY = """
query getSlackMentionSettings {
    globalSettings {
        integrationSettings {
            slackSettings {
                datahubAtMentionEnabled
            }
        }
    }
}
"""


class SlackGlobalSettings(_FrozenConnectionModel):
    datahub_at_mention_enabled: bool = False


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60 * 5))
def get_global_settings() -> SlackGlobalSettings:
    """
    Gets the current global settings from DataHub.

    Settings are cached for 5 minutes via TTLCache decorator to ensure changes
    to Slack integration settings (e.g., @mention enablement) take effect within
    a reasonable timeframe without requiring service restart.
    """
    try:
        data = graph.execute_graphql(_SLACK_GET_GLOBAL_SETTINGS_QUERY)
        slack_settings: dict = data["globalSettings"]["integrationSettings"][
            "slackSettings"
        ]
        return SlackGlobalSettings(
            datahub_at_mention_enabled=slack_settings["datahubAtMentionEnabled"]
        )
    except Exception as e:
        logger.error(f"Failed to fetch global slack settings: {e}")
        return SlackGlobalSettings(datahub_at_mention_enabled=False)


@dataclass
class _SlackConfigManager:
    """A caching wrapper around the Slack config."""

    _connection: Optional[SlackConnection] = None
    _slack_history_cache: SlackHistoryCache = dataclasses.field(
        default_factory=SlackHistoryCache
    )

    def get_connection(self, force_refresh: bool = False) -> SlackConnection:
        if self._connection is None or force_refresh:
            self.reload()
            assert self._connection is not None

        return self._connection

    def get_global_settings(self) -> SlackGlobalSettings:
        # Fetch from the cached function, which automatically expires every 5 minutes
        return get_global_settings()

    def get_slack_history_cache(self) -> SlackHistoryCache:
        return self._slack_history_cache

    @classmethod
    def _get_app_id(cls, config: Optional[SlackConnection]) -> Optional[str]:
        if config and config.app_details and config.app_details.app_id:
            return config.app_details.app_id
        return None

    def reload(self) -> SlackConnection:
        logger.info("Reloading slack connection and settings")
        old_config = self._connection
        self._connection = _get_current_slack_connection()

        if (old_app_id := self._get_app_id(old_config)) != (
            new_app_id := self._get_app_id(self._connection)
        ):
            # NOTE: This isn't an ideal spot to put the slack_history_cache,
            # but it's the easiest path forward right now.
            logger.info(
                f"After reloading slack config, the Slack app ID changed from {old_app_id} to {new_app_id}; "
                "resetting slack history cache"
            )
            self._slack_history_cache = SlackHistoryCache()

        return self._connection

    def save_config(self, config: SlackConnection) -> None:
        logger.info("Setting slack config")
        self._connection = config
        _set_current_slack_connection(config)


slack_config = _SlackConfigManager()
