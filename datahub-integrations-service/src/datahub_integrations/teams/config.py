import os
from dataclasses import dataclass
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
from datahub_integrations.teams.teams_history import TeamsHistoryCache

_TEAMS_CONFIG_ID = "__system_teams-0"
_TEAMS_CONFIG_URN = f"urn:li:dataHubConnection:{_TEAMS_CONFIG_ID}"
_TEAMS_PLATFORM_URN: str = DataPlatformUrn("teams").urn()


class _FrozenConnectionModel(ConnectionModel):
    model_config = pydantic.ConfigDict(frozen=True)


class TeamsAppDetails(_FrozenConnectionModel):
    app_id: Optional[str] = None
    app_password: Optional[str] = None
    tenant_id: Optional[str] = None


class TeamsConnection(_FrozenConnectionModel):
    app_details: Optional[TeamsAppDetails] = None
    webhook_url: Optional[str] = None
    enable_conversation_history: bool = True  # Enable conversation history by default


def _get_current_teams_config() -> TeamsConnection:
    """Gets the current Teams config from DataHub."""

    obj = get_connection_json(graph=graph, urn=_TEAMS_CONFIG_URN)

    if not obj:
        logger.debug("No Teams config found in DataHub, checking environment variables")

        # Fallback to environment variables
        app_id = os.environ.get("DATAHUB_TEAMS_APP_ID")
        app_password = os.environ.get("DATAHUB_TEAMS_APP_SECRET")
        tenant_id = os.environ.get("DATAHUB_TEAMS_TENANT_ID")
        webhook_url = os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL")
        enable_history = (
            os.environ.get("DATAHUB_TEAMS_ENABLE_CONVERSATION_HISTORY", "true").lower()
            == "true"
        )

        if app_id and app_password and tenant_id:
            logger.info("Using Teams configuration from environment variables")
            return TeamsConnection(
                app_details=TeamsAppDetails(
                    app_id=app_id, app_password=app_password, tenant_id=tenant_id
                ),
                webhook_url=webhook_url,
                enable_conversation_history=enable_history,
            )

        logger.debug("No Teams config found, returning an empty config")
        return TeamsConnection()

    config = TeamsConnection.model_validate(obj)

    # Merge with environment variables for missing values

    app_id = config.app_details.app_id if config.app_details else None
    app_password = config.app_details.app_password if config.app_details else None
    tenant_id = config.app_details.tenant_id if config.app_details else None

    # Fill in missing values from environment variables
    app_id = app_id or os.environ.get("DATAHUB_TEAMS_APP_ID")
    app_password = app_password or os.environ.get("DATAHUB_TEAMS_APP_PASSWORD")
    tenant_id = tenant_id or os.environ.get("DATAHUB_TEAMS_TENANT_ID")
    webhook_url = config.webhook_url or os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL")

    # Create merged config with both database and environment values
    if app_id and app_password and tenant_id:
        logger.info(
            "Using Teams configuration merged from database and environment variables"
        )
        return TeamsConnection(
            app_details=TeamsAppDetails(
                app_id=app_id, app_password=app_password, tenant_id=tenant_id
            ),
            webhook_url=webhook_url,
            enable_conversation_history=config.enable_conversation_history,
        )

    return config


def _set_current_teams_config(config: TeamsConnection) -> None:
    """Sets the current Teams config in DataHub."""

    save_connection_json(
        graph=graph,
        urn=_TEAMS_CONFIG_URN,
        platform_urn=_TEAMS_PLATFORM_URN,
        config=config,
    )


@dataclass
class _TeamsConfigManager:
    """A caching wrapper around the Teams config."""

    _config: Optional[TeamsConnection] = None
    _teams_history_cache: Optional[TeamsHistoryCache] = None

    def get_config(self, force_refresh: bool = False) -> TeamsConnection:
        if self._config is None or force_refresh:
            logger.info("Getting Teams config")
            self._config = _get_current_teams_config()

        return self._config

    def reload(self) -> TeamsConnection:
        logger.info("Reloading Teams config")
        self._config = _get_current_teams_config()
        if self._config.app_details:
            logger.info(
                f"Teams config loaded - App ID: {self._config.app_details.app_id}"
            )
            logger.info(
                f"Teams config loaded - Tenant ID: {self._config.app_details.tenant_id}"
            )
            logger.info(
                f"Teams config loaded - Webhook URL: {self._config.webhook_url}"
            )
            logger.info(
                f"Teams config loaded - Conversation history: {self._config.enable_conversation_history}"
            )
        else:
            logger.info("Teams config loaded - No app details found")
        return self._config

    def save_config(self, config: TeamsConnection) -> None:
        logger.info("Setting Teams config")
        self._config = config
        _set_current_teams_config(config)

    def get_teams_history_cache(self) -> TeamsHistoryCache:
        if self._teams_history_cache is None:
            self._teams_history_cache = TeamsHistoryCache()
        return self._teams_history_cache


teams_config = _TeamsConfigManager()
