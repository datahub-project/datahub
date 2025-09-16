import os
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    app_tenant_id: Optional[str] = None
    tenant_id: Optional[str] = None


class TeamsConnection(_FrozenConnectionModel):
    app_details: Optional[TeamsAppDetails] = None
    webhook_url: Optional[str] = None
    enable_conversation_history: bool = True  # Enable conversation history by default

    def valid(self) -> bool:
        """Check if the Teams configuration is complete and valid.

        Returns:
            True if the configuration has all required fields, False otherwise
        """
        # Check if app_details exists
        if not self.app_details:
            return False

        # Check required app credentials (from environment variables)
        if not self.app_details.app_id:
            return False
        if not self.app_details.app_password:
            return False
        if not self.app_details.app_tenant_id:
            return False

        # Check customer tenant_id (from DataHub) - this is critical for sending notifications
        if not self.app_details.tenant_id:
            return False

        return True


def _get_current_teams_config() -> TeamsConnection:
    """Gets the current Teams config from DataHub."""

    # Some fields are directly sourced from environment variables
    app_id = os.environ.get("DATAHUB_TEAMS_APP_ID")
    app_password = os.environ.get("DATAHUB_TEAMS_APP_PASSWORD")
    app_tenant_id = os.environ.get("DATAHUB_TEAMS_TENANT_ID")
    webhook_url = os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL")
    enable_history = (
        os.environ.get("DATAHUB_TEAMS_ENABLE_CONVERSATION_HISTORY", "true").lower()
        == "true"
    )

    if not app_id or not app_password or not app_tenant_id:
        logger.error("No Teams config found, returning an empty config")
        return TeamsConnection()

    obj = get_connection_json(graph=graph, urn=_TEAMS_CONFIG_URN)

    if not obj:
        logger.debug("No Teams config found in DataHub, using env vars only")
        return TeamsConnection(
            app_details=TeamsAppDetails(
                app_id=app_id,
                app_password=app_password,
                app_tenant_id=app_tenant_id,
                tenant_id=None,  # No customer tenant yet
            ),
            webhook_url=os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL"),
            enable_conversation_history=True,
        )

    # Parse DataHub config for customer-specific settings
    config = TeamsConnection.model_validate(obj)

    # Merge: env vars for app credentials, DataHub for customer settings
    return TeamsConnection(
        app_details=TeamsAppDetails(
            app_id=app_id,  # Always from env
            app_password=app_password,  # Always from env
            app_tenant_id=app_tenant_id,  # Always from env
            tenant_id=config.app_details.tenant_id
            if config.app_details
            else None,  # From DataHub
        ),
        webhook_url=webhook_url,
        enable_conversation_history=enable_history,
    )


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
    """A caching wrapper around the Teams config with time-based throttling."""

    _config: Optional[TeamsConnection] = None
    _teams_history_cache: Optional[TeamsHistoryCache] = None
    _last_credentials_refresh_attempt: Optional[datetime] = None

    def get_config(self, force_refresh: bool = False) -> TeamsConnection:
        """Get Teams configuration with time-based throttling.

        Args:
            force_refresh: If True, always reload from DataHub regardless of cache state
        """
        if force_refresh:
            logger.info("Force reloading Teams configuration from DataHub")
            self._config = _get_current_teams_config()
            self._last_credentials_refresh_attempt = datetime.now()
            return self._config

        # Time-based throttling: only reload if more than 5 minutes have passed
        current_time = datetime.now()
        should_reload = self._last_credentials_refresh_attempt is None or (
            current_time - self._last_credentials_refresh_attempt
        ) > timedelta(minutes=5)

        # Also reload if current config is invalid (bypass cache for invalid configs)
        config_is_invalid = self._config is not None and not self._config.valid()

        if should_reload or config_is_invalid:
            if config_is_invalid:
                logger.info("Reloading Teams configuration (invalid config detected)")
            else:
                logger.info("Reloading Teams configuration (time-based refresh)")
            self._config = _get_current_teams_config()
            self._last_credentials_refresh_attempt = current_time
        elif self._config is None:
            # First time loading
            logger.info("Loading Teams configuration for first time")
            self._config = _get_current_teams_config()
            self._last_credentials_refresh_attempt = current_time

        return self._config

    def reload(self) -> TeamsConnection:
        logger.info("Reloading Teams config")
        self._config = _get_current_teams_config()
        self._last_credentials_refresh_attempt = datetime.now()
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
