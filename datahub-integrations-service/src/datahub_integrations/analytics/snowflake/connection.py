import logging
from dataclasses import dataclass
from typing import Optional

from datahub.configuration.common import ConnectionModel
from datahub.ingestion.graph.client import DataHubGraph
from pydantic import model_validator

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import (
    get_connection_json,
    save_connection_json,
)
from datahub_integrations.propagation.snowflake.config import (
    SnowflakeAuthenticationType,
)

logger = logging.getLogger(__name__)


class _FrozenConnectionModel(ConnectionModel, frozen=True):  # type: ignore[misc]  # Frozen/non-frozen inheritance works at runtime
    pass


class SnowflakeConnection(_FrozenConnectionModel):  # type: ignore[misc]  # Frozen/non-frozen inheritance works at runtime
    account: str
    warehouse: str
    user: str
    password: Optional[str] = None
    role: Optional[str] = None
    authentication_type: SnowflakeAuthenticationType = (
        SnowflakeAuthenticationType.DEFAULT_AUTHENTICATOR
    )
    private_key: Optional[str] = None
    private_key_password: Optional[str] = None

    @model_validator(mode="after")
    def validate_authentication_type(self):
        # If private_key is set, authentication_type should be KEY_PAIR_AUTHENTICATOR
        if (
            self.private_key is not None
            and self.authentication_type
            != SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
        ):
            raise ValueError(
                f"`private_key` is set but `authentication_type` is {self.authentication_type}. "
                f"Should be set to '{SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR}' when using key pair authentication"
            )
        # If authentication_type is KEY_PAIR_AUTHENTICATOR, private_key must be set
        if (
            self.authentication_type
            == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
            and self.private_key is None
        ):
            raise ValueError(
                f"`private_key` must be set when using {SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR} authentication"
            )
        return self

    @classmethod
    def from_datahub(cls, graph: DataHubGraph) -> "SnowflakeConnection":
        obj = get_connection_json(graph=graph, urn="urn:li:datahubConnection:snowflake")
        if not obj:
            raise Exception("No snowflake config found")

        config = SnowflakeConnection.parse_obj(obj)

        return config

    def to_datahub(self, graph: DataHubGraph) -> None:
        # Use mode='json' to serialize enums to their string values
        config_dict = self.dict(mode="json")
        logger.info(
            f"Saving Snowflake connection to DataHub. authentication_type={config_dict.get('authentication_type')} (type={type(config_dict.get('authentication_type'))})"
        )

        save_connection_json(
            graph=graph,
            urn="urn:li:datahubConnection:snowflake",
            platform_urn="urn:li:dataPlatform:snowflake",
            config=config_dict,
        )


def _get_current_snowflake_config() -> SnowflakeConnection:
    """Gets the current snowflake config from DataHub."""

    # For local testing, you can use this instead:
    # import pathlib
    # return SlackConnection.parse_obj(
    #     json.loads(pathlib.Path("slack_details.json").read_text())
    # )

    return SnowflakeConnection.from_datahub(graph=graph)


@dataclass
class _SnowflakeConfigManager:
    """A caching wrapper around the Snowflake config."""

    _config: Optional[SnowflakeConnection] = None

    def get_config(self, force_refresh: bool = False) -> SnowflakeConnection:
        if self._config is None or force_refresh:
            logger.info("Getting snowflake config")
            self._config = _get_current_snowflake_config()

        return self._config

    def reload(self) -> SnowflakeConnection:
        logger.info("Reloading snowflake config")
        self._config = _get_current_snowflake_config()
        return self._config

    # def save_config(self, config: SlackConnection) -> None:
    #     logger.info("Setting slack config")
    #     self._config = config
    #     _set_current_slack_config(config)


snowflake_config = _SnowflakeConfigManager()
