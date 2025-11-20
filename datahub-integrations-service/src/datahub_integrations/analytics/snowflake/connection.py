import logging
from dataclasses import dataclass
from typing import Optional

from datahub.configuration.common import ConnectionModel
from datahub.ingestion.graph.client import DataHubGraph
from pydantic import SecretStr, field_validator, model_validator

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import (
    get_connection_json,
    save_connection_json,
)
from datahub_integrations.propagation.snowflake.constants import (
    AUTH_TYPE_KEY_PAIR,
    DEFAULT_AUTH_TYPE,
    VALID_AUTH_TYPES,
    AuthenticationType,
)

logger = logging.getLogger(__name__)


class _FrozenConnectionModel(ConnectionModel, frozen=True):  # type: ignore[misc]  # Frozen/non-frozen inheritance works at runtime
    pass


class SnowflakeConnection(_FrozenConnectionModel):  # type: ignore[misc]  # Frozen/non-frozen inheritance works at runtime
    account: str
    warehouse: str
    user: str
    password: Optional[SecretStr] = None
    role: Optional[str] = None
    authentication_type: AuthenticationType = DEFAULT_AUTH_TYPE
    private_key: Optional[SecretStr] = None
    private_key_password: Optional[SecretStr] = None

    @field_validator("authentication_type", mode="before")
    @classmethod
    def validate_authentication_type_value(cls, v) -> AuthenticationType:
        """Validate that the authentication type is supported."""
        if v not in VALID_AUTH_TYPES:
            raise ValueError(
                f"Invalid authentication_type: {v}. "
                f"Must be one of: {', '.join(VALID_AUTH_TYPES.keys())}"
            )
        return v

    @model_validator(mode="after")
    def validate_authentication_config(self):
        """Validate authentication configuration is consistent."""
        # If private_key is set, authentication_type should be KEY_PAIR_AUTHENTICATOR
        if (
            self.private_key is not None
            and self.authentication_type != AUTH_TYPE_KEY_PAIR
        ):
            raise ValueError(
                f"`private_key` is set but `authentication_type` is {self.authentication_type}. "
                f"Should be set to '{AUTH_TYPE_KEY_PAIR}' when using key pair authentication"
            )
        # If authentication_type is KEY_PAIR_AUTHENTICATOR, private_key must be set
        if self.authentication_type == AUTH_TYPE_KEY_PAIR and self.private_key is None:
            raise ValueError(
                f"`private_key` must be set when using {AUTH_TYPE_KEY_PAIR} authentication"
            )
        return self

    @classmethod
    def from_datahub(cls, graph: DataHubGraph) -> "SnowflakeConnection":
        obj = get_connection_json(graph=graph, urn="urn:li:datahubConnection:snowflake")
        if not obj:
            raise Exception("No snowflake config found")

        config = SnowflakeConnection.model_validate(obj)

        return config

    def to_datahub(self, graph: DataHubGraph) -> None:
        # Serialize connection config to JSON-compatible dict
        config_dict = self.model_dump(mode="json")
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
