import logging
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, validator

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.secret.datahub_secrets_client import DataHubSecretsClient
from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)


class DataHubSecretStoreConfig(BaseModel):
    graph_client: Optional[DataHubGraph] = None
    graph_client_config: Optional[DatahubClientConfig] = None

    class Config:
        arbitrary_types_allowed = True

    @validator("graph_client")
    def check_graph_connection(cls, v: DataHubGraph) -> DataHubGraph:
        if v is not None:
            v.test_connection()
        return v


# An implementation of SecretStore that fetches secrets from DataHub
class DataHubSecretStore(SecretStore):
    # Client for fetching secrets from DataHub GraphQL API
    client: DataHubSecretsClient

    def __init__(self, config: DataHubSecretStoreConfig):
        # Attempt to establish an outbound connection to DataHub and create a client.
        if config.graph_client is not None:
            self.client = DataHubSecretsClient(graph=config.graph_client)
        elif config.graph_client_config is not None:
            graph = DataHubGraph(config.graph_client_config)
            self.client = DataHubSecretsClient(graph)
        else:
            raise Exception(
                "Invalid configuration provided: unable to construct DataHub Graph Client."
            )

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        # Fetch the secret from DataHub, using the credentials provided in the configuration.
        # Use the GraphQL API.
        try:
            return self.client.get_secret_values(secret_names)
        except Exception:
            # Failed to resolve secrets, return empty.
            logger.exception(
                f"Caught exception while attempting to fetch secrets from DataHub. Secret names: {secret_names}"
            )
            return {}

    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        secret_value_dict = self.get_secret_values([secret_name])
        return secret_value_dict.get(secret_name)

    def get_id(self) -> str:
        return "datahub"

    @classmethod
    def create(cls, config: Any) -> "DataHubSecretStore":
        config = DataHubSecretStoreConfig.parse_obj(config)
        return cls(config)
