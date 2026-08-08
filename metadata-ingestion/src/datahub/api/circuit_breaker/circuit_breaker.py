import logging
from abc import abstractmethod
from typing import Optional

from gql import Client
from pydantic import Field

from datahub.api.gql_transport import build_gql_transport
from datahub.configuration.common import ConfigModel, TransparentSecretStr
from datahub.ingestion.auth.registry import AuthConfig

logger = logging.getLogger(__name__)


class CircuitBreakerConfig(ConfigModel):
    datahub_host: str = Field(description="Url of the DataHub instance")
    datahub_token: Optional[TransparentSecretStr] = Field(
        default=None, description="The datahub token"
    )
    timeout: Optional[int] = Field(
        default=None,
        description="The number of seconds to wait for your client to establish a connection to a remote machine",
    )


class AbstractCircuitBreaker:
    client: Client

    def __init__(
        self,
        datahub_host: str,
        datahub_token: Optional[str] = None,  # accepts raw str from callers
        timeout: Optional[int] = None,
        datahub_auth: Optional[AuthConfig] = None,
    ):
        self.transport = build_gql_transport(
            url=datahub_host,
            token=datahub_token,
            auth=datahub_auth,
            timeout=timeout,
        )
        self.client = Client(
            transport=self.transport,
            fetch_schema_from_transport=True,
        )

    @abstractmethod
    def is_circuit_breaker_active(self, urn: str) -> bool:
        pass
