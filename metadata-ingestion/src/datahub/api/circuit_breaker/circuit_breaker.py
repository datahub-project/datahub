import logging
from abc import abstractmethod
from typing import Optional

from gql import Client
from gql.transport.requests import RequestsHTTPTransport
from pydantic import Field

from datahub.configuration.common import ConfigModel

logger = logging.getLogger(__name__)


class CircuitBreakerConfig(ConfigModel):
    datahub_host: str = Field(description="Url of the DataHub instance")
    datahub_token: Optional[str] = Field(default=None, description="The datahub token")
    timeout: Optional[int] = Field(
        default=None,
        description="The number of seconds to wait for your client to establish a connection to a remote machine",
    )


class AbstractCircuitBreaker:
    client: Client

    def __init__(
        self,
        datahub_host: str,
        datahub_token: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        # logging.basicConfig(level=logging.DEBUG)

        # Select your transport with a defined url endpoint
        self.transport = RequestsHTTPTransport(
            url=datahub_host + "/api/graphql",
            headers=(
                {"Authorization": "Bearer " + datahub_token}
                if datahub_token is not None
                else None
            ),
            method="POST",
            timeout=timeout,
        )
        self.client = Client(
            transport=self.transport,
            fetch_schema_from_transport=True,
        )

    @abstractmethod
    def is_circuit_breaker_active(self, urn: str) -> bool:
        pass
