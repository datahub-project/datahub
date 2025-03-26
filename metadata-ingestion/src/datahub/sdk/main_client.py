from __future__ import annotations

from typing import Optional, overload

from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.sdk.entity_client import EntityClient
from datahub.sdk.resolver_client import ResolverClient
from datahub.sdk.search_client import SearchClient


class DataHubClient:
    """Main client for interacting with DataHub.

    This class provides the primary interface for interacting with DataHub,
    including entity management, search, and resolution capabilities.

    The client can be initialized in three ways:
    1. With a server URL and optional token
    2. With a DatahubClientConfig object
    3. With an existing (legacy) :py:class:`DataHubGraph` instance
    """

    @overload
    def __init__(self, *, server: str, token: Optional[str] = None): ...
    @overload
    def __init__(self, *, config: DatahubClientConfig): ...
    @overload
    def __init__(self, *, graph: DataHubGraph): ...
    def __init__(
        self,
        *,
        server: Optional[str] = None,
        token: Optional[str] = None,
        graph: Optional[DataHubGraph] = None,
        config: Optional[DatahubClientConfig] = None,
    ):
        """Initialize a new DataHubClient instance.

        Args:
            server: The URL of the DataHub server (e.g. "http://localhost:8080").
            token: Optional authentication token.
            graph: An existing DataHubGraph instance to use.
            config: A DatahubClientConfig object with connection details.

        Raises:
            SdkUsageError: If invalid combinations of arguments are provided.
        """
        if server is not None:
            if config is not None:
                raise SdkUsageError("Cannot specify both server and config")
            if graph is not None:
                raise SdkUsageError("Cannot specify both server and graph")
            graph = DataHubGraph(config=DatahubClientConfig(server=server, token=token))
        elif config is not None:
            if graph is not None:
                raise SdkUsageError("Cannot specify both config and graph")
            graph = DataHubGraph(config=config)
        elif graph is None:
            raise SdkUsageError("Must specify either server, config, or graph")

        self._graph = graph

    # TODO: test connection

    @classmethod
    def from_env(cls) -> "DataHubClient":
        """Initialize a DataHubClient from the environment variables or ~/.datahubenv file.

        This will first check DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN. If not present,
        it will read credentials from ~/.datahubenv. That file can be created using
        the `datahub init` command.

        If you're looking to specify the server/token in code, use the
        DataHubClient(server=..., token=...) constructor instead.

        Returns:
            A DataHubClient instance.
        """

        # Inspired by the DockerClient.from_env() method.
        # TODO: This one also reads from ~/.datahubenv, so the "from_env" name might be a bit confusing.
        # That file is part of the "environment", but is not a traditional "env variable".
        graph = get_default_graph()

        return cls(graph=graph)

    @property
    def entities(self) -> EntityClient:
        return EntityClient(self)

    @property
    def resolve(self) -> ResolverClient:
        return ResolverClient(self)

    @property
    def search(self) -> SearchClient:
        return SearchClient(self)

    # TODO: lineage client
