from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Union, overload

import jwt

from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.metadata.schema_classes import AuditStampClass
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.sdk.entity_client import EntityClient
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.resolver_client import ResolverClient
from datahub.sdk.search_client import SearchClient

try:
    from acryl_datahub_cloud._sdk_extras import (  # type: ignore[import-not-found]
        AssertionsClient,
    )
except ImportError:
    AssertionsClient = None

# TODO: Change __ingestion to _ingestion.
DEFAULT_ACTOR_URN = CorpUserUrn("__ingestion")

logger = logging.getLogger(__name__)


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

    def test_connection(self) -> None:
        self._graph.test_connection()

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
        graph = get_default_graph(ClientMode.SDK)

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

    @property
    def lineage(self) -> LineageClient:
        return LineageClient(self)

    @property
    def assertions(self) -> AssertionsClient:  # type: ignore[return-value]  # Type is not available if assertion_client is not installed
        if AssertionsClient is None:
            raise SdkUsageError(
                "AssertionsClient is not installed, please install it with `pip install acryl-datahub-cloud`"
            )
        return AssertionsClient(self)

    def audit_actor(
        self, fallback_actor: Optional[Union[CorpUserUrn, CorpGroupUrn]] = None
    ) -> Union[CorpUserUrn, CorpGroupUrn]:
        """Get the actor to be used for audit purposes.

        It gets the actor from the JWT token used to initialize the DataHubGraph.
        If no token was provided or any error when processing the JWT token, it defaults to fallback_actor and, if not given, it fallbacks to DEFAULT_ACTOR_URN.
        """
        default_actor = fallback_actor or DEFAULT_ACTOR_URN

        if self._graph.config.token is None:
            return default_actor

        jwt_token = self._graph.config.token
        try:
            payload = jwt.decode(jwt_token, options={"verify_signature": False})
        except Exception as e:
            logger.warning(
                f"Failed to decode JWT token: {e} Falling back to default actor: {default_actor}"
            )
            return default_actor

        if "actorId" not in payload or not payload["actorId"]:
            logger.warning(
                f"Missing 'actorId' in JWT token, using default actor: {default_actor}"
            )
            return default_actor

        if "type" not in payload or payload["type"] != "PERSONAL":
            logger.warning(
                f"JWT token 'type' is not 'PERSONAL', using default actor: {default_actor}"
            )
            return default_actor

        return CorpUserUrn.create_from_id(payload["actorId"])

    def audit_stamp(
        self,
        fallback_actor: Optional[Union[CorpUserUrn, CorpGroupUrn]] = None,
        fallback_timestamp: Optional[datetime] = None,
    ) -> AuditStampClass:
        """Get an AuditStampClass for auditing purposes.
        It uses the actor obtained from the audit_actor method and the current timestamp, unless fallback_timestamp is given.
        """
        actor_urn = self.audit_actor(fallback_actor=fallback_actor)
        timestamp = fallback_timestamp or datetime.now()

        # Convert datetime to milliseconds timestamp as expected by AuditStampClass
        timestamp_ms = int(timestamp.timestamp() * 1000)

        return AuditStampClass(time=timestamp_ms, actor=str(actor_urn))
