from __future__ import annotations

from typing import Optional, overload

from datahub.errors import ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.urns import (
    CorpUserUrn,
    DomainUrn,
    GlossaryTermUrn,
)
from datahub.sdk.entity_client import EntityClient
from datahub.sdk.lineage_client import LineageClient


class DataHubClient:
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

    @classmethod
    def from_env(cls) -> "DataHubClient":
        # Inspired by the DockerClient.from_env() method.
        # TODO: This one also reads from ~/.datahubenv, so the "from_env" name might be a bit confusing.
        # That file is part of the "environment", but is not a traditional "env variable".
        graph = get_default_graph()
        return cls(graph=graph)

    @property
    def lineage(self) -> LineageClient:
        return LineageClient(self)

    @property
    def entities(self) -> EntityClient:
        return EntityClient(self)

    # TODO: Make things more generic across entity types.

    # TODO: Make all of these methods sync by default.

    def resolve_domain(self, *, name: str) -> DomainUrn:
        # TODO: add caching to this method
        urn_str = self._graph.get_domain_urn_by_name(name)
        if urn_str is None:
            raise ItemNotFoundError(f"Domain with name {name} not found")
        return DomainUrn.from_string(urn_str)

    @overload
    def resolve_user(self, *, name: str) -> CorpUserUrn: ...
    @overload
    def resolve_user(self, *, email: str) -> CorpUserUrn: ...
    def resolve_user(
        self, *, name: Optional[str] = None, email: Optional[str] = None
    ) -> CorpUserUrn:
        filter_explanation: str
        filters = []
        if name is not None:
            if email is not None:
                raise SdkUsageError("Cannot specify both name and email for auto_user")
            # TODO: do we filter on displayName or fullName?
            filter_explanation = f"with name {name}"
            filters.append(
                {
                    "field": "fullName",
                    "values": [name],
                    "condition": "EQUAL",
                }
            )
        elif email is not None:
            filter_explanation = f"with email {email}"
            filters.append(
                {
                    "field": "email",
                    "values": [email],
                    "condition": "EQUAL",
                }
            )
        else:
            raise SdkUsageError("Must specify either name or email for auto_user")

        users = list(
            self._graph.get_urns_by_filter(
                entity_types=[CorpUserUrn.ENTITY_TYPE],
                extraFilters=filters,
            )
        )
        if len(users) == 0:
            # TODO: In auto methods, should we just create the user/domain/etc if it doesn't exist?
            raise ItemNotFoundError(f"User {filter_explanation} not found")
        elif len(users) > 1:
            raise SdkUsageError(f"Multiple users found {filter_explanation}: {users}")
        else:
            return CorpUserUrn.from_string(users[0])

    def resolve_term(self, *, name: str) -> GlossaryTermUrn:
        # TODO: Add some limits on the graph fetch
        terms = list(
            self._graph.get_urns_by_filter(
                entity_types=[GlossaryTermUrn.ENTITY_TYPE],
                extraFilters=[
                    {
                        "field": "id",
                        "values": [name],
                        "condition": "EQUAL",
                    }
                ],
            )
        )
        if len(terms) == 0:
            raise ItemNotFoundError(f"Term with name {name} not found")
        elif len(terms) > 1:
            raise SdkUsageError(f"Multiple terms found with name {name}: {terms}")
        else:
            return GlossaryTermUrn.from_string(terms[0])

    """
    def propose(
        self,
        entity: Union[DatasetUrn, ContainerUrn, SchemaFieldUrn],
        proposal: Union[TagUrn, GlossaryTermUrn, str],  # str = description?
    ) -> None:
        # TODO: Also need to evaluate if this is the right interface?
        # e.g. a single unified "propose" interface vs multiple individual methods
        raise NotImplementedError("TODO: need to implement this")
    """
