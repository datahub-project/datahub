from __future__ import annotations

from typing import Optional, Union, overload

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.urns import (
    ContainerUrn,
    CorpUserUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    Urn,
)
from datahub.sdk._all_entities import ENTITY_CLASSES
from datahub.sdk._shared import Entity, UrnOrStr
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
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
        # This one also reads from ~/.datahubenv, so the "from_env" name might be a bit confusing.
        # The file is part of the "environment", but is not a traditional "env variable".
        graph = get_default_graph()
        return cls(graph=graph)

    @property
    def lineage(self) -> LineageClient:
        return LineageClient(self)

    # TODO: Make things more generic across entity types.

    # TODO: Make all of these methods sync by default.

    @overload
    def get(self, urn: ContainerUrn) -> Container: ...
    @overload
    def get(self, urn: DatasetUrn) -> Dataset: ...
    @overload
    def get(self, urn: Union[Urn, str]) -> Entity: ...
    def get(self, urn: UrnOrStr) -> Entity:
        if not isinstance(urn, Urn):
            urn = Urn.from_string(urn)

        # TODO: add error handling around this with a suggested alternative if not yet supported
        EntityClass = ENTITY_CLASSES[urn.entity_type]

        aspects = self._graph.get_entity_semityped(str(urn))
        # TODO throw an error if the entity doesn't exist
        # raise ItemNotFoundError(f"Entity {urn} not found")

        # TODO: save the timestamp so we can use If-Unmodified-Since on the updates
        return EntityClass._new_from_graph(urn, aspects)

    def create(self, entity: Entity) -> None:
        # TODO: add a "replace" parameter? how do we make it more clear that metadata could be overwritten?
        # When doing a replace, should we fail if there's aspects on the entity that we aren't about to overwrite?
        mcps = []

        if self._graph.exists(str(entity.urn)):
            raise SdkUsageError(
                f"Entity {entity.urn} already exists, and hence cannot be created."
            )

        # Extra safety check: by putting this first, we can ensure that
        # the request fails if the entity already exists.
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(entity.urn),
                aspect=entity.urn.to_key_aspect(),
                changeType=models.ChangeTypeClass.CREATE_ENTITY,
            )
        )

        mcps.extend(entity._as_mcps(models.ChangeTypeClass.CREATE))

        self._graph.emit_mcps(mcps)

    def put(self, entity: Entity) -> None:
        # TODO: this is a bit of a weird name. the purpose of this method is to declare
        # metadata for the entity. If it doesn't exist, it will be created.
        # If it does exist, it will be updated using a best effort, aspect-oriented update.
        # That per-aspect upsert may behave unexpectedly for users if edits were also
        # made via the UI.
        # alternative name candidates: upsert, save

        mcps = entity._as_mcps(models.ChangeTypeClass.UPSERT)

        # TODO respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "upsert" or "update" (e.g. if not modified) or "update_force"

        # TODO: require that upsert is used with new entities, not read-modify-write flows?
        # TODO: alternatively, require that all aspects associated with the entity are set?

        self._graph.emit_mcps(mcps)

    """
    def update(
        self, entity: MetadataPatchProposal, *, check_exists: bool = True
    ) -> None:
        if check_exists and not self._graph.exists(entity.urn):
            raise SdkUsageError(
                f"Entity {entity.urn} does not exist, and hence cannot be updated. "
                "You can bypass this check by setting check_exists=False."
            )

        mcps = entity.build()
        self._graph.emit_mcps(mcps)
    """

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
