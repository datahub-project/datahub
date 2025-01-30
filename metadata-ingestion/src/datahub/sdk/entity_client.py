from __future__ import annotations

from typing import TYPE_CHECKING, Union, overload

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import (
    ContainerUrn,
    DatasetUrn,
    Urn,
)
from datahub.sdk._all_entities import ENTITY_CLASSES
from datahub.sdk._entity import Entity
from datahub.sdk._shared import UrnOrStr
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class EntityClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    @property
    def _graph(self) -> DataHubGraph:
        return self._client._graph

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

    def upsert(self, entity: Entity) -> None:
        # TODO: this is a bit of a weird name. the purpose of this method is to declare
        # metadata for the entity. If it doesn't exist, it will be created.
        # If it does exist, it will be updated using a best effort, aspect-oriented update.
        # That per-aspect upsert may behave unexpectedly for users if edits were also
        # made via the UI.
        # alternative name candidates: upsert, save
        # TODO: another idea is to have a "with_attribution" parameter here - that can be used to
        # isolate the updates to just stuff with the same attribution? I'm not sure if it totally
        # works with read-modify-write entities, but it does work for creates.

        if entity._prev_aspects is not None:
            raise SdkUsageError(
                "For entities obtained via client.get(), use client.update() instead"
            )

        mcps = entity._as_mcps(models.ChangeTypeClass.UPSERT)

        # TODO: require that upsert is used with new entities, not read-modify-write flows?
        # TODO: alternatively, require that all aspects associated with the entity are set?

        self._graph.emit_mcps(mcps)

    def update(self, entity: Union[Entity, MetadataPatchProposal]) -> None:
        if isinstance(entity, MetadataPatchProposal):
            return self._update_patch(entity)

        if entity._prev_aspects is None:
            raise SdkUsageError(
                f"For entities created via {entity.__class__.__name__}(...), use client.create() or client.upsert() instead"
            )

        # TODO: respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "update" (e.g. if not modified) or "update_force"

        mcps = entity._as_mcps(models.ChangeTypeClass.UPSERT)
        self._graph.emit_mcps(mcps)

    def _update_patch(
        self, updater: MetadataPatchProposal, check_exists: bool = True
    ) -> None:
        if check_exists and not self._graph.exists(updater.urn):
            raise SdkUsageError(
                f"Entity {updater.urn} does not exist, and hence cannot be updated. "
                "You can bypass this check by setting check_exists=False."
            )

        mcps = updater.build()
        self._graph.emit_mcps(mcps)
