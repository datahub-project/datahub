from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Union, overload

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.errors import IngestionAttributionWarning, ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import (
    ContainerUrn,
    DatasetUrn,
    MlModelGroupUrn,
    MlModelUrn,
    Urn,
)
from datahub.sdk._all_entities import ENTITY_CLASSES
from datahub.sdk._shared import UrnOrStr
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class EntityClient:
    """Client for managing DataHub entities.

    This class provides methods for retrieving and managing DataHub entities
    such as datasets, containers, and other metadata objects.
    """

    def __init__(self, client: DataHubClient):
        """Private constructor - use :py:attr:`DataHubClient.entities` instead.

        Args:
            client: The parent DataHubClient instance.
        """
        self._client = client

    # TODO: Make all of these methods sync by default.

    @property
    def _graph(self) -> DataHubGraph:
        return self._client._graph

    @overload
    def get(self, urn: ContainerUrn) -> Container: ...
    @overload
    def get(self, urn: DatasetUrn) -> Dataset: ...
    @overload
    def get(self, urn: MlModelUrn) -> MLModel: ...
    @overload
    def get(self, urn: MlModelGroupUrn) -> MLModelGroup: ...
    @overload
    def get(self, urn: Union[Urn, str]) -> Entity: ...
    def get(self, urn: UrnOrStr) -> Entity:
        """Retrieve an entity by its urn.

        Args:
            urn: The urn of the entity to retrieve. Can be a string or :py:class:`Urn` object.

        Returns:
            The retrieved entity instance.

        Raises:
            ItemNotFoundError: If the entity does not exist.
            SdkUsageError: If the entity type is not yet supported.
            InvalidUrnError: If the URN is invalid.
        """
        if not isinstance(urn, Urn):
            urn = Urn.from_string(urn)

        # TODO: add error handling around this with a suggested alternative if not yet supported
        EntityClass = ENTITY_CLASSES[urn.entity_type]

        if not self._graph.exists(str(urn)):
            raise ItemNotFoundError(f"Entity {urn} not found")

        aspects = self._graph.get_entity_semityped(str(urn))

        # TODO: save the timestamp so we can use If-Unmodified-Since on the updates
        return EntityClass._new_from_graph(urn, aspects)

    def create(self, entity: Entity) -> None:
        mcps = []

        if self._graph.exists(str(entity.urn)):
            raise SdkUsageError(
                f"Entity {entity.urn} already exists. Use client.entities.upsert() to update it."
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
        mcps.extend(entity.as_mcps(models.ChangeTypeClass.CREATE))

        self._graph.emit_mcps(mcps)

    def upsert(self, entity: Entity) -> None:
        if entity._prev_aspects is None and self._graph.exists(str(entity.urn)):
            warnings.warn(
                f"The entity {entity.urn} already exists. This operation will partially overwrite the existing entity.",
                IngestionAttributionWarning,
                stacklevel=2,
            )
            # TODO: If there are no previous aspects but the entity exists, should we delete aspects that are not present here?

        mcps = entity.as_mcps(models.ChangeTypeClass.UPSERT)
        self._graph.emit_mcps(mcps)

    def update(self, entity: Union[Entity, MetadataPatchProposal]) -> None:
        if isinstance(entity, MetadataPatchProposal):
            return self._update_patch(entity)

        if entity._prev_aspects is None:
            raise SdkUsageError(
                f"For entities created via {entity.__class__.__name__}(...), use client.entities.create() or client.entities.upsert() instead"
            )

        # TODO: respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "update" (e.g. if not modified) or "update_force"

        mcps = entity.as_mcps(models.ChangeTypeClass.UPSERT)
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
