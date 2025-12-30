from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Optional, Union, overload

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.emitter.rest_emitter import EmitMode
from datahub.errors import IngestionAttributionWarning, ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import (
    ChartUrn,
    ContainerUrn,
    DashboardUrn,
    DataFlowUrn,
    DataJobUrn,
    DatasetUrn,
    DocumentUrn,
    MlModelGroupUrn,
    MlModelUrn,
    Urn,
)
from datahub.sdk._all_entities import ENTITY_CLASSES
from datahub.sdk._shared import UrnOrStr
from datahub.sdk.chart import Chart
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.document import Document
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
    def get(self, urn: DocumentUrn) -> Document: ...
    @overload
    def get(self, urn: MlModelUrn) -> MLModel: ...
    @overload
    def get(self, urn: MlModelGroupUrn) -> MLModelGroup: ...
    @overload
    def get(self, urn: DataFlowUrn) -> DataFlow: ...
    @overload
    def get(self, urn: DataJobUrn) -> DataJob: ...
    @overload
    def get(self, urn: DashboardUrn) -> Dashboard: ...
    @overload
    def get(self, urn: ChartUrn) -> Chart: ...
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
        try:
            EntityClass = ENTITY_CLASSES[urn.entity_type]
        except KeyError as e:
            # Try to import cloud-specific entities if not found
            try:
                from acryl_datahub_cloud.sdk.entities.assertion import Assertion
                from acryl_datahub_cloud.sdk.entities.monitor import Monitor
                from acryl_datahub_cloud.sdk.entities.subscription import Subscription

                if urn.entity_type == "assertion":
                    EntityClass = Assertion
                elif urn.entity_type == "subscription":
                    EntityClass = Subscription
                elif urn.entity_type == "monitor":
                    EntityClass = Monitor
                else:
                    raise SdkUsageError(
                        f"Entity type {urn.entity_type} is not yet supported"
                    ) from e
            except ImportError as e:
                raise SdkUsageError(
                    f"Entity type {urn.entity_type} is not yet supported"
                ) from e

        if not self._graph.exists(str(urn)):
            raise ItemNotFoundError(f"Entity {urn} not found")

        aspects = self._graph.get_entity_semityped(str(urn))

        # TODO: save the timestamp so we can use If-Unmodified-Since on the updates
        entity = EntityClass._new_from_graph(urn, aspects)

        # Type narrowing for cloud-specific entities
        if urn.entity_type == "assertion":
            from acryl_datahub_cloud.sdk.entities.assertion import Assertion

            assert isinstance(entity, Assertion)
        elif urn.entity_type == "monitor":
            from acryl_datahub_cloud.sdk.entities.monitor import Monitor

            assert isinstance(entity, Monitor)
        elif urn.entity_type == "subscription":
            from acryl_datahub_cloud.sdk.entities.subscription import Subscription

            assert isinstance(entity, Subscription)

        return entity

    def create(self, entity: Entity, *, emit_mode: Optional[EmitMode] = None) -> None:
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

        if emit_mode:
            self._graph.emit_mcps(mcps, emit_mode=emit_mode)
        else:
            self._graph.emit_mcps(mcps)

    def upsert(self, entity: Entity, *, emit_mode: Optional[EmitMode] = None) -> None:
        if entity._prev_aspects is None and self._graph.exists(str(entity.urn)):
            warnings.warn(
                f"The entity {entity.urn} already exists. This operation will partially overwrite the existing entity.",
                IngestionAttributionWarning,
                stacklevel=2,
            )
            # TODO: If there are no previous aspects but the entity exists, should we delete aspects that are not present here?

        mcps = entity.as_mcps(models.ChangeTypeClass.UPSERT)
        if emit_mode:
            self._graph.emit_mcps(mcps, emit_mode=emit_mode)
        else:
            self._graph.emit_mcps(mcps)

    def update(
        self,
        entity: Union[Entity, MetadataPatchProposal],
        *,
        emit_mode: Optional[EmitMode] = None,
    ) -> None:
        if isinstance(entity, MetadataPatchProposal):
            return self._update_patch(entity)

        if entity._prev_aspects is None:
            raise SdkUsageError(
                f"For entities created via {entity.__class__.__name__}(...), use client.entities.create() or client.entities.upsert() instead"
            )

        # TODO: respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "update" (e.g. if not modified) or "update_force"

        mcps = entity.as_mcps(models.ChangeTypeClass.UPSERT)
        if emit_mode:
            self._graph.emit_mcps(mcps, emit_mode=emit_mode)
        else:
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

    def delete(
        self,
        urn: UrnOrStr,
        check_exists: bool = True,
        cascade: bool = False,
        hard: bool = False,
    ) -> None:
        """Delete an entity by its urn.

        Args:
            urn: The urn of the entity to delete. Can be a string or :py:class:`Urn` object.
            check_exists: Whether to check if the entity exists before deletion. Defaults to True.
            cascade: Whether to cascade delete related entities. When True, deletes child entities
                like datajobs within dataflows, datasets within containers, etc. Not yet supported.
            hard: Whether to perform a hard delete (permanent) or soft delete. Defaults to False.

        Raises:
            SdkUsageError: If the entity does not exist and check_exists is True, or if cascade is True (not supported).

        Note:
            When hard is True, the operation is irreversible and the entity will be permanently removed.

            Impact of cascade deletion (still to be done) depends on the input entity type:
            - Container: Recursively deletes all containers and data assets within the container.
            - Dataflow: Recursively deletes all data jobs within the dataflow.
            - Dashboard: TBD
            - DataPlatformInstance: TBD
            - ...
        """
        urn_str = str(urn) if isinstance(urn, Urn) else urn
        if check_exists and not self._graph.exists(entity_urn=urn_str):
            raise SdkUsageError(
                f"Entity {urn_str} does not exist, and hence cannot be deleted. "
                "You can bypass this check by setting check_exists=False."
            )

        if cascade:
            raise SdkUsageError("The 'cascade' parameter is not yet supported.")

        self._graph.delete_entity(urn=urn_str, hard=hard)
