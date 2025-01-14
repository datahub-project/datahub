from typing import TYPE_CHECKING

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, Urn
from datahub.sdk._shared import Entity, UrnOrStr

if TYPE_CHECKING:
    from datahub.sdk.dataset import Dataset


class SDKGraph:
    def __init__(self, graph: DataHubGraph):
        self._graph = graph

    # TODO: Make things more generic across entity types.

    # TODO: Make all of these methods sync by default.

    def get_dataset(self, urn: UrnOrStr) -> "Dataset":
        from datahub.sdk._all_entities import ENTITY_CLASSES
        from datahub.sdk.dataset import Dataset

        if not isinstance(urn, Urn):
            urn = Urn.from_string(urn)

        assert isinstance(urn, DatasetUrn)

        assert ENTITY_CLASSES
        aspects = self._graph.get_entity_semityped(str(urn))

        # TODO get the right entity type subclass
        # TODO: save the timestamp so we can use If-Unmodified-Since on the updates
        return Dataset._new_from_graph(urn, aspects)

    def create(self, entity: Entity) -> None:
        mcps = []

        # By putting this first, we can ensure that the request fails if the entity already exists?
        # TODO: actually validate that this works.
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
        mcps = entity._as_mcps(models.ChangeTypeClass.UPSERT)

        # TODO respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "upsert" or "update" (e.g. if not modified) or "update_force"

        self._graph.emit_mcps(mcps)

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
