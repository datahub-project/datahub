from typing import TYPE_CHECKING

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, Urn
from datahub.sdk._shared import Entity, UrnOrStr
from datahub.specific.dataset import DatasetPatchBuilder

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

    def create(self, dataset: "Dataset") -> None:
        mcps = []

        # By putting this first, we can ensure that the request fails if the entity already exists?
        # TODO: actually validate that this works.
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataset.urn),
                aspect=dataset.urn.to_key_aspect(),
                changeType=models.ChangeTypeClass.CREATE_ENTITY,
            )
        )

        mcps.extend(dataset._as_mcps(models.ChangeTypeClass.CREATE))

        self._graph.emit_mcps(mcps)

    def upsert(self, entity: Entity) -> None:
        mcps = entity._as_mcps(models.ChangeTypeClass.UPSERT)

        # TODO respect If-Unmodified-Since?
        # -> probably add a "mode" parameter that can be "upsert" or "update" (e.g. if not modified) or "update_force"

        self._graph.emit_mcps(mcps)

    def update(
        self, dataset: DatasetPatchBuilder, *, check_exists: bool = True
    ) -> None:
        if check_exists and not self._graph.exists(dataset.urn):
            raise SdkUsageError(
                f"Dataset {dataset.urn} does not exist, and hence cannot be updated. "
                "You can bypass this check by setting check_exists=False."
            )

        mcps = dataset.build()
        self._graph.emit_mcps(mcps)
