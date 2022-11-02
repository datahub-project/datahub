import logging
from typing import Iterable, List

import pydantic

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


class UnityCatalogCheckpointState(
    StaleEntityCheckpointStateBase["UnityCatalogCheckpointState"]
):
    """
    Class for representing the checkpoint state for UnityCatalog sources.
    Stores all datasets and containers being ingested and is
    used to remove any stale entities.
    """

    urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["*"]

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        if urn not in self.urns:
            self.urns.append(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "UnityCatalogCheckpointState"
    ) -> Iterable[str]:
        diff = set(self.urns) - set(other_checkpoint_state.urns)

        # To maintain backwards compatibility, we provide this filtering mechanism.
        if type == "*":
            yield from diff
        else:
            yield from (urn for urn in diff if guess_entity_type(urn) == type)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "UnityCatalogCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [(self.urns, old_checkpoint_state.urns)]
        )
