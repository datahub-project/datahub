from typing import Iterable, List

import pydantic

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.urns.urn import guess_entity_type


class LdapCheckpointState(StaleEntityCheckpointStateBase["LdapCheckpointState"]):
    """
    Base class for representing the checkpoint state for all LDAP based sources.
    Stores all corpuser and corpGroup and being ingested and is used to remove any stale entities.
    """

    urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["corpuser", "corpGroup"]

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        assert type in self.get_supported_types()
        self.urns.append(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "LdapCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        diff = set(self.urns) - set(other_checkpoint_state.urns)
        yield from (urn for urn in diff if guess_entity_type(urn) == type)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "LdapCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [(self.urns, old_checkpoint_state.urns)]
        )
