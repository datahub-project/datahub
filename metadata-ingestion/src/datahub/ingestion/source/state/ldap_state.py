import pydantic
from typing import Iterable, List, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)


class BaseLdapCheckpointState(
    StaleEntityCheckpointStateBase["BaseLdapCheckpointState"]
):
    """
    Base class for representing the checkpoint state for all LDAP based sources.
    Stores all ldap_users being ingested and is used to remove any stale entities.
    """

    encoded_ldap_users: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["corpuser"]

    @staticmethod
    def _get_urns_not_in(
        encoded_ldap_users_1: List[str], encoded_ldap_users_2: List[str]
    ) -> Iterable[str]:
        difference = set(encoded_ldap_users_1) - set(encoded_ldap_users_2)
        for ldap_user in difference:
            yield ldap_user

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "BaseLdapCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        if type == "corpuser":
            yield from self._get_urns_not_in(
                self.encoded_ldap_users, other_checkpoint_state.encoded_ldap_users
            )

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        assert type in self.get_supported_types()
        self.encoded_ldap_users.append(urn)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "BaseLdapCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [(self.encoded_ldap_users, old_checkpoint_state.encoded_ldap_users)]
        )
