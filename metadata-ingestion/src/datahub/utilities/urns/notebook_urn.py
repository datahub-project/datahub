from typing import List

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class NotebookUrn(Urn):
    """
    expected dataset urn format: urn:li:notebook:(<platform_name>,<notebook_id>). example: "urn:li:notebook:(querybook,1234)"
    """

    ENTITY_TYPE: str = "notebook"

    def __init__(
        self, entity_type: str, entity_id: List[str], domain: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, domain)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "NotebookUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_ids(cls, platform_id: str, notebook_id: str) -> "NotebookUrn":
        return cls(NotebookUrn.ENTITY_TYPE, [platform_id, notebook_id])

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != NotebookUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {NotebookUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        if len(entity_id) != 2:
            raise InvalidUrnError(
                f"Expect 2 parts in entity id, but found{len(entity_id)}"
            )

    def get_platform_id(self) -> str:
        return self.get_entity_id()[0]

    def get_notebook_id(self) -> str:
        return self.get_entity_id()[1]
