from typing import List

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class TagUrn(Urn):
    """
    expected tag urn format: urn:li:tag:<tag_id>. example: "urn:li:tag:product"
    """

    ENTITY_TYPE: str = "tag"

    def __init__(
        self, entity_type: str, entity_id: List[str], tag: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, tag)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "TagUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_id(cls, tag_id: str) -> "TagUrn":
        return cls(TagUrn.ENTITY_TYPE, [tag_id])

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != TagUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {TagUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        if len(entity_id) != 1:
            raise InvalidUrnError(
                f"Expect 1 part in entity id, but found{len(entity_id)}"
            )
