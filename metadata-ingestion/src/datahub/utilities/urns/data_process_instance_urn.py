from typing import List

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DataProcessInstanceUrn(Urn):
    """
    expected domain urn format: urn:li:dataProcessInstance:<dataprocessinstance_key>
    """

    ENTITY_TYPE: str = "dataProcessInstance"

    def __init__(
        self, entity_type: str, entity_id: List[str], domain_id: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, domain_id)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "DataProcessInstanceUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_id(cls, dataprocessinstance_id: str) -> "DataProcessInstanceUrn":
        return cls(DataProcessInstanceUrn.ENTITY_TYPE, [dataprocessinstance_id])

    def get_dataprocessinstance_id(self) -> str:
        """
        :return: the dataprocess instance id from this DatasetUrn
        """
        return self.get_entity_id()[0]

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != DataProcessInstanceUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {DataProcessInstanceUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        if len(entity_id) != 1:
            raise InvalidUrnError(
                f"Expect 1 part in entity id, but found{len(entity_id)}"
            )
