from typing import List

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DataPlatformUrn(Urn):
    """
    expected dataset urn format: urn:li:dataPlatform:<platform_name>. example: "urn:li:dataPlatform:hive"
    """

    ENTITY_TYPE: str = "dataPlatform"

    def __init__(self, entity_type: str, entity_id: List[str], domain: str = "li"):
        super().__init__(entity_type, entity_id, domain)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "DataPlatformUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_id(cls, platform_id: str) -> "DataPlatformUrn":
        return cls(DataPlatformUrn.ENTITY_TYPE, [platform_id])

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != DataPlatformUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {DataPlatformUrn.ENTITY_TYPE} but found {entity_type}"
            )

    def get_platform_name(self) -> str:
        return self.get_entity_id()[0]
