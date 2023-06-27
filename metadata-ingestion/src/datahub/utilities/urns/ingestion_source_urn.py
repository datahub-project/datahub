from typing import List

from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class IngestionSourceUrn(Urn):
    """
    expected ingestion source urn format: urn:li:dataHubIngestionSource:<source_id>.
    Example: "urn:li:dataHubIngestionSource:cli-1896c8e224615b139e3d81fe60b7f3fd"
    """

    ENTITY_TYPE: str = "dataHubIngestionSource"

    def __init__(
            self, entity_type: str, entity_id: List[str], domain: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, domain)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "IngestionSourceUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_id(cls, domain_id: str) -> "IngestionSourceUrn":
        return cls(IngestionSourceUrn.ENTITY_TYPE, [domain_id])

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != IngestionSourceUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {IngestionSourceUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        if len(entity_id) != 1:
            raise InvalidUrnError(
                f"Expect 1 part in entity id, but found{len(entity_id)}"
            )
