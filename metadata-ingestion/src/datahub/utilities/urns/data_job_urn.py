from typing import List

from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DataJobUrn(Urn):
    """
    expected Data job urn format: urn:li:dataJob:(<data_flow_urn>,<job_id>). example:
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,sample_flow,prod),sample_job)"
    """

    ENTITY_TYPE: str = "dataJob"

    def __init__(
        self, entity_type: str, entity_id: List[str], domain: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, domain)

    def get_data_flow_urn(self) -> DataFlowUrn:
        return DataFlowUrn.create_from_string(self.get_entity_id()[0])

    def get_job_id(self) -> str:
        return self.get_entity_id()[1]

    @classmethod
    def create_from_string(cls, urn_str: str) -> "DataJobUrn":
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    @classmethod
    def create_from_ids(cls, data_flow_urn: str, job_id: str) -> "DataJobUrn":
        return cls(DataJobUrn.ENTITY_TYPE, [data_flow_urn, job_id])

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != DataJobUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {DataJobUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        if len(entity_id) != 2:
            raise InvalidUrnError(
                f"Expect 2 part in entity id, but found{len(entity_id)}"
            )

        data_flow_urn_str = entity_id[0]
        DataFlowUrn.validate(data_flow_urn_str)
