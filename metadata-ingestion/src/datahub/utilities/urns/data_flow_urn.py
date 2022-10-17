from typing import List

from datahub.configuration.source_common import ALL_ENV_TYPES
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DataFlowUrn(Urn):
    """
    expected data flow urn format: urn:li:dataFlow:(<orchestrator>,<flow_id>,<env>). example:
    urn:li:dataFlow:(airflow,ingest_user,prod)
    """

    ENTITY_TYPE: str = "dataFlow"

    def __init__(
        self, entity_type: str, entity_id: List[str], domain: str = Urn.LI_DOMAIN
    ):
        super().__init__(entity_type, entity_id, domain)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "DataFlowUrn":
        """
        Create a DataFlowUrn from the its string representation
        :param urn_str: the string representation of the DataFlowUrn
        :return: DataFlowUrn of the given string representation
        :raises InvalidUrnError is the string representation is in invalid format
        """
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    def get_orchestrator_name(self) -> str:
        """
        :return: the orchestrator name for the Dataflow
        """
        return self.get_entity_id()[0]

    def get_flow_id(self) -> str:
        """
        :return: the data flow id from this DataFlowUrn
        """
        return self.get_entity_id()[1]

    def get_env(self) -> str:
        """
        :return: the environment where the DataFlow is run
        """
        return self.get_entity_id()[2]

    @classmethod
    def create_from_ids(
        cls, orchestrator: str, flow_id: str, env: str
    ) -> "DataFlowUrn":
        entity_id: List[str] = [
            orchestrator,
            flow_id,
            env,
        ]
        return cls(DataFlowUrn.ENTITY_TYPE, entity_id)

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != DataFlowUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {DataFlowUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        # expected entity id format (<platform_urn>,<table_name>,<env>)
        if len(entity_id) != 3:
            raise InvalidUrnError(
                f"Expect 3 parts in the entity id but found {entity_id}"
            )

        env = entity_id[2].upper()
        if env not in ALL_ENV_TYPES:
            raise InvalidUrnError(f"Invalid env:{env}. Allowed evn are {ALL_ENV_TYPES}")
