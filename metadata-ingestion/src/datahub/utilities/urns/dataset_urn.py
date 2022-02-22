from typing import List, Set

from datahub.metadata.schema_classes import FabricTypeClass
from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DatasetUrn(Urn):
    """
    expected dataset urn format: urn:li:dataset:(<platform_urn_str>,<table_name>,env). example:
    urn:li:dataset:(urn:li:dataPlatform:hive,member,prod)
    """

    ENTITY_TYPE: str = "dataset"
    VALID_FABRIC_SET: Set[str] = set(
        [
            str(getattr(FabricTypeClass, attr)).upper()
            for attr in dir(FabricTypeClass)
            if not callable(getattr(FabricTypeClass, attr)) and not attr.startswith("_")
        ]
    )

    def __init__(self, entity_type: str, entity_id: List[str], domain: str = "li"):
        super().__init__(entity_type, entity_id, domain)

    @classmethod
    def create_from_string(cls, urn_str: str) -> "DatasetUrn":
        """
        Create a DatasetUrn from the its string representation
        :param urn_str: the string representation of the DatasetUrn
        :return: DatasetUrn of the given string representation
        :raises InvalidUrnError is the string representation is in invalid format
        """
        urn: Urn = super().create_from_string(urn_str)
        return cls(urn.get_type(), urn.get_entity_id(), urn.get_domain())

    def get_data_platform_urn(self) -> DataPlatformUrn:
        """
        :return: the DataPlatformUrn of where the Dataset is created
        """
        return DataPlatformUrn.create_from_string(self.get_entity_id()[0])

    def get_dataset_name(self) -> str:
        """
        :return: the dataset name from this DatasetUrn
        """
        return self.get_entity_id()[1]

    def get_env(self) -> str:
        """
        :return: the environment where the Dataset is created
        """
        return self.get_entity_id()[2]

    @classmethod
    def create_from_ids(
        cls, platform_id: str, table_name: str, env: str
    ) -> "DatasetUrn":
        entity_id: List[str] = [
            str(DataPlatformUrn.create_from_id(platform_id)),
            table_name,
            env,
        ]
        return cls(DatasetUrn.ENTITY_TYPE, entity_id)

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type != DatasetUrn.ENTITY_TYPE:
            raise InvalidUrnError(
                f"Entity type should be {DatasetUrn.ENTITY_TYPE} but found {entity_type}"
            )

    @staticmethod
    def _validate_entity_id(entity_id: List[str]) -> None:
        # expected entity id format (<platform_urn>,<table_name>,<env>)
        if len(entity_id) != 3:
            raise InvalidUrnError(
                f"Expect 3 parts in the entity id but found {entity_id}"
            )

        platform_urn_str = entity_id[0]

        DataPlatformUrn.validate(platform_urn_str)
        env = entity_id[2].upper()
        if env not in DatasetUrn.VALID_FABRIC_SET:
            raise InvalidUrnError(
                f"Invalid env:{env}. Allowed evn are {DatasetUrn.VALID_FABRIC_SET}"
            )
