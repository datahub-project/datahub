from typing import List, Optional

from datahub.configuration.source_common import ALL_ENV_TYPES
from datahub.utilities.urn_encoder import UrnEncoder
from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn


class DatasetUrn(Urn):
    """
    expected dataset urn format: urn:li:dataset:(<platform_urn_str>,<table_name>,env). example:
    urn:li:dataset:(urn:li:dataPlatform:hive,member,prod)
    """

    ENTITY_TYPE: str = "dataset"

    def __init__(self, entity_type: str, entity_id: List[str], domain: str = "li"):
        super().__init__(entity_type, UrnEncoder.encode_string_array(entity_id), domain)

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
        cls,
        platform_id: str,
        table_name: str,
        env: str,
        platform_instance: Optional[str] = None,
    ) -> "DatasetUrn":
        entity_id: List[str]
        if platform_instance:
            entity_id = [
                str(DataPlatformUrn.create_from_id(platform_id)),
                f"{platform_instance}.{table_name}",
                env,
            ]
        else:
            entity_id = [
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
        if env not in ALL_ENV_TYPES:
            raise InvalidUrnError(
                f"Invalid env:{env}. Allowed envs are {ALL_ENV_TYPES}"
            )

    """A helper function to extract simple . path notation from the v2 field path"""

    @staticmethod
    def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
        if field_path.startswith("[version=2.0]"):
            # this is a v2 field path
            tokens = [
                t
                for t in field_path.split(".")
                if not (t.startswith("[") or t.endswith("]"))
            ]
            path = ".".join(tokens)
            return path
        else:
            # not a v2, we assume this is a simple path
            return field_path
