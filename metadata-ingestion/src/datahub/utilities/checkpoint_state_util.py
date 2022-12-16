from typing import List, Set

from datahub.emitter.mce_builder import dataset_key_to_urn, make_dataset_urn
from datahub.metadata.schema_classes import DatasetKeyClass


class CheckpointStateUtil:
    """
    A utility class to provide common functionalities around Urn and CheckPointState of different DataHub entities
    """

    @staticmethod
    def get_separator() -> str:
        # Unique small string not allowed in URNs.
        return "||"

    @staticmethod
    def get_encoded_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Set[str]:
        return set(encoded_urns_1) - set(encoded_urns_2)

    @staticmethod
    def get_urn_from_encoded_dataset(encoded_urn: str) -> str:
        platform, name, env = encoded_urn.split(CheckpointStateUtil.get_separator())
        return dataset_key_to_urn(
            DatasetKeyClass(platform=platform, name=name, origin=env)
        )

    @staticmethod
    def get_urn_from_encoded_topic(encoded_urn: str) -> str:
        platform, name, env = encoded_urn.split(CheckpointStateUtil.get_separator())
        return make_dataset_urn(platform, name, env)
