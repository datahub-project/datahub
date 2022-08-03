from typing import Iterable, List, Set

from datahub.emitter.mce_builder import dataset_urn_to_key, make_dataset_urn


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
    def get_dataset_lightweight_repr(dataset_urn: str) -> str:
        SEP = CheckpointStateUtil.get_separator()
        key = dataset_urn_to_key(dataset_urn)
        assert key is not None
        return f"{key.platform}{SEP}{key.name}{SEP}{key.origin}"

    @staticmethod
    def get_dataset_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Iterable[str]:
        difference = CheckpointStateUtil.get_encoded_urns_not_in(
            encoded_urns_1, encoded_urns_2
        )
        for encoded_urn in difference:
            platform, name, env = encoded_urn.split(CheckpointStateUtil.get_separator())
            yield make_dataset_urn(platform, name, env)
