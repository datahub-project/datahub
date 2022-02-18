from typing import Iterable, List

import pydantic

from datahub.emitter.mce_builder import dataset_urn_to_key, make_dataset_urn
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class KafkaCheckpointState(CheckpointStateBase):
    """
    This Class represents the checkpoint state for Kafka based sources.
    Stores all the topics being ingested and it is used to remove any stale entities.
    """

    encoded_topic_urns: List[str] = pydantic.Field(default_factory=list)

    @staticmethod
    def _get_separator() -> str:
        # Unique small string not allowed in URNs.
        return "||"

    @staticmethod
    def _get_lightweight_repr(dataset_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        SEP = KafkaCheckpointState._get_separator()
        key = dataset_urn_to_key(dataset_urn)
        assert key is not None
        return f"{key.platform}{SEP}{key.name}{SEP}{key.origin}"

    @staticmethod
    def _get_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Iterable[str]:
        difference = set(encoded_urns_1) - set(encoded_urns_2)
        for encoded_urn in difference:
            platform, name, env = encoded_urn.split(
                KafkaCheckpointState._get_separator()
            )
            yield make_dataset_urn(platform, name, env)

    def get_topic_urns_not_in(
        self, checkpoint: "KafkaCheckpointState"
    ) -> Iterable[str]:
        yield from self._get_urns_not_in(
            self.encoded_topic_urns, checkpoint.encoded_topic_urns
        )

    def add_topic_urn(self, topic_urn: str) -> None:
        self.encoded_topic_urns.append(self._get_lightweight_repr(topic_urn))
