from typing import Iterable, List

import pydantic

from datahub.emitter.mce_builder import dataset_urn_to_key, make_dataset_urn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)


class KafkaCheckpointState(StaleEntityCheckpointStateBase["KafkaCheckpointState"]):
    """
    This Class represents the checkpoint state for Kafka based sources.
    Stores all the topics being ingested and it is used to remove any stale entities.
    """

    encoded_topic_urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["topic"]

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

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        assert type in self.get_supported_types()
        if type == "topic":
            self.encoded_topic_urns.append(self._get_lightweight_repr(urn))

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "KafkaCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        if type == "topic":
            yield from self._get_urns_not_in(
                self.encoded_topic_urns, other_checkpoint_state.encoded_topic_urns
            )

    def get_percent_entities_changed(
        self, old_checkpoint_state: "KafkaCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [(self.encoded_topic_urns, old_checkpoint_state.encoded_topic_urns)]
        )
