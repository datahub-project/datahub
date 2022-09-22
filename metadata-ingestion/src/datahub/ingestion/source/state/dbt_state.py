import logging
from typing import Callable, Dict, Iterable, List

import pydantic

from datahub.emitter.mce_builder import make_assertion_urn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class DbtCheckpointState(StaleEntityCheckpointStateBase["DbtCheckpointState"]):
    """
    Class for representing the checkpoint state for DBT sources.
    Stores all nodes and assertions being ingested and is used to remove any stale entities.
    """

    encoded_node_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_assertion_urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["assertion", "dataset"]

    @staticmethod
    def _get_assertion_lightweight_repr(assertion_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        urn = Urn.create_from_string(assertion_urn)
        key = urn.get_entity_id_as_string()
        assert key is not None
        return key

    def _add_assertion_urn(self, assertion_urn: str) -> None:
        self.encoded_assertion_urns.append(
            self._get_assertion_lightweight_repr(assertion_urn)
        )

    def _get_assertion_urns_not_in(
        self, checkpoint: "DbtCheckpointState"
    ) -> Iterable[str]:
        """
        Dbt assertion are mapped to DataHub assertion concept
        """
        difference = CheckpointStateUtil.get_encoded_urns_not_in(
            self.encoded_assertion_urns, checkpoint.encoded_assertion_urns
        )
        for key in difference:
            yield make_assertion_urn(key)

    def _get_node_urns_not_in(self, checkpoint: "DbtCheckpointState") -> Iterable[str]:
        """
        Dbt node are mapped to DataHub dataset concept
        """
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_node_urns, checkpoint.encoded_node_urns
        )

    def _add_node_urn(self, node_urn: str) -> None:
        self.encoded_node_urns.append(
            CheckpointStateUtil.get_dataset_lightweight_repr(node_urn)
        )

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        supported_entities_add_handlers: Dict[str, Callable[[str], None]] = {
            "dataset": self._add_node_urn,
            "assertion": self._add_assertion_urn,
        }

        if type not in supported_entities_add_handlers:
            logger.error(f"Can not save Unknown entity {type} to checkpoint.")

        supported_entities_add_handlers[type](urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "DbtCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        if type == "dataset":
            yield from self._get_node_urns_not_in(other_checkpoint_state)
        elif type == "assertion":
            yield from self._get_assertion_urns_not_in(other_checkpoint_state)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "DbtCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [
                (self.encoded_node_urns, old_checkpoint_state.encoded_node_urns),
                (
                    self.encoded_assertion_urns,
                    old_checkpoint_state.encoded_assertion_urns,
                ),
            ]
        )

    def prepare_for_commit(self) -> None:
        self.encoded_node_urns = list(set(self.encoded_node_urns))
        self.encoded_assertion_urns = list(set(self.encoded_assertion_urns))
