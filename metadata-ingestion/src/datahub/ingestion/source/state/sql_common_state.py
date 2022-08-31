from typing import Iterable, List

import pydantic

from datahub.emitter.mce_builder import (
    assertion_urn_to_key,
    container_urn_to_key,
    make_assertion_urn,
    make_container_urn,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil


class BaseSQLAlchemyCheckpointState(
    StaleEntityCheckpointStateBase["BaseSQLAlchemyCheckpointState"]
):
    """
    Base class for representing the checkpoint state for all SQLAlchemy based sources.
    Stores all tables and views being ingested and is used to remove any stale entities.
    Subclasses can define additional state as appropriate.
    """

    encoded_table_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_view_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_container_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_assertion_urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["assertion", "container", "table", "view"]

    @staticmethod
    def _get_lightweight_repr(dataset_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        return CheckpointStateUtil.get_dataset_lightweight_repr(dataset_urn)

    @staticmethod
    def _get_container_lightweight_repr(container_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        key = container_urn_to_key(container_urn)
        assert key is not None
        return f"{key.guid}"

    @staticmethod
    def _get_container_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Iterable[str]:
        difference = CheckpointStateUtil.get_encoded_urns_not_in(
            encoded_urns_1, encoded_urns_2
        )
        for guid in difference:
            yield make_container_urn(guid)

    def _get_table_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        """Tables are mapped to DataHub dataset concept."""
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_table_urns, checkpoint.encoded_table_urns
        )

    def _get_view_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        """Views are mapped to DataHub dataset concept."""
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_view_urns, checkpoint.encoded_view_urns
        )

    def _get_assertion_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        """Tables are mapped to DataHub dataset concept."""
        diff = CheckpointStateUtil.get_encoded_urns_not_in(
            self.encoded_assertion_urns, checkpoint.encoded_assertion_urns
        )
        for assertion_id in diff:
            yield make_assertion_urn(assertion_id)

    def _add_table_urn(self, table_urn: str) -> None:
        self.encoded_table_urns.append(self._get_lightweight_repr(table_urn))

    def _add_assertion_urn(self, assertion_urn: str) -> None:
        key = assertion_urn_to_key(assertion_urn)
        assert key is not None
        self.encoded_assertion_urns.append(key.assertionId)

    def _add_view_urn(self, view_urn: str) -> None:
        self.encoded_view_urns.append(self._get_lightweight_repr(view_urn))

    def _add_container_guid(self, container_urn: str) -> None:
        self.encoded_container_urns.append(
            self._get_container_lightweight_repr(container_urn)
        )

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        assert type in self.get_supported_types()
        if type == "assertion":
            self._add_assertion_urn(urn)
        elif type == "container":
            self._add_container_guid(urn)
        elif type == "table":
            self._add_table_urn(urn)
        elif type == "view":
            self._add_view_urn(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        if type == "assertion":
            yield from self._get_assertion_urns_not_in(other_checkpoint_state)
        if type == "container":
            yield from self._get_container_urns_not_in(
                self.encoded_container_urns,
                other_checkpoint_state.encoded_container_urns,
            )
        elif type == "table":
            yield from self._get_table_urns_not_in(other_checkpoint_state)
        elif type == "view":
            yield from self._get_view_urns_not_in(other_checkpoint_state)
