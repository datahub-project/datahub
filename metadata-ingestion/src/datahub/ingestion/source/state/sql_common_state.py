from typing import Iterable, List

import pydantic

from datahub.emitter.mce_builder import container_urn_to_key, make_container_urn
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil


class BaseSQLAlchemyCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all SQLAlchemy based sources.
    Stores all tables and views being ingested and is used to remove any stale entities.
    Subclasses can define additional state as appropriate.
    """

    encoded_table_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_view_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_container_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_assertion_urns: List[str] = pydantic.Field(default_factory=list)

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

    def get_table_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        """Tables are mapped to DataHub dataset concept."""
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_table_urns, checkpoint.encoded_table_urns
        )

    def get_view_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        """Views are mapped to DataHub dataset concept."""
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_view_urns, checkpoint.encoded_view_urns
        )

    def get_container_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        yield from self._get_container_urns_not_in(
            self.encoded_container_urns, checkpoint.encoded_container_urns
        )

    def add_table_urn(self, table_urn: str) -> None:
        self.encoded_table_urns.append(self._get_lightweight_repr(table_urn))

    def add_view_urn(self, view_urn: str) -> None:
        self.encoded_view_urns.append(self._get_lightweight_repr(view_urn))

    def add_container_guid(self, container_urn: str) -> None:
        self.encoded_container_urns.append(
            self._get_container_lightweight_repr(container_urn)
        )
