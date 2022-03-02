from typing import Iterable, List

import pydantic

from datahub.emitter.mce_builder import (
    container_urn_to_key,
    dataset_urn_to_key,
    make_container_urn,
    make_dataset_urn,
)
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class BaseSQLAlchemyCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all SQLAlchemy based sources.
    Stores all tables and views being ingested and is used to remove any stale entities.
    Subclasses can define additional state as appropriate.
    """

    encoded_table_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_view_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_container_urns: List[str] = pydantic.Field(default_factory=list)

    @staticmethod
    def _get_separator() -> str:
        # Unique small string not allowed in URNs.
        return "||"

    @staticmethod
    def _get_lightweight_repr(dataset_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        SEP = BaseSQLAlchemyCheckpointState._get_separator()
        key = dataset_urn_to_key(dataset_urn)
        assert key is not None
        return f"{key.platform}{SEP}{key.name}{SEP}{key.origin}"

    @staticmethod
    def _get_container_lightweight_repr(container_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        key = container_urn_to_key(container_urn)
        assert key is not None
        return f"{key.guid}"

    @staticmethod
    def _get_dataset_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Iterable[str]:
        difference = set(encoded_urns_1) - set(encoded_urns_2)
        for encoded_urn in difference:
            platform, name, env = encoded_urn.split(
                BaseSQLAlchemyCheckpointState._get_separator()
            )
            yield make_dataset_urn(platform, name, env)

    @staticmethod
    def _get_container_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Iterable[str]:
        difference = set(encoded_urns_1) - set(encoded_urns_2)
        for guid in difference:
            yield make_container_urn(guid)

    def get_table_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        yield from self._get_dataset_urns_not_in(
            self.encoded_table_urns, checkpoint.encoded_table_urns
        )

    def get_view_urns_not_in(
        self, checkpoint: "BaseSQLAlchemyCheckpointState"
    ) -> Iterable[str]:
        yield from self._get_dataset_urns_not_in(
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
