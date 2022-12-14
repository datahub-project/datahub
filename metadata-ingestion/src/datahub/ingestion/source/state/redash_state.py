from typing import Dict, List, Set

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class RedashCheckpointsList(CheckpointStateBase):
    state_ids: List[int] = list()

    def get_ids(self) -> List[int]:
        return self.state_ids

    def add_id(self, checkpoint_id: int):
        self.state_ids.append(checkpoint_id)


class RedashCheckpointState(CheckpointStateBase):
    workunits_hash_by_urn: Dict[str, Set[int]] = dict()

    def get_entries(self) -> Dict[str, Set[int]]:
        return self.workunits_hash_by_urn

    def add_entry(self, urn: str, wu_list: Set[int]):
        self.workunits_hash_by_urn[urn] = wu_list

    def size(self) -> int:
        return len(self.workunits_hash_by_urn)
