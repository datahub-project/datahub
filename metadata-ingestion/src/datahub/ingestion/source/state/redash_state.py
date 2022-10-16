import base64
import bz2
import functools
import zlib
from typing import Callable, Dict, Iterable, Set

import pickle

from datahub.ingestion.api.common import WorkUnit
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class RedashCheckpointState(CheckpointStateBase):

    workunits_hash_by_urn: Dict[str, Set[int]] = None
    b85encoded_workunits_hash: str = None

    def get_urns_not_in(self, checkpoint: "RedashCheckpointState") -> Iterable[str]:
        return self.get_workunits().keys() - checkpoint.get_workunits().keys()

    def has_workunit(self, urn: str, wu: WorkUnit):
        workunit_hash = RedashCheckpointState._hash_workunit(wu)
        return workunit_hash in self.get_workunits().get(urn, set())

    def add_workunit(self, urn: str, wu: WorkUnit) -> None:
        workunits = self.get_workunits().get(urn, set())
        workunits.add(RedashCheckpointState._hash_workunit(wu))
        self.get_workunits()[urn] = workunits

    def get_workunits(self) -> Dict[str, Set[int]]:
        # Lazy decode workunits
        if (
            self.workunits_hash_by_urn is None
            and self.b85encoded_workunits_hash is None
        ):
            self.workunits_hash_by_urn = dict()
        if (
            self.workunits_hash_by_urn is None
            and self.b85encoded_workunits_hash is not None
        ):
            self.workunits_hash_by_urn = RedashCheckpointState._decompress_workunits(
                self.b85encoded_workunits_hash
            )
            self.b85encoded_workunits_hash = None
        return self.workunits_hash_by_urn

    @staticmethod
    def _hash_workunit(wu: WorkUnit) -> int:
        return zlib.crc32(pickle.dumps(wu)) & 0xFFFFFFFF

    @staticmethod
    def _compress_workunits(workunits: Dict[str, Set[int]]) -> str:
        return base64.b85encode(bz2.compress(pickle.dumps(workunits), compresslevel=9)).decode("utf-8")

    @staticmethod
    def _decompress_workunits(compressed_workunits: str) -> Dict[str, Set[int]]:
        return pickle.loads(bz2.decompress(base64.b85decode(compressed_workunits)))

    # Custom serialization with compression
    def to_bytes(
        self,
        compressor: Callable[[bytes], bytes] = functools.partial(
            bz2.compress, compresslevel=9
        ),
        # fmt: off
            # 4 MB
            max_allowed_state_size: int = 2**22,
        # fmt: on
    ) -> bytes:
        self.b85encoded_workunits_hash = RedashCheckpointState._compress_workunits(
            self.get_workunits()
        )
        self.workunits_hash_by_urn = None
        return super().to_bytes(compressor, max_allowed_state_size)
