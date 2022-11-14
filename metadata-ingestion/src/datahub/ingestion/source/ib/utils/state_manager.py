import pickle
import zlib
from typing import Callable, Dict, Optional, Set, Type, cast

from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.redash_state import (
    RedashCheckpointsList,
    RedashCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfigBase,
    StateType,
)


class StateManager:
    _config: StatefulIngestionConfigBase
    _ctx: PipelineContext
    _job_id_prefix: str
    _platform_instance: str

    _last_states_factory: Callable[[JobId, Type[StateType]], Optional[Checkpoint]]
    _last_state_loaded: bool = False
    _last_state: Dict[str, Set[int]] = None

    _current_states_factory: Callable[[JobId], Optional[Checkpoint]]
    _current_state_created: bool = False
    _current_state: Dict[str, Set[int]] = None

    def __init__(
        self,
        config: StatefulIngestionConfigBase,
        ctx: PipelineContext,
        job_id_prefix: str,
        platform_instance: str,
        last_states_factory: Callable[[JobId, Type[StateType]], Optional[Checkpoint]],
        current_states_factory: Callable[[JobId], Optional[Checkpoint]],
    ):
        self._config = config
        self._ctx = ctx
        self._job_id_prefix = job_id_prefix
        self._platform_instance = platform_instance
        self._last_states_factory = last_states_factory
        self._current_states_factory = current_states_factory

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        assert self._ctx.pipeline_name is not None
        if job_id.startswith(self._job_id_prefix):
            if job_id == self._get_checkpoints_list_job_id():
                state = RedashCheckpointsList()
            else:
                state = RedashCheckpointState()

            return Checkpoint(
                job_name=job_id,
                pipeline_name=self._ctx.pipeline_name,
                platform_instance_id=self._platform_instance,
                run_id=self._ctx.run_id,
                config=self._config,
                state=state,
            )
        return None

    def current_add_workunit(self, urn: str, wu: WorkUnit):
        state = self._get_current_state()
        if state is None:
            return

        workunits = state.get(urn, set())
        workunits.add(StateManager._hash_workunit(wu))
        state[urn] = workunits

    def last_has_workunit(self, urn: str, wu: WorkUnit) -> bool:
        state = self._get_last_state()
        if state is None:
            return False

        workunit_hash = StateManager._hash_workunit(wu)
        return workunit_hash in state.get(urn, set())

    def get_deleted_urns(self) -> Set[str]:
        last_state = self._get_last_state()
        current_state = self._get_current_state()

        if last_state is not None and current_state is not None:
            return last_state.keys() - current_state.keys()
        else:
            return set()

    def save_state(self):
        state = self._get_current_state()
        if (
            self._config.stateful_ingestion is None
            or self._config.stateful_ingestion.ignore_new_state
            or state is None
        ):
            return

        cur_checkpoint_list = self._current_states_factory(
            self._get_checkpoints_list_job_id()
        )

        if cur_checkpoint_list is None:
            return

        cur_checkpoint_list_state = cast(
            RedashCheckpointsList, cur_checkpoint_list.state
        )

        checkpoint_id: int = 0
        cur_checkpoint = self._current_states_factory(
            self._get_checkpoint_job_id(checkpoint_id)
        )
        cur_checkpoint_state = cast(RedashCheckpointState, cur_checkpoint.state)
        cur_checkpoint_list_state.add_id(checkpoint_id)

        for urn, wu_list in state.items():
            cur_checkpoint_state.add_entry(urn, wu_list)
            if cur_checkpoint_state.size() >= 15000:
                checkpoint_id += 1
                cur_checkpoint = self._current_states_factory(
                    self._get_checkpoint_job_id(checkpoint_id)
                )
                cur_checkpoint_state = cast(RedashCheckpointState, cur_checkpoint.state)
                cur_checkpoint_list_state.add_id(checkpoint_id)

    def _get_last_state(self) -> Dict[str, Set[int]]:
        if self._last_state_loaded:
            return self._last_state

        last_checkpoints_list = self._last_states_factory(
            self._get_checkpoints_list_job_id(), RedashCheckpointsList
        )
        last_checkpoints_list_state = (
            cast(RedashCheckpointsList, last_checkpoints_list.state)
            if last_checkpoints_list is not None
            else None
        )
        if last_checkpoints_list_state is None:
            self._last_state_loaded = True
            return None

        self._last_state = dict()
        for checkpoint_id in last_checkpoints_list_state.get_ids():
            checkpoint = self._last_states_factory(
                self._get_checkpoint_job_id(checkpoint_id), RedashCheckpointState
            )
            checkpoint_state = cast(RedashCheckpointState, checkpoint.state)
            for urn, wu_list in checkpoint_state.get_entries().items():
                self._last_state[urn] = wu_list

        self._last_state_loaded = True
        return self._last_state

    def _get_current_state(self) -> Dict[str, Set[int]]:
        if self._current_state_created:
            return self._current_state

        if (
            self._config.stateful_ingestion
            and not self._config.stateful_ingestion.ignore_new_state
        ):
            self._current_state = dict()
        else:
            self._current_state = None
        self._current_state_created = True
        return self._current_state

    def _get_checkpoints_list_job_id(self):
        return self._job_id_prefix + "checkpoints_list"

    def _get_checkpoint_job_id(self, checkpoint_id: int):
        return self._job_id_prefix + str(checkpoint_id)

    @staticmethod
    def _hash_workunit(wu: WorkUnit):
        return zlib.crc32(pickle.dumps(wu)) & 0xFFFFFFFF
