import logging
from collections import defaultdict
from typing import Optional, cast

import pydantic

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.profiling_state import ProfilingCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)

logger: logging.Logger = logging.getLogger(__name__)


class ProfilingStatefulIngestionConfig(StatefulIngestionConfig):
    """
    Base specialized config of Stateful Profiling.
    """


class ProfilingHandler(StatefulIngestionUsecaseHandlerBase[ProfilingCheckpointState]):
    """
    The stateful ingestion helper class that handles skipping redundant runs.
    This contains the generic logic for all sources that need to support skipping redundant runs.
    """

    INVALID_TIMESTAMP_VALUE: pydantic.PositiveInt = 1

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase[ProfilingStatefulIngestionConfig],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.state_provider = source.state_provider
        self.stateful_ingestion_config: Optional[
            ProfilingStatefulIngestionConfig
        ] = config.stateful_ingestion
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.checkpointing_enabled: bool = (
            self.state_provider.is_stateful_ingestion_configured()
        )
        self._job_id = self._init_job_id()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    def _ignore_old_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_old_state
        ):
            return True
        return False

    def _ignore_new_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_new_state
        ):
            return True
        return False

    def _init_job_id(self) -> JobId:
        return JobId("profiling")

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def is_checkpointing_enabled(self) -> bool:
        return self.checkpointing_enabled

    def create_checkpoint(self) -> Optional[Checkpoint[ProfilingCheckpointState]]:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return None

        assert self.pipeline_name is not None
        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=ProfilingCheckpointState(last_profiled=defaultdict()),
        )

    def get_current_state(self) -> Optional[ProfilingCheckpointState]:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return None
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(ProfilingCheckpointState, cur_checkpoint.state)
        return cur_state

    def add_to_state(
        self,
        urn: str,
        profile_time_millis: pydantic.PositiveInt,
    ) -> None:
        cur_state = self.get_current_state()
        if cur_state:
            cur_state.last_profiled[urn] = profile_time_millis

    def get_last_state(self) -> Optional[ProfilingCheckpointState]:
        if not self.is_checkpointing_enabled() or self._ignore_old_state():
            return None
        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, ProfilingCheckpointState
        )
        if last_checkpoint and last_checkpoint.state:
            return cast(ProfilingCheckpointState, last_checkpoint.state)

        return None

    def get_last_profiled(self, urn: str) -> Optional[pydantic.PositiveInt]:
        state = self.get_last_state()
        if state:
            return state.last_profiled.get(urn)

        return None
