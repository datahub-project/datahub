import logging
from collections import defaultdict
from typing import Dict, Optional, cast

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


class StatefulProfilingConfig(StatefulIngestionConfig):
    """
    Base specialized config of Stateful Ingestion to skip redundant runs.
    """

    # Defines the alias 'force_rerun' for ignore_old_state field.
    ignore_old_state = pydantic.Field(False, alias="force_rerun")


class ProfilingHandler(StatefulIngestionUsecaseHandlerBase[ProfilingCheckpointState]):
    """
    The stateful ingestion helper class that handles skipping redundant runs.
    This contains the generic logic for all sources that need to support skipping redundant runs.
    """

    INVALID_TIMESTAMP_VALUE: pydantic.PositiveInt = 1

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase[StatefulProfilingConfig],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.source = source
        self.stateful_ingestion_config: Optional[
            StatefulProfilingConfig
        ] = config.stateful_ingestion
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.checkpointing_enabled: bool = source.is_stateful_ingestion_configured()
        self._job_id = self._init_job_id()
        self.source.register_stateful_ingestion_usecase_handler(self)

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
        platform: Optional[str] = None
        source_class = type(self.source)
        if hasattr(source_class, "get_platform_name"):
            platform = source_class.get_platform_name()  # type: ignore

        # Default name for everything else
        job_name_suffix = "profiling"
        return JobId(f"{platform}_{job_name_suffix}" if platform else job_name_suffix)

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
        cur_checkpoint = self.source.get_current_checkpoint(self.job_id)
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

    def add_all_to_state(
        self,
        urns: Dict[str, pydantic.PositiveInt],
    ) -> None:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return
        cur_checkpoint = self.source.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(ProfilingCheckpointState, cur_checkpoint.state)
        cur_state.last_profiled.update(urns)

    def get_last_state(self) -> Optional[ProfilingCheckpointState]:
        if not self.is_checkpointing_enabled() or self._ignore_old_state():
            return None
        last_checkpoint = self.source.get_last_checkpoint(
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
