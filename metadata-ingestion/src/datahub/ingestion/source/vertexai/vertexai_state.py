import threading
from functools import lru_cache
from typing import Any, Dict, Optional

from pydantic import Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)


class VertexAICheckpointState(CheckpointStateBase):
    """
    Checkpoint state for Vertex AI source.
    Tracks the last seen update_time for each resource type to enable incremental ingestion.
    """

    last_update_times: Dict[str, int] = Field(
        default_factory=dict,
    )


class VertexAIStateHandler(
    StatefulIngestionUsecaseHandlerBase[VertexAICheckpointState]
):
    """
    State handler for Vertex AI incremental ingestion.
    Manages checkpointing of last seen update timestamps per resource type.
    """

    @property
    def job_id(self) -> JobId:
        return JobId("vertexai-checkpoint-state")

    def __init__(
        self,
        source: Any,
        stateful_ingestion_config: Optional[StatefulStaleMetadataRemovalConfig],
    ):
        self.source = source
        self.state_provider = source.state_provider
        self.stateful_ingestion_config: Optional[StatefulStaleMetadataRemovalConfig] = (
            stateful_ingestion_config
        )
        self.run_id = source.ctx.run_id
        self.pipeline_name = source.ctx.pipeline_name

        if self.is_checkpointing_enabled():
            self.state_provider.register_stateful_ingestion_usecase_handler(self)
        self._prior_state: Optional[VertexAICheckpointState] = None
        self._running_state: Optional[VertexAICheckpointState] = None
        # Phase-1 worker threads can make the first read or update together.
        self._lock = threading.RLock()

    @lru_cache(maxsize=1)
    def is_checkpointing_enabled(self) -> bool:
        return self.state_provider.is_stateful_ingestion_configured()

    def get_last_checkpoint_state(self) -> VertexAICheckpointState:
        if self.is_checkpointing_enabled() and not getattr(
            self.stateful_ingestion_config, "ignore_old_state", False
        ):
            last_checkpoint = self.state_provider.get_last_checkpoint(
                self.job_id, VertexAICheckpointState
            )
            if last_checkpoint and last_checkpoint.state:
                return last_checkpoint.state

        return VertexAICheckpointState()

    def _prior(self) -> VertexAICheckpointState:
        with self._lock:
            if self._prior_state is None:
                self._prior_state = self.get_last_checkpoint_state()
            return self._prior_state

    def _running(self) -> VertexAICheckpointState:
        with self._lock:
            if self._running_state is None:
                self._running_state = self._prior().model_copy(deep=True)
            return self._running_state

    def get_last_update_time(self, resource_type: str) -> Optional[int]:
        # Reads only the previous run's checkpoint; the running maximum would
        # make a later project or region drop items an earlier one recorded.
        return self._prior().last_update_times.get(resource_type)

    def update_resource_timestamp(
        self, resource_type: str, update_time_millis: int
    ) -> None:
        with self._lock:
            times = self._running().last_update_times
            times[resource_type] = max(times.get(resource_type, 0), update_time_millis)

    def create_checkpoint(self) -> Optional[Checkpoint[VertexAICheckpointState]]:
        if not self.is_checkpointing_enabled() or getattr(
            self.stateful_ingestion_config, "ignore_new_state", False
        ):
            return None

        if self.pipeline_name is None:
            raise ValueError(
                "Pipeline name must be set to use stateful Vertex AI ingestion"
            )

        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=self._running(),
        )
