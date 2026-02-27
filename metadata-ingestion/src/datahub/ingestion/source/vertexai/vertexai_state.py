from functools import lru_cache
from typing import Any, Dict, Optional

from pydantic import Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
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

    def __init__(self, source: Any, stateful_ingestion_config: Any):
        self.source = source
        self.state_provider = source.state_provider
        self.stateful_ingestion_config = stateful_ingestion_config
        self.run_id = source.ctx.run_id
        self.pipeline_name = source.ctx.pipeline_name
        self._last_state: Optional[VertexAICheckpointState] = None

        if self.is_checkpointing_enabled():
            self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @lru_cache(maxsize=1)
    def is_checkpointing_enabled(self) -> bool:
        return self.state_provider.is_stateful_ingestion_configured()

    def get_last_checkpoint_state(self) -> VertexAICheckpointState:
        if (
            self.is_checkpointing_enabled()
            and not self.stateful_ingestion_config.ignore_old_state
        ):
            last_checkpoint = self.state_provider.get_last_checkpoint(
                self.job_id, VertexAICheckpointState
            )
            if last_checkpoint and last_checkpoint.state:
                self._last_state = last_checkpoint.state
                return last_checkpoint.state

        return VertexAICheckpointState()

    def get_last_update_time(self, resource_type: str) -> Optional[int]:
        if not self._last_state:
            self._last_state = self.get_last_checkpoint_state()

        return self._last_state.last_update_times.get(resource_type)

    def update_resource_timestamp(
        self, resource_type: str, update_time_millis: int
    ) -> None:
        if not self._last_state:
            self._last_state = VertexAICheckpointState()

        current_max = self._last_state.last_update_times.get(resource_type, 0)
        self._last_state.last_update_times[resource_type] = max(
            current_max, update_time_millis
        )

    def create_checkpoint(self) -> Optional[Checkpoint[VertexAICheckpointState]]:
        if (
            not self.is_checkpointing_enabled()
            or self.stateful_ingestion_config.ignore_new_state
        ):
            return None

        if self.pipeline_name is None:
            raise ValueError(
                "Pipeline name must be set to use stateful Vertex AI ingestion"
            )

        if not self._last_state:
            self._last_state = VertexAICheckpointState()

        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=self._last_state,
        )
