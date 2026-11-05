import threading
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StateProviderWrapper,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)


class AppCheckpoint(BaseModel):
    fingerprint: str
    urns: List[str]


class QlikAppCheckpointState(CheckpointStateBase):
    apps: Dict[str, AppCheckpoint] = Field(default_factory=dict)


class QlikAppStateHandler(StatefulIngestionUsecaseHandlerBase[QlikAppCheckpointState]):
    """Remembers, per app, what it looked like and which entities it produced."""

    def __init__(
        self,
        state_provider: StateProviderWrapper,
        config: QlikSourceConfig,
        pipeline_name: Optional[str],
        run_id: str,
    ) -> None:
        self.state_provider = state_provider
        self.config = config
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._job_id = JobId("qlik-sense_unchanged_apps")
        # Apps are compared from worker threads; load the last checkpoint once.
        self._load_lock = threading.Lock()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def is_checkpointing_enabled(self) -> bool:
        return (
            self.config.skip_unchanged_apps
            and self.state_provider.is_stateful_ingestion_configured()
        )

    def create_checkpoint(self) -> Optional[Checkpoint[QlikAppCheckpointState]]:
        stateful = self.state_provider.stateful_ingestion_config
        if not self.is_checkpointing_enabled() or (
            stateful is not None and stateful.ignore_new_state
        ):
            return None
        assert self.pipeline_name is not None
        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=QlikAppCheckpointState(),
        )

    def previous(self, app_id: str) -> Optional[AppCheckpoint]:
        if not self.is_checkpointing_enabled():
            return None
        with self._load_lock:
            last = self.state_provider.get_last_checkpoint(
                self.job_id, QlikAppCheckpointState
            )
        return last.state.apps.get(app_id) if last else None

    def record(self, app_id: str, checkpoint: AppCheckpoint) -> None:
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        if current is not None:
            current.state.apps[app_id] = checkpoint
