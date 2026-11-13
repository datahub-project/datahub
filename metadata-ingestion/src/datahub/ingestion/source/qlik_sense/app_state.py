import threading
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qvd_lineage import AppQvds
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StateProviderWrapper,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)


class AppCheckpoint(BaseModel):
    # None, or "" in older state, makes the next run read the app again.
    fingerprint: Optional[str] = None
    urns: List[str]
    # Consecutive runs in which the app was only partly read and these URNs,
    # from its last complete read, were kept instead.
    partial_reads: int = 0
    # Table QRI -> dataset URN of the app's tables, so datasets an app kept from
    # its last read stores into still link to them. None in older state.
    table_urns_by_qri: Optional[Dict[str, str]] = None
    # The QVDs the app writes and reads, on-prem. None in older state.
    qvds: Optional[AppQvds] = None


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
        stale_job_id: Optional[JobId],
    ) -> None:
        self.state_provider = state_provider
        # A run without skip_unchanged_apps commits the stale entity removal
        # state but no app checkpoints, so an app checkpoint from before it
        # misses entities created since. App checkpoints are only trusted when
        # they come from the same run as the last stale entity removal state.
        self.stale_job_id = stale_job_id
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
        # Kept whenever stale entity removal runs, so a partly read app keeps
        # what its last read produced, and for skipping unchanged apps.
        return (
            self.stale_job_id is not None or self.config.skips_unchanged_apps()
        ) and self.state_provider.is_stateful_ingestion_configured()

    def is_too_large(self) -> bool:
        """Whether this run's checkpoint is over the state size limit, in which
        case it is not saved and the next run finds no usable checkpoint."""
        stateful = self.state_provider.stateful_ingestion_config
        current = (
            self.state_provider.get_current_checkpoint(self.job_id)
            if self.is_checkpointing_enabled()
            else None
        )
        if current is None or stateful is None:
            return False
        try:
            current.state.to_bytes(
                max_allowed_state_size=stateful.max_checkpoint_state_size
            )
        except ValueError:
            return True
        return False

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
        last = self._trusted_last_apps()
        return last.get(app_id) if last is not None else None

    def keep_unrecorded_apps(self) -> None:
        """Carries the last read of every app this run did not record into its
        checkpoint. A failed run may not reach every app, and without this its
        checkpoint would drop them."""
        current = (
            self.state_provider.get_current_checkpoint(self.job_id)
            if self.is_checkpointing_enabled()
            else None
        )
        last = self._trusted_last_apps()
        if current is None or last is None:
            return
        for app_id, checkpoint in last.items():
            current.state.apps.setdefault(app_id, checkpoint)

    def _trusted_last_apps(self) -> Optional[Dict[str, AppCheckpoint]]:
        if not self.is_checkpointing_enabled():
            return None
        with self._load_lock:
            last = self.state_provider.get_last_checkpoint(
                self.job_id, QlikAppCheckpointState
            )
            stale = (
                self.state_provider.get_last_checkpoint(
                    self.stale_job_id, GenericCheckpointState
                )
                if self.stale_job_id
                else None
            )
        if last is None:
            return None
        if self.stale_job_id and (stale is None or stale.run_id != last.run_id):
            return None
        return last.state.apps

    def record(self, app_id: str, checkpoint: AppCheckpoint) -> None:
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        if current is not None:
            current.state.apps[app_id] = checkpoint
