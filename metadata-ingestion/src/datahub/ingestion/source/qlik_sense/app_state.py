import threading
from typing import Dict, Iterable, List, Optional, Tuple

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


class UnmappedFileStore(BaseModel):
    # The file's `lib://` path, as reported when no app reads it.
    path: str
    table_urns: List[str]


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
    # Some of the app's tables may be missing from table_urns_by_qri.
    tables_unknown: bool = False
    # The app's tables were read, but some of its lineage graph table nodes
    # have no URN in table_urns_by_qri.
    graph_tables_unresolved: bool = False
    # The QVDs the app writes and reads, on-prem. None in older state.
    qvds: Optional[AppQvds] = None
    # Table name, in lower case -> fields of the app's data model, kept for
    # apps that `binary_file_to_app` names, so apps loading them with BINARY
    # link their columns while the app is kept.
    data_model_fields: Optional[Dict[str, List[str]]] = None
    # The read ingested tables the app's BINARY statement brought in, so a
    # later read that can't find them keeps them. None in older state.
    binary_tables: Optional[bool] = None
    # The files on storage the app stores into, on Qlik Cloud.
    stored_file_urns: List[str] = []
    # File URN -> the app's tables stored into a file on a connection of
    # unknown storage, so a run that keeps the app still links them when
    # another app reads the file.
    unmapped_file_stores: Dict[str, UnmappedFileStore] = {}
    # Files on a connection of unknown storage that the app's lineage reads,
    # so a run that keeps the app still counts it as their reader.
    read_unmapped_file_urns: List[str] = []
    # Datasets only Qlik's lineage graph names that the app's lineage reads,
    # recorded even when a dataset is left out for lack of a certain path, so a
    # run that keeps the app still counts its reload toward that path.
    read_graph_named_urns: List[str] = []
    # Field names of the app's tables, so a renamed load from a kept app still
    # names its source field. None in older state.
    field_names: Optional[List[str]] = None


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

    def previous_apps(self) -> Optional[Dict[str, AppCheckpoint]]:
        """Every app the last run recorded, or None when an earlier run left
        state but no trusted app checkpoints, so apps may be missing."""
        if not self.is_checkpointing_enabled():
            return {}
        last, stale = self._last_checkpoints()
        if last is None and stale is None:
            # No earlier run kept state, so there is no app to miss.
            return {}
        return self._trusted(last, stale)

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
        return self._trusted(*self._last_checkpoints())

    def _last_checkpoints(
        self,
    ) -> Tuple[
        Optional[Checkpoint[QlikAppCheckpointState]],
        Optional[Checkpoint[GenericCheckpointState]],
    ]:
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
        return last, stale

    def _trusted(
        self,
        last: Optional[Checkpoint[QlikAppCheckpointState]],
        stale: Optional[Checkpoint[GenericCheckpointState]],
    ) -> Optional[Dict[str, AppCheckpoint]]:
        if last is None:
            return None
        if self.stale_job_id and (stale is None or stale.run_id != last.run_id):
            return None
        return last.state.apps

    def add_urns(self, app_id: str, urns: Iterable[str]) -> None:
        """Adds entities emitted at the end of the run to the checkpoint this
        run recorded for the app, if any."""
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        checkpoint = current.state.apps.get(app_id) if current is not None else None
        if checkpoint is not None:
            known = set(checkpoint.urns)
            checkpoint.urns.extend(urn for urn in sorted(urns) if urn not in known)

    def add_read_unmapped_files(self, app_id: str, urns: Iterable[str]) -> None:
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        checkpoint = current.state.apps.get(app_id) if current is not None else None
        if checkpoint is not None:
            checkpoint.read_unmapped_file_urns = sorted(
                set(checkpoint.read_unmapped_file_urns).union(urns)
            )

    def add_read_graph_named(self, app_id: str, urns: Iterable[str]) -> None:
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        checkpoint = current.state.apps.get(app_id) if current is not None else None
        if checkpoint is not None:
            checkpoint.read_graph_named_urns = sorted(
                set(checkpoint.read_graph_named_urns).union(urns)
            )

    def add_stored_files(self, app_id: str, file_urns: List[str]) -> None:
        """Adds files to the stored files of an app this run recorded."""
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        checkpoint = current.state.apps.get(app_id) if current is not None else None
        if checkpoint is not None:
            checkpoint.stored_file_urns = sorted(
                {*checkpoint.stored_file_urns, *file_urns}
            )

    def record(self, app_id: str, checkpoint: AppCheckpoint) -> None:
        if not self.is_checkpointing_enabled():
            return
        current = self.state_provider.get_current_checkpoint(self.job_id)
        if current is not None:
            current.state.apps[app_id] = checkpoint
