import threading
from typing import Dict, List, Optional, TypeVar

from pydantic import BaseModel, Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.models import normalize_object_id
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StateProviderWrapper,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)


class RecordedUsageTarget(BaseModel):
    object_id: str
    kind: str
    urn: str


class ObjectCheckpoint(BaseModel):
    """A dashboard or standalone report as the last run read it."""

    # None makes the next run read the object again.
    fingerprint: Optional[str] = None
    urns: List[str] = Field(default_factory=list)
    # Reports and cubes the object reads, re-checked for changes on every run.
    dependency_report_ids: List[str] = Field(default_factory=list)
    # Report ids a dashboard adds to the dashboard-linked report scope.
    linked_report_ids: List[str] = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)
    referenced_object_ids: List[str] = Field(default_factory=list)


class ReportDatasetCheckpoint(BaseModel):
    """A dataset_identity=per_report report dataset as the last run emitted it."""

    fingerprint: Optional[str] = None
    urns: List[str] = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)
    referenced_object_ids: List[str] = Field(default_factory=list)
    # Normalized ids of the dashboards whose definitions built the dataset.
    dashboard_ids: List[str] = Field(default_factory=list)
    from_report_pass: bool = False


class MicroStrategyCheckpointState(CheckpointStateBase):
    # Keyed by checkpoint_key(project_id, object_id).
    dashboards: Dict[str, ObjectCheckpoint] = Field(default_factory=dict)
    reports: Dict[str, ObjectCheckpoint] = Field(default_factory=dict)
    report_datasets: Dict[str, ReportDatasetCheckpoint] = Field(default_factory=dict)


def checkpoint_key(project_id: str, object_id: str) -> str:
    return f"{project_id}:{normalize_object_id(object_id)}"


_Entry = TypeVar("_Entry", ObjectCheckpoint, ReportDatasetCheckpoint)


def _in_project(entries: Dict[str, _Entry], project_id: str) -> Dict[str, _Entry]:
    prefix = f"{project_id}:"
    return {
        key[len(prefix) :]: entry
        for key, entry in entries.items()
        if key.startswith(prefix)
    }


class MicroStrategyStateHandler(
    StatefulIngestionUsecaseHandlerBase[MicroStrategyCheckpointState]
):
    """Remembers, per dashboard, report and per_report dataset, what it looked
    like and which entities it produced."""

    def __init__(
        self,
        state_provider: StateProviderWrapper,
        config: MicroStrategyConfig,
        pipeline_name: Optional[str],
        run_id: str,
        stale_job_id: Optional[JobId],
    ) -> None:
        self.state_provider = state_provider
        # A run without skip_unchanged_objects commits the stale entity removal
        # state but no object checkpoints, so an object checkpoint from before
        # it misses entities created since. Object checkpoints are only trusted
        # when they come from the same run as the last stale entity removal state.
        self.stale_job_id = stale_job_id
        self.config = config
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._job_id = JobId("microstrategy_unchanged_objects")
        # Change checks may run on worker threads; the checkpoints are loaded
        # once and written under the same lock.
        self._lock = threading.RLock()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def is_checkpointing_enabled(self) -> bool:
        # Kept whenever stale entity removal runs, so an object that fails to
        # read keeps what its last read produced, and for skipping.
        return (
            self.stale_job_id is not None or self.config.skip_unchanged_objects
        ) and self.state_provider.is_stateful_ingestion_configured()

    def is_too_large(self) -> bool:
        """Whether this run's checkpoint is over the state size limit, in which
        case it is not saved and the next run finds no usable checkpoint."""
        stateful = self.state_provider.stateful_ingestion_config
        current = self._current()
        if current is None or stateful is None:
            return False
        try:
            with self._lock:
                current.state.to_bytes(
                    max_allowed_state_size=stateful.max_checkpoint_state_size
                )
        except ValueError:
            return True
        return False

    def create_checkpoint(self) -> Optional[Checkpoint[MicroStrategyCheckpointState]]:
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
            state=MicroStrategyCheckpointState(),
        )

    def previous_dashboard(
        self, project_id: str, dashboard_id: str
    ) -> Optional[ObjectCheckpoint]:
        last = self._trusted_last()
        if last is None:
            return None
        return last.dashboards.get(checkpoint_key(project_id, dashboard_id))

    def previous_report(
        self, project_id: str, report_id: str
    ) -> Optional[ObjectCheckpoint]:
        last = self._trusted_last()
        if last is None:
            return None
        return last.reports.get(checkpoint_key(project_id, report_id))

    def previous_dashboards(self, project_id: str) -> Dict[str, ObjectCheckpoint]:
        """The project's dashboards the last run recorded, by normalized id."""
        last = self._trusted_last()
        return _in_project(last.dashboards, project_id) if last is not None else {}

    def previous_report_datasets(
        self, project_id: str
    ) -> Dict[str, ReportDatasetCheckpoint]:
        """The project's per_report datasets the last run recorded, by
        normalized report id."""
        last = self._trusted_last()
        return _in_project(last.report_datasets, project_id) if last is not None else {}

    def record_dashboard(
        self, project_id: str, dashboard_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                current.state.dashboards[checkpoint_key(project_id, dashboard_id)] = (
                    checkpoint
                )

    def record_report(
        self, project_id: str, report_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                current.state.reports[checkpoint_key(project_id, report_id)] = (
                    checkpoint
                )

    def record_report_dataset(
        self, project_id: str, report_id: str, checkpoint: ReportDatasetCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                current.state.report_datasets[checkpoint_key(project_id, report_id)] = (
                    checkpoint
                )

    def keep_unrecorded(self) -> None:
        """Carries the last read of every object this run did not record into
        its checkpoint. A failed run may not reach every object, and without
        this its checkpoint would drop them."""
        current = self._current()
        last = self._trusted_last()
        if current is None or last is None:
            return
        with self._lock:
            for key, entry in last.dashboards.items():
                current.state.dashboards.setdefault(key, entry)
            for key, entry in last.reports.items():
                current.state.reports.setdefault(key, entry)
            for key, dataset in last.report_datasets.items():
                current.state.report_datasets.setdefault(key, dataset)

    def _current(self) -> Optional[Checkpoint[MicroStrategyCheckpointState]]:
        if not self.is_checkpointing_enabled():
            return None
        with self._lock:
            return self.state_provider.get_current_checkpoint(self.job_id)

    def _trusted_last(self) -> Optional[MicroStrategyCheckpointState]:
        if not self.is_checkpointing_enabled():
            return None
        with self._lock:
            last = self.state_provider.get_last_checkpoint(
                self.job_id, MicroStrategyCheckpointState
            )
            stale = (
                self.state_provider.get_last_checkpoint(
                    self.stale_job_id, GenericCheckpointState
                )
                if self.stale_job_id
                else None
            )
        return self._trusted(last, stale)

    def _trusted(
        self,
        last: Optional[Checkpoint[MicroStrategyCheckpointState]],
        stale: Optional[Checkpoint[GenericCheckpointState]],
    ) -> Optional[MicroStrategyCheckpointState]:
        if last is None:
            return None
        if self.stale_job_id and (stale is None or stale.run_id != last.run_id):
            return None
        return last.state
