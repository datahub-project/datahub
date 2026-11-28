import threading
from typing import Callable, Dict, List, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_UNCHANGED_OBJECTS_JOB_ID,
    MicroStrategyReadOutcome,
    UsageTargetKind,
)
from datahub.ingestion.source.microstrategy.spill_cache import object_cache_key
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StateProviderWrapper,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)


class RecordedUsageTarget(BaseModel):
    model_config = ConfigDict(frozen=True)

    object_id: str
    kind: UsageTargetKind
    urn: str


# Mutable: filled in while the object's workunits are generated.
class ObjectCheckpoint(BaseModel):
    # None makes the next run read the object again.
    fingerprint: Optional[str] = None
    urns: List[str] = Field(default_factory=list)
    # Reports and cubes the object reads, re-checked for changes on every run.
    dependency_ids: List[str] = Field(default_factory=list)
    # Report ids a dashboard adds to the dashboard-linked report scope.
    linked_report_ids: List[str] = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)
    # Set during this run's read and never saved.
    outcome: MicroStrategyReadOutcome = Field(
        default=MicroStrategyReadOutcome.COMPLETE, exclude=True
    )


class ReportDatasetCheckpoint(BaseModel):
    fingerprint: Optional[str] = None
    urns: List[str] = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)
    # Normalized ids of the dashboards whose definitions built the dataset.
    dashboard_ids: List[str] = Field(default_factory=list)
    from_report_pass: bool = False


OptionalCheckpoint = Union[ObjectCheckpoint, ReportDatasetCheckpoint, None]


class MicroStrategyCheckpointState(CheckpointStateBase):
    # Keyed by object_cache_key(project_id, object_id).
    dashboards: Dict[str, ObjectCheckpoint] = Field(default_factory=dict)
    reports: Dict[str, ObjectCheckpoint] = Field(default_factory=dict)
    report_datasets: Dict[str, ReportDatasetCheckpoint] = Field(default_factory=dict)


_Entry = TypeVar("_Entry", ObjectCheckpoint, ReportDatasetCheckpoint)
_Section = Callable[[MicroStrategyCheckpointState], Dict[str, _Entry]]


def _dashboards(state: MicroStrategyCheckpointState) -> Dict[str, ObjectCheckpoint]:
    return state.dashboards


def _reports(state: MicroStrategyCheckpointState) -> Dict[str, ObjectCheckpoint]:
    return state.reports


def _report_datasets(
    state: MicroStrategyCheckpointState,
) -> Dict[str, ReportDatasetCheckpoint]:
    return state.report_datasets


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
    def __init__(
        self,
        state_provider: StateProviderWrapper,
        config: MicroStrategyConfig,
        pipeline_name: Optional[str],
        run_id: str,
        stale_job_id: Optional[JobId],
    ) -> None:
        self.state_provider = state_provider
        # Only trusted from the run that wrote the last stale-entity state, or
        # it may miss entities a run without skip_unchanged_objects created.
        self.stale_job_id = stale_job_id
        self.config = config
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._job_id = JobId(MSTR_UNCHANGED_OBJECTS_JOB_ID)
        # Change checks may run on worker threads.
        self._lock = threading.RLock()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def is_checkpointing_enabled(self) -> bool:
        # Also kept for stale removal, so a failed read keeps its last entities.
        return (
            self.stale_job_id is not None or self.config.skip_unchanged_objects
        ) and self.state_provider.is_stateful_ingestion_configured()

    def is_too_large(self) -> bool:
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
        return self._previous(_dashboards, project_id, dashboard_id)

    def previous_report(
        self, project_id: str, report_id: str
    ) -> Optional[ObjectCheckpoint]:
        return self._previous(_reports, project_id, report_id)

    def previous_report_dataset(
        self, project_id: str, report_id: str
    ) -> Optional[ReportDatasetCheckpoint]:
        return self._previous(_report_datasets, project_id, report_id)

    def previous_dashboards(self, project_id: str) -> Dict[str, ObjectCheckpoint]:
        return self._previous_in_project(_dashboards, project_id)

    def previous_report_datasets(
        self, project_id: str
    ) -> Dict[str, ReportDatasetCheckpoint]:
        return self._previous_in_project(_report_datasets, project_id)

    def record_dashboard(
        self, project_id: str, dashboard_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        self._record(_dashboards, project_id, dashboard_id, checkpoint)

    def record_report(
        self, project_id: str, report_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        self._record(_reports, project_id, report_id, checkpoint)

    def record_report_dataset(
        self, project_id: str, report_id: str, checkpoint: ReportDatasetCheckpoint
    ) -> None:
        self._record(_report_datasets, project_id, report_id, checkpoint)

    def keep_unrecorded(self) -> None:
        # A failed run may not reach every object; keep their last read.
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

    def _previous(
        self, section: _Section[_Entry], project_id: str, object_id: str
    ) -> Optional[_Entry]:
        last = self._trusted_last()
        if last is None:
            return None
        return section(last).get(object_cache_key(project_id, object_id))

    def _previous_in_project(
        self, section: _Section[_Entry], project_id: str
    ) -> Dict[str, _Entry]:
        last = self._trusted_last()
        return _in_project(section(last), project_id) if last is not None else {}

    def _record(
        self,
        section: _Section[_Entry],
        project_id: str,
        object_id: str,
        checkpoint: _Entry,
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                section(current.state)[object_cache_key(project_id, object_id)] = (
                    checkpoint
                )

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
