import logging
import threading
from typing import Annotated, Dict, List, Optional, TypeVar

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_UNCHANGED_OBJECTS_JOB_ID,
    MicroStrategyReadOutcome,
    UsageTargetKind,
)
from datahub.ingestion.source.microstrategy.models import normalize_object_id
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StateProviderWrapper,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)

logger = logging.getLogger(__name__)

NormalizedId = Annotated[str, AfterValidator(normalize_object_id)]


def _sorted_unique(values: List[str]) -> List[str]:
    return sorted(set(values))


SortedUrns = Annotated[List[str], AfterValidator(_sorted_unique)]


class RecordedUsageTarget(BaseModel):
    model_config = ConfigDict(frozen=True)

    object_id: str
    kind: UsageTargetKind
    urn: str


class CheckpointRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    # None makes the next run read the object again.
    fingerprint: Optional[str] = None
    urns: SortedUrns = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)


class ObjectCheckpoint(CheckpointRecord):
    # Reports and cubes the object reads, re-checked for changes on every run.
    dependency_ids: List[NormalizedId] = Field(default_factory=list)
    # Report ids a dashboard adds to the dashboard-linked report scope.
    linked_report_ids: List[NormalizedId] = Field(default_factory=list)


class ReportDatasetCheckpoint(CheckpointRecord):
    # Dashboards whose definitions built the dataset.
    dashboard_ids: List[NormalizedId] = Field(default_factory=list)
    from_report_pass: bool = False


class CheckpointDraft(BaseModel):
    """What one object's read has collected so far; saved as a frozen record."""

    fingerprint: Optional[str] = None
    urns: List[str] = Field(default_factory=list)
    usage_targets: List[RecordedUsageTarget] = Field(default_factory=list)
    dependency_ids: List[str] = Field(default_factory=list)
    linked_report_ids: List[str] = Field(default_factory=list)
    dashboard_ids: List[str] = Field(default_factory=list)
    from_report_pass: bool = False
    outcome: MicroStrategyReadOutcome = MicroStrategyReadOutcome.COMPLETE

    def mark_incomplete(self) -> None:
        if self.outcome == MicroStrategyReadOutcome.COMPLETE:
            self.outcome = MicroStrategyReadOutcome.INCOMPLETE

    def to_object_checkpoint(self) -> ObjectCheckpoint:
        return ObjectCheckpoint(
            fingerprint=self.fingerprint,
            urns=self.urns,
            usage_targets=self.usage_targets,
            dependency_ids=self.dependency_ids,
            linked_report_ids=self.linked_report_ids,
        )

    def to_report_dataset_checkpoint(self) -> ReportDatasetCheckpoint:
        return ReportDatasetCheckpoint(
            fingerprint=self.fingerprint,
            urns=self.urns,
            usage_targets=self.usage_targets,
            dashboard_ids=self.dashboard_ids,
            from_report_pass=self.from_report_pass,
        )


class MicroStrategyCheckpointState(CheckpointStateBase):
    # project id -> object id -> the object's last read, both normalized.
    dashboards: Dict[NormalizedId, Dict[NormalizedId, ObjectCheckpoint]] = Field(
        default_factory=dict
    )
    reports: Dict[NormalizedId, Dict[NormalizedId, ObjectCheckpoint]] = Field(
        default_factory=dict
    )
    report_datasets: Dict[NormalizedId, Dict[NormalizedId, ReportDatasetCheckpoint]] = (
        Field(default_factory=dict)
    )


_Record = TypeVar("_Record", ObjectCheckpoint, ReportDatasetCheckpoint)


def _merge_missing(
    current: Dict[str, Dict[str, _Record]], last: Dict[str, Dict[str, _Record]]
) -> None:
    for project_id, entries in last.items():
        target = current.setdefault(project_id, {})
        for object_id, entry in entries.items():
            target.setdefault(object_id, entry)


class MicroStrategyStateHandler(
    StatefulIngestionUsecaseHandlerBase[MicroStrategyCheckpointState]
):
    def __init__(
        self,
        state_provider: StateProviderWrapper,
        config: MicroStrategyConfig,
        report: MicroStrategyReport,
        pipeline_name: Optional[str],
        run_id: str,
        stale_job_id: Optional[JobId],
    ) -> None:
        self.state_provider = state_provider
        # Only trusted from the run that wrote the last stale-entity state, or
        # it may miss entities a run without skip_unchanged_objects created.
        self.stale_job_id = stale_job_id
        self.config = config
        self.report = report
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._job_id = JobId(MSTR_UNCHANGED_OBJECTS_JOB_ID)
        self._last_unreadable = False
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
        last = self._trusted_last()
        if last is None:
            return None
        project = last.dashboards.get(normalize_object_id(project_id), {})
        return project.get(normalize_object_id(dashboard_id))

    def previous_report(
        self, project_id: str, report_id: str
    ) -> Optional[ObjectCheckpoint]:
        last = self._trusted_last()
        if last is None:
            return None
        project = last.reports.get(normalize_object_id(project_id), {})
        return project.get(normalize_object_id(report_id))

    def previous_report_dataset(
        self, project_id: str, report_id: str
    ) -> Optional[ReportDatasetCheckpoint]:
        return self.previous_report_datasets(project_id).get(
            normalize_object_id(report_id)
        )

    def previous_dashboards(self, project_id: str) -> Dict[str, ObjectCheckpoint]:
        last = self._trusted_last()
        if last is None:
            return {}
        return dict(last.dashboards.get(normalize_object_id(project_id), {}))

    def previous_report_datasets(
        self, project_id: str
    ) -> Dict[str, ReportDatasetCheckpoint]:
        last = self._trusted_last()
        if last is None:
            return {}
        return dict(last.report_datasets.get(normalize_object_id(project_id), {}))

    def record_dashboard(
        self, project_id: str, dashboard_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                project = current.state.dashboards.setdefault(
                    normalize_object_id(project_id), {}
                )
                project[normalize_object_id(dashboard_id)] = checkpoint

    def record_report(
        self, project_id: str, report_id: str, checkpoint: ObjectCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                project = current.state.reports.setdefault(
                    normalize_object_id(project_id), {}
                )
                project[normalize_object_id(report_id)] = checkpoint

    def record_report_dataset(
        self, project_id: str, report_id: str, checkpoint: ReportDatasetCheckpoint
    ) -> None:
        current = self._current()
        if current is not None:
            with self._lock:
                project = current.state.report_datasets.setdefault(
                    normalize_object_id(project_id), {}
                )
                project[normalize_object_id(report_id)] = checkpoint

    def keep_unrecorded(self) -> None:
        # A failed run may not reach every object; keep their last read.
        current = self._current()
        last = self._trusted_last()
        if current is None or last is None:
            return
        with self._lock:
            _merge_missing(current.state.dashboards, last.dashboards)
            _merge_missing(current.state.reports, last.reports)
            _merge_missing(current.state.report_datasets, last.report_datasets)

    def _current(self) -> Optional[Checkpoint[MicroStrategyCheckpointState]]:
        if not self.is_checkpointing_enabled():
            return None
        with self._lock:
            return self.state_provider.get_current_checkpoint(self.job_id)

    def _trusted_last(self) -> Optional[MicroStrategyCheckpointState]:
        if not self.is_checkpointing_enabled():
            return None
        with self._lock:
            if self._last_unreadable:
                return None
            try:
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
            except Exception as error:
                # Saved by another connector version, or no longer valid.
                self._last_unreadable = True
                self.report.warning(
                    title="Unchanged-object checkpoint unreadable",
                    message=(
                        "The saved checkpoint could not be read, so every "
                        "object is read as if there were no previous run."
                    ),
                    exc=error,
                )
                return None
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
