import threading
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.vertexai.vertexai_constants import (
    LINKS_LOST_ON_STOP,
    READ_SWITCHES,
    ResourceReads,
)
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_scope import (
    ErrorKind,
    Scope,
    StoppedScopes,
    classify_error,
    error_message,
    error_title,
    is_google_error,
    keeps_like_temporary,
    scope_context,
    with_switch,
)

_LINK_READS = (ResourceReads.TRAINING_JOBS, ResourceReads.PIPELINES)
# A temporary loss may be gone tomorrow, and a project-wide one stops the model
# list too, so both hold the models back; any other loss writes them without.
_HOLDS_BACK = (ErrorKind.TRANSIENT, ErrorKind.PROJECT_WIDE)


@dataclass
class _Read:
    error: Optional[Exception] = None
    wrote_output: bool = False


@dataclass(frozen=True)
class _LostRead:
    region: str
    kind: str
    error: Exception

    @property
    def temporary(self) -> bool:
        return classify_error(self.error) is ErrorKind.TRANSIENT


class ReadLedger:
    """What each project's reads produced this run, keyed by (region, read), and
    so what is held back and reported. Safe to call from worker threads."""

    def __init__(self, report: VertexAISourceReport, stopped: StoppedScopes) -> None:
        self._report = report
        self._stopped = stopped
        self._lock = threading.Lock()
        self._reads: Dict[str, Dict[Tuple[str, str], _Read]] = {}

    def _read(self, project_id: str, region: str, kind: str) -> _Read:
        """Callers hold the lock."""
        return self._reads.setdefault(project_id, {}).setdefault(
            (region, kind), _Read()
        )

    def record_read(
        self,
        project_id: str,
        region: str,
        kind: str,
        error: Optional[Exception] = None,
    ) -> None:
        if error is not None:
            # The stored error keeps its traceback; only the stack's lines are reported.
            traceback.clear_frames(error.__traceback__)
        with self._lock:
            self._read(project_id, region, kind).error = error

    def record_wrote_output(self, project_id: str, region: str, kind: str) -> None:
        with self._lock:
            self._read(project_id, region, kind).wrote_output = True

    def _failed(self, project_id: str) -> List[_LostRead]:
        with self._lock:
            return [
                _LostRead(region=region, kind=kind, error=read.error)
                for (region, kind), read in self._reads.get(project_id, {}).items()
                if read.error is not None
            ]

    def project_wide_error(self, project_id: str) -> Optional[Exception]:
        """The error of a read that no region of the project can get past."""
        return next(
            (
                lost.error
                for lost in self._failed(project_id)
                if classify_error(lost.error) is ErrorKind.PROJECT_WIDE
            ),
            None,
        )

    def lost_phase1_kinds(self, project_id: str, region: str) -> Dict[str, Exception]:
        """Each phase 1 read lost in the region, with its error."""
        return {
            lost.kind: lost.error
            for lost in self._failed(project_id)
            if lost.region == region and lost.kind != ResourceReads.MODELS
        }

    def every_read_failed(self, project_id: str) -> bool:
        with self._lock:
            reads = list(self._reads.get(project_id, {}).values())
        return bool(reads) and all(
            read.error is not None and not read.wrote_output for read in reads
        )

    def representative_error(self, project_id: str) -> Exception:
        """The error that names why a project could not be read: project-wide
        first, then any other except a refusal, then a refusal."""
        rank = {ErrorKind.PROJECT_WIDE: 0, ErrorKind.REFUSED: 2}
        return min(
            (lost.error for lost in self._failed(project_id)),
            key=lambda error: rank.get(classify_error(error), 1),
        )

    def keep_cause(self, project_id: str) -> Exception:
        """The error that decides what a project whose every read failed keeps.
        Any loss that keeps like a temporary one decides it, so the order the
        reads finished in cannot."""
        return min(
            (lost.error for lost in self._failed(project_id)),
            key=lambda error: (not keeps_like_temporary(error), is_google_error(error)),
        )

    def hold_back_error(self, project_id: str, region: str) -> Optional[Exception]:
        """Models read the links phase 1 collects. A temporary loss of the
        training job or pipeline list, or of a pipeline's tasks, keeps them as
        they were for this run; a lasting loss writes them without those links.
        Experiment links never reach written models, so experiments never hold
        them back. Returns the error that holds them back, or None."""
        for call_kind in LINKS_LOST_ON_STOP:
            scope = Scope(project=project_id, region=region, kind=call_kind)
            if self._stopped.stop_kind(scope) is ErrorKind.TRANSIENT:
                return self._stopped.stop_error(scope)
        return next(
            (
                lost.error
                for lost in self._failed(project_id)
                if classify_error(lost.error) in _HOLDS_BACK
                and lost.region == region
                and lost.kind in _LINK_READS
            ),
            None,
        )

    def _list_context(self, project_id: str, lost: _LostRead) -> str:
        """The context of a lost list read. A temporary loss names the pacing
        switch; a lasting one, the switch that turns the read off."""
        switch = (
            self._stopped.detail_switch(None, lost.error)
            if lost.temporary
            else READ_SWITCHES.get(lost.kind)
        )
        scope = Scope(project=project_id, region=lost.region, kind=lost.kind)
        return with_switch(scope_context(scope, lost.error), switch)

    def record_lost_reads(self, project_id: str) -> None:
        """Lists, in the report's summary, every lost list read of the project
        and every detail read stopped or refused there."""
        failed = self._failed(project_id)
        for lost in failed:
            self._report.report_lost_read(
                project_id,
                self._list_context(project_id, lost),
                temporary=lost.temporary,
            )
        lost_lists = {(lost.region, lost.kind) for lost in failed}
        for scope, loss_kind, error in self._stopped.lost_in(project_id):
            # An item of a lost list is already counted in the list's entry.
            if (scope.region, scope.kind) in lost_lists:
                continue
            self._report.report_lost_read(
                project_id,
                self._stopped.detail_context(scope, error),
                temporary=loss_kind is ErrorKind.TRANSIENT,
            )

    def report_lost_reads(self, project_id: str, listed: bool) -> None:
        """A temporary loss is a warning. A lasting one fails the run for a listed
        project; a discovered project, which nobody chose to ingest, gets a
        warning instead."""
        for lost in self._failed(project_id):
            if lost.temporary and lost.kind == ResourceReads.MODELS:
                title = "Model step skipped"
                message = "The model step was skipped in this region, because the links it depends on or the model list could not be read. Its earlier entities are kept and not soft-deleted."
            elif lost.temporary:
                title = "Resource type could not be read"
                message = "This resource type could not be read in this region. Its earlier entities are kept and not soft-deleted. If it is the training job or pipeline list, or a pipeline's tasks, the region's models also keep their previous version for this run."
            elif listed:
                title = "Resource type could not be read"
                message = "This resource type could not be read in this region, and the error repeats on every run. Its earlier entities are kept, and what depends on it is written without it. Grant the permission named in the context, or turn the read off with the switch named there."
            else:
                title = "Resource type could not be read"
                message = "This resource type could not be read in this region of a discovered project, and the error repeats on every run. Its earlier entities are kept, except experiment folders when an 'Experiment folders not kept while unreadable' warning names the project, and what depends on it is written without it. Grant the permission named in the context, or turn the read off with the switch named there."
            emit = (
                self._report.failure
                if listed and not lost.temporary
                else self._report.warning
            )
            context = self._list_context(project_id, lost)
            emit(
                title=error_title(lost.error, title),
                message=error_message(lost.error, message),
                context=context,
                exc=lost.error,
            )
            # A project-wide error holds the region's models back like a temporary one.
            if (
                lost.kind == ResourceReads.PIPELINES
                and classify_error(lost.error) in _HOLDS_BACK
            ):
                self._report.report_pipeline_links_elsewhere(context)
