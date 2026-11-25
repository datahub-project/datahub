from dataclasses import dataclass
from typing import Dict, Optional, Union

from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.jobs import _RunnableJob
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1.types import (
    Execution,
    JobState,
    PipelineState,
    PipelineTaskDetail,
)

from datahub.metadata.schema_classes import RunResultTypeClass

AUTOML_JOB_STATE_MAPPING: Dict[int, Union[str, RunResultTypeClass]] = {
    PipelineState.PIPELINE_STATE_SUCCEEDED: RunResultTypeClass.SUCCESS,
    PipelineState.PIPELINE_STATE_FAILED: RunResultTypeClass.FAILURE,
    PipelineState.PIPELINE_STATE_CANCELLED: "Cancelled",
    PipelineState.PIPELINE_STATE_PAUSED: "Paused",
    PipelineState.PIPELINE_STATE_QUEUED: "Queued",
    PipelineState.PIPELINE_STATE_RUNNING: "Running",
    PipelineState.PIPELINE_STATE_UNSPECIFIED: "Unspecific",
}

CUSTOM_JOB_STATE_MAPPING: Dict[int, Union[str, RunResultTypeClass]] = {
    JobState.JOB_STATE_SUCCEEDED: RunResultTypeClass.SUCCESS,
    JobState.JOB_STATE_FAILED: RunResultTypeClass.FAILURE,
    JobState.JOB_STATE_CANCELLED: "Cancelled",
    JobState.JOB_STATE_PAUSED: "Paused",
    JobState.JOB_STATE_QUEUED: "Queued",
    JobState.JOB_STATE_RUNNING: "Running",
    JobState.JOB_STATE_CANCELLING: "Cancelling",
    JobState.JOB_STATE_EXPIRED: "Expired",
    JobState.JOB_STATE_UPDATING: "Updating",
}

EXECUTION_STATUS_MAPPING: Dict[int, Union[str, RunResultTypeClass]] = {
    0: "STATE_UNSPECIFIED",
    1: "PENDING",
    2: "RUNNING",
    3: RunResultTypeClass.SUCCESS,
    4: RunResultTypeClass.FAILURE,
}

UNKNOWN_STATUS = "UNKNOWN"


def get_automl_job_result_type(state: PipelineState) -> Union[str, RunResultTypeClass]:
    return AUTOML_JOB_STATE_MAPPING.get(state, UNKNOWN_STATUS)


def get_custom_job_result_type(state: JobState) -> Union[str, RunResultTypeClass]:
    return CUSTOM_JOB_STATE_MAPPING.get(state, UNKNOWN_STATUS)


def get_job_result_status(job: VertexAiResourceNoun) -> Union[str, RunResultTypeClass]:
    if isinstance(job, _TrainingJob) and isinstance(job.state, PipelineState):
        return get_automl_job_result_type(job.state)
    elif isinstance(job, _RunnableJob) and isinstance(job.state, JobState):
        return get_custom_job_result_type(job.state)
    return UNKNOWN_STATUS


def get_execution_result_status(status: int) -> Union[str, RunResultTypeClass]:
    """
    State of the execution.
    STATE_UNSPECIFIED = 0
    PENDING = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    """
    return EXECUTION_STATUS_MAPPING.get(status, UNKNOWN_STATUS)


@dataclass(frozen=True)
class RunOutcome:
    result: str
    native: str


_TASK_OUTCOMES: Dict[int, str] = {
    PipelineTaskDetail.State.SUCCEEDED: RunResultTypeClass.SUCCESS,
    PipelineTaskDetail.State.FAILED: RunResultTypeClass.FAILURE,
    PipelineTaskDetail.State.CANCELLED: RunResultTypeClass.SKIPPED,
}

_PIPELINE_OUTCOMES: Dict[int, str] = {
    PipelineState.PIPELINE_STATE_SUCCEEDED: RunResultTypeClass.SUCCESS,
    PipelineState.PIPELINE_STATE_FAILED: RunResultTypeClass.FAILURE,
    PipelineState.PIPELINE_STATE_CANCELLED: RunResultTypeClass.SKIPPED,
}


def is_cache_hit(detail: PipelineTaskDetail) -> bool:
    return (
        detail.state == PipelineTaskDetail.State.SKIPPED
        or detail.execution.state == Execution.State.CACHED
    )


def task_run_outcome(detail: PipelineTaskDetail) -> Optional[RunOutcome]:
    """Result of a finished task run, or None while it is unfinished.

    The Vertex state is kept as the native result type. A cache hit is a
    successful run, however Vertex labels it.
    """
    result = (
        RunResultTypeClass.SUCCESS
        if is_cache_hit(detail)
        else _TASK_OUTCOMES.get(detail.state)
    )
    if result is None:
        return None
    # A state newer than the installed client library reads back as a bare number.
    state = detail.state
    native = state.name if isinstance(state, PipelineTaskDetail.State) else str(state)
    return RunOutcome(result=result, native=native)


def pipeline_run_outcome(state: PipelineState) -> Optional[RunOutcome]:
    result = _PIPELINE_OUTCOMES.get(state)
    if result is None:
        return None
    return RunOutcome(result=result, native=PipelineState(state).name)


def is_status_for_run_event_class(status: Union[str, RunResultTypeClass]) -> bool:
    return status in [RunResultTypeClass.SUCCESS, RunResultTypeClass.FAILURE]
