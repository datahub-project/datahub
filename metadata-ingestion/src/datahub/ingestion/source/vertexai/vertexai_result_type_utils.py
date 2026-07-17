from typing import Dict, Optional, Union

from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.jobs import _RunnableJob
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1.types import JobState, PipelineState, PipelineTaskDetail

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

PIPELINE_TASK_STATE_MAPPING: Dict[int, Union[str, RunResultTypeClass]] = {
    PipelineTaskDetail.State.SUCCEEDED: RunResultTypeClass.SUCCESS,
    PipelineTaskDetail.State.FAILED: RunResultTypeClass.FAILURE,
    PipelineTaskDetail.State.SKIPPED: RunResultTypeClass.SKIPPED,
}

# Note: The following Vertex AI pipeline task states cannot be mapped because
# DataHub's RunResultType enum only supports: SUCCESS, FAILURE, SKIPPED, UP_FOR_RETRY.
# These states will return UNKNOWN_STATUS and won't generate run events:
# - PipelineTaskDetail.State.STATE_UNSPECIFIED
# - PipelineTaskDetail.State.PENDING
# - PipelineTaskDetail.State.RUNNING
# - PipelineTaskDetail.State.CANCEL_PENDING
# - PipelineTaskDetail.State.CANCELLING
# - PipelineTaskDetail.State.NOT_TRIGGERED
# - PipelineTaskDetail.State.CANCELLED (could potentially map to SKIPPED, but unclear semantics)

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


def get_pipeline_task_result_status(
    status: Optional[PipelineTaskDetail.State],
) -> Union[str, RunResultTypeClass]:
    """
    Map Vertex AI pipeline task state to DataHub RunResultType.

    Only terminal states (SUCCEEDED, FAILED, SKIPPED) are mapped to DataHub enums.
    Intermediate states (PENDING, RUNNING, etc.) return UNKNOWN_STATUS as they
    don't have corresponding values in DataHub's RunResultType enum.
    """
    if status is None:
        return UNKNOWN_STATUS
    return PIPELINE_TASK_STATE_MAPPING.get(status, UNKNOWN_STATUS)


def is_status_for_run_event_class(status: Union[str, RunResultTypeClass]) -> bool:
    return status in [RunResultTypeClass.SUCCESS, RunResultTypeClass.FAILURE]
