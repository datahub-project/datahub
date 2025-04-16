from typing import Optional, Union

from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.jobs import _RunnableJob
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1.types import JobState, PipelineState, PipelineTaskDetail

from datahub.metadata.schema_classes import RunResultTypeClass


def get_automl_job_result_type(state: PipelineState) -> Union[str, RunResultTypeClass]:
    state_mapping = {
        PipelineState.PIPELINE_STATE_SUCCEEDED: RunResultTypeClass.SUCCESS,
        PipelineState.PIPELINE_STATE_FAILED: RunResultTypeClass.FAILURE,
        PipelineState.PIPELINE_STATE_CANCELLED: "Cancelled",
        PipelineState.PIPELINE_STATE_PAUSED: "Paused",
        PipelineState.PIPELINE_STATE_QUEUED: "Queued",
        PipelineState.PIPELINE_STATE_RUNNING: "Running",
        PipelineState.PIPELINE_STATE_UNSPECIFIED: "Unspecific",
    }

    return state_mapping.get(state, "UNKNOWN")


def get_custom_job_result_type(state: JobState) -> Union[str, RunResultTypeClass]:
    state_mapping = {
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
    return state_mapping.get(state, "UNKNOWN")


def get_job_result_status(job: VertexAiResourceNoun) -> Union[str, RunResultTypeClass]:
    if isinstance(job, _TrainingJob) and isinstance(job.state, PipelineState):
        return get_automl_job_result_type(job.state)
    elif isinstance(job, _RunnableJob) and isinstance(job.state, JobState):
        return get_custom_job_result_type(job.state)
    return "UNKNOWN"


def get_execution_result_status(status: int) -> Union[str, RunResultTypeClass]:
    """
    State of the execution.
    STATE_UNSPECIFIED = 0
    PENDING = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    """
    status_mapping = {
        0: "STATE_UNSPECIFIED",
        1: "PENDING",
        2: "RUNNING",
        3: RunResultTypeClass.SUCCESS,
        4: RunResultTypeClass.FAILURE,
    }
    return status_mapping.get(status, "UNKNOWN")


def get_pipeline_task_result_status(
    status: Optional[PipelineTaskDetail.State],
) -> Union[str, RunResultTypeClass]:
    # TODO: DataProcessInstanceRunResultClass fails with status string except for SUCCESS, FAILURE, SKIPPED,
    #  which will be fixed in the future
    status_mapping = {
        # PipelineTaskDetail.State.STATE_UNSPECIFIED: "STATE_UNSPECIFIED",
        # PipelineTaskDetail.State.PENDING: "PENDING",
        # PipelineTaskDetail.State.RUNNING: "RUNNING",
        # PipelineTaskDetail.State.CANCEL_PENDING: "CANCEL_PENDING",
        # PipelineTaskDetail.State.CANCELLING: "CANCELLING",
        # PipelineTaskDetail.State.NOT_TRIGGERED: "NOT_TRIGGERED",
        PipelineTaskDetail.State.SUCCEEDED: RunResultTypeClass.SUCCESS,
        PipelineTaskDetail.State.FAILED: RunResultTypeClass.FAILURE,
        PipelineTaskDetail.State.SKIPPED: RunResultTypeClass.SKIPPED,
    }
    if status is None:
        return "UNKNOWN"
    return status_mapping.get(status, "UNKNOWN")


def is_status_for_run_event_class(status: Union[str, RunResultTypeClass]) -> bool:
    return status in [RunResultTypeClass.SUCCESS, RunResultTypeClass.FAILURE]
