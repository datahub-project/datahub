from typing import Union

from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.jobs import _RunnableJob
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1.types import JobState, PipelineState

from datahub.metadata.schema_classes import RunResultTypeClass


def get_automl_job_result_type(job: _TrainingJob) -> Union[str, RunResultTypeClass]:
    state = job.state
    if state == PipelineState.PIPELINE_STATE_SUCCEEDED:
        return RunResultTypeClass.SUCCESS
    elif state == PipelineState.PIPELINE_STATE_FAILED:
        return RunResultTypeClass.FAILURE
    elif state == PipelineState.PIPELINE_STATE_CANCELLED:
        return "Cancelled"
    elif state == PipelineState.PIPELINE_STATE_PAUSED:
        return "Paused"
    elif state == PipelineState.PIPELINE_STATE_QUEUED:
        return "Queued"
    elif state == PipelineState.PIPELINE_STATE_RUNNING:
        return "Running"
    elif state == PipelineState.PIPELINE_STATE_UNSPECIFIED:
        return "Unspecific"
    else:
        return "UNKNOWN"


def get_custom_job_result_type(job: _RunnableJob) -> Union[str, RunResultTypeClass]:
    state = job.state
    if state == JobState.JOB_STATE_SUCCEEDED:
        return RunResultTypeClass.SUCCESS
    elif state == JobState.JOB_STATE_FAILED:
        return RunResultTypeClass.FAILURE
    elif state == JobState.JOB_STATE_CANCELLED:
        return "Cancelled"
    elif state == JobState.JOB_STATE_PAUSED:
        return "Paused"
    elif state == JobState.JOB_STATE_QUEUED:
        return "Queued"
    elif state == JobState.JOB_STATE_RUNNING:
        return "Running"
    elif state == JobState.JOB_STATE_CANCELLING:
        return "Cancelling"
    elif state == JobState.JOB_STATE_EXPIRED:
        return "Expired"
    elif state == JobState.JOB_STATE_UPDATING:
        return "Updating"
    else:
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
    if status == 3:
        return RunResultTypeClass.SUCCESS
    elif status == 4:
        return RunResultTypeClass.FAILURE
    elif status == 2:
        return "RUNNING"
    elif status == 1:
        return "PENDING"
    elif status == 0:
        return "STATE_UNSPECIFIED"
    else:
        return "UNKNOWN"


def get_job_result_type(job: VertexAiResourceNoun) -> Union[str, RunResultTypeClass]:
    if isinstance(job, _TrainingJob):
        return get_automl_job_result_type(job)
    elif isinstance(job, _RunnableJob):
        return get_custom_job_result_type(job)
    return "UNKNOWN"
