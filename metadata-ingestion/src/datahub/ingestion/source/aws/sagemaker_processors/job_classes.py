from typing import Dict

from typing_extensions import Final

from datahub.metadata.schema_classes import JobStatusClass


class SageMakerJobInfo:

    # Note: The weird typing for the following commands is because the child classes
    # use a Final type to ensure that they're interpreted by the boto3 stubs correctly.
    # If we try to type these as plain strings, we get a TypeError because it's being converted
    # to a non-overwritable type.
    # See https://mypy.readthedocs.io/en/stable/final_attrs.html#details-of-using-final

    # boto3 command to get list of jobs
    @property
    def list_command(self) -> str:
        raise NotImplementedError

    # field in job listing response containing actual list
    @property
    def list_key(self) -> str:
        raise NotImplementedError

    # field in job listing response element corresponding to job name
    @property
    def list_name_key(self) -> str:
        raise NotImplementedError

    # field in job listing response element corresponding to job ARN
    @property
    def list_arn_key(self) -> str:
        raise NotImplementedError

    # boto3 command to get job details
    @property
    def describe_command(self) -> str:
        raise NotImplementedError

    # field in job description response corresponding to job name
    @property
    def describe_name_key(self) -> str:
        raise NotImplementedError

    # field in job description response corresponding to job ARN
    @property
    def describe_arn_key(self) -> str:
        raise NotImplementedError

    # field in job description response corresponding to job status
    @property
    def describe_status_key(self) -> str:
        raise NotImplementedError

    # job-specific mapping from boto3 status strings to DataHub-native enum
    status_map: Dict[str, str]

    # name of function for processing job for ingestion
    processor: str


class AutoMlJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_auto_ml_jobs
    list_command: Final = "list_auto_ml_jobs"
    list_key: Final = "AutoMLJobSummaries"
    list_name_key: Final = "AutoMLJobName"
    list_arn_key: Final = "AutoMLJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
    describe_command: Final = "describe_auto_ml_job"
    describe_name_key: Final = "AutoMLJobName"
    describe_arn_key: Final = "AutoMLJobArn"
    describe_status_key: Final = "AutoMLJobStatus"
    status_map = {
        "Completed": JobStatusClass.COMPLETED,
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Failed": JobStatusClass.FAILED,
        "Stopped": JobStatusClass.STOPPED,
        "Stopping": JobStatusClass.STOPPING,
    }

    processor = "process_auto_ml_job"


class CompilationJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_compilation_jobs
    list_command: Final = "list_compilation_jobs"
    list_key: Final = "CompilationJobSummaries"
    list_name_key: Final = "CompilationJobName"
    list_arn_key: Final = "CompilationJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
    describe_command: Final = "describe_compilation_job"
    describe_name_key: Final = "CompilationJobName"
    describe_arn_key: Final = "CompilationJobArn"
    describe_status_key: Final = "CompilationJobStatus"
    status_map = {
        "INPROGRESS": JobStatusClass.IN_PROGRESS,
        "COMPLETED": JobStatusClass.COMPLETED,
        "FAILED": JobStatusClass.FAILED,
        "STARTING": JobStatusClass.STARTING,
        "STOPPING": JobStatusClass.STOPPING,
        "STOPPED": JobStatusClass.STOPPED,
    }
    processor = "process_compilation_job"


class EdgePackagingJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_edge_packaging_jobs
    list_command: Final = "list_edge_packaging_jobs"
    list_key: Final = "EdgePackagingJobSummaries"
    list_name_key: Final = "EdgePackagingJobName"
    list_arn_key: Final = "EdgePackagingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
    describe_command: Final = "describe_edge_packaging_job"
    describe_name_key: Final = "EdgePackagingJobName"
    describe_arn_key: Final = "EdgePackagingJobArn"
    describe_status_key: Final = "EdgePackagingJobStatus"
    status_map = {
        "INPROGRESS": JobStatusClass.IN_PROGRESS,
        "COMPLETED": JobStatusClass.COMPLETED,
        "FAILED": JobStatusClass.FAILED,
        "STARTING": JobStatusClass.STARTING,
        "STOPPING": JobStatusClass.STOPPING,
        "STOPPED": JobStatusClass.STOPPED,
    }
    processor = "process_edge_packaging_job"


class HyperParameterTuningJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_hyper_parameter_tuning_jobs
    list_command: Final = "list_hyper_parameter_tuning_jobs"
    list_key: Final = "HyperParameterTuningJobSummaries"
    list_name_key: Final = "HyperParameterTuningJobName"
    list_arn_key: Final = "HyperParameterTuningJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
    describe_command: Final = "describe_hyper_parameter_tuning_job"
    describe_name_key: Final = "HyperParameterTuningJobName"
    describe_arn_key: Final = "HyperParameterTuningJobArn"
    describe_status_key: Final = "HyperParameterTuningJobStatus"
    status_map = {
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_hyper_parameter_tuning_job"


class LabelingJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_labeling_jobs
    list_command: Final = "list_labeling_jobs"
    list_key: Final = "LabelingJobSummaryList"
    list_name_key: Final = "LabelingJobName"
    list_arn_key: Final = "LabelingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
    describe_command: Final = "describe_labeling_job"
    describe_name_key: Final = "LabelingJobName"
    describe_arn_key: Final = "LabelingJobArn"
    describe_status_key: Final = "LabelingJobStatus"
    status_map = {
        "Initializing": JobStatusClass.STARTING,
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_labeling_job"


class ProcessingJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs
    list_command: Final = "list_processing_jobs"
    list_key: Final = "ProcessingJobSummaries"
    list_name_key: Final = "ProcessingJobName"
    list_arn_key: Final = "ProcessingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
    describe_command: Final = "describe_processing_job"
    describe_name_key: Final = "ProcessingJobName"
    describe_arn_key: Final = "ProcessingJobArn"
    describe_status_key: Final = "ProcessingJobStatus"
    status_map = {
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_processing_job"


class TrainingJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs
    list_command: Final = "list_training_jobs"
    list_key: Final = "TrainingJobSummaries"
    list_name_key: Final = "TrainingJobName"
    list_arn_key: Final = "TrainingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
    describe_command: Final = "describe_training_job"
    describe_name_key: Final = "TrainingJobName"
    describe_arn_key: Final = "TrainingJobArn"
    describe_status_key: Final = "TrainingJobStatus"
    status_map = {
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_training_job"


class TransformJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs
    list_command: Final = "list_transform_jobs"
    list_key: Final = "TransformJobSummaries"
    list_name_key: Final = "TransformJobName"
    list_arn_key: Final = "TransformJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
    describe_command: Final = "describe_transform_job"
    describe_name_key: Final = "TransformJobName"
    describe_arn_key: Final = "TransformJobArn"
    describe_status_key: Final = "TransformJobStatus"
    status_map = {
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_transform_job"
