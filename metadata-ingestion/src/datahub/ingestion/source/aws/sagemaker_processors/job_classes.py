from typing import Dict, Literal

from datahub.metadata.schema_classes import JobStatusClass


class SageMakerJobInfo:
    # boto3 command to get list of jobs
    list_command: str
    # field in job listing response containing actual list
    list_key: str
    # field in job listing response element corresponding to job name
    list_name_key: str
    # field in job listing response element corresponding to job ARN
    list_arn_key: str

    # boto3 command to get job details
    describe_command: str
    # field in job description response corresponding to job name
    describe_name_key: str
    # field in job description response corresponding to job ARN
    describe_arn_key: str
    # field in job description response corresponding to job status
    describe_status_key: str
    # job-specific mapping from boto3 status strings to DataHub-native enum
    status_map: Dict[str, str]

    # name of function for processing job for ingestion
    processor: str


class AutoMlJobInfo(SageMakerJobInfo):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_auto_ml_jobs
    list_command: Literal["list_auto_ml_jobs"] = "list_auto_ml_jobs"
    list_key: Literal["AutoMLJobSummaries"] = "AutoMLJobSummaries"
    list_name_key: Literal["AutoMLJobName"] = "AutoMLJobName"
    list_arn_key: Literal["AutoMLJobArn"] = "AutoMLJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
    describe_command: Literal["describe_auto_ml_job"] = "describe_auto_ml_job"
    describe_name_key: Literal["AutoMLJobName"] = "AutoMLJobName"
    describe_arn_key: Literal["AutoMLJobArn"] = "AutoMLJobArn"
    describe_status_key: Literal["AutoMLJobStatus"] = "AutoMLJobStatus"
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
    list_command: Literal["list_compilation_jobs"] = "list_compilation_jobs"
    list_key: Literal["CompilationJobSummaries"] = "CompilationJobSummaries"
    list_name_key: Literal["CompilationJobName"] = "CompilationJobName"
    list_arn_key: Literal["CompilationJobArn"] = "CompilationJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
    describe_command: Literal["describe_compilation_job"] = "describe_compilation_job"
    describe_name_key: Literal["CompilationJobName"] = "CompilationJobName"
    describe_arn_key: Literal["CompilationJobArn"] = "CompilationJobArn"
    describe_status_key: Literal["CompilationJobStatus"] = "CompilationJobStatus"
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
    list_command: Literal["list_edge_packaging_jobs"] = "list_edge_packaging_jobs"
    list_key: Literal["EdgePackagingJobSummaries"] = "EdgePackagingJobSummaries"
    list_name_key: Literal["EdgePackagingJobName"] = "EdgePackagingJobName"
    list_arn_key: Literal["EdgePackagingJobArn"] = "EdgePackagingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
    describe_command: Literal[
        "describe_edge_packaging_job"
    ] = "describe_edge_packaging_job"
    describe_name_key: Literal["EdgePackagingJobName"] = "EdgePackagingJobName"
    describe_arn_key: Literal["EdgePackagingJobArn"] = "EdgePackagingJobArn"
    describe_status_key: Literal["EdgePackagingJobStatus"] = "EdgePackagingJobStatus"
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
    list_command: Literal[
        "list_hyper_parameter_tuning_jobs"
    ] = "list_hyper_parameter_tuning_jobs"
    list_key: Literal[
        "HyperParameterTuningJobSummaries"
    ] = "HyperParameterTuningJobSummaries"
    list_name_key: Literal[
        "HyperParameterTuningJobName"
    ] = "HyperParameterTuningJobName"
    list_arn_key: Literal["HyperParameterTuningJobArn"] = "HyperParameterTuningJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
    describe_command: Literal[
        "describe_hyper_parameter_tuning_job"
    ] = "describe_hyper_parameter_tuning_job"
    describe_name_key: Literal[
        "HyperParameterTuningJobName"
    ] = "HyperParameterTuningJobName"
    describe_arn_key: Literal[
        "HyperParameterTuningJobArn"
    ] = "HyperParameterTuningJobArn"
    describe_status_key: Literal[
        "HyperParameterTuningJobStatus"
    ] = "HyperParameterTuningJobStatus"
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
    list_command: Literal["list_labeling_jobs"] = "list_labeling_jobs"
    list_key: Literal["LabelingJobSummaryList"] = "LabelingJobSummaryList"
    list_name_key: Literal["LabelingJobName"] = "LabelingJobName"
    list_arn_key: Literal["LabelingJobArn"] = "LabelingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
    describe_command: Literal["describe_labeling_job"] = "describe_labeling_job"
    describe_name_key: Literal["LabelingJobName"] = "LabelingJobName"
    describe_arn_key: Literal["LabelingJobArn"] = "LabelingJobArn"
    describe_status_key: Literal["LabelingJobStatus"] = "LabelingJobStatus"
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
    list_command: Literal["list_processing_jobs"] = "list_processing_jobs"
    list_key: Literal["ProcessingJobSummaries"] = "ProcessingJobSummaries"
    list_name_key: Literal["ProcessingJobName"] = "ProcessingJobName"
    list_arn_key: Literal["ProcessingJobArn"] = "ProcessingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
    describe_command: Literal["describe_processing_job"] = "describe_processing_job"
    describe_name_key: Literal["ProcessingJobName"] = "ProcessingJobName"
    describe_arn_key: Literal["ProcessingJobArn"] = "ProcessingJobArn"
    describe_status_key: Literal["ProcessingJobStatus"] = "ProcessingJobStatus"
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
    list_command: Literal["list_training_jobs"] = "list_training_jobs"
    list_key: Literal["TrainingJobSummaries"] = "TrainingJobSummaries"
    list_name_key: Literal["TrainingJobName"] = "TrainingJobName"
    list_arn_key: Literal["TrainingJobArn"] = "TrainingJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
    describe_command: Literal["describe_training_job"] = "describe_training_job"
    describe_name_key: Literal["TrainingJobName"] = "TrainingJobName"
    describe_arn_key: Literal["TrainingJobArn"] = "TrainingJobArn"
    describe_status_key: Literal["TrainingJobStatus"] = "TrainingJobStatus"
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
    list_command: Literal["list_transform_jobs"] = "list_transform_jobs"
    list_key: Literal["TransformJobSummaries"] = "TransformJobSummaries"
    list_name_key: Literal["TransformJobName"] = "TransformJobName"
    list_arn_key: Literal["TransformJobArn"] = "TransformJobArn"
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
    describe_command: Literal["describe_transform_job"] = "describe_transform_job"
    describe_name_key: Literal["TransformJobName"] = "TransformJobName"
    describe_arn_key: Literal["TransformJobArn"] = "TransformJobArn"
    describe_status_key: Literal["TransformJobStatus"] = "TransformJobStatus"
    status_map = {
        "InProgress": JobStatusClass.IN_PROGRESS,
        "Completed": JobStatusClass.COMPLETED,
        "Failed": JobStatusClass.FAILED,
        "Stopping": JobStatusClass.STOPPING,
        "Stopped": JobStatusClass.STOPPED,
    }
    processor = "process_transform_job"
