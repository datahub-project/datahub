from datetime import datetime
from typing import Any, Dict, List, Optional


def process_auto_ml_job(job):
    """
    Process outputs from Boto3 describe_auto_ml_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
    """

    name: str = job["AutoMLJobName"]
    arn: str = job["AutoMLJobArn"]
    status: str = job["AutoMLJobStatus"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    end_time: Optional[datetime] = job.get("Endtime")

    input_data: Optional[Dict[str, str]] = (
        job["InputDataConfig"].get("DataSource", {}).get("S3DataSource")
    )

    if input_data:
        s3_datatype: str = input_data.get("S3DataType")
        s3_uri: str = input_data.get("S3Uri")

    output_data = job["OutputDataConfig"].get("S3OutputPath")

    objective: str = job["AutoMLJobObjective"]["MetricName"]
    problem: str = job["ProblemType"]

    job_config: Dict[str, Any] = job["AutoMLJobConfig"]

    failure_reason: Optional[str] = job.get("FailureReason")

    deploy_config_endpoint: str = job["ModelDeployConfig"]["EndpointName"]
    deploy_result_endpoint: str = job["ModelDeployResult"]["EndpointName"]


def process_compilation_job(job):

    """
    Process outputs from Boto3 describe_compilation_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
    """

    name: str = job["CompilationJobName"]
    arn: str = job["CompilationJobArn"]
    status: str = job["CompilationJobStatus"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    start_time: Optional[datetime] = job.get("CompilationStartTime")
    end_time: Optional[datetime] = job.get("CompilationEndTime")

    image: Optional[str] = job.get("InferenceImage")

    model_artifacts: Optional[str] = job.get("ModelArtifacts", {}).get(
        "S3ModelArtifacts"
    )

    input_config = job.get("InputConfig", {})
    input_s3_uri: Optional[str] = input_config.get("S3Uri")
    input_framework: Optional[str] = input_config.get("Framework")
    input_framework_version: Optional[str] = input_config.get("FrameworkVersion")

    output_config = job.get("OutputConfig", {})
    output_s3_uri: Optional[str] = output_config.get("S3OutputLocation")
    target_device: Optional[str] = output_config.get("TargetDevice")
    target_platform: Optional[Dict[str, str]] = output_config.get("TargetPlatform", {})

    target_platform_os: Optional[str] = target_platform.get("Os")
    target_platform_arch: Optional[str] = target_platform.get("Arch")
    target_platform_accelerator: Optional[str] = target_platform.get("Accelerator")

    return


def process_edge_packaging_job(job):

    """
    Process outputs from Boto3 describe_edge_packaging_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
    """

    name: str = job["EdgePackagingJobName"]
    arn: str = job["EdgePackagingJobArn"]
    status: str = job["EdgePackagingJobStatus"]
    status_message: str = job["EdgePackagingJobStatusMessage"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")

    model_artifact_s3_uri: Optional[str] = job.get("ModelArtifact")
    output_s3_uri: Optional[str] = job.get("OutputConfig", {}).get("S3OutputLocation")

    compilation_job: Optional[str] = job.get("CompilationJobName")
    model: Optional[str] = job.get("ModelName")
    model_version: Optional[str] = job.get("ModelVersion")

    return


def process_hyper_parameter_tuning_job(job):

    """
    Process outputs from Boto3 describe_hyper_parameter_tuning_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
    """

    name: str = job["HyperParameterTuningJobName"]
    arn: str = job["HyperParameterTuningJobArn"]
    status: str = job["HyperParameterTuningJobStatus"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    end_time: Optional[datetime] = job.get("HyperParameterTuningEndTime")

    return


def process_labeling_job(job):

    """
    Process outputs from Boto3 describe_labeling_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
    """

    name: str = job["LabelingJobName"]
    arn: str = job["LabelingJobArn"]
    status: str = job["LabelingJobStatus"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")

    attribute: str = job["LabelAttributeName"]

    tags: List[Dict[str, str]] = job["Tags"]

    input_s3_uri: Optional[str] = (
        job.get("InputConfig", {})
        .get("DataSource", {})
        .get("S3DataSource", {})
        .get("ManifestS3Uri")
    )
    category_config_s3_uri: Optional[str] = job.get("LabelCategoryConfigS3Uri")
    output_config_s3_uri: Optional[str] = job.get("OutputConfig", {}).get(
        "S3OutputPath"
    )
    output_s3_uri: Optional[str] = job.get("LabelingJobOutput", {}).get(
        "OutputDatasetS3Uri"
    )

    task_config: Dict[str, Any] = job["HumanTaskConfig"]
    input_config: Dict[str, Any] = job["InputConfig"]
    output_config: Dict[str, str] = job["OutputConfig"]

    return


def process_processing_job(job):

    """
    Process outputs from Boto3 describe_processing_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
    """

    name: str = job["ProcessingJobName"]
    arn: str = job["ProcessingJobArn"]
    status: str = job["ProcessingJobStatus"]

    role: str = job["RoleArn"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    start_time: Optional[datetime] = job.get("ProcessingStartTime")
    end_time: Optional[datetime] = job.get("ProcessingEndTime")

    auto_ml_arn: Optional[str] = job.get("AutoMLJobArn")
    training_arn: Optional[str] = job.get("TrainingJobArn")

    inputs = job["ProcessingInputs"]
    processed_inputs = []

    for input_config in inputs:

        input_name = input_config["InputName"]
        input_s3 = input_config.get("S3Input", {})
        input_s3_uri = input_s3.get("S3Uri")
        input_s3_type = input_s3.get("S3DataType")
        input_s3_mode = input_s3.get("S3InputMode")
        input_s3_distribution = input_s3.get("S3DataDistributionType")
        input_s3_compression = input_s3.get("S3CompressionType")

        input_athena = input_config.get("DatasetDefinition", {}).get(
            "AthenaDatasetDefinition", {}
        )
        input_athena_catalog = input_athena.get("Catalog")
        input_athena_database = input_athena.get("Database")
        input_athena_querystring = input_athena.get("QueryString")
        input_athena_workgroup = input_athena.get("WorkGroup")
        input_athena_s3_uri = input_athena.get("OutputS3Uri")
        input_athena_kms = input_athena.get("KmsKeyId")
        input_athena_format = input_athena.get("OutputFormat")
        input_athena_compression = input_athena.get("OutputCompression")

        input_redshift = input_config.get("DatasetDefinition", {}).get(
            "RedshiftDatasetDefinition", {}
        )
        input_redshift_cluster = input_redshift.get("ClusterId")
        input_redshift_database = input_redshift.get("Database")
        input_redshift_user = input_redshift.get("DbUser")
        input_redshift_querystring = input_redshift.get("QueryString")
        input_redshift_cluster_arn = input_redshift.get("ClusterRoleArn")
        input_redshift_s3_uri = input_redshift.get("OutputS3Uri")
        input_redshift_kms = input_redshift.get("KmsKeyId")
        input_redshift_format = input_redshift.get("OutputFormat")
        input_redshift_compression = input_redshift.get("OutputCompression")

    resources: Dict[str, str] = job["ProcessingResources"]["ClusterConfig"]

    outputs: List[Dict[str, Any]] = job.get("ProcessingOutputConfig", {}).get(
        "Outputs", []
    )
    processed_outputs = [
        {
            "output_name": output["OutputName"],
            "output_s3_uri": output.get("S3Output", {}).get("S3Uri"),
            "output_feature_store": output.get("FeatureStoreOutput", {}).get(
                "FeatureGroupName"
            ),
        }
        for output in outputs
    ]
    return


def process_training_job(job):

    """
    Process outputs from Boto3 describe_training_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
    """

    name: str = job["TrainingJobName"]
    arn: str = job["TrainingJobArn"]
    status: str = job["TrainingJobStatus"]
    secondary_status = job["SecondaryStatus"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    start_time: Optional[datetime] = job.get("TrainingStartTime")
    end_time: Optional[datetime] = job.get("TrainingEndTime")

    hyperparameters = job.get("HyperParameters", {})

    input_data_configs = job.get("InputDataConfig", [])

    processed_input_data = []

    for config in input_data_configs:

        channel_name = config.get("ChannelName")
        data_source = config.get("DataSource", {})

        s3_source = data_source.get("S3DataSource", {})
        s3_type = s3_source.get("S3Datatype")
        s3_uri = s3_source.get("S3Uri")
        s3_distribution_type = s3_source.get("S3DataDistributionType")
        s3_attribute_names = s3_source.get("AttributeNames")

        file_system_source = data_source.get("FileSystemDataSource", {})
        file_system_id = file_system_source.get("FileSystemId")
        file_system_mode = file_system_source.get("FileSystemAccessMode")
        file_system_type = file_system_source.get("FileSystemType")
        file_system_path = file_system_source.get("DirectoryPath")

        processed_input_data.append(
            {
                "channel_name": channel_name,
                "s3_type": s3_type,
                "s3_uri": s3_uri,
                "s3_distribution_type": s3_distribution_type,
                "s3_attribute_names": s3_attribute_names,
                "file_system_id": file_system_id,
                "file_system_mode": file_system_mode,
                "file_system_type": file_system_type,
                "file_system_path": file_system_path,
            }
        )

    output_s3_uri = job.get("OutputDataConfig", {}).get("S3OutputPath")

    checkpoint_s3_uri = job.get("CheckpointConfig", {}).get("S3Uri")

    debug_s3_path = job.get("DebugHookConfig", {}).get("S3OutputPath")

    debug_rule_configs = job.get("DebugRuleConfigurations", [])

    processed_debug_configs = [
        {"s3_uri": config.get("S3OutputPath")} for config in debug_rule_configs
    ]

    tensorboard_output_path = job.get("TensorBoardOutputConfig", {}).get("S3OutputPath")
    profiler_output_path = job.get("ProfilerConfig", {}).get("S3OutputPath")

    profiler_rule_configs = job.get("ProfilerRuleConfigurations", [])

    processed_profiler_configs = [
        {"s3_uri": config.get("S3OutputPath")} for config in profiler_rule_configs
    ]

    return


def process_transform_job(job):

    """
    Process outputs from Boto3 describe_transform_job()

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
    """

    name: str = job["TransformJobName"]
    arn: str = job["TransformJobArn"]
    status: str = job["TransformJobStatus"]

    create_time: Optional[datetime] = job.get("CreationTime")
    last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
    start_time: Optional[datetime] = job.get("TransformStartTime")
    end_time: Optional[datetime] = job.get("TransformEndTime")

    job_input = job.get("TransformInput", {})
    input_s3 = job_input.get("DataSource", {}).get("S3DataSource", {})
    input_s3_type = input_s3.get("S3DataType")
    input_s3_uri = input_s3.get("S3Uri")
    input_s3_compression = job_input.get("CompressionType")
    input_s3_split_type = job_input.get("SplitType")

    job_output = job.get("TransformOutput", {})
    output_s3_uri = job_output.get("S3OutputPath")

    labeling_arn = job.get("LabelingJobArn")
    auto_ml_arn = job.get("AutoMLJobArn")

    return
