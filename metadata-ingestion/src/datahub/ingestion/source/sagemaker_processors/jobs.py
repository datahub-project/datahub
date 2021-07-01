from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from datahub.emitter import mce_builder
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobSnapshotClass,
    MetadataChangeEventClass,
)

SAGEMAKER_JOB_TYPES = {
    "auto_ml": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_auto_ml_jobs
        "list_command": "list_auto_ml_jobs",
        "list_key": "AutoMLJobSummaries",
        "list_name_key": "LabelingJobName",
        "list_arn_key": "LabelingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        "describe_command": "describe_auto_ml_job",
        "describe_name_key": "AutoMLJobName",
    },
    "compilation": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_compilation_jobs
        "list_command": "list_compilation_jobs",
        "list_key": "CompilationJobSummaries",
        "list_name_key": "CompilationJobName",
        "list_arn_key": "CompilationJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
        "describe_command": "describe_compilation_job",
        "describe_name_key": "CompilationJobName",
    },
    "edge_packaging": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_edge_packaging_jobs
        "list_command": "list_edge_packaging_jobs",
        "list_key": "EdgePackagingJobSummaries",
        "list_name_key": "EdgePackagingJobArn",
        "list_arn_key": "EdgePackagingJobName",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
        "describe_command": "describe_edge_packaging_job",
        "describe_name_key": "EdgePackagingJobName",
    },
    "hyper_parameter_tuning": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_hyper_parameter_tuning_jobs
        "list_command": "list_hyper_parameter_tuning_jobs",
        "list_key": "HyperParameterTuningJobSummaries",
        "list_name_key": "HyperParameterTuningJobName",
        "list_arn_key": "HyperParameterTuningJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
        "describe_command": "describe_hyper_parameter_tuning_job",
        "describe_name_key": "HyperParameterTuningJobName",
    },
    "labeling": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_labeling_jobs
        "list_command": "list_labeling_jobs",
        "list_key": "LabelingJobSummaryList",
        "list_name_key": "LabelingJobName",
        "list_arn_key": "LabelingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
        "describe_command": "describe_labeling_job",
        "describe_name_key": "LabelingJobName",
    },
    "processing": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs
        "list_command": "list_processing_jobs",
        "list_key": "ProcessingJobSummaries",
        "list_name_key": "ProcessingJobName",
        "list_arn_key": "ProcessingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
        "describe_command": "describe_processing_job",
        "describe_name_key": "ProcessingJobName",
    },
    "training": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs
        "list_command": "list_training_jobs",
        "list_key": "TrainingJobSummaries",
        "list_name_key": "TrainingJobName",
        "list_arn_key": "TrainingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
        "describe_command": "describe_training_job",
        "describe_name_key": "TrainingJobName",
    },
    "transform": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs
        "list_command": "list_transform_jobs",
        "list_key": "TransformJobSummaries",
        "list_name_key": "TransformJobName",
        "list_arn_key": "TransformJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
        "describe_command": "describe_transform_job",
        "describe_name_key": "TransformJobName",
    },
}


def make_s3_urn(s3_uri: str, env: str, suffix: Optional[str] = None) -> str:
    # TODO: update Glue to use this as well

    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URIs should begin with 's3://'")
    # remove S3 prefix (s3://)
    s3_name = s3_uri[5:]

    if s3_name.endswith("/"):
        s3_name = s3_name[:-1]

    if suffix is not None:
        return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name}_{suffix},{env})"

    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name},{env})"


def make_sagemaker_job_urn(arn) -> str:

    # SageMaker has no global grouping property for jobs,
    # so we just file all of them under an umbrella DataFlow
    return mce_builder.make_data_job_urn(
        orchestrator="sagemaker",
        flow_id="sagemaker",
        job_id=arn,
    )


@dataclass
class SageMakerJob:
    job: MetadataChangeEventClass
    input_datasets: Dict[str, Dict[str, Any]] = {}
    output_datasets: Dict[str, Dict[str, Any]] = {}
    input_jobs: List[str] = []
    # TODO
    # we resolve output jobs to input ones after processing
    output_jobs: List[str] = []


def create_common_job_mce(
    name: str,
    arn: str,
    job_type: str,
    properties: Dict[str, Any],
) -> MetadataChangeEventClass:

    job_urn = make_sagemaker_job_urn(arn)

    return MetadataChangeEventClass(
        proposedSnapshot=DataJobSnapshotClass(
            urn=job_urn,
            aspects=[
                DataJobInfoClass(
                    name=f"{job_type}:{name}",
                    type="SAGEMAKER",
                    customProperties={
                        **{key: str(value) for key, value in properties.items()},
                        "jobType": job_type,
                    },
                ),
                # TODO: generate DataJobInputOutputClass aspects afterwards
            ],
        )
    )


@dataclass
class SageMakerJobProcessor:
    arn_to_name: Dict[str, Tuple[str, str]]
    name_to_arn: Dict[Tuple[str, str], str]
    env: str

    def process_auto_ml_job(self, job) -> SageMakerJob:
        """
        Process outputs from Boto3 describe_auto_ml_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        """

        name: str = job["AutoMLJobName"]
        arn: str = job["AutoMLJobArn"]
        # TODO: figure out what to do with these attributes
        # status: str = job["AutoMLJobStatus"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # end_time: Optional[datetime] = job.get("Endtime")

        input_data: Optional[Dict[str, str]] = (
            job["InputDataConfig"].get("DataSource", {}).get("S3DataSource")
        )

        input_datasets = {}

        if input_data is not None and "S3Uri" in input_data:
            input_datasets[make_s3_urn(input_data["S3Uri"], self.env)] = {
                "dataset_type": "s3",
                "uri": input_data["S3Uri"],
                "datatype": input_data.get("S3DataType"),
            }

        output_datasets = {}

        output_s3_path = job.get("OutputDataConfig", {}).get("S3OutputPath")

        if output_s3_path is not None:
            output_datasets[make_s3_urn(output_s3_path, self.env)] = {
                "dataset_type": "s3",
                "uri": output_s3_path,
            }

        job_mce = create_common_job_mce(name, arn, "AutoML", job)

        return SageMakerJob(
            job=job_mce, input_datasets=input_datasets, output_datasets=output_datasets
        )

    def process_compilation_job(self, job) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_compilation_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
        """

        name: str = job["CompilationJobName"]
        arn: str = job["CompilationJobArn"]
        # status: str = job["CompilationJobStatus"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # start_time: Optional[datetime] = job.get("CompilationStartTime")
        # end_time: Optional[datetime] = job.get("CompilationEndTime")

        input_datasets = {}

        input_data: Optional[Dict[str, Any]] = job.get("InputConfig")

        if input_data is not None and "S3Uri" in input_data:
            input_datasets[make_s3_urn(input_data["S3Uri"], self.env)] = {
                "dataset_type": "s3",
                "uri": input_data["S3Uri"],
                "framework": input_data.get("Framework"),
                "framework_version": input_data.get("FrameworkVersion"),
            }

        output_datasets = {}

        output_data: Optional[Dict[str, Any]] = job.get("OutputConfig")

        if output_data is not None and "S3OutputLocation" in output_data:
            output_datasets[make_s3_urn(output_data["S3OutputLocation"], self.env)] = {
                "dataset_type": "s3",
                "uri": output_data["S3Uri"],
                "target_device": output_data.get("TargetDevice"),
                "target_platform": output_data.get("TargetPlatform"),
            }

        job_mce = create_common_job_mce(name, arn, "Compilation", job)

        return SageMakerJob(
            job=job_mce, input_datasets=input_datasets, output_datasets=output_datasets
        )

    def process_edge_packaging_job(
        self,
        job,
    ) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_edge_packaging_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
        """

        name: str = job["EdgePackagingJobName"]
        arn: str = job["EdgePackagingJobArn"]
        # status: str = job["EdgePackagingJobStatus"]
        # status_message: str = job["EdgePackagingJobStatusMessage"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")

        output_datasets = {}

        model_artifact_s3_uri: Optional[str] = job.get("ModelArtifact")
        output_s3_uri: Optional[str] = job.get("OutputConfig", {}).get(
            "S3OutputLocation"
        )

        if model_artifact_s3_uri is not None:
            output_datasets[make_s3_urn(model_artifact_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": model_artifact_s3_uri,
            }

        if output_s3_uri is not None:
            output_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": output_s3_uri,
            }

        # "The name of the SageMaker Neo compilation job that is used to locate model artifacts that are being packaged."
        compilation_job_name: Optional[str] = job.get("CompilationJobName")

        output_jobs = []
        if compilation_job_name is not None:

            # globally unique job name
            job_name = ("compilation", compilation_job_name)

            if job_name in self.name_to_arn:

                output_jobs.append(make_sagemaker_job_urn(self.name_to_arn[job_name]))
            else:

                # TODO: make this a warning
                raise ValueError("Could not find compilation job")

        # TODO: see if we can link models here
        # model: Optional[str] = job.get("ModelName")
        # model_version: Optional[str] = job.get("ModelVersion")

        job_mce = create_common_job_mce(name, arn, "EdgePackaging", job)

        return SageMakerJob(
            job=job_mce, output_datasets=output_datasets, output_jobs=output_jobs
        )

    def process_hyper_parameter_tuning_job(
        self,
        job,
    ) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_hyper_parameter_tuning_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
        """

        name: str = job["HyperParameterTuningJobName"]
        arn: str = job["HyperParameterTuningJobArn"]
        # status: str = job["HyperParameterTuningJobStatus"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # end_time: Optional[datetime] = job.get("HyperParameterTuningEndTime")

        job_mce = create_common_job_mce(name, arn, "HyperParameterTuning", job)

        training_jobs = []

        for job in job.get("TrainingJobDefinitions", []):

            job_name = ("training", job["DefinitionName"])

            if job_name in self.name_to_arn:

                training_jobs.append(make_sagemaker_job_urn(self.name_to_arn[job_name]))
            else:

                # TODO: make this a warning
                raise ValueError("Could not find training job")

        return SageMakerJob(
            job=job_mce,
            output_jobs=training_jobs,
        )

    def process_labeling_job(self, job) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_labeling_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
        """

        name: str = job["LabelingJobName"]
        arn: str = job["LabelingJobArn"]
        # status: str = job["LabelingJobStatus"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")

        # attribute: str = job["LabelAttributeName"]

        # tags: List[Dict[str, str]] = job["Tags"]

        input_datasets = {}

        input_s3_uri: Optional[str] = (
            job.get("InputConfig", {})
            .get("DataSource", {})
            .get("S3DataSource", {})
            .get("ManifestS3Uri")
        )
        if input_s3_uri is not None:
            input_datasets[make_s3_urn(input_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": input_s3_uri,
            }
        category_config_s3_uri: Optional[str] = job.get("LabelCategoryConfigS3Uri")
        if category_config_s3_uri is not None:
            input_datasets[make_s3_urn(category_config_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": category_config_s3_uri,
            }

        output_datasets = {}

        output_s3_uri: Optional[str] = job.get("LabelingJobOutput", {}).get(
            "OutputDatasetS3Uri"
        )
        if output_s3_uri is not None:
            output_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": output_s3_uri,
            }
        output_config_s3_uri: Optional[str] = job.get("OutputConfig", {}).get(
            "S3OutputPath"
        )
        if output_config_s3_uri is not None:
            output_datasets[make_s3_urn(output_config_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": output_config_s3_uri,
            }

        job_mce = create_common_job_mce(name, arn, "Labeling", job)

        return SageMakerJob(
            job=job_mce,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_processing_job(self, job) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_processing_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
        """

        name: str = job["ProcessingJobName"]
        arn: str = job["ProcessingJobArn"]
        # status: str = job["ProcessingJobStatus"]

        # role: str = job["RoleArn"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # start_time: Optional[datetime] = job.get("ProcessingStartTime")
        # end_time: Optional[datetime] = job.get("ProcessingEndTime")

        input_jobs = []

        auto_ml_arn: Optional[str] = job.get("AutoMLJobArn")
        training_arn: Optional[str] = job.get("TrainingJobArn")

        if auto_ml_arn is not None:
            input_jobs.append(make_sagemaker_job_urn(auto_ml_arn))
        if training_arn:
            input_jobs.append(make_sagemaker_job_urn(training_arn))

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

        job_mce = create_common_job_mce(name, arn, "Processing", job)

        return SageMakerJob(
            job=job_mce,
            input_jobs=input_jobs,
        )

    def process_training_job(self, job) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_training_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
        """

        name: str = job["TrainingJobName"]
        arn: str = job["TrainingJobArn"]
        # status: str = job["TrainingJobStatus"]
        # secondary_status = job["SecondaryStatus"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # start_time: Optional[datetime] = job.get("TrainingStartTime")
        # end_time: Optional[datetime] = job.get("TrainingEndTime")

        # hyperparameters = job.get("HyperParameters", {})

        input_datasets = {}

        input_data_configs = job.get("InputDataConfig", [])

        for config in input_data_configs:

            data_source = config.get("DataSource", {})

            s3_source = data_source.get("S3DataSource", {})
            s3_uri = s3_source.get("S3Uri")

            if s3_uri is not None:
                input_datasets[make_s3_urn(s3_uri, self.env)] = {
                    "dataset_type": "s3",
                    "uri": s3_uri,
                    "datatype": s3_source.get("S3Datatype"),
                    "distribution_type": s3_source.get("S3DataDistributionType"),
                    "attribute_names": s3_source.get("AttributeNames"),
                    "channel_name": config.get("ChannelName"),
                }

        output_s3_uri = job.get("OutputDataConfig", {}).get("S3OutputPath")
        checkpoint_s3_uri = job.get("CheckpointConfig", {}).get("S3Uri")
        debug_s3_path = job.get("DebugHookConfig", {}).get("S3OutputPath")
        tensorboard_output_path = job.get("TensorBoardOutputConfig", {}).get(
            "S3OutputPath"
        )
        profiler_output_path = job.get("ProfilerConfig", {}).get("S3OutputPath")

        debug_rule_configs = job.get("DebugRuleConfigurations", [])
        processed_debug_configs = [
            config.get("S3OutputPath") for config in debug_rule_configs
        ]
        profiler_rule_configs = job.get("ProfilerRuleConfigurations", [])
        processed_profiler_configs = [
            config.get("S3OutputPath") for config in profiler_rule_configs
        ]

        output_datasets = {}

        for output_s3_uri in [
            output_s3_uri,
            checkpoint_s3_uri,
            debug_s3_path,
            tensorboard_output_path,
            profiler_output_path,
            *processed_debug_configs,
            *processed_profiler_configs,
        ]:

            if output_s3_uri is not None:
                output_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                    "dataset_type": "s3",
                    "uri": output_s3_uri,
                }

        job_mce = create_common_job_mce(name, arn, "Training", job)

        return SageMakerJob(
            job=job_mce,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_transform_job(self, job) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_transform_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
        """

        name: str = job["TransformJobName"]
        arn: str = job["TransformJobArn"]
        # status: str = job["TransformJobStatus"]

        # create_time: Optional[datetime] = job.get("CreationTime")
        # last_modified_time: Optional[datetime] = job.get("LastModifiedTime")
        # start_time: Optional[datetime] = job.get("TransformStartTime")
        # end_time: Optional[datetime] = job.get("TransformEndTime")

        job_input = job.get("TransformInput", {})
        input_s3 = job_input.get("DataSource", {}).get("S3DataSource", {})

        input_s3_uri = input_s3.get("S3Uri")

        input_datasets = {}

        if input_s3_uri is not None:

            input_datasets[make_s3_urn(input_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": input_s3_uri,
                "datatype": input_s3.get("S3DataType"),
                "compression": job_input.get("CompressionType"),
                "split": job_input.get("SplitType"),
            }

        output_datasets = {}

        output_s3_uri = job.get("TransformOutput", {}).get("S3OutputPath")

        if output_s3_uri is not None:
            input_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": output_s3_uri,
            }

        labeling_arn = job.get("LabelingJobArn")
        auto_ml_arn = job.get("AutoMLJobArn")

        input_jobs = []

        if labeling_arn is not None:
            input_jobs.append(make_sagemaker_job_urn(labeling_arn))
        if auto_ml_arn is not None:
            input_jobs.append(make_sagemaker_job_urn(auto_ml_arn))

        job_mce = create_common_job_mce(name, arn, "Transform", job)

        return SageMakerJob(
            job=job_mce,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
            input_jobs=input_jobs,
        )
