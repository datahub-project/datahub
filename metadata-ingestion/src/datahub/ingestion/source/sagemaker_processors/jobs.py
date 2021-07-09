from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from datahub.emitter import mce_builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_common import make_s3_urn
from datahub.ingestion.source.sagemaker_processors.common import SagemakerSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataFlowInfoClass,
    DataFlowSnapshotClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobSnapshotClass,
    DatasetPropertiesClass,
    JobStatusClass,
)


@dataclass
class SageMakerJobType:
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


# map from SageMaker job code to metadata on API access commands, fields, and processors
SAGEMAKER_JOB_TYPES = {
    "auto_ml": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_auto_ml_jobs
        list_command="list_auto_ml_jobs",
        list_key="AutoMLJobSummaries",
        list_name_key="AutoMLJobName",
        list_arn_key="AutoMLJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        describe_command="describe_auto_ml_job",
        describe_name_key="AutoMLJobName",
        describe_arn_key="AutoMLJobArn",
        describe_status_key="AutoMLJobStatus",
        status_map={
            "Completed": JobStatusClass.COMPLETED,
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Failed": JobStatusClass.FAILED,
            "Stopped": JobStatusClass.STOPPED,
            "Stopping": JobStatusClass.STOPPING,
        },
        processor="process_auto_ml_job",
    ),
    "compilation": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_compilation_jobs
        list_command="list_compilation_jobs",
        list_key="CompilationJobSummaries",
        list_name_key="CompilationJobName",
        list_arn_key="CompilationJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
        describe_command="describe_compilation_job",
        describe_name_key="CompilationJobName",
        describe_arn_key="CompilationJobArn",
        describe_status_key="CompilationJobStatus",
        status_map={
            "INPROGRESS": JobStatusClass.IN_PROGRESS,
            "COMPLETED": JobStatusClass.COMPLETED,
            "FAILED": JobStatusClass.FAILED,
            "STARTING": JobStatusClass.STARTING,
            "STOPPING": JobStatusClass.STOPPING,
            "STOPPED": JobStatusClass.STOPPED,
        },
        processor="process_compilation_job",
    ),
    "edge_packaging": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_edge_packaging_jobs
        list_command="list_edge_packaging_jobs",
        list_key="EdgePackagingJobSummaries",
        list_name_key="EdgePackagingJobName",
        list_arn_key="EdgePackagingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
        describe_command="describe_edge_packaging_job",
        describe_name_key="EdgePackagingJobName",
        describe_arn_key="EdgePackagingJobArn",
        describe_status_key="EdgePackagingJobStatus",
        status_map={
            "INPROGRESS": JobStatusClass.IN_PROGRESS,
            "COMPLETED": JobStatusClass.COMPLETED,
            "FAILED": JobStatusClass.FAILED,
            "STARTING": JobStatusClass.STARTING,
            "STOPPING": JobStatusClass.STOPPING,
            "STOPPED": JobStatusClass.STOPPED,
        },
        processor="process_edge_packaging_job",
    ),
    "hyper_parameter_tuning": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_hyper_parameter_tuning_jobs
        list_command="list_hyper_parameter_tuning_jobs",
        list_key="HyperParameterTuningJobSummaries",
        list_name_key="HyperParameterTuningJobName",
        list_arn_key="HyperParameterTuningJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
        describe_command="describe_hyper_parameter_tuning_job",
        describe_name_key="HyperParameterTuningJobName",
        describe_arn_key="HyperParameterTuningJobArn",
        describe_status_key="HyperParameterTuningJobStatus",
        status_map={
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Completed": JobStatusClass.COMPLETED,
            "Failed": JobStatusClass.FAILED,
            "Stopping": JobStatusClass.STOPPING,
            "Stopped": JobStatusClass.STOPPED,
        },
        processor="process_hyper_parameter_tuning_job",
    ),
    "labeling": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_labeling_jobs
        list_command="list_labeling_jobs",
        list_key="LabelingJobSummaryList",
        list_name_key="LabelingJobName",
        list_arn_key="LabelingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
        describe_command="describe_labeling_job",
        describe_name_key="LabelingJobName",
        describe_arn_key="LabelingJobArn",
        describe_status_key="LabelingJobStatus",
        status_map={
            "Initializing": JobStatusClass.STARTING,
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Completed": JobStatusClass.COMPLETED,
            "Failed": JobStatusClass.FAILED,
            "Stopping": JobStatusClass.STOPPING,
            "Stopped": JobStatusClass.STOPPED,
        },
        processor="process_labeling_job",
    ),
    "processing": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs
        list_command="list_processing_jobs",
        list_key="ProcessingJobSummaries",
        list_name_key="ProcessingJobName",
        list_arn_key="ProcessingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
        describe_command="describe_processing_job",
        describe_name_key="ProcessingJobName",
        describe_arn_key="ProcessingJobArn",
        describe_status_key="ProcessingJobStatus",
        status_map={
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Completed": JobStatusClass.COMPLETED,
            "Failed": JobStatusClass.FAILED,
            "Stopping": JobStatusClass.STOPPING,
            "Stopped": JobStatusClass.STOPPED,
        },
        processor="process_processing_job",
    ),
    "training": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs
        list_command="list_training_jobs",
        list_key="TrainingJobSummaries",
        list_name_key="TrainingJobName",
        list_arn_key="TrainingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
        describe_command="describe_training_job",
        describe_name_key="TrainingJobName",
        describe_arn_key="TrainingJobArn",
        describe_status_key="TrainingJobStatus",
        status_map={
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Completed": JobStatusClass.COMPLETED,
            "Failed": JobStatusClass.FAILED,
            "Stopping": JobStatusClass.STOPPING,
            "Stopped": JobStatusClass.STOPPED,
        },
        processor="process_training_job",
    ),
    "transform": SageMakerJobType(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs
        list_command="list_transform_jobs",
        list_key="TransformJobSummaries",
        list_name_key="TransformJobName",
        list_arn_key="TransformJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
        describe_command="describe_transform_job",
        describe_name_key="TransformJobName",
        describe_arn_key="TransformJobArn",
        describe_status_key="TransformJobStatus",
        status_map={
            "InProgress": JobStatusClass.IN_PROGRESS,
            "Completed": JobStatusClass.COMPLETED,
            "Failed": JobStatusClass.FAILED,
            "Stopping": JobStatusClass.STOPPING,
            "Stopped": JobStatusClass.STOPPED,
        },
        processor="process_transform_job",
    ),
}


def make_sagemaker_flow_urn(job_type: str, job_name: str, env: str) -> str:

    return mce_builder.make_data_flow_urn(
        orchestrator="sagemaker", flow_id=f"{job_type}:{job_name}", cluster=env
    )


def make_sagemaker_job_urn(job_type: str, job_name: str, arn: str, env: str) -> str:

    flow_urn = make_sagemaker_flow_urn(job_type, job_name, env)

    # SageMaker has no global grouping property for jobs,
    # so we create a flow for every single job
    return mce_builder.make_data_job_urn_with_flow(flow_urn=flow_urn, job_id=arn)


@dataclass
class SageMakerJob:
    """
    Intermediate job representation for storing result of initial ingestion from raw API response.

    Produced by first-pass ingestion and basis for subsequent extraction.
    """

    job_snapshot: DataJobSnapshotClass
    job_name: str
    job_arn: str
    job_type: str
    input_datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    output_datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    input_jobs: Set[str] = field(default_factory=set)
    # we resolve output jobs to input ones after processing
    output_jobs: Set[str] = field(default_factory=set)


@dataclass
class JobProcessor:
    """
    Job ingestion module, called by top-level SageMaker ingestion handler.
    """

    # boto3 SageMaker client
    sagemaker_client: Any
    env: str
    report: SagemakerSourceReport
    # config filter for specific job types to ingest (see metadata-ingestion README)
    job_type_filter: Union[Dict[str, str], bool, None]

    # translators between ARNs and job names (represented as tuples of (job_type, job_name))
    arn_to_name: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    name_to_arn: Dict[Tuple[str, str], str] = field(default_factory=dict)

    def get_all_jobs(
        self,
    ) -> List[Dict[str, Any]]:
        """
        List all jobs in SageMaker.
        """

        jobs = []

        # dictionaries for translating between type-specific job names and ARNs
        self.arn_to_name: Dict[str, Tuple[str, str]] = {}
        self.name_to_arn: Dict[Tuple[str, str], str] = {}

        if self.job_type_filter is True:
            allowed_jobs = sorted(SAGEMAKER_JOB_TYPES.keys())
        elif isinstance(self.job_type_filter, dict):
            allowed_jobs = sorted(
                [
                    job_type
                    for job_type in SAGEMAKER_JOB_TYPES.keys()
                    if self.job_type_filter.get(job_type, True) is True
                ]
            )

        # iterate through keys in sorted order for consistency
        for job_type in sorted(allowed_jobs):

            job_spec = SAGEMAKER_JOB_TYPES[job_type]

            paginator = self.sagemaker_client.get_paginator(job_spec.list_command)
            for page in paginator.paginate():
                page_jobs = page[job_spec.list_key]

                for job in page_jobs:
                    job_name = (job_type, job[job_spec.list_name_key])
                    job_arn = job[job_spec.list_arn_key]

                    self.arn_to_name[job_arn] = job_name
                    self.name_to_arn[job_name] = job_arn

                page_jobs = [{**job, "type": job_type} for job in page_jobs]

                jobs += page_jobs

        return jobs

    def get_job_details(self, job_name: str, job_type: str) -> Dict[str, Any]:
        """
        Get boto3 describe_<job> response
        """

        describe_command = SAGEMAKER_JOB_TYPES[job_type].describe_command
        describe_name_key = SAGEMAKER_JOB_TYPES[job_type].describe_name_key

        return getattr(self.sagemaker_client, describe_command)(
            **{describe_name_key: job_name}
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        jobs = self.get_all_jobs()

        processed_jobs: Dict[str, SageMakerJob] = {}

        # first pass: process jobs and collect datasets used
        for job in jobs:

            job_type = SAGEMAKER_JOB_TYPES[job["type"]]
            job_name = job[job_type.list_name_key]

            job_details = self.get_job_details(job_name, job["type"])

            processed_job = getattr(self, job_type.processor)(job_details)
            processed_jobs[processed_job.job_snapshot.urn] = processed_job

        all_datasets = {}

        # second pass:
        #   - move output jobs to inputs
        #   - aggregate i/o datasets
        for job_urn in sorted(processed_jobs):
            processed_job = processed_jobs[job_urn]

            for output_job_urn in processed_job.output_jobs:
                processed_jobs[output_job_urn].input_jobs.add(output_job_urn)

            all_datasets.update(processed_job.input_datasets)
            all_datasets.update(processed_job.output_datasets)

        # yield datasets
        for dataset_urn, dataset in all_datasets.items():

            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[],
            )
            dataset_snapshot.aspects.append(
                DatasetPropertiesClass(
                    customProperties={k: str(v) for k, v in dataset.items()},
                    tags=[],
                )
            )
            dataset_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            dataset_wu = MetadataWorkUnit(
                id=dataset_urn,
                mce=dataset_mce,
            )
            self.report.report_dataset_scanned()
            self.report.report_workunit(dataset_wu)
            yield dataset_wu

        # third pass: construct and yield MCEs
        for job_urn in sorted(processed_jobs):

            processed_job = processed_jobs[job_urn]
            job_snapshot = processed_job.job_snapshot

            flow_urn = make_sagemaker_flow_urn(
                processed_job.job_type, processed_job.job_name, self.env
            )

            # create flow for each job
            flow_mce = MetadataChangeEvent(
                proposedSnapshot=DataFlowSnapshotClass(
                    urn=flow_urn,
                    aspects=[
                        DataFlowInfoClass(
                            name=processed_job.job_name,
                        ),
                    ],
                )
            )
            flow_wu = MetadataWorkUnit(
                id=flow_urn,
                mce=flow_mce,
            )
            self.report.report_workunit(flow_wu)
            yield flow_wu

            job_snapshot.aspects.append(
                DataJobInputOutputClass(
                    inputDatasets=sorted(list(processed_job.input_datasets.keys())),
                    outputDatasets=sorted(list(processed_job.output_datasets.keys())),
                    inputDatajobs=sorted(list(processed_job.input_jobs)),
                )
            )

            job_mce = MetadataChangeEvent(proposedSnapshot=job_snapshot)
            job_wu = MetadataWorkUnit(
                id=job_urn,
                mce=job_mce,
            )
            self.report.report_job_scanned()
            self.report.report_workunit(job_wu)
            yield job_wu

    def create_common_job_snapshot(
        self,
        job: Dict[str, Any],
        job_type: str,
    ) -> Tuple[DataJobSnapshotClass, str, str]:
        """
        General function for generating a job snapshot.
        """

        job_type_info = SAGEMAKER_JOB_TYPES[job_type]

        name = job[job_type_info.describe_name_key]
        arn = job[job_type_info.describe_arn_key]

        sagemaker_status = job[job_type_info.describe_status_key]

        mapped_status = job_type_info.status_map.get(sagemaker_status)

        if mapped_status is None:
            mapped_status = JobStatusClass.UNKNOWN

            self.report.report_warning(
                name,
                f"Unknown status for {name} ({arn}): {sagemaker_status}",
            )

        job_urn = make_sagemaker_job_urn(job_type, name, arn, self.env)
        job_snapshot = DataJobSnapshotClass(
            urn=job_urn,
            aspects=[
                DataJobInfoClass(
                    name=name,
                    type="SAGEMAKER",
                    status=mapped_status,
                    customProperties={
                        **{key: str(value) for key, value in job.items()},
                        "jobType": job_type,
                    },
                ),
                BrowsePathsClass(paths=[f"{job_type}/{name}"]),
            ],
        )

        return job_snapshot, name, arn

    def process_auto_ml_job(self, job: Dict[str, Any]) -> SageMakerJob:
        """
        Process outputs from Boto3 describe_auto_ml_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        """

        JOB_TYPE = "auto_ml"

        input_datasets = {}

        for input_config in job.get("InputDataConfig", []):
            input_data = input_config.get("DataSource", {}).get("S3DataSource")

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

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_compilation_job(self, job: Dict[str, Any]) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_compilation_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
        """

        JOB_TYPE = "compilation"

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
                "uri": output_data["S3OutputLocation"],
                "target_device": output_data.get("TargetDevice"),
                "target_platform": output_data.get("TargetPlatform"),
            }

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_edge_packaging_job(
        self,
        job: Dict[str, Any],
    ) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_edge_packaging_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
        """

        JOB_TYPE = "edge_packaging"

        name: str = job["EdgePackagingJobName"]
        arn: str = job["EdgePackagingJobArn"]

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

        # from docs: "The name of the SageMaker Neo compilation job that is used to locate model artifacts that are being packaged."
        compilation_job_name: Optional[str] = job.get("CompilationJobName")

        output_jobs = set()
        if compilation_job_name is not None:

            # globally unique job name
            full_job_name = ("compilation", compilation_job_name)

            if full_job_name in self.name_to_arn:

                output_jobs.add(
                    make_sagemaker_job_urn(
                        "compilation",
                        compilation_job_name,
                        self.name_to_arn[full_job_name],
                        self.env,
                    )
                )
            else:

                self.report.report_warning(
                    name,
                    f"Unable to find ARN for compilation job {compilation_job_name} produced by edge packaging job {arn}",
                )

        # TODO: see if we can link models here (will require adding some aspect to either jobs or models)
        # model: Optional[str] = job.get("ModelName")
        # model_version: Optional[str] = job.get("ModelVersion")

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            output_datasets=output_datasets,
            output_jobs=output_jobs,
        )

    def process_hyper_parameter_tuning_job(
        self,
        job: Dict[str, Any],
    ) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_hyper_parameter_tuning_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
        """

        JOB_TYPE = "hyper_parameter_tuning"

        name: str = job["HyperParameterTuningJobName"]
        arn: str = job["HyperParameterTuningJobArn"]

        training_jobs = set()

        for training_job in job.get("TrainingJobDefinitions", []):

            full_job_name = ("training", training_job["DefinitionName"])

            if full_job_name in self.name_to_arn:

                training_jobs.add(
                    make_sagemaker_job_urn(
                        "training",
                        training_job["DefinitionName"],
                        self.name_to_arn[full_job_name],
                        self.env,
                    )
                )
            else:

                self.report.report_warning(
                    name,
                    f"Unable to find ARN for training job {training_job['DefinitionName']} produced by hyperparameter tuning job {arn}",
                )

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            output_jobs=training_jobs,
        )

    def process_labeling_job(self, job: Dict[str, Any]) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_labeling_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
        """

        JOB_TYPE = "labeling"

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

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_processing_job(self, job: Dict[str, Any]) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_processing_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
        """

        JOB_TYPE = "processing"

        input_jobs = set()

        auto_ml_arn: Optional[str] = job.get("AutoMLJobArn")
        training_arn: Optional[str] = job.get("TrainingJobArn")

        if auto_ml_arn is not None:
            auto_ml_type, auto_ml_name = self.arn_to_name.get(auto_ml_arn, (None, None))

            if auto_ml_type is not None and auto_ml_name is not None:
                input_jobs.add(
                    make_sagemaker_job_urn(
                        auto_ml_type, auto_ml_name, auto_ml_arn, self.env
                    )
                )

        if training_arn is not None:
            training_type, training_name = self.arn_to_name.get(
                training_arn, (None, None)
            )
            if training_type is not None and training_name is not None:
                input_jobs.add(
                    make_sagemaker_job_urn(
                        training_type, training_name, training_arn, self.env
                    )
                )

        input_datasets = {}

        inputs = job["ProcessingInputs"]

        for input_config in inputs:

            input_name = input_config["InputName"]

            input_s3 = input_config.get("S3Input", {})
            input_s3_uri = input_s3.get("S3Uri")

            if input_s3_uri is not None:

                input_datasets[make_s3_urn(input_s3_uri, self.env)] = {
                    "dataset_type": "s3",
                    "uri": input_s3_uri,
                    "datatype": input_s3.get("S3DataType"),
                    "mode": input_s3.get("S3InputMode"),
                    "distribution_type": input_s3.get("S3DataDistributionType"),
                    "compression": input_s3.get("S3CompressionType"),
                    "name": input_name,
                }

            # TODO: ingest Athena and Redshift data sources
            # We don't do this at the moment because we need to parse the QueryString SQL
            # in order to get the tables used (otherwise we just have databases)

            # input_athena = input_config.get("DatasetDefinition", {}).get(
            #     "AthenaDatasetDefinition", {}
            # )

            # input_redshift = input_config.get("DatasetDefinition", {}).get(
            #     "RedshiftDatasetDefinition", {}
            # )

        outputs: List[Dict[str, Any]] = job.get("ProcessingOutputConfig", {}).get(
            "Outputs", []
        )

        output_datasets = {}

        for output in outputs:
            output_name = output["OutputName"]

            output_s3_uri = output.get("S3Output", {}).get("S3Uri")
            if output_s3_uri is not None:
                output_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                    "dataset_type": "s3",
                    "uri": output_s3_uri,
                    "name": output_name,
                }

            output_feature_group = output.get("FeatureStoreOutput", {}).get(
                "FeatureGroupName"
            )
            if output_feature_group is not None:
                output_datasets[
                    mce_builder.make_ml_feature_table_urn(
                        "sagemaker", output_feature_group
                    )
                ] = {
                    "dataset_type": "sagemaker_feature_group",
                }

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            input_jobs=input_jobs,
        )

    def process_training_job(self, job: Dict[str, Any]) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_training_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
        """

        JOB_TYPE = "training"

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

        # process all output datasets at once
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

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
        )

    def process_transform_job(self, job: Dict[str, Any]) -> SageMakerJob:

        """
        Process outputs from Boto3 describe_transform_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
        """

        JOB_TYPE = "transform"

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
            output_datasets[make_s3_urn(output_s3_uri, self.env)] = {
                "dataset_type": "s3",
                "uri": output_s3_uri,
            }

        labeling_arn = job.get("LabelingJobArn")
        auto_ml_arn = job.get("AutoMLJobArn")

        input_jobs = set()

        if labeling_arn is not None:
            labeling_type, labeling_name = self.arn_to_name.get(
                labeling_arn, (None, None)
            )

            if labeling_type is not None and labeling_name is not None:
                input_jobs.add(
                    make_sagemaker_job_urn(
                        labeling_type, labeling_name, labeling_arn, self.env
                    )
                )

        if auto_ml_arn is not None:
            auto_ml_type, auto_ml_name = self.arn_to_name.get(auto_ml_arn, (None, None))

            if auto_ml_type is not None and auto_ml_name is not None:
                input_jobs.add(
                    make_sagemaker_job_urn(
                        auto_ml_type, auto_ml_name, auto_ml_arn, self.env
                    )
                )

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
        )

        return SageMakerJob(
            job_name=job_name,
            job_arn=job_arn,
            job_type=JOB_TYPE,
            job_snapshot=job_snapshot,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
            input_jobs=input_jobs,
        )
