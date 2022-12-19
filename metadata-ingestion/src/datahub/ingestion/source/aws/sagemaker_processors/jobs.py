from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from datahub.emitter import mce_builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.aws.sagemaker_processors.common import (
    SagemakerSourceReport,
)
from datahub.ingestion.source.aws.sagemaker_processors.job_classes import (
    AutoMlJobInfo,
    CompilationJobInfo,
    EdgePackagingJobInfo,
    HyperParameterTuningJobInfo,
    LabelingJobInfo,
    ProcessingJobInfo,
    TrainingJobInfo,
    TransformJobInfo,
)
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

if TYPE_CHECKING:
    from mypy_boto3_sagemaker import SageMakerClient

JobInfo = TypeVar(
    "JobInfo",
    AutoMlJobInfo,
    CompilationJobInfo,
    EdgePackagingJobInfo,
    HyperParameterTuningJobInfo,
    LabelingJobInfo,
    ProcessingJobInfo,
    TrainingJobInfo,
    TransformJobInfo,
)


class JobType(Enum):
    AUTO_ML = "auto_ml"
    COMPILATION = "compilation"
    EDGE_PACKAGING = "edge_packaging"
    HYPER_PARAMETER_TUNING = "hyper_parameter_tuning"
    LABELING = "labeling"
    PROCESSING = "processing"
    TRAINING = "training"
    TRANSFORM = "transform"


job_types = sorted([x for x in JobType], key=lambda x: x.value)

job_type_to_info: Mapping[JobType, Any] = {
    JobType.AUTO_ML: AutoMlJobInfo(),
    JobType.COMPILATION: CompilationJobInfo(),
    JobType.EDGE_PACKAGING: EdgePackagingJobInfo(),
    JobType.HYPER_PARAMETER_TUNING: HyperParameterTuningJobInfo(),
    JobType.LABELING: LabelingJobInfo(),
    JobType.PROCESSING: ProcessingJobInfo(),
    JobType.TRAINING: TrainingJobInfo(),
    JobType.TRANSFORM: TransformJobInfo(),
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
    job_type: JobType
    input_datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    output_datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    input_jobs: Set[str] = field(default_factory=set)
    # we resolve output jobs to input ones after processing
    output_jobs: Set[str] = field(default_factory=set)


class JobDirection(Enum):
    TRAINING = "training"
    DOWNSTREAM = "downstream"


class JobKey(NamedTuple):
    job_urn: str
    job_direction: JobDirection


@dataclass()
class ModelJob:
    """
    Intermediate representation of a job's related models. Subsequently used by the SageMaker jobs ingestion framework.
    """

    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def update(self, hyperparameters: Dict[str, Any], metrics: Dict[str, Any]) -> None:
        self.hyperparameters.update(hyperparameters)
        self.metrics.update(metrics)


@dataclass
class JobProcessor:
    """
    Job ingestion module, called by top-level SageMaker ingestion handler.
    """

    # boto3 SageMaker client
    sagemaker_client: "SageMakerClient"
    env: str
    report: SagemakerSourceReport
    # config filter for specific job types to ingest (see metadata-ingestion README)
    job_type_filter: Union[Dict[str, str], bool, None]
    aws_region: str

    # translators between ARNs and job names (represented as tuples of (job_type, job_name))
    arn_to_name: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    name_to_arn: Dict[Tuple[str, str], str] = field(default_factory=dict)

    # map from model image file path to jobs referencing the model
    model_image_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    # map from model name to jobs referencing the model
    model_name_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    def get_jobs(self, job_type: JobType, job_spec: JobInfo) -> List[Any]:
        jobs = []

        paginator = self.sagemaker_client.get_paginator(job_spec.list_command)
        for page in paginator.paginate():
            page_jobs: List[Any] = page[job_spec.list_key]

            for job in page_jobs:
                job_name = (
                    job_type.value,
                    job[job_spec.list_name_key],
                )
                job_arn = job[job_spec.list_arn_key]

                self.arn_to_name[job_arn] = job_name
                self.name_to_arn[job_name] = job_arn

                job["type"] = job_type

            jobs += page_jobs

        return jobs

    def update_model_image_jobs(
        self,
        model_data_url: str,
        job_key: JobKey,
        metrics: Dict[str, Any] = {},
        hyperparameters: Dict[str, Any] = {},
    ) -> None:
        model_jobs = self.model_image_to_jobs[model_data_url]

        # if model doesn't have job yet, init
        if job_key in model_jobs:
            model_jobs[job_key].update(hyperparameters, metrics)

        else:
            model_jobs[job_key] = ModelJob(hyperparameters, metrics)

    def update_model_name_jobs(
        self,
        model_name: str,
        job_key: JobKey,
        metrics: Dict[str, Any] = {},
        hyperparameters: Dict[str, Any] = {},
    ) -> None:
        model_jobs = self.model_name_to_jobs[model_name]

        # if model doesn't have job yet, init
        if job_key in model_jobs:
            model_jobs[job_key].update(hyperparameters, metrics)

        else:
            model_jobs[job_key] = ModelJob(hyperparameters, metrics)

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
            allowed_jobs = job_types
        elif isinstance(self.job_type_filter, dict):
            allowed_jobs = [
                job_type
                for job_type in job_types
                if self.job_type_filter.get(job_type.value, True) is True
            ]

        # iterate through keys in sorted order for consistency
        for job_type in allowed_jobs:
            job_spec = job_type_to_info[job_type]

            job_type_jobs = self.get_jobs(job_type, job_spec)

            jobs += job_type_jobs

        return jobs

    def get_job_details(self, job_name: str, job_type: JobType) -> Dict[str, Any]:
        """
        Get boto3 describe_<job> response
        """

        describe_command = job_type_to_info[job_type].describe_command
        describe_name_key = job_type_to_info[job_type].describe_name_key

        return getattr(self.sagemaker_client, describe_command)(
            **{describe_name_key: job_name}
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        jobs = self.get_all_jobs()

        processed_jobs: Dict[str, SageMakerJob] = {}

        # first pass: process jobs and collect datasets used
        for job in jobs:
            job_type = job_type_to_info[job["type"]]
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
                processed_job.job_type.value, processed_job.job_name, self.env
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
        job_type: JobType,
        job_url: Optional[str] = None,
    ) -> Tuple[DataJobSnapshotClass, str, str]:
        """
        General function for generating a job snapshot.
        """

        job_type_info = job_type_to_info[job_type]

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

        job_urn = make_sagemaker_job_urn(job_type.value, name, arn, self.env)
        job_snapshot = DataJobSnapshotClass(
            urn=job_urn,
            aspects=[
                DataJobInfoClass(
                    name=name,
                    type="SAGEMAKER",
                    status=mapped_status,
                    externalUrl=job_url,
                    customProperties={
                        **{key: str(value) for key, value in job.items()},
                        "jobType": job_type.value,
                    },
                ),
                BrowsePathsClass(paths=[f"/{job_type.value}/{name}"]),
            ],
        )

        return job_snapshot, name, arn

    def process_auto_ml_job(self, job: Dict[str, Any]) -> SageMakerJob:
        """
        Process outputs from Boto3 describe_auto_ml_job()

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        """

        JOB_TYPE = JobType.AUTO_ML

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

        model_containers = job.get("BestCandidate", {}).get("InferenceContainers", [])

        for model_container in model_containers:
            model_data_url = model_container.get("ModelDataUrl")

            if model_data_url is not None:
                job_key = JobKey(job_snapshot.urn, JobDirection.TRAINING)

                self.update_model_image_jobs(model_data_url, job_key)

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

        JOB_TYPE = JobType.COMPILATION

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/compilation-jobs/{job['CompilationJobName']}",
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

        JOB_TYPE = JobType.EDGE_PACKAGING

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

        job_snapshot, job_name, job_arn = self.create_common_job_snapshot(
            job,
            JOB_TYPE,
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/edge-packaging-jobs/{job['EdgePackagingJobName']}",
        )

        if job.get("ModelName") is not None:
            job_key = JobKey(job_snapshot.urn, JobDirection.DOWNSTREAM)

            self.update_model_name_jobs(job["ModelName"], job_key)

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

        JOB_TYPE = JobType.HYPER_PARAMETER_TUNING

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/hyper-tuning-jobs/{job['HyperParameterTuningJobName']}",
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

        JOB_TYPE = JobType.LABELING

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/labeling-jobs/{job['LabelingJobName']}",
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

        JOB_TYPE = JobType.PROCESSING

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

        inputs = job.get("ProcessingInputs", [])

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/processing-jobs/{job['ProcessingJobName']}",
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

        JOB_TYPE = JobType.TRAINING

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/jobs/{job['TrainingJobName']}",
        )

        model_data_url = job.get("ModelArtifacts", {}).get("S3ModelArtifacts")

        job_metrics = job.get("FinalMetricDataList", [])
        # sort first by metric name, then from latest -> earliest
        sorted_metrics = sorted(
            job_metrics, key=lambda x: (x["MetricName"], x["Timestamp"]), reverse=True
        )
        # extract the last recorded metric values
        latest_metrics = []
        seen_keys = set()
        for metric in sorted_metrics:
            if metric["MetricName"] not in seen_keys:
                latest_metrics.append(metric)
                seen_keys.add(metric["MetricName"])

        metrics = dict(
            zip(
                [metric["MetricName"] for metric in latest_metrics],
                [metric["Value"] for metric in latest_metrics],
            )
        )

        if model_data_url is not None:
            job_key = JobKey(job_snapshot.urn, JobDirection.TRAINING)

            self.update_model_image_jobs(
                model_data_url,
                job_key,
                metrics,
                job.get("HyperParameters", {}),
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

        JOB_TYPE = JobType.TRANSFORM

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
            f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/transform-jobs/{job['TransformJobName']}",
        )

        if job.get("ModelName") is not None:
            job_key = JobKey(job_snapshot.urn, JobDirection.DOWNSTREAM)

            self.update_model_name_jobs(
                job["ModelName"],
                job_key,
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
