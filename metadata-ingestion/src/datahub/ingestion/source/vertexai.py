import logging
import time
from typing import Iterable, List, Optional, TypeVar

from google.cloud import aiplatform
from google.cloud.aiplatform import (
    AutoMLForecastingTrainingJob,
    AutoMLImageTrainingJob,
    AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob,
    AutoMLVideoTrainingJob,
    Endpoint,
    TabularDataset,
)
from google.cloud.aiplatform.datasets import _Dataset
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
    AuditStampClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    TimeStampClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    VersionTagClass,
    _Aspect,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)

class VertexAIConfig(EnvConfigMixin):
    project_id: str = Field(description=("Project ID in Google Cloud Platform"))
    region: str = Field(
        description=("Region of your project in Google Cloud Platform"),
    )
    bucket_uri: Optional[str] = Field(
        default=None,
        description=("Bucket URI used in your project"),
    )

    vertexai_url: Optional[str] = Field(
        default="https://console.cloud.google.com/vertex-ai",
        description=("VertexUI URI"),
    )

    model_name_separator: str = Field(
        default="_",
        description="A string which separates model name from its version (e.g. model_1 or model-1)",
    )


@platform_name("vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for vertexai Registered Models and Model Versions",
)
@capability(SourceCapability.TAGS, "Extract tags for VertexAI Registered Model Stages")
class VertexAISource(Source):
    platform = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        aiplatform.init(project=config.project_id, location=config.region)
        self.client = aiplatform
        self.endpoints = None
        self.datasets = None

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main Function to fetch and yields work units for various VertexAI resources.
        - Models and Model Versions from the Model Registry
        - Training Jobs
        """
        # Fetch Models, Model Versions a from Model Registry
        yield from self._get_ml_model_workunits()
        # Fetch Training Jobs
        yield from self._get_training_job_workunit()
        # TODO Fetch Experiments and Experiment Runs

    def _create_workunit(self, urn: str, aspect: _Aspect) -> MetadataWorkUnit:
        """
        Utility to create an MCP workunit.
        """
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        ).as_workunit()

    def _validate_training_job(self, model: Model) -> bool:
        """
        Validate Model Has Valid Training Job
        """
        job = model.training_job
        if not job:
            return False

        try:
            # when model has ref to training job, but field is not accessible, it is not valid
            name = job.name
            logger.debug((f"can fetch training job name: {name} for model: (name:{model.display_name} id:{model.name})"))
            return True
        except RuntimeError:
            logger.debug(f"cannot fetch training job name, not valid for model (name:{model.display_name} id:{model.name})")

        return False

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch List of Models in Model Registry and generate a corresponding work unit.
        """
        registered_models = self.client.Model.list()
        for model in registered_models:
            # create work unit for Model Group (= Model in VertexAI)
            yield self._get_ml_group_workunit(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:

                # create work unit for Training Job (if Model has reference to Training Job)
                if self._validate_training_job(model):
                    logger.info(
                        f"Generating TrainingJob work unit for model: {model_version.model_display_name}")
                    yield from self._get_data_process_properties_workunit(model.training_job)

                # create work unit for Model (= Model Version in VertexAI)
                logger.info(f"Generating work unit for model (name: {model.display_name} id:{model.name})")
                yield from self._get_ml_model_endpoint_workunit(model=model, model_version=model_version)


    def _get_training_job_workunit(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetches training jobs from Vertex AI and generates corresponding work units.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates work units containing metadata
        about the job, its inputs, and its outputs.
        """
        logger.info("Fetching a list of CustomJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.CustomJob.list())
        logger.info("Fetching a list of CustomTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.CustomTrainingJob.list())
        logger.info("Fetching a list of CustomContainerTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.CustomContainerTrainingJob.list())
        logger.info("Fetching a list of CustomPythonPackageTrainingJob from VertexAI server")
        yield from self._get_data_process_workunit(self.client.CustomPythonPackageTrainingJob.list())
        logger.info("Fetching a list of AutoMLTabularTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.AutoMLTabularTrainingJob.list())
        logger.info("Fetching a list of AutoMLTextTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.AutoMLTextTrainingJob.list())
        logger.info("Fetching a list of AutoMLImageTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.AutoMLImageTrainingJob.list())
        logger.info("Fetching a list of AutoMLVideoTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.AutoMLVideoTrainingJob.list())
        logger.info("Fetching a list of AutoMLForecastingTrainingJobs from VertexAI server")
        yield from self._get_data_process_workunit(self.client.AutoMLForecastingTrainingJob.list())

    def _get_data_process_workunit(self, jobs: List[_TrainingJob]) -> Iterable[MetadataWorkUnit]:
        for job in jobs:
            yield self._get_data_process_properties_workunit(job)
            yield from self._get_job_output_workunit(job)
            yield from self._get_job_input_workunit(job)

    def _get_ml_group_workunit(
        self,
        model: Model,
    ) -> MetadataWorkUnit:
        """
        Generate an MLModelGroup work unit for a VertexAI  Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)
        ml_model_group_properties = MLModelGroupPropertiesClass(
            name=self._make_vertexai_name("model_group", model.name),
            description=model.description,
            createdAt=int(model.create_time.timestamp()),
            customProperties={"displayName": model.display_name}
        )
        wu = self._create_workunit(
            urn=ml_model_group_urn,
            aspect=ml_model_group_properties,
        )
        return wu

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._make_vertexai_name("model",model.name),
            env=self.config.env,
        )
        return urn

    def _get_data_process_properties_workunit(self, job: _TrainingJob) -> MetadataWorkUnit:
        """
        Generate a work unit for VertexAI Training Job
        """
        created_time = int(job.start_time.timestamp()) or int(time.time() * 1000)
        created_actor = f"urn:li:platformResource:{self.platform}"

        job_id = self._make_vertexai_name(entity_type="job", entity_id=job.name)
        entityUrn = builder.make_data_process_instance_urn(job_id)
        aspect = DataProcessInstancePropertiesClass(
                name=job_id,
                created=AuditStampClass(
                    time=created_time,
                    actor=created_actor,
                ),
                externalUrl=self._make_job_external_url(job),
                customProperties={"displayName": job.display_name}
            )

        logging.info(f"Generating data process instance for training job: {entityUrn}")
        return self._create_workunit(urn=entityUrn, aspect=aspect)

    def _is_automl_job(self, job: _TrainingJob) -> bool:
        return ((isinstance(job, AutoMLTabularTrainingJob) or
                isinstance(job, AutoMLTextTrainingJob) or
                isinstance(job, AutoMLImageTrainingJob) or
                isinstance(job, AutoMLVideoTrainingJob)) or
                isinstance(job, AutoMLForecastingTrainingJob))

    def _search_model_version(self, model:Model, version_id:str) -> Optional[VersionInfo]:
        for version in model.versioning_registry.list_versions():
            if version.version_id == version_id:
                return version
        return None


    def _get_job_output_workunit(self, job: _TrainingJob) -> Iterable[MetadataWorkUnit]:
        """
        This method creates work units that link the training job to the model version
        that it produces. It checks if the job configuration contains a model to upload,
        and if so, it generates a work unit for the model version with the training job
        as part of its properties.
        """

        job_conf = job.to_dict()
        if "modelToUpload" in job_conf and "name" in job_conf["modelToUpload"] and job_conf["modelToUpload"]["name"]:

            model_version_str = job_conf["modelToUpload"]["versionId"]
            job_urn = self._make_job_urn(job)

            model = Model(model_name=job_conf["modelToUpload"]["name"])
            model_version = self._search_model_version(model, model_version_str)
            if model and model_version:
                logger.info(
                    f"found that training job: {job.display_name} generated "
                    f"a model (name:{model.display_name} id:{model_version_str})")
                yield from self._get_ml_model_endpoint_workunit(model, model_version, job_urn)


    def _search_dataset(self, dataset_id: str) -> Optional[_Dataset]:
        """
        Search for a dataset by its ID in Vertex AI.
        This method iterates through different types of datasets (Text, Tabular, Image,
        TimeSeries, and Video) to find a dataset that matches the given dataset ID.
        """

        if self.datasets is None:
            self.datasets = []
            self.datasets.extend(self.client.datasets.TextDataset.list())
            self.datasets.extend(self.client.datasets.TabularDataset.list())
            self.datasets.extend(self.client.datasets.ImageDataset.list())
            self.datasets.extend(self.client.datasets.TimeSeriesDataset.list())
            self.datasets.extend(self.client.datasets.VideoDataset.list())

        for dataset in self.datasets:
            if dataset.name == dataset_id:
                return dataset

        return None

    def _make_dataset_aspect(self, ds: _Dataset) -> Optional[DatasetPropertiesClass]:
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """
        aspect = DatasetPropertiesClass(
            name=self._make_vertexai_name("dataset", ds.name),
            created=TimeStampClass(time=int(ds.create_time.timestamp())),
            description=f"Dataset: {ds.display_name} for training job",
            customProperties={"displayName": ds.display_name,
                              "resourceName": ds.resource_name,
                              },
            qualifiedName=ds.resource_name
        )
        return aspect



    def _get_job_input_workunit(self, job: _TrainingJob) -> Iterable[MetadataWorkUnit]:
        """
         Generate work units for the input data of a training job.
         This method checks if the training job is an AutoML job and if it has an input dataset
         configuration. If so, it creates a work unit for the input dataset.
         """

        if self._is_automl_job(job):
            job_conf = job.to_dict()
            if "inputDataConfig" in job_conf and "datasetId" in job_conf["inputDataConfig"]:
                # Create URN of Input Dataset for Training Job
                dataset_id = job_conf["inputDataConfig"]["datasetId"]
                logger.info(f"found that training job {job.display_name} used input dataset: {dataset_id}")

                if dataset_id:
                    yield from self._get_data_process_input_workunit(job, dataset_id)

    def _get_data_process_input_workunit(self, job: _TrainingJob, dataset_id: str) -> Iterable[MetadataWorkUnit]:
        """
        This method creates a work unit for the input dataset of a training job. It constructs the URN
        for the input dataset and the training job, and then creates a DataProcessInstanceInputClass aspect
        to link the input dataset to the training job.
        """

        # Create URN of Input Dataset for Training Job
        dataset_name = self._make_vertexai_name(entity_type="dataset", entity_id=dataset_id)
        dataset_urn = builder.make_dataset_urn(
            platform=self.platform,
            name=dataset_name,
            env=self.config.env,
        )

        dataset = self._search_dataset(dataset_id)
        if dataset:
            aspect = self._make_dataset_aspect(dataset)
            if aspect:
                yield self._create_workunit(urn=dataset_urn, aspect=aspect)

        # Create URN of Training Job
        job_id = self._make_vertexai_name(entity_type="job", entity_id=job.name)
        entityUrn = builder.make_data_process_instance_urn(job_id)
        dp_aspect = DataProcessInstanceInputClass(
            inputs=[dataset_urn]
        )
        logger.info(f"generating input dataset {dataset_name}")
        yield self._create_workunit(urn=entityUrn, aspect=dp_aspect)

    def _get_ml_model_endpoint_workunit(self, model: Model, model_version: VersionInfo,
                                          training_job_urn: Optional[str] = None) -> Iterable[MetadataWorkUnit]:

        """
         Generate an MLModel and Endpoint work unit for an VertexAI Model Version.
         """

        endpoint: Optional[Endpoint] = self._search_endpoint(model)
        endpoint_urn = None

        if endpoint:
            endpoint_urn = builder.make_ml_model_deployment_urn(
                    platform=self.platform,
                    deployment_name=self._make_vertexai_name("endpoint", endpoint.display_name),
                    env=self.config.env
                ) if endpoint else None
            ml_deployment_properties = MLModelDeploymentPropertiesClass(
                description=model.description,
                createdAt=int(endpoint.create_time.timestamp()),
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                customProperties={"displayName": endpoint.display_name}
            )
            yield self._create_workunit(urn=endpoint_urn, aspect=ml_deployment_properties)

        yield self._get_ml_model_properties_workunit(model, model_version,training_job_urn, endpoint_urn)

    def _get_ml_model_properties_workunit(self, model: Model, model_version: VersionInfo,
                                          training_job_urn:Optional[str] = None, endpoint_urn:Optional[str] = None) -> MetadataWorkUnit:
        """
        Generate an MLModel workunit for an VertexAI Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup
        corresponding to a registered Model in VertexAI Model Registry.
        """
        logging.info(f"starting model work unit for model {model.name}")

        ml_model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_name(entity_type="model", entity_id=model.name)
        model_version_name = f"{model_name}{self.config.model_name_separator}{model_version.version_id}"
        ml_model_urn = self._make_ml_model_urn(model_version, model_name=model_name)


        ml_model_properties = MLModelPropertiesClass(
            name=model_version_name,
            description=model_version.version_description,
            customProperties={"displayName": model_version.model_display_name +
                                             self.config.model_name_separator + model_version.version_id,
                              "resourceName": model.resource_name},
            created=TimeStampClass(model_version.version_create_time.second),
            lastModified=TimeStampClass(model_version.version_update_time.second),
            version=VersionTagClass(versionTag=str(model_version.version_id)),
            groups=[ml_model_group_urn], # link model version to model group
            trainingJobs=[training_job_urn] if training_job_urn else None, # link to training job
            deployments=[endpoint_urn] if endpoint_urn else [], # link to model registry and endpoint
            externalUrl=self._make_model_version_external_url(model),
        )

        # logging.info(f"created model version {ml_model_properties.name} associated with group {ml_model_group_urn}")
        return self._create_workunit(urn=ml_model_urn, aspect=ml_model_properties)



    def _search_endpoint(self, model: Model) -> Optional[Endpoint]:
        """
        Search for an endpoint associated with the model.
        """

        if self.endpoints is None:
            self.endpoints = self.client.Endpoint.list()
        for endpoint in self.endpoints:
            deployed_models = endpoint.list_models()
            if model.resource_name in deployed_models:
                return endpoint

        return None


    def _make_ml_model_urn(self, model_version: VersionInfo, model_name:str) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_name}{self.config.model_name_separator}{model_version.version_id}",
            env=self.config.env,
        )
        return urn

    def _make_job_urn(self, job: _TrainingJob) -> str:
        job_id = self._make_vertexai_name(entity_type="job", entity_id=job.name)
        urn = builder.make_data_process_instance_urn(
            dataProcessInstanceId=job_id
        )
        return urn


    def _make_vertexai_name(self,
                            entity_type:str,
                            entity_id:str,
                            separator:str=".") -> str:
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"


    def _make_job_external_url(self, job: _TrainingJob):
        """
        Model external URL in Vertex AI
        Sample URLs:
        https://console.cloud.google.com/vertex-ai/training/training-pipelines?project=acryl-poc&trainingPipelineId=5401695018589093888
        """
        entity_type = "training"
        external_url = (f"{self.config.vertexai_url}/{entity_type}/training-pipelines?trainingPipelineId={job.name}"
                        f"?project={self.config.project_id}")
        return external_url

    def _make_model_external_url(self, model: Model):
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=acryl-poc
        """
        entity_type = "models"
        external_url = (f"{self.config.vertexai_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
                        f"?project={self.config.project_id}")
        return external_url

    def _make_model_version_external_url(self, model: Model):
        """
        Model Version external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=acryl-poc
        """
        entity_type = "models"
        external_url = (f"{self.config.vertexai_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
                        f"/versions/{model.version_id}"
                        f"?project={self.config.project_id}")
        return external_url

