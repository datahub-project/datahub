import dataclasses
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, List, Optional, TypeVar

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform
from google.cloud.aiplatform import (
    AutoMLForecastingTrainingJob,
    AutoMLImageTrainingJob,
    AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob,
    AutoMLVideoTrainingJob,
    Endpoint,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Model, VersionInfo
from google.oauth2 import service_account
from pydantic import PrivateAttr
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub._codegen.aspect import _Aspect
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ProjectIdKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLTrainingRunProperties,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    SubTypesClass,
    TimeStampClass,
    VersionTagClass,
)
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.time import datetime_to_ts_millis

T = TypeVar("T")

logger = logging.getLogger(__name__)


class VertexAIConfig(EnvConfigMixin):
    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )
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
    _credentials_path: Optional[str] = PrivateAttr(None)

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file(
                self.project_id
            )
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )


class MLTypes(StrEnum):
    TRAINING_JOB = "Training Job"
    MODEL = "ML Model"
    MODEL_GROUP = "ML Model Group"
    ENDPOINT = "Endpoint"
    DATASET = "Dataset"
    PROJECT = "Project"


@dataclasses.dataclass
class TrainingJobMetadata:
    job: VertexAiResourceNoun
    input_dataset: Optional[VertexAiResourceNoun] = None
    output_model: Optional[Model] = None
    output_model_version: Optional[VersionInfo] = None


@dataclasses.dataclass
class ModelMetadata:
    model: Model
    model_version: VersionInfo
    training_job_urn: Optional[str] = None
    endpoints: Optional[List[Endpoint]] = None


@platform_name("Vertex AI", id="vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for Vertex AI Registered Models and Model Versions",
)
@capability(SourceCapability.TAGS, "Extract tags for Vertex AI Registered Model Stages")
class VertexAISource(Source):
    platform: str = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        credentials = (
            service_account.Credentials.from_service_account_file(
                self.config._credentials_path
            )
            if self.config.credential
            else None
        )

        aiplatform.init(
            project=config.project_id, location=config.region, credentials=credentials
        )
        self.client = aiplatform
        self.endpoints: Optional[dict] = None
        self.datasets: Optional[dict] = None

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main Function to fetch and yields mcps for various VertexAI resources.
        - Models and Model Versions from the Model Registry
        - Training Jobs
        """

        # Ingest Project
        yield from self._gen_project_workunits()
        # Fetch and Ingest Models, Model Versions a from Model Registry
        yield from auto_workunit(self._get_ml_models_mcps())
        # Fetch and Ingest Training Jobs
        yield from auto_workunit(self._get_training_jobs_mcps())
        # TODO Fetch Experiments and Experiment Runs

    def _gen_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._get_project_container(),
            name=self.config.project_id,
            sub_types=[MLTypes.PROJECT],
        )

    def _get_ml_models_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Fetch List of Models in Model Registry and generate a corresponding mcp.
        """
        registered_models = self.client.Model.list()
        for model in registered_models:
            # create mcp for Model Group (= Model in VertexAI)
            yield from self._gen_ml_group_mcps(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create mcp for Model (= Model Version in VertexAI)
                logger.info(
                    f"Ingesting a model (name: {model.display_name} id:{model.name})"
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
                )

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataChangeProposalWrapper]:
        model_meta: ModelMetadata = self._get_ml_model_metadata(model, model_version)
        # Create ML Model Entity
        yield from self._gen_ml_model_mcps(model_meta)
        # Create Endpoint Entity
        yield from self._gen_endpoint_mcps(model_meta)

    def _get_ml_model_metadata(
        self, model: Model, model_version: VersionInfo
    ) -> ModelMetadata:
        model_meta = ModelMetadata(model=model, model_version=model_version)
        # Search for endpoints associated with the model
        endpoints = self._search_endpoint(model)
        model_meta.endpoints = endpoints
        return model_meta

    def _get_training_jobs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Fetches training jobs from Vertex AI and generates corresponding mcps.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates mcps containing metadata
        about the job, its inputs, and its outputs.
        """
        class_names = [
            "CustomJob",
            "CustomTrainingJob",
            "CustomContainerTrainingJob",
            "CustomPythonPackageTrainingJob",
            "AutoMLTabularTrainingJob",
            "AutoMLTextTrainingJob",
            "AutoMLImageTrainingJob",
            "AutoMLVideoTrainingJob",
            "AutoMLForecastingTrainingJob",
        ]
        # Iterate over class names and call the list() function
        for class_name in class_names:
            logger.info(f"Fetching a list of {class_name}s from VertexAI server")
            for job in getattr(self.client, class_name).list():
                yield from self._get_training_job_mcps(job)

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataChangeProposalWrapper]:
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job)
        # Create DataProcessInstance for the training job
        yield from self._gen_training_job_mcps(job_meta)
        # Create Dataset entity for Input Dataset of Training job
        yield from self._get_input_dataset_mcps(job_meta)
        # Create ML Model entity for output ML model of this training job
        yield from self._gen_output_model_mcps(job_meta)

    def _gen_output_model_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if job_meta.output_model and job_meta.output_model_version:
            job = job_meta.job
            job_urn = builder.make_data_process_instance_urn(
                self._make_vertexai_job_name(entity_id=job.name)
            )

            yield from self._gen_ml_model_mcps(
                ModelMetadata(
                    model=job_meta.output_model,
                    model_version=job_meta.output_model_version,
                    training_job_urn=job_urn,
                )
            )

    def _gen_training_job_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Generate a mcp for VertexAI Training Job
        """
        job = job_meta.job
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        created_time = (
            datetime_to_ts_millis(job.create_time)
            if job.create_time
            else datetime_to_ts_millis(datetime.now())
        )
        created_actor = f"urn:li:platformResource:{self.platform}"

        aspects: List[_Aspect] = list()
        aspects.append(
            DataProcessInstancePropertiesClass(
                name=job_id,
                created=AuditStampClass(
                    time=created_time,
                    actor=created_actor,
                ),
                externalUrl=self._make_job_external_url(job),
                customProperties={
                    "displayName": job.display_name,
                    "jobType": job.__class__.__name__,
                },
            )
        )
        aspects.append(
            MLTrainingRunProperties(
                externalUrl=self._make_job_external_url(job), id=job.name
            )
        )
        aspects.append(SubTypesClass(typeNames=[MLTypes.TRAINING_JOB]))
        aspects.append(ContainerClass(container=self._get_project_container().as_urn()))

        # If Training job has Input Dataset
        if job_meta.input_dataset:
            dataset_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=self._make_vertexai_dataset_name(
                    entity_id=job_meta.input_dataset.name
                ),
                env=self.config.env,
            )
            aspects.append(
                DataProcessInstanceInputClass(inputs=[dataset_urn]),
            )

        yield from MetadataChangeProposalWrapper.construct_many(
            job_urn, aspects=aspects
        )

    def _gen_ml_group_mcps(
        self,
        model: Model,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Generate an MLModelGroup mcp for a VertexAI  Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)

        aspects: List[_Aspect] = list()
        aspects.append(
            MLModelGroupPropertiesClass(
                name=self._make_vertexai_model_group_name(model.name),
                description=model.description,
                created=TimeStampClass(time=datetime_to_ts_millis(model.create_time))
                if model.create_time
                else None,
                lastModified=TimeStampClass(
                    time=datetime_to_ts_millis(model.update_time)
                )
                if model.update_time
                else None,
                customProperties={"displayName": model.display_name},
            )
        )

        # TODO add following when metadata model for mlgroup is updated (these aspects not supported currently)
        # aspects.append(SubTypesClass(typeNames=[MLTypes.MODEL_GROUP]))
        # aspects.append(ContainerClass(container=self._get_project_container().as_urn()))

        yield from MetadataChangeProposalWrapper.construct_many(
            ml_model_group_urn, aspects=aspects
        )

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._make_vertexai_model_name(model.name),
            env=self.config.env,
        )
        return urn

    def _get_project_container(self) -> ProjectIdKey:
        return ProjectIdKey(project_id=self.config.project_id, platform=self.platform)

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        return (
            isinstance(job, AutoMLTabularTrainingJob)
            or isinstance(job, AutoMLTextTrainingJob)
            or isinstance(job, AutoMLImageTrainingJob)
            or isinstance(job, AutoMLVideoTrainingJob)
            or isinstance(job, AutoMLForecastingTrainingJob)
        )

    def _search_model_version(
        self, model: Model, version_id: str
    ) -> Optional[VersionInfo]:
        for version in model.versioning_registry.list_versions():
            if version.version_id == version_id:
                return version
        return None

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
        """
        Search for a dataset by its ID in Vertex AI.
        This method iterates through different types of datasets (Text, Tabular, Image,
        TimeSeries, and Video) to find a dataset that matches the given dataset ID.
        """

        dataset_types = [
            "TextDataset",
            "TabularDataset",
            "ImageDataset",
            "TimeSeriesDataset",
            "VideoDataset",
        ]

        if self.datasets is None:
            self.datasets = dict()

            for dtype in dataset_types:
                dataset_class = getattr(self.client.datasets, dtype)
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return self.datasets.get(dataset_id) if dataset_id in self.datasets else None

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """
        ds = job_meta.input_dataset

        if ds:
            # Create URN of Input Dataset for Training Job
            dataset_name = self._make_vertexai_dataset_name(entity_id=ds.name)
            dataset_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=dataset_name,
                env=self.config.env,
            )

            # Create aspects for the dataset
            aspects: List[_Aspect] = list()
            aspects.append(
                DatasetPropertiesClass(
                    name=self._make_vertexai_dataset_name(ds.name),
                    created=TimeStampClass(time=datetime_to_ts_millis(ds.create_time))
                    if ds.create_time
                    else None,
                    description=f"Dataset: {ds.display_name}",
                    customProperties={
                        "displayName": ds.display_name,
                        "resourceName": ds.resource_name,
                    },
                    qualifiedName=ds.resource_name,
                )
            )

            aspects.append(SubTypesClass(typeNames=[MLTypes.DATASET]))
            # Create a container for Project as parent of the dataset
            aspects.append(
                ContainerClass(container=self._get_project_container().as_urn())
            )
            yield from MetadataChangeProposalWrapper.construct_many(
                dataset_urn, aspects=aspects
            )

    def _get_training_job_metadata(
        self, job: VertexAiResourceNoun
    ) -> TrainingJobMetadata:
        """
        Retrieve metadata for a given Vertex AI training job.
        This method extracts metadata for a Vertex AI training job, including input datasets
        and output models. It checks if the job is an AutoML job and retrieves the relevant
        input dataset and output model information.
        """

        job_meta = TrainingJobMetadata(job=job)

        # Check if the job is an AutoML job
        if self._is_automl_job(job):
            job_conf = job.to_dict()

            # Check if input dataset is present in the job configuration
            if (
                "inputDataConfig" in job_conf
                and "datasetId" in job_conf["inputDataConfig"]
            ):
                # Create URN of Input Dataset for Training Job
                dataset_id = job_conf["inputDataConfig"]["datasetId"]
                logger.info(
                    f"Found input dataset (id: {dataset_id}) for training job ({job.display_name})"
                )

                if dataset_id:
                    input_ds = self._search_dataset(dataset_id)
                    if input_ds:
                        logger.info(
                            f"Found the name of input dataset ({input_ds.display_name}) with dataset id ({dataset_id})"
                        )
                    job_meta.input_dataset = input_ds

            # Check if output model is present in the job configuration
            if (
                "modelToUpload" in job_conf
                and "name" in job_conf["modelToUpload"]
                and job_conf["modelToUpload"]["name"]
                and job_conf["modelToUpload"]["versionId"]
            ):
                model_version_str = job_conf["modelToUpload"]["versionId"]
                try:
                    model = Model(model_name=job_conf["modelToUpload"]["name"])
                    model_version = self._search_model_version(model, model_version_str)
                    if model and model_version:
                        logger.info(
                            f"Found output model (name:{model.display_name} id:{model_version_str}) "
                            f"for training job: {job.display_name}"
                        )
                        job_meta.output_model = model
                        job_meta.output_model_version = model_version
                except GoogleAPICallError:
                    logger.error(
                        f"Error while fetching model version {model_version_str}"
                    )

        return job_meta

    def _gen_endpoint_mcps(
        self, model_meta: ModelMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        model: Model = model_meta.model
        model_version: VersionInfo = model_meta.model_version

        if model_meta.endpoints:
            for endpoint in model_meta.endpoints:
                endpoint_urn = builder.make_ml_model_deployment_urn(
                    platform=self.platform,
                    deployment_name=self._make_vertexai_endpoint_name(
                        entity_id=endpoint.display_name
                    ),
                    env=self.config.env,
                )

                aspects: List[_Aspect] = list()
                aspects.append(
                    MLModelDeploymentPropertiesClass(
                        description=model.description,
                        createdAt=datetime_to_ts_millis(endpoint.create_time),
                        version=VersionTagClass(
                            versionTag=str(model_version.version_id)
                        ),
                        customProperties={"displayName": endpoint.display_name},
                    )
                )

                # TODO add followings when metadata for MLModelDeployment is updated (these aspects not supported currently)
                # aspects.append(
                #     ContainerClass(container=self._get_project_container().as_urn())
                # )
                # aspects.append(SubTypesClass(typeNames=[MLTypes.ENDPOINT]))

                yield from MetadataChangeProposalWrapper.construct_many(
                    endpoint_urn, aspects=aspects
                )

    def _gen_ml_model_mcps(
        self, ModelMetadata: ModelMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Generate an MLModel and Endpoint mcp for an VertexAI Model Version.
        """

        model: Model = ModelMetadata.model
        model_version: VersionInfo = ModelMetadata.model_version
        training_job_urn: Optional[str] = ModelMetadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = ModelMetadata.endpoints
        endpoint_urns: List[str] = list()

        logging.info(f"generating model mcp for {model.name}")

        # Generate list of endpoint URL
        if endpoints:
            for endpoint in endpoints:
                logger.info(
                    f"found endpoint ({endpoint.display_name}) for model ({model.resource_name})"
                )
                endpoint_urns.append(
                    builder.make_ml_model_deployment_urn(
                        platform=self.platform,
                        deployment_name=self._make_vertexai_endpoint_name(
                            entity_id=endpoint.display_name
                        ),
                        env=self.config.env,
                    )
                )

        # Create URN for Model and Model Version
        model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_model_name(entity_id=model.name)
        model_version_name = f"{model_name}_{model_version.version_id}"
        model_urn = self._make_ml_model_urn(model_version, model_name=model_name)

        # Create aspects for ML Model
        aspects: List[_Aspect] = list()

        aspects.append(
            MLModelPropertiesClass(
                name=model_version_name,
                description=model_version.version_description,
                customProperties={
                    "displayName": f"{model_version.model_display_name}_{model_version.version_id}",
                    "resourceName": model.resource_name,
                },
                created=TimeStampClass(
                    datetime_to_ts_millis(model_version.version_create_time)
                )
                if model_version.version_create_time
                else None,
                lastModified=TimeStampClass(
                    datetime_to_ts_millis(model_version.version_update_time)
                )
                if model_version.version_update_time
                else None,
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                groups=[model_group_urn],  # link model version to model group
                trainingJobs=[training_job_urn]
                if training_job_urn
                else None,  # link to training job
                deployments=endpoint_urns,
                externalUrl=self._make_model_version_external_url(model),
                type="ML Model",
            )
        )

        # TODO Add a container for Project as parent of the dataset
        # aspects.append(
        #     ContainerClass(
        #             container=self._get_project_container().as_urn(),
        #     )
        # )

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=model_urn, aspects=aspects
        )

    def _search_endpoint(self, model: Model) -> List[Endpoint]:
        """
        Search for an endpoint associated with the model.
        """
        if self.endpoints is None:
            endpoint_dict = defaultdict(list)
            for endpoint in self.client.Endpoint.list():
                for resource in endpoint.list_models():
                    endpoint_dict[resource.model].append(endpoint)
            self.endpoints = endpoint_dict

        endpoints = self.endpoints[model.resource_name]
        return endpoints

    def _make_ml_model_urn(self, model_version: VersionInfo, model_name: str) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_name}_{model_version.version_id}",
            env=self.config.env,
        )
        return urn

    def _make_job_urn(self, job: VertexAiResourceNoun) -> str:
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        urn = builder.make_data_process_instance_urn(dataProcessInstanceId=job_id)
        return urn

    def _make_vertexai_model_group_name(
        self,
        entity_id: str,
    ) -> str:
        separator: str = "."
        return f"{self.config.project_id}{separator}model_group{separator}{entity_id}"

    def _make_vertexai_endpoint_name(self, entity_id: str) -> str:
        separator: str = "."
        return f"{self.config.project_id}{separator}endpoint{separator}{entity_id}"

    def _make_vertexai_model_name(self, entity_id: str) -> str:
        separator: str = "."
        return f"{self.config.project_id}{separator}model{separator}{entity_id}"

    def _make_vertexai_dataset_name(self, entity_id: str) -> str:
        separator: str = "."
        return f"{self.config.project_id}{separator}dataset{separator}{entity_id}"

    def _make_vertexai_job_name(
        self,
        entity_id: Optional[str],
    ) -> str:
        separator: str = "."
        return f"{self.config.project_id}{separator}job{separator}{entity_id}"

    def _make_job_external_url(self, job: VertexAiResourceNoun) -> str:
        """
        Model external URL in Vertex AI
        Sample URLs:
        https://console.cloud.google.com/vertex-ai/training/training-pipelines?project=acryl-poc&trainingPipelineId=5401695018589093888
        """
        external_url: str = (
            f"{self.config.vertexai_url}/training/training-pipelines?trainingPipelineId={job.name}"
            f"?project={self.config.project_id}"
        )
        return external_url

    def _make_model_external_url(self, model: Model) -> str:
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"?project={self.config.project_id}"
        )
        return external_url

    def _make_model_version_external_url(self, model: Model) -> str:
        """
        Model Version external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"/versions/{model.version_id}"
            f"?project={self.config.project_id}"
        )
        return external_url
