import logging
import time
from typing import Any, Iterable, List, Optional, TypeVar

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
from datahub.ingestion.source.common.credentials import GCPCredential
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
    # Generic SubTypes
    TRAINING_JOB = "Training Job"
    MODEL = "ML Model"
    MODEL_GROUP = "ML Model Group"
    ENDPOINT = "Endpoint"
    DATASET = "Dataset"


@platform_name("Vertex AI", id="vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for vertexai Registered Models and Model Versions",
)
@capability(SourceCapability.TAGS, "Extract tags for VertexAI Registered Model Stages")
class VertexAISource(Source):
    platform: str = "vertexai"
    model_name_separator = "_"

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
        self.endpoints: Optional[List[Endpoint]] = None
        self.datasets: Optional[dict] = None

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main Function to fetch and yields work units for various VertexAI resources.
        - Models and Model Versions from the Model Registry
        - Training Jobs
        """

        # Ingest Project
        yield from self._gen_project_workunits()
        # Fetch and Ingest Models, Model Versions a from Model Registry
        yield from self._get_ml_models_workunits()
        # Fetch and Ingest Training Jobs
        yield from self._get_training_jobs_workunits()
        # TODO Fetch Experiments and Experiment Runs

    def _gen_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._get_project_container(),
            name=self.config.project_id,
            sub_types=["Project"],
        )

    def _get_ml_models_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch List of Models in Model Registry and generate a corresponding work unit.
        """
        registered_models = self.client.Model.list()
        for model in registered_models:
            # create work unit for Model Group (= Model in VertexAI)
            yield from self._gen_ml_group_workunits(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create work unit for Model (= Model Version in VertexAI)
                logger.info(
                    f"Ingesting a model (name: {model.display_name} id:{model.name})"
                )
                yield from self._gen_ml_model_endpoint_workunits(
                    model=model, model_version=model_version
                )

    def _get_training_jobs_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetches training jobs from Vertex AI and generates corresponding work units.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates work units containing metadata
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
                yield from self._get_training_job_workunits(job)

    def _get_training_job_workunits(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._gen_data_process_workunits(job)
        yield from self._get_job_output_workunits(job)
        yield from self._get_job_input_workunits(job)

    def _gen_ml_group_workunits(
        self,
        model: Model,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate an MLModelGroup work unit for a VertexAI  Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)

        aspects: List[_Aspect] = list()
        aspects.append(
            MLModelGroupPropertiesClass(
                name=self._make_vertexai_model_group_name(model.name),
                description=model.description,
                created=TimeStampClass(time=int(model.create_time.timestamp() * 1000))
                if model.create_time
                else None,
                lastModified=TimeStampClass(
                    time=int(model.update_time.timestamp() * 1000)
                )
                if model.update_time
                else None,
                customProperties={"displayName": model.display_name},
            )
        )

        # TODO add following when metadata model for mlgroup is updated (these aspects not supported currently)
        # aspects.append(SubTypesClass(typeNames=[MLTypes.MODEL_GROUP]))
        # aspects.append(ContainerClass(container=self._get_project_container().as_urn()))

        yield from auto_workunit(
            MetadataChangeProposalWrapper.construct_many(
                ml_model_group_urn, aspects=aspects
            )
        )

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._make_vertexai_model_name(model.name),
            env=self.config.env,
        )
        return urn

    def _gen_data_process_workunits(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate a work unit for VertexAI Training Job
        """

        created_time = (
            int(job.create_time.timestamp() * 1000)
            if job.create_time
            else int(time.time() * 1000)
        )
        created_actor = f"urn:li:platformResource:{self.platform}"

        job_id = self._make_vertexai_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

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

        # TODO add status of the job
        # aspects.append(
        #     DataProcessInstanceRunEventClass(
        #             status=DataProcessRunStatusClass.COMPLETE,
        #             timestampMillis=0
        #     )
        # }

        yield from auto_workunit(
            MetadataChangeProposalWrapper.construct_many(job_urn, aspects=aspects)
        )

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

    def _get_job_output_workunits(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        This method creates work units that link the training job to the model version
        that it produces. It checks if the job configuration contains a model to upload,
        and if so, it generates a work unit for the model version with the training job
        as part of its properties.
        """

        job_conf = job.to_dict()
        if (
            "modelToUpload" in job_conf
            and "name" in job_conf["modelToUpload"]
            and job_conf["modelToUpload"]["name"]
        ):
            model_version_str = job_conf["modelToUpload"]["versionId"]
            job_urn = self._make_job_urn(job)

            model = Model(model_name=job_conf["modelToUpload"]["name"])
            model_version = self._search_model_version(model, model_version_str)
            if model and model_version:
                logger.info(
                    f"Found output model (name:{model.display_name} id:{model_version_str}) "
                    f"for training job: {job.display_name}"
                )
                yield from self._gen_ml_model_endpoint_workunits(
                    model, model_version, job_urn
                )

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

        return self.datasets.get(dataset_id)

    def _get_dataset_workunits(
        self, dataset_urn: str, ds: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """

        # Create aspects for the dataset
        aspects: List[_Aspect] = list()
        aspects.append(
            DatasetPropertiesClass(
                name=self._make_vertexai_dataset_name(ds.name),
                created=TimeStampClass(time=int(ds.create_time.timestamp() * 1000))
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
        aspects.append(ContainerClass(container=self._get_project_container().as_urn()))
        yield from auto_workunit(
            MetadataChangeProposalWrapper.construct_many(dataset_urn, aspects=aspects)
        )

    def _get_job_input_workunits(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units for the input data of a training job.
        This method checks if the training job is an AutoML job and if it has an input dataset
        configuration. If so, it creates a work unit for the input dataset.
        """

        if self._is_automl_job(job):
            job_conf = job.to_dict()
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
                    yield from self._gen_input_dataset_workunits(job, dataset_id)

    def _gen_input_dataset_workunits(
        self, job: VertexAiResourceNoun, dataset_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        This method creates a work unit for the input dataset of a training job. It constructs the URN
        for the input dataset and the training job, and then creates a DataProcessInstanceInputClass aspect
        to link the input dataset to the training job.
        """

        # Create URN of Input Dataset for Training Job
        dataset_name = self._make_vertexai_dataset_name(entity_id=dataset_id)
        dataset_urn = builder.make_dataset_urn(
            platform=self.platform,
            name=dataset_name,
            env=self.config.env,
        )

        dataset = self._search_dataset(dataset_id) if dataset_id else None
        if dataset:
            logger.info(
                f"Found the name of input dataset ({dataset_name}) with dataset id ({dataset_id})"
            )
            # Yield aspect of input dataset
            yield from self._get_dataset_workunits(dataset_urn=dataset_urn, ds=dataset)

            # Yield aspect(DataProcessInstanceInputClass) of training job
            job_id = self._make_vertexai_job_name(entity_id=job.name)
            yield MetadataChangeProposalWrapper(
                entityUrn=builder.make_data_process_instance_urn(job_id),
                aspect=DataProcessInstanceInputClass(inputs=[dataset_urn]),
            ).as_workunit()

        else:
            logger.error(
                f"Unable to find the name of input dataset ({dataset_name}) with dataset id ({dataset_id})"
            )

    def _gen_endpoint_workunits(
        self, endpoint: Endpoint, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataWorkUnit]:
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
                createdAt=int(endpoint.create_time.timestamp() * 1000),
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                customProperties={"displayName": endpoint.display_name},
            )
        )

        aspects.append(
            ContainerClass(
                container=self._get_project_container().as_urn(),
            )
        )

        aspects.append(SubTypesClass(typeNames=[MLTypes.ENDPOINT]))

        yield from auto_workunit(
            MetadataChangeProposalWrapper.construct_many(endpoint_urn, aspects=aspects)
        )

    def _gen_ml_model_endpoint_workunits(
        self,
        model: Model,
        model_version: VersionInfo,
        training_job_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate an MLModel and Endpoint work unit for an VertexAI Model Version.
        """

        endpoint: Optional[Endpoint] = self._search_endpoint(model)
        endpoint_urn = None

        if endpoint:
            yield from self._gen_endpoint_workunits(endpoint, model, model_version)

        yield from self._gen_ml_model_workunits(
            model, model_version, training_job_urn, endpoint_urn
        )

    def _gen_ml_model_workunits(
        self,
        model: Model,
        model_version: VersionInfo,
        training_job_urn: Optional[str] = None,
        endpoint_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate an MLModel workunit for an VertexAI Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup
        corresponding to a registered Model in VertexAI Model Registry.
        """
        logging.info(f"starting model work unit for model {model.name}")

        model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_model_name(entity_id=model.name)
        model_version_name = (
            f"{model_name}{self.model_name_separator}{model_version.version_id}"
        )
        model_urn = self._make_ml_model_urn(model_version, model_name=model_name)

        aspects: List[_Aspect] = list()

        aspects.append(
            MLModelPropertiesClass(
                name=model_version_name,
                description=model_version.version_description,
                customProperties={
                    "displayName": model_version.model_display_name
                    + self.model_name_separator
                    + model_version.version_id,
                    "resourceName": model.resource_name,
                },
                created=TimeStampClass(
                    int(model_version.version_create_time.timestamp() * 1000)
                )
                if model_version.version_create_time
                else None,
                lastModified=TimeStampClass(
                    int(model_version.version_update_time.timestamp() * 1000)
                )
                if model_version.version_update_time
                else None,
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                groups=[model_group_urn],  # link model version to model group
                trainingJobs=[training_job_urn]
                if training_job_urn
                else None,  # link to training job
                deployments=[endpoint_urn]
                if endpoint_urn
                else [],  # link to model registry and endpoint
                externalUrl=self._make_model_version_external_url(model),
                type="ML Model",
            )
        )

        # TO BE ADDED: Create a container for Project as parent of the dataset
        # aspects.append(
        #     ContainerClass(
        #             container=self._get_project_container().as_urn(),
        #     )
        # )

        yield from auto_workunit(
            MetadataChangeProposalWrapper.construct_many(
                entityUrn=model_urn, aspects=aspects
            )
        )

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

    def _make_ml_model_urn(self, model_version: VersionInfo, model_name: str) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_name}{self.model_name_separator}{model_version.version_id}",
            env=self.config.env,
        )
        return urn

    def _make_job_urn(self, job: VertexAiResourceNoun) -> str:
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        urn = builder.make_data_process_instance_urn(dataProcessInstanceId=job_id)
        return urn

    def _make_vertexai_model_group_name(
        self, entity_id: str, separator: str = "."
    ) -> str:
        entity_type = "model_group"
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

    def _make_vertexai_endpoint_name(self, entity_id: str, separator: str = ".") -> str:
        entity_type = "endpoint"
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

    def _make_vertexai_model_name(self, entity_id: str, separator: str = ".") -> str:
        entity_type = "model"
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

    def _make_vertexai_dataset_name(self, entity_id: str, separator: str = ".") -> str:
        entity_type = "dataset"
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

    def _make_vertexai_job_name(
        self, entity_id: Optional[str], separator: str = "."
    ) -> str:
        entity_type = "job"
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

    def _make_job_external_url(self, job: VertexAiResourceNoun) -> str:
        """
        Model external URL in Vertex AI
        Sample URLs:
        https://console.cloud.google.com/vertex-ai/training/training-pipelines?project=acryl-poc&trainingPipelineId=5401695018589093888
        """
        entity_type = "training"
        external_url: str = (
            f"{self.config.vertexai_url}/{entity_type}/training-pipelines?trainingPipelineId={job.name}"
            f"?project={self.config.project_id}"
        )
        return external_url

    def _make_model_external_url(self, model: Model) -> str:
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=acryl-poc
        """
        entity_type = "models"
        external_url: str = (
            f"{self.config.vertexai_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
            f"?project={self.config.project_id}"
        )
        return external_url

    def _make_model_version_external_url(self, model: Model) -> str:
        """
        Model Version external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=acryl-poc
        """
        entity_type = "models"
        external_url: str = (
            f"{self.config.vertexai_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
            f"/versions/{model.version_id}"
            f"?project={self.config.project_id}"
        )
        return external_url
