import json
import logging
import os
import tempfile
import time
from typing import Any, Iterable, List, Optional, TypeVar, Dict

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
from pydantic import PrivateAttr, root_validator
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.source_common import EnvConfigMixin
from datahub.configuration.validate_multiline_string import pydantic_multiline_string
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
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
    AuditStampClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    SubTypesClass,
    TimeStampClass,
)
from datahub.metadata.schema_classes import (
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    VersionTagClass,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)

class GCPCredential(ConfigModel):

    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(default="service_account", description="Authentication type")
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

    _fix_private_key_newlines = pydantic_multiline_string("private_key")

    @root_validator(skip_on_failure=True)
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("client_x509_cert_url") is None:
            values["client_x509_cert_url"] = (
                f"https://www.googleapis.com/robot/v1/metadata/x509/{values['client_email']}"
            )
        return values

    def create_credential_temp_file(self, project_id:str) -> str:

        # Adding project_id from the top level config
        configs = self.dict()
        configs["project_id"] = project_id
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(configs, indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name


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
            self._credentials_path = self.credential.create_credential_temp_file(self.project_id)
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path


@platform_name("Vertex AI")
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
        aiplatform.init(project=config.project_id, location=config.region)
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
        # Fetch Models, Model Versions a from Model Registry
        yield from self._get_ml_model_workunits()
        # Fetch Training Jobs
        yield from self._get_training_jobs_workunit()
        # TODO Fetch Experiments and Experiment Runs

    def _validate_training_job(self, model: Model) -> bool:
        """
        Validate Model Has Valid Training Job
        """
        job = model.training_job
        if not job:
            return False

        try:
            # when model has ref to training job, but field is sometimes not accessible and RunTImeError thrown when accessed
            # if RunTimeError is not thrown, it is valid and proceed
            name = job.name
            logger.debug(
                (
                    f"can fetch training job name: {name} for model: (name:{model.display_name} id:{model.name})"
                )
            )
            return True
        except RuntimeError:
            logger.debug(
                f"cannot fetch training job name, not valid for model (name:{model.display_name} id:{model.name})"
            )

        return False

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch List of Models in Model Registry and generate a corresponding work unit.
        """
        registered_models = self.client.Model.list()
        for model in registered_models:
            # create work unit for Model Group (= Model in VertexAI)
            yield from self._get_ml_group_workunit(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create work unit for Training Job (if Model has reference to Training Job)
                if self._validate_training_job(model):
                    logger.info(
                        f"Ingesting a training job for a model: {model_version.model_display_name}"
                    )
                    if model.training_job:
                        yield from self._get_data_process_properties_workunit(
                            model.training_job
                        )

                # create work unit for Model (= Model Version in VertexAI)
                logger.info(
                    f"Ingesting a model (name: {model.display_name} id:{model.name})"
                )
                yield from self._get_ml_model_endpoint_workunit(
                    model=model, model_version=model_version
                )

    def _get_training_jobs_workunit(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetches training jobs from Vertex AI and generates corresponding work units.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates work units containing metadata
        about the job, its inputs, and its outputs.
        """
        logger.info("Fetching a list of CustomJobs from VertexAI server")
        for job in self.client.CustomJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info("Fetching a list of CustomTrainingJobs from VertexAI server")
        for job in self.client.CustomTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info(
            "Fetching a list of CustomContainerTrainingJobs from VertexAI server"
        )
        for job in self.client.CustomContainerTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info(
            "Fetching a list of CustomPythonPackageTrainingJob from VertexAI server"
        )
        for job in self.client.CustomPythonPackageTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info("Fetching a list of AutoMLTabularTrainingJobs from VertexAI server")
        for job in self.client.AutoMLTabularTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info("Fetching a list of AutoMLTextTrainingJobs from VertexAI server")
        for job in self.client.AutoMLTextTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info("Fetching a list of AutoMLImageTrainingJobs from VertexAI server")
        for job in self.client.AutoMLImageTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info("Fetching a list of AutoMLVideoTrainingJobs from VertexAI server")
        for job in self.client.AutoMLVideoTrainingJob.list():
            yield from self._get_training_job_workunit(job)

        logger.info(
            "Fetching a list of AutoMLForecastingTrainingJobs from VertexAI server"
        )
        for job in self.client.AutoMLForecastingTrainingJob.list():
            yield from self._get_training_job_workunit(job)

    def _get_training_job_workunit(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._get_data_process_properties_workunit(job)
        yield from self._get_job_output_workunit(job)
        yield from self._get_job_input_workunit(job)

    def _get_ml_group_workunit(
        self,
        model: Model,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate an MLModelGroup work unit for a VertexAI  Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=ml_model_group_urn,
            aspect=MLModelGroupPropertiesClass(
                name=self._make_vertexai_model_group_name(model.name),
                description=model.description,
                createdAt=int(model.create_time.timestamp()),
                customProperties={"displayName": model.display_name},
            ),
        )

        yield from auto_workunit([mcp])

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._make_vertexai_model_name(model.name),
            env=self.config.env,
        )
        return urn

    def _get_data_process_properties_workunit(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate a work unit for VertexAI Training Job
        """

        created_time = int(job.create_time.timestamp()) or int(time.time() * 1000)
        created_actor = f"urn:li:platformResource:{self.platform}"

        job_id = self._make_vertexai_job_name(entity_id=job.name)
        entityUrn = builder.make_data_process_instance_urn(job_id)

        prop_mcp = MetadataChangeProposalWrapper(
            entityUrn=entityUrn,
            aspect=DataProcessInstancePropertiesClass(
                name=job_id,
                created=AuditStampClass(
                    time=created_time,
                    actor=created_actor,
                ),
                externalUrl=self._make_job_external_url(job),
                customProperties={"displayName": job.display_name},
            ),
        )

        jobtype = job.__class__.__name__
        subtype_mcp = MetadataChangeProposalWrapper(
            entityUrn=entityUrn, aspect=SubTypesClass(typeNames=[f"{jobtype}"])
        )

        yield from auto_workunit([prop_mcp, subtype_mcp])

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        return (
            isinstance(job, AutoMLTabularTrainingJob)
            or isinstance(job, AutoMLTextTrainingJob)
            or isinstance(job, AutoMLImageTrainingJob)
            or isinstance(job, AutoMLVideoTrainingJob)
        ) or isinstance(job, AutoMLForecastingTrainingJob)

    def _search_model_version(
        self, model: Model, version_id: str
    ) -> Optional[VersionInfo]:
        for version in model.versioning_registry.list_versions():
            if version.version_id == version_id:
                return version
        return None

    def _get_job_output_workunit(
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
                    f" found a training job: {job.display_name} generated "
                    f"a model (name:{model.display_name} id:{model_version_str})"
                )
                yield from self._get_ml_model_endpoint_workunit(
                    model, model_version, job_urn
                )

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
        """
        Search for a dataset by its ID in Vertex AI.
        This method iterates through different types of datasets (Text, Tabular, Image,
        TimeSeries, and Video) to find a dataset that matches the given dataset ID.
        """

        if self.datasets is None:
            self.datasets = dict()
            for ds in self.client.datasets.TextDataset.list():
                self.datasets[ds.name] = ds
            for ds in self.client.datasets.TabularDataset.list():
                self.datasets[ds.name] = ds
            for ds in self.client.datasets.ImageDataset.list():
                self.datasets[ds.name] = ds
            for ds in self.client.datasets.TimeSeriesDataset.list():
                self.datasets[ds.name] = ds
            for ds in self.client.datasets.VideoDataset.list():
                self.datasets[ds.name] = ds

        return self.datasets[dataset_id] if dataset_id in self.datasets else None

    def _get_dataset_workunit(
        self, urn: str, ds: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """

        mcps = []
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(
                    name=self._make_vertexai_dataset_name(ds.name),
                    created=TimeStampClass(time=int(ds.create_time.timestamp())),
                    description=f"Dataset: {ds.display_name} for training job",
                    customProperties={
                        "displayName": ds.display_name,
                        "resourceName": ds.resource_name,
                    },
                    qualifiedName=ds.resource_name,
                ),
            )
        )

        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=SubTypesClass(typeNames=["Dataset"])
            )
        )

        yield from auto_workunit(mcps)

    def _get_job_input_workunit(
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
                    f" found a training job: {job.display_name} used input dataset: {id: dataset_id}"
                )

                if dataset_id:
                    yield from self._get_data_process_input_workunit(job, dataset_id)

    def _get_data_process_input_workunit(
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

        dataset = self._search_dataset(dataset_id)
        if dataset:
            yield from self._get_dataset_workunit(urn=dataset_urn, ds=dataset)

        # Create URN of Training Job
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=builder.make_data_process_instance_urn(job_id),
            aspect=DataProcessInstanceInputClass(inputs=[dataset_urn]),
        )
        logger.info(
            f" found training job :{job.display_name} used input dataset : {dataset_name}"
        )
        yield from auto_workunit([mcp])

    def _get_ml_model_endpoint_workunit(
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
            endpoint_urn = builder.make_ml_model_deployment_urn(
                platform=self.platform,
                deployment_name=self._make_vertexai_endpoint_name(
                    entity_id=endpoint.display_name
                ),
                env=self.config.env,
            )
            deployment_aspect = MLModelDeploymentPropertiesClass(
                description=model.description,
                createdAt=int(endpoint.create_time.timestamp()),
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                customProperties={"displayName": endpoint.display_name},
            )

            mcp = MetadataChangeProposalWrapper(
                entityUrn=endpoint_urn, aspect=deployment_aspect
            )
            yield from auto_workunit([mcp])

        yield from self._get_ml_model_properties_workunit(
            model, model_version, training_job_urn, endpoint_urn
        )

    def _get_ml_model_properties_workunit(
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

        ml_model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_model_name(entity_id=model.name)
        model_version_name = (
            f"{model_name}{self.model_name_separator}{model_version.version_id}"
        )
        ml_model_urn = self._make_ml_model_urn(model_version, model_name=model_name)

        ml_model_properties = MLModelPropertiesClass(
            name=model_version_name,
            description=model_version.version_description,
            customProperties={
                "displayName": model_version.model_display_name
                + self.model_name_separator
                + model_version.version_id,
                "resourceName": model.resource_name,
            },
            created=TimeStampClass(model_version.version_create_time.second),
            lastModified=TimeStampClass(model_version.version_update_time.second),
            version=VersionTagClass(versionTag=str(model_version.version_id)),
            groups=[ml_model_group_urn],  # link model version to model group
            trainingJobs=[training_job_urn]
            if training_job_urn
            else None,  # link to training job
            deployments=[endpoint_urn]
            if endpoint_urn
            else [],  # link to model registry and endpoint
            externalUrl=self._make_model_version_external_url(model),
        )

        # logging.info(f"created model version {ml_model_properties.name} associated with group {ml_model_group_urn}")
        mcp = MetadataChangeProposalWrapper(
            entityUrn=ml_model_urn, aspect=ml_model_properties
        )
        yield from auto_workunit([mcp])

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

    def _make_vertexai_name(
        self, entity_type: str, entity_id: str, separator: str = "."
    ) -> str:
        return f"{self.config.project_id}{separator}{entity_type}{separator}{entity_id}"

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

    def _make_vertexai_job_name(self, entity_id: str, separator: str = ".") -> str:
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
