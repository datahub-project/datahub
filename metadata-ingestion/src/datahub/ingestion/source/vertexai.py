import logging
import time
from typing import Iterable, Optional, TypeVar, List

from google.cloud import aiplatform
from google.cloud.aiplatform import AutoMLTabularTrainingJob, CustomJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, \
    AutoMLVideoTrainingJob, AutoMLForecastingTrainingJob
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
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
from datahub.metadata._schema_classes import DataProcessInstancePropertiesClass, AuditStampClass, \
    DataProcessInstanceInputClass
from datahub.metadata.schema_classes import (
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    VersionTagClass,
    MLModelDeploymentPropertiesClass,
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
    vertexai_base_url = "https://console.cloud.google.com/vertex-ai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        aiplatform.init(project=config.project_id, location=config.region)
        self.client = aiplatform

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
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
            return True
        except RuntimeError:
            logger.info("Job name is not accessible, not valid training job for %s ", model.name)

        return False

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch List of Models in Model Registry and generate a corresponding workunit.
        """

        registered_models = self.client.Model.list()
        for model in registered_models:
            # create work unit for Model Group (= Model in VertexAI)
            yield self._get_ml_group_workunit(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create work unit for Training Job (if Model has reference to Training Job)
                if self._validate_training_job(model):
                    yield self._get_data_process_properties_workunit(model.training_job)

                # create work unit for Model (= Model Version in VertexAI)
                yield self._get_ml_model_properties_workunit(
                    model=model, model_version=model_version
                )

    def _get_training_job_workunit(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetches training jobs from Vertex AI and generates corresponding workunits.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates workunits containing metadata
        about the job, its inputs, and its outputs.
        """
        yield from self._get_data_process_workunit(self.client.CustomJob.list())
        yield from self._get_data_process_workunit(self.client.CustomTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.CustomContainerTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.CustomPythonPackageTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.AutoMLTabularTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.AutoMLTextTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.AutoMLImageTrainingJob.list())
        yield from self._get_data_process_workunit(self.client.AutoMLVideoTrainingJob.list())
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
        Generate an MLModelGroup workunit for a VertexAI  Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)
        ml_model_group_properties = MLModelGroupPropertiesClass(
            name=model.name,
            description=model.description,
            createdAt=model.create_time,
            trainingJobs=[],
        )
        wu = self._create_workunit(
            urn=ml_model_group_urn,
            aspect=ml_model_group_properties,
        )
        return wu

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=model.name,
            env=self.config.env,
        )
        return urn

    def _get_data_process_properties_workunit(self, job: _TrainingJob) -> MetadataWorkUnit:
        """
        Generate a work unit for VertexAI Training Job
        """
        created_time = job.start_time or int(time.time() * 1000)
        created_actor = ""
        # created_actor = (
        #     f"urn:li:platformResource:{job, "user"}" if getattr(job, "user") else ""
        # )

        job_id = self._make_vertexai_name(entity_type="job", entity_id=job.name)
        entityUrn = builder.make_data_process_instance_urn(job_id)
        aspect = DataProcessInstancePropertiesClass(
                name=job_id,
                created=AuditStampClass(
                    time=created_time,
                    actor=created_actor,
                ),
                externalUrl=self._make_job_external_url(job),
                customProperties={"displayName": job.display_name},
            )

        return self._create_workunit(urn=entityUrn, aspect=aspect)

    def _is_automl_job(self, job: _TrainingJob) -> bool:
        return ((isinstance(job, AutoMLTabularTrainingJob) or
                isinstance(job, AutoMLTextTrainingJob) or
                isinstance(job, AutoMLImageTrainingJob) or
                isinstance(job, AutoMLVideoTrainingJob)) or
                isinstance(job, AutoMLForecastingTrainingJob))

    def _get_job_output_workunit(self, job: _TrainingJob) -> Iterable[MetadataWorkUnit]:
        """
        This method creates work units that link the training job to the model version
        that it produces. It checks if the job configuration contains a model to upload,
        and if so, it generates a work unit for the model version with the training job
        as part of its properties.
        """

        job_conf = job.to_dict()
        if ("modelToUpload" in job_conf and "name" in job_conf["modelToUpload"] and job_conf["modelToUpload"]["name"]):

            model_id = job_conf["modelToUpload"]["name"].split("/")[-1]
            model_version = job_conf["modelToUpload"]["versionId"]
            model_display_name = job_conf["modelToUpload"]["displayName"]
            entity_id = f"{model_id}{self.config.model_name_separator}{model_version}"
            model_version = self._make_vertexai_name(entity_type="model", entity_id=entity_id)

            model_version_urn = builder.make_ml_model_urn(
                platform=self.platform,
                model_name=model_version,
                env=self.config.env,
            )

            job_urn = self._make_job_urn(job)

            aspect = MLModelPropertiesClass(
                trainingJobs=[job_urn],
                customProperties={"displayName": model_display_name}
            )
            yield self._create_workunit(urn=model_version_urn, aspect=aspect)

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
                if dataset_id:
                    yield self._get_data_process_input_workunit(job, dataset_id)

    def _get_data_process_input_workunit(self, job: _TrainingJob, dataset_id: str) -> MetadataWorkUnit:
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

        # Create URN of Training Job
        job_id = self._make_vertexai_name(entity_type="job", entity_id=job.name)
        entityUrn = builder.make_data_process_instance_urn(job_id)
        aspect = DataProcessInstanceInputClass(
            inputs=[dataset_urn]
        )
        return self._create_workunit(urn=entityUrn, aspect=aspect)


    def _get_ml_model_properties_workunit(
        self,
        model: Model,
        model_version: VersionInfo,
    ) -> MetadataWorkUnit:
        """
        Generate an MLModel workunit for an VertexAI Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup
        corresponding to a registered Model in VertexAI Model Registry.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_name(entity_type="model", entity_id=model.name)
        ml_model_urn = self._make_ml_model_urn(model_version, model_name=model_name)

        training_job_names = None
        training_metrics = None
        hyperparams = None

        if self._validate_training_job(model):
            training_job_names = [model.training_job.name]

        ml_model_properties = MLModelPropertiesClass(
            name=model.name,
            description=model_version.version_description,
            created=model_version.version_create_time,
            lastModified=model_version.version_update_time,
            version=VersionTagClass(versionTag=str(model_version.version_id)),
            hyperParams=hyperparams,
            trainingMetrics=training_metrics,
            groups=[ml_model_group_urn], # link model version to model group
            trainingJobs=training_job_names if training_job_names else None, # link to training job
            deployments=[], # link to model registry and endpoint
            externalUrl=self._make_model_version_external_url(model)
            # tags=list(model_version.tags.keys()),
            # customProperties=model_version.tags,
        )

        wu = self._create_workunit(urn=ml_model_urn, aspect=ml_model_properties)
        return wu



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
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/default?backTo=training&trainingPipelineId=5401695018589093888&project=acryl-poc
        """
        entity_type = "training"
        external_url = (f"{self.vertexai_base_url}/{entity_type}/training-pipelines?trainingPipelineId={job.name}"
                        f"?project={self.config.project_id}")
        return external_url

    def _make_model_external_url(self, model: Model):
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=acryl-poc
        """
        entity_type = "models"
        external_url = (f"{self.vertexai_base_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
                        f"?project={self.config.project_id}")
        return external_url

    def _make_model_version_external_url(self, model: Model):
        """
        Model Version external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=acryl-poc
        """
        entity_type = "models"
        external_url = (f"{self.vertexai_base_url}/{entity_type}/locations/{self.config.region}/{entity_type}/{model.name}"
                        f"/versions/{model.version_id}"
                        f"?project={self.config.project_id}")
        return external_url

