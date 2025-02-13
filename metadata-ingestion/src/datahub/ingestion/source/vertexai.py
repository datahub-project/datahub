import logging
from typing import Iterable, Optional, TypeVar

from google.cloud import aiplatform
from google.cloud.aiplatform.models import Model, VersionInfo
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
from datahub.metadata.schema_classes import (
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    VersionTagClass,
    _Aspect,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


class VertexAIConfig(EnvConfigMixin):
    project_id: str = Field(description=("Project ID"))
    region: str = Field(
        description=("Region"),
    )
    bucket_uri: Optional[str] = Field(
        default=None,
        description=("Bucket URI"),
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
@capability(SourceCapability.TAGS, "Extract tags for vertexai Registered Model Stages")
class VertexAISource(Source):
    platform = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        aiplatform.init(project=config.project_id, location=config.region)
        self.client = aiplatform

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._get_ml_model_workunits()

    def _create_workunit(self, urn: str, aspect: _Aspect) -> MetadataWorkUnit:
        """
        Utility to create an MCP workunit.
        """
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        ).as_workunit()

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Traverse each Registered Model in Model Registry and generate a corresponding workunit.
        """
        registered_models = self.client.Model.list()
        for model in registered_models:
            yield self._get_ml_group_workunit(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                yield self._get_ml_model_properties_workunit(
                    model=model, model_version=model_version
                )

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

    def _get_ml_model_properties_workunit(
        self,
        model: Model,
        model_version: VersionInfo,
    ) -> MetadataWorkUnit:
        """
        Generate an MLModel workunit for an VertexAI Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup
        corresponding to a Registered Model in VertexAI Model Registry.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(model)
        ml_model_urn = self._make_ml_model_urn(model_version)

        training_job = None
        training_metrics = None
        hyperparams = None
        try:
            job = model.training_job
            training_job = [job]
        except RuntimeError:
            logger.info("No training job found for model %s", model.name)

        ml_model_properties = MLModelPropertiesClass(
            name=model.name,
            description=model_version.version_description,
            date=model_version.version_create_time,
            version=VersionTagClass(versionTag=str(model_version.version_id)),
            hyperParams=hyperparams,
            trainingMetrics=training_metrics,
            trainingJobs=training_job,
            groups=[ml_model_group_urn],
            # tags=list(model_version.tags.keys()),
            # customProperties=model_version.tags,
            # externalUrl=self._make_external_url(model_version),
        )

        wu = self._create_workunit(urn=ml_model_urn, aspect=ml_model_properties)
        return wu

    def _make_ml_model_urn(self, model_version: VersionInfo) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_version.model_display_name}{self.config.model_name_separator}{model_version.version_id}",
            env=self.config.env,
        )
        return urn
