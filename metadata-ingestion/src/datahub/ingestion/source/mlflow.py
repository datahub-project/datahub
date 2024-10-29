from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypeVar, Union

from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.entities.model_registry import ModelVersion, RegisteredModel
from mlflow.store.entities import PagedList
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
    GlobalTagsClass,
    MLHyperParamClass,
    MLMetricClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    TagAssociationClass,
    TagPropertiesClass,
    VersionTagClass,
    _Aspect,
)

T = TypeVar("T")


class MLflowConfig(EnvConfigMixin):
    tracking_uri: Optional[str] = Field(
        default=None,
        description="Tracking server URI. If not set, an MLflow default tracking_uri is used (local `mlruns/` directory or `MLFLOW_TRACKING_URI` environment variable)",
    )
    registry_uri: Optional[str] = Field(
        default=None,
        description="Registry server URI. If not set, an MLflow default registry_uri is used (value of tracking_uri or `MLFLOW_REGISTRY_URI` environment variable)",
    )
    model_name_separator: str = Field(
        default="_",
        description="A string which separates model name from its version (e.g. model_1 or model-1)",
    )


@dataclass
class MLflowRegisteredModelStageInfo:
    name: str
    description: str
    color_hex: str


@platform_name("MLflow")
@config_class(MLflowConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for MLflow Registered Models and Model Versions",
)
@capability(SourceCapability.TAGS, "Extract tags for MLflow Registered Model Stages")
class MLflowSource(Source):
    platform = "mlflow"
    registered_model_stages_info = (
        MLflowRegisteredModelStageInfo(
            name="Production",
            description="Production Stage for an ML model in MLflow Model Registry",
            color_hex="#308613",
        ),
        MLflowRegisteredModelStageInfo(
            name="Staging",
            description="Staging Stage for an ML model in MLflow Model Registry",
            color_hex="#FACB66",
        ),
        MLflowRegisteredModelStageInfo(
            name="Archived",
            description="Archived Stage for an ML model in MLflow Model Registry",
            color_hex="#5D7283",
        ),
        MLflowRegisteredModelStageInfo(
            name="None",
            description="None Stage for an ML model in MLflow Model Registry",
            color_hex="#F2F4F5",
        ),
    )

    def __init__(self, ctx: PipelineContext, config: MLflowConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.client = MlflowClient(
            tracking_uri=self.config.tracking_uri,
            registry_uri=self.config.registry_uri,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._get_tags_workunits()
        yield from self._get_ml_model_workunits()

    def _get_tags_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Create tags for each Stage in MLflow Model Registry.
        """
        for stage_info in self.registered_model_stages_info:
            tag_urn = self._make_stage_tag_urn(stage_info.name)
            tag_properties = TagPropertiesClass(
                name=self._make_stage_tag_name(stage_info.name),
                description=stage_info.description,
                colorHex=stage_info.color_hex,
            )
            wu = self._create_workunit(urn=tag_urn, aspect=tag_properties)
            yield wu

    def _make_stage_tag_urn(self, stage_name: str) -> str:
        tag_name = self._make_stage_tag_name(stage_name)
        tag_urn = builder.make_tag_urn(tag_name)
        return tag_urn

    def _make_stage_tag_name(self, stage_name: str) -> str:
        return f"{self.platform}_{stage_name.lower()}"

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
        registered_models = self._get_mlflow_registered_models()
        for registered_model in registered_models:
            yield self._get_ml_group_workunit(registered_model)
            model_versions = self._get_mlflow_model_versions(registered_model)
            for model_version in model_versions:
                run = self._get_mlflow_run(model_version)
                yield self._get_ml_model_properties_workunit(
                    registered_model=registered_model,
                    model_version=model_version,
                    run=run,
                )
                yield self._get_global_tags_workunit(model_version=model_version)

    def _get_mlflow_registered_models(self) -> Iterable[RegisteredModel]:
        """
        Get all Registered Models in MLflow Model Registry.
        """
        registered_models: Iterable[
            RegisteredModel
        ] = self._traverse_mlflow_search_func(
            search_func=self.client.search_registered_models,
        )
        return registered_models

    @staticmethod
    def _traverse_mlflow_search_func(
        search_func: Callable[..., PagedList[T]],
        **kwargs: Any,
    ) -> Iterable[T]:
        """
        Utility to traverse an MLflow search_* functions which return PagedList.
        """
        next_page_token = None
        while True:
            paged_list = search_func(page_token=next_page_token, **kwargs)
            yield from paged_list.to_list()
            next_page_token = paged_list.token
            if not next_page_token:
                return

    def _get_ml_group_workunit(
        self,
        registered_model: RegisteredModel,
    ) -> MetadataWorkUnit:
        """
        Generate an MLModelGroup workunit for an MLflow Registered Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        ml_model_group_properties = MLModelGroupPropertiesClass(
            customProperties=registered_model.tags,
            description=registered_model.description,
            createdAt=registered_model.creation_timestamp,
        )
        wu = self._create_workunit(
            urn=ml_model_group_urn,
            aspect=ml_model_group_properties,
        )
        return wu

    def _make_ml_model_group_urn(self, registered_model: RegisteredModel) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=registered_model.name,
            env=self.config.env,
        )
        return urn

    def _get_mlflow_model_versions(
        self,
        registered_model: RegisteredModel,
    ) -> Iterable[ModelVersion]:
        """
        Get all Model Versions for each Registered Model.
        """
        filter_string = f"name = '{registered_model.name}'"
        model_versions: Iterable[ModelVersion] = self._traverse_mlflow_search_func(
            search_func=self.client.search_model_versions,
            filter_string=filter_string,
        )
        return model_versions

    def _get_mlflow_run(self, model_version: ModelVersion) -> Union[None, Run]:
        """
        Get a Run associated with a Model Version. Some MVs may exist without Run.
        """
        if model_version.run_id:
            run = self.client.get_run(model_version.run_id)
            return run
        else:
            return None

    def _get_ml_model_properties_workunit(
        self,
        registered_model: RegisteredModel,
        model_version: ModelVersion,
        run: Union[None, Run],
    ) -> MetadataWorkUnit:
        """
        Generate an MLModel workunit for an MLflow Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup corresponding to a Registered Model.
        If a model was registered without an associated Run then hyperparams and metrics are not available.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        ml_model_urn = self._make_ml_model_urn(model_version)
        if run:
            hyperparams = [
                MLHyperParamClass(name=k, value=str(v))
                for k, v in run.data.params.items()
            ]
            training_metrics = [
                MLMetricClass(name=k, value=str(v)) for k, v in run.data.metrics.items()
            ]
        else:
            hyperparams = None
            training_metrics = None
        ml_model_properties = MLModelPropertiesClass(
            customProperties=model_version.tags,
            externalUrl=self._make_external_url(model_version),
            description=model_version.description,
            date=model_version.creation_timestamp,
            version=VersionTagClass(versionTag=str(model_version.version)),
            hyperParams=hyperparams,
            trainingMetrics=training_metrics,
            # mlflow tags are dicts, but datahub tags are lists. currently use only keys from mlflow tags
            tags=list(model_version.tags.keys()),
            groups=[ml_model_group_urn],
        )
        wu = self._create_workunit(urn=ml_model_urn, aspect=ml_model_properties)
        return wu

    def _make_ml_model_urn(self, model_version: ModelVersion) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_version.name}{self.config.model_name_separator}{model_version.version}",
            env=self.config.env,
        )
        return urn

    def _make_external_url(self, model_version: ModelVersion) -> Union[None, str]:
        """
        Generate URL for a Model Version to MLflow UI.
        """
        base_uri = self.client.tracking_uri
        if base_uri.startswith("http"):
            return f"{base_uri.rstrip('/')}/#/models/{model_version.name}/versions/{model_version.version}"
        else:
            return None

    def _get_global_tags_workunit(
        self,
        model_version: ModelVersion,
    ) -> MetadataWorkUnit:
        """
        Associate a Model Version Stage with a corresponding tag.
        """
        global_tags = GlobalTagsClass(
            tags=[
                TagAssociationClass(
                    tag=self._make_stage_tag_urn(model_version.current_stage),
                ),
            ]
        )
        wu = self._create_workunit(
            urn=self._make_ml_model_urn(model_version),
            aspect=global_tags,
        )
        return wu

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MLflowConfig.parse_obj(config_dict)
        return cls(ctx, config)
