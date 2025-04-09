import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, Union

from mlflow import MlflowClient
from mlflow.entities import Dataset as MlflowDataset, Experiment, Run
from mlflow.entities.model_registry import ModelVersion, RegisteredModel
from mlflow.store.entities import PagedList
from pydantic.fields import Field
import logging

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
)
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.data_platforms import KNOWN_VALID_PLATFORM_NAMES
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    GlobalTagsClass,
    MetadataAttributionClass,
    MLHyperParamClass,
    MLMetricClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    MLTrainingRunPropertiesClass,
    PlatformResourceInfoClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    TimeStampClass,
    UpstreamClass,
    UpstreamLineageClass,
    VersionPropertiesClass,
    VersionTagClass,
    _Aspect,
)
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn, MlModelUrn, VersionSetUrn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset

T = TypeVar("T")

logger = logging.getLogger(__name__)

class MLflowConfig(StatefulIngestionConfigBase, EnvConfigMixin):
    tracking_uri: Optional[str] = Field(
        default=None,
        description=(
            "Tracking server URI. If not set, an MLflow default tracking_uri is used"
            " (local `mlruns/` directory or `MLFLOW_TRACKING_URI` environment variable)"
        ),
    )
    registry_uri: Optional[str] = Field(
        default=None,
        description=(
            "Registry server URI. If not set, an MLflow default registry_uri is used"
            " (value of tracking_uri or `MLFLOW_REGISTRY_URI` environment variable)"
        ),
    )
    model_name_separator: str = Field(
        default="_",
        description="A string which separates model name from its version (e.g. model_1 or model-1)",
    )
    base_external_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL to use when constructing external URLs to MLflow."
            " If not set, tracking_uri is used if it's an HTTP URL."
            " If neither is set, external URLs are not generated."
        ),
    )
    materialize_dataset_inputs: Optional[bool] = Field(
        default=False,
        description="Whether to materialize dataset inputs for each run",
    )
    source_mapping_to_platform: Optional[dict] = Field(
        default=None, description="Mapping of source type to datahub platform"
    )

    username: Optional[str] = Field(
        default=None, description="Username for MLflow authentication"
    )
    password: Optional[str] = Field(
        default=None, description="Password for MLflow authentication"
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


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
class MLflowSource(StatefulIngestionSourceBase):
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
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.client = self._configure_client()

    def _configure_client(self) -> MlflowClient:
        if bool(self.config.username) != bool(self.config.password):
            raise ValueError("Both username and password must be set together")

        if self.config.username and self.config.password:
            os.environ["MLFLOW_TRACKING_USERNAME"] = self.config.username
            os.environ["MLFLOW_TRACKING_PASSWORD"] = self.config.password

        return MlflowClient(
            tracking_uri=self.config.tracking_uri,
            registry_uri=self.config.registry_uri,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._get_tags_workunits()
        # yield from self._get_experiment_workunits()
        yield from self._get_ml_model_workunits()

    def _get_tags_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Create tags for each Stage in MLflow Model Registry.
        """
        logger.info("!!! getting tags workunits")
        for stage_info in self.registered_model_stages_info:
            tag_urn = self._make_stage_tag_urn(stage_info.name)
            tag_properties = TagPropertiesClass(
                name=self._make_stage_tag_name(stage_info.name),
                description=stage_info.description,
                colorHex=stage_info.color_hex,
            )
            wu = self._create_workunit(urn=tag_urn, aspect=tag_properties)
        return iter([])

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

    def _get_experiment_workunits(self) -> Iterable[MetadataWorkUnit]:
        experiments = self._get_mlflow_experiments()
        for experiment in experiments:
            yield from self._get_experiment_container_workunit(experiment)

            runs = self._get_mlflow_runs_from_experiment(experiment)
            if runs:
                for run in runs:
                    yield from self._get_run_workunits(experiment, run)
                    yield from self._get_dataset_input_workunits(run)

    def _get_experiment_custom_properties(self, experiment):
        experiment_custom_props = getattr(experiment, "tags", {}) or {}
        experiment_custom_props.pop("mlflow.note.content", None)
        experiment_custom_props["artifacts_location"] = experiment.artifact_location
        return experiment_custom_props

    def _get_experiment_container_workunit(
        self, experiment: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        experiment_container = Container(
            container_key=ExperimentKey(
                platform=str(DataPlatformUrn(platform_name=self.platform)),
                id=experiment.name,
            ),
            subtype=MLAssetSubTypes.MLFLOW_EXPERIMENT,
            display_name=experiment.name,
            description=experiment.tags.get("mlflow.note.content"),
            extra_properties=self._get_experiment_custom_properties(experiment),
        )

        yield from experiment_container.as_workunits()

    def _get_run_metrics(self, run: Run) -> List[MLMetricClass]:
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.data.metrics.items()
        ]

    def _get_run_params(self, run: Run) -> List[MLHyperParamClass]:
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.data.params.items()
        ]

    def _convert_run_result_type(
        self, status: str
    ) -> DataProcessInstanceRunResultClass:
        if status == "FINISHED":
            return DataProcessInstanceRunResultClass(
                type="SUCCESS", nativeResultType=self.platform
            )
        elif status == "FAILED":
            return DataProcessInstanceRunResultClass(
                type="FAILURE", nativeResultType=self.platform
            )
        else:
            return DataProcessInstanceRunResultClass(
                type="SKIPPED", nativeResultType=self.platform
            )

    def _get_dataset_schema(
        self, dataset: MlflowDataset
    ) -> Optional[List[Tuple[str, str]]]:
        try:
            schema_dict = json.loads(dataset.schema)
        except json.JSONDecodeError:
            self.report.warning(
                title="Failed to load dataset schema",
                message="Schema metadata will be missing due to a JSON parsing error.",
                context=f"Dataset: {dataset.name}, Schema: {dataset.schema}",
            )
            return None

        if "mlflow_colspec" in schema_dict:
            try:
                return [
                    (field["name"], field["type"])
                    for field in schema_dict["mlflow_colspec"]
                ]
            except (KeyError, TypeError):
                return None
        # If the schema is not formatted, return None
        return None

    def _get_mlflow_registered_models(self) -> Iterable[RegisteredModel]:
        """
        Get all Registered Models in MLflow Model Registry.
        """
        logger.info("!!! getting mlflow registered models")
        registered_models: Iterable[RegisteredModel] = (
            self._traverse_mlflow_search_func(
                search_func=self.client.search_registered_models,
            )
        )
        return registered_models

    def _get_mlflow_experiments(self) -> Iterable[Experiment]:
        experiments: Iterable[Experiment] = self._traverse_mlflow_search_func(
            search_func=self.client.search_experiments,
        )
        return experiments

    def _get_mlflow_runs_from_experiment(self, experiment: Experiment) -> Iterable[Run]:
        runs: Iterable[Run] = self._traverse_mlflow_search_func(
            search_func=self.client.search_runs,
            experiment_ids=[experiment.experiment_id],
        )
        return runs

    @staticmethod
    def _traverse_mlflow_search_func(
        search_func: Callable[..., PagedList[T]],
        **kwargs: Any,
    ) -> Iterable[T]:
        """
        Utility to traverse an MLflow search_* functions which return PagedList.
        """
        logger.info(f"!!! traversing mlflow search function with {search_func.__name__}")
        try:
            next_page_token = None
            while True:
                paged_list = search_func(page_token=next_page_token, **kwargs)
                yield from paged_list.to_list()
                next_page_token = paged_list.token
                if not next_page_token:
                    return
        except Exception as e:
            logger.error(f"Error traversing MLflow search function: {e}")
            return

    def _get_latest_version(self, registered_model: RegisteredModel) -> Optional[str]:
        return (
            str(registered_model.latest_versions[0].version)
            if registered_model.latest_versions
            else None
        )

    def _get_ml_group_workunit(
        self,
        registered_model: RegisteredModel,
    ) -> MetadataWorkUnit:
        """
        Generate an MLModelGroup workunit for an MLflow Registered Model.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        logger.info(f"!!! getting tags for registered model {registered_model.name}")
        try:
            tags = registered_model.tags
            print("!!! got tags:", tags)
        except Exception as e:
            logger.error(f"Error getting tags for registered model {registered_model.name}: {e}")
        # ml_model_group_properties = MLModelGroupPropertiesClass(
        #     customProperties=registered_model.tags,
        #     description=registered_model.description,
        #     createdAt=registered_model.creation_timestamp,
        # )
        # wu = self._create_workunit(
        #     urn=ml_model_group_urn,
        #     aspect=ml_model_group_properties,
        # )
        # return wu
        return iter([])

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

    def get_mlflow_model_versions_from_run(self, run_id):
        filter_string = f"run_id = '{run_id}'"

        model_versions: Iterable[ModelVersion] = self._traverse_mlflow_search_func(
            search_func=self.client.search_model_versions,
            filter_string=filter_string,
        )

        return list(model_versions)

    def _get_mlflow_run(self, model_version: ModelVersion) -> Union[None, Run]:
        """
        Get a Run associated with a Model Version. Some MVs may exist without Run.
        """
        if model_version.run_id:
            run = self.client.get_run(model_version.run_id)
            return run
        else:
            return None

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Traverse each Registered Model in Model Registry and generate a corresponding workunit.
        """
        logger.info("!!! getting ml model workunits")
        registered_models = self._get_mlflow_registered_models()
        logger.info(f"!!! got registered models")
        logger.info(f"!!! iterating through registered models")
        for registered_model in registered_models:
            self._get_ml_group_workunit(registered_model)
            model_versions = self._get_mlflow_model_versions(registered_model)
            for model_version in model_versions:
                run = self._get_mlflow_run(model_version)
                self._get_ml_model_properties_workunit(
                    registered_model=registered_model,
                    model_version=model_version,
                    run=run,
                )
                self._get_global_tags_workunit(model_version=model_version)
        return iter([])

    def _get_version_set_urn(self, registered_model: RegisteredModel) -> VersionSetUrn:
        guid_dict = {"platform": self.platform, "name": registered_model.name}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=MlModelUrn.ENTITY_TYPE,
        )

        return version_set_urn

    def _get_ml_model_version_properties_workunit(
        self,
        model_version: ModelVersion,
        version_set_urn: VersionSetUrn,
    ) -> MetadataWorkUnit:
        ml_model_urn = self._make_ml_model_urn(model_version)

        # get mlmodel name from ml model urn
        ml_model_version_properties = VersionPropertiesClass(
            version=VersionTagClass(
                versionTag=str(model_version.version),
                metadataAttribution=MetadataAttributionClass(
                    time=model_version.creation_timestamp,
                    actor="urn:li:corpuser:datahub",
                ),
            ),
            versionSet=str(version_set_urn),
            sortId=str(model_version.version).zfill(10),
            aliases=[
                VersionTagClass(versionTag=alias) for alias in model_version.aliases
            ],
        )

        wu = MetadataChangeProposalWrapper(
            entityUrn=str(ml_model_urn),
            aspect=ml_model_version_properties,
        ).as_workunit()

        return wu

    def _get_ml_model_properties_workunit(
        self,
        registered_model: RegisteredModel,
        model_version: ModelVersion,
        run: Union[None, Run],
    ) -> Optional[MetadataWorkUnit]:
        """
        Generate an MLModel workunit for an MLflow Model Version.
        Every Model Version is a DataHub MLModel entity associated with an MLModelGroup corresponding to a Registered Model.
        If a model was registered without an associated Run then hyperparams and metrics are not available.
        """
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        ml_model_urn = self._make_ml_model_urn(model_version)

        if run:
            # Use the same metrics and hyperparams from the run
            hyperparams = self._get_run_params(run)
            training_metrics = self._get_run_metrics(run)
            run_urn = DataProcessInstance(
                id=run.info.run_id,
                orchestrator=self.platform,
            ).urn

            training_jobs = [str(run_urn)] if run_urn else []
        else:
            hyperparams = None
            training_metrics = None
        try:
            print("!!! getting tags for model version:", model_version.name)
            model_version_tags = model_version.tags
            print("!!! got tags:", model_version_tags)
            print("!!! getting tags keys")
            tag_keys = list(model_version_tags.keys())
            print("!!! got tag keys:", tag_keys)
        except Exception as e:
            logger.warning(f"Error getting tags for model version {model_version.name}: {e}")

        # ml_model_properties = MLModelPropertiesClass(
        #     customProperties=model_version_tags,
        #     externalUrl=self._make_external_url(model_version),
        #     description=model_version.description,
        #     date=model_version.creation_timestamp,
        #     version=VersionTagClass(versionTag=str(model_version.version)),
        #     hyperParams=hyperparams,
        #     trainingMetrics=training_metrics,
        #     tags=tag_keys,
        #     groups=[ml_model_group_urn],
        # )

        # return self._create_workunit(urn=ml_model_urn, aspect=ml_model_properties)
        return iter([])

    def _make_ml_model_urn(self, model_version: ModelVersion) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_version.name}{self.config.model_name_separator}{model_version.version}",
            env=self.config.env,
        )
        return urn

    def _get_base_external_url_from_tracking_uri(self) -> Optional[str]:
        if isinstance(
            self.client.tracking_uri, str
        ) and self.client.tracking_uri.startswith("http"):
            return self.client.tracking_uri
        else:
            return None

    def _make_external_url(self, model_version: ModelVersion) -> Optional[str]:
        """
        Generate URL for a Model Version to MLflow UI.
        """
        base_uri = (
            self.config.base_external_url
            or self._get_base_external_url_from_tracking_uri()
        )
        if base_uri:
            return f"{base_uri.rstrip('/')}/#/models/{model_version.name}/versions/{model_version.version}"
        else:
            return None

    def _make_external_url_from_run(
        self, experiment: Experiment, run: Run
    ) -> Union[None, str]:
        base_uri = self.client.tracking_uri
        if base_uri.startswith("http"):
            return f"{base_uri.rstrip('/')}/#/experiments/{experiment.experiment_id}/runs/{run.info.run_id}"
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

    def _is_valid_platform(self, platform: Optional[str]) -> bool:
        """Check if platform is registered as a source plugin"""
        return platform in KNOWN_VALID_PLATFORM_NAMES

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MLflowSource":
        config = MLflowConfig.parse_obj(config_dict)
        return cls(ctx, config)
