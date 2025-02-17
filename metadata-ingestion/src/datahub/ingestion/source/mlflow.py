import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union

from mlflow import MlflowClient
from mlflow.entities import Experiment, Run
from mlflow.entities.model_registry import ModelVersion, RegisteredModel
from mlflow.store.entities import PagedList
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
)
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
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
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    MLHyperParamClass,
    MLMetricClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    MLTrainingRunPropertiesClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    TimeStampClass,
    VersionPropertiesClass,
    VersionSetKeyClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import (
    DataPlatformUrn,
    VersionSetUrn,
)

T = TypeVar("T")


class ContainerKeyWithId(ContainerKey):
    id: str


@dataclass
class Container:
    key: ContainerKeyWithId
    subtype: str
    name: Optional[str] = None
    description: Optional[str] = None

    def generate_mcp(
        self,
    ) -> Iterable[Union[MetadataChangeProposalClass, MetadataChangeProposalWrapper]]:
        container_urn = self.key.as_urn()

        container_subtype = SubTypesClass(typeNames=[self.subtype])

        container_info = ContainerPropertiesClass(
            name=self.name or self.key.id,
            description=self.description,
            customProperties={},
        )

        browse_path = BrowsePathsV2Class(path=[])

        dpi = DataPlatformInstanceClass(
            platform=self.key.platform,
            instance=self.key.instance,
        )

        return MetadataChangeProposalWrapper.construct_many(
            entityUrn=container_urn,
            aspects=[container_subtype, container_info, browse_path, dpi],
        )


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


@dataclass
class MLflowEntityMap:
    """
    Maintains mappings between MLflow IDs and DataHub URNs during ingestion.
    """

    experiment_id_to_urn: Dict[str, str] = field(default_factory=dict)
    run_id_to_urn: Dict[str, str] = field(default_factory=dict)
    model_version_to_urn: Dict[str, str] = field(default_factory=dict)


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
        self.entity_map = MLflowEntityMap()
        self.graph = get_default_graph()

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._get_tags_workunits()
        yield from self._get_experiment_workunits()
        yield from self._get_ml_model_workunits()

    def _get_tags_workunits(self) -> Iterable[MetadataWorkUnit]:
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

    def _create_workunit(self, urn: str, aspect: Any) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        ).as_workunit()

    def _get_experiment_workunits(self) -> Iterable[MetadataWorkUnit]:
        experiments = self._get_mlflow_experiments()
        for experiment in experiments:
            for wu in self._get_experiment_container_workunit(experiment):
                yield wu

            runs = self._get_mlflow_runs_from_experiment(experiment)
            if runs:
                for run in runs:
                    for wu in self._get_run_workunits(experiment, run):
                        yield wu

    def _get_experiment_custom_properties(self, experiment):
        experiment_custom_props = getattr(experiment, "tags", {}) or {}
        experiment_custom_props.pop("mlflow.note.content", None)
        experiment_custom_props["artifacts_location"] = experiment.artifact_location
        return experiment_custom_props

    def _get_experiment_container_workunit(
        self, experiment: Experiment
    ) -> List[MetadataWorkUnit]:
        experiment_container = Container(
            key=ContainerKeyWithId(
                platform=str(DataPlatformUrn.create_from_id("mlflow")),
                id=experiment.name,
            ),
            subtype="ML Experiment",
            name=experiment.name,
            description=experiment.tags.get("mlflow.note.content"),
        )
        self.entity_map.experiment_id_to_urn[experiment.experiment_id] = (
            experiment_container.key.as_urn()
        )

        workunits = [mcp.as_workunit() for mcp in experiment_container.generate_mcp()]
        return workunits

    def _get_run_custom_properties(self, run: Run):
        custom_props = {}
        custom_props.update(getattr(run, "tags", {}) or {})
        return custom_props

    def _get_run_metrics(self, run: Run):
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.data.metrics.items()
        ]

    def _get_run_params(self, run: Run):
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.data.params.items()
        ]

    def _convert_run_result_type(
        self, status: str
    ) -> DataProcessInstanceRunResultClass:
        if status == "FINISHED":
            return DataProcessInstanceRunResultClass(
                type="SUCCESS", nativeResultType="mlflow"
            )
        elif status == "FAILED":
            return DataProcessInstanceRunResultClass(
                type="FAILURE", nativeResultType="mlflow"
            )
        else:
            return DataProcessInstanceRunResultClass(
                type="SKIPPED", nativeResultType="mlflow"
            )

    def _get_run_workunits(
        self, experiment: Experiment, run: Run
    ) -> List[MetadataWorkUnit]:
        experiment_key = ContainerKeyWithId(
            platform=str(DataPlatformUrn.create_from_id("mlflow")), id=experiment.name
        )

        dpi_id = run.info.run_name or run.info.run_id
        data_process_instance = DataProcessInstance(
            id=dpi_id,
            orchestrator="mlflow",
            template_urn=None,
        )

        self.entity_map.run_id_to_urn[run.info.run_id] = str(data_process_instance.urn)
        workunits = []

        run_custom_props = self._get_run_custom_properties(run)
        created_time = run.info.start_time or int(time.time() * 1000)
        created_actor = (
            f"urn:li:platformResource:{run.info.user_id}" if run.info.user_id else None
        )

        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=DataProcessInstancePropertiesClass(
                    name=run.info.run_name or run.info.run_id,
                    created=AuditStampClass(
                        time=created_time,
                        actor=created_actor,
                    ),
                    externalUrl=self._make_external_url_from_run(experiment, run),
                    customProperties=run_custom_props,
                ),
            ).as_workunit()
        )

        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=ContainerClass(container=experiment_key.as_urn()),
            ).as_workunit()
        )

        model_versions = self.get_mlflow_model_versions_from_run(run.info.run_id)
        if model_versions:
            model_version_urn = self._make_ml_model_urn(model_versions[0])
            workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(data_process_instance.urn),
                    aspect=DataProcessInstanceOutputClass(outputs=[model_version_urn]),
                ).as_workunit()
            )

        metrics = self._get_run_metrics(run)
        hyperparams = self._get_run_params(run)
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=MLTrainingRunPropertiesClass(
                    hyperParams=hyperparams,
                    trainingMetrics=metrics,
                    outputUrls=[run.info.artifact_uri],
                    id=run.info.run_id,
                ),
            ).as_workunit()
        )

        if run.info.end_time:
            duration_millis = run.info.end_time - run.info.start_time
            workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(data_process_instance.urn),
                    aspect=DataProcessInstanceRunEventClass(
                        status=DataProcessRunStatusClass.COMPLETE,
                        timestampMillis=run.info.end_time,
                        result=DataProcessInstanceRunResultClass(
                            type=self._convert_run_result_type(run.info.status).type,
                            nativeResultType="mlflow",
                        ),
                        durationMillis=duration_millis,
                    ),
                ).as_workunit()
            )

        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=DataPlatformInstanceClass(
                    platform=str(DataPlatformUrn.create_from_id("mlflow"))
                ),
            ).as_workunit()
        )

        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=SubTypesClass(typeNames=["ML Training Run"]),
            ).as_workunit()
        )

        return workunits

    def _get_mlflow_registered_models(self) -> Iterable[RegisteredModel]:
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

    def _get_mlflow_runs_from_experiment(self, experiment: Experiment) -> List[Run]:
        runs: List[Run] = self._traverse_mlflow_search_func(
            search_func=self.client.search_runs,
            experiment_ids=[experiment.experiment_id],
        )
        return runs

    @staticmethod
    def _traverse_mlflow_search_func(
        search_func: Callable[..., PagedList[T]],
        **kwargs: Any,
    ) -> Iterable[T]:
        next_page_token = None
        while True:
            paged_list = search_func(page_token=next_page_token, **kwargs)
            yield from paged_list
            next_page_token = paged_list.token
            if not next_page_token:
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
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        ml_model_group_properties = MLModelGroupPropertiesClass(
            customProperties=registered_model.tags,
            description=registered_model.description,
            created=TimeStampClass(
                time=registered_model.creation_timestamp, actor=None
            ),
            lastModified=TimeStampClass(
                time=registered_model.last_updated_timestamp,
                actor=None,
            ),
            version=VersionTagClass(
                versionTag=self._get_latest_version(registered_model)
            ),
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
        if model_version.run_id:
            run = self.client.get_run(model_version.run_id)
            return run
        else:
            return None

    class SequencedMetadataWorkUnit(MetadataWorkUnit):
        """A workunit that knows its dependencies"""

        def __init__(
            self,
            id: str,
            mcp: MetadataChangeProposalWrapper,
            depends_on: Optional[str] = None,
        ):
            super().__init__(id=id, mcp=mcp)
            self.depends_on = depends_on

    def _get_ml_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        registered_models = self._get_mlflow_registered_models()
        for registered_model in registered_models:
            yield self._get_ml_group_workunit(registered_model)
            model_versions = self._get_mlflow_model_versions(registered_model)
            for model_version in model_versions:
                run = self._get_mlflow_run(model_version)
                version_set_urn = self._get_version_set_urn(model_version)
                yield self._get_ml_model_properties_workunit(
                    registered_model=registered_model,
                    model_version=model_version,
                    run=run,
                )
                yield self._get_version_set(
                    version_set_urn=version_set_urn,
                )
                yield self._get_ml_model_version_properties_workunit(
                    model_version=model_version,
                    version_set_urn=version_set_urn,
                )
                # yield self._get_version_latest(
                #     model_version=model_version,
                #     version_set_urn=version_set_urn,
                # )
                yield self._get_global_tags_workunit(model_version=model_version)

    def _get_version_set_urn(
        self,
        model_version: ModelVersion,
    ) -> VersionSetUrn:
        version_set_urn = VersionSetUrn(
            id=f"{model_version.name}{self.config.model_name_separator}{model_version.version}",
            entity_type="mlModel",
        )

        return version_set_urn

    def _get_version_set(
        self,
        version_set_urn: VersionSetUrn,
    ) -> MetadataWorkUnit:
        version_set_key = VersionSetKeyClass(
            id=version_set_urn.id,
            entityType="mlModel",
        )

        wu = MetadataChangeProposalWrapper(
            entityUrn=str(version_set_urn),
            aspect=version_set_key,
        ).as_workunit()

        return wu

    def _get_version_latest(
        self, model_version: ModelVersion, version_set_urn: VersionSetUrn
    ) -> MetadataWorkUnit:
        ml_model_urn = self._make_ml_model_urn(model_version)
        version_set_properties = VersionSetPropertiesClass(
            latest=str(
                ml_model_urn
            ),  # TODO: this returns cannot set latest to unversioned entity
            versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",  # TODO: wait for change in the backend
        )

        wu = MetadataChangeProposalWrapper(
            entityUrn=str(version_set_urn),
            aspect=version_set_properties,
        ).as_workunit()

        return wu

    def _get_ml_model_version_properties_workunit(
        self,
        model_version: ModelVersion,
        version_set_urn: VersionSetUrn,
    ) -> List[MetadataWorkUnit]:
        ml_model_urn = self._make_ml_model_urn(model_version)

        # get mlmodel name from ml model urn
        ml_model_version_properties = VersionPropertiesClass(
            version=VersionTagClass(
                versionTag=str(model_version.version),
            ),
            versionSet=str(version_set_urn),
            sortId="AAAAAAAA",  # TODO: wait for change in the backend
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
    ) -> MetadataWorkUnit:
        ml_model_group_urn = self._make_ml_model_group_urn(registered_model)
        ml_model_urn = self._make_ml_model_urn(model_version)

        if run:
            # Use the same metrics and hyperparams from the run
            hyperparams = self._get_run_params(run)
            training_metrics = self._get_run_metrics(run)
            # TODO: this should be actually mapped the guid from the run id
            run_urn = self.entity_map.run_id_to_urn.get(run.info.run_id)
            training_jobs = [run_urn] if run_urn else []
        else:
            hyperparams = None
            training_metrics = None
            training_jobs = []

        created_time = model_version.creation_timestamp
        created_actor = (
            f"urn:li:platformResource:{model_version.user_id}"
            if model_version.user_id
            else None
        )

        ml_model_properties = MLModelPropertiesClass(
            customProperties=model_version.tags,
            lastModified=TimeStampClass(
                time=model_version.last_updated_timestamp,
                actor=None,
            ),
            externalUrl=self._make_external_url_from_model_version(model_version),
            description=model_version.description,
            created=TimeStampClass(
                time=created_time,
                actor=created_actor,
            ),
            hyperParams=hyperparams,
            trainingMetrics=training_metrics,
            tags=list(model_version.tags.keys()),
            groups=[str(ml_model_group_urn)],
            trainingJobs=training_jobs,
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

    def _make_external_url_from_model_version(
        self, model_version: ModelVersion
    ) -> Union[None, str]:
        base_uri = self.client.tracking_uri
        if base_uri.startswith("http"):
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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MLflowConfig.parse_obj(config_dict)
        return cls(ctx, config)
