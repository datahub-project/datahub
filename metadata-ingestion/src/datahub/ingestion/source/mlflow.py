import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, Union

from mlflow import MlflowClient
from mlflow.entities import Dataset as MlflowDataset, Experiment, Run
from mlflow.entities.model_registry import ModelVersion, RegisteredModel
from mlflow.exceptions import MlflowException
from mlflow.store.entities import PagedList
from pydantic.fields import Field

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
        yield from self._get_experiment_workunits()
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

    def _get_external_dataset_urn(self, platform: str, dataset_name: str) -> str:
        """
        Get the URN for an external dataset.
        Args:
            platform: The platform of the external dataset (e.g., 's3', 'bigquery')
            dataset: The MLflow dataset
        Returns:
            str: The URN of the external dataset
        """
        return str(DatasetUrn(platform=platform, name=dataset_name))

    def _get_dataset_input_workunits(self, run: Run) -> Iterable[MetadataWorkUnit]:
        """
        Generate workunits for dataset inputs in a run.

        For each dataset input:
        1. If source type is 'local' or 'code':
           - Create a local dataset reference
        2. Otherwise:
           - If materialization is enabled:
             - Create a hosted dataset and a dataset reference with upstream
           - If materialization is not enabled:
             - Create a dataset reference and add upstream if dataset exists
        3. Add all dataset references as upstreams for the run
        """
        run_urn = DataProcessInstance(
            id=run.info.run_id,
            orchestrator=self.platform,
        ).urn

        dataset_reference_urns = []

        for dataset_input in run.inputs.dataset_inputs:
            dataset = dataset_input.dataset
            source_type = dataset.source_type
            dataset_tags = {k[1]: v[1] for k, v in dataset_input.tags}

            # Prepare dataset properties
            custom_properties = dataset_tags
            formatted_schema = self._get_dataset_schema(dataset)
            if formatted_schema is None:
                custom_properties["schema"] = dataset.schema

            # Handle local/code datasets
            if source_type in ("local", "code"):
                local_dataset = Dataset(
                    platform=self.platform,
                    name=dataset.name,
                    schema=formatted_schema,
                    custom_properties=custom_properties,
                )
                yield from local_dataset.as_workunits()
                dataset_reference_urns.append(local_dataset.urn)
                continue

            # Handle hosted datasets
            formatted_platform = self._get_dataset_platform_from_source_type(
                source_type
            )

            # Validate platform if materialization is enabled
            if self.config.materialize_dataset_inputs:
                if not formatted_platform:
                    self.report.failure(
                        title="Unable to materialize dataset inputs",
                        message=f"No mapping dataPlatform found for dataset input source type '{source_type}'",
                        context=f"please add `materialize_dataset_inputs.source_mapping_to_platform` in config "
                        f"(e.g. '{source_type}': 'snowflake')",
                    )
                    continue
                # Create hosted dataset
                hosted_dataset = Dataset(
                    platform=formatted_platform,
                    name=dataset.name,
                    schema=formatted_schema,
                    custom_properties=dataset_tags,
                )
                yield from hosted_dataset.as_workunits()

            # Create dataset reference with upstream
            hosted_dataset_reference = Dataset(
                platform=self.platform,
                name=dataset.name,
                schema=formatted_schema,
                custom_properties=dataset_tags,
                upstreams=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            self._get_external_dataset_urn(
                                formatted_platform, dataset.name
                            ),
                            type="COPY",
                        )
                    ]
                )
                if formatted_platform
                else None,
            )
            dataset_reference_urns.append(hosted_dataset_reference.urn)
            yield from hosted_dataset_reference.as_workunits()

        # Add dataset references as upstreams for the run
        if dataset_reference_urns:
            input_edges = [
                EdgeClass(destinationUrn=str(dataset_ref_urn))
                for dataset_ref_urn in dataset_reference_urns
            ]
            yield MetadataChangeProposalWrapper(
                entityUrn=str(run_urn),
                aspect=DataProcessInstanceInputClass(inputs=[], inputEdges=input_edges),
            ).as_workunit()

    def _get_dataset_platform_from_source_type(self, source_type: str) -> Optional[str]:
        """
        Map MLflow source type to DataHub platform.

        Priority:
        1. User-provided mapping in config
        2. Internal mapping
        3. Direct platform match from list of supported platforms
        """
        source_type = source_type.lower()

        # User-provided mapping
        platform = self._get_platform_from_user_mapping(source_type)
        if platform:
            return platform

        # Internal mapping
        if source_type == "gs":
            return "gcs"

        # Check direct platform match
        if self._is_valid_platform(source_type):
            return source_type

        return None

    def _get_platform_from_user_mapping(self, source_type: str) -> Optional[str]:
        """
        Get platform from user-provided mapping in config.
        Returns None if mapping is invalid or platform is not supported.
        """
        source_mapping = self.config.source_mapping_to_platform
        if not source_mapping:
            return None

        platform = source_mapping.get(source_type)
        if not platform:
            return None

        return platform

    def _get_run_workunits(
        self, experiment: Experiment, run: Run
    ) -> Iterable[MetadataWorkUnit]:
        experiment_key = ExperimentKey(
            platform=str(DataPlatformUrn(self.platform)), id=experiment.name
        )

        data_process_instance = DataProcessInstance(
            id=run.info.run_id,
            orchestrator=self.platform,
            template_urn=None,
        )

        created_time = run.info.start_time or int(time.time() * 1000)
        user_id = run.info.user_id if run.info.user_id else "mlflow"
        guid_dict_user = {"platform": self.platform, "user": user_id}
        platform_user_urn = (
            f"urn:li:platformResource:{builder.datahub_guid(guid_dict_user)}"
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=platform_user_urn,
            aspect=PlatformResourceInfoClass(
                resourceType="user",
                primaryKey=user_id,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(data_process_instance.urn),
            aspect=DataProcessInstancePropertiesClass(
                name=run.info.run_name or run.info.run_id,
                created=AuditStampClass(
                    time=created_time,
                    actor=platform_user_urn,
                ),
                externalUrl=self._make_external_url_from_run(experiment, run),
                customProperties=getattr(run, "tags", {}) or {},
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(data_process_instance.urn),
            aspect=ContainerClass(container=experiment_key.as_urn()),
        ).as_workunit()

        model_versions = self.get_mlflow_model_versions_from_run(run.info.run_id)
        if model_versions:
            model_version_urn = self._make_ml_model_urn(model_versions[0])
            yield MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=[
                        EdgeClass(destinationUrn=model_version_urn),
                    ],
                ),
            ).as_workunit()

        metrics = self._get_run_metrics(run)
        hyperparams = self._get_run_params(run)
        yield MetadataChangeProposalWrapper(
            entityUrn=str(data_process_instance.urn),
            aspect=MLTrainingRunPropertiesClass(
                hyperParams=hyperparams,
                trainingMetrics=metrics,
                outputUrls=[run.info.artifact_uri],
                id=run.info.run_id,
            ),
        ).as_workunit()

        if run.info.end_time:
            duration_millis = run.info.end_time - run.info.start_time

            yield MetadataChangeProposalWrapper(
                entityUrn=str(data_process_instance.urn),
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=run.info.end_time,
                    result=DataProcessInstanceRunResultClass(
                        type=self._convert_run_result_type(run.info.status).type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=duration_millis,
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(data_process_instance.urn),
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(self.platform))
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(data_process_instance.urn),
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.MLFLOW_TRAINING_RUN]),
        ).as_workunit()

    def _get_mlflow_registered_models(self) -> Iterable[RegisteredModel]:
        """
        Get all Registered Models in MLflow Model Registry.
        """
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

    def _traverse_mlflow_search_func(
        self,
        search_func: Callable[..., PagedList[T]],
        **kwargs: Any,
    ) -> Iterable[T]:
        """
        Utility to traverse an MLflow search_* functions which return PagedList.
        """
        next_page_token = None
        try:
            while True:
                paged_list = search_func(page_token=next_page_token, **kwargs)
                yield from paged_list.to_list()
                next_page_token = paged_list.token
                if not next_page_token:
                    return
        except MlflowException as e:
            if e.error_code == "ENDPOINT_NOT_FOUND":
                self.report.warning(
                    title="MLflow API Endpoint Not Found for Experiments.",
                    message="Please upgrade to version 1.28.0 or higher to ensure compatibility. Skipping ingestion for experiments and runs.",
                    context=None,
                    exc=e,
                )
                return
            else:
                raise  # Only re-raise other exceptions

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
                versionTag=self._get_latest_version(registered_model),
                metadataAttribution=MetadataAttributionClass(
                    time=registered_model.last_updated_timestamp,
                    actor="urn:li:corpuser:datahub",
                ),
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
        registered_models = self._get_mlflow_registered_models()
        for registered_model in registered_models:
            version_set_urn = self._get_version_set_urn(registered_model)
            yield self._get_ml_group_workunit(registered_model)
            model_versions = self._get_mlflow_model_versions(registered_model)
            for model_version in model_versions:
                run = self._get_mlflow_run(model_version)
                yield self._get_ml_model_properties_workunit(
                    registered_model=registered_model,
                    model_version=model_version,
                    run=run,
                )
                yield self._get_ml_model_version_properties_workunit(
                    model_version=model_version,
                    version_set_urn=version_set_urn,
                )
                yield self._get_global_tags_workunit(model_version=model_version)

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
    ) -> MetadataWorkUnit:
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
            training_jobs = []

        created_time = model_version.creation_timestamp
        created_actor = (
            f"urn:li:platformResource:{model_version.user_id}"
            if model_version.user_id
            else None
        )
        model_version_tags = [f"{k}:{v}" for k, v in model_version.tags.items()]

        ml_model_properties = MLModelPropertiesClass(
            customProperties=model_version.tags,
            externalUrl=self._make_external_url(model_version),
            lastModified=TimeStampClass(
                time=model_version.last_updated_timestamp,
                actor=None,
            ),
            description=model_version.description,
            created=TimeStampClass(
                time=created_time,
                actor=created_actor,
            ),
            hyperParams=hyperparams,
            trainingMetrics=training_metrics,
            tags=model_version_tags,
            groups=[ml_model_group_urn],
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
