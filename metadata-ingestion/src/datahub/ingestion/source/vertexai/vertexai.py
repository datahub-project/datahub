import logging
from datetime import timedelta
from numbers import Real
from typing import Dict, Iterable, List, Optional, TypeVar, Union

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform, aiplatform_v1
from google.cloud.aiplatform import (
    AutoMLForecastingTrainingJob,
    AutoMLImageTrainingJob,
    AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob,
    AutoMLVideoTrainingJob,
    Endpoint,
    ExperimentRun,
    ModelEvaluation,
    PipelineJob,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1 import MetadataServiceClient
from google.cloud.aiplatform_v1.types import (
    Execution,
    ListExecutionsRequest,
    PipelineJob as PipelineJobType,
    PipelineTaskDetail,
    QueryExecutionInputsAndOutputsRequest,
)
from google.oauth2 import service_account

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ExperimentKey,
    ProjectIdKey,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    resolve_gcp_projects,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.vertexai.ml_metadata_helper import MLMetadataHelper
from datahub.ingestion.source.vertexai.protobuf_utils import (
    extract_numeric_value,
    extract_protobuf_value,
)
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    DATAHUB_ACTOR,
    DatasetTypes,
    DateTimeFormat,
    DurationUnit,
    ExternalURLs,
    HyperparameterPatterns,
    LabelFormat,
    MetricPatterns,
    MLMetadataDefaults,
    MLMetadataSchemas,
    MLModelType,
    ResourceCategory,
    TrainingJobTypes,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ArtifactURNs,
    AutoMLJobConfig,
    DatasetCustomProperties,
    EndpointDeploymentCustomProperties,
    LineageMetadata,
    MLMetadataConfig,
    MLModelCustomProperties,
    ModelEvaluationCustomProperties,
    ModelGroupKey,
    ModelMetadata,
    PipelineMetadata,
    PipelineProperties,
    PipelineTaskArtifacts,
    PipelineTaskMetadata,
    PipelineTaskProperties,
    RunTimestamps,
    TrainingJobCustomProperties,
    TrainingJobMetadata,
    VertexAIResourceCategoryKey,
)
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_execution_result_status,
    get_job_result_status,
    get_pipeline_task_result_status,
    is_status_for_run_event_class,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceRelationships,
)
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLTrainingRunProperties,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BaseDataClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DatasetPropertiesClass,
    EdgeClass,
    MetadataAttributionClass,
    MLHyperParamClass,
    MLMetricClass,
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    MLTrainingRunPropertiesClass,
    RunResultTypeClass,
    SubTypesClass,
    TimeStampClass,
    TrainingDataClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import (
    DataFlowUrn,
    DataJobUrn,
    DataPlatformUrn,
    MlModelUrn,
    VersionSetUrn,
)
from datahub.utilities.time import datetime_to_ts_millis
from datahub.utilities.urns.dataset_urn import DatasetUrn

T = TypeVar("T")

logger = logging.getLogger(__name__)


def log_progress(
    current: int, total: Optional[int], item_type: str, interval: int = 100
) -> None:
    """Log progress for large collections at regular intervals."""
    if current % interval == 0:
        logger.info(f"Processed {current} {item_type} from VertexAI server")
    if total and current == total:
        logger.info(f"Finished processing {total} {item_type} from VertexAI server")


@platform_name("Vertex AI", id="vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for Vertex AI Registered Models and Model Versions",
)
class VertexAISource(StatefulIngestionSourceBase):
    platform: str = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: StaleEntityRemovalSourceReport = StaleEntityRemovalSourceReport()
        self._model_to_downstream_jobs: Dict[str, List[str]] = {}

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )
        self._credentials = credentials

        # Determine target projects
        self._projects: List[str]
        wants_multi_project = bool(
            config.project_ids or config.project_labels or not config.project_id
        )
        if wants_multi_project:
            filter_cfg = GcpProjectFilterConfig(
                project_ids=config.project_ids,
                project_labels=config.project_labels,
                project_id_pattern=config.project_id_pattern,
            )
            resolved_projects = resolve_gcp_projects(filter_cfg, self.report)
            self._projects = [p.id for p in resolved_projects]
            if not self._projects and config.project_id:
                self._projects = [config.project_id]
        else:
            self._projects = [config.project_id]

        self._project_to_regions: Dict[str, List[str]] = {}
        if self.config.discover_regions:
            for project_id in self._projects:
                regions = self._discover_regions_for_project(project_id)
                self._project_to_regions[project_id] = (
                    regions if regions else [self.config.region]
                )
        else:
            regions_from_config = (
                self.config.regions
                if self.config.regions
                else ([self.config.region] if self.config.region else [])
            )
            for project_id in self._projects:
                self._project_to_regions[project_id] = regions_from_config

        # dynamic context for current project/region during iteration
        self._current_project_id: Optional[str] = None
        self._current_region: Optional[str] = None

        self.client = aiplatform
        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None
        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None
        self.experiments: Optional[List[Experiment]] = None

        self._metadata_client: Optional[MetadataServiceClient] = None
        self._ml_metadata_helper: Optional[MLMetadataHelper] = None

        self.urn_builder = VertexAIUrnBuilder(
            platform=self.platform,
            env=self.config.env,
            project_id=self._get_project_id(),
            platform_instance=self.config.platform_instance,
        )
        self.name_formatter = VertexAINameFormatter(project_id=self._get_project_id())
        self.url_builder = VertexAIExternalURLBuilder(
            base_url=self.config.vertexai_url or ExternalURLs.BASE_URL,
            project_id=self._get_project_id(),
            region=self._get_region(),
        )
        self.uri_parser = VertexAIURIParser(
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            platform_to_instance_map=self.config.platform_to_instance_map,
        )

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _track_model_usage(self, model_urn: str, job_urn: str) -> None:
        """Track that a job uses a model as input, for downstream lineage."""
        if model_urn not in self._model_to_downstream_jobs:
            self._model_to_downstream_jobs[model_urn] = []
        if job_urn not in self._model_to_downstream_jobs[model_urn]:
            self._model_to_downstream_jobs[model_urn].append(job_urn)

    def _yield_common_aspects(
        self,
        entity_urn: str,
        subtype: str,
        include_container: bool = True,
        include_platform: bool = True,
        resource_category: Optional[str] = None,
        include_subtypes: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        if include_container:
            if resource_category:
                container_urn = self._get_resource_category_container(
                    resource_category
                ).as_urn()
            else:
                container_urn = self._get_project_container().as_urn()

            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=ContainerClass(container=container_urn),
            ).as_workunit()

        if include_subtypes:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=SubTypesClass(typeNames=[subtype]),
            ).as_workunit()

        if include_platform:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataPlatformInstanceClass(
                    platform=DataPlatformUrn(self.platform).urn(),
                    instance=self.config.platform_instance,
                ),
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main Function to fetch and yields mcps for various VertexAI resources.
        - Models and Model Versions from the Model Registry
        - Training Jobs
        """
        for project_id in self._projects:
            regions = self._project_to_regions.get(project_id, [self._get_region()])
            for region in regions:
                logger.info(
                    f"Initializing Vertex AI client for project={project_id} region={region}"
                )
                aiplatform.init(
                    project=project_id,
                    location=region,
                    credentials=self._credentials,
                )
                self._current_project_id = project_id
                self._current_region = region
                self.endpoints = None
                self.datasets = None
                self.experiments = None

                if (
                    self.config.use_ml_metadata_for_lineage
                    or self.config.extract_execution_metrics
                ):
                    try:
                        self._metadata_client = MetadataServiceClient(
                            client_options={
                                "api_endpoint": f"{region}-aiplatform.googleapis.com"
                            },
                            credentials=self._credentials,
                        )

                        ml_metadata_config = MLMetadataConfig(
                            project_id=project_id,
                            region=region,
                            metadata_store=MLMetadataDefaults.DEFAULT_METADATA_STORE,
                            enable_lineage_extraction=self.config.use_ml_metadata_for_lineage,
                            enable_metrics_extraction=self.config.extract_execution_metrics,
                            max_execution_search_limit=self.config.ml_metadata_max_execution_search_limit,
                        )

                        self._ml_metadata_helper = MLMetadataHelper(
                            metadata_client=self._metadata_client,
                            config=ml_metadata_config,
                            dataset_urn_converter=self.uri_parser.dataset_urns_from_artifact_uri,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to initialize ML Metadata client for project={project_id} region={region}: {e}"
                        )
                        self._metadata_client = None
                        self._ml_metadata_helper = None

                # Ingest Project
                yield from self._gen_project_workunits()

                # Process jobs/pipelines/experiments FIRST to track model usage
                # (for downstream lineage)
                if self.config.include_training_jobs:
                    yield from auto_workunit(self._get_training_jobs_mcps())

                if self.config.include_experiments:
                    yield from self._get_experiments_workunits()
                    yield from auto_workunit(self._get_experiment_runs_mcps())

                if self.config.include_pipelines:
                    yield from auto_workunit(self._get_pipelines_mcps())

                # Process models AFTER jobs so we can add downstream lineage
                if self.config.include_models:
                    yield from auto_workunit(self._get_ml_models_mcps())

                if self.config.include_evaluations:
                    yield from auto_workunit(self._get_model_evaluations_mcps())

    def _get_pipelines_mcps(self) -> Iterable[MetadataWorkUnit]:
        """Fetches pipelines from Vertex AI and generates corresponding mcps."""
        pipeline_jobs = list(self.client.PipelineJob.list(order_by="update_time desc"))

        total = len(pipeline_jobs)
        for i, pipeline in enumerate(pipeline_jobs, start=1):
            log_progress(i, total, "pipelines")
            logger.debug(f"Fetching pipeline ({pipeline.name})")
            pipeline_meta = self._get_pipeline_metadata(pipeline)
            yield from self._get_pipeline_mcps(pipeline_meta)
            yield from self._gen_pipeline_task_mcps(pipeline_meta)

    def _extract_pipeline_task_inputs(
        self, task_detail: PipelineTaskDetail, task_name: str, task_urn: str
    ) -> Optional[List[str]]:
        """Extract input dataset and model URNs from pipeline task artifacts."""
        if not task_detail.inputs:
            return None

        input_urns = []
        try:
            for input_entry in task_detail.inputs.values():
                if input_entry.artifacts:
                    for artifact in input_entry.artifacts:
                        if artifact.uri:
                            # Try to extract as model first
                            model_urn = self.uri_parser.model_urn_from_artifact_uri(
                                artifact.uri
                            )
                            if model_urn:
                                input_urns.append(model_urn)
                                self._track_model_usage(model_urn, task_urn)
                            else:
                                # If not a model, try as dataset
                                input_urns.extend(
                                    self.uri_parser.dataset_urns_from_artifact_uri(
                                        artifact.uri
                                    )
                                )
            return input_urns if input_urns else None
        except Exception as e:
            logger.debug(
                f"Failed to extract input artifacts for task {task_name}: {e}",
                exc_info=True,
            )
            return None

    def _extract_pipeline_task_outputs(
        self, task_detail: PipelineTaskDetail, task_name: str
    ) -> PipelineTaskArtifacts:
        """Extract output dataset and model URNs from pipeline task artifacts.

        Returns:
            PipelineTaskArtifacts with output_dataset_urns and output_model_urns populated
        """
        if not task_detail.outputs:
            return PipelineTaskArtifacts()

        output_dataset_urns = []
        output_model_urns = []
        try:
            for output_entry in task_detail.outputs.values():
                if output_entry.artifacts:
                    for artifact in output_entry.artifacts:
                        if artifact.uri:
                            # Try to extract as model first
                            model_urn = self.uri_parser.model_urn_from_artifact_uri(
                                artifact.uri
                            )
                            if model_urn:
                                output_model_urns.append(model_urn)
                            else:
                                # If not a model, try as dataset
                                output_dataset_urns.extend(
                                    self.uri_parser.dataset_urns_from_artifact_uri(
                                        artifact.uri
                                    )
                                )
            return PipelineTaskArtifacts(
                output_dataset_urns=output_dataset_urns
                if output_dataset_urns
                else None,
                output_model_urns=output_model_urns if output_model_urns else None,
            )
        except Exception as e:
            logger.debug(
                f"Failed to extract output artifacts for task {task_name}: {e}",
                exc_info=True,
            )
            return PipelineTaskArtifacts()

    def _get_pipeline_tasks_metadata(
        self, pipeline: PipelineJob, pipeline_urn: DataFlowUrn
    ) -> List[PipelineTaskMetadata]:
        tasks: List[PipelineTaskMetadata] = list()
        task_map: Dict[str, PipelineTaskDetail] = dict()
        for task in pipeline.task_details:
            task_map[task.task_name] = task

        resource = pipeline.gca_resource
        if isinstance(resource, PipelineJobType):
            for task_name in resource.pipeline_spec["root"]["dag"]["tasks"]:
                logger.debug(
                    f"fetching pipeline task ({task_name}) in pipeline ({pipeline.name})"
                )
                task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=pipeline_urn.urn(),
                    job_id=self.name_formatter.format_pipeline_task_id(task_name),
                )
                task_meta = PipelineTaskMetadata(name=task_name, urn=task_urn)

                # Create DPI URN for downstream tracking (same as used in _gen_pipeline_task_run_mcps)
                dpi_urn = builder.make_data_process_instance_urn(
                    self.name_formatter.format_pipeline_task_run_id(entity_id=task_name)
                )
                if (
                    "dependentTasks"
                    in resource.pipeline_spec["root"]["dag"]["tasks"][task_name]
                ):
                    upstream_tasks = resource.pipeline_spec["root"]["dag"]["tasks"][
                        task_name
                    ]["dependentTasks"]
                    upstream_urls = [
                        DataJobUrn.create_from_ids(
                            data_flow_urn=pipeline_urn.urn(),
                            job_id=self.name_formatter.format_pipeline_task_id(
                                upstream_task
                            ),
                        )
                        for upstream_task in upstream_tasks
                    ]
                    task_meta.upstreams = upstream_urls

                task_detail = task_map.get(task_name)
                if task_detail:
                    task_meta.id = task_detail.task_id
                    task_meta.state = task_detail.state
                    task_meta.start_time = task_detail.start_time
                    task_meta.create_time = task_detail.create_time
                    if task_detail.end_time and task_meta.start_time:
                        task_meta.end_time = task_detail.end_time
                        if task_meta.start_time and task_meta.end_time:
                            task_meta.duration = int(
                                (
                                    task_meta.end_time.timestamp()
                                    - task_meta.start_time.timestamp()
                                )
                                * 1000
                            )

                    task_meta.input_dataset_urns = self._extract_pipeline_task_inputs(
                        task_detail, task_name, dpi_urn
                    )
                    outputs = self._extract_pipeline_task_outputs(
                        task_detail, task_name
                    )
                    task_meta.output_dataset_urns = outputs.output_dataset_urns
                    task_meta.output_model_urns = outputs.output_model_urns

                tasks.append(task_meta)
        return tasks

    def _get_pipeline_metadata(self, pipeline: PipelineJob) -> PipelineMetadata:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            env=self.config.env,
            flow_id=self.name_formatter.format_pipeline_id(pipeline.name),
            platform_instance=self.config.platform_instance,
        )
        tasks = self._get_pipeline_tasks_metadata(
            pipeline=pipeline, pipeline_urn=dataflow_urn
        )

        pipeline_meta = PipelineMetadata(
            name=pipeline.name,
            resource_name=pipeline.resource_name,
            urn=dataflow_urn,
            tasks=tasks,
        )
        pipeline_meta.resource_name = pipeline.resource_name
        pipeline_meta.labels = pipeline.labels
        pipeline_meta.create_time = pipeline.create_time
        pipeline_meta.region = pipeline.location
        if pipeline.update_time:
            pipeline_meta.update_time = pipeline.update_time
            pipeline_meta.duration = timedelta(
                milliseconds=datetime_to_ts_millis(pipeline.update_time)
                - datetime_to_ts_millis(pipeline.create_time)
            )
        return pipeline_meta

    def _gen_pipeline_task_run_mcps(
        self, task: PipelineTaskMetadata, datajob: DataJob, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        dpi_urn = builder.make_data_process_instance_urn(
            self.name_formatter.format_pipeline_task_run_id(entity_id=task.name)
        )
        result_status: Union[str, RunResultTypeClass] = get_pipeline_task_result_status(
            task.state
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=task.name,
                created=AuditStampClass(
                    time=(
                        int(task.create_time.timestamp() * 1000)
                        if task.create_time
                        else 0
                    ),
                    actor=self._get_actor_from_labels(pipeline.labels) or DATAHUB_ACTOR,
                ),
                externalUrl=self.url_builder.make_pipeline_url(pipeline.name),
                customProperties={},
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dpi_urn,
            subtype=VertexAISubTypes.PIPELINE_TASK_RUN,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[], parentTemplate=datajob.urn.urn()
            ),
        ).as_workunit()

        if is_status_for_run_event_class(result_status) and task.duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=(
                        int(task.create_time.timestamp() * 1000)
                        if task.create_time
                        else 0
                    ),
                    result=DataProcessInstanceRunResultClass(
                        type=result_status,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=task.duration,
                ),
            ).as_workunit()

        input_edges = []
        if task.input_dataset_urns:
            input_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.input_dataset_urns]
            )

        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

        output_edges = []
        if task.output_dataset_urns:
            output_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.output_dataset_urns]
            )
        if task.output_model_urns:
            output_edges.extend(
                [EdgeClass(destinationUrn=urn) for urn in task.output_model_urns]
            )

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_pipeline_task_mcps(
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        dataflow_urn = pipeline.urn

        for task in pipeline.tasks:
            # DataJob inlets/outlets only support dataset URNs (not models)
            inlets = (
                [
                    DatasetUrn.from_string(urn)
                    for urn in task.input_dataset_urns
                    if ":dataset:" in urn
                ]
                if task.input_dataset_urns
                else []
            )
            outlets = (
                [
                    DatasetUrn.from_string(urn)
                    for urn in task.output_dataset_urns
                    if ":dataset:" in urn
                ]
                if task.output_dataset_urns
                else []
            )

            owner = self._get_actor_from_labels(pipeline.labels)
            datajob = DataJob(
                id=self.name_formatter.format_pipeline_task_id(task.name),
                flow_urn=dataflow_urn,
                name=task.name,
                properties={},
                owners={owner} if owner else set(),
                upstream_urns=task.upstreams if task.upstreams else [],
                inlets=inlets,
                outlets=outlets,
                url=self.url_builder.make_pipeline_url(pipeline.name),
                platform_instance=self.config.platform_instance,
            )

            # DataJobs get container pointing to their parent DataFlow
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob.urn.urn(),
                aspect=ContainerClass(container=str(dataflow_urn)),
            ).as_workunit()

            yield from self._yield_common_aspects(
                entity_urn=datajob.urn.urn(),
                subtype=VertexAISubTypes.PIPELINE_TASK,
                include_platform=False,
                include_container=False,
            )
            for mcp in datajob.generate_mcp():
                yield mcp.as_workunit()
            yield from self._gen_pipeline_task_run_mcps(task, datajob, pipeline)

    def _format_pipeline_duration(self, td: timedelta) -> str:
        days = td.days
        hours, remainder = divmod(td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = td.microseconds // 1000

        parts = []
        if days:
            parts.append(f"{days}{DurationUnit.DAYS}")
        if hours:
            parts.append(f"{hours}{DurationUnit.HOURS}")
        if minutes:
            parts.append(f"{minutes}{DurationUnit.MINUTES}")
        if seconds:
            parts.append(f"{seconds}{DurationUnit.SECONDS}")
        if milliseconds:
            parts.append(f"{milliseconds}{DurationUnit.MILLISECONDS}")
        return " ".join(parts) if parts else f"0{DurationUnit.SECONDS}"

    def _get_pipeline_task_properties(
        self, task: PipelineTaskMetadata
    ) -> PipelineTaskProperties:
        return PipelineTaskProperties(
            created_time=(
                task.create_time.strftime(DateTimeFormat.TIMESTAMP)
                if task.create_time
                else ""
            )
        )

    def _get_pipeline_properties(
        self, pipeline: PipelineMetadata
    ) -> PipelineProperties:
        return PipelineProperties(
            resource_name=pipeline.resource_name if pipeline.resource_name else "",
            create_time=(
                pipeline.create_time.isoformat() if pipeline.create_time else ""
            ),
            update_time=(
                pipeline.update_time.isoformat() if pipeline.update_time else ""
            ),
            duration=(
                self._format_pipeline_duration(pipeline.duration)
                if pipeline.duration
                else ""
            ),
            location=(pipeline.region if pipeline.region else ""),
            labels=LabelFormat.ITEM_SEPARATOR.join(
                [
                    f"{k}{LabelFormat.KEY_VALUE_SEPARATOR}{v}"
                    for k, v in pipeline.labels.items()
                ]
            )
            if pipeline.labels
            else "",
        )

    def _get_pipeline_mcps(
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataWorkUnit]:
        owner = self._get_actor_from_labels(pipeline.labels)
        dataflow = DataFlow(
            orchestrator=self.platform,
            id=self.name_formatter.format_pipeline_id(pipeline.name),
            env=self.config.env,
            name=pipeline.name,
            platform_instance=self.config.platform_instance,
            properties=self._get_pipeline_properties(pipeline).to_custom_properties(),
            owners={owner} if owner else set(),
            url=self.url_builder.make_pipeline_url(pipeline_name=pipeline.name),
        )

        for mcp in dataflow.generate_mcp():
            yield mcp.as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=dataflow.urn.urn(),
            subtype=VertexAISubTypes.PIPELINE,
            include_platform=False,
            resource_category=ResourceCategory.PIPELINES,
        )

    def _get_experiments_workunits(self) -> Iterable[MetadataWorkUnit]:
        exps = aiplatform.Experiment.list()
        filtered = [
            e for e in exps if self.config.experiment_name_pattern.allowed(e.name)
        ]
        filtered.sort(key=lambda x: x.update_time, reverse=True)  # type: ignore[attr-defined]
        if self.config.max_experiments is not None:
            filtered = filtered[: self.config.max_experiments]
        self.experiments = filtered

        logger.info("Fetching experiments from VertexAI server")
        total = len(self.experiments)
        for i, experiment in enumerate(self.experiments, start=1):
            log_progress(i, total, "experiments")
            yield from self._gen_experiment_workunits(experiment)

    def _get_experiment_runs_mcps(self) -> Iterable[MetadataWorkUnit]:
        if self.experiments is None:
            exps = [
                e
                for e in aiplatform.Experiment.list()
                if self.config.experiment_name_pattern.allowed(e.name)
            ]
            exps.sort(key=lambda x: x.update_time, reverse=True)  # type: ignore[attr-defined]
            self.experiments = exps
        for experiment in self.experiments:
            logger.info(f"Fetching experiment runs for experiment {experiment.name}")
            experiment_runs = list(
                aiplatform.ExperimentRun.list(experiment=experiment.name)
            )
            experiment_runs.sort(key=lambda x: x.update_time, reverse=True)  # type: ignore[attr-defined]
            if self.config.max_runs_per_experiment is not None:
                experiment_runs = experiment_runs[: self.config.max_runs_per_experiment]
            for i, run in enumerate(experiment_runs, start=1):
                log_progress(i, None, f"runs for experiment {experiment.name}")
                yield from self._gen_experiment_run_mcps(experiment, run)

    def _gen_experiment_workunits(
        self, experiment: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            parent_container_key=self._get_project_container(),
            container_key=ExperimentKey(
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
                id=self.name_formatter.format_experiment_id(experiment.name),
            ),
            name=experiment.name,
            sub_types=[VertexAISubTypes.EXPERIMENT],
            extra_properties={
                "name": experiment.name,
                "resourceName": experiment.resource_name,
                "dashboardURL": experiment.dashboard_url
                if experiment.dashboard_url
                else "",
            },
            external_url=self.url_builder.make_experiment_url(experiment.name),
        )

    def _get_experiment_run_params(self, run: ExperimentRun) -> List[MLHyperParamClass]:
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.get_params().items()
        ]

    def _get_experiment_run_metrics(self, run: ExperimentRun) -> List[MLMetricClass]:
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.get_metrics().items()
        ]

    def _get_run_timestamps(self, run: ExperimentRun) -> RunTimestamps:
        executions = run.get_executions()
        if len(executions) == 1:
            create_time = executions[0].create_time
            update_time = executions[0].update_time
            if create_time and update_time:
                duration = (
                    update_time.timestamp() * 1000 - create_time.timestamp() * 1000
                )
                return RunTimestamps(
                    created_time_ms=int(create_time.timestamp() * 1000),
                    duration_ms=int(duration),
                )
        # When no execution context started, start time and duration are not available
        # When multiple execution contexts started on a run, unable to know which context to use for create_time and duration
        return RunTimestamps()

    def _get_run_result_status(self, status: str) -> Union[str, RunResultTypeClass]:
        if status == "COMPLETE":
            return RunResultTypeClass.SUCCESS
        elif status == "FAILED":
            return RunResultTypeClass.FAILURE
        elif status == "RUNNING":  # No Corresponding RunResultTypeClass for RUNNING
            return "RUNNING"
        else:
            return "UNKNOWN"

    def _make_custom_properties_for_run(
        self, experiment: Experiment, run: ExperimentRun
    ) -> dict:
        properties: Dict[str, str] = dict()
        properties["externalUrl"] = self.url_builder.make_experiment_run_url(
            experiment, run
        )
        for exec in run.get_executions():
            exec_name = exec.name
            properties[f"created time ({exec_name})"] = str(exec.create_time)
            properties[f"update time ({exec_name}) "] = str(exec.update_time)
        return properties

    def _make_custom_properties_for_execution(self, execution: Execution) -> dict:
        properties: Dict[str, Optional[str]] = dict()
        for input in execution.get_input_artifacts():
            input_name = getattr(input, "name", "")
            properties[f"input artifact ({input_name})"] = getattr(input, "uri", "")
        for output in execution.get_output_artifacts():
            output_name = getattr(output, "name", "")
            properties[f"output artifact ({output_name})"] = getattr(output, "uri", "")

        return properties

    def _gen_run_execution(
        self, execution: Execution, run: ExperimentRun, exp: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        create_time = execution.create_time
        update_time = execution.update_time
        duration: Optional[int] = None
        if create_time and update_time:
            duration = datetime_to_ts_millis(update_time) - datetime_to_ts_millis(
                create_time
            )
        result_status: Union[str, RunResultTypeClass] = get_execution_result_status(
            execution.state
        )
        execution_urn = builder.make_data_process_instance_urn(
            self.name_formatter.format_run_execution_name(execution.name)
        )

        input_edges = []
        output_edges = []
        for input_art in execution.get_input_artifacts():
            for ds_urn in self.uri_parser.dataset_urns_from_artifact_uri(input_art.uri):
                input_edges.append(EdgeClass(destinationUrn=ds_urn))
            # Track model inputs for downstream lineage
            model_urn = self.uri_parser.model_urn_from_artifact_uri(input_art.uri)
            if model_urn:
                input_edges.append(EdgeClass(destinationUrn=model_urn))
                self._track_model_usage(model_urn, execution_urn)
        for output_art in execution.get_output_artifacts():
            for ds_urn in self.uri_parser.dataset_urns_from_artifact_uri(
                output_art.uri
            ):
                output_edges.append(EdgeClass(destinationUrn=ds_urn))
            model_urn = self.uri_parser.model_urn_from_artifact_uri(output_art.uri)
            if model_urn:
                output_edges.append(EdgeClass(destinationUrn=model_urn))

        yield MetadataChangeProposalWrapper(
            entityUrn=execution_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=execution.name,
                created=AuditStampClass(
                    time=datetime_to_ts_millis(create_time) if create_time else 0,
                    actor=DATAHUB_ACTOR,
                ),
                externalUrl=self.url_builder.make_artifact_url(experiment=exp, run=run),
                customProperties=self._make_custom_properties_for_execution(execution),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=execution_urn,
            subtype=VertexAISubTypes.EXECUTION,
            include_container=False,
        )

        if is_status_for_run_event_class(result_status) and duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=datetime_to_ts_millis(create_time)
                    if create_time
                    else 0,
                    result=DataProcessInstanceRunResultClass(
                        type=result_status,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=int(duration),
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=execution_urn,
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[self.urn_builder.make_experiment_run_urn(exp, run)],
                parentInstance=self.urn_builder.make_experiment_run_urn(exp, run),
            ),
        ).as_workunit()

        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

        if output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=execution_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=output_edges,
                ),
            ).as_workunit()

    def _gen_experiment_run_mcps(
        self, experiment: Experiment, run: ExperimentRun
    ) -> Iterable[MetadataWorkUnit]:
        experiment_key = ExperimentKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            id=self.name_formatter.format_experiment_id(experiment.name),
        )
        run_urn = self.urn_builder.make_experiment_run_urn(experiment, run)
        timestamps = self._get_run_timestamps(run)
        run_result_type = self._get_run_result_status(run.get_state())

        for execution in run.get_executions():
            yield from self._gen_run_execution(
                execution=execution, exp=experiment, run=run
            )

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=run.name,
                created=AuditStampClass(
                    time=timestamps.created_time_ms
                    if timestamps.created_time_ms
                    else 0,
                    actor=DATAHUB_ACTOR,
                ),
                externalUrl=self.url_builder.make_experiment_run_url(experiment, run),
                customProperties=self._make_custom_properties_for_run(experiment, run),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=ContainerClass(container=experiment_key.as_urn()),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=run_urn,
            aspect=MLTrainingRunPropertiesClass(
                hyperParams=self._get_experiment_run_params(run),
                trainingMetrics=self._get_experiment_run_metrics(run),
                externalUrl=self.url_builder.make_experiment_run_url(experiment, run),
                id=f"{experiment.name}-{run.name}",
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=run_urn,
            subtype=VertexAISubTypes.EXPERIMENT_RUN,
            include_container=False,
        )

        if is_status_for_run_event_class(run_result_type):
            yield MetadataChangeProposalWrapper(
                entityUrn=run_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=timestamps.created_time_ms
                    if timestamps.created_time_ms
                    else 0,  # None is not allowed, 0 as default value
                    result=DataProcessInstanceRunResultClass(
                        type=run_result_type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=timestamps.duration_ms
                    if timestamps.duration_ms
                    else None,
                ),
            ).as_workunit()

    def _gen_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._get_project_container(),
            name=self._get_project_id(),
            sub_types=[VertexAISubTypes.PROJECT],
        )

        yield from self._generate_resource_category_containers()

    def _get_ml_models_mcps(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch List of Models in Model Registry and generate a corresponding mcp.
        """
        registered_models = self.client.Model.list(order_by="update_time desc")
        total_versions = 0
        for model_idx, model in enumerate(registered_models, start=1):
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue
            yield from self._gen_ml_group_mcps(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                total_versions += 1
                if total_versions % 100 == 0:
                    logger.info(
                        f"Processed {total_versions} model versions from VertexAI server"
                    )
                logger.debug(
                    f"Ingesting model version (name: {model.display_name} id:{model.name} version:{model_version.version_id})"
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
                )

            if (
                self.config.max_models is not None
                and model_idx >= self.config.max_models
            ):
                break

        if total_versions > 0:
            logger.info(
                f"Finished processing {total_versions} model versions from VertexAI server"
            )

    def _get_model_evaluations_mcps(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch model evaluations from Vertex AI and generate corresponding mcps.
        """
        registered_models = self.client.Model.list(order_by="update_time desc")
        total_evaluations = 0

        for model in registered_models:
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue

            try:
                evaluations = list(model.list_model_evaluations())

                if self.config.max_evaluations_per_model is not None:
                    evaluations = evaluations[: self.config.max_evaluations_per_model]

                for evaluation in evaluations:
                    total_evaluations += 1
                    if total_evaluations % 100 == 0:
                        logger.info(
                            f"Processed {total_evaluations} model evaluations from VertexAI server"
                        )
                    logger.debug(
                        f"Ingesting evaluation for model {model.display_name}: {evaluation.name}"
                    )
                    yield from self._gen_model_evaluation_mcps(model, evaluation)

            except Exception as e:
                logger.warning(
                    f"Failed to fetch evaluations for model {model.display_name}: {e}"
                )
                continue

        if total_evaluations > 0:
            logger.info(
                f"Finished processing {total_evaluations} model evaluations from VertexAI server"
            )

    def _extract_evaluation_metrics(
        self, evaluation: ModelEvaluation
    ) -> List[MLMetricClass]:
        """Extract metrics from a model evaluation."""
        metrics: List[MLMetricClass] = []
        if not getattr(evaluation, "metrics", None):
            return metrics

        try:
            # evaluation.metrics can be dict-like or have specific attributes
            if isinstance(evaluation.metrics, dict):
                for metric_name, metric_value in evaluation.metrics.items():
                    try:
                        if isinstance(metric_value, Real) and not isinstance(
                            metric_value, bool
                        ):
                            metrics.append(
                                MLMetricClass(name=metric_name, value=str(metric_value))
                            )
                        elif isinstance(metric_value, str):
                            try:
                                float(metric_value)
                                metrics.append(
                                    MLMetricClass(name=metric_name, value=metric_value)
                                )
                            except ValueError:
                                pass
                    except Exception as e:
                        logger.debug(f"Skipping metric {metric_name}: {e}")
            else:
                for attr_name in dir(evaluation.metrics):
                    if not attr_name.startswith("_"):
                        try:
                            attr_value = getattr(evaluation.metrics, attr_name)
                            if isinstance(attr_value, Real) and not isinstance(
                                attr_value, bool
                            ):
                                metrics.append(
                                    MLMetricClass(name=attr_name, value=str(attr_value))
                                )
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"Failed to extract metrics from evaluation: {e}")

        return metrics

    def _get_evaluation_model_urn(self, model: Model) -> Optional[str]:
        """Get the model URN for an evaluation."""
        try:
            model_versions = model.versioning_registry.list_versions()
            if model_versions:
                model_version = model_versions[0]
                return self.urn_builder.make_ml_model_urn(
                    model_version=model_version,
                    model_name=self.name_formatter.format_model_name(model.name),
                )
        except Exception as e:
            logger.warning(f"Failed to get model URN for evaluation lineage: {e}")
        return None

    def _extract_evaluation_dataset_urns(
        self, evaluation: ModelEvaluation
    ) -> List[str]:
        """Extract evaluation dataset URNs from evaluation metadata."""
        dataset_urns: List[str] = []
        if not hasattr(evaluation, "metadata") or not evaluation.metadata:
            return dataset_urns

        try:
            metadata_dict = dict(evaluation.metadata) if evaluation.metadata else {}
            for key, value in metadata_dict.items():
                if "dataset" in key.lower() or "test_data" in key.lower():
                    value_str = str(value)
                    dataset_urns.extend(
                        self.uri_parser.dataset_urns_from_artifact_uri(value_str)
                    )
        except Exception as e:
            logger.debug(f"Could not extract evaluation dataset from metadata: {e}")

        return dataset_urns

    def _gen_model_evaluation_mcps(
        self, model: Model, evaluation: ModelEvaluation
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate MCPs for a model evaluation.
        """
        evaluation_id = self.name_formatter.format_evaluation_name(evaluation.name)
        evaluation_urn = builder.make_data_process_instance_urn(evaluation_id)

        metrics = self._extract_evaluation_metrics(evaluation)
        model_urn = self._get_evaluation_model_urn(model)

        created_time = 0
        if getattr(evaluation, "create_time", None):
            created_time = datetime_to_ts_millis(evaluation.create_time)

        yield MetadataChangeProposalWrapper(
            entityUrn=evaluation_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=f"Evaluation: {model.display_name}",
                created=AuditStampClass(
                    time=created_time,
                    actor=self._get_actor_from_labels(getattr(model, "labels", None))
                    or DATAHUB_ACTOR,
                ),
                customProperties=ModelEvaluationCustomProperties(
                    evaluation_id=evaluation.name,
                    model_name=model.display_name,
                    model_resource_name=model.resource_name,
                ).to_custom_properties(),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=evaluation_urn,
            subtype=VertexAISubTypes.MODEL_EVALUATION,
            resource_category=ResourceCategory.EVALUATIONS,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=evaluation_urn,
            aspect=MLTrainingRunProperties(
                id=evaluation.name,
                trainingMetrics=metrics if metrics else None,
            ),
        ).as_workunit()

        # Lineage: evaluation depends on model and potentially test datasets
        input_edges = []
        if model_urn:
            input_edges.append(EdgeClass(destinationUrn=model_urn))

        for ds_urn in self._extract_evaluation_dataset_urns(evaluation):
            input_edges.append(EdgeClass(destinationUrn=ds_urn))

        if input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=evaluation_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=input_edges,
                ),
            ).as_workunit()

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataWorkUnit]:
        model_meta: ModelMetadata = self._get_ml_model_metadata(model, model_version)
        yield from self._gen_ml_model_mcps(model_meta)
        yield from self._gen_endpoints_mcps(model_meta)

    def _get_ml_model_metadata(
        self, model: Model, model_version: VersionInfo
    ) -> ModelMetadata:
        model_meta = ModelMetadata(model=model, model_version=model_version)
        endpoints = self._search_endpoint(model)
        model_meta.endpoints = endpoints
        return model_meta

    def _get_training_jobs_mcps(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetches training jobs from Vertex AI and generates corresponding mcps.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates mcps containing metadata
        about the job, its inputs, and its outputs.
        """
        for class_name in TrainingJobTypes.all():
            if not self.config.training_job_type_pattern.allowed(class_name):
                continue
            logger.info(f"Fetching a list of {class_name}s from VertexAI server")

            jobs = list(
                getattr(self.client, class_name).list(order_by="update_time desc")
            )

            if self.config.max_training_jobs_per_type is not None:
                jobs = jobs[: self.config.max_training_jobs_per_type]

            for i, job in enumerate(jobs, start=1):
                if i % 100 == 0:
                    logger.info(f"Processed {i} {class_name}s from VertexAI server")
                yield from self._get_training_job_mcps(job)

            if jobs:
                logger.info(
                    f"Finished processing {len(jobs)} {class_name}s from VertexAI server"
                )

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataWorkUnit]:
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job)
        external_urns = self.uri_parser.extract_external_uris_from_job(job)
        job_meta.external_input_urns = external_urns.input_urns
        job_meta.external_output_urns = external_urns.output_urns
        yield from self._gen_training_job_mcps(job_meta)
        yield from self._get_input_dataset_mcps(job_meta)
        yield from self._gen_output_model_mcps(job_meta)

    def _gen_output_model_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        if job_meta.output_model and job_meta.output_model_version:
            job = job_meta.job
            job_urn = builder.make_data_process_instance_urn(
                self.name_formatter.format_job_name(entity_id=job.name)
            )

            training_data_urns = []
            if job_meta.input_dataset:
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=self.name_formatter.format_dataset_name(
                        entity_id=job_meta.input_dataset.name
                    ),
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
                training_data_urns.append(dataset_urn)
            if job_meta.external_input_urns:
                training_data_urns.extend(job_meta.external_input_urns)

            yield from self._gen_ml_model_mcps(
                ModelMetadata(
                    model=job_meta.output_model,
                    model_version=job_meta.output_model_version,
                    training_job_urn=job_urn,
                    training_data_urns=training_data_urns
                    if training_data_urns
                    else None,
                )
            )

    def _get_job_duration_millis(self, job: VertexAiResourceNoun) -> Optional[int]:
        create_time = job.create_time
        duration = None
        if isinstance(job, _TrainingJob) and job.create_time and job.end_time:
            end_time = job.end_time
            duration = datetime_to_ts_millis(end_time) - datetime_to_ts_millis(
                create_time
            )

        return int(duration) if duration else None

    def _gen_training_job_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate a mcp for VertexAI Training Job
        """
        job = job_meta.job
        job_id = self.name_formatter.format_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        created_time = datetime_to_ts_millis(job.create_time) if job.create_time else 0
        duration = self._get_job_duration_millis(job)

        dataset_urn = (
            builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=self.name_formatter.format_dataset_name(
                    entity_id=job_meta.input_dataset.name
                ),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            if job_meta.input_dataset
            else None
        )
        model_urn = (
            self.urn_builder.make_ml_model_urn(
                model_version=job_meta.output_model_version,
                model_name=self.name_formatter.format_model_name(
                    entity_id=job_meta.output_model.name
                ),
            )
            if job_meta.output_model and job_meta.output_model_version
            else None
        )
        external_input_edges = [
            EdgeClass(destinationUrn=u) for u in (job_meta.external_input_urns or [])
        ]
        external_output_edges = [
            EdgeClass(destinationUrn=u) for u in (job_meta.external_output_urns or [])
        ]

        result_type = get_job_result_status(job)

        hyperparams: List[MLHyperParamClass] = []
        metrics: List[MLMetricClass] = []

        if self.config.extract_execution_metrics:
            lineage = self._get_job_lineage_from_ml_metadata(job)
            if lineage:
                hyperparams = lineage.hyperparams
                metrics = lineage.metrics

                if hyperparams:
                    logger.info(
                        f"Extracted {len(hyperparams)} hyperparameters from ML Metadata for job {job.display_name}"
                    )
                if metrics:
                    logger.info(
                        f"Extracted {len(metrics)} metrics from ML Metadata for job {job.display_name}"
                    )

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=job.display_name,
                created=AuditStampClass(
                    time=created_time,
                    actor=self._get_actor_from_labels(getattr(job, "labels", None))
                    or DATAHUB_ACTOR,
                ),
                externalUrl=self.url_builder.make_job_url(job.name),
                customProperties=TrainingJobCustomProperties(
                    job_type=job.__class__.__name__,
                ).to_custom_properties(),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=MLTrainingRunProperties(
                externalUrl=self.url_builder.make_job_url(job.name),
                id=job.name,
                hyperParams=hyperparams if hyperparams else None,
                trainingMetrics=metrics if metrics else None,
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=job_urn,
            subtype=VertexAISubTypes.TRAINING_JOB,
            resource_category=ResourceCategory.TRAINING_JOBS,
        )

        if dataset_urn or external_input_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=(
                        ([EdgeClass(destinationUrn=dataset_urn)] if dataset_urn else [])
                        + external_input_edges
                    ),
                ),
            ).as_workunit()

        if model_urn or external_output_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceOutputClass(
                    outputs=[],
                    outputEdges=(
                        ([EdgeClass(destinationUrn=model_urn)] if model_urn else [])
                        + external_output_edges
                    ),
                ),
            ).as_workunit()

        if is_status_for_run_event_class(result_type) and duration:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=created_time,
                    result=DataProcessInstanceRunResultClass(
                        type=result_type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=duration,
                ),
            ).as_workunit()

    def _gen_ml_group_mcps(
        self,
        model: Model,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate container and MLModelGroup mcp for a VertexAI Model.
        """
        model_group_container_key = self._get_model_group_container(model)

        yield from gen_containers(
            parent_container_key=self._get_resource_category_container(
                ResourceCategory.MODELS
            ),
            container_key=model_group_container_key,
            name=model.display_name,
            sub_types=[VertexAISubTypes.MODEL_GROUP],
        )

        ml_model_group_urn = self.urn_builder.make_ml_model_group_urn(model)

        yield MetadataChangeProposalWrapper(
            entityUrn=ml_model_group_urn,
            aspect=MLModelGroupPropertiesClass(
                name=model.display_name,
                description=model.description,
                created=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model.create_time),
                        actor=self._get_actor_from_labels(
                            getattr(model, "labels", None)
                        )
                        or DATAHUB_ACTOR,
                    )
                    if model.create_time
                    else None
                ),
                lastModified=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model.update_time),
                        actor=DATAHUB_ACTOR,
                    )
                    if model.update_time
                    else None
                ),
                customProperties=None,
                externalUrl=self.url_builder.make_model_url(model.name),
            ),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=ml_model_group_urn,
            subtype=VertexAISubTypes.MODEL_GROUP,
            resource_category=ResourceCategory.MODELS,
        )

    def _get_project_container(self) -> ProjectIdKey:
        return ProjectIdKey(
            project_id=self._get_project_id(),
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _get_resource_category_container(
        self, category: str
    ) -> VertexAIResourceCategoryKey:
        return VertexAIResourceCategoryKey(
            project_id=self._get_project_id(),
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            category=category,
        )

    def _get_model_group_container(self, model: Model) -> ModelGroupKey:
        return ModelGroupKey(
            project_id=self._get_project_id(),
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            model_group_name=self.name_formatter.format_model_group_name(model.name),
        )

    def _generate_resource_category_containers(self) -> Iterable[MetadataWorkUnit]:
        """Generate all resource category containers for the current project."""
        categories = [
            ResourceCategory.MODELS,
            ResourceCategory.TRAINING_JOBS,
            ResourceCategory.DATASETS,
            ResourceCategory.ENDPOINTS,
            ResourceCategory.PIPELINES,
            ResourceCategory.EVALUATIONS,
        ]

        for category in categories:
            category_key = self._get_resource_category_container(category)
            yield from gen_containers(
                parent_container_key=self._get_project_container(),
                container_key=category_key,
                name=category,
                sub_types=[DatasetContainerSubTypes.FOLDER],
            )

    def _get_project_id(self) -> str:
        return self._current_project_id or self.config.project_id

    def _get_region(self) -> str:
        return self._current_region or self.config.region

    def _is_automl_job(self, job: VertexAiResourceNoun) -> bool:
        return isinstance(
            job,
            (
                AutoMLTabularTrainingJob,
                AutoMLTextTrainingJob,
                AutoMLImageTrainingJob,
                AutoMLVideoTrainingJob,
                AutoMLForecastingTrainingJob,
            ),
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

        if self.datasets is None:
            self.datasets = {}

            for dtype in DatasetTypes.all():
                dataset_class = getattr(self.client.datasets, dtype)
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return self.datasets.get(dataset_id)

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Retrieve and cache all datasets from Vertex AI.
        """
        if self.datasets is None:
            self.datasets = {}

            for dtype in DatasetTypes.all():
                dataset_class = getattr(self.client.datasets, dtype)
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return []

    def _get_dataset_workunits_from_job_metadata(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """
        ds = job_meta.input_dataset

        if ds:
            dataset_name = self.name_formatter.format_dataset_name(entity_id=ds.name)
            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=ds.display_name,
                    created=(
                        TimeStampClass(time=datetime_to_ts_millis(ds.create_time))
                        if ds.create_time
                        else None
                    ),
                    description=ds.display_name,
                    customProperties=DatasetCustomProperties(
                        resource_name=ds.resource_name,
                    ).to_custom_properties(),
                    qualifiedName=ds.resource_name,
                ),
            ).as_workunit()

            yield from self._yield_common_aspects(
                entity_urn=dataset_urn,
                subtype=VertexAISubTypes.DATASET,
                resource_category=ResourceCategory.DATASETS,
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
        if self._is_automl_job(job):
            try:
                job_config = AutoMLJobConfig(**job.to_dict())
            except Exception as e:
                logger.warning(
                    f"Failed to parse AutoML job config for {job.display_name}: {e}"
                )
                return job_meta

            if job_config.inputDataConfig and job_config.inputDataConfig.datasetId:
                dataset_id = job_config.inputDataConfig.datasetId
                logger.info(
                    f"Found input dataset (id: {dataset_id}) for training job ({job.display_name})"
                )

                input_ds = self._search_dataset(dataset_id)
                if input_ds:
                    logger.info(
                        f"Found the name of input dataset ({input_ds.display_name}) with dataset id ({dataset_id})"
                    )
                    job_meta.input_dataset = input_ds

            if (
                job_config.modelToUpload
                and job_config.modelToUpload.name
                and job_config.modelToUpload.versionId
            ):
                model_name = job_config.modelToUpload.name
                model_version_str = job_config.modelToUpload.versionId
                try:
                    model = Model(model_name=model_name)
                    model_version = self._search_model_version(model, model_version_str)
                    if model and model_version:
                        logger.info(
                            f"Found output model (name:{model.display_name} id:{model_version_str}) "
                            f"for training job: {job.display_name}"
                        )
                        job_meta.output_model = model
                        job_meta.output_model_version = model_version
                except GoogleAPICallError as e:
                    self.report.report_failure(
                        title="Unable to fetch model and model version",
                        message="Encountered an error while fetching output model and model version which training job generates",
                        exc=e,
                    )
        else:
            lineage = self._get_job_lineage_from_ml_metadata(job)

            if lineage:
                if lineage.input_urns:
                    job_meta.external_input_urns = lineage.input_urns
                    logger.info(
                        f"Extracted {len(lineage.input_urns)} input URNs from ML Metadata for job {job.display_name}"
                    )

                if lineage.output_urns:
                    job_meta.external_output_urns = lineage.output_urns
                    logger.info(
                        f"Extracted {len(lineage.output_urns)} output URNs from ML Metadata for job {job.display_name}"
                    )
            elif self.config.use_ml_metadata_for_lineage:
                logger.debug(
                    f"No lineage metadata found for CustomJob {job.display_name}. "
                    "Ensure job logs to ML Metadata for lineage tracking."
                )

        return job_meta

    def _gen_endpoints_mcps(
        self, model_meta: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        model: Model = model_meta.model
        model_version: VersionInfo = model_meta.model_version

        if model_meta.endpoints:
            for endpoint in model_meta.endpoints:
                endpoint_urn = builder.make_ml_model_deployment_urn(
                    platform=self.platform,
                    deployment_name=self.name_formatter.format_endpoint_name(
                        entity_id=endpoint.name
                    ),
                    env=self.config.env,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=endpoint_urn,
                    aspect=MLModelDeploymentPropertiesClass(
                        description=model.description,
                        createdAt=datetime_to_ts_millis(endpoint.create_time),
                        version=VersionTagClass(
                            versionTag=str(model_version.version_id)
                        ),
                        customProperties=EndpointDeploymentCustomProperties(
                            display_name=endpoint.display_name,
                        ).to_custom_properties(),
                    ),
                ).as_workunit()

                yield from self._yield_common_aspects(
                    entity_urn=endpoint_urn,
                    subtype=VertexAISubTypes.ENDPOINT,
                    resource_category=ResourceCategory.ENDPOINTS,
                    include_subtypes=False,
                )

    def _gen_ml_model_mcps(
        self, ModelMetadata: ModelMetadata
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate an MLModel and Endpoint mcp for an VertexAI Model Version.
        """

        model: Model = ModelMetadata.model
        model_version: VersionInfo = ModelMetadata.model_version
        training_job_urn: Optional[str] = ModelMetadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = ModelMetadata.endpoints
        endpoint_urns: List[str] = list()

        logging.info(f"generating model mcp for {model.name}")

        if endpoints:
            for endpoint in endpoints:
                logger.info(
                    f"found endpoint ({endpoint.display_name}) for model ({model.resource_name})"
                )
                endpoint_urns.append(
                    builder.make_ml_model_deployment_urn(
                        platform=self.platform,
                        deployment_name=self.name_formatter.format_endpoint_name(
                            entity_id=endpoint.display_name
                        ),
                        env=self.config.env,
                    )
                )

        model_group_urn = self.urn_builder.make_ml_model_group_urn(model)
        model_name = self.name_formatter.format_model_name(entity_id=model.name)
        model_urn = self.urn_builder.make_ml_model_urn(
            model_version, model_name=model_name
        )

        downstream_job_urns = self._model_to_downstream_jobs.get(model_urn, [])

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=MLModelPropertiesClass(
                name=f"{model.display_name}_{model_version.version_id}",
                description=model_version.version_description,
                customProperties=MLModelCustomProperties(
                    version_id=str(model_version.version_id),
                    resource_name=model.resource_name,
                ).to_custom_properties(),
                created=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model_version.version_create_time),
                        actor=self._get_actor_from_labels(
                            getattr(model, "labels", None)
                        )
                        or DATAHUB_ACTOR,
                    )
                    if model_version.version_create_time
                    else None
                ),
                lastModified=(
                    TimeStampClass(
                        time=datetime_to_ts_millis(model_version.version_update_time),
                        actor=DATAHUB_ACTOR,
                    )
                    if model_version.version_update_time
                    else None
                ),
                version=VersionTagClass(versionTag=str(model_version.version_id)),
                groups=[model_group_urn],
                trainingJobs=([training_job_urn] if training_job_urn else None),
                downstreamJobs=(downstream_job_urns if downstream_job_urns else None),
                deployments=endpoint_urns,
                externalUrl=self.url_builder.make_model_version_url(
                    model.name, model.version_id
                ),
                type=MLModelType.ML_MODEL,
            ),
        ).as_workunit()

        model_group_container_urn = self._get_model_group_container(model).as_urn()
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=ContainerClass(container=model_group_container_urn),
        ).as_workunit()

        yield from self._yield_common_aspects(
            entity_urn=model_urn,
            subtype=VertexAISubTypes.MODEL,
            include_container=False,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=VersionPropertiesClass(
                version=VersionTagClass(
                    versionTag=str(model_version.version_id),
                    metadataAttribution=(
                        MetadataAttributionClass(
                            time=int(
                                model_version.version_create_time.timestamp() * 1000
                            ),
                            actor=self._get_actor_from_labels(
                                getattr(model, "labels", None)
                            )
                            or DATAHUB_ACTOR,
                        )
                        if model_version.version_create_time
                        else None
                    ),
                ),
                versionSet=self._get_version_set_urn(model).urn(),
                sortId=str(model_version.version_id).zfill(10),
                aliases=None,
            ),
        ).as_workunit()

        training_data_urns = ModelMetadata.training_data_urns
        if training_data_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=TrainingDataClass(
                    trainingData=[
                        BaseDataClass(dataset=ds_urn) for ds_urn in training_data_urns
                    ]
                ),
            ).as_workunit()

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        guid_dict = {"platform": self.platform, "name": model.name}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=MlModelUrn.ENTITY_TYPE,
        )
        return version_set_urn

    def _discover_regions_for_project(self, project_id: str) -> List[str]:
        """Discover available Vertex AI regions for a project using Locations API."""
        try:
            client = aiplatform_v1.PipelineServiceClient(
                client_options={"api_endpoint": "aiplatform.googleapis.com"}
            )
            locations = client.list_locations(
                request={"name": f"projects/{project_id}"}
            )
            discovered: List[str] = []
            for loc in locations:
                # loc.name like projects/{project}/locations/{region}
                try:
                    parts = loc.name.split("/")
                    region = parts[parts.index("locations") + 1]
                    if region and region not in discovered:
                        discovered.append(region)
                except Exception:
                    continue
            return discovered
        except Exception:
            logger.warning(
                "Failed to discover Vertex AI regions for project %s; falling back to configured region(s)",
                project_id,
            )
            return []

    def _search_endpoint(self, model: Model) -> List[Endpoint]:
        """
        Search for an endpoint associated with the model.
        """
        if self.endpoints is None:
            endpoint_dict: Dict[str, List[Endpoint]] = {}
            for endpoint in self.client.Endpoint.list():
                for resource in endpoint.list_models():
                    if resource.model not in endpoint_dict:
                        endpoint_dict[resource.model] = []
                    endpoint_dict[resource.model].append(endpoint)
            self.endpoints = endpoint_dict

        return self.endpoints.get(model.resource_name, [])

    def _get_actor_from_labels(self, labels: Optional[Dict[str, str]]) -> Optional[str]:
        """
        Extract actor URN from resource labels if present.
        Checks for common label keys: created_by, creator, owner.
        """
        if not labels:
            return None

        actor_keys = ["created_by", "creator", "owner"]
        for key in actor_keys:
            if key in labels and labels[key]:
                return builder.make_user_urn(labels[key])

        return None

    def _get_job_lineage_from_ml_metadata(
        self, job: VertexAiResourceNoun
    ) -> Optional[LineageMetadata]:
        if not self._ml_metadata_helper:
            return None
        return self._ml_metadata_helper.get_job_lineage_metadata(job)

    def _get_training_job_executions(
        self, job: VertexAiResourceNoun
    ) -> List[Execution]:
        """
        Query ML Metadata for Executions linked to this training job.
        Returns Executions that match the job's display name or resource name.
        """
        if not self._metadata_client or not self.config.use_ml_metadata_for_lineage:
            return []

        try:
            parent = MLMetadataDefaults.METADATA_STORE_PATH_TEMPLATE.format(
                project_id=self._get_project_id(),
                region=self._get_region(),
                metadata_store=MLMetadataDefaults.DEFAULT_METADATA_STORE,
            )

            filter_str = f'display_name="{job.display_name}"'

            request = ListExecutionsRequest(
                parent=parent,
                filter=filter_str,
            )

            executions_response = self._metadata_client.list_executions(request=request)
            executions = list(executions_response)

            if not executions:
                filter_str = f'schema_title="{MLMetadataSchemas.CONTAINER_EXECUTION}" OR schema_title="{MLMetadataSchemas.RUN}"'
                request = ListExecutionsRequest(
                    parent=parent,
                    filter=filter_str,
                    page_size=MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS,
                )
                all_executions = list(
                    self._metadata_client.list_executions(request=request)
                )

                for execution in all_executions:
                    if getattr(execution, "metadata", None):
                        metadata_str = str(execution.metadata)
                        if job.name in metadata_str or job.display_name in metadata_str:
                            executions.append(execution)

            logger.info(
                f"Found {len(executions)} executions for training job {job.display_name}"
            )
            return executions

        except Exception as e:
            logger.warning(
                f"Failed to query executions for job {job.display_name}: {e}",
                exc_info=True,
            )
            return []

    def _get_execution_artifacts(self, execution_name: str) -> ArtifactURNs:
        """
        Get input and output artifact URNs for an execution by querying ML Metadata.
        """
        if not self._metadata_client:
            return ArtifactURNs()

        try:
            request = QueryExecutionInputsAndOutputsRequest(execution=execution_name)
            response = self._metadata_client.query_execution_inputs_and_outputs(
                request=request
            )

            input_urns: List[str] = []
            output_urns: List[str] = []

            artifact_events: Dict[str, str] = {}
            for event in response.events:
                artifact_events[event.artifact] = event.type_.name

            for artifact in response.artifacts:
                event_type = artifact_events.get(artifact.name, "")

                if artifact.uri:
                    dataset_urns = self.uri_parser.dataset_urns_from_artifact_uri(
                        artifact.uri
                    )

                    if event_type == "INPUT":
                        input_urns.extend(dataset_urns)
                    elif event_type == "OUTPUT":
                        output_urns.extend(dataset_urns)

            logger.debug(
                f"Extracted {len(input_urns)} input URNs and {len(output_urns)} output URNs from execution {execution_name}"
            )
            return ArtifactURNs(input_urns=input_urns, output_urns=output_urns)

        except Exception as e:
            logger.warning(
                f"Failed to query artifacts for execution {execution_name}: {e}",
                exc_info=True,
            )
            return ArtifactURNs()

    def _get_execution_hyperparams(
        self, execution: Execution
    ) -> List[MLHyperParamClass]:
        """
        Extract hyperparameters from execution metadata.
        Looks for common hyperparameter naming patterns.
        """
        hyperparams: List[MLHyperParamClass] = []

        if not getattr(execution, "metadata", None):
            return hyperparams

        for key in execution.metadata:
            value = execution.metadata[key]
            is_hyperparam = HyperparameterPatterns.is_hyperparam(key)

            if is_hyperparam:
                param_value = extract_protobuf_value(value)
                if param_value:
                    hyperparams.append(MLHyperParamClass(name=key, value=param_value))

        return hyperparams

    def _get_execution_metrics(self, execution: Execution) -> List[MLMetricClass]:
        """
        Extract training metrics from execution metadata.
        Looks for common metric naming patterns.
        """
        metrics: List[MLMetricClass] = []

        if not getattr(execution, "metadata", None):
            return metrics

        for key in execution.metadata:
            value = execution.metadata[key]
            is_metric = MetricPatterns.is_metric(key)

            if is_metric:
                metric_value = extract_numeric_value(value)
                if metric_value:
                    metrics.append(MLMetricClass(name=key, value=metric_value))

        return metrics
