import dataclasses
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from google.api_core.exceptions import (
    FailedPrecondition,
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
)
from google.cloud import aiplatform, resourcemanager_v3
from google.cloud.aiplatform import (
    AutoMLForecastingTrainingJob,
    AutoMLImageTrainingJob,
    AutoMLTabularTrainingJob,
    AutoMLTextTrainingJob,
    AutoMLVideoTrainingJob,
    Endpoint,
    ExperimentRun,
    PipelineJob,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.metadata.execution import Execution
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
from google.cloud.aiplatform.models import Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.cloud.aiplatform_v1.types import (
    PipelineJob as PipelineJobType,
    PipelineTaskDetail,
)
from google.protobuf import timestamp_pb2

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
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_credentials_config import (
    gcp_credentials_context,
)
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
    call_with_retry,
    get_gcp_error_type,
    get_projects,
    get_projects_client,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
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

T = TypeVar("T")

logger = logging.getLogger(__name__)


VERTEX_AI_RETRY_TIMEOUT = 600.0


@dataclasses.dataclass
class _ErrorGuidance:
    """Guidance for a specific error type."""

    action: str
    debug_cmd: str


_ERROR_GUIDANCE: Dict[type, _ErrorGuidance] = {
    InvalidArgument: _ErrorGuidance(
        action="Enable Vertex AI API or use project_id_pattern to skip this project",
        debug_cmd="gcloud services enable aiplatform.googleapis.com --project={project_id}",
    ),
    FailedPrecondition: _ErrorGuidance(
        action="Enable Vertex AI API or use project_id_pattern to skip this project",
        debug_cmd="gcloud services enable aiplatform.googleapis.com --project={project_id}",
    ),
    NotFound: _ErrorGuidance(
        action="Verify project ID is correct and project exists",
        debug_cmd="gcloud projects describe {project_id}",
    ),
    PermissionDenied: _ErrorGuidance(
        action="Grant roles/aiplatform.viewer to service account",
        debug_cmd="gcloud projects get-iam-policy {project_id}",
    ),
    ResourceExhausted: _ErrorGuidance(
        action="Wait and retry, or request quota increase",
        debug_cmd="gcloud compute project-info describe --project={project_id}",
    ),
}

_DEFAULT_GUIDANCE = _ErrorGuidance(
    action="Check service account permissions and project configuration",
    debug_cmd="gcloud projects describe {project_id}",
)


def _is_config_error(exc: Exception) -> bool:
    """Check if exception indicates a configuration error (e.g., API not enabled)."""
    return isinstance(exc, (InvalidArgument, FailedPrecondition))


def _call_with_retry(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Execute a Vertex AI API call with retry for rate limits and transient errors."""
    return call_with_retry(func, *args, timeout=VERTEX_AI_RETRY_TIMEOUT, **kwargs)


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


@dataclasses.dataclass
class PipelineTaskMetadata:
    name: str
    urn: DataJobUrn
    id: Optional[int] = None
    type: Optional[str] = None
    state: Optional[PipelineTaskDetail.State] = None
    start_time: Optional[timestamp_pb2.Timestamp] = None
    create_time: Optional[timestamp_pb2.Timestamp] = None
    end_time: Optional[timestamp_pb2.Timestamp] = None
    upstreams: Optional[List[DataJobUrn]] = None
    duration: Optional[int] = None


@dataclasses.dataclass
class PipelineMetadata:
    name: str
    resource_name: str
    tasks: List[PipelineTaskMetadata]
    urn: DataFlowUrn
    id: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    duration: Optional[timedelta] = None
    region: Optional[str] = None


@platform_name("Vertex AI", id="vertexai")
@config_class(VertexAIConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extract descriptions for Vertex AI Registered Models and Model Versions",
)
class VertexAISource(Source):
    platform: str = "vertexai"

    def __init__(self, ctx: PipelineContext, config: VertexAIConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self._projects: Optional[List[GCPProject]] = None
        self._current_project_id: Optional[str] = None

        self.client = aiplatform
        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None
        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None
        self.experiments: Optional[List[Experiment]] = None

    def _get_projects_to_process(self) -> List[GCPProject]:
        if self._projects is not None:
            return self._projects

        client: Optional[resourcemanager_v3.ProjectsClient] = None
        if not self.config.project_ids:
            client = get_projects_client()

        self._projects = get_projects(
            project_ids=self.config.project_ids or None,
            project_labels=self.config.project_labels or None,
            project_id_pattern=self.config.project_id_pattern,
            client=client,
        )

        logger.info("Will process %d projects", len(self._projects))
        return self._projects

    def _init_for_project(self, project: GCPProject) -> None:
        self._current_project_id = project.id

        aiplatform.init(
            project=project.id,
            location=self.config.region,
        )
        self.client = aiplatform

        self.endpoints = None
        self.datasets = None
        self.experiments = None

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        with gcp_credentials_context(self.config.get_credentials_dict()):
            yield from super().get_workunits()

    def _build_no_projects_error(self) -> str:
        """Build detailed error message for 'no projects to process' scenario."""
        pattern_info = (
            f"allow: {self.config.project_id_pattern.allow}, "
            f"deny: {self.config.project_id_pattern.deny}"
        )

        if self.config.project_ids:
            project_list = ", ".join(self.config.project_ids[:3])
            if len(self.config.project_ids) > 3:
                project_list += f"... ({len(self.config.project_ids)} total)"
            first_project = self.config.project_ids[0]
            return (
                f"No projects to process from project_ids: [{project_list}]. "
                f"Pattern: {pattern_info}.\n\n"
                "Troubleshooting:\n"
                f"  1. Verify project exists: gcloud projects describe {first_project}\n"
                "  2. Check if project_id_pattern filtered all projects (try removing it)\n"
                f"  3. Verify credentials: gcloud projects list --filter='projectId:{first_project}'"
            )

        if self.config.project_labels:
            label_preview = ", ".join(self.config.project_labels[:3])
            first_label = self.config.project_labels[0]
            label_filter = (
                f"labels.{first_label}"
                if ":" in first_label
                else f"labels.{first_label}:*"
            )
            return (
                f"No projects found with labels: [{label_preview}]. "
                f"Pattern: {pattern_info}.\n\n"
                "Troubleshooting:\n"
                f"  1. Verify labeled projects exist: gcloud projects list --filter='{label_filter}'\n"
                "  2. Check permissions: service account needs resourcemanager.projects.list\n"
                "  3. Check if project_id_pattern filtered all matches (try removing it)\n"
                "  4. Alternative: use explicit project_ids instead of labels"
            )

        return (
            f"No projects discovered via auto-discovery. Pattern: {pattern_info}.\n\n"
            "Troubleshooting:\n"
            "  1. Verify access: gcloud projects list --filter='lifecycleState:ACTIVE'\n"
            "  2. Check permissions: service account needs resourcemanager.projects.list\n"
            "  3. Check if project_id_pattern filtered all projects (try removing it)\n"
            "  4. Alternative: use explicit project_ids or project_labels"
        )

    def _build_project_error_context(self, project_id: str, exc: Exception) -> str:
        """Build consistent error context for project-level errors.
        Returns a uniformly formatted message:
        1. What failed (project)
        2. Error details
        3. Actionable next step
        4. Debug command
        """
        guidance = _ERROR_GUIDANCE.get(type(exc), _DEFAULT_GUIDANCE)
        debug_cmd = guidance.debug_cmd.format(project_id=project_id)

        return f"Error: {exc}\nAction: {guidance.action}\nDebug: {debug_cmd}"

    def _handle_api_error(
        self,
        error: GoogleAPICallError,
        operation: str,
        resource_type: str,
        resource_id: Optional[str] = None,
    ) -> None:
        """
        Handle Google API errors with structured logging and reporting.

        Args:
            error: The caught exception
            operation: What we were trying to do ("fetch model", "list endpoints")
            resource_type: Type of resource ("model", "endpoint", "pipeline")
            resource_id: Optional identifier for the resource
        """
        error_context = {
            "operation": operation,
            "resource_type": resource_type,
            "error_type": type(error).__name__,
            "error_code": getattr(error, "code", "unknown"),
            "project_id": self._current_project_id,
        }
        if resource_id:
            error_context["resource_id"] = resource_id

        logger.error(
            "API error during %s for %s: %s",
            operation,
            resource_type,
            error,
            extra=error_context,
        )

    def _handle_project_error(
        self,
        project_id: str,
        exc: Exception,
        failed_projects: List[str],
        failed_exceptions: Dict[str, Exception],
    ) -> None:
        """Handle project-level errors consistently."""
        failed_projects.append(project_id)
        failed_exceptions[project_id] = exc
        error_type = get_gcp_error_type(exc)

        error_context = self._build_project_error_context(project_id, exc)

        self.report.failure(
            title=f"{error_type}: {project_id}",
            message=error_context,
            exc=exc,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Fetch MetadataWorkUnits for Vertex AI models, pipelines, training jobs, and experiments.
        Supports multi-project scanning via project_ids config.
        """
        projects = self._get_projects_to_process()

        if not projects:
            raise RuntimeError(self._build_no_projects_error())

        successful_projects = 0
        failed_projects: List[str] = []
        failed_exceptions: Dict[str, Exception] = {}

        for project in projects:
            logger.info("Processing Vertex AI resources for project: %s", project.id)
            try:
                self._init_for_project(project)
                yield from self._process_current_project()
                successful_projects += 1
            except GoogleAPICallError as exc:
                self._handle_project_error(
                    project.id, exc, failed_projects, failed_exceptions
                )

        if failed_projects:
            if successful_projects == 0:
                error_types = {
                    get_gcp_error_type(e) for e in failed_exceptions.values()
                }
                first_exception = next(iter(failed_exceptions.values()))
                raise RuntimeError(
                    f"All {len(failed_projects)} projects failed ({', '.join(error_types)}). "
                    f"Projects: {failed_projects}"
                ) from first_exception
            self.report.warning(
                title=f"Partial ingestion: {len(failed_projects)}/{len(projects)} projects failed",
                message=f"Failed: {', '.join(failed_projects)}",
            )

    def _process_current_project(self) -> Iterable[MetadataWorkUnit]:
        yield from self._gen_project_workunits()

        resource_fetchers = [
            ("models", lambda: auto_workunit(self._get_ml_models_mcps())),
            ("training_jobs", lambda: auto_workunit(self._get_training_jobs_mcps())),
            ("experiments", self._get_experiments_workunits),
            (
                "experiment_runs",
                lambda: auto_workunit(self._get_experiment_runs_mcps()),
            ),
            ("pipelines", lambda: auto_workunit(self._get_pipelines_mcps())),
        ]

        for resource_type, fetch_func in resource_fetchers:
            try:
                yield from fetch_func()
            except Exception as e:
                logger.warning(
                    "Failed to fetch %s for project %s: %s",
                    resource_type,
                    self._current_project_id,
                    e,
                )
                self.report.warning(
                    title=f"Failed to fetch {resource_type}",
                    message=f"Project: {self._current_project_id}. Error: {e}",
                    exc=e,
                )

    def _get_pipelines_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch Vertex AI Pipeline Jobs and generate DataJob MCPs with lineage to tasks."""
        pipeline_jobs = _call_with_retry(self.client.PipelineJob.list)

        for pipeline in pipeline_jobs:
            try:
                logger.info("Fetching pipeline: %s", pipeline.name)
                pipeline_meta = self._get_pipeline_metadata(pipeline)
                yield from self._get_pipeline_mcps(pipeline_meta)
                yield from self._gen_pipeline_task_mcps(pipeline_meta)
            except Exception as e:
                logger.warning("Failed to process pipeline %s: %s", pipeline.name, e)
                self.report.warning(
                    title=f"Failed to process pipeline {pipeline.name}",
                    message=str(e),
                    exc=e,
                )

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
                    "fetching pipeline task (%s) in pipeline (%s)",
                    task_name,
                    pipeline.name,
                )
                task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(pipeline_urn),
                    job_id=self._make_vertexai_pipeline_task_id(task_name),
                )
                task_meta = PipelineTaskMetadata(name=task_name, urn=task_urn)
                if (
                    "dependentTasks"
                    in resource.pipeline_spec["root"]["dag"]["tasks"][task_name]
                ):
                    upstream_tasks = resource.pipeline_spec["root"]["dag"]["tasks"][
                        task_name
                    ]["dependentTasks"]
                    upstream_urls = [
                        DataJobUrn.create_from_ids(
                            data_flow_urn=str(pipeline_urn),
                            job_id=self._make_vertexai_pipeline_task_id(upstream_task),
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
                        task_meta.duration = int(
                            (
                                task_meta.end_time.timestamp()
                                - task_meta.start_time.timestamp()
                            )
                            * 1000
                        )

                tasks.append(task_meta)
        return tasks

    def _get_pipeline_metadata(self, pipeline: PipelineJob) -> PipelineMetadata:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            env=self.config.env,
            flow_id=self._make_vertexai_pipeline_id(pipeline.name),
            platform_instance=self.platform,
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
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dpi_urn = builder.make_data_process_instance_urn(
            self._make_vertexai_pipeline_task_run_id(entity_id=task.name)
        )
        result_status: Union[str, RunResultTypeClass] = get_pipeline_task_result_status(
            task.state
        )

        yield from MetadataChangeProposalWrapper.construct_many(
            dpi_urn,
            aspects=[
                DataProcessInstancePropertiesClass(
                    name=task.name,
                    created=AuditStampClass(
                        time=(
                            int(task.create_time.timestamp() * 1000)
                            if task.create_time
                            else 0
                        ),
                        actor="urn:li:corpuser:datahub",
                    ),
                    externalUrl=self._make_pipeline_external_url(pipeline.name),
                    customProperties={},
                ),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE_TASK_RUN]),
                ContainerClass(container=self._get_project_container().as_urn()),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
                DataProcessInstanceRelationships(
                    upstreamInstances=[], parentTemplate=str(datajob.urn)
                ),
                (
                    DataProcessInstanceRunEventClass(
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
                    )
                    if is_status_for_run_event_class(result_status) and task.duration
                    else None
                ),
            ],
        )

    def _gen_pipeline_task_mcps(
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dataflow_urn = pipeline.urn

        for task in pipeline.tasks:
            datajob = DataJob(
                id=self._make_vertexai_pipeline_task_id(task.name),
                flow_urn=dataflow_urn,
                name=task.name,
                properties={},
                owners={"urn:li:corpuser:datahub"},
                upstream_urns=task.upstreams if task.upstreams else [],
                url=self._make_pipeline_external_url(pipeline.name),
            )
            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=str(datajob.urn),
                aspects=[
                    ContainerClass(container=self._get_project_container().as_urn()),
                    SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE_TASK]),
                ],
            )
            yield from datajob.generate_mcp()
            yield from self._gen_pipeline_task_run_mcps(task, datajob, pipeline)

    def _format_pipeline_duration(self, td: timedelta) -> str:
        days = td.days
        hours, remainder = divmod(td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = td.microseconds // 1000

        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if seconds:
            parts.append(f"{seconds}s")
        if milliseconds:
            parts.append(f"{milliseconds}ms")
        return " ".join(parts) if parts else "0s"

    def _get_pipeline_task_properties(
        self, task: PipelineTaskMetadata
    ) -> Dict[str, str]:
        return {
            "created_time": (
                task.create_time.strftime("%Y-%m-%d %H:%M:%S")
                if task.create_time
                else ""
            )
        }

    def _get_pipeline_properties(self, pipeline: PipelineMetadata) -> Dict[str, str]:
        return {
            "resource_name": pipeline.resource_name if pipeline.resource_name else "",
            "create_time": (
                pipeline.create_time.isoformat() if pipeline.create_time else ""
            ),
            "update_time": (
                pipeline.update_time.isoformat() if pipeline.update_time else ""
            ),
            "duration": (
                self._format_pipeline_duration(pipeline.duration)
                if pipeline.duration
                else ""
            ),
            "location": (pipeline.region if pipeline.region else ""),
            "labels": ",".join([f"{k}:{v}" for k, v in pipeline.labels.items()])
            if pipeline.labels
            else "",
        }

    def _get_pipeline_mcps(
        self, pipeline: PipelineMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dataflow = DataFlow(
            orchestrator=self.platform,
            id=self._make_vertexai_pipeline_id(pipeline.name),
            env=self.config.env,
            name=pipeline.name,
            platform_instance=self.platform,
            properties=self._get_pipeline_properties(pipeline),
            owners={"urn:li:corpuser:datahub"},
            url=self._make_pipeline_external_url(pipeline_name=pipeline.name),
        )

        yield from dataflow.generate_mcp()

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=str(dataflow.urn),
            aspects=[
                ContainerClass(container=self._get_project_container().as_urn()),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE]),
            ],
        )

    def _get_experiments_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.experiments = _call_with_retry(aiplatform.Experiment.list)

        logger.info("Fetching experiments from VertexAI server")
        for experiment in self.experiments:
            try:
                yield from self._gen_experiment_workunits(experiment)
            except Exception as e:
                logger.warning(
                    "Failed to process experiment %s: %s", experiment.name, e
                )
                self.report.warning(
                    title=f"Failed to process experiment {experiment.name}",
                    message=str(e),
                    exc=e,
                )

    def _get_experiment_runs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.experiments is None:
            self.experiments = _call_with_retry(aiplatform.Experiment.list)
        for experiment in self.experiments:
            try:
                logger.info(
                    "Fetching experiment runs for experiment: %s", experiment.name
                )
                experiment_runs = _call_with_retry(
                    aiplatform.ExperimentRun.list, experiment=experiment.name
                )
                for run in experiment_runs:
                    yield from self._gen_experiment_run_mcps(experiment, run)
            except Exception as e:
                logger.warning(
                    "Failed to fetch runs for experiment %s: %s", experiment.name, e
                )
                self.report.warning(
                    title=f"Failed to fetch runs for experiment {experiment.name}",
                    message=str(e),
                    exc=e,
                )

    def _gen_experiment_workunits(
        self, experiment: Experiment
    ) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            parent_container_key=self._get_project_container(),
            container_key=ExperimentKey(
                platform=self.platform,
                id=self._make_vertexai_experiment_id(experiment.name),
            ),
            name=experiment.name,
            sub_types=[MLAssetSubTypes.VERTEX_EXPERIMENT],
            extra_properties={
                "name": experiment.name,
                "resourceName": experiment.resource_name,
                "dashboardURL": experiment.dashboard_url
                if experiment.dashboard_url
                else "",
            },
            external_url=self._make_experiment_external_url(experiment),
        )

    def _get_experiment_run_params(self, run: ExperimentRun) -> List[MLHyperParamClass]:
        return [
            MLHyperParamClass(name=k, value=str(v)) for k, v in run.get_params().items()
        ]

    def _get_experiment_run_metrics(self, run: ExperimentRun) -> List[MLMetricClass]:
        return [
            MLMetricClass(name=k, value=str(v)) for k, v in run.get_metrics().items()
        ]

    def _get_run_timestamps(
        self, run: ExperimentRun
    ) -> Tuple[Optional[int], Optional[int]]:
        executions = run.get_executions()
        if len(executions) == 1:
            create_time = executions[0].create_time
            update_time = executions[0].update_time
            if create_time and update_time:
                duration = (
                    update_time.timestamp() * 1000 - create_time.timestamp() * 1000
                )
                return int(create_time.timestamp() * 1000), int(duration)
        # When no execution context started,  start time and duration are not available
        # When multiple execution contexts stared on a run, not unable to know which context to use for create_time and duration
        return None, None

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
        properties["externalUrl"] = self._make_experiment_run_external_url(
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
            properties[f"input artifact ({input.name})"] = input.uri
        for output in execution.get_output_artifacts():
            properties[f"output artifact ({output.name})"] = output.uri

        return properties

    def _gen_run_execution(
        self, execution: Execution, run: ExperimentRun, exp: Experiment
    ) -> Iterable[MetadataChangeProposalWrapper]:
        create_time = execution.create_time
        update_time = execution.update_time
        duration = None
        if create_time and update_time:
            duration = datetime_to_ts_millis(update_time) - datetime_to_ts_millis(
                create_time
            )
        result_status: Union[str, RunResultTypeClass] = get_execution_result_status(
            execution.state
        )
        execution_urn = builder.make_data_process_instance_urn(
            self._make_vertexai_run_execution_name(execution.name)
        )

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=str(execution_urn),
            aspects=[
                DataProcessInstancePropertiesClass(
                    name=execution.name,
                    created=AuditStampClass(
                        time=datetime_to_ts_millis(create_time) if create_time else 0,
                        actor="urn:li:corpuser:datahub",
                    ),
                    externalUrl=self._make_artifact_external_url(
                        experiment=exp, run=run
                    ),
                    customProperties=self._make_custom_properties_for_execution(
                        execution
                    ),
                ),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_EXECUTION]),
                (
                    DataProcessInstanceRunEventClass(
                        status=DataProcessRunStatusClass.COMPLETE,
                        timestampMillis=datetime_to_ts_millis(create_time)
                        if create_time
                        else 0,
                        result=DataProcessInstanceRunResultClass(
                            type=result_status,
                            nativeResultType=self.platform,
                        ),
                        durationMillis=int(duration),
                    )
                    if is_status_for_run_event_class(result_status) and duration
                    else None
                ),
                DataProcessInstanceRelationships(
                    upstreamInstances=[self._make_experiment_run_urn(exp, run)],
                    parentInstance=self._make_experiment_run_urn(exp, run),
                ),
                DataProcessInstanceInputClass(
                    inputs=[],
                    inputEdges=[
                        EdgeClass(
                            destinationUrn=self._make_experiment_run_urn(exp, run)
                        ),
                    ],
                ),
            ],
        )

    def _gen_experiment_run_mcps(
        self, experiment: Experiment, run: ExperimentRun
    ) -> Iterable[MetadataChangeProposalWrapper]:
        experiment_key = ExperimentKey(
            platform=self.platform,
            id=self._make_vertexai_experiment_id(experiment.name),
        )
        run_urn = self._make_experiment_run_urn(experiment, run)
        created_time, duration = self._get_run_timestamps(run)
        created_actor = "urn:li:corpuser:datahub"
        run_result_type = self._get_run_result_status(run.get_state())

        for execution in run.get_executions():
            yield from self._gen_run_execution(
                execution=execution, exp=experiment, run=run
            )

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=run_urn,
            aspects=[
                DataProcessInstancePropertiesClass(
                    name=run.name,
                    created=AuditStampClass(
                        time=created_time if created_time else 0,
                        actor=created_actor,
                    ),
                    externalUrl=self._make_experiment_run_external_url(experiment, run),
                    customProperties=self._make_custom_properties_for_run(
                        experiment, run
                    ),
                ),
                ContainerClass(container=experiment_key.as_urn()),
                MLTrainingRunPropertiesClass(
                    hyperParams=self._get_experiment_run_params(run),
                    trainingMetrics=self._get_experiment_run_metrics(run),
                    externalUrl=self._make_experiment_run_external_url(experiment, run),
                    id=f"{experiment.name}-{run.name}",
                ),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_EXPERIMENT_RUN]),
                (
                    DataProcessInstanceRunEventClass(
                        status=DataProcessRunStatusClass.COMPLETE,
                        timestampMillis=created_time
                        if created_time
                        else 0,  # None is not allowed, 0 as default value
                        result=DataProcessInstanceRunResultClass(
                            type=run_result_type,
                            nativeResultType=self.platform,
                        ),
                        durationMillis=duration if duration else None,
                    )
                    if is_status_for_run_event_class(run_result_type)
                    else None
                ),
            ],
        )

    def _gen_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._get_project_container(),
            name=self._get_current_project_id(),
            sub_types=[MLAssetSubTypes.VERTEX_PROJECT],
        )

    def _get_ml_models_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch models from Vertex AI Model Registry and generate MLModelGroup + MLModel MCPs."""
        registered_models = _call_with_retry(self.client.Model.list)
        for model in registered_models:
            try:
                yield from self._gen_ml_group_mcps(model)
                model_versions = _call_with_retry(
                    model.versioning_registry.list_versions
                )
                for model_version in model_versions:
                    logger.info(
                        "Ingesting a model (name: %s id:%s)",
                        model.display_name,
                        model.name,
                    )
                    yield from self._get_ml_model_mcps(
                        model=model, model_version=model_version
                    )
            except Exception as e:
                logger.warning("Failed to process model %s: %s", model.display_name, e)
                self.report.warning(
                    title=f"Failed to process model {model.display_name}",
                    message=str(e),
                    exc=e,
                )

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataChangeProposalWrapper]:
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

    def _get_training_jobs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch Vertex AI training jobs (Custom and AutoML) and generate DataJob MCPs with lineage."""
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
        for class_name in class_names:
            try:
                logger.info("Fetching a list of %ss from VertexAI server", class_name)
                jobs = _call_with_retry(getattr(self.client, class_name).list)
                for job in jobs:
                    yield from self._get_training_job_mcps(job)
            except Exception as e:
                logger.warning(
                    "Failed to fetch %s for project %s: %s",
                    class_name,
                    self._current_project_id,
                    e,
                )
                self.report.warning(
                    title=f"Failed to fetch {class_name}",
                    message=f"Project: {self._current_project_id}. Error: {e}",
                    exc=e,
                )

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataChangeProposalWrapper]:
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job)
        yield from self._gen_training_job_mcps(job_meta)
        yield from self._get_input_dataset_mcps(job_meta)
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
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Generate DataJob MCPs with input dataset and output model lineage."""
        job = job_meta.job
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        created_time = datetime_to_ts_millis(job.create_time) if job.create_time else 0
        duration = self._get_job_duration_millis(job)

        dataset_urn = (
            builder.make_dataset_urn(
                platform=self.platform,
                name=self._make_vertexai_dataset_name(
                    entity_id=job_meta.input_dataset.name
                ),
                env=self.config.env,
            )
            if job_meta.input_dataset
            else None
        )
        model_urn = (
            self._make_ml_model_urn(
                model_version=job_meta.output_model_version,
                model_name=self._make_vertexai_model_name(
                    entity_id=job_meta.output_model.name
                ),
            )
            if job_meta.output_model and job_meta.output_model_version
            else None
        )

        result_type = get_job_result_status(job)

        yield from MetadataChangeProposalWrapper.construct_many(
            job_urn,
            aspects=[
                DataProcessInstancePropertiesClass(
                    name=job.display_name,
                    created=AuditStampClass(
                        time=created_time,
                        actor="urn:li:corpuser:datahub",
                    ),
                    externalUrl=self._make_job_external_url(job),
                    customProperties={
                        "jobType": job.__class__.__name__,
                    },
                ),
                MLTrainingRunProperties(
                    externalUrl=self._make_job_external_url(job), id=job.name
                ),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_TRAINING_JOB]),
                ContainerClass(container=self._get_project_container().as_urn()),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
                (
                    DataProcessInstanceInputClass(
                        inputs=[],
                        inputEdges=[
                            EdgeClass(destinationUrn=dataset_urn),
                        ],
                    )
                    if dataset_urn
                    else None
                ),
                (
                    DataProcessInstanceOutputClass(
                        outputs=[],
                        outputEdges=[
                            EdgeClass(destinationUrn=model_urn),
                        ],
                    )
                    if model_urn
                    else None
                ),
                (
                    DataProcessInstanceRunEventClass(
                        status=DataProcessRunStatusClass.COMPLETE,
                        timestampMillis=created_time,
                        result=DataProcessInstanceRunResultClass(
                            type=result_type,
                            nativeResultType=self.platform,
                        ),
                        durationMillis=duration,
                    )
                    if is_status_for_run_event_class(result_type) and duration
                    else None
                ),
            ],
        )

    def _gen_ml_group_mcps(
        self,
        model: Model,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Generate MLModelGroup MCP (groups all versions of this model)."""
        ml_model_group_urn = self._make_ml_model_group_urn(model)

        yield from MetadataChangeProposalWrapper.construct_many(
            ml_model_group_urn,
            aspects=[
                MLModelGroupPropertiesClass(
                    name=model.display_name,
                    description=model.description,
                    created=(
                        TimeStampClass(
                            time=datetime_to_ts_millis(model.create_time),
                            actor="urn:li:corpuser:datahub",
                        )
                        if model.create_time
                        else None
                    ),
                    lastModified=(
                        TimeStampClass(
                            time=datetime_to_ts_millis(model.update_time),
                            actor="urn:li:corpuser:datahub",
                        )
                        if model.update_time
                        else None
                    ),
                    customProperties=None,
                    externalUrl=self._make_model_external_url(model),
                ),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_MODEL_GROUP]),
                ContainerClass(container=self._get_project_container().as_urn()),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
            ],
        )

    def _make_ml_model_group_urn(self, model: Model) -> str:
        urn = builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._make_vertexai_model_group_name(model.name),
            env=self.config.env,
        )
        return urn

    def _get_project_container(self) -> ProjectIdKey:
        return ProjectIdKey(
            project_id=self._get_current_project_id(), platform=self.platform
        )

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
        versions = _call_with_retry(model.versioning_registry.list_versions)
        for version in versions:
            if version.version_id == version_id:
                return version
        return None

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
        """Search for a dataset by ID across all Vertex AI dataset types."""
        dataset_types = [
            "TextDataset",
            "TabularDataset",
            "ImageDataset",
            "TimeSeriesDataset",
            "VideoDataset",
        ]

        if self.datasets is None:
            self.datasets = {}

            for dtype in dataset_types:
                dataset_class = getattr(self.client.datasets, dtype)
                datasets = _call_with_retry(dataset_class.list)
                for ds in datasets:
                    self.datasets[ds.name] = ds

        return self.datasets.get(dataset_id)

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Generate Dataset MCP with DatasetProperties."""
        ds = job_meta.input_dataset

        if ds:
            dataset_name = self._make_vertexai_dataset_name(entity_id=ds.name)
            dataset_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=dataset_name,
                env=self.config.env,
            )

            yield from MetadataChangeProposalWrapper.construct_many(
                dataset_urn,
                aspects=[
                    DatasetPropertiesClass(
                        name=ds.display_name,
                        created=(
                            TimeStampClass(time=datetime_to_ts_millis(ds.create_time))
                            if ds.create_time
                            else None
                        ),
                        description=ds.display_name,
                        customProperties={
                            "resourceName": ds.resource_name,
                        },
                        qualifiedName=ds.resource_name,
                    ),
                    SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_DATASET]),
                    ContainerClass(container=self._get_project_container().as_urn()),
                    DataPlatformInstanceClass(
                        platform=str(DataPlatformUrn(self.platform))
                    ),
                ],
            )

    def _get_training_job_metadata(
        self, job: VertexAiResourceNoun
    ) -> TrainingJobMetadata:
        """Extract input dataset IDs and output model names from training job metadata."""
        job_meta = TrainingJobMetadata(job=job)
        if self._is_automl_job(job):
            job_conf = job.to_dict()
            if (
                "inputDataConfig" in job_conf
                and "datasetId" in job_conf["inputDataConfig"]
            ):
                dataset_id = job_conf["inputDataConfig"]["datasetId"]
                logger.info(
                    "Found input dataset (id: %s) for training job (%s)",
                    dataset_id,
                    job.display_name,
                )

                if dataset_id:
                    input_ds = self._search_dataset(dataset_id)
                    if input_ds:
                        logger.info(
                            "Found the name of input dataset (%s) with dataset id (%s)",
                            input_ds.display_name,
                            dataset_id,
                        )
                        job_meta.input_dataset = input_ds

            if (
                "modelToUpload" in job_conf
                and "name" in job_conf["modelToUpload"]
                and job_conf["modelToUpload"]["name"]
                and job_conf["modelToUpload"]["versionId"]
            ):
                model_name = job_conf["modelToUpload"]["name"]
                model_version_str = job_conf["modelToUpload"]["versionId"]
                try:
                    model = _call_with_retry(Model, model_name=model_name)
                    model_version = self._search_model_version(model, model_version_str)
                    if model and model_version:
                        logger.info(
                            "Found output model (name:%s id:%s) for training job: %s",
                            model.display_name,
                            model_version_str,
                            job.display_name,
                        )
                        job_meta.output_model = model
                        job_meta.output_model_version = model_version
                except PermissionDenied as e:
                    logger.warning(
                        "Permission denied accessing model '%s'. "
                        "Ensure service account has 'aiplatform.models.get' permission. "
                        "Error: %s",
                        model_name,
                        str(e),
                    )
                    self.report.warning(
                        title=f"No permission to access model {model_name}",
                        message=f"Check IAM permissions. Error: {str(e)}",
                    )
                except NotFound:
                    logger.info(
                        "Model '%s' or version '%s' not found. "
                        "This may be expected if the model was deleted.",
                        model_name,
                        model_version_str,
                    )
                except GoogleAPICallError as e:
                    # Transient errors (ResourceExhausted, DeadlineExceeded, ServiceUnavailable)
                    # are already retried by _call_with_retry above. If we get here, retries
                    # were exhausted or it's a non-transient error.
                    self._handle_api_error(e, "fetch model", "model", model_name)
                    error_code = getattr(e, "code", "unknown")
                    self.report.failure(
                        title=f"API error fetching model: {type(e).__name__}",
                        message=f"Error (status={error_code}): {str(e)}",
                    )

        return job_meta

    def _gen_endpoints_mcps(
        self, model_meta: ModelMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        model: Model = model_meta.model
        model_version: VersionInfo = model_meta.model_version

        if model_meta.endpoints:
            for endpoint in model_meta.endpoints:
                endpoint_urn = builder.make_ml_model_deployment_urn(
                    platform=self.platform,
                    deployment_name=self._make_vertexai_endpoint_name(
                        entity_id=endpoint.name
                    ),
                    env=self.config.env,
                )

                yield from MetadataChangeProposalWrapper.construct_many(
                    entityUrn=endpoint_urn,
                    aspects=[
                        MLModelDeploymentPropertiesClass(
                            description=model.description,
                            createdAt=datetime_to_ts_millis(endpoint.create_time),
                            version=VersionTagClass(
                                versionTag=str(model_version.version_id)
                            ),
                            customProperties={"displayName": endpoint.display_name},
                        ),
                        ContainerClass(
                            container=self._get_project_container().as_urn()
                        ),
                        # TODO add Subtype when metadata for MLModelDeployment is updated (not supported)
                        # SubTypesClass(typeNames=[MLTypes.ENDPOINT])
                    ],
                )

    def _gen_ml_model_mcps(
        self, ModelMetadata: ModelMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Generate MLModel MCP with deployment endpoint lineage."""
        model: Model = ModelMetadata.model
        model_version: VersionInfo = ModelMetadata.model_version
        training_job_urn: Optional[str] = ModelMetadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = ModelMetadata.endpoints
        endpoint_urns: List[str] = list()

        logger.info("generating model mcp for %s", model.name)

        if endpoints:
            for endpoint in endpoints:
                logger.info(
                    "found endpoint (%s) for model (%s)",
                    endpoint.display_name,
                    model.resource_name,
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

        model_group_urn = self._make_ml_model_group_urn(model)
        model_name = self._make_vertexai_model_name(entity_id=model.name)
        model_urn = self._make_ml_model_urn(model_version, model_name=model_name)

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=model_urn,
            aspects=[
                MLModelPropertiesClass(
                    name=f"{model.display_name}_{model_version.version_id}",
                    description=model_version.version_description,
                    customProperties={
                        "versionId": f"{model_version.version_id}",
                        "resourceName": model.resource_name,
                    },
                    created=(
                        TimeStampClass(
                            time=datetime_to_ts_millis(
                                model_version.version_create_time
                            ),
                            actor="urn:li:corpuser:datahub",
                        )
                        if model_version.version_create_time
                        else None
                    ),
                    lastModified=(
                        TimeStampClass(
                            time=datetime_to_ts_millis(
                                model_version.version_update_time
                            ),
                            actor="urn:li:corpuser:datahub",
                        )
                        if model_version.version_update_time
                        else None
                    ),
                    version=VersionTagClass(versionTag=str(model_version.version_id)),
                    groups=[model_group_urn],
                    trainingJobs=[training_job_urn] if training_job_urn else None,
                    deployments=endpoint_urns,
                    externalUrl=self._make_model_version_external_url(model),
                    type="ML Model",
                ),
                ContainerClass(
                    container=self._get_project_container().as_urn(),
                ),
                SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_MODEL]),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
                VersionPropertiesClass(
                    version=VersionTagClass(
                        versionTag=str(model_version.version_id),
                        metadataAttribution=(
                            MetadataAttributionClass(
                                time=int(
                                    model_version.version_create_time.timestamp() * 1000
                                ),
                                actor="urn:li:corpuser:datahub",
                            )
                            if model_version.version_create_time
                            else None
                        ),
                    ),
                    versionSet=str(self._get_version_set_urn(model)),
                    sortId=str(model_version.version_id).zfill(10),
                    aliases=None,
                ),
            ],
        )

    def _get_version_set_urn(self, model: Model) -> VersionSetUrn:
        guid_dict = {"platform": self.platform, "name": model.name}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=MlModelUrn.ENTITY_TYPE,
        )
        return version_set_urn

    def _search_endpoint(self, model: Model) -> List[Endpoint]:
        """Find the Vertex AI Endpoints where this model is deployed, if any."""
        if self.endpoints is None:
            endpoint_dict: Dict[str, List[Endpoint]] = {}
            endpoints = _call_with_retry(self.client.Endpoint.list)
            for endpoint in endpoints:
                for resource in endpoint.list_models():
                    if resource.model not in endpoint_dict:
                        endpoint_dict[resource.model] = []
                    endpoint_dict[resource.model].append(endpoint)
            self.endpoints = endpoint_dict

        return self.endpoints.get(model.resource_name, [])

    def _make_experiment_run_urn(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        return builder.make_data_process_instance_urn(
            self._make_vertexai_experiment_run_name(
                entity_id=f"{experiment.name}-{run.name}"
            )
        )

    def _make_ml_model_urn(self, model_version: VersionInfo, model_name: str) -> str:
        urn = builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_name}_{model_version.version_id}",
            env=self.config.env,
        )
        return urn

    def _make_training_job_urn(self, job: VertexAiResourceNoun) -> str:
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        urn = builder.make_data_process_instance_urn(dataProcessInstanceId=job_id)
        return urn

    def _get_current_project_id(self) -> str:
        assert self._current_project_id is not None, "Bug: project ID not initialized"
        return self._current_project_id

    def _make_resource_name(self, resource_type: str, entity_id: Optional[str]) -> str:
        """Generate a namespaced resource name: {project_id}.{resource_type}.{entity_id}"""
        return f"{self._get_current_project_id()}.{resource_type}.{entity_id}"

    def _make_vertexai_model_group_name(self, entity_id: str) -> str:
        return self._make_resource_name("model_group", entity_id)

    def _make_vertexai_endpoint_name(self, entity_id: str) -> str:
        return self._make_resource_name("endpoint", entity_id)

    def _make_vertexai_model_name(self, entity_id: str) -> str:
        return self._make_resource_name("model", entity_id)

    def _make_vertexai_dataset_name(self, entity_id: str) -> str:
        return self._make_resource_name("dataset", entity_id)

    def _make_vertexai_job_name(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("job", entity_id)

    def _make_vertexai_experiment_id(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("experiment", entity_id)

    def _make_vertexai_experiment_run_name(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("experiment_run", entity_id)

    def _make_vertexai_run_execution_name(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("execution", entity_id)

    def _make_vertexai_pipeline_id(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("pipeline", entity_id)

    def _make_vertexai_pipeline_task_id(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("pipeline_task", entity_id)

    def _make_vertexai_pipeline_task_run_id(self, entity_id: Optional[str]) -> str:
        return self._make_resource_name("pipeline_task_run", entity_id)

    def _make_artifact_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/test-experiment/runs/test-run-3/artifacts?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}/runs/{experiment.name}-{run.name}/artifacts"
            f"?project={project_id}"
        )

    def _make_job_external_url(self, job: VertexAiResourceNoun) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/training/training-pipelines?project=my-project&trainingPipelineId=5401695018589093888
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/training/training-pipelines?trainingPipelineId={job.name}"
            f"?project={project_id}"
        )

    def _make_model_external_url(self, model: Model) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"?project={project_id}"
        )

    def _make_model_version_external_url(self, model: Model) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"/versions/{model.version_id}"
            f"?project={project_id}"
        )

    def _make_experiment_external_url(self, experiment: Experiment) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/my-experiment/runs?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}"
            f"/runs?project={project_id}"
        )

    def _make_experiment_run_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/my-experiment/runs/my-run-1/charts?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}"
            f"/runs/{experiment.name}-{run.name}/charts?project={project_id}"
        )

    def _make_pipeline_external_url(self, pipeline_name: str) -> str:
        """
        Example: https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/pipeline-example-tasks-20250320210739?project=my-project
        """
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/pipelines/locations/{self.config.region}/runs/{pipeline_name}"
            f"?project={project_id}"
        )
