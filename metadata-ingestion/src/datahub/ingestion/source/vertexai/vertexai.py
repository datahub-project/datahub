import dataclasses
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from google.api_core.exceptions import (
    FailedPrecondition,
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
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
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
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


def _is_config_error(exc: GoogleAPICallError) -> bool:
    if isinstance(exc, (InvalidArgument, FailedPrecondition)):
        return True
    # Fallback: check error message for known config issues
    msg = str(exc).lower()
    return "api" in msg and "not enabled" in msg


def _cleanup_credentials(credentials_path: Optional[str]) -> None:
    if credentials_path is not None:
        try:
            os.unlink(credentials_path)
            logger.debug("Cleaned up temp credentials file: %s", credentials_path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(
                "Failed to cleanup temp credentials file %s: %s. Check filesystem permissions.",
                credentials_path,
                e,
            )


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
        # Cache is safe: source instances are short-lived (one ingestion run)
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

        logger.info("Will process %d project(s)", len(self._projects))
        return self._projects

    def _init_for_project(self, project: GCPProject) -> None:
        self._current_project_id = project.id

        aiplatform.init(
            project=project.id,
            location=self.config.region,
        )
        self.client = aiplatform

        # Reset caches: each project's data must be fetched fresh to avoid
        # returning stale data from a previous project in a multi-project run
        self.endpoints = None
        self.datasets = None
        self.experiments = None

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        _cleanup_credentials(self.config._credentials_path)
        super().close()

    def _build_no_projects_error(self) -> str:
        if self.config.project_ids:
            mode = f"project_ids: {self.config.project_ids}"
            tips = "Verify IDs exist; check project_id_pattern"
        elif self.config.project_labels:
            mode = f"project_labels: {self.config.project_labels}"
            tips = "Verify labels exist; check permissions and project_id_pattern"
        else:
            mode = "auto-discovery"
            tips = "Check resourcemanager.projects.list permission; check project_id_pattern"

        return (
            f"No projects to process. Mode: {mode}, allow: {self.config.project_id_pattern.allow}, "
            f"deny: {self.config.project_id_pattern.deny}. {tips}"
        )

    def _handle_project_error(self, project_id: str, exc: Exception) -> None:
        error_configs = {
            NotFound: (
                "Project not found",
                f"gcloud projects describe {project_id}",
                logger.error,
            ),
            PermissionDenied: (
                "Permission denied",
                f"gcloud projects get-iam-policy {project_id}",
                logger.warning,
            ),
        }
        label, debug_cmd, log_fn = error_configs.get(
            type(exc),
            (
                "GCP API error",
                f"gcloud ai models list --project={project_id} --region={self.config.region}",
                logger.warning,
            ),
        )

        title = f"{label}: {project_id}"
        message = f"{label} for project {project_id}. Debug: {debug_cmd}"

        log_fn("Project %s: %s", project_id, message)
        self.report.failure(title=title, message=message, exc=exc)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        projects = self._get_projects_to_process()

        if not projects:
            raise RuntimeError(self._build_no_projects_error())

        explicit_projects = self.config.has_explicit_project_ids()
        successful_projects = 0
        failed_projects: List[str] = []

        for project in projects:
            logger.info("Processing Vertex AI resources for project: %s", project.id)
            try:
                self._init_for_project(project)
                yield from self._process_current_project()
                successful_projects += 1
            except NotFound as e:
                if explicit_projects:
                    raise RuntimeError(
                        f"Project '{project.id}' not found. This project was explicitly configured "
                        f"in project_ids - check for typos. Debug: gcloud projects describe {project.id}"
                    ) from e
                failed_projects.append(project.id)
                self._handle_project_error(project.id, e)
                continue
            except PermissionDenied as e:
                if explicit_projects:
                    raise RuntimeError(
                        f"Permission denied for project '{project.id}'. This project was explicitly "
                        f"configured in project_ids. Ensure the service account has "
                        f"roles/aiplatform.viewer. Debug: gcloud projects get-iam-policy {project.id}"
                    ) from e
                failed_projects.append(project.id)
                self._handle_project_error(project.id, e)
                continue
            except GoogleAPICallError as e:
                if _is_config_error(e):
                    raise RuntimeError(
                        f"Configuration error for project {project.id}: {e}. "
                        "Check that Vertex AI API is enabled and region is valid."
                    ) from e
                failed_projects.append(project.id)
                self._handle_project_error(project.id, e)
                continue

        if failed_projects:
            if successful_projects == 0:
                raise RuntimeError(
                    f"All {len(failed_projects)} projects failed to process: {failed_projects}. "
                    "Check the failure reports above for specific errors per project. "
                    "Common causes: (1) Service account lacks roles/aiplatform.viewer, "
                    "(2) Project IDs are incorrect, (3) Vertex AI API not enabled in projects."
                )
            logger.warning(
                "=" * 60 + "\n"
                "PARTIAL INGESTION: %d of %d projects failed!\n"
                "Failed projects: %s\n"
                "Data from these projects is MISSING from this ingestion run.\n"
                "Review failures above and re-run if needed.\n" + "=" * 60,
                len(failed_projects),
                len(projects),
                failed_projects,
            )
            self.report.warning(
                title=f"PARTIAL INGESTION: {len(failed_projects)}/{len(projects)} projects failed",
                message=f"Failed: {', '.join(failed_projects)}. Data from these projects is missing.",
            )

    def _process_current_project(self) -> Iterable[MetadataWorkUnit]:
        yield from self._gen_project_workunits()
        yield from auto_workunit(self._get_ml_models_mcps())
        yield from auto_workunit(self._get_training_jobs_mcps())
        yield from self._get_experiments_workunits()
        yield from auto_workunit(self._get_experiment_runs_mcps())
        yield from auto_workunit(self._get_pipelines_mcps())

    def _get_pipelines_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        pipeline_jobs = self.client.PipelineJob.list()

        for pipeline in pipeline_jobs:
            logger.info("Fetching pipeline: %s", pipeline.name)
            pipeline_meta = self._get_pipeline_metadata(pipeline)
            yield from self._get_pipeline_mcps(pipeline_meta)
            yield from self._gen_pipeline_task_mcps(pipeline_meta)

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
        self.experiments = aiplatform.Experiment.list()

        logger.info("Fetching experiments from VertexAI server")
        for experiment in self.experiments:
            yield from self._gen_experiment_workunits(experiment)

    def _get_experiment_runs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.experiments is None:
            self.experiments = aiplatform.Experiment.list()
        for experiment in self.experiments:
            logger.info("Fetching experiment runs for experiment: %s", experiment.name)
            experiment_runs = aiplatform.ExperimentRun.list(experiment=experiment.name)
            for run in experiment_runs:
                yield from self._gen_experiment_run_mcps(experiment, run)

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
        registered_models = self.client.Model.list()
        for model in registered_models:
            # create mcp for Model Group (= Model in VertexAI)
            yield from self._gen_ml_group_mcps(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create mcp for Model (= Model Version in VertexAI)
                logger.info(
                    "Ingesting a model (name: %s id:%s)",
                    model.display_name,
                    model.name,
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
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
            logger.info("Fetching a list of %ss from VertexAI server", class_name)
            for job in getattr(self.client, class_name).list():
                yield from self._get_training_job_mcps(job)

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
        for version in model.versioning_registry.list_versions():
            if version.version_id == version_id:
                return version
        return None

    def _search_dataset(self, dataset_id: str) -> Optional[VertexAiResourceNoun]:
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
                for ds in dataset_class.list():
                    self.datasets[ds.name] = ds

        return self.datasets.get(dataset_id)

    def _get_input_dataset_mcps(
        self, job_meta: TrainingJobMetadata
    ) -> Iterable[MetadataChangeProposalWrapper]:
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
                    model = Model(model_name=model_name)
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
                except GoogleAPICallError as e:
                    self.report.failure(
                        title="Unable to fetch model and model version",
                        message="Failed to fetch output model/version for training job",
                        exc=e,
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
        if self.endpoints is None:
            endpoint_dict: Dict[str, List[Endpoint]] = {}
            for endpoint in self.client.Endpoint.list():
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
        if self._current_project_id is None:
            raise RuntimeError(
                "Internal error: attempted to access project ID before initialization. "
                "This is a bug in the VertexAI source. Please report this issue."
            )
        return self._current_project_id

    def _make_vertexai_model_group_name(
        self,
        entity_id: str,
    ) -> str:
        return f"{self._get_current_project_id()}.model_group.{entity_id}"

    def _make_vertexai_endpoint_name(self, entity_id: str) -> str:
        return f"{self._get_current_project_id()}.endpoint.{entity_id}"

    def _make_vertexai_model_name(self, entity_id: str) -> str:
        return f"{self._get_current_project_id()}.model.{entity_id}"

    def _make_vertexai_dataset_name(self, entity_id: str) -> str:
        return f"{self._get_current_project_id()}.dataset.{entity_id}"

    def _make_vertexai_job_name(
        self,
        entity_id: Optional[str],
    ) -> str:
        return f"{self._get_current_project_id()}.job.{entity_id}"

    def _make_vertexai_experiment_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.experiment.{entity_id}"

    def _make_vertexai_experiment_run_name(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.experiment_run.{entity_id}"

    def _make_vertexai_run_execution_name(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.execution.{entity_id}"

    def _make_vertexai_pipeline_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.pipeline.{entity_id}"

    def _make_vertexai_pipeline_task_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.pipeline_task.{entity_id}"

    def _make_vertexai_pipeline_task_run_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_current_project_id()}.pipeline_task_run.{entity_id}"

    def _make_artifact_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}/runs/{experiment.name}-{run.name}/artifacts"
            f"?project={project_id}"
        )

    def _make_job_external_url(self, job: VertexAiResourceNoun) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/training/training-pipelines?trainingPipelineId={job.name}"
            f"?project={project_id}"
        )

    def _make_model_external_url(self, model: Model) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"?project={project_id}"
        )

    def _make_model_version_external_url(self, model: Model) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/models/locations/{self.config.region}/models/{model.name}"
            f"/versions/{model.version_id}"
            f"?project={project_id}"
        )

    def _make_experiment_external_url(self, experiment: Experiment) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}"
            f"/runs?project={project_id}"
        )

    def _make_experiment_run_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/experiments/locations/{self.config.region}/experiments/{experiment.name}"
            f"/runs/{experiment.name}-{run.name}/charts?project={project_id}"
        )

    def _make_pipeline_external_url(self, pipeline_name: str) -> str:
        project_id = self._get_current_project_id()
        return (
            f"{self.config.vertexai_url}/pipelines/locations/{self.config.region}/runs/{pipeline_name}"
            f"?project={project_id}"
        )
