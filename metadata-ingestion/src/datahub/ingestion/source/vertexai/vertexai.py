import dataclasses
import logging
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple, TypeVar, Union

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
from google.oauth2 import service_account
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
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    resolve_gcp_projects,
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


@dataclasses.dataclass
class TrainingJobMetadata:
    job: VertexAiResourceNoun
    input_dataset: Optional[VertexAiResourceNoun] = None
    output_model: Optional[Model] = None
    output_model_version: Optional[VersionInfo] = None
    external_input_urns: Optional[List[str]] = None
    external_output_urns: Optional[List[str]] = None


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
            self._projects = resolve_gcp_projects(filter_cfg, self.report)
            if not self._projects and config.project_id:
                # Fallback to single project for robustness
                self._projects = [config.project_id]
        else:
            self._projects = [config.project_id]

        # Determine regions per project
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

        # Initialize client once; will re-init per project/region in get_workunits
        self.client = aiplatform
        self.endpoints: Optional[Dict[str, List[Endpoint]]] = None
        self.datasets: Optional[Dict[str, VertexAiResourceNoun]] = None
        self.experiments: Optional[List[Experiment]] = None

    def get_report(self) -> SourceReport:
        return self.report

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
                # set context for downstream id/url builders
                self._current_project_id = project_id
                self._current_region = region
                # reset caches per project/region
                self.endpoints = None
                self.datasets = None
                self.experiments = None

                # Ingest Project
                yield from self._gen_project_workunits()
                # Fetch and Ingest Models, Model Versions from Model Registry
                if self.config.include_models:
                    yield from auto_workunit(self._get_ml_models_mcps())
                # Fetch and Ingest Training Jobs
                if self.config.include_training_jobs:
                    yield from auto_workunit(self._get_training_jobs_mcps())
                # Fetch and Ingest Experiments
                if self.config.include_experiments:
                    yield from self._get_experiments_workunits()
                    # Fetch and Ingest Experiment Runs
                    yield from auto_workunit(self._get_experiment_runs_mcps())
                # Fetch Pipelines and Tasks
                if self.config.include_pipelines:
                    yield from auto_workunit(self._get_pipelines_mcps())

    def _get_pipelines_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Fetches pipelines from Vertex AI and generates corresponding mcps.
        """

        pipeline_jobs = self.client.PipelineJob.list()

        for pipeline in pipeline_jobs:
            logger.info(f"fetching pipeline ({pipeline.name})")
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
                    f"fetching pipeline task ({task_name}) in pipeline ({pipeline.name})"
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
                    if task_detail.end_time:
                        task_meta.end_time = task_detail.end_time
                        if task_meta.start_time and task_meta.end_time:
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
    ) -> (Iterable)[MetadataChangeProposalWrapper]:
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
        # List all experiments
        exps = aiplatform.Experiment.list()
        filtered = [
            e for e in exps if self.config.experiment_name_pattern.allowed(e.name)
        ]
        if self.config.max_experiments is not None:
            filtered = filtered[: self.config.max_experiments]
        self.experiments = filtered

        logger.info("Fetching experiments from VertexAI server")
        for experiment in self.experiments:
            yield from self._gen_experiment_workunits(experiment)

    def _get_experiment_runs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.experiments is None:
            self.experiments = [
                e
                for e in aiplatform.Experiment.list()
                if self.config.experiment_name_pattern.allowed(e.name)
            ]
        for experiment in self.experiments:
            logger.info(f"Fetching experiment runs for experiment {experiment.name}")
            experiment_runs = aiplatform.ExperimentRun.list(experiment=experiment.name)
            if self.config.max_runs_per_experiment is not None:
                experiment_runs = experiment_runs[: self.config.max_runs_per_experiment]
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
            duration = update_time.timestamp() * 1000 - create_time.timestamp() * 1000
            return int(create_time.timestamp() * 1000), int(duration)
        # When no execution context started,  start time and duration are not available
        # When multiple execution contexts stared on a run, not unable to know which context to use for create_time and duration
        else:
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
            input_name = getattr(input, "name", "")
            properties[f"input artifact ({input_name})"] = getattr(input, "uri", "")
        for output in execution.get_output_artifacts():
            output_name = getattr(output, "name", "")
            properties[f"output artifact ({output_name})"] = getattr(output, "uri", "")

        return properties

    def _gen_run_execution(
        self, execution: Execution, run: ExperimentRun, exp: Experiment
    ) -> Iterable[MetadataChangeProposalWrapper]:
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
            self._make_vertexai_run_execution_name(execution.name)
        )

        # Build cross-platform lineage from artifact URIs
        input_edges = []
        output_edges = []
        for input_art in execution.get_input_artifacts():
            for ds_urn in self._dataset_urns_from_artifact_uri(input_art.uri):
                input_edges.append(EdgeClass(destinationUrn=ds_urn))
        for output_art in execution.get_output_artifacts():
            for ds_urn in self._dataset_urns_from_artifact_uri(output_art.uri):
                output_edges.append(EdgeClass(destinationUrn=ds_urn))

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=str(execution_urn),
            aspects=[
                DataProcessInstancePropertiesClass(
                    name=execution.name,
                    created=AuditStampClass(
                        time=datetime_to_ts_millis(create_time),
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
                        timestampMillis=(
                            datetime_to_ts_millis(create_time) if create_time else 0
                        ),
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
                (
                    DataProcessInstanceInputClass(
                        inputs=[],
                        inputEdges=input_edges,
                    )
                    if input_edges
                    else None
                ),
                (
                    DataProcessInstanceOutputClass(
                        outputs=[],
                        outputEdges=output_edges,
                    )
                    if output_edges
                    else None
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

        # generating mcps for run execution
        for execution in run.get_executions():
            yield from self._gen_run_execution(
                execution=execution, exp=experiment, run=run
            )

        # generating mcps for run
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
            name=self._get_project_id(),
            sub_types=[MLAssetSubTypes.VERTEX_PROJECT],
        )

    def _get_ml_models_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Fetch List of Models in Model Registry and generate a corresponding mcp.
        """
        registered_models = self.client.Model.list()
        count = 0
        for model in registered_models:
            if not self.config.model_name_pattern.allowed(model.display_name or ""):
                continue
            # create mcp for Model Group (= Model in VertexAI)
            yield from self._gen_ml_group_mcps(model)
            model_versions = model.versioning_registry.list_versions()
            for model_version in model_versions:
                # create mcp for Model (= Model Version in VertexAI)
                logger.info(
                    f"Ingesting a model (name: {model.display_name} id:{model.name})"
                )
                yield from self._get_ml_model_mcps(
                    model=model, model_version=model_version
                )
            count += 1
            if self.config.max_models is not None and count >= self.config.max_models:
                break

    def _get_ml_model_mcps(
        self, model: Model, model_version: VersionInfo
    ) -> Iterable[MetadataChangeProposalWrapper]:
        model_meta: ModelMetadata = self._get_ml_model_metadata(model, model_version)
        # Create ML Model Entity
        yield from self._gen_ml_model_mcps(model_meta)
        # Create Endpoint Entity
        yield from self._gen_endpoints_mcps(model_meta)

    def _get_ml_model_metadata(
        self, model: Model, model_version: VersionInfo
    ) -> ModelMetadata:
        model_meta = ModelMetadata(model=model, model_version=model_version)
        # Search for endpoints associated with the model
        endpoints = self._search_endpoint(model)
        model_meta.endpoints = endpoints
        return model_meta

    def _get_training_jobs_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Fetches training jobs from Vertex AI and generates corresponding mcps.
        This method retrieves various types of training jobs from Vertex AI, including
        CustomJob, CustomTrainingJob, CustomContainerTrainingJob, CustomPythonPackageTrainingJob,
        AutoMLTabularTrainingJob, AutoMLTextTrainingJob, AutoMLImageTrainingJob, AutoMLVideoTrainingJob,
        and AutoMLForecastingTrainingJob. For each job, it generates mcps containing metadata
        about the job, its inputs, and its outputs.
        """
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
        # Iterate over class names and call the list() function
        for class_name in class_names:
            if not self.config.training_job_type_pattern.allowed(class_name):
                continue
            logger.info(f"Fetching a list of {class_name}s from VertexAI server")
            jobs = list(getattr(self.client, class_name).list())
            if self.config.max_training_jobs_per_type is not None:
                jobs = jobs[: self.config.max_training_jobs_per_type]
            for job in jobs:
                yield from self._get_training_job_mcps(job)

    def _get_training_job_mcps(
        self, job: VertexAiResourceNoun
    ) -> Iterable[MetadataChangeProposalWrapper]:
        job_meta: TrainingJobMetadata = self._get_training_job_metadata(job)
        # Extract external lineage from job config
        ext_input_urns, ext_output_urns = self._extract_external_uris_from_job(job)
        job_meta.external_input_urns = ext_input_urns
        job_meta.external_output_urns = ext_output_urns
        # Create DataProcessInstance for the training job
        yield from self._gen_training_job_mcps(job_meta)
        # Create Dataset entity for Input Dataset of Training job
        yield from self._get_input_dataset_mcps(job_meta)
        # Create ML Model entity for output ML model of this training job
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
        """
        Generate a mcp for VertexAI Training Job
        """
        job = job_meta.job
        job_id = self._make_vertexai_job_name(entity_id=job.name)
        job_urn = builder.make_data_process_instance_urn(job_id)

        created_time = datetime_to_ts_millis(job.create_time) if job.create_time else 0
        duration = self._get_job_duration_millis(job)

        # If Training job has Input Dataset
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
        # If Training Job has Output Model
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
        # External lineage edges
        external_input_edges = [
            EdgeClass(destinationUrn=u)
            for u in getattr(job_meta, "external_input_urns", []) or []
        ]
        external_output_edges = [
            EdgeClass(destinationUrn=u)
            for u in getattr(job_meta, "external_output_urns", []) or []
        ]

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
                        inputEdges=(
                            (
                                [EdgeClass(destinationUrn=dataset_urn)]
                                if dataset_urn
                                else []
                            )
                            + external_input_edges
                        ),
                    )
                    if (dataset_urn or external_input_edges)
                    else None
                ),
                (
                    DataProcessInstanceOutputClass(
                        outputs=[],
                        outputEdges=(
                            ([EdgeClass(destinationUrn=model_urn)] if model_urn else [])
                            + external_output_edges
                        ),
                    )
                    if (model_urn or external_output_edges)
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
        """
        Generate an MLModelGroup mcp for a VertexAI  Model.
        """
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
        return ProjectIdKey(project_id=self._get_project_id(), platform=self.platform)

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
        """
        Create a DatasetPropertiesClass aspect for a given Vertex AI dataset.
        """
        ds = job_meta.input_dataset

        if ds:
            # Create URN of Input Dataset for Training Job
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
        """
        Retrieve metadata for a given Vertex AI training job.
        This method extracts metadata for a Vertex AI training job, including input datasets
        and output models. It checks if the job is an AutoML job and retrieves the relevant
        input dataset and output model information.
        """
        job_meta = TrainingJobMetadata(job=job)
        # Check if the job is an AutoML job
        if self._is_automl_job(job):
            job_conf = job.to_dict()
            # Check if input dataset is present in the job configuration
            if (
                "inputDataConfig" in job_conf
                and "datasetId" in job_conf["inputDataConfig"]
            ):
                # Create URN of Input Dataset for Training Job
                dataset_id = job_conf["inputDataConfig"]["datasetId"]
                logger.info(
                    f"Found input dataset (id: {dataset_id}) for training job ({job.display_name})"
                )

                if dataset_id:
                    input_ds = self._search_dataset(dataset_id)
                    if input_ds:
                        logger.info(
                            f"Found the name of input dataset ({input_ds.display_name}) with dataset id ({dataset_id})"
                        )
                        job_meta.input_dataset = input_ds

            # Check if output model is present in the job configuration
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
        """
        Generate an MLModel and Endpoint mcp for an VertexAI Model Version.
        """

        model: Model = ModelMetadata.model
        model_version: VersionInfo = ModelMetadata.model_version
        training_job_urn: Optional[str] = ModelMetadata.training_job_urn
        endpoints: Optional[List[Endpoint]] = ModelMetadata.endpoints
        endpoint_urns: List[str] = list()

        logging.info(f"generating model mcp for {model.name}")

        # Generate list of endpoint URL
        if endpoints:
            for endpoint in endpoints:
                logger.info(
                    f"found endpoint ({endpoint.display_name}) for model ({model.resource_name})"
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

        # Create URN for Model and Model Version
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
                    groups=[model_group_urn],  # link model version to model group
                    trainingJobs=(
                        [training_job_urn] if training_job_urn else None
                    ),  # link to training job
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

    def _make_experiment_run_urn(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        return builder.make_data_process_instance_urn(
            self._make_vertexai_experiment_run_name(
                entity_id=f"{experiment.name}-{run.name}"
            )
        )

    def _extract_external_uris_from_job(
        self, job: VertexAiResourceNoun
    ) -> Tuple[List[str], List[str]]:
        def looks_like_uri(s: str) -> bool:
            return s.startswith("gs://") or s.startswith("bq://") or "/datasets/" in s

        input_uris: List[str] = []
        output_uris: List[str] = []

        def walk(obj: object, key_path: List[str]) -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    walk(v, key_path + [str(k)])
            elif isinstance(obj, list):
                for v in obj:
                    walk(v, key_path)
            elif isinstance(obj, str) and looks_like_uri(obj):
                kp = ".".join([k.lower() for k in key_path])
                if any(tok in kp for tok in ["input", "source"]):
                    input_uris.append(obj)
                elif any(tok in kp for tok in ["output", "destination", "sink"]):
                    output_uris.append(obj)
                else:
                    # Default to outputs if ambiguous
                    output_uris.append(obj)

        try:
            job_conf = job.to_dict()
            walk(job_conf, [])
        except Exception:
            logger.debug(
                "Failed to parse training job config for external URIs", exc_info=True
            )

        input_urns: List[str] = []
        for uri in input_uris:
            input_urns.extend(self._dataset_urns_from_artifact_uri(uri))
        output_urns: List[str] = []
        for uri in output_uris:
            output_urns.extend(self._dataset_urns_from_artifact_uri(uri))

        return input_urns, output_urns

    def _dataset_urns_from_artifact_uri(self, uri: Optional[str]) -> List[str]:
        urns: List[str] = []
        if not uri:
            return urns
        try:
            if uri.startswith("gs://"):
                # Use full URI as the dataset name for GCS
                urns.append(
                    builder.make_dataset_urn(
                        platform="gcs", name=uri, env=self.config.env
                    )
                )
            elif uri.startswith("bq://"):
                # Format bq://project.dataset.table
                name = uri.replace("bq://", "")
                urns.append(
                    builder.make_dataset_urn(
                        platform="bigquery", name=name, env=self.config.env
                    )
                )
            elif "projects/" in uri and "datasets/" in uri and "tables/" in uri:
                # Format: projects/{project}/datasets/{dataset}/tables/{table}
                parts = uri.split("/")
                # crude parse with guards
                project = parts[parts.index("projects") + 1]
                dataset = parts[parts.index("datasets") + 1]
                table = parts[parts.index("tables") + 1]
                name = f"{project}.{dataset}.{table}"
                urns.append(
                    builder.make_dataset_urn(
                        platform="bigquery", name=name, env=self.config.env
                    )
                )
        except Exception:
            logger.debug(
                f"Could not parse artifact uri for lineage: {uri}", exc_info=True
            )
        return urns

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

    def _make_vertexai_model_group_name(
        self,
        entity_id: str,
    ) -> str:
        return f"{self._get_project_id()}.model_group.{entity_id}"

    def _make_vertexai_endpoint_name(self, entity_id: str) -> str:
        return f"{self._get_project_id()}.endpoint.{entity_id}"

    def _make_vertexai_model_name(self, entity_id: str) -> str:
        return f"{self._get_project_id()}.model.{entity_id}"

    def _make_vertexai_dataset_name(self, entity_id: str) -> str:
        return f"{self._get_project_id()}.dataset.{entity_id}"

    def _make_vertexai_job_name(
        self,
        entity_id: Optional[str],
    ) -> str:
        return f"{self._get_project_id()}.job.{entity_id}"

    def _make_vertexai_experiment_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.experiment.{entity_id}"

    def _make_vertexai_experiment_run_name(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.experiment_run.{entity_id}"

    def _make_vertexai_run_execution_name(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.execution.{entity_id}"

    def _make_vertexai_pipeline_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.pipeline.{entity_id}"

    def _make_vertexai_pipeline_task_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.pipeline_task.{entity_id}"

    def _make_vertexai_pipeline_task_run_id(self, entity_id: Optional[str]) -> str:
        return f"{self._get_project_id()}.pipeline_task_run.{entity_id}"

    def _make_artifact_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/test-experiment-job-metadata/runs/test-experiment-job-metadata-run-3/artifacts?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/experiments/locations/{self._get_region()}/experiments/{experiment.name}/runs/{experiment.name}-{run.name}/artifacts"
            f"?project={self._get_project_id()}"
        )
        return external_url

    def _make_job_external_url(self, job: VertexAiResourceNoun) -> str:
        """
        Model external URL in Vertex AI
        Sample URLs:
        https://console.cloud.google.com/vertex-ai/training/training-pipelines?project=acryl-poc&trainingPipelineId=5401695018589093888
        """
        external_url: str = (
            f"{self.config.vertexai_url}/training/training-pipelines?trainingPipelineId={job.name}"
            f"?project={self._get_project_id()}"
        )
        return external_url

    def _make_model_external_url(self, model: Model) -> str:
        """
        Model external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/models/locations/{self._get_region()}/models/{model.name}"
            f"?project={self._get_project_id()}"
        )
        return external_url

    def _make_model_version_external_url(self, model: Model) -> str:
        """
        Model Version external URL in Vertex AI
        Sample URL:
        https://console.cloud.google.com/vertex-ai/models/locations/us-west2/models/812468724182286336/versions/1?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/models/locations/{self._get_region()}/models/{model.name}"
            f"/versions/{model.version_id}"
            f"?project={self._get_project_id()}"
        )
        return external_url

    def _make_experiment_external_url(self, experiment: Experiment) -> str:
        """
        Experiment external URL in Vertex AI
        https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/experiment-run-with-automljob-1/runs?project=acryl-poc
        """

        external_url: str = (
            f"{self.config.vertexai_url}/experiments/locations/{self._get_region()}/experiments/{experiment.name}"
            f"/runs?project={self._get_project_id()}"
        )
        return external_url

    def _make_experiment_run_external_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        """
        Experiment Run external URL in Vertex AI
        https://console.cloud.google.com/vertex-ai/experiments/locations/us-west2/experiments/experiment-run-with-automljob-1/runs/experiment-run-with-automljob-1-automl-job-with-run-1/charts?project=acryl-poc
        """

        external_url: str = (
            f"{self.config.vertexai_url}/experiments/locations/{self._get_region()}/experiments/{experiment.name}"
            f"/runs/{experiment.name}-{run.name}/charts?project={self._get_project_id()}"
        )
        return external_url

    def _make_pipeline_external_url(self, pipeline_name: str) -> str:
        """
        Pipeline Run external URL in Vertex AI
        https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/pipeline-example-more-tasks-3-20250320210739?project=acryl-poc
        """
        external_url: str = (
            f"{self.config.vertexai_url}/pipelines/locations/{self._get_region()}/runs/{pipeline_name}"
            f"?project={self._get_project_id()}"
        )
        return external_url
