import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus

from pydantic import ValidationError
from requests import HTTPError, RequestException
from requests.exceptions import ConnectionError, Timeout

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    FlowContainerSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    API_MAX_PAGE_SIZE,
    API_TIMEOUT_TUNING_GUIDANCE,
    DPI_TYPE_BATCH_AD_HOC,
    DPI_TYPE_BATCH_SCHEDULED,
    MATILLION_DPI_OBSERVABILITY_URL,
    MATILLION_NAMESPACE_PREFIX,
    MATILLION_PIPELINE_OBSERVABILITY_URL,
    MATILLION_PLATFORM,
    MATILLION_TO_DATAHUB_RESULT_TYPE,
    MATILLION_TRIGGER_SCHEDULE,
    MAX_LINEAGE_DATE_RANGE_DAYS,
)
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion_dpc.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion_dpc.matillion_lineage import OpenLineageParser
from datahub.ingestion.source.matillion_dpc.matillion_streaming import (
    MatillionStreamingHandler,
)
from datahub.ingestion.source.matillion_dpc.matillion_utils import (
    MatillionUrnBuilder,
    build_data_job_custom_properties,
    extract_base_pipeline_name,
    extract_folder_segments,
    extract_pipeline_file_name,
    make_dataset_urn_from_matillion_dataset,
    make_execution_dpi_urn,
    make_step_dpi_urn,
    match_pipeline_name,
    parse_iso_timestamp,
)
from datahub.ingestion.source.matillion_dpc.models import (
    ExecutionWithSteps,
    MatillionDatasetInfo,
    MatillionEnvironment,
    MatillionJobNamespace,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionPipelineExecutionStepResult,
    MatillionProject,
    OutputLineageAccumulator,
    PipelineGroupKey,
    SqlAggregatorKey,
    StepLineage,
    StepTiming,
    TimeWindow,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataJobInputOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    RunResultTypeClass,
)
from datahub.metadata.urns import DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


@platform_name("Matillion", id="matillion-dpc")
@config_class(MatillionSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default via OpenLineage data from pipeline executions",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `parse_sql_for_lineage`",
)
class MatillionSource(StatefulIngestionSourceBase):
    config: MatillionSourceConfig
    report: MatillionSourceReport
    platform: str = MATILLION_PLATFORM

    def __init__(self, config: MatillionSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = MatillionSourceReport()
        self.api_client = MatillionAPIClient(self.config.api_config)
        self.urn_builder = MatillionUrnBuilder(self.config)
        self.container_handler = MatillionContainerHandler(
            self.config, self.report, self.urn_builder
        )
        self.streaming_handler = MatillionStreamingHandler(
            self.config, self.report, self.urn_builder, self.container_handler
        )

        self.openlineage_parser = OpenLineageParser(
            namespace_to_platform_instance=self.config.namespace_to_platform_instance,
            platform_mapping=self.config.lineage_platform_mapping,
            env=self.config.env,
        )

        # Keyed by platform/instance/env so datasets on the same platform but
        # different instances/environments don't share an aggregator.
        self._sql_aggregators: Dict[
            SqlAggregatorKey, Optional[SqlParsingAggregator]
        ] = {}
        self._registered_urns: set[str] = set()

        self._projects_cache: Dict[str, MatillionProject] = {}
        self._environments_cache: Dict[str, MatillionEnvironment] = {}
        self._schedules_cache: Dict[str, list] = {}
        self._lineage_events_cache: Optional[List[Dict]] = None

        # (project_id, full pipeline path) -> environment name. Lets lineage nest a job
        # under the right environment. The lineage namespace's environment is an opaque
        # UUID that no API maps back to the name, so this is sourced from published
        # pipelines and from executions (which report the environment name), keyed on the
        # path, which lineage, discovery, and executions all report identically.
        self._discovered_env_by_path: Dict[Tuple[str, str], str] = {}

        # DataFlow URNs already emitted by discovery, so lineage can reuse a published
        # pipeline's flow (with its richer properties) instead of re-emitting it.
        self._emitted_flow_urns: set[str] = set()

        self.lineage_start_time: Optional[datetime] = None
        self.lineage_end_time: Optional[datetime] = None

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "MatillionSource":
        config = MatillionSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def test_connection(self) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            self.api_client.get_projects()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except HTTPError as e:
            if e.response.status_code == 401:
                failure_reason = "Authentication failed. Please verify your OAuth2 client ID and client secret."
            elif e.response.status_code == 403:
                failure_reason = "Authorization failed. Credentials need 'pipeline-execution' scope for API access."
            elif e.response.status_code == 404:
                failure_reason = f"API endpoint not found. Verify region is correct: {self.config.api_config.region}"
            else:
                failure_reason = (
                    f"HTTP error {e.response.status_code}: {e.response.text}"
                )
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=failure_reason
            )
        except (ConnectionError, Timeout) as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Network connection failed: {str(e)}. Verify API is reachable.",
            )

        return test_report

    def _get_sql_aggregator_for_platform(
        self, platform: str, platform_instance: Optional[str], env: str
    ) -> Optional[SqlParsingAggregator]:
        if not platform:
            logger.debug("No platform specified, skipping SQL aggregator creation")
            return None

        if not self.ctx.graph:
            logger.debug("No DataHub graph available, SQL parsing disabled")
            return None

        if not self.config.parse_sql_for_lineage:
            return None

        key = SqlAggregatorKey(
            platform=platform, platform_instance=platform_instance, env=env
        )
        if key in self._sql_aggregators:
            return self._sql_aggregators[key]

        try:
            logger.info(
                f"Creating SQL parsing aggregator for platform: {platform} "
                f"(instance: {platform_instance}, env: {env})"
            )

            self._sql_aggregators[key] = SqlParsingAggregator(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                graph=self.ctx.graph,
                eager_graph_load=False,
                generate_lineage=True,  # Column-level lineage
                generate_queries=False,
                generate_usage_statistics=False,
                generate_operations=False,
            )
            return self._sql_aggregators[key]

        except (ValueError, TypeError, KeyError) as e:
            logger.info(f"SQL aggregator skipped for {platform}: {e}")
            self._sql_aggregators[key] = None
            return None

    def get_platform_instance_id(self) -> str:
        return self.config.platform_instance or self.platform

    def _discover_executions(self) -> List[MatillionPipelineExecution]:
        started_after = self.config.start_time.isoformat().replace("+00:00", "Z")
        started_before = self.config.end_time.isoformat().replace("+00:00", "Z")
        logger.info(
            f"Fetching pipeline executions for discovery "
            f"(startedAfter={started_after}, startedBefore={started_before})"
        )
        self.report.report_api_call()
        try:
            return self.api_client.get_pipeline_executions(
                started_after=started_after,
                started_before=started_before,
            )
        except (Timeout, ConnectionError) as e:
            self.report.report_warning(
                "pipeline-executions-timeout",
                f"Timed out discovering pipelines from executions ({e}); unpublished "
                f"pipelines may be missing. {API_TIMEOUT_TUNING_GUIDANCE}",
            )
            return []

    def _record_execution_environments(
        self, executions: List[MatillionPipelineExecution]
    ) -> None:
        # Executions report the environment name and full pipeline path, so they let
        # lineage resolve an environment for pipelines that are not published.
        for execution in executions:
            if not execution.project_id or not execution.pipeline_name:
                continue
            env_name = execution.environment_name
            if not env_name or not self.config.environment_patterns.allowed(env_name):
                continue
            self._discovered_env_by_path[
                (execution.project_id, execution.pipeline_name)
            ] = env_name

    def _group_executions_for_discovery(
        self,
    ) -> Dict[PipelineGroupKey, List[MatillionPipelineExecution]]:
        all_executions = self._discover_executions()
        self.report.executions_scanned += len(all_executions)
        logger.info(f"Found {len(all_executions)} total executions")
        self._record_execution_environments(all_executions)

        pipeline_groups: Dict[PipelineGroupKey, List[MatillionPipelineExecution]] = (
            defaultdict(list)
        )
        for execution in all_executions:
            if not execution.project_id or not execution.pipeline_name:
                continue
            key = PipelineGroupKey(
                project_id=execution.project_id,
                environment_name=execution.environment_name,
                pipeline_name=extract_base_pipeline_name(execution.pipeline_name),
            )
            pipeline_groups[key].append(execution)

        logger.info(
            f"Discovered {len(pipeline_groups)} unique pipelines from executions"
        )
        return pipeline_groups

    def _published_pipeline_groups(
        self, published_pipelines_index: Dict[PipelineGroupKey, MatillionPipeline]
    ) -> Dict[PipelineGroupKey, List[MatillionPipelineExecution]]:
        logger.info(
            "Skipping unpublished pipeline discovery (include_unpublished_pipelines=False)"
        )
        pipeline_groups: Dict[PipelineGroupKey, List[MatillionPipelineExecution]] = {
            key: [] for key in published_pipelines_index
        }

        if not self.config.extract_run_history:
            return pipeline_groups

        # Executions report the full path in pipelineName, so they key on the published
        # pipeline directly; executions without a published match are dropped.
        all_executions = self._discover_executions()
        self.report.executions_scanned += len(all_executions)
        self._record_execution_environments(all_executions)
        for execution in all_executions:
            if not execution.project_id or not execution.pipeline_name:
                continue
            key = PipelineGroupKey(
                project_id=execution.project_id,
                environment_name=execution.environment_name,
                pipeline_name=execution.pipeline_name,
            )
            if key in pipeline_groups:
                pipeline_groups[key].append(execution)

        return pipeline_groups

    def _discover_and_process_pipelines_from_executions(
        self, projects: List[MatillionProject]
    ) -> Iterable[MetadataWorkUnit]:
        published_pipelines_index: Dict[PipelineGroupKey, MatillionPipeline] = {}

        for project in projects:
            if not self.config.project_patterns.allowed(project.name):
                self.report.filtered_projects.append(project.name)
                continue

            self.report.report_projects_scanned()
            self._projects_cache[project.id] = project
            self.container_handler.register_project(project)

            yield from self.container_handler.emit_project_container(project)

            self.report.report_api_call()
            environments = self.api_client.get_environments(project.id)
            self.report.report_environments_scanned(len(environments))

            for env in environments:
                if not self.config.environment_patterns.allowed(env.name):
                    self.report.filtered_environments.append(env.name)
                    continue

                self._environments_cache[env.name] = env
                yield from self.container_handler.emit_environment_container(
                    env, project
                )

                self.report.report_api_call()
                published_pipelines = self.api_client.get_pipelines(
                    project.id, env.name
                )
                for pipeline in published_pipelines:
                    key = PipelineGroupKey(
                        project_id=project.id,
                        environment_name=env.name,
                        pipeline_name=pipeline.name,
                    )
                    published_pipelines_index[key] = pipeline
                    self._discovered_env_by_path[(project.id, pipeline.name)] = env.name
                    logger.debug(
                        f"Indexed published pipeline: {pipeline.name} in {project.name}/{env.name}"
                    )

                if self.config.include_streaming_pipelines:
                    self.report.report_api_call()
                    streaming_pipelines = self.api_client.get_streaming_pipelines(
                        project.id
                    )
                    self.report.report_streaming_pipelines_scanned(
                        len(streaming_pipelines)
                    )
                    for streaming_pipeline in streaming_pipelines:
                        yield from self.streaming_handler.emit_streaming_pipeline(
                            streaming_pipeline, project
                        )

        if self.config.include_unpublished_pipelines:
            pipeline_groups = self._group_executions_for_discovery()
        else:
            pipeline_groups = self._published_pipeline_groups(published_pipelines_index)

        for key in pipeline_groups:
            project_id = key.project_id
            env_name = key.environment_name
            pipeline_name = key.pipeline_name
            executions = pipeline_groups[key]
            if env_name and not self.config.environment_patterns.allowed(env_name):
                self.report.filtered_environments.append(env_name)
                continue
            if not self.config.pipeline_patterns.allowed(pipeline_name):
                self.report.filtered_pipelines.append(pipeline_name)
                continue

            project = self._projects_cache.get(project_id)  # type: ignore[assignment]
            if not project:
                logger.warning(
                    f"Project {project_id} not found in cache, skipping pipeline {pipeline_name}"
                )
                continue

            environment = None
            if env_name:
                environment = self._environments_cache.get(env_name)
                if not environment:
                    environment = MatillionEnvironment(
                        name=env_name,
                        project_id=project_id,
                    )
                    self._environments_cache[env_name] = environment

                    yield from self.container_handler.emit_environment_container(
                        environment, project
                    )
                    self.report.report_environments_scanned(1)

            published_pipeline = None
            if environment:
                key = PipelineGroupKey(
                    project_id=project_id,
                    environment_name=env_name,
                    pipeline_name=pipeline_name,
                )
                published_pipeline = published_pipelines_index.get(key)

            yield from self._process_pipeline_from_executions(
                project, environment, pipeline_name, executions, published_pipeline
            )

    def _process_pipeline_from_executions(
        self,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
        pipeline_name: str,
        executions: List[MatillionPipelineExecution],
        published_pipeline: Optional[MatillionPipeline] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if environment:
            flow_name = f"{project.id}.{environment.name}.{pipeline_name}"
        else:
            flow_name = f"{project.id}.{pipeline_name}"

        pipeline_type = executions[0].pipeline_type if executions else None

        custom_properties = {
            "pipeline_name": pipeline_name,
            "project_id": project.id,
            "project_name": project.name,
            "discovered_from": "executions_api",
            "execution_count": str(len(executions)),
        }

        if environment:
            custom_properties["environment_name"] = environment.name
        if pipeline_type:
            custom_properties["pipeline_type"] = pipeline_type

        if published_pipeline:
            custom_properties["is_published"] = "true"
            if published_pipeline.published_time:
                custom_properties["published_time"] = (
                    published_pipeline.published_time.isoformat()
                    if isinstance(published_pipeline.published_time, datetime)
                    else published_pipeline.published_time
                )
        else:
            custom_properties["is_published"] = "false"

        external_url = MATILLION_PIPELINE_OBSERVABILITY_URL.format(
            pipeline_name=quote_plus(extract_pipeline_file_name(pipeline_name))
        )

        dataflow = DataFlow(
            name=flow_name,
            platform=MATILLION_PLATFORM,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=extract_base_pipeline_name(pipeline_name),
            external_url=external_url,
            custom_properties=custom_properties,
            subtype=FlowContainerSubTypes.MATILLION_PIPELINE,
        )

        if published_pipeline:
            self._enrich_sdk_dataflow_with_schedule(
                dataflow, published_pipeline, project.id
            )

        yield from dataflow.as_workunits()
        pipeline_urn = str(dataflow.urn)
        self._emitted_flow_urns.add(pipeline_urn)

        yield from self.container_handler.add_pipeline_to_container(
            pipeline_urn,
            project,
            environment=environment,
            folder_segments=extract_folder_segments(pipeline_name),
        )

        self.report.report_pipelines_emitted()
        logger.debug(f"Emitted DataFlow for pipeline: {pipeline_name}")

        executions_to_process = sorted(
            executions, key=lambda e: e.started_at or datetime.min, reverse=True
        )[: self.config.max_executions_per_pipeline]

        all_steps_by_name: Dict[str, List[MatillionPipelineExecutionStepResult]] = {}
        executions_with_steps: List[ExecutionWithSteps] = []
        steps_by_execution_id: Dict[
            str, List[MatillionPipelineExecutionStepResult]
        ] = {}

        for execution in executions_to_process:
            try:
                steps = self.api_client.get_pipeline_execution_steps(
                    project.id, execution.pipeline_execution_id
                )
                if steps:
                    executions_with_steps.append(
                        ExecutionWithSteps(execution=execution, steps=steps)
                    )
                    steps_by_execution_id[execution.pipeline_execution_id] = steps
                    for step in steps:
                        if step.name not in all_steps_by_name:
                            all_steps_by_name[step.name] = []
                        all_steps_by_name[step.name].append(step)
                logger.debug(
                    f"Fetched {len(steps)} steps for execution {execution.pipeline_execution_id[:8]}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch steps for execution {execution.pipeline_execution_id}: {e}"
                )
                continue

        if not all_steps_by_name:
            logger.debug(f"No steps found for pipeline {pipeline_name}")
            self.report.report_pipelines_scanned()
            return

        pipeline = MatillionPipeline(
            name=pipeline_name,
            id=None,
            published_time=None,
        )

        yield from self._generate_step_based_data_jobs(
            pipeline, project, pipeline_urn, all_steps_by_name, environment=environment
        )

        for exec_with_steps in executions_with_steps:
            # Pipeline-level run so the DataFlow's "Runs" tab is populated, mirroring
            # how DAG-level runs surface on the pipeline (not just on its components).
            yield from self._generate_execution_dpi(
                exec_with_steps.execution, pipeline, pipeline_urn, project
            )
            for step in exec_with_steps.steps:
                yield from self._generate_step_dpi(
                    step,
                    exec_with_steps.execution,
                    pipeline,
                    pipeline_urn,
                    project,
                    steps_by_execution_id,
                )

        self.report.report_pipelines_scanned()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting Matillion metadata extraction")

        self.report.report_api_call()
        projects = self.api_client.get_projects()
        logger.info(f"Found {len(projects)} projects")

        yield from self._discover_and_process_pipelines_from_executions(projects)

        yield from self._generate_lineage_from_events(projects)

        yield from self._generate_sql_aggregator_workunits()

        self._update_lineage_state()

        logger.info("Completed Matillion metadata extraction")

    def _enrich_sdk_dataflow_with_schedule(
        self,
        dataflow: DataFlow,
        pipeline: MatillionPipeline,
        project_id: str,
    ) -> None:
        try:
            if project_id not in self._schedules_cache:
                self.report.report_api_call()
                self._schedules_cache[project_id] = self.api_client.get_schedules(
                    project_id
                )

            schedules = self._schedules_cache[project_id]
            matching_schedules = [
                s for s in schedules if s.pipeline_name == pipeline.name
            ]

            if matching_schedules:
                schedule = matching_schedules[0]
                if schedule.cron_expression:
                    dataflow.custom_properties["schedule_cron"] = (
                        schedule.cron_expression
                    )
                if schedule.schedule_enabled is not None:
                    dataflow.custom_properties["schedule_enabled"] = str(
                        schedule.schedule_enabled
                    )
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(
                    f"No schedule found for pipeline {pipeline.name} (HTTP 404)"
                )
            else:
                logger.info(
                    f"API error fetching schedule for pipeline {pipeline.name}: HTTP {e.response.status_code}"
                )
        except RequestException as e:
            logger.info(
                f"Network error fetching schedule for pipeline {pipeline.name}: {e}"
            )
        except (ValidationError, KeyError) as e:
            logger.debug(
                f"Parse error processing schedule for pipeline {pipeline.name}: {e}"
            )

    def _preload_schemas_for_sql_parsing(self, all_events: List[Dict]) -> None:
        if not self.ctx.graph or not self.config.parse_sql_for_lineage:
            return

        if not all_events:
            return

        logger.info("Preloading schemas from DataHub for SQL parsing")

        # Group by the aggregator key so each schema is registered into the same
        # aggregator that will later parse SQL for that dataset's instance/env.
        urns_by_aggregator_key: Dict[SqlAggregatorKey, List[str]] = defaultdict(list)

        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})

            for dataset_list_key in ["inputs", "outputs"]:
                for dataset_dict in event.get(dataset_list_key, []):
                    dataset_info = self.openlineage_parser._extract_dataset_info(
                        dataset_dict, dataset_list_key
                    )
                    if not dataset_info:
                        continue

                    dataset_urn = make_dataset_urn_from_matillion_dataset(dataset_info)
                    key = SqlAggregatorKey(
                        platform=dataset_info.platform,
                        platform_instance=dataset_info.platform_instance,
                        env=dataset_info.env,
                    )
                    if dataset_urn not in urns_by_aggregator_key[key]:
                        urns_by_aggregator_key[key].append(dataset_urn)

        for key, urns in urns_by_aggregator_key.items():
            logger.debug(f"Preloading {len(urns)} schemas for platform {key.platform}")

            aggregator = self._get_sql_aggregator_for_platform(
                platform=key.platform,
                platform_instance=key.platform_instance,
                env=key.env,
            )

            if not aggregator:
                continue

            for urn in urns:
                if urn in self._registered_urns:
                    continue

                try:
                    schema_metadata = self.ctx.graph.get_schema_metadata(urn)
                    if schema_metadata and schema_metadata.fields:
                        aggregator.register_schema(urn, schema_metadata)
                        self._registered_urns.add(urn)
                        self.report.schemas_preloaded += 1
                        logger.debug(
                            f"Preloaded schema for {urn} ({len(schema_metadata.fields)} fields)"
                        )
                except Exception as e:
                    logger.debug(f"Could not preload schema for {urn}: {e}")

        logger.info(
            f"Preloaded {self.report.schemas_preloaded} schemas across "
            f"{len(urns_by_aggregator_key)} platform/instance groups"
        )

    def _get_lineage_time_window(self) -> TimeWindow:
        redundant_handler = getattr(self, "redundant_run_skip_handler", None)

        if redundant_handler:
            start_time, end_time = redundant_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
            return TimeWindow(start_time=start_time, end_time=end_time)
        else:
            return TimeWindow(
                start_time=self.config.start_time, end_time=self.config.end_time
            )

    def _update_lineage_state(self) -> None:
        redundant_handler = getattr(self, "redundant_run_skip_handler", None)

        if redundant_handler and self.lineage_start_time and self.lineage_end_time:
            # Update checkpoint to current end_time so next run starts from here
            redundant_handler.update_state(
                self.lineage_start_time, self.lineage_end_time
            )
            logger.info(
                f"Updated lineage checkpoint: {self.lineage_start_time.isoformat()} to {self.lineage_end_time.isoformat()}"
            )

    def _generate_lineage_from_events(
        self, projects: List[MatillionProject]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit lineage directly from OpenLineage job events.

        The OpenLineage ``job.name`` is the full pipeline path (folders included),
        identical to the discovered published pipeline path. So lineage is filtered
        on that path and, when it matches a discovered pipeline, attached to that
        pipeline's DataFlow rather than to a separate bare-named flow.
        """
        all_events = self._fetch_lineage_events()
        if not all_events:
            return

        allowed_project_ids = {
            project.id
            for project in projects
            if self.config.project_patterns.allowed(project.name)
        }

        events_by_job: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            namespace = job.get("namespace", "")
            if not namespace.startswith(MATILLION_NAMESPACE_PREFIX):
                continue

            job_namespace = MatillionJobNamespace.parse(namespace)
            if not job_namespace.project_id:
                continue
            if (
                allowed_project_ids
                and job_namespace.project_id not in allowed_project_ids
            ):
                continue

            job_name = job.get("name", "")
            if not job_name:
                continue

            events_by_job[(job_namespace.project_id, job_name)].append(event)

        logger.info(
            f"Generating lineage from {len(events_by_job)} distinct Matillion jobs "
            f"across {len(all_events)} OpenLineage events"
        )

        for (project_id, full_path), events in events_by_job.items():
            if not self.config.pipeline_patterns.allowed(full_path):
                self.report.filtered_pipelines.append(full_path)
                continue

            project = self._projects_cache.get(project_id) or MatillionProject(
                id=project_id, name=project_id
            )
            yield from self._emit_lineage_for_job(project, full_path, events)

    def _emit_lineage_for_job(
        self,
        project: MatillionProject,
        full_path: str,
        events: List[Dict],
    ) -> Iterable[MetadataWorkUnit]:
        all_inputs: Dict[str, MatillionDatasetInfo] = {}
        outputs_by_urn: Dict[str, OutputLineageAccumulator] = {}

        for event in events:
            try:
                inputs, outputs, _ = self.openlineage_parser.parse_lineage_event(event)
            except (KeyError, ValueError, IndexError) as e:
                logger.debug(f"Failed to parse lineage event for {full_path}: {e}")
                continue

            sql_query = self.openlineage_parser.extract_sql_from_event(event)

            for input_dataset in inputs:
                all_inputs[make_dataset_urn_from_matillion_dataset(input_dataset)] = (
                    input_dataset
                )

            for output_dataset in outputs:
                output_urn = make_dataset_urn_from_matillion_dataset(output_dataset)
                accumulator = outputs_by_urn.setdefault(
                    output_urn, OutputLineageAccumulator(info=output_dataset)
                )
                for input_dataset in inputs:
                    accumulator.inputs[
                        make_dataset_urn_from_matillion_dataset(input_dataset)
                    ] = input_dataset
                accumulator.column_lineages.extend(
                    self.openlineage_parser.extract_column_lineage(
                        event, output_dataset, inputs
                    )
                )
                if sql_query:
                    accumulator.sql_queries.append(sql_query)

        if not all_inputs and not outputs_by_urn:
            return

        display_name = extract_base_pipeline_name(full_path)

        # Without a resolved environment we cannot nest the job, so skip it rather
        # than float a flow under the bare project.
        env_name = self._discovered_env_by_path.get((project.id, full_path))
        if env_name is None:
            self.report.lineage_jobs_skipped_no_environment += 1
            return

        environment = self._environments_cache.get(env_name)
        flow_name = f"{project.id}.{env_name}.{full_path}"

        dataflow = DataFlow(
            name=flow_name,
            platform=MATILLION_PLATFORM,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=display_name,
            external_url=MATILLION_PIPELINE_OBSERVABILITY_URL.format(
                pipeline_name=quote_plus(extract_pipeline_file_name(full_path))
            ),
            custom_properties={
                "pipeline_name": full_path,
                "project_id": project.id,
                "discovered_from": "lineage_events",
            },
            subtype=FlowContainerSubTypes.MATILLION_PIPELINE,
        )
        pipeline_urn = str(dataflow.urn)

        # A pipeline not already emitted by discovery is a dependent (child) pipeline
        # pulled in purely via lineage events. Skip it entirely when the user opts out.
        is_dependent = pipeline_urn not in self._emitted_flow_urns
        if is_dependent and not self.config.include_dependent_pipelines:
            self.report.lineage_dependent_pipelines_skipped += 1
            return

        # Reuse the discovered pipeline's flow (with its richer published properties)
        # when discovery already emitted it; otherwise mint it here, nested under the
        # resolved environment.
        if is_dependent:
            yield from dataflow.as_workunits()
            self._emitted_flow_urns.add(pipeline_urn)
            yield from self.container_handler.add_pipeline_to_container(
                pipeline_urn,
                project,
                environment=environment,
                folder_segments=extract_folder_segments(full_path),
            )

        datajob = DataJob(
            name=flow_name,
            flow_urn=pipeline_urn,
            display_name=display_name,
            custom_properties={"job_name": full_path},
            subtype=JobContainerSubTypes.MATILLION_COMPONENT,
        )
        yield from datajob.as_workunits()
        data_job_urn = str(datajob.urn)

        yield from self._emit_data_job_browse_path(
            data_job_urn, pipeline_urn, full_path, project, environment
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=data_job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=sorted(all_inputs),
                outputDatasets=sorted(outputs_by_urn),
                inputDatajobs=[],
            ),
        ).as_workunit()

        for output_urn, accumulator in outputs_by_urn.items():
            # Gap-fill: when OpenLineage carried no column lineage for this output
            # but a SQL query produced it and the output exists in DataHub, hand the
            # SQL to the aggregator to parse additional column lineage. The
            # aggregator emits later (enriching this downstream) and is non-primary,
            # so no ghost entities are created.
            if (
                not accumulator.column_lineages
                and accumulator.sql_queries
                and output_urn in self._registered_urns
            ):
                for sql_query in dict.fromkeys(accumulator.sql_queries):
                    self._register_sql_query(full_path, sql_query, accumulator.info)

            upstream_lineage = self.openlineage_parser.create_upstream_lineage(
                list[MatillionDatasetInfo](accumulator.inputs.values()),
                accumulator.info,
                accumulator.column_lineages,
            )
            if not upstream_lineage.upstreams:
                continue

            # The output dataset lives on its own platform (Snowflake, etc.) and is
            # owned by that connector, not Matillion. Mark non-primary so we add the
            # lineage edge without materializing a status aspect / claiming ownership.
            yield MetadataChangeProposalWrapper(
                entityUrn=output_urn,
                aspect=upstream_lineage,
            ).as_workunit(is_primary_source=False)
            self.report.lineage_emitted += 1

    def _fetch_lineage_events(self) -> List[Dict]:
        if self._lineage_events_cache is not None:
            return self._lineage_events_cache

        time_window = self._get_lineage_time_window()
        self.lineage_start_time = time_window.start_time
        self.lineage_end_time = time_window.end_time

        self.report.lineage_start_time = self.lineage_start_time
        self.report.lineage_end_time = self.lineage_end_time

        redundant_handler = getattr(self, "redundant_run_skip_handler", None)
        logger.info(
            f"Fetching OpenLineage events from "
            f"{self.lineage_start_time.isoformat()} to {self.lineage_end_time.isoformat()} "
            f"(stateful: {redundant_handler is not None})"
        )

        all_events: List[Dict] = []

        try:
            # The lineage API rejects ranges wider than 31 days, so the window is
            # fetched in sub-windows no larger than that.
            window_start = self.lineage_start_time
            chunk = timedelta(days=MAX_LINEAGE_DATE_RANGE_DAYS)

            while window_start < self.lineage_end_time:
                window_end = min(window_start + chunk, self.lineage_end_time)
                generated_from = window_start.isoformat().replace("+00:00", "Z")
                generated_before = window_end.isoformat().replace("+00:00", "Z")
                logger.info(
                    f"Fetching lineage events for window {generated_from} to {generated_before}"
                )

                chunk_event_count = 0
                page = 0
                while True:
                    self.report.report_api_call()
                    events_page = self.api_client.get_lineage_events(
                        generated_from,
                        generated_before,
                        page=page,
                        size=API_MAX_PAGE_SIZE,
                    )

                    if not events_page:
                        break

                    all_events.extend(events_page)
                    chunk_event_count += len(events_page)

                    if len(events_page) < API_MAX_PAGE_SIZE:
                        break

                    page += 1

                logger.info(
                    f"Retrieved {chunk_event_count} lineage events for window "
                    f"{generated_from} to {generated_before} (running total: {len(all_events)})"
                )

                window_start = window_end

        except (Timeout, ConnectionError) as e:
            self.report.report_warning(
                "lineage-events-timeout",
                f"Timed out fetching lineage events ({e}); returning {len(all_events)} "
                f"events collected so far. {API_TIMEOUT_TUNING_GUIDANCE}",
            )
        except Exception as e:
            self.report.report_warning(
                "lineage_events", f"Failed to fetch lineage events: {e}"
            )

        logger.info(f"Fetched total of {len(all_events)} OpenLineage events")
        self._lineage_events_cache = all_events

        self._preload_schemas_for_sql_parsing(all_events)

        return all_events

    def _register_sql_query(
        self,
        job_label: str,
        sql_query: str,
        output: MatillionDatasetInfo,
    ) -> None:
        # The namespace mapping's database/schema act as defaults so unqualified
        # table names in the SQL resolve to the same URNs as the rest of ingestion.
        aggregator = self._get_sql_aggregator_for_platform(
            output.platform, output.platform_instance, output.env
        )
        if not aggregator:
            return

        self.report.sql_parsing_attempts += 1
        try:
            aggregator.add_observed_query(
                ObservedQuery(
                    query=sql_query,
                    default_db=output.database,
                    default_schema=output.default_schema,
                )
            )
            self.report.sql_parsing_successes += 1
            logger.debug(
                f"Registered SQL for job {job_label} on platform {output.platform}"
            )
        except Exception as e:
            self.report.sql_parsing_failures += 1
            self.report.warning(
                title="SQL parsing error",
                message=f"Could not parse SQL for job '{job_label}'. "
                f"This may be due to an unsupported SQL dialect or parsing error. "
                f"Basic lineage from OpenLineage will still be emitted.",
                context=f"job: {job_label}, platform: {output.platform}, "
                f"sql_preview: {sql_query[:100]}...",
                exc=e,
            )

    def _emit_data_job_browse_path(
        self,
        data_job_urn: str,
        pipeline_urn: str,
        pipeline_path: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.container_handler.add_data_job_to_container(
            data_job_urn,
            pipeline_urn,
            project,
            environment=environment,
            folder_segments=extract_folder_segments(pipeline_path),
        )

    def _fetch_steps_for_pipeline(
        self,
        project: MatillionProject,
        pipeline: MatillionPipeline,
        executions: List[MatillionPipelineExecution],
    ) -> Dict[str, List[MatillionPipelineExecutionStepResult]]:
        steps_by_name: Dict[str, List[MatillionPipelineExecutionStepResult]] = {}

        for execution in executions:
            try:
                steps = self.api_client.get_pipeline_execution_steps(
                    project.id, execution.pipeline_execution_id
                )
                for step in steps:
                    if step.name not in steps_by_name:
                        steps_by_name[step.name] = []
                    steps_by_name[step.name].append(step)

                logger.debug(
                    f"Fetched {len(steps)} steps for execution {execution.pipeline_execution_id[:8]}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch steps for execution {execution.pipeline_execution_id}: {e}"
                )
                continue

        return steps_by_name

    def _generate_step_based_data_jobs(
        self,
        pipeline: MatillionPipeline,
        project: MatillionProject,
        pipeline_urn: str,
        steps_by_name: Dict[str, List[MatillionPipelineExecutionStepResult]],
        environment: Optional[MatillionEnvironment] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not steps_by_name:
            logger.debug(f"No steps found for pipeline {pipeline.name}")
            return

        custom_properties_base = build_data_job_custom_properties(pipeline, project)

        step_lineage_map = self._extract_step_level_lineage(pipeline, steps_by_name)
        step_order = self._determine_step_order(steps_by_name)

        for step_idx, step_name in enumerate(step_order):
            step_executions = steps_by_name[step_name]

            if environment:
                job_id = f"{project.id}.{environment.name}.{pipeline.name}.{step_name}"
            else:
                job_id = f"{project.id}.{pipeline.name}.{step_name}"

            data_job_urn = DataJobUrn.create_from_ids(
                job_id=job_id,
                data_flow_urn=pipeline_urn,
            )

            custom_properties = custom_properties_base.copy()
            custom_properties["step_name"] = step_name
            custom_properties["step_execution_count"] = str(len(step_executions))

            datajob = DataJob(
                name=job_id,
                flow_urn=pipeline_urn,
                display_name=step_name,
                external_url=None,
                custom_properties=custom_properties,
                subtype=JobContainerSubTypes.MATILLION_COMPONENT,
            )

            yield from datajob.as_workunits()

            yield from self._emit_data_job_browse_path(
                data_job_urn.urn(), pipeline_urn, pipeline.name, project, environment
            )

            yield from self._emit_step_input_output(
                data_job_urn.urn(),
                step_name,
                step_idx,
                step_order,
                step_lineage_map,
                pipeline_urn,
                pipeline.name,
                project,
                environment,
            )

            logger.debug(
                f"Emitted DataJob for step '{step_name}' with {len(step_executions)} executions"
            )

    def _determine_step_order(
        self,
        steps_by_name: Dict[str, List[MatillionPipelineExecutionStepResult]],
    ) -> List[str]:
        step_timings: List[StepTiming] = []

        for step_name, executions in steps_by_name.items():
            earliest_start = None
            for execution in executions:
                result = execution.result or {}
                started_at_str = result.get("startedAt")
                if started_at_str:
                    try:
                        started_at = parse_iso_timestamp(started_at_str)
                        if earliest_start is None or started_at < earliest_start:
                            earliest_start = started_at
                    except (ValueError, AttributeError):
                        continue

            # Use timezone-aware datetime.min to match timezone-aware timestamps from API
            step_timings.append(
                StepTiming(
                    step_name=step_name,
                    timestamp=earliest_start
                    or datetime.min.replace(tzinfo=timezone.utc),
                )
            )

        step_timings.sort(key=lambda x: x.timestamp)
        return [timing.step_name for timing in step_timings]

    def _extract_step_level_lineage(
        self,
        pipeline: MatillionPipeline,
        steps_by_name: Dict[str, List[MatillionPipelineExecutionStepResult]],
    ) -> Dict[str, StepLineage]:
        step_lineage_map: Dict[str, StepLineage] = {}

        all_events = self._fetch_lineage_events()
        if not all_events:
            return step_lineage_map

        # OpenLineage events may not have explicit step info, so we aggregate at pipeline level
        pipeline_inputs: List[str] = []
        pipeline_outputs: List[str] = []

        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            job_name = job.get("name", "")
            namespace = job.get("namespace", "")

            if not namespace.startswith(MATILLION_NAMESPACE_PREFIX):
                continue

            if not match_pipeline_name(pipeline.name, job_name):
                continue

            try:
                inputs, outputs, _ = self.openlineage_parser.parse_lineage_event(event)

                for input_dataset in inputs:
                    input_urn = make_dataset_urn_from_matillion_dataset(input_dataset)
                    if input_urn not in pipeline_inputs:
                        pipeline_inputs.append(input_urn)

                for output_dataset in outputs:
                    output_urn = make_dataset_urn_from_matillion_dataset(output_dataset)
                    if output_urn not in pipeline_outputs:
                        pipeline_outputs.append(output_urn)

            except (KeyError, ValueError, IndexError) as e:
                logger.debug(f"Failed to parse lineage event for {pipeline.name}: {e}")

        # Log summary of lineage extraction for this pipeline
        if pipeline_inputs or pipeline_outputs:
            logger.info(
                f"Extracted lineage for pipeline '{pipeline.name}': "
                f"{len(pipeline_inputs)} input datasets, {len(pipeline_outputs)} output datasets"
            )
            if pipeline_inputs:
                logger.debug(f"  Input datasets: {pipeline_inputs}")
            if pipeline_outputs:
                logger.debug(f"  Output datasets: {pipeline_outputs}")
        else:
            logger.debug(
                f"No lineage datasets extracted for pipeline '{pipeline.name}' "
                f"from {len(all_events)} OpenLineage events"
            )

        # Store pipeline-level inputs/outputs; will be filtered by step position later
        # (first step gets inputs, last step gets outputs, middle steps get neither)
        for step_name in steps_by_name:
            step_lineage_map[step_name] = StepLineage(
                input_urns=pipeline_inputs, output_urns=pipeline_outputs
            )

        return step_lineage_map

    def _emit_step_input_output(
        self,
        data_job_urn: str,
        step_name: str,
        step_idx: int,
        step_order: List[str],
        step_lineage_map: Dict[str, StepLineage],
        pipeline_urn: str,
        pipeline_name: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment] = None,
    ) -> Iterable[MetadataWorkUnit]:
        input_datasets: List[str] = []
        output_datasets: List[str] = []
        input_data_jobs: List[str] = []

        # Assign datasets based on position in step order
        if step_name in step_lineage_map:
            step_lineage = step_lineage_map[step_name]

            # First step gets pipeline inputs
            if step_idx == 0:
                input_datasets.extend(step_lineage.input_urns)
                if step_lineage.input_urns:
                    logger.debug(
                        f"Assigning {len(step_lineage.input_urns)} input datasets to first step '{step_name}' "
                        f"in pipeline {pipeline_name}: {step_lineage.input_urns[:3]}{'...' if len(step_lineage.input_urns) > 3 else ''}"
                    )

            # Last step gets pipeline outputs
            if step_idx == len(step_order) - 1:
                output_datasets.extend(step_lineage.output_urns)
                if step_lineage.output_urns:
                    logger.debug(
                        f"Assigning {len(step_lineage.output_urns)} output datasets to last step '{step_name}' "
                        f"in pipeline {pipeline_name}: {step_lineage.output_urns[:3]}{'...' if len(step_lineage.output_urns) > 3 else ''}"
                    )

        if step_idx > 0:
            prev_step_name = step_order[step_idx - 1]

            if environment:
                prev_job_id = (
                    f"{project.id}.{environment.name}.{pipeline_name}.{prev_step_name}"
                )
            else:
                prev_job_id = f"{project.id}.{pipeline_name}.{prev_step_name}"

            prev_data_job_urn = DataJobUrn.create_from_ids(
                job_id=prev_job_id,
                data_flow_urn=pipeline_urn,
            ).urn()

            input_data_jobs.append(prev_data_job_urn)

        # Only emit if there are inputs or outputs
        if input_datasets or output_datasets or input_data_jobs:
            input_output = DataJobInputOutputClass(
                inputDatasets=input_datasets if input_datasets else [],
                outputDatasets=output_datasets if output_datasets else [],
                inputDatajobs=input_data_jobs if input_data_jobs else [],
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=data_job_urn,
                aspect=input_output,
            ).as_workunit()

            logger.debug(
                f"Emitted inputOutput for step '{step_name}': "
                f"{len(input_datasets)} input datasets, "
                f"{len(output_datasets)} output datasets, "
                f"{len(input_data_jobs)} input jobs"
            )

    def _build_step_dpi_properties(
        self,
        step: MatillionPipelineExecutionStepResult,
        execution: MatillionPipelineExecution,
        pipeline: MatillionPipeline,
    ) -> DataProcessInstancePropertiesClass:
        exec_id = execution.pipeline_execution_id
        step_result = step.result or {}

        started_at_str = step_result.get("startedAt")
        finished_at_str = step_result.get("finishedAt")
        status = step_result.get("status", "UNKNOWN")
        message = step_result.get("message", "")

        created_timestamp = 0
        if started_at_str:
            try:
                started_at = parse_iso_timestamp(started_at_str)
                created_timestamp = int(started_at.timestamp() * 1000)
            except (ValueError, AttributeError):
                pass

        execution_url = MATILLION_DPI_OBSERVABILITY_URL.format(execution_id=exec_id)

        properties = DataProcessInstancePropertiesClass(
            name=f"{pipeline.name}-{step.name}-{exec_id[:8]}",
            created=AuditStampClass(
                time=created_timestamp,
                actor="urn:li:corpuser:datahub",
            ),
            type=DPI_TYPE_BATCH_SCHEDULED
            if execution.trigger == MATILLION_TRIGGER_SCHEDULE
            else DPI_TYPE_BATCH_AD_HOC,
            externalUrl=execution_url,
            customProperties={
                "execution_id": exec_id,
                "step_id": step.id,
                "step_name": step.name,
                "pipeline_name": pipeline.name,
                "status": status,
                "message": message,
            },
        )

        if execution.environment_name:
            properties.customProperties["environment_name"] = execution.environment_name
        if execution.project_id:
            properties.customProperties["project_id"] = execution.project_id
        if started_at_str:
            properties.customProperties["started_at"] = started_at_str
        if finished_at_str:
            properties.customProperties["finished_at"] = finished_at_str

        return properties

    def _build_child_pipeline_upstream_instances(
        self,
        step: MatillionPipelineExecutionStepResult,
        project: MatillionProject,
        properties: DataProcessInstancePropertiesClass,
        steps_by_execution_id: Dict[str, List[MatillionPipelineExecutionStepResult]],
    ) -> List[str]:
        upstream_instances: List[str] = []

        if not step.child_pipeline:
            return upstream_instances

        child_pipeline_id = step.child_pipeline.get("id")
        child_pipeline_name = step.child_pipeline.get("name")

        if not child_pipeline_id:
            return upstream_instances

        properties.customProperties["child_pipeline_id"] = child_pipeline_id
        if child_pipeline_name:
            properties.customProperties["child_pipeline_name"] = child_pipeline_name

        child_steps = steps_by_execution_id.get(child_pipeline_id)
        if not child_steps:
            logger.debug(
                f"Child pipeline execution {child_pipeline_id} not in cache, skipping dependency link"
            )
            return upstream_instances

        for child_step in child_steps:
            if child_pipeline_name:
                child_dpi_urn = make_step_dpi_urn(
                    self.config,
                    project.id,
                    child_pipeline_name,
                    child_pipeline_id,
                    child_step.id,
                )
                upstream_instances.append(child_dpi_urn)

        return upstream_instances

    def _build_and_emit_run_event(
        self,
        dpi_urn: str,
        step: MatillionPipelineExecutionStepResult,
        exec_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        step_result = step.result or {}
        finished_at_str = step_result.get("finishedAt")
        status = step_result.get("status", "UNKNOWN")

        if not finished_at_str:
            return

        try:
            finished_at = parse_iso_timestamp(finished_at_str)
            finished_timestamp = int(finished_at.timestamp() * 1000)

            result_type = MATILLION_TO_DATAHUB_RESULT_TYPE.get(
                status, RunResultTypeClass.SKIPPED
            )

            run_result = DataProcessInstanceRunResultClass(
                type=result_type,
                nativeResultType=status,
            )

            run_event = DataProcessInstanceRunEventClass(
                timestampMillis=finished_timestamp,
                status=DataProcessRunStatusClass.COMPLETE,
                result=run_result,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=run_event,
            ).as_workunit()

            self.report.report_pipeline_executions_emitted()
            logger.debug(f"Emitted DPI for step '{step.name}' execution {exec_id[:8]}")
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse timestamp for step {step.name}: {e}")

    def _generate_execution_dpi(
        self,
        execution: MatillionPipelineExecution,
        pipeline: MatillionPipeline,
        pipeline_urn: str,
        project: MatillionProject,
    ) -> Iterable[MetadataWorkUnit]:
        exec_id = execution.pipeline_execution_id
        dpi_urn = make_execution_dpi_urn(
            self.config, project.id, pipeline.name, exec_id
        )

        custom_properties = {
            "execution_id": exec_id,
            "pipeline_name": pipeline.name,
            "status": execution.status,
        }
        if execution.environment_name:
            custom_properties["environment_name"] = execution.environment_name
        if execution.project_id:
            custom_properties["project_id"] = execution.project_id
        if execution.started_at:
            custom_properties["started_at"] = execution.started_at.isoformat()
        if execution.finished_at:
            custom_properties["finished_at"] = execution.finished_at.isoformat()

        created_timestamp = (
            int(execution.started_at.timestamp() * 1000) if execution.started_at else 0
        )

        properties = DataProcessInstancePropertiesClass(
            name=f"{pipeline.name}-{exec_id[:8]}",
            created=AuditStampClass(
                time=created_timestamp,
                actor="urn:li:corpuser:datahub",
            ),
            type=DPI_TYPE_BATCH_SCHEDULED
            if execution.trigger == MATILLION_TRIGGER_SCHEDULE
            else DPI_TYPE_BATCH_AD_HOC,
            externalUrl=MATILLION_DPI_OBSERVABILITY_URL.format(execution_id=exec_id),
            customProperties=custom_properties,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=properties,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[],
                parentTemplate=pipeline_urn,
            ),
        ).as_workunit()

        if not execution.finished_at:
            return

        finished_timestamp = int(execution.finished_at.timestamp() * 1000)
        result_type = MATILLION_TO_DATAHUB_RESULT_TYPE.get(
            execution.status, RunResultTypeClass.SKIPPED
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRunEventClass(
                timestampMillis=finished_timestamp,
                status=DataProcessRunStatusClass.COMPLETE,
                result=DataProcessInstanceRunResultClass(
                    type=result_type,
                    nativeResultType=execution.status,
                ),
            ),
        ).as_workunit()
        self.report.report_pipeline_executions_emitted()

    def _generate_step_dpi(
        self,
        step: MatillionPipelineExecutionStepResult,
        execution: MatillionPipelineExecution,
        pipeline: MatillionPipeline,
        pipeline_urn: str,
        project: MatillionProject,
        steps_by_execution_id: Dict[str, List[MatillionPipelineExecutionStepResult]],
    ) -> Iterable[MetadataWorkUnit]:
        exec_id = execution.pipeline_execution_id

        dpi_urn = make_step_dpi_urn(
            self.config, project.id, pipeline.name, exec_id, step.id
        )

        properties = self._build_step_dpi_properties(step, execution, pipeline)

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=properties,
        ).as_workunit()

        if execution.environment_name:
            job_id = (
                f"{project.id}.{execution.environment_name}.{pipeline.name}.{step.name}"
            )
        else:
            job_id = f"{project.id}.{pipeline.name}.{step.name}"

        data_job_urn = DataJobUrn.create_from_ids(
            job_id=job_id,
            data_flow_urn=pipeline_urn,
        ).urn()

        upstream_instances = self._build_child_pipeline_upstream_instances(
            step, project, properties, steps_by_execution_id
        )

        relationships = DataProcessInstanceRelationshipsClass(
            upstreamInstances=upstream_instances,
            parentTemplate=data_job_urn,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=relationships,
        ).as_workunit()

        yield from self._build_and_emit_run_event(dpi_urn, step, exec_id)

    def _generate_sql_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self._sql_aggregators:
            return

        active_aggregators = {
            key: aggregator
            for key, aggregator in self._sql_aggregators.items()
            if aggregator is not None
        }

        if not active_aggregators:
            return

        logger.info(
            f"Generating lineage from {len(active_aggregators)} SQL aggregator(s)"
        )
        for key, aggregator in active_aggregators.items():
            label = (
                f"{key.platform} (instance: {key.platform_instance}, env: {key.env})"
            )
            logger.info(f"Generating lineage from {label} SQL aggregator")
            try:
                for mcp in aggregator.gen_metadata():
                    # Aggregator lineage targets external datasets owned by their own
                    # platform connectors, so it must not materialize/own them here.
                    yield mcp.as_workunit(is_primary_source=False)
            except Exception as e:
                logger.warning(
                    f"Failed to generate metadata from {label} SQL aggregator: {e}",
                    exc_info=True,
                )
            finally:
                try:
                    aggregator.close()
                except Exception as e:
                    logger.debug(f"Error closing {label} SQL aggregator: {e}")

    def get_report(self) -> SourceReport:
        return self.report
