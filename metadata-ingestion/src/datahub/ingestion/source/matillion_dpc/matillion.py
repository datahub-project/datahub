import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional

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
    MetadataWorkUnitProcessor,
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
    API_PATH_SUFFIX,
    DPI_TYPE_BATCH_AD_HOC,
    DPI_TYPE_BATCH_SCHEDULED,
    MATILLION_NAMESPACE_PREFIX,
    MATILLION_OBSERVABILITY_DASHBOARD_URL,
    MATILLION_PLATFORM,
    MATILLION_PROJECT_BRANCHES_URL,
    MATILLION_TO_DATAHUB_RESULT_TYPE,
    MATILLION_TRIGGER_SCHEDULE,
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
    make_dataset_urn_from_matillion_dataset,
    make_step_dpi_urn,
    match_pipeline_name,
)
from datahub.ingestion.source.matillion_dpc.models import (
    ExecutionWithSteps,
    MatillionDatasetInfo,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionPipelineExecutionStepResult,
    MatillionProject,
    PipelineGroupKey,
    PipelineLineageResult,
    StepLineage,
    StepTiming,
    TimeWindow,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
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
    KnownQueryLineageInfo,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


@platform_name("Matillion DPC", id="matillion-dpc")
@config_class(MatillionSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, can be disabled via configuration `include_lineage`",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
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

        self._sql_aggregators: Dict[str, Optional[SqlParsingAggregator]] = {}
        self._registered_urns: set[str] = set()

        self._projects_cache: Dict[str, MatillionProject] = {}
        self._environments_cache: Dict[str, MatillionEnvironment] = {}
        self._schedules_cache: Dict[str, list] = {}
        self._lineage_events_cache: Optional[List[Dict]] = None

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

        if platform in self._sql_aggregators:
            return self._sql_aggregators[platform]

        try:
            logger.info(
                f"Creating SQL parsing aggregator for platform: {platform} "
                f"(instance: {platform_instance}, env: {env})"
            )

            self._sql_aggregators[platform] = SqlParsingAggregator(
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
            return self._sql_aggregators[platform]

        except (ValueError, TypeError, KeyError) as e:
            logger.info(f"SQL aggregator skipped for {platform}: {e}")
            self._sql_aggregators[platform] = None
            return None

    def get_platform_instance_id(self) -> str:
        return self.config.platform_instance or self.platform

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

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

            if self.config.extract_projects_to_containers:
                yield from self.container_handler.emit_project_container(project)

            self.report.report_api_call()
            environments = self.api_client.get_environments(project.id)
            self.report.report_environments_scanned(len(environments))

            for env in environments:
                if not self.config.environment_patterns.allowed(env.name):
                    self.report.filtered_environments.append(env.name)
                    continue

                self._environments_cache[env.name] = env
                if self.config.extract_projects_to_containers:
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

        pipeline_groups: Dict[PipelineGroupKey, List[MatillionPipelineExecution]] = {}

        if self.config.include_unpublished_pipelines:
            logger.info("Fetching pipeline executions for discovery")
            self.report.report_api_call()
            all_executions = self.api_client.get_pipeline_executions(limit=100)
            logger.info(f"Found {len(all_executions)} total executions")

            pipeline_groups = defaultdict(list)

            for execution in all_executions:
                if not execution.project_id or not execution.pipeline_name:
                    continue

                normalized_name = extract_base_pipeline_name(execution.pipeline_name)
                env_name = execution.environment_name
                key = PipelineGroupKey(
                    project_id=execution.project_id,
                    environment_name=env_name,
                    pipeline_name=normalized_name,
                )
                pipeline_groups[key].append(execution)

            logger.info(
                f"Discovered {len(pipeline_groups)} unique pipelines from executions"
            )
        else:
            logger.info(
                "Skipping unpublished pipeline discovery (include_unpublished_pipelines=False)"
            )

        for key in pipeline_groups:
            project_id = key.project_id
            env_name = key.environment_name
            pipeline_name = key.pipeline_name
            executions = pipeline_groups[key]
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

                    if self.config.extract_projects_to_containers:
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

        base_url = self.config.api_config.get_base_url()
        if base_url.endswith(API_PATH_SUFFIX):
            base_url = base_url[: -len(API_PATH_SUFFIX)]
        external_url = MATILLION_PROJECT_BRANCHES_URL.format(project_id=project.id)

        dataflow = DataFlow(
            name=flow_name,
            platform=MATILLION_PLATFORM,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=pipeline_name,
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

        if self.config.extract_projects_to_containers:
            yield from self.container_handler.add_pipeline_to_container(
                pipeline_urn, project, environment=environment
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

        dataset_urns_by_platform: Dict[str, List[str]] = {}

        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})

            for dataset_list_key in ["inputs", "outputs"]:
                for dataset_dict in event.get(dataset_list_key, []):
                    dataset_info = self.openlineage_parser._extract_dataset_info(
                        dataset_dict, dataset_list_key
                    )
                    if dataset_info:
                        dataset_urn = make_dataset_urn_from_matillion_dataset(
                            dataset_info
                        )
                        platform = dataset_info.platform

                        if platform not in dataset_urns_by_platform:
                            dataset_urns_by_platform[platform] = []

                        if dataset_urn not in dataset_urns_by_platform[platform]:
                            dataset_urns_by_platform[platform].append(dataset_urn)

        for platform, urns in dataset_urns_by_platform.items():
            logger.debug(f"Preloading {len(urns)} schemas for platform {platform}")

            aggregator = self._get_sql_aggregator_for_platform(
                platform=platform,
                platform_instance=None,  # Will be extracted from URNs
                env=self.config.env,
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
            f"Preloaded {self.report.schemas_preloaded} schemas across {len(dataset_urns_by_platform)} platforms"
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

    def _fetch_lineage_events(self) -> List[Dict]:
        if self._lineage_events_cache is not None:
            return self._lineage_events_cache

        time_window = self._get_lineage_time_window()
        self.lineage_start_time = time_window.start_time
        self.lineage_end_time = time_window.end_time

        self.report.lineage_start_time = self.lineage_start_time
        self.report.lineage_end_time = self.lineage_end_time

        generated_from = self.lineage_start_time.isoformat().replace("+00:00", "Z")
        generated_before = self.lineage_end_time.isoformat().replace("+00:00", "Z")

        redundant_handler = getattr(self, "redundant_run_skip_handler", None)
        logger.info(
            f"Fetching OpenLineage events from {generated_from} to {generated_before} "
            f"(stateful: {redundant_handler is not None})"
        )

        all_events = []
        page = 0

        try:
            while True:
                self.report.report_api_call()
                events_page = self.api_client.get_lineage_events(
                    generated_from, generated_before, page=page, size=100
                )

                if not events_page:
                    break

                all_events.extend(events_page)
                logger.debug(
                    f"Fetched page {page} with {len(events_page)} events (total: {len(all_events)})"
                )

                if len(events_page) < 100:
                    break

                page += 1

        except Exception as e:
            logger.warning(f"Error fetching lineage events: {e}")
            self.report.report_warning(
                "lineage_events", f"Failed to fetch lineage events: {e}"
            )

        logger.info(f"Fetched total of {len(all_events)} OpenLineage events")
        self._lineage_events_cache = all_events

        self._preload_schemas_for_sql_parsing(all_events)

        return all_events

    def _extract_lineage_for_pipeline(
        self,
        pipeline: MatillionPipeline,
        data_job_urn: str,
    ) -> PipelineLineageResult:
        result = PipelineLineageResult()

        all_events = self._fetch_lineage_events()
        if not all_events:
            return result

        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            job_name = job.get("name", "")
            namespace = job.get("namespace", "")

            # Only process Matillion job events (not dataset events like snowflake://)
            if not namespace.startswith(MATILLION_NAMESPACE_PREFIX):
                continue

            if not match_pipeline_name(pipeline.name, job_name):
                continue

            try:
                inputs, outputs, _ = self.openlineage_parser.parse_lineage_event(event)

                for input_dataset in inputs:
                    input_urn = make_dataset_urn_from_matillion_dataset(input_dataset)
                    if input_urn not in result.input_urns:
                        result.input_urns.append(input_urn)

                for output_dataset in outputs:
                    output_urn = make_dataset_urn_from_matillion_dataset(output_dataset)
                    if output_urn not in result.output_urns:
                        result.output_urns.append(output_urn)

                if self.config.parse_sql_for_lineage and outputs:
                    sql_query = self.openlineage_parser.extract_sql_from_event(event)
                    if sql_query and sql_query not in result.sql_queries:
                        result.sql_queries.append(sql_query)
                        self._register_sql_for_parsing(
                            pipeline, data_job_urn, sql_query, inputs, outputs[0]
                        )

            except (KeyError, ValueError, IndexError) as e:
                logger.info(
                    f"Skipping lineage for DataJob {pipeline.name} due to parsing error: {e}"
                )
                self.report.report_warning(
                    "lineage_parsing_error",
                    f"Failed to parse lineage for {pipeline.name}: {e}",
                )

        return result

    def _register_sql_for_parsing(
        self,
        pipeline: MatillionPipeline,
        data_job_urn: str,
        sql_query: str,
        inputs: List[MatillionDatasetInfo],
        output: MatillionDatasetInfo,
    ) -> None:
        aggregator = self._get_sql_aggregator_for_platform(
            output.platform, output.platform_instance, output.env
        )

        if not aggregator:
            return

        self.report.sql_parsing_attempts += 1
        try:
            input_urns = [
                make_dataset_urn_from_matillion_dataset(input_dataset)
                for input_dataset in inputs
            ]
            output_urn = make_dataset_urn_from_matillion_dataset(output)

            # Only emit upstream lineage if downstream dataset is known (has schema in DataHub)
            if output_urn in self._registered_urns:
                aggregator.add_known_query_lineage(
                    KnownQueryLineageInfo(
                        query_text=sql_query,
                        downstream=output_urn,
                        upstreams=input_urns,
                    )
                )
                logger.debug(
                    f"Registered known lineage for DataJob {pipeline.name}: {len(input_urns)} upstreams -> {output_urn}"
                )
            else:
                logger.debug(
                    f"Skipping known lineage for DataJob {pipeline.name}: downstream {output_urn} not found in DataHub"
                )

            aggregator.add_view_definition(
                view_urn=data_job_urn,
                view_definition=sql_query,
                default_db=None,
                default_schema=None,
            )
            self.report.sql_parsing_successes += 1
            logger.debug(
                f"Registered SQL for DataJob {pipeline.name} on platform {output.platform}"
            )
        except Exception as e:
            log_level = (
                logging.ERROR
                if isinstance(e, (AttributeError, TypeError))
                else logging.DEBUG
            )
            logger.log(
                log_level,
                f"Failed to parse SQL for {pipeline.name}: {type(e).__name__}: {e}",
            )
            self.report.sql_parsing_failures += 1
            self.report.warning(
                title="SQL parsing error",
                message=f"Could not parse SQL for pipeline '{pipeline.name}'. "
                f"This may be due to an unsupported SQL dialect or parsing error. "
                f"Basic lineage from OpenLineage will still be emitted.",
                context=f"pipeline: {pipeline.name}, platform: {output.platform}, "
                f"sql_preview: {sql_query[:100]}...",
                exc=e,
            )

    def _emit_data_job_browse_path(
        self,
        data_job_urn: str,
        pipeline_urn: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_projects_to_containers:
            return

        # Note: auto_browse_path_v2 processor will prepend platform instance if configured
        path_elements = []

        project_urn = self.container_handler.get_project_container_urn(project)
        path_elements.append(BrowsePathEntryClass(id=project_urn, urn=project_urn))

        if environment:
            env_urn = self.container_handler.get_environment_container_urn(
                environment, project
            )
            path_elements.append(BrowsePathEntryClass(id=env_urn, urn=env_urn))

        path_elements.append(BrowsePathEntryClass(id=pipeline_urn, urn=pipeline_urn))
        path_elements.append(BrowsePathEntryClass(id=data_job_urn, urn=data_job_urn))

        browse_path = BrowsePathsV2Class(path=path_elements)
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job_urn,
            aspect=browse_path,
        ).as_workunit()

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
                display_name=step_name,  # Use step name as display name
                custom_properties=custom_properties,
                subtype=JobContainerSubTypes.MATILLION_COMPONENT,
            )

            yield from datajob.as_workunits()

            yield from self._emit_data_job_browse_path(
                data_job_urn.urn(), pipeline_urn, project, environment
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
                        started_at = datetime.fromisoformat(
                            started_at_str.replace("Z", "+00:00")
                        )
                        if earliest_start is None or started_at < earliest_start:
                            earliest_start = started_at
                    except (ValueError, AttributeError):
                        continue

            step_timings.append(
                StepTiming(
                    step_name=step_name, timestamp=earliest_start or datetime.min
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

            # Last step gets pipeline outputs
            if step_idx == len(step_order) - 1:
                output_datasets.extend(step_lineage.output_urns)

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

    def _generate_pipeline_lineage_workunits(
        self,
        pipeline: MatillionPipeline,
        project: MatillionProject,
        pipeline_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        all_events = self._fetch_lineage_events()

        if not all_events:
            logger.debug(f"No lineage events available for pipeline {pipeline.name}")
            return

        pipeline_events = []
        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            job_name = job.get("name", "")
            namespace = job.get("namespace", "")

            # Only process Matillion job events (not dataset events like snowflake://)
            if not namespace.startswith(MATILLION_NAMESPACE_PREFIX):
                continue

            if match_pipeline_name(pipeline.name, job_name):
                pipeline_events.append(event)

        if not pipeline_events:
            logger.debug(
                f"No matching lineage events found for pipeline {pipeline.name}"
            )
            return

        logger.info(
            f"Found {len(pipeline_events)} lineage events for pipeline {pipeline.name}"
        )

        emitted_datasets: set[str] = set()

        for event in pipeline_events:
            try:
                inputs, outputs, column_lineages = (
                    self.openlineage_parser.parse_lineage_event(event)
                )

                for output_dataset in outputs:
                    output_urn = make_dataset_urn_from_matillion_dataset(output_dataset)

                    if output_urn in emitted_datasets:
                        continue

                    output_column_lineages = [
                        cl
                        for cl in column_lineages
                        if make_dataset_urn_from_matillion_dataset(output_dataset)
                        == output_urn
                    ]

                    upstream_lineage = self.openlineage_parser.create_upstream_lineage(
                        inputs, output_dataset, output_column_lineages
                    )

                    if upstream_lineage.upstreams:
                        yield MetadataChangeProposalWrapper(
                            entityUrn=output_urn,
                            aspect=upstream_lineage,
                        ).as_workunit()

                        emitted_datasets.add(output_urn)
                        logger.debug(
                            f"Emitted lineage for {output_urn} with {len(upstream_lineage.upstreams)} upstreams"
                        )

            except (KeyError, ValueError, IndexError) as e:
                logger.warning(
                    f"Error parsing lineage event for pipeline {pipeline.name}: {e}"
                )
                self.report.report_warning(
                    "lineage_parsing",
                    f"Failed to parse lineage event for {pipeline.name}: {e}",
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
                started_at = datetime.fromisoformat(
                    started_at_str.replace("Z", "+00:00")
                )
                created_timestamp = int(started_at.timestamp() * 1000)
            except (ValueError, AttributeError):
                pass

        execution_url = MATILLION_OBSERVABILITY_DASHBOARD_URL.format(
            execution_id=exec_id
        )

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
            finished_at = datetime.fromisoformat(finished_at_str.replace("Z", "+00:00"))
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
            platform: aggregator
            for platform, aggregator in self._sql_aggregators.items()
            if aggregator is not None
        }

        if not active_aggregators:
            return

        logger.info(
            f"Generating lineage from {len(active_aggregators)} SQL aggregator(s)"
        )
        for platform, aggregator in active_aggregators.items():
            logger.info(f"Generating lineage from {platform} SQL aggregator")
            try:
                for mcp in aggregator.gen_metadata():
                    yield mcp.as_workunit()
            except Exception as e:
                logger.warning(
                    f"Failed to generate metadata from {platform} SQL aggregator: {e}",
                    exc_info=True,
                )
            finally:
                try:
                    aggregator.close()
                except Exception as e:
                    logger.debug(f"Error closing {platform} SQL aggregator: {e}")

    def get_report(self) -> SourceReport:
        return self.report
