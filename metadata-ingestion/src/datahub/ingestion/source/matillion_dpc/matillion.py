import logging
from typing import Dict, Iterable, List, Optional

from pydantic import ValidationError
from requests import HTTPError, RequestException

from datahub.emitter.mce_builder import datahub_guid, make_data_process_instance_urn
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
from datahub.ingestion.source.common.subtypes import JobContainerSubTypes
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    API_PATH_SUFFIX,
    MATILLION_PLATFORM,
    UI_PATH_PIPELINES,
)
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion_dpc.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion_dpc.matillion_lineage import OpenLineageParser
from datahub.ingestion.source.matillion_dpc.matillion_streaming import (
    MatillionStreamingHandler,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionDatasetInfo,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
    PipelineLineageResult,
)
from datahub.ingestion.source.matillion_dpc.urn_builder import MatillionUrnBuilder
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DataTransformClass,
    DataTransformLogicClass,
    QueryLanguageClass,
    QueryStatementClass,
    RunResultTypeClass,
)
from datahub.metadata.urns import DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

EXECUTION_STATUS_TO_RESULT_TYPE = {
    "success": RunResultTypeClass.SUCCESS,
    "failed": RunResultTypeClass.FAILURE,
    "cancelled": RunResultTypeClass.SKIPPED,
}


@platform_name("Matillion DPC", id="matillion")
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
    """
    Matillion Data Productivity Cloud (DPC) connector for DataHub.

    Lineage Sources:
    ================
    - OpenLineage events (/v1/lineage/events): Provides pipeline execution lineage including
      data transformations, input/output datasets, and SQL statements for column-level lineage
    - Pipeline executions (/v1/pipeline-executions): Provides operational metadata emitted
      as DataProcessInstances

    Not Used:
    =========
    - Audit Log API (/v1/audit): Administrative actions only (user logins, config changes),
      not applicable for data lineage
    """

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

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "MatillionSource":
        config = MatillionSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def test_connection(self) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            self.api_client.get_projects()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to Matillion API: {str(e)}",
            )
            return test_report

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

        except Exception as e:
            logger.warning(
                f"Failed to create SQL aggregator for platform {platform}. "
                f"SQL parsing will be disabled for this platform, but basic lineage will still be emitted. "
                f"Error: {e}"
            )
            self._sql_aggregators[platform] = None
            return None

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting Matillion metadata extraction")

        self.report.report_api_call()
        projects = self.api_client.get_projects()
        logger.info(f"Found {len(projects)} projects")

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
                self._environments_cache[env.name] = env
                if self.config.extract_projects_to_containers:
                    yield from self.container_handler.emit_environment_container(
                        env, project
                    )

                self.report.report_api_call()
                pipelines = self.api_client.get_pipelines(project.id, env.name)
                logger.info(
                    f"Found {len(pipelines)} pipelines in project {project.name}, environment {env.name}"
                )

                for pipeline in pipelines:
                    if not self.config.pipeline_patterns.allowed(pipeline.name):
                        self.report.filtered_pipelines.append(pipeline.name)
                        continue

                    self.report.report_pipelines_scanned()
                    yield from self._generate_pipeline_workunits(
                        pipeline, project, environment=env
                    )

            if self.config.include_streaming_pipelines:
                self.report.report_api_call()
                streaming_pipelines = self.api_client.get_streaming_pipelines(
                    project.id
                )
                logger.info(
                    f"Found {len(streaming_pipelines)} streaming pipelines in project {project.name}"
                )

                for streaming_pipeline in streaming_pipelines:
                    if not self.config.streaming_pipeline_patterns.allowed(
                        streaming_pipeline.name
                    ):
                        self.report.filtered_streaming_pipelines.append(
                            streaming_pipeline.name
                        )
                        continue

                    self.report.report_streaming_pipeline_scanned()
                    yield from self.streaming_handler.emit_streaming_pipeline(
                        streaming_pipeline, project
                    )

        if self._sql_aggregators:
            active_aggregators = {
                platform: aggregator
                for platform, aggregator in self._sql_aggregators.items()
                if aggregator is not None
            }

            if active_aggregators:
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
                            logger.debug(
                                f"Error closing {platform} SQL aggregator: {e}"
                            )

        logger.info("Completed Matillion metadata extraction")

    def _enrich_sdk_dataflow_with_schedule(
        self,
        dataflow: DataFlow,
        pipeline: MatillionPipeline,
        project_id: str,
    ) -> None:
        """Enrich SDK V2 DataFlow with schedule information from Matillion API."""
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
                if schedule.enabled is not None:
                    dataflow.custom_properties["schedule_enabled"] = str(
                        schedule.enabled
                    )
        except (HTTPError, RequestException) as e:
            logger.debug(
                f"API error fetching schedule for pipeline {pipeline.name}: {e}"
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
                        dataset_urn = self.openlineage_parser._make_dataset_urn(
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

    def _fetch_lineage_events(self) -> List[Dict]:
        if self._lineage_events_cache is not None:
            return self._lineage_events_cache

        from datetime import datetime, timedelta

        end_time = datetime.now()
        start_time = end_time - timedelta(days=self.config.lineage_start_days_ago)

        generated_from = start_time.isoformat() + "Z"
        generated_before = end_time.isoformat() + "Z"

        logger.info(
            f"Fetching OpenLineage events from {generated_from} to {generated_before}"
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
        """Extract lineage information for a pipeline from OpenLineage events.

        Returns a PipelineLineageResult containing input URNs, output URNs, and SQL queries.
        """
        result = PipelineLineageResult()

        if not self.config.include_lineage:
            return result

        all_events = self._fetch_lineage_events()
        if not all_events:
            return result

        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            job_name = job.get("name", "")

            if not (pipeline.name in job_name or job_name.endswith(pipeline.name)):
                continue

            try:
                inputs, outputs, _ = self.openlineage_parser.parse_lineage_event(event)

                for input_dataset in inputs:
                    input_urn = self.openlineage_parser._make_dataset_urn(input_dataset)
                    if input_urn not in result.input_urns:
                        result.input_urns.append(input_urn)

                for output_dataset in outputs:
                    output_urn = self.openlineage_parser._make_dataset_urn(
                        output_dataset
                    )
                    if output_urn not in result.output_urns:
                        result.output_urns.append(output_urn)

                if self.config.parse_sql_for_lineage and outputs:
                    sql_query = self.openlineage_parser.extract_sql_from_event(event)
                    if sql_query and sql_query not in result.sql_queries:
                        result.sql_queries.append(sql_query)
                        self._register_sql_for_parsing(
                            pipeline, data_job_urn, sql_query, outputs[0]
                        )

            except Exception as e:
                logger.debug(
                    f"Error extracting lineage for DataJob {pipeline.name}: {e}"
                )

        return result

    def _register_sql_for_parsing(
        self,
        pipeline: MatillionPipeline,
        data_job_urn: str,
        sql_query: str,
        output: MatillionDatasetInfo,
    ) -> None:
        aggregator = self._get_sql_aggregator_for_platform(
            output.platform, output.platform_instance, output.env
        )

        if not aggregator:
            return

        self.report.sql_parsing_attempts += 1
        try:
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
        except (AttributeError, TypeError) as e:
            # AttributeError/TypeError indicate programming errors (e.g., None where object expected,
            # incorrect type passed to aggregator). These should not occur in production and indicate
            # a bug in the connector logic, so we re-raise them for visibility.
            logger.error(
                f"Programming error registering SQL for {pipeline.name}: "
                f"{type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
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
            logger.debug(f"Failed to parse SQL for DataJob {pipeline.name}: {e}")

    def _build_data_job_custom_properties(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> Dict[str, str]:
        custom_properties = {
            "project_id": project.id,
        }

        if hasattr(pipeline, "id") and pipeline.id:
            custom_properties["pipeline_id"] = pipeline.id
        if hasattr(pipeline, "published_time") and pipeline.published_time:
            custom_properties["published_time"] = pipeline.published_time.isoformat()

        return custom_properties

    def _emit_data_job_browse_path(
        self,
        data_job_urn: str,
        pipeline_urn: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_projects_to_containers:
            return

        from datahub.emitter.mce_builder import make_data_platform_urn
        from datahub.metadata.schema_classes import (
            BrowsePathEntryClass,
            BrowsePathsV2Class,
        )

        platform_urn = make_data_platform_urn(MATILLION_PLATFORM)
        project_urn = self.container_handler.get_project_container_urn(project)
        path_elements = [
            BrowsePathEntryClass(id=platform_urn, urn=platform_urn),
            BrowsePathEntryClass(id=project_urn, urn=project_urn),
        ]

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

    def _generate_data_job_workunits(
        self,
        pipeline: MatillionPipeline,
        project: MatillionProject,
        pipeline_urn: str,
        environment: Optional[MatillionEnvironment] = None,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = self._build_data_job_custom_properties(pipeline, project)

        if environment:
            job_name = f"{project.id}.{environment.name}.{pipeline.name}"
        else:
            job_name = f"{project.id}.{pipeline.name}"

        data_job_urn = DataJobUrn.create_from_ids(
            job_id=job_name,
            data_flow_urn=pipeline_urn,
        )

        lineage = self._extract_lineage_for_pipeline(pipeline, data_job_urn.urn())

        datajob = DataJob(
            name=job_name,
            flow_urn=pipeline_urn,
            display_name=pipeline.name,
            custom_properties=custom_properties,
            subtype=JobContainerSubTypes.MATILLION_PIPELINE,
            inlets=lineage.input_urns or None,
            outlets=lineage.output_urns or None,
        )

        yield from datajob.as_workunits()

        logger.debug(
            f"Emitted DataJob {data_job_urn} with {len(lineage.input_urns)} inputs, {len(lineage.output_urns)} outputs"
        )

        # SQL transformation logic still needs to be emitted as MCP (not part of SDK V2 DataJob)
        if lineage.sql_queries:
            combined_sql = "\n\n-- ===== Query Separator =====\n\n".join(
                lineage.sql_queries
            )

            data_transform_logic = DataTransformLogicClass(
                transforms=[
                    DataTransformClass(
                        queryStatement=QueryStatementClass(
                            value=combined_sql,
                            language=QueryLanguageClass.SQL,
                        )
                    )
                ]
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=data_job_urn.urn(),
                aspect=data_transform_logic,
            ).as_workunit()

            logger.debug(
                f"Emitted SQL transformation logic for DataJob {pipeline.name} "
                f"({len(lineage.sql_queries)} queries)"
            )

    def _generate_pipeline_lineage_workunits(
        self,
        pipeline: MatillionPipeline,
        project: MatillionProject,
        pipeline_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        all_events = self._fetch_lineage_events()

        if not all_events:
            logger.debug(f"No lineage events available for pipeline {pipeline.name}")
            return

        pipeline_events = []
        for event_wrapper in all_events:
            event = event_wrapper.get("event", {})
            job = event.get("job", {})
            job_name = job.get("name", "")

            if pipeline.name in job_name or job_name.endswith(pipeline.name):
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
                    output_urn = self.openlineage_parser._make_dataset_urn(
                        output_dataset
                    )

                    if output_urn in emitted_datasets:
                        continue

                    output_column_lineages = [
                        cl
                        for cl in column_lineages
                        if self.openlineage_parser._make_dataset_urn(output_dataset)
                        == output_urn
                    ]

                    if self.config.include_column_lineage:
                        upstream_lineage = (
                            self.openlineage_parser.create_upstream_lineage(
                                inputs, output_dataset, output_column_lineages
                            )
                        )
                    else:
                        upstream_lineage = (
                            self.openlineage_parser.create_upstream_lineage(
                                inputs, output_dataset, []
                            )
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

            except Exception as e:
                logger.warning(
                    f"Error parsing lineage event for pipeline {pipeline.name}: {e}"
                )
                self.report.report_warning(
                    "lineage_parsing",
                    f"Failed to parse lineage event for {pipeline.name}: {e}",
                )

    def _generate_pipeline_workunits(
        self,
        pipeline: MatillionPipeline,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment] = None,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = {
            "pipeline_name": pipeline.name,
            "project_id": project.id,
            "project_name": project.name,
        }

        if environment:
            flow_name = f"{project.id}.{environment.name}.{pipeline.name}"
            custom_properties["environment_name"] = environment.name
        else:
            flow_name = f"{project.id}.{pipeline.name}"

        base_url = self.config.api_config.get_base_url()
        if base_url.endswith(API_PATH_SUFFIX):
            base_url = base_url[: -len(API_PATH_SUFFIX)]
        external_url = f"{base_url}/{UI_PATH_PIPELINES}/{pipeline.name}"

        dataflow = DataFlow(
            name=flow_name,
            platform=MATILLION_PLATFORM,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=pipeline.name,
            external_url=external_url,
            custom_properties=custom_properties,
        )

        self._enrich_sdk_dataflow_with_schedule(dataflow, pipeline, project.id)

        yield from dataflow.as_workunits()

        pipeline_urn = str(dataflow.urn)

        if self.config.extract_projects_to_containers:
            yield from self.container_handler.add_pipeline_to_container(
                pipeline_urn, project, environment=environment
            )

        self.report.report_pipelines_emitted()

        # This must come after DataFlow so container relationship works
        yield from self._generate_data_job_workunits(
            pipeline, project, pipeline_urn, environment=environment
        )

        if self.config.include_lineage:
            yield from self._generate_pipeline_lineage_workunits(
                pipeline, project, pipeline_urn
            )

        if self.config.include_pipeline_executions:
            self.report.report_api_call()
            executions = self.api_client.get_pipeline_executions(
                pipeline.name, self.config.max_executions_per_pipeline
            )
            logger.debug(
                f"Found {len(executions)} executions for pipeline {pipeline.name}"
            )

            for execution in executions:
                yield from self._generate_execution_workunits(
                    execution, pipeline, pipeline_urn, project
                )

    def _generate_execution_workunits(
        self,
        execution: MatillionPipelineExecution,
        pipeline: MatillionPipeline,
        pipeline_urn: str,
        project: MatillionProject,
    ) -> Iterable[MetadataWorkUnit]:
        exec_id = execution.pipeline_execution_id
        dpi_id = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "pipeline_name": pipeline.name,
                "execution_id": exec_id,
            }
        )
        dpi_urn = make_data_process_instance_urn(dpi_id)

        finished_timestamp = execution.finished_at
        created_timestamp = (
            int(execution.started_at.timestamp() * 1000)
            if execution.started_at
            else int(finished_timestamp.timestamp() * 1000)
            if finished_timestamp
            else 0
        )

        execution_trigger = execution.trigger or "API"
        properties = DataProcessInstancePropertiesClass(
            name=f"{pipeline.name}-{exec_id}",
            created=AuditStampClass(
                time=created_timestamp,
                actor="urn:li:corpuser:datahub",
            ),
            type="BATCH_SCHEDULED"
            if execution_trigger == "SCHEDULE"
            else "BATCH_AD_HOC",
            customProperties={
                "execution_id": exec_id,
                "pipeline_name": execution.pipeline_name,
                "status": execution.status,
            },
        )

        if execution_trigger:
            properties.customProperties["trigger"] = execution_trigger

        if execution.environment_name:
            properties.customProperties["environment_name"] = execution.environment_name

        if execution.project_id:
            properties.customProperties["project_id"] = execution.project_id

        if execution.pipeline_type:
            properties.customProperties["pipeline_type"] = execution.pipeline_type

        if execution.schedule_id:
            properties.customProperties["schedule_id"] = execution.schedule_id

        if execution.message:
            properties.customProperties["message"] = execution.message

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=properties,
        ).as_workunit()

        # DataProcessInstance should reference the DataJob (executable template), not the DataFlow (container)
        data_job_urn = self.urn_builder.make_data_job_urn(pipeline, project)
        relationships = DataProcessInstanceRelationshipsClass(
            upstreamInstances=[],
            parentTemplate=data_job_urn,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=relationships,
        ).as_workunit()

        if execution.started_at:
            run_event = DataProcessInstanceRunEventClass(
                timestampMillis=int(execution.started_at.timestamp() * 1000),
                status=DataProcessRunStatusClass.STARTED,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=run_event,
            ).as_workunit()

        finished_timestamp = execution.finished_at
        if finished_timestamp:
            result_type = EXECUTION_STATUS_TO_RESULT_TYPE.get(
                execution.status.lower(), RunResultTypeClass.SUCCESS
            )

            result = DataProcessInstanceRunResultClass(
                type=result_type,
                nativeResultType=execution.status,
            )

            run_event = DataProcessInstanceRunEventClass(
                timestampMillis=int(finished_timestamp.timestamp() * 1000),
                status=DataProcessRunStatusClass.COMPLETE,
                result=result,
            )

            if execution.started_at and finished_timestamp:
                duration_ms = int(
                    (finished_timestamp - execution.started_at).total_seconds() * 1000
                )
                run_event.durationMillis = duration_ms

            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=run_event,
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report
