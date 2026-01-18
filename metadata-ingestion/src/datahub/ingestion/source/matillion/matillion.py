import logging
from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import make_data_process_instance_urn
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
from datahub.ingestion.source.matillion.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.constants import MATILLION_PLATFORM
from datahub.ingestion.source.matillion.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion.matillion_connection import (
    MatillionConnectionHandler,
)
from datahub.ingestion.source.matillion.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion.matillion_lineage import (
    MatillionLineageHandler,
)
from datahub.ingestion.source.matillion.matillion_streaming import (
    MatillionStreamingHandler,
)
from datahub.ingestion.source.matillion.models import (
    MatillionConnection,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)
from datahub.ingestion.source.matillion.urn_builder import MatillionUrnBuilder
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    RunResultTypeClass,
    StatusClass,
)

logger = logging.getLogger(__name__)

EXECUTION_STATUS_TO_RESULT_TYPE = {
    "success": RunResultTypeClass.SUCCESS,
    "failed": RunResultTypeClass.FAILURE,
    "cancelled": RunResultTypeClass.SKIPPED,
}


@platform_name("Matillion", id="matillion")
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
        self.lineage_handler = MatillionLineageHandler(
            self.config, self.report, self.ctx.graph
        )
        self.streaming_handler = MatillionStreamingHandler(
            self.config, self.report, self.urn_builder, self.container_handler
        )
        self.connection_handler = MatillionConnectionHandler(
            self.config, self.report, self.urn_builder
        )

        self._projects_cache: Dict[str, MatillionProject] = {}
        self._environments_cache: Dict[str, MatillionEnvironment] = {}
        self._connections_cache: Dict[str, Dict[str, MatillionConnection]] = {}

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
                self._environments_cache[env.id] = env
                if self.config.extract_projects_to_containers:
                    yield from self.container_handler.emit_environment_container(
                        env, project
                    )

            if self.config.emit_connection_datasets:
                self.report.report_api_call()
                connections = self.api_client.get_connections(project_id=project.id)
                logger.info(
                    f"Found {len(connections)} connections in project {project.name}"
                )
                self._connections_cache[project.id] = {
                    conn.id: conn for conn in connections
                }
                for connection in connections:
                    yield from self.connection_handler.emit_connection_dataset(
                        connection, project
                    )

            self.report.report_api_call()
            pipelines = self.api_client.get_pipelines(project.id)
            logger.info(f"Found {len(pipelines)} pipelines in project {project.name}")

            for pipeline in pipelines:
                if not self.config.pipeline_patterns.allowed(pipeline.name):
                    self.report.filtered_pipelines.append(pipeline.name)
                    continue

                self.report.report_pipelines_scanned()
                yield from self._generate_pipeline_workunits(pipeline, project)

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

        logger.info("Completed Matillion metadata extraction")

    def _build_pipeline_dataflow_info(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> DataFlowInfoClass:
        custom_properties = {
            "pipeline_id": pipeline.id,
            "project_id": project.id,
            "project_name": project.name,
            "pipeline_type": pipeline.pipeline_type or "unknown",
        }

        if pipeline.environment_id:
            custom_properties["environment_id"] = pipeline.environment_id
        if pipeline.version:
            custom_properties["version"] = pipeline.version
        if pipeline.branch:
            custom_properties["branch"] = pipeline.branch

        return DataFlowInfoClass(
            name=pipeline.name,
            description=pipeline.description,
            externalUrl=f"{self.config.api_config.get_base_url().rstrip('/dpc')}/pipelines/{pipeline.id}",
            customProperties=custom_properties,
        )

    def _enrich_with_repository_details(
        self, dataflow_info: DataFlowInfoClass, pipeline: MatillionPipeline
    ) -> None:
        if not pipeline.repository_id:
            return

        dataflow_info.customProperties["repository_id"] = pipeline.repository_id
        try:
            self.report.report_api_call()
            repository = self.api_client.get_repository_by_id(pipeline.repository_id)
            if repository:
                if repository.repository_url:
                    dataflow_info.customProperties["repository_url"] = (
                        repository.repository_url
                    )
                if repository.provider:
                    dataflow_info.customProperties["repository_provider"] = (
                        repository.provider
                    )
        except Exception as e:
            logger.debug(
                f"Failed to fetch repository details for pipeline {pipeline.name}: {e}"
            )

    def _enrich_with_schedule_details(
        self, dataflow_info: DataFlowInfoClass, pipeline: MatillionPipeline
    ) -> None:
        try:
            self.report.report_api_call()
            schedules = self.api_client.get_schedules(pipeline_id=pipeline.id)
            if schedules:
                schedule = schedules[0]
                if schedule.cron_expression:
                    dataflow_info.customProperties["schedule_cron"] = (
                        schedule.cron_expression
                    )
                if schedule.enabled is not None:
                    dataflow_info.customProperties["schedule_enabled"] = str(
                        schedule.enabled
                    )
        except Exception as e:
            logger.debug(f"Failed to fetch schedule for pipeline {pipeline.name}: {e}")

    def _enrich_with_audit_events(
        self, dataflow_info: DataFlowInfoClass, pipeline: MatillionPipeline
    ) -> None:
        if not self.config.include_audit_events:
            return

        try:
            self.report.report_api_call()
            audit_events = self.api_client.get_audit_events(
                resource_id=pipeline.id, resource_type="pipeline"
            )
            if audit_events:
                recent_event = audit_events[0]
                if recent_event.user_id:
                    dataflow_info.customProperties["last_modified_by"] = (
                        recent_event.user_id
                    )
                if recent_event.timestamp:
                    dataflow_info.customProperties["last_modified_at"] = (
                        recent_event.timestamp.isoformat()
                    )
                if recent_event.event_type:
                    dataflow_info.customProperties["last_event_type"] = (
                        recent_event.event_type
                    )
        except Exception as e:
            logger.debug(
                f"Failed to fetch audit events for pipeline {pipeline.name}: {e}"
            )

    def _enrich_with_consumption_metrics(
        self, dataflow_info: DataFlowInfoClass, pipeline: MatillionPipeline
    ) -> None:
        if not self.config.include_consumption_metrics:
            return

        try:
            self.report.report_api_call()
            consumption_data = self.api_client.get_consumption(pipeline_id=pipeline.id)
            if consumption_data:
                total_credits = sum(c.credits_consumed or 0 for c in consumption_data)
                dataflow_info.customProperties["total_credits_consumed"] = str(
                    total_credits
                )
                recent = consumption_data[0]
                if recent.period_start:
                    dataflow_info.customProperties["consumption_period_start"] = (
                        recent.period_start.isoformat()
                    )
                if recent.period_end:
                    dataflow_info.customProperties["consumption_period_end"] = (
                        recent.period_end.isoformat()
                    )
        except Exception as e:
            logger.debug(
                f"Failed to fetch consumption metrics for pipeline {pipeline.name}: {e}"
            )

    def _generate_pipeline_workunits(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        pipeline_urn = self.urn_builder.make_pipeline_urn(pipeline, project)

        dataflow_info = self._build_pipeline_dataflow_info(pipeline, project)
        self._enrich_with_repository_details(dataflow_info, pipeline)
        self._enrich_with_schedule_details(dataflow_info, pipeline)
        self._enrich_with_audit_events(dataflow_info, pipeline)
        self._enrich_with_consumption_metrics(dataflow_info, pipeline)

        yield MetadataChangeProposalWrapper(
            entityUrn=str(pipeline_urn),
            aspect=dataflow_info,
        ).as_workunit()

        status = StatusClass(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=str(pipeline_urn),
            aspect=status,
        ).as_workunit()

        if self.config.extract_projects_to_containers:
            environment = None
            if pipeline.environment_id:
                environment = self._environments_cache.get(pipeline.environment_id)
            yield from self.container_handler.add_pipeline_to_container(
                str(pipeline_urn), project, environment
            )

        self.report.report_pipelines_emitted()

        if self.config.include_lineage:
            try:
                self.report.report_api_call()
                lineage_graph = self.api_client.get_pipeline_lineage(pipeline.id)
                if lineage_graph:
                    logger.debug(
                        f"Found lineage graph with {len(lineage_graph.nodes)} nodes "
                        f"and {len(lineage_graph.edges)} edges for pipeline {pipeline.name}"
                    )
                    yield from self.lineage_handler.emit_pipeline_lineage(
                        pipeline, str(pipeline_urn), lineage_graph
                    )
                else:
                    logger.debug(f"No lineage data found for pipeline {pipeline.name}")
            except Exception as e:
                logger.warning(
                    f"Failed to extract lineage for pipeline {pipeline.name}: {e}",
                    exc_info=True,
                )

        if self.config.include_pipeline_executions:
            self.report.report_api_call()
            executions = self.api_client.get_pipeline_executions(
                pipeline.id, self.config.max_executions_per_pipeline
            )
            logger.debug(
                f"Found {len(executions)} executions for pipeline {pipeline.name}"
            )

            for execution in executions:
                yield from self._generate_execution_workunits(
                    execution, pipeline, pipeline_urn
                )

    def _generate_execution_workunits(
        self,
        execution: MatillionPipelineExecution,
        pipeline: MatillionPipeline,
        pipeline_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        dpi_id = f"{pipeline.id}_{execution.id}"
        dpi_urn = make_data_process_instance_urn(dpi_id)

        created_timestamp = (
            int(execution.started_at.timestamp() * 1000)
            if execution.started_at
            else int(execution.completed_at.timestamp() * 1000)
            if execution.completed_at
            else 0
        )

        properties = DataProcessInstancePropertiesClass(
            name=f"{pipeline.name}-{execution.id}",
            created=AuditStampClass(
                time=created_timestamp,
                actor="urn:li:corpuser:datahub",
            ),
            type="BATCH_SCHEDULED"
            if execution.trigger_type == "schedule"
            else "BATCH_AD_HOC",
            customProperties={
                "execution_id": execution.id,
                "pipeline_id": pipeline.id,
                "status": execution.status,
            },
        )

        if execution.triggered_by:
            properties.customProperties["triggered_by"] = execution.triggered_by

        if execution.trigger_type:
            properties.customProperties["trigger_type"] = execution.trigger_type

        if execution.agent_id:
            properties.customProperties["agent_id"] = execution.agent_id

        if execution.rows_processed is not None:
            properties.customProperties["rows_processed"] = str(
                execution.rows_processed
            )

        if execution.error_message:
            properties.customProperties["error_message"] = execution.error_message

        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=properties,
        ).as_workunit()

        relationships = DataProcessInstanceRelationshipsClass(
            upstreamInstances=[],
            parentTemplate=pipeline_urn,
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

        if execution.completed_at:
            result_type = EXECUTION_STATUS_TO_RESULT_TYPE.get(
                execution.status.lower(), RunResultTypeClass.SUCCESS
            )

            result = DataProcessInstanceRunResultClass(
                type=result_type,
                nativeResultType=execution.status,
            )

            run_event = DataProcessInstanceRunEventClass(
                timestampMillis=int(execution.completed_at.timestamp() * 1000),
                status=DataProcessRunStatusClass.COMPLETE,
                result=result,
            )

            if execution.duration_ms:
                run_event.durationMillis = execution.duration_ms

            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=run_event,
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report
