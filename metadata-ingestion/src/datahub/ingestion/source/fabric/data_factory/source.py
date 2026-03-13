"""Microsoft Fabric Data Factory ingestion source for DataHub.

This connector extracts metadata from Microsoft Fabric Data Factory items:
- Workspaces as Containers
- Data Pipelines as DataFlows with Activities as DataJobs
- Copy Jobs as DataFlows with dataset-level lineage
- Dataflow Gen2 as DataFlows (metadata only)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Union

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    FabricConnection,
    FabricItem,
    FabricItemType,
    FabricJobInstance,
    FabricWorkspace,
    ItemJobStatus,
    WorkspaceKey,
    build_workspace_container,
)
from datahub.ingestion.source.fabric.common.urn_generator import (
    make_activity_job_id,
    make_activity_run_id,
    make_pipeline_flow_id,
    make_pipeline_run_id,
)
from datahub.ingestion.source.fabric.data_factory.client import (
    FabricDataFactoryClient,
)
from datahub.ingestion.source.fabric.data_factory.config import (
    FabricDataFactorySourceConfig,
)
from datahub.ingestion.source.fabric.data_factory.lineage import (
    CopyActivityLineageExtractor,
)
from datahub.ingestion.source.fabric.data_factory.models import PipelineActivityRun
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactoryClientReport,
    FabricDataFactorySourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
    EdgeClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity
from datahub.utilities.urns.data_process_instance_urn import (
    DataProcessInstanceUrn,
)

logger = logging.getLogger(__name__)

PLATFORM = "fabric-data-factory"

# Earliest possible start date for activity run queries.
# The Fabric queryActivityRuns API requires a lastUpdatedAfter timestamp;
# this ensures we capture all activity runs regardless of pipeline age.
ACTIVITY_RUNS_MIN_START = datetime(2015, 1, 1, tzinfo=timezone.utc)

# Fabric run status → DataHub InstanceRunResult
_FABRIC_STATUS_TO_RESULT: dict[str, InstanceRunResult] = {
    ItemJobStatus.COMPLETED: InstanceRunResult.SUCCESS,
    ItemJobStatus.FAILED: InstanceRunResult.FAILURE,
    ItemJobStatus.CANCELLED: InstanceRunResult.SKIPPED,
    ItemJobStatus.DEDUPED: InstanceRunResult.SKIPPED,
}

# Activity run status → DataHub InstanceRunResult
# Activity runs use "Succeeded"/"Failed"/"Cancelled" (not ItemJobStatus constants)
_ACTIVITY_STATUS_TO_RESULT: dict[str, InstanceRunResult] = {
    "Succeeded": InstanceRunResult.SUCCESS,
    "Failed": InstanceRunResult.FAILURE,
    "Cancelled": InstanceRunResult.SKIPPED,
}


def _parse_iso_to_millis(iso_str: str) -> int:
    """Convert an ISO 8601 timestamp string to epoch milliseconds."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    # Fabric API returns UTC timestamps without offset suffix
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


@platform_name("Fabric Data Factory")
@config_class(FabricDataFactorySourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class FabricDataFactorySource(StatefulIngestionSourceBase):
    """Extracts metadata from Microsoft Fabric Data Factory."""

    config: FabricDataFactorySourceConfig
    report: FabricDataFactorySourceReport
    platform: str = PLATFORM

    def __init__(self, config: FabricDataFactorySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FabricDataFactorySourceReport()

        auth_helper = FabricAuthHelper(config.credential)
        self.client_report = FabricDataFactoryClientReport()
        self.client = FabricDataFactoryClient(
            auth_helper,
            timeout=config.api_timeout,
            report=self.client_report,
        )
        self.report.client_report = self.client_report

        # Connection cache: connection GUID → FabricConnection
        # Populated once before processing pipelines, used by lineage
        # resolvers to map activity connection refs to DataHub platforms.
        self._connections_cache: dict[str, FabricConnection] = {}

        # Pipeline run ID → DPI URN, populated during pipeline run emission,
        # used to set parent_instance on activity run DPIs.
        self._pipeline_run_urns: dict[str, DataProcessInstanceUrn] = {}

    def _cache_connections(self) -> None:
        """Fetch all tenant connections and cache them keyed by ID.

        Mirrors ADF's _cache_factory_resources pattern — called once in
        get_workunits_internal before processing any workspaces.
        """
        for raw in self.client.list_connections():
            connection_id = raw.get("id", "")
            if connection_id:
                self._connections_cache[connection_id] = FabricConnection.from_dict(raw)
        logger.info(f"Cached {len(self._connections_cache)} Fabric connection(s)")

    def get_connection(self, connection_id: str) -> Optional[FabricConnection]:
        """Look up a cached connection by GUID."""
        return self._connections_cache.get(connection_id)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "FabricDataFactorySource":
        config = FabricDataFactorySourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(
        self,
    ) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> FabricDataFactorySourceReport:
        return self.report

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Generate workunits for all Fabric Data Factory resources."""
        logger.info("Starting Fabric Data Factory ingestion")

        try:
            # Cache connections up-front for lineage resolution
            self._cache_connections()
            self._copy_lineage_extractor = CopyActivityLineageExtractor(
                connections_cache=self._connections_cache,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            workspaces = list(self.client.list_workspaces())

            for workspace in workspaces:
                if not self.config.workspace_pattern.allowed(workspace.name):
                    self.report.report_workspace_filtered(workspace.name)
                    continue

                self.report.report_workspace_scanned()
                logger.info(f"Processing workspace: {workspace.name} ({workspace.id})")

                try:
                    yield from self._create_workspace_container(workspace)

                    yield from self._process_pipelines(workspace)
                    # TODO: yield from self._process_copyjobs(workspace)
                    # TODO: yield from self._process_dataflows(workspace)

                except Exception as e:
                    self.report.report_warning(
                        title="Failed to Process Workspace",
                        message="Error processing workspace. Skipping to next.",
                        context=f"workspace={workspace.name}",
                        exc=e,
                    )

        except Exception as e:
            self.report.report_failure(
                title="Failed to List Workspaces",
                message="Unable to retrieve workspaces from Fabric.",
                context="",
                exc=e,
            )
        finally:
            self.client.close()

    def _create_workspace_container(
        self, workspace: FabricWorkspace
    ) -> Iterable[Entity]:
        yield from build_workspace_container(
            workspace=workspace,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _process_pipelines(
        self, workspace: FabricWorkspace
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        if not self.config.extract_pipelines:
            return

        workspace_key = WorkspaceKey(
            platform=FABRIC_WORKSPACE_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            workspace_id=workspace.id,
        )

        for item in self.client.list_items(
            workspace.id, item_type=FabricItemType.DATA_PIPELINE
        ):
            if not self.config.pipeline_pattern.allowed(item.name):
                self.report.report_pipeline_filtered(item.name)
                continue

            self.report.report_pipeline_scanned()
            logger.debug(f"Processing pipeline: {item.name} ({item.id})")

            flow_id = make_pipeline_flow_id(
                workspace_id=workspace.id,
                pipeline_id=item.id,
            )
            dataflow = DataFlow(
                platform=PLATFORM,
                name=flow_id,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                display_name=item.name,
                description=item.description,
                custom_properties={
                    "pipeline_id": item.id,
                    "workspace_id": workspace.id,
                },
                parent_container=workspace_key,
            )
            yield dataflow

            yield from self._process_pipeline_activities(item, dataflow, workspace_key)

            if self.config.extract_pipeline_runs:
                yield from self._process_pipeline_runs(item, dataflow.urn)

    def _process_pipeline_activities(
        self,
        pipeline_item: FabricItem,
        dataflow: DataFlow,
        workspace_key: WorkspaceKey,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Emit DataJobs for pipeline activities with dependency lineage."""
        try:
            activities = self.client.get_pipeline_activities(
                workspace_id=pipeline_item.workspace_id,
                pipeline_id=pipeline_item.id,
            )
        except Exception as e:
            self.report.report_warning(
                title="Failed to Fetch Pipeline Activities",
                message="Could not retrieve activities. Skipping activities for this pipeline.",
                context=f"pipeline={pipeline_item.name}",
                exc=e,
            )
            return

        if not activities:
            return

        # Build activity_name → DataJobUrn lookup for dependency resolution
        activity_urn_map: dict[str, DataJobUrn] = {}
        for activity in activities:
            job_id = make_activity_job_id(activity.name)
            activity_urn_map[activity.name] = DataJobUrn.create_from_ids(
                job_id=job_id,
                data_flow_urn=str(dataflow.urn),
            )

        # Emit each activity as a DataJob
        for activity in activities:
            custom_props: dict[str, str] = {
                "activity_type": activity.type,
            }
            if activity.state:
                custom_props["state"] = activity.state
            if activity.on_inactive_mark_as:
                custom_props["on_inactive_mark_as"] = activity.on_inactive_mark_as

            # Resolve upstream job dependencies from dependsOn
            upstream_edges: list[EdgeClass] = []
            if activity.depends_on:
                for dep in activity.depends_on:
                    upstream_urn = activity_urn_map.get(dep.activity)
                    if upstream_urn:
                        edge_props: Optional[Dict[str, str]] = None
                        if dep.dependency_conditions:
                            edge_props = {
                                "dependencyConditions": ",".join(
                                    dep.dependency_conditions
                                )
                            }
                        upstream_edges.append(
                            EdgeClass(
                                destinationUrn=str(upstream_urn),
                                properties=edge_props,
                            )
                        )
                    else:
                        self.report.report_warning(
                            title="Unknown Activity Dependency",
                            message="Activity depends on an activity not found in this pipeline.",
                            context=f"pipeline={pipeline_item.name}, "
                            f"activity={activity.name}, dependency={dep.activity}",
                        )

            # Extract dataset lineage for Copy activities
            input_urns: list[str] = []
            output_urns: list[str] = []
            if activity.type == "Copy":
                input_urns, output_urns = self._copy_lineage_extractor.extract_lineage(
                    activity=activity,
                    workspace_id=pipeline_item.workspace_id,
                )

            datajob = DataJob(
                name=make_activity_job_id(activity.name),
                flow=dataflow,
                display_name=activity.name,
                custom_properties=custom_props,
                extra_aspects=[
                    DataJobInputOutputClass(
                        inputDatasets=input_urns,
                        outputDatasets=output_urns,
                        inputDatajobEdges=upstream_edges,
                    )
                ]
                if upstream_edges or input_urns or output_urns
                else None,
            )
            datajob._set_container(workspace_key)

            yield datajob
            self.report.report_activity_scanned()

    def _process_pipeline_runs(
        self,
        pipeline_item: FabricItem,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit DataProcessInstances for recent pipeline runs and their activity runs."""
        lookback_window_start = datetime.now(timezone.utc) - timedelta(
            days=self.config.lookback_days
        )

        try:
            runs = self.client.get_pipeline_runs(
                workspace_id=pipeline_item.workspace_id,
                pipeline_id=pipeline_item.id,
                lookback_window_start=lookback_window_start,
            )
        except Exception as e:
            self.report.report_warning(
                title="Failed to Fetch Pipeline Runs",
                message="Could not retrieve run history. Skipping runs for this pipeline.",
                context=f"pipeline={pipeline_item.name}",
                exc=e,
            )
            return

        for run in runs:
            if run.status == ItemJobStatus.NOT_STARTED:
                continue
            yield from self._emit_pipeline_run(run, flow_urn)
            self.report.report_pipeline_run_scanned()

            # Query activity runs for this pipeline run
            yield from self._process_activity_runs(
                pipeline_item=pipeline_item,
                run=run,
                flow_urn=flow_urn,
            )

    def _emit_pipeline_run(
        self,
        run: FabricJobInstance,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit MCPs for a single pipeline run as a DataProcessInstance."""
        dpi_id = make_pipeline_run_id(
            workspace_id=run.workspace_id,
            pipeline_id=run.item_id,
            run_id=run.id,
        )

        custom_properties: dict[str, str] = {
            "run_id": run.id,
            "workspace_id": run.workspace_id,
            "pipeline_id": run.item_id,
        }
        if run.invoke_type is not None:
            custom_properties["invoke_type"] = run.invoke_type
        if run.failure_reason:
            custom_properties["failure_reason"] = run.failure_reason

        dpi = DataProcessInstance(
            id=dpi_id,
            orchestrator=PLATFORM,
            cluster=self.config.env or "PROD",
            template_urn=flow_urn,
            type=DataProcessTypeClass.BATCH_SCHEDULED,
            properties=custom_properties,
            data_platform_instance=self.config.platform_instance,
        )

        # Cache the URN for parent_instance linkage in activity runs
        self._pipeline_run_urns[run.id] = dpi.urn

        start_ts_millis = (
            _parse_iso_to_millis(run.start_time_utc) if run.start_time_utc else None
        )
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts_millis,
            materialize_iolets=False,
        ):
            yield mcp.as_workunit()

        if start_ts_millis:
            for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts_millis):
                yield mcp.as_workunit()

        result = _FABRIC_STATUS_TO_RESULT.get(run.status)
        if result is not None and run.end_time_utc:
            end_ts_millis = _parse_iso_to_millis(run.end_time_utc)
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=end_ts_millis,
                result=result,
                result_type=PLATFORM,
                start_timestamp_millis=start_ts_millis,
            ):
                yield mcp.as_workunit()

    def _process_activity_runs(
        self,
        pipeline_item: FabricItem,
        run: FabricJobInstance,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Query and emit DataProcessInstances for activity runs within a pipeline run."""
        try:
            activity_runs = self.client.query_activity_runs(
                workspace_id=pipeline_item.workspace_id,
                pipeline_run_id=run.id,
                lookback_window_start=ACTIVITY_RUNS_MIN_START,
                lookback_window_end=datetime.now(timezone.utc),
            )
        except Exception as e:
            self.report.report_warning(
                title="Failed to Fetch Activity Runs",
                message="Could not retrieve activity runs. Skipping for this pipeline run.",
                context=f"pipeline={pipeline_item.name}, run={run.id}",
                exc=e,
            )
            return

        for activity_run in activity_runs:
            yield from self._emit_activity_run(
                activity_run=activity_run,
                pipeline_item=pipeline_item,
                flow_urn=flow_urn,
            )
            self.report.report_activity_run_scanned()

    def _emit_activity_run(
        self,
        activity_run: PipelineActivityRun,
        pipeline_item: FabricItem,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit MCPs for a single activity run as a DataProcessInstance."""
        # Build the template URN: the activity DataJob this run belongs to
        job_id = make_activity_job_id(activity_run.activity_name)
        template_urn = DataJobUrn.create_from_ids(
            job_id=job_id,
            data_flow_urn=str(flow_urn),
        )

        dpi_id = make_activity_run_id(
            pipeline_id=pipeline_item.id,
            activity_run_id=activity_run.activity_run_id,
        )

        custom_properties: dict[str, str] = {
            "activity_run_id": activity_run.activity_run_id,
            "pipeline_run_id": activity_run.pipeline_run_id,
            "activity_type": activity_run.activity_type,
        }
        if activity_run.duration_in_ms is not None:
            custom_properties["duration_in_ms"] = str(activity_run.duration_in_ms)
        if activity_run.error_message:
            custom_properties["error_message"] = activity_run.error_message
        if activity_run.retry_attempt is not None:
            custom_properties["retry_attempt"] = str(activity_run.retry_attempt)

        parent_instance_urn = self._pipeline_run_urns.get(activity_run.pipeline_run_id)

        dpi = DataProcessInstance(
            id=dpi_id,
            orchestrator=PLATFORM,
            cluster=self.config.env or "PROD",
            template_urn=template_urn,
            parent_instance=parent_instance_urn,
            type=DataProcessTypeClass.BATCH_SCHEDULED,
            properties=custom_properties,
            data_platform_instance=self.config.platform_instance,
        )

        start_ts_millis = (
            _parse_iso_to_millis(activity_run.activity_run_start)
            if activity_run.activity_run_start
            else None
        )
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts_millis,
            materialize_iolets=False,
        ):
            yield mcp.as_workunit()

        if start_ts_millis:
            for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts_millis):
                yield mcp.as_workunit()

        result = _ACTIVITY_STATUS_TO_RESULT.get(activity_run.status)
        if result is not None and activity_run.activity_run_end:
            end_ts_millis = _parse_iso_to_millis(activity_run.activity_run_end)
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=end_ts_millis,
                result=result,
                result_type=PLATFORM,
                start_timestamp_millis=start_ts_millis,
            ):
                yield mcp.as_workunit()
