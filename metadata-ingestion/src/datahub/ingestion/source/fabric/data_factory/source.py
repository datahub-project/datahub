"""Microsoft Fabric Data Factory ingestion source for DataHub."""

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
from datahub.ingestion.source.common.subtypes import DataJobSubTypes
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import (
    FabricConnection,
    FabricItem,
    FabricItemType,
    FabricJobInstance,
    FabricWorkspace,
    ItemJobStatus,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.common.urn_generator import (
    make_activity_job_id,
    make_activity_job_urn,
    make_activity_run_id,
    make_pipeline_flow_id,
    make_pipeline_flow_urn,
    make_pipeline_run_id,
)
from datahub.ingestion.source.fabric.common.utils import (
    build_workspace_container,
    make_workspace_key,
    parse_iso_datetime,
)
from datahub.ingestion.source.fabric.data_factory.client import (
    FabricDataFactoryClient,
)
from datahub.ingestion.source.fabric.data_factory.config import (
    FabricDataFactorySourceConfig,
)
from datahub.ingestion.source.fabric.data_factory.lineage import (
    CopyActivityLineageExtractor,
    InvokePipelineLineageExtractor,
)
from datahub.ingestion.source.fabric.data_factory.models import (
    InvokePipelineActivityLineage,
    PipelineActivity,
    PipelineActivityRun,
)
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
# Fabric activity type → DataHub DataJob subtype.
# Reuses ADF subtypes where activity semantics match.
_ACTIVITY_SUBTYPE_MAP: dict[str, str] = {
    "Copy": DataJobSubTypes.ADF_COPY_ACTIVITY,
    "InvokePipeline": DataJobSubTypes.ADF_EXECUTE_PIPELINE,
    "ExecutePipeline": DataJobSubTypes.ADF_EXECUTE_PIPELINE,
    "Lookup": DataJobSubTypes.ADF_LOOKUP_ACTIVITY,
    "GetMetadata": DataJobSubTypes.ADF_GET_METADATA_ACTIVITY,
    "SqlServerStoredProcedure": DataJobSubTypes.ADF_STORED_PROCEDURE_ACTIVITY,
    "Script": DataJobSubTypes.ADF_SCRIPT_ACTIVITY,
    "WebActivity": DataJobSubTypes.ADF_WEB_ACTIVITY,
    "WebHook": DataJobSubTypes.ADF_WEBHOOK_ACTIVITY,
    "IfCondition": DataJobSubTypes.ADF_IF_CONDITION,
    "ForEach": DataJobSubTypes.ADF_FOREACH_LOOP,
    "Until": DataJobSubTypes.ADF_UNTIL_LOOP,
    "Wait": DataJobSubTypes.ADF_WAIT_ACTIVITY,
    "SetVariable": DataJobSubTypes.ADF_SET_VARIABLE,
    "AppendVariable": DataJobSubTypes.ADF_APPEND_VARIABLE,
    "Switch": DataJobSubTypes.ADF_SWITCH_ACTIVITY,
    "Filter": DataJobSubTypes.ADF_FILTER_ACTIVITY,
    "Fail": DataJobSubTypes.ADF_FAIL_ACTIVITY,
    "Delete": DataJobSubTypes.ADF_DELETE_ACTIVITY,
    "Custom": DataJobSubTypes.ADF_CUSTOM_ACTIVITY,
    "AzureFunction": DataJobSubTypes.ADF_AZURE_FUNCTION_ACTIVITY,
    "DatabricksNotebook": DataJobSubTypes.ADF_DATABRICKS_NOTEBOOK,
    # Fabric-specific activity types
    "SparkJobDefinition": DataJobSubTypes.FABRIC_SPARK_JOB_DEFINITION,
    "InvokeCopyJob": DataJobSubTypes.FABRIC_INVOKE_COPY_JOB,
    "ExecuteSSISPackage": DataJobSubTypes.FABRIC_EXECUTE_SSIS_PACKAGE,
    "AzureHDInsight": DataJobSubTypes.FABRIC_HDINSIGHT_ACTIVITY,
    "KustoQueryLanguage": DataJobSubTypes.FABRIC_KQL_ACTIVITY,
    "DataLakeAnalyticsScope": DataJobSubTypes.FABRIC_DATA_LAKE_ANALYTICS,
    "AzureMLExecutePipeline": DataJobSubTypes.FABRIC_AZURE_ML_EXECUTE_PIPELINE,
    "TridentNotebook": DataJobSubTypes.FABRIC_TRIDENT_NOTEBOOK,
    "RefreshDataFlow": DataJobSubTypes.FABRIC_REFRESH_DATAFLOW,
    "Office365Email": DataJobSubTypes.FABRIC_OFFICE365_EMAIL,
    "Email": DataJobSubTypes.FABRIC_EMAIL_ACTIVITY,
    "MicrosoftTeams": DataJobSubTypes.FABRIC_TEAMS_ACTIVITY,
    "Teams": DataJobSubTypes.FABRIC_TEAMS_ACTIVITY,
    "PBISemanticModelRefresh": DataJobSubTypes.FABRIC_PBI_SEMANTIC_MODEL_REFRESH,
}

# Activity runs use "Succeeded"/"Failed"/"Cancelled" (not ItemJobStatus constants)
_ACTIVITY_STATUS_TO_RESULT: dict[str, InstanceRunResult] = {
    "Succeeded": InstanceRunResult.SUCCESS,
    "Failed": InstanceRunResult.FAILURE,
    "Cancelled": InstanceRunResult.SKIPPED,
}


def _parse_iso_to_millis(iso_str: str) -> int:
    """Convert an ISO 8601 timestamp string to epoch milliseconds."""
    dt = parse_iso_datetime(iso_str)
    return int(dt.timestamp() * 1000)


@platform_name("Fabric Data Factory")
@config_class(FabricDataFactorySourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default via Copy and InvokePipeline activities",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via stateful_ingestion config",
)
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

        # Pipeline activities cache: (workspace_id, pipeline_id) → list[PipelineActivity]
        # Populated in first pass of _process_pipelines, used by InvokePipeline lineage.
        self._pipeline_activities_cache: dict[
            tuple[str, str], list[PipelineActivity]
        ] = {}

        # Cross-pipeline edges: child_datajob_urn → list of parent InvokePipeline URNs.
        # Built after pass 1 resolves all InvokePipeline activities, consumed in pass 2
        # when emitting the child activity's DataJobInputOutput to merge the edge in.
        self._cross_pipeline_edges: dict[str, list[str]] = {}

    def _cache_connections(self) -> None:
        """Fetch all tenant connections and cache them keyed by ID."""
        try:
            for raw in self.client.list_connections():
                connection_id = raw.get("id", "")
                if connection_id:
                    self._connections_cache[connection_id] = FabricConnection.from_dict(
                        raw
                    )
            logger.info(f"Cached {len(self._connections_cache)} Fabric connection(s)")
        except Exception as e:
            self.report.report_warning(
                title="Failed to Cache Connections",
                message="Could not retrieve tenant connections. "
                "Copy activity lineage may be incomplete.",
                context="",
                exc=e,
            )

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
            # Tenant-wide caches
            self._cache_connections()
            self._copy_lineage_extractor = CopyActivityLineageExtractor(
                connections_cache=self._connections_cache,
                report=self.report,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
                platform_instance_map=self.config.platform_instance_map,
            )
            self._invoke_pipeline_extractor = InvokePipelineLineageExtractor(
                pipeline_activities_cache=self._pipeline_activities_cache,
                report=self.report,
                platform=PLATFORM,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            # Pass 1: fetch and cache all pipeline activities across all
            # workspaces so that cross-workspace InvokePipeline edges can be
            # resolved regardless of workspace processing order.
            workspace_pipelines: list[tuple[FabricWorkspace, list[FabricItem]]] = []
            for workspace in self.client.list_workspaces():
                if not self.config.workspace_pattern.allowed(workspace.name):
                    self.report.report_workspace_filtered(workspace.name)
                    continue

                self.report.report_workspace_scanned()
                logger.info(f"Processing workspace: {workspace.name} ({workspace.id})")

                if self.config.extract_pipelines:
                    pipeline_items = self._fetch_pipeline_activities(workspace.id)
                else:
                    pipeline_items = []
                workspace_pipelines.append((workspace, pipeline_items))

            # Pass 2: resolve edges and emit (cache is now fully populated)
            for workspace, pipeline_items in workspace_pipelines:
                try:
                    yield from self._create_workspace_container(workspace)

                    if self.config.extract_pipelines and pipeline_items:
                        invoke_pipeline_lineage = self._resolve_invoke_pipeline_edges(
                            workspace.id, pipeline_items
                        )
                        yield from self._emit_pipelines(
                            workspace,
                            pipeline_items,
                            invoke_pipeline_lineage,
                        )

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

    def _fetch_pipeline_activities(self, workspace_id: str) -> list[FabricItem]:
        """Fetch pipelines and their activities for a workspace into cache.

        Applies pipeline_pattern filter. Populates
        ``self._pipeline_activities_cache``.
        """
        pipeline_items: list[FabricItem] = []
        for item in self.client.list_items(
            workspace_id, item_type=FabricItemType.DATA_PIPELINE
        ):
            if not self.config.pipeline_pattern.allowed(item.name):
                self.report.report_pipeline_filtered(item.name)
                continue
            pipeline_items.append(item)
            try:
                activities = self.client.get_pipeline_activities(
                    workspace_id=workspace_id,
                    pipeline_id=item.id,
                )
                self._pipeline_activities_cache[(workspace_id, item.id)] = activities
            except Exception as e:
                self.report.report_warning(
                    title="Failed to Fetch Pipeline Activities",
                    message="Could not retrieve activities. "
                    "Skipping activities for this pipeline.",
                    context=f"pipeline={item.name}",
                    exc=e,
                )
                self._pipeline_activities_cache[(workspace_id, item.id)] = []
        return pipeline_items

    def _create_workspace_container(
        self, workspace: FabricWorkspace
    ) -> Iterable[Entity]:
        yield from build_workspace_container(
            workspace=workspace,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    @staticmethod
    def _get_pipeline_url(workspace_id: str, pipeline_id: str) -> str:
        """Generate Fabric web UI URL for a data pipeline."""
        return f"{FABRIC_APP_BASE_URL}/groups/{workspace_id}/pipelines/{pipeline_id}"

    @staticmethod
    def _get_pipeline_run_url(workspace_id: str, pipeline_id: str, run_id: str) -> str:
        """Generate Fabric web UI URL for a specific pipeline run."""
        return (
            f"{FABRIC_APP_BASE_URL}/workloads/data-pipeline"
            f"/monitoring/workspaces/{workspace_id}"
            f"/pipelines/{pipeline_id}/{run_id}"
        )

    def _emit_pipelines(
        self,
        workspace: FabricWorkspace,
        pipeline_items: list[FabricItem],
        invoke_pipeline_lineage: dict[str, InvokePipelineActivityLineage],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Emit DataFlow and DataJob entities for cached pipelines."""
        workspace_key = make_workspace_key(
            workspace_id=workspace.id,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        for item in pipeline_items:
            self.report.report_pipeline_scanned()
            logger.debug(f"Emitting pipeline: {item.name} ({item.id})")

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
                external_url=self._get_pipeline_url(workspace.id, item.id),
                custom_properties={
                    "pipeline_id": item.id,
                    "workspace_id": workspace.id,
                },
                parent_container=workspace_key,
            )
            yield dataflow

            yield from self._emit_pipeline_activities(
                item, dataflow, workspace_key, invoke_pipeline_lineage
            )

            if self.config.include_execution_history:
                yield from self._process_pipeline_runs(item, dataflow.urn)

    def _resolve_invoke_pipeline_edges(
        self,
        workspace_id: str,
        pipeline_items: list[FabricItem],
    ) -> dict[str, InvokePipelineActivityLineage]:
        """Resolve all InvokePipeline activities and build cross-pipeline edge map.

        Must run after all activities are cached so that child pipeline
        lookups succeed regardless of processing order.

        Returns a cache keyed by the InvokePipeline activity's DataJob URN
        string. Also populates ``self._cross_pipeline_edges`` mapping
        child root activity URN → list of parent InvokePipeline URNs.
        """
        invoke_pipeline_lineage: dict[str, InvokePipelineActivityLineage] = {}

        for item in pipeline_items:
            flow_urn = make_pipeline_flow_urn(
                workspace_id=workspace_id,
                pipeline_id=item.id,
                platform=PLATFORM,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            activities = self._pipeline_activities_cache.get(
                (workspace_id, item.id), []
            )
            for activity in activities:
                if activity.type != "InvokePipeline":
                    continue

                result = self._invoke_pipeline_extractor.extract_lineage(
                    activity=activity,
                    parent_workspace_id=workspace_id,
                )
                if not result:
                    continue

                parent_job_urn = str(make_activity_job_urn(activity.name, flow_urn))
                invoke_pipeline_lineage[parent_job_urn] = result

                if result.child_datajob_urn:
                    self._cross_pipeline_edges.setdefault(
                        result.child_datajob_urn, []
                    ).append(parent_job_urn)

        return invoke_pipeline_lineage

    def _emit_pipeline_activities(
        self,
        pipeline_item: FabricItem,
        dataflow: DataFlow,
        workspace_key: WorkspaceKey,
        invoke_pipeline_lineage: dict[str, InvokePipelineActivityLineage],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Emit DataJobs for pipeline activities with dependency lineage."""
        activities = self._pipeline_activities_cache.get(
            (pipeline_item.workspace_id, pipeline_item.id), []
        )

        if not activities:
            return

        # Build activity_name → DataJobUrn lookup for dependency resolution
        activity_urn_map: dict[str, DataJobUrn] = {}
        for activity in activities:
            activity_urn_map[activity.name] = make_activity_job_urn(
                activity.name, dataflow.urn
            )

        for activity in activities:
            try:
                custom_props: dict[str, str] = {
                    "activity_type": activity.type,
                }
                if activity.state:
                    custom_props["state"] = activity.state
                if activity.on_inactive_mark_as:
                    custom_props["on_inactive_mark_as"] = activity.on_inactive_mark_as

                upstream_edges = self._resolve_upstream_edges(
                    activity, activity_urn_map
                )
                input_urns, output_urns = self._extract_activity_lineage(
                    activity, pipeline_item
                )

                datajob_urn_str = str(activity_urn_map[activity.name])
                self._merge_cross_pipeline_info(
                    datajob_urn_str,
                    custom_props,
                    upstream_edges,
                    invoke_pipeline_lineage,
                )

                has_io = upstream_edges or input_urns or output_urns
                datajob = DataJob(
                    name=make_activity_job_id(activity.name),
                    flow=dataflow,
                    display_name=activity.name,
                    description=activity.description,
                    subtype=_ACTIVITY_SUBTYPE_MAP.get(activity.type),
                    external_url=self._get_pipeline_url(
                        pipeline_item.workspace_id, pipeline_item.id
                    ),
                    custom_properties=custom_props,
                    extra_aspects=[
                        DataJobInputOutputClass(
                            inputDatasets=input_urns,
                            outputDatasets=output_urns,
                            inputDatajobEdges=upstream_edges,
                        )
                    ]
                    if has_io
                    else None,
                )
                datajob._set_container(workspace_key)

                yield datajob
                self.report.report_activity_scanned()
            except Exception as e:
                self.report.report_warning(
                    title="Failed to Emit Activity",
                    message="Error processing activity. Skipping to next.",
                    context=f"pipeline={pipeline_item.name}, "
                    f"activity={activity.name}, type={activity.type}",
                    exc=e,
                )

    def _resolve_upstream_edges(
        self,
        activity: PipelineActivity,
        activity_urn_map: dict[str, DataJobUrn],
    ) -> list[EdgeClass]:
        """Build upstream edges from activity dependsOn references."""
        edges: list[EdgeClass] = []
        for dep in activity.depends_on:
            upstream_urn = activity_urn_map.get(dep.activity)
            if not upstream_urn:
                continue
            edge_props: Optional[Dict[str, str]] = None
            if dep.dependency_conditions:
                edge_props = {
                    "dependencyConditions": ",".join(dep.dependency_conditions)
                }
            edges.append(
                EdgeClass(
                    destinationUrn=str(upstream_urn),
                    properties=edge_props,
                )
            )
        return edges

    def _extract_activity_lineage(
        self,
        activity: PipelineActivity,
        pipeline_item: FabricItem,
    ) -> tuple[list[str], list[str]]:
        """Dispatch lineage extraction based on activity type.

        Returns (input_urns, output_urns).
        """
        if activity.type == "Copy":
            return self._extract_copy_activity_lineage(activity, pipeline_item)
        return [], []

    def _extract_copy_activity_lineage(
        self,
        activity: PipelineActivity,
        pipeline_item: FabricItem,
    ) -> tuple[list[str], list[str]]:
        """Extract dataset lineage for a Copy activity, with resilient reporting.

        Returns (input_urns, output_urns).
        """
        activity_key = f"{pipeline_item.name}.{activity.name}"
        try:
            input_urns, output_urns = self._copy_lineage_extractor.extract_lineage(
                activity=activity,
                workspace_id=pipeline_item.workspace_id,
            )
            if input_urns or output_urns:
                self.report.report_lineage_extracted()
            else:
                self.report.report_lineage_failed(activity_key)
                self.report.report_warning(
                    title="Copy Activity Lineage Not Resolved",
                    message="Could not resolve dataset lineage. "
                    "Retrieve the pipeline definition JSON to "
                    "identify the unsupported format.",
                    context=activity_key,
                )
            return input_urns, output_urns
        except Exception as e:
            self.report.report_lineage_failed(activity_key)
            self.report.report_warning(
                title="Copy Activity Lineage Extraction Error",
                message="Unexpected error extracting lineage. "
                "Activity will be emitted without lineage.",
                context=activity_key,
                exc=e,
            )
            return [], []

    def _merge_cross_pipeline_info(
        self,
        datajob_urn_str: str,
        custom_props: dict[str, str],
        upstream_edges: list[EdgeClass],
        invoke_pipeline_lineage: dict[str, InvokePipelineActivityLineage],
    ) -> None:
        """Merge InvokePipeline properties and cross-pipeline parent edges."""
        invoke_result = invoke_pipeline_lineage.get(datajob_urn_str)
        if invoke_result:
            custom_props.update(invoke_result.custom_properties)

        for parent_urn in self._cross_pipeline_edges.get(datajob_urn_str, []):
            upstream_edges.append(EdgeClass(destinationUrn=parent_urn))

    def _process_pipeline_runs(
        self,
        pipeline_item: FabricItem,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit DataProcessInstances for recent pipeline runs and their activity runs."""
        lookback_window_start = datetime.now(timezone.utc) - timedelta(
            days=self.config.execution_history_days
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
            try:
                yield from self._emit_pipeline_run(run, flow_urn)
                self.report.report_pipeline_run_scanned()

                # Query activity runs for this pipeline run
                yield from self._process_activity_runs(
                    pipeline_item=pipeline_item,
                    run=run,
                    flow_urn=flow_urn,
                )
            except Exception as e:
                self.report.report_warning(
                    title="Failed to Process Pipeline Run",
                    message="Error processing pipeline run. Skipping to next run.",
                    context=f"pipeline={pipeline_item.name}, run={run.id}",
                    exc=e,
                )

    @staticmethod
    def _emit_dpi_workunits(
        dpi: DataProcessInstance,
        start_time_iso: Optional[str],
        end_time_iso: Optional[str],
        run_result: Optional[InstanceRunResult],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit generate/start/end MCPs for a DataProcessInstance.

        Shared by pipeline-run and activity-run emission paths.
        """
        start_ts_millis = (
            _parse_iso_to_millis(start_time_iso) if start_time_iso else None
        )
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts_millis,
            materialize_iolets=False,
        ):
            yield mcp.as_workunit()

        if start_ts_millis:
            for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts_millis):
                yield mcp.as_workunit()

        if run_result is not None and end_time_iso:
            end_ts_millis = _parse_iso_to_millis(end_time_iso)
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=end_ts_millis,
                result=run_result,
                result_type=PLATFORM,
                start_timestamp_millis=start_ts_millis,
            ):
                yield mcp.as_workunit()

    def _emit_pipeline_run(
        self,
        run: FabricJobInstance,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit MCPs for a single pipeline run as a DataProcessInstance."""
        dpi_id = make_pipeline_run_id(run_id=run.id)

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
            url=self._get_pipeline_run_url(run.workspace_id, run.item_id, run.id),
            data_platform_instance=self.config.platform_instance,
        )

        # Cache the URN for parent_instance linkage in activity runs
        self._pipeline_run_urns[run.id] = dpi.urn

        yield from self._emit_dpi_workunits(
            dpi=dpi,
            start_time_iso=run.start_time_utc,
            end_time_iso=run.end_time_utc,
            run_result=_FABRIC_STATUS_TO_RESULT.get(run.status),
        )

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
            try:
                yield from self._emit_activity_run(
                    activity_run=activity_run,
                    pipeline_item=pipeline_item,
                    flow_urn=flow_urn,
                )
                self.report.report_activity_run_scanned()
            except Exception as e:
                self.report.report_warning(
                    title="Failed to Emit Activity Run",
                    message="Error processing activity run. Skipping to next.",
                    context=f"pipeline={pipeline_item.name}, "
                    f"activity={activity_run.activity_name}, "
                    f"run={activity_run.activity_run_id}",
                    exc=e,
                )

    def _emit_activity_run(
        self,
        activity_run: PipelineActivityRun,
        pipeline_item: FabricItem,
        flow_urn: DataFlowUrn,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit MCPs for a single activity run as a DataProcessInstance."""
        # Build the template URN: the activity DataJob this run belongs to
        template_urn = make_activity_job_urn(activity_run.activity_name, flow_urn)

        dpi_id = make_activity_run_id(
            pipeline_run_id=activity_run.pipeline_run_id,
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
            url=self._get_pipeline_run_url(
                pipeline_item.workspace_id,
                pipeline_item.id,
                activity_run.pipeline_run_id,
            ),
            data_platform_instance=self.config.platform_instance,
        )

        yield from self._emit_dpi_workunits(
            dpi=dpi,
            start_time_iso=activity_run.activity_run_start,
            end_time_iso=activity_run.activity_run_end,
            run_result=_ACTIVITY_STATUS_TO_RESULT.get(activity_run.status),
        )
