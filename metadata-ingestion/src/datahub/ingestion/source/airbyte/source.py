import logging
from functools import partial
from typing import Dict, Iterable, List, Optional

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.client import (
    AirbyteAuthenticationError,
    AirbyteBaseClient,
    create_airbyte_client,
)
from datahub.ingestion.source.airbyte.config import (
    KNOWN_SOURCE_TYPE_MAPPING,
    AirbyteSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDatasetUrns,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamConfig,
    AirbyteStreamDetails,
    AirbyteStreamInfo,
    AirbyteTagInfo,
    DataFlowResult,
    DataJobResult,
    PlatformInfo,
    PropertyFieldPath,
    ValidatedPipelineIds,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    FabricTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn

logger = logging.getLogger(__name__)

# Mapping from Airbyte job status to DataHub InstanceRunResult
AIRBYTE_JOB_STATUS_MAP = {
    "succeeded": InstanceRunResult.SUCCESS,
    "completed": InstanceRunResult.SUCCESS,
    "success": InstanceRunResult.SUCCESS,
    "failed": InstanceRunResult.FAILURE,
    "failure": InstanceRunResult.FAILURE,
    "error": InstanceRunResult.FAILURE,
    "cancelled": InstanceRunResult.SKIPPED,
    "canceled": InstanceRunResult.SKIPPED,
    "running": InstanceRunResult.UP_FOR_RETRY,
    "incomplete": InstanceRunResult.UP_FOR_RETRY,
    "pending": InstanceRunResult.UP_FOR_RETRY,
}


def _sanitize_platform_name(platform_name: str) -> str:
    """Sanitize platform name for URNs (lowercase, spaces to hyphens)."""
    return platform_name.lower().replace(" ", "-")


def _map_source_type_to_platform(
    source_type: str, source_type_mapping: Dict[str, str]
) -> str:
    """Map Airbyte source type to DataHub platform name."""
    if source_type in source_type_mapping:
        return source_type_mapping[source_type]

    source_type_lower = source_type.lower()
    if source_type_lower in KNOWN_SOURCE_TYPE_MAPPING:
        return KNOWN_SOURCE_TYPE_MAPPING[source_type_lower]

    return _sanitize_platform_name(source_type)


def _validate_urn_component(component: str, component_name: str) -> str:
    """Validate URN component for empty values and invalid characters."""
    if not component or not component.strip():
        raise ValueError(f"{component_name} cannot be empty")

    invalid_chars = [",", "(", ")"]
    for char in invalid_chars:
        if char in component:
            logger.warning(
                f"URN component '{component_name}' contains invalid character '{char}': {component}"
            )

    if " " in component.strip():
        logger.warning(
            "URN component '%s' contains spaces: %s", component_name, component
        )

    return component.strip()


@platform_name("Airbyte")
@config_class(AirbyteSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default",
)
@capability(SourceCapability.TAGS, "Requires recipe configuration")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on.",
)
class AirbyteSource(StatefulIngestionSourceBase):
    platform: str
    report: StaleEntityRemovalSourceReport
    client: AirbyteBaseClient

    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.platform = "airbyte"
        self.source_config = config
        self.client = create_airbyte_client(config)
        self.report = StaleEntityRemovalSourceReport()
        self._warned_source_ids: set[str] = set()
        self._warned_destination_ids: set[str] = set()
        self.known_urns: set[str] = set()
        self._source_platform_cache: Dict[str, PlatformInfo] = {}
        self._dest_platform_cache: Dict[str, PlatformInfo] = {}

        logger.debug(
            "Initialized Airbyte source with deployment type: %s",
            config.deployment_type,
        )
        if config.platform_instance:
            logger.debug("Using platform instance: %s", config.platform_instance)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AirbyteSource":
        config = AirbyteSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _get_platform_for_source(self, source: AirbyteSourcePartial) -> PlatformInfo:
        """Return platform information for source."""
        if source.source_id in self._source_platform_cache:
            return self._source_platform_cache[source.source_id]

        source_details = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )

        if source_details.platform:
            platform = source_details.platform
        elif source.source_type:
            platform = _map_source_type_to_platform(
                source.source_type, self.source_config.source_type_mapping
            )
        elif source.name:
            if source.source_id not in self._warned_source_ids:
                self.report.warning(
                    title="Platform Detection Fallback",
                    message=f"Source {source.source_id} missing source_type, using name as fallback",
                    context=f"source_id={source.source_id}, source_name={source.name}",
                )
                self._warned_source_ids.add(source.source_id)
            platform = _map_source_type_to_platform(
                source.name, self.source_config.source_type_mapping
            )
        else:
            if source.source_id not in self._warned_source_ids:
                self.report.warning(
                    title="Platform Detection Failed",
                    message=f"Source {source.source_id} missing both source_type and name",
                    context=f"source_id={source.source_id}",
                )
                self._warned_source_ids.add(source.source_id)
            platform = ""

        result = PlatformInfo(
            platform=platform,
            platform_instance=source_details.platform_instance,
            env=source_details.env,
        )
        self._source_platform_cache[source.source_id] = result
        return result

    def _get_platform_for_destination(
        self, destination: AirbyteDestinationPartial
    ) -> PlatformInfo:
        """Return platform information for destination."""
        if destination.destination_id in self._dest_platform_cache:
            return self._dest_platform_cache[destination.destination_id]

        dest_details = self.source_config.destinations_to_platform_instance.get(
            destination.destination_id, PlatformDetail()
        )

        if dest_details.platform:
            platform = dest_details.platform
        elif destination.destination_type:
            platform = _map_source_type_to_platform(
                destination.destination_type, self.source_config.source_type_mapping
            )
        elif destination.name:
            if destination.destination_id not in self._warned_destination_ids:
                self.report.warning(
                    title="Platform Detection Fallback",
                    message=f"Destination {destination.destination_id} missing destination_type, using name as fallback",
                    context=f"destination_id={destination.destination_id}, destination_name={destination.name}",
                )
                self._warned_destination_ids.add(destination.destination_id)
            platform = _map_source_type_to_platform(
                destination.name, self.source_config.source_type_mapping
            )
        else:
            if destination.destination_id not in self._warned_destination_ids:
                self.report.warning(
                    title="Platform Detection Failed",
                    message=f"Destination {destination.destination_id} missing both destination_type and name",
                    context=f"destination_id={destination.destination_id}",
                )
                self._warned_destination_ids.add(destination.destination_id)
            platform = ""

        result = PlatformInfo(
            platform=platform,
            platform_instance=dest_details.platform_instance,
            env=dest_details.env,
        )
        self._dest_platform_cache[destination.destination_id] = result
        return result

    def _validate_pipeline_ids(
        self, pipeline_info: AirbytePipelineInfo, operation: str
    ) -> Optional[ValidatedPipelineIds]:
        """Validate and extract required IDs from pipeline info.

        Args:
            pipeline_info: Pipeline information to validate
            operation: Operation name for logging purposes

        Returns:
            ValidatedPipelineIds object if valid, None otherwise
        """
        workspace_id = pipeline_info.workspace.workspace_id
        connection_id = pipeline_info.connection.connection_id

        if not workspace_id or not connection_id:
            self.report.warning(
                title="Missing Pipeline IDs",
                message=f"Skipping {operation} - missing required IDs",
                context=f"workspace_id={workspace_id}, connection_id={connection_id}",
            )
            return None

        return ValidatedPipelineIds(
            workspace_id=workspace_id, connection_id=connection_id
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(auto_incremental_lineage, self.source_config.incremental_lineage),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def _get_pipelines(self) -> Iterable[AirbytePipelineInfo]:
        for workspace in self.client.list_workspaces(
            pattern=self.source_config.workspace_pattern
        ):
            try:
                if not workspace.workspace_id:
                    self.report.warning(
                        title="Invalid Workspace",
                        message="Skipping workspace with missing ID",
                        context=f"workspace_name={workspace.name}",
                    )
                    continue

                logger.debug("Processing workspace %s", workspace.workspace_id)

                for connection in self.client.list_connections(
                    workspace.workspace_id,
                    pattern=self.source_config.connection_pattern,
                ):
                    try:
                        if (
                            not connection.connection_id
                            or not connection.source_id
                            or not connection.destination_id
                        ):
                            self.report.warning(
                                title="Invalid Connection",
                                message="Skipping connection with missing required IDs",
                                context=f"connection_name={connection.name}, connection_id={connection.connection_id}, source_id={connection.source_id}, destination_id={connection.destination_id}",
                            )
                            continue

                        logger.debug(
                            f"Processing connection {connection.connection_id}"
                        )

                        connection = self.client.get_connection(
                            connection.connection_id
                        )

                        source = self.client.get_source(connection.source_id)
                        destination = self.client.get_destination(
                            connection.destination_id
                        )

                        if (
                            source.name
                            and not self.source_config.source_pattern.allowed(
                                source.name
                            )
                        ):
                            logger.debug(
                                f"Skipping source {source.name} due to source pattern filter"
                            )
                            continue

                        if (
                            destination.name
                            and not self.source_config.destination_pattern.allowed(
                                destination.name
                            )
                        ):
                            logger.debug(
                                f"Skipping destination {destination.name} due to destination pattern filter"
                            )
                            continue

                        yield AirbytePipelineInfo(
                            workspace=workspace,
                            connection=connection,
                            source=source,
                            destination=destination,
                        )
                    except AirbyteAuthenticationError:
                        logger.error("Authentication failed. Stopping ingestion.")
                        raise
                    except Exception as e:
                        conn_id = getattr(connection, "connection_id", "unknown")
                        conn_name = getattr(connection, "name", "unknown")
                        ws_id = getattr(workspace, "workspace_id", "unknown")
                        self.report.report_failure(
                            message="Failed to process connection",
                            context=f"workspace-{ws_id}/connection-{conn_id}/{conn_name}",
                            exc=e,
                        )
            except AirbyteAuthenticationError:
                logger.error("Authentication failed. Stopping ingestion.")
                raise
            except Exception as e:
                workspace_id = getattr(workspace, "workspace_id", "unknown")
                self.report.report_failure(
                    message="Failed to process workspace",
                    context=f"workspace-{workspace_id}",
                    exc=e,
                )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for pipeline_info in self._get_pipelines():
            try:
                yield from self._create_dataflow_workunits(pipeline_info)
                yield from self._create_lineage_workunits(pipeline_info)
            except Exception as e:
                conn_id = pipeline_info.connection.connection_id or "unknown"
                self.report.report_failure(
                    message="Failed to process pipeline",
                    context=f"pipeline-{conn_id}",
                    exc=e,
                )

    def _create_dataflow_workunits(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        workspace = pipeline_info.workspace
        workspace_id = workspace.workspace_id
        workspace_name = workspace.name or "Unnamed Workspace"

        dataflow_urn = make_data_flow_urn(
            orchestrator=self.platform,
            flow_id=workspace_id,
            cluster=self.source_config.env,
            platform_instance=self.source_config.platform_instance,
        )

        dataflow_info = DataFlowInfoClass(
            name=workspace_id,
            description=f"Airbyte workspace: {workspace_name}",
            externalUrl=f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}",
            customProperties={
                "workspace_id": workspace_id,
                "platform": "airbyte",
                "deployment_type": self.source_config.deployment_type.value,
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataflow_urn, aspect=dataflow_info
        ).as_workunit()

    def _enrich_job_properties_with_details(
        self, properties: Dict[str, str], job_details: dict, stream_name: str
    ) -> None:
        """Enrich job execution properties with detailed metrics from get_job().

        Args:
            properties: Dictionary to enrich with job details
            job_details: Response from get_job() API call
            stream_name: Name of the stream to extract stream-specific metrics
        """
        if job_details.get("bytesCommitted"):
            properties["bytes_committed"] = str(job_details["bytesCommitted"])
        if job_details.get("recordsCommitted"):
            properties["records_committed"] = str(job_details["recordsCommitted"])

        stream_stats = job_details.get("streamStatuses", [])
        for stream_stat in stream_stats:
            if stream_stat.get("streamName") == stream_name:
                if stream_stat.get("recordsCommitted") is not None:
                    properties["stream_records_committed"] = str(
                        stream_stat["recordsCommitted"]
                    )
                if stream_stat.get("bytesCommitted") is not None:
                    properties["stream_bytes_committed"] = str(
                        stream_stat["bytesCommitted"]
                    )
                break

        if job_details.get("failureReason"):
            failure_info = job_details["failureReason"]
            if isinstance(failure_info, dict):
                if failure_info.get("failureType"):
                    properties["failure_type"] = failure_info["failureType"]
                if failure_info.get("externalMessage"):
                    properties["failure_message"] = failure_info["externalMessage"]
            elif isinstance(failure_info, str):
                properties["failure_message"] = failure_info

    def _create_job_executions_workunits(
        self,
        pipeline_info: AirbytePipelineInfo,
        datajob_urn: DataJobUrn,
        stream_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Create job execution work units for a DataJob representing a specific stream.

        Note: pipeline_info is validated in _get_pipelines, so IDs are guaranteed to exist.

        Args:
            pipeline_info: Airbyte pipeline information
            datajob_urn: URN of the DataJob
            stream_name: Name of the stream

        Returns:
            Iterable of work units for job executions
        """
        connection_id = pipeline_info.connection.connection_id
        workspace_id = pipeline_info.workspace.workspace_id

        try:
            jobs = self.client.list_jobs(
                connection_id=connection_id,
                workspace_id=workspace_id,
                start_date=self.source_config.job_status_start_date,
                end_date=self.source_config.job_status_end_date,
                limit=self.source_config.job_statuses_limit,
            )

            logger.debug(
                f"Found {len(jobs)} jobs for connection {connection_id} and stream {stream_name}"
            )

            for job in jobs:
                try:
                    job_id = job.get("id")
                    if not job_id:
                        continue

                    job_details = None
                    try:
                        job_details = self.client.get_job(str(job_id))
                    except Exception as e:
                        logger.debug(
                            f"Could not fetch detailed job info for {job_id}: {e}"
                        )

                    attempts = job.get("attempts", [])
                    for attempt_idx, attempt in enumerate(attempts):
                        attempt_id = attempt.get("id", attempt_idx)
                        attempt_status = attempt.get("status", "").lower()

                        result = AIRBYTE_JOB_STATUS_MAP.get(
                            attempt_status, InstanceRunResult.FAILURE
                        )

                        start_time_millis = attempt.get("createdAt")
                        end_time_millis = attempt.get("endedAt")

                        if not start_time_millis:
                            continue

                        execution_id = f"{job_id}-{attempt_id}-{stream_name}"

                        properties = {
                            "job_id": str(job_id),
                            "attempt_id": str(attempt_id),
                            "connection_id": connection_id,
                            "stream_name": stream_name,
                            "status": attempt_status,
                        }

                        if job_details:
                            self._enrich_job_properties_with_details(
                                properties, job_details, stream_name
                            )

                        job_url = f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}/connections/{connection_id}/status"

                        dpi = DataProcessInstance(
                            orchestrator=self.platform,
                            id=execution_id,
                            template_urn=datajob_urn,
                            properties=properties,
                            url=job_url,
                        )

                        for mcp in dpi.generate_mcp(
                            created_ts_millis=start_time_millis,
                            materialize_iolets=False,
                        ):
                            yield mcp.as_workunit()

                        for mcp in dpi.start_event_mcp(
                            start_timestamp_millis=start_time_millis,
                        ):
                            yield mcp.as_workunit()

                        if end_time_millis and attempt_status not in [
                            "running",
                            "pending",
                            "incomplete",
                        ]:
                            for mcp in dpi.end_event_mcp(
                                end_timestamp_millis=end_time_millis,
                                start_timestamp_millis=start_time_millis,
                                result=result,
                                result_type=self.platform,
                            ):
                                yield mcp.as_workunit()
                except Exception as e:
                    self.report.warning(
                        title="Job Processing Failed",
                        message="Failed to process job execution",
                        context=f"job_id={job.get('id', 'unknown')}, connection_id={connection_id}, stream={stream_name}",
                        exc=e,
                    )
                    continue

        except Exception as e:
            self.report.report_failure(
                message="Failed to process job executions",
                context=f"job-executions-{connection_id}-{stream_name}",
                exc=e,
            )

    def _fetch_streams_for_source(
        self, pipeline_info: AirbytePipelineInfo
    ) -> List[AirbyteStreamInfo]:
        """Fetch stream details from connection sync catalog.

        Note: pipeline_info is validated in _get_pipelines, so source is guaranteed to exist.

        We extract streams from the connection's sync_catalog rather than the /streams API endpoint
        because the endpoint is not available in all Airbyte versions (notably absent in v0.30.1).
        The sync_catalog approach is more reliable and works across all Airbyte versions.
        """
        source_id = pipeline_info.source.source_id
        source_schema = pipeline_info.source.get_schema

        if not source_id:
            self.report.warning(
                title="Missing Source ID",
                message="Cannot fetch streams - source_id is None",
                context=f"source_name={pipeline_info.source.name}",
            )
            return []

        streams = []
        if not pipeline_info.connection.sync_catalog:
            self.report.warning(
                title="Missing Sync Catalog",
                message=f"Connection {pipeline_info.connection.connection_id} has no sync_catalog",
                context=f"connection_id={pipeline_info.connection.connection_id}, connection_name={pipeline_info.connection.name}",
            )
            return []

        if not pipeline_info.connection.sync_catalog.streams:
            self.report.warning(
                title="Empty Sync Catalog",
                message=f"Connection {pipeline_info.connection.connection_id} sync_catalog has no streams",
                context=f"connection_id={pipeline_info.connection.connection_id}, connection_name={pipeline_info.connection.name}",
            )
            return []

        logger.debug(
            f"Found {len(pipeline_info.connection.sync_catalog.streams)} stream configs in sync_catalog"
        )

        if (
            pipeline_info.connection.sync_catalog
            and pipeline_info.connection.sync_catalog.streams
        ):
            for stream_config in pipeline_info.connection.sync_catalog.streams:
                if not stream_config or not stream_config.stream:
                    logger.debug("Skipping stream_config with no stream")
                    continue
                if not stream_config.is_enabled():
                    logger.debug(
                        f"Skipping disabled stream: {stream_config.stream.name if stream_config.stream else 'unknown'}"
                    )
                    continue

                stream = stream_config.stream
                namespace = (
                    stream.namespace if stream.namespace else source_schema or ""
                )

                properties = {}
                if stream.json_schema and "properties" in stream.json_schema:
                    properties = stream.json_schema.get("properties", {})
                elif not stream.json_schema:
                    logger.debug(
                        f"Stream {stream.name} has no jsonSchema - column-level lineage will not be available"
                    )

                property_fields = []
                for field_name, _field_schema in properties.items():
                    if stream_config.is_field_selected(field_name):
                        property_fields.append(PropertyFieldPath(path=[field_name]))

                stream_details = AirbyteStreamDetails(
                    stream_name=stream.name,
                    namespace=namespace,
                    property_fields=property_fields,
                )
                streams.append(
                    AirbyteStreamInfo(config=stream_config, details=stream_details)
                )

        logger.debug("Using %d streams from connection sync catalog", len(streams))
        return streams

    def _fetch_tags_for_workspace(self, workspace_id: str) -> List[AirbyteTagInfo]:
        try:
            tags_list = self.client.list_tags(workspace_id)
            logger.debug("Retrieved %d tags from Airbyte", len(tags_list))
            return [AirbyteTagInfo.model_validate(tag) for tag in tags_list]
        except Exception as e:
            self.report.warning(
                title="Tag Retrieval Failed",
                message="Failed to retrieve tags from Airbyte",
                context=f"workspace_id={workspace_id}",
                exc=e,
            )
            return []

    def _extract_connection_tags(
        self, pipeline_info: AirbytePipelineInfo, airbyte_tags: List[AirbyteTagInfo]
    ) -> List[str]:
        """Extract tag URNs for a connection."""
        tags: List[str] = []
        connection_id = pipeline_info.connection.connection_id

        for tag_info in airbyte_tags:
            if tag_info.resource_id == connection_id and tag_info.name:
                tags.append(make_tag_urn(tag_info.name))

        return tags

    def _create_connection_dataflow(
        self, pipeline_info: AirbytePipelineInfo, tags: List[str]
    ) -> DataFlowResult:
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        connection_id = connection.connection_id
        source_id = source.source_id or ""
        destination_id = destination.destination_id or ""
        source_name = source.name or "Unknown Source"
        destination_name = destination.name or "Unknown Destination"
        workspace_id = workspace.workspace_id
        workspace_name = workspace.name or "Unnamed Workspace"
        connection_name = connection.name or "Unnamed Connection"

        connection_dataflow_urn = DataFlowUrn(
            orchestrator=self.platform,
            flow_id=connection_id,
            cluster=self.source_config.env,
        )

        dataflow_info = DataFlowInfoClass(
            name=connection_name,
            description=f"Airbyte connection from {source_name} to {destination_name}",
            externalUrl=f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}/connections/{connection_id}",
            customProperties={
                "connection_id": connection_id,
                "source_id": source_id,
                "destination_id": destination_id,
                "source_name": source_name,
                "destination_name": destination_name,
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "platform": "airbyte",
                "deployment_type": self.source_config.deployment_type.value,
            },
        )

        work_units = []

        if tags:
            dataflow_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=tag) for tag in tags]
            )
            work_units.append(
                MetadataChangeProposalWrapper(
                    entityUrn=connection_dataflow_urn.urn(), aspect=dataflow_tags
                ).as_workunit()
            )

        work_units.append(
            MetadataChangeProposalWrapper(
                entityUrn=connection_dataflow_urn.urn(), aspect=dataflow_info
            ).as_workunit()
        )

        return DataFlowResult(
            dataflow_urn=connection_dataflow_urn, work_units=work_units
        )

    def _create_stream_datajob(
        self,
        connection_dataflow_urn: DataFlowUrn,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        source_urn: str,
        destination_urn: str,
        tags: List[str],
    ) -> DataJobResult:
        """Create DataJob for stream with column-level lineage on InputOutput aspect."""
        work_units: List[MetadataWorkUnit] = []
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        connection_id = connection.connection_id
        stream_name = stream.stream_name or ""
        source_name = source.name or "Unknown Source"
        destination_name = destination.name or "Unknown Destination"
        workspace_id = workspace.workspace_id

        job_id = f"{connection_id}_{stream_name}"
        datajob_urn = DataJobUrn(flow=connection_dataflow_urn, job_id=job_id)

        datajob_info = DataJobInfoClass(
            name=stream_name,
            description=f"Airbyte sync for stream {stream_name} from {source_name} to {destination_name}",
            externalUrl=f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}/connections/{connection_id}",
            type="BATCH",
            customProperties={
                "stream_name": stream_name,
                "namespace": stream.namespace,
                "connection_id": connection_id,
                "connection_name": connection.name or "",
                "source_id": source.source_id if source else "",
                "source_name": source_name,
                "destination_id": destination.destination_id if destination else "",
                "destination_name": destination_name,
            },
        )

        fine_grained_lineages: List[FineGrainedLineageClass] = []
        if self.source_config.extract_column_level_lineage:
            property_fields = stream.get_column_names()
            if property_fields:
                logger.debug(
                    f"Processing column-level lineage for {len(property_fields)} columns on DataJob"
                )
                for column_name in property_fields:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[make_schema_field_urn(source_urn, column_name)],
                            downstreams=[
                                make_schema_field_urn(destination_urn, column_name)
                            ],
                        )
                    )

        input_output = DataJobInputOutputClass(
            inputDatasets=[source_urn],
            outputDatasets=[destination_urn],
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )

        work_units.append(
            MetadataChangeProposalWrapper(
                entityUrn=datajob_urn.urn(), aspect=datajob_info
            ).as_workunit()
        )

        work_units.append(
            MetadataChangeProposalWrapper(
                entityUrn=datajob_urn.urn(), aspect=input_output
            ).as_workunit()
        )

        if tags:
            job_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=tag) for tag in tags]
            )
            work_units.append(
                MetadataChangeProposalWrapper(
                    entityUrn=datajob_urn.urn(), aspect=job_tags
                ).as_workunit()
            )

        return DataJobResult(datajob_urn=datajob_urn, work_units=work_units)

    def _create_dataset_lineage(
        self,
        source_urn: str,
        destination_urn: str,
        stream: AirbyteStreamDetails,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit dataset lineage with column-level mappings. Marked as non-primary source."""
        work_units = []

        fine_grained_lineages: List[FineGrainedLineageClass] = []
        if self.source_config.extract_column_level_lineage:
            property_fields = stream.get_column_names()
            if property_fields:
                for column_name in property_fields:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[make_schema_field_urn(source_urn, column_name)],
                            downstreams=[
                                make_schema_field_urn(destination_urn, column_name)
                            ],
                        )
                    )

        # Only emit lineage if destination is also a source (prevents phantom datasets)
        if destination_urn in self.known_urns:
            work_units.append(
                MetadataChangeProposalWrapper(
                    entityUrn=destination_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=source_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        ],
                        fineGrainedLineages=fine_grained_lineages or None,
                    ),
                ).as_workunit(is_primary_source=False)
            )
        else:
            logger.debug(
                "Skipping lineage emission for destination '%s' - not in known URNs. "
                "Destination connector should create this dataset.",
                destination_urn,
            )

        return work_units

    def _collect_source_urns(self, pipeline_info: AirbytePipelineInfo) -> None:
        """First pass: collect all source dataset URNs that Airbyte reads from."""
        if (
            not pipeline_info.connection.sync_catalog
            or not pipeline_info.connection.sync_catalog.streams
        ):
            return

        source = pipeline_info.source
        platform_info = self._get_platform_for_source(source)

        source_details = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )

        for stream_config in pipeline_info.connection.sync_catalog.streams:
            if not stream_config or not stream_config.stream:
                continue

            stream = stream_config.stream
            schema_name = stream.namespace or ""
            table_name = stream.name

            if platform_info.platform and table_name:
                dataset_name = (
                    f"{schema_name}.{table_name}" if schema_name else table_name
                )
                if source_details.convert_urns_to_lowercase:
                    dataset_name = dataset_name.lower()
                source_urn = make_dataset_urn_with_platform_instance(
                    platform=_sanitize_platform_name(platform_info.platform),
                    name=dataset_name,
                    env=platform_info.env or FabricTypeClass.PROD,
                    platform_instance=platform_info.platform_instance,
                )
                self.known_urns.add(source_urn)

    def _create_dataset_urns(
        self,
        pipeline_info: AirbytePipelineInfo,
        stream_config: AirbyteStreamConfig,
        stream: AirbyteStreamDetails,
        platform_instance: Optional[str],
    ) -> AirbyteDatasetUrns:
        source = pipeline_info.source
        destination = pipeline_info.destination

        source_platform_info = self._get_platform_for_source(source)
        dest_platform_info = self._get_platform_for_destination(destination)

        source_details = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )
        dest_details = self.source_config.destinations_to_platform_instance.get(
            destination.destination_id, PlatformDetail()
        )

        # Log connection-level namespace configuration for troubleshooting
        connection = pipeline_info.connection
        namespace_def = connection.get_namespace_definition
        namespace_fmt = connection.get_namespace_format
        table_prefix = connection.get_prefix

        if namespace_def or namespace_fmt or table_prefix:
            logger.debug(
                "Connection %s has namespace config: definition=%s, format=%s, prefix=%s",
                connection.connection_id,
                namespace_def or "None",
                namespace_fmt or "None",
                table_prefix or "None",
            )

        schema_name = self._resolve_schema_name(
            stream.namespace,
            source.get_schema,
            stream.stream_name,
            source.source_id,
        )
        table_name = stream.stream_name

        if table_prefix:
            logger.debug("Applying prefix '%s' to table '%s'", table_prefix, table_name)
            table_name = f"{table_prefix}{table_name}"

        if not schema_name and source.configuration:
            logger.debug(
                "No schema found for source %s (type: %s). Configuration keys: %s",
                source.source_id,
                source.source_type,
                list(source.configuration.keys()) if source.configuration else "None",
            )

        if "." in table_name:
            logger.debug(
                "Stream name '%s' contains dots - may be fully qualified. Will extract table name only.",
                table_name,
            )
            table_name = table_name.split(".")[-1]

        source_database = source.get_database

        if not source_database and source.configuration:
            logger.debug(
                "No database found for source %s (type: %s). Configuration keys: %s",
                source.source_id,
                source.source_type,
                list(source.configuration.keys()) if source.configuration else "None",
            )

        source_dataset_name = self._build_dataset_name(
            source_database,
            schema_name,
            table_name,
            source_details.include_schema_in_urn,
            "source",
        )

        validated_source_platform = _validate_urn_component(
            source_platform_info.platform, "source_platform"
        )
        validated_source_name = _validate_urn_component(
            source_dataset_name, "source_dataset_name"
        )

        normalized_source_name = (
            validated_source_name.lower()
            if source_details.convert_urns_to_lowercase
            else validated_source_name
        )

        source_urn = make_dataset_urn_with_platform_instance(
            platform=_sanitize_platform_name(validated_source_platform),
            name=normalized_source_name,
            env=source_platform_info.env or FabricTypeClass.PROD,
            platform_instance=source_platform_info.platform_instance,
        )

        dest_schema = self._resolve_destination_schema(
            stream_config=stream_config,
            connection=connection,
            source_schema=schema_name,
            destination=destination,
            stream_name=stream.stream_name,
        )

        dest_database = destination.get_database

        if not dest_schema and destination.configuration:
            logger.debug(
                "No schema found for destination %s (type: %s). Using source schema: %s, Configuration keys: %s",
                destination.destination_id,
                destination.destination_type,
                schema_name or "None",
                list(destination.configuration.keys())
                if destination.configuration
                else "None",
            )

        if not dest_database and destination.configuration:
            logger.debug(
                "No database found for destination %s (type: %s). Configuration keys: %s",
                destination.destination_id,
                destination.destination_type,
                list(destination.configuration.keys())
                if destination.configuration
                else "None",
            )
        dest_dataset_name = self._build_dataset_name(
            dest_database,
            dest_schema,
            table_name,
            dest_details.include_schema_in_urn,
            "destination",
        )

        validated_dest_platform = _validate_urn_component(
            dest_platform_info.platform, "destination_platform"
        )
        validated_dest_name = _validate_urn_component(
            dest_dataset_name, "destination_dataset_name"
        )

        normalized_dest_name = (
            validated_dest_name.lower()
            if dest_details.convert_urns_to_lowercase
            else validated_dest_name
        )

        destination_urn = make_dataset_urn_with_platform_instance(
            platform=_sanitize_platform_name(validated_dest_platform),
            name=normalized_dest_name,
            env=dest_platform_info.env or FabricTypeClass.PROD,
            platform_instance=dest_platform_info.platform_instance,
        )

        logger.debug("Created lineage from %s to %s", source_urn, destination_urn)

        return AirbyteDatasetUrns(
            source_urn=source_urn, destination_urn=destination_urn
        )

    def _resolve_schema_name(
        self,
        stream_namespace: Optional[str],
        config_schema: Optional[str],
        stream_name: str,
        source_id: str,
    ) -> str:
        """Resolve schema name following Airbyte's precedence.

        Precedence:
        1. Per-stream namespace (most specific)
        2. Source config schema (connector default)
        """
        if stream_namespace:
            if config_schema and stream_namespace != config_schema:
                logger.debug(
                    "Stream '%s' namespace '%s' differs from config schema '%s' - using stream namespace",
                    stream_name,
                    stream_namespace,
                    config_schema,
                )
            return stream_namespace
        return config_schema or ""

    def _resolve_destination_schema(
        self,
        stream_config: AirbyteStreamConfig,
        connection: "AirbyteConnectionPartial",
        source_schema: str,
        destination: "AirbyteDestinationPartial",
        stream_name: str,
    ) -> str:
        """Resolve destination schema following Airbyte's namespace precedence.

        Airbyte's namespace precedence for destinations:
        1. Per-stream destinationNamespace (from stream_config)
        2. Connection-level namespace rules (namespace_definition + namespace_format)
        3. Destination default schema (destination.get_schema)
        4. Fall back to source schema

        Args:
            stream_config: Stream configuration from Airbyte sync catalog
            connection: Connection configuration
            source_schema: The resolved source schema
            destination: Airbyte destination object
            stream_name: Name of the stream for logging

        Returns:
            Resolved destination schema name
        """
        stream_dest_namespace = stream_config.get_destination_namespace()
        if stream_dest_namespace:
            logger.debug(
                "Stream '%s' has destinationNamespace override: '%s'",
                stream_name,
                stream_dest_namespace,
            )
            return stream_dest_namespace

        namespace_def = connection.get_namespace_definition

        if namespace_def:
            logger.debug(
                "Applying connection namespace_definition '%s' for stream '%s'",
                namespace_def,
                stream_name,
            )

            if namespace_def == "source":
                logger.debug(
                    "Using source schema '%s' for destination (namespace_definition=source)",
                    source_schema,
                )
                return source_schema

            elif namespace_def == "destination":
                dest_config_schema = destination.get_schema
                if dest_config_schema:
                    logger.debug(
                        "Using destination default schema '%s' (namespace_definition=destination)",
                        dest_config_schema,
                    )
                    return dest_config_schema

            elif namespace_def == "customformat":
                namespace_fmt = connection.get_namespace_format
                if namespace_fmt:
                    custom_namespace = namespace_fmt.replace(
                        "${SOURCE_NAMESPACE}", source_schema
                    )
                    logger.debug(
                        "Applied custom namespace format '%s' -> '%s' for stream '%s'",
                        namespace_fmt,
                        custom_namespace,
                        stream_name,
                    )
                    return custom_namespace

        dest_config_schema = destination.get_schema
        if dest_config_schema:
            if source_schema and dest_config_schema != source_schema:
                logger.debug(
                    "Destination schema '%s' differs from source schema '%s' for stream '%s'",
                    dest_config_schema,
                    source_schema,
                    stream_name,
                )
            return dest_config_schema

        logger.debug(
            "No destination schema found, falling back to source schema '%s'",
            source_schema,
        )
        return source_schema

    def _build_dataset_name(
        self,
        database: Optional[str],
        schema: str,
        table: str,
        include_schema_override: Optional[bool],
        entity_type: str,
    ) -> str:
        """Build dataset name with 2-tier/3-tier auto-detection."""
        if database:
            should_include_schema = include_schema_override

            if should_include_schema is None:
                if schema and schema == database:
                    should_include_schema = False
                    logger.debug(
                        "Auto-detected 2-tier platform for %s: schema '%s' equals database '%s', excluding schema from URN",
                        entity_type,
                        schema,
                        database,
                    )
                else:
                    should_include_schema = True

            if schema and should_include_schema:
                return f"{database}.{schema}.{table}"
            else:
                return f"{database}.{table}"
        else:
            return f"{schema}.{table}" if schema else table

    def _create_lineage_workunits(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Create lineage work units between source and destination datasets.

        Note: pipeline_info is validated in _get_pipelines, so all required components exist.

        Args:
            pipeline_info: Information about the Airbyte pipeline

        Returns:
            Iterable of work units for the lineage
        """
        connection_id = pipeline_info.connection.connection_id
        workspace_id = pipeline_info.workspace.workspace_id
        source_name = pipeline_info.source.name or ""
        destination_name = pipeline_info.destination.name or ""

        logger.debug("Creating lineage for connection: %s", connection_id)

        if not source_name or not destination_name:
            self.report.warning(
                title="Missing Source or Destination Name",
                message="Missing source name or destination name for connection",
                context=f"connection_id={connection_id}, source_name={source_name}, destination_name={destination_name}",
            )
            return

        airbyte_tags = self._fetch_tags_for_workspace(workspace_id)
        tags = self._extract_connection_tags(pipeline_info, airbyte_tags)
        dataflow_result = self._create_connection_dataflow(pipeline_info, tags)

        yield from dataflow_result.work_units

        platform_instance = None
        if (
            self.source_config.use_workspace_name_as_platform_instance
            and pipeline_info.workspace.name
        ):
            platform_instance = pipeline_info.workspace.name
        else:
            platform_instance = self.source_config.platform_instance

        streams = self._fetch_streams_for_source(pipeline_info)

        for stream_info in streams:
            try:
                dataset_urns = self._create_dataset_urns(
                    pipeline_info,
                    stream_info.config,
                    stream_info.details,
                    platform_instance,
                )

                datajob_result = self._create_stream_datajob(
                    dataflow_result.dataflow_urn,
                    pipeline_info,
                    stream_info.details,
                    dataset_urns.source_urn,
                    dataset_urns.destination_urn,
                    tags,
                )

                yield from datajob_result.work_units

                if self.source_config.include_statuses:
                    yield from self._create_job_executions_workunits(
                        pipeline_info=pipeline_info,
                        datajob_urn=datajob_result.datajob_urn,
                        stream_name=stream_info.details.stream_name,
                    )

                lineage_workunits = self._create_dataset_lineage(
                    dataset_urns.source_urn,
                    dataset_urns.destination_urn,
                    stream_info.details,
                )

                yield from lineage_workunits

            except Exception as e:
                conn_name = getattr(pipeline_info.connection, "name", "unknown")
                ws_id = getattr(pipeline_info.workspace, "workspace_id", "unknown")
                self.report.report_failure(
                    message="Failed to process stream",
                    context=f"workspace-{ws_id}/connection-{connection_id}/{conn_name}/stream-{stream_info.details.stream_name}",
                    exc=e,
                )

    def get_report(self) -> SourceReport:
        return self.report
