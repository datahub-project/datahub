import logging
from functools import partial
from typing import Dict, Iterable, List, Optional

from datahub.api.entities.datajob import DataJob
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
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteDatasetMapping,
    AirbyteDatasetUrns,
    AirbyteDestinationPartial,
    AirbyteInputOutputDatasets,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamDetails,
    AirbyteTagInfo,
    DataFlowResult,
    DataJobResult,
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
from datahub.utilities.urns.dataset_urn import DatasetUrn

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
    # Check if user provided an explicit mapping
    if source_type in source_type_mapping:
        return source_type_mapping[source_type]

    # Check known source type mapping
    source_type_lower = source_type.lower()
    if source_type_lower in KNOWN_SOURCE_TYPE_MAPPING:
        return KNOWN_SOURCE_TYPE_MAPPING[source_type_lower]

    # Otherwise sanitize the source type (lowercase, replace spaces with hyphens)
    return _sanitize_platform_name(source_type)


def _validate_urn_component(component: str, component_name: str) -> str:
    """Validate URN component for empty values and invalid characters."""
    if not component or not component.strip():
        raise ValueError(f"{component_name} cannot be empty")

    # Check for problematic characters
    invalid_chars = [",", "(", ")"]
    for char in invalid_chars:
        if char in component:
            logger.warning(
                f"URN component '{component_name}' contains invalid character '{char}': {component}"
            )

    # Check for spaces (common issue)
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

    def _get_platform_for_source(
        self, source: AirbyteSourcePartial
    ) -> tuple[str, Optional[str], Optional[str]]:
        """Return (platform, platform_instance, env) for source."""
        from datahub.ingestion.source.airbyte.config import PlatformDetail

        # Check if there's a per-source override
        source_details = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )

        # Determine platform
        if source_details.platform:
            platform = source_details.platform
        elif source.source_type:
            platform = _map_source_type_to_platform(
                source.source_type, self.source_config.source_type_mapping
            )
        elif source.name:
            self.report.warning(
                title="Platform Detection Fallback",
                message=f"Source {source.source_id} missing source_type, using name as fallback",
                context=f"source_id={source.source_id}, source_name={source.name}",
            )
            platform = _map_source_type_to_platform(
                source.name, self.source_config.source_type_mapping
            )
        else:
            self.report.warning(
                title="Platform Detection Failed",
                message=f"Source {source.source_id} missing both source_type and name",
                context=f"source_id={source.source_id}",
            )
            platform = ""

        platform_instance = source_details.platform_instance
        env = source_details.env

        return platform, platform_instance, env

    def _get_platform_for_destination(
        self, destination: AirbyteDestinationPartial
    ) -> tuple[str, Optional[str], Optional[str]]:
        """Return (platform, platform_instance, env) for destination."""
        from datahub.ingestion.source.airbyte.config import PlatformDetail

        # Check if there's a per-destination override
        dest_details = self.source_config.destinations_to_platform_instance.get(
            destination.destination_id, PlatformDetail()
        )

        # Determine platform
        if dest_details.platform:
            platform = dest_details.platform
        elif destination.destination_type:
            platform = _map_source_type_to_platform(
                destination.destination_type, self.source_config.source_type_mapping
            )
        elif destination.name:
            self.report.warning(
                title="Platform Detection Fallback",
                message=f"Destination {destination.destination_id} missing destination_type, using name as fallback",
                context=f"destination_id={destination.destination_id}, destination_name={destination.name}",
            )
            platform = _map_source_type_to_platform(
                destination.name, self.source_config.source_type_mapping
            )
        else:
            self.report.warning(
                title="Platform Detection Failed",
                message=f"Destination {destination.destination_id} missing both destination_type and name",
                context=f"destination_id={destination.destination_id}",
            )
            platform = ""

        platform_instance = dest_details.platform_instance
        env = dest_details.env

        return platform, platform_instance, env

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

                        # Fetch full connection details including syncCatalog
                        # list_connections() may return summaries without full syncCatalog
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
                        # Auth errors should fail fast - don't continue processing
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
                # Auth errors should fail fast - don't continue processing
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
                # Create dataflow for the workspace
                yield from self._create_dataflow_workunits(pipeline_info)

                # Create datajob for the connection
                yield from self._create_datajob_workunits(pipeline_info)

                # Create lineage information
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

    def _create_datajob_workunits(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Create datajob workunits for a connection.

        Note: pipeline_info is validated in _get_pipelines, so IDs are guaranteed to exist.
        """
        workspace = pipeline_info.workspace
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination

        workspace_id = workspace.workspace_id
        connection_id = connection.connection_id
        connection_name = connection.name or "Unnamed Connection"

        # Get platform details with per-source/destination overrides
        source_platform, _, _ = self._get_platform_for_source(source)
        destination_platform, _, _ = self._get_platform_for_destination(destination)

        # Prepare inputs and outputs from source and destination
        dataset_urns = AirbyteInputOutputDatasets()

        if connection.sync_catalog:
            dataset_urns = self._get_input_output_datasets(
                pipeline_info,
                source_platform,
                destination_platform,
            )

        # Create a DataJob object with the parent flow URN
        datajob = DataJob(
            id=connection_id,
            flow_urn=DataFlowUrn(
                orchestrator=self.platform,
                flow_id=workspace_id,
                cluster=self.source_config.env,
            ),
            name=connection_name,
        )

        # Add inlet and outlet datasets
        datajob.inlets = [
            DatasetUrn.from_string(input_urn) for input_urn in dataset_urns.input_urns
        ]
        datajob.outlets = [
            DatasetUrn.from_string(output_urn)
            for output_urn in dataset_urns.output_urns
        ]

        # Add custom properties
        datajob.properties = {
            "connection_id": connection_id,
            "source_id": source.source_id or "",
            "destination_id": destination.destination_id or "",
            "source_name": source.name or "",
            "destination_name": destination.name or "",
            "source_platform": source_platform,
            "destination_platform": destination_platform,
            "status": connection.status or "",
            "schedule_type": connection.schedule_type or "",
            "schedule_data": str(connection.schedule_data or {}),
            "platform": "airbyte",
            "deployment_type": self.source_config.deployment_type.value,
        }

        # Generate all MCPs for this datajob
        for mcp in datajob.generate_mcp(materialize_iolets=False):
            yield mcp.as_workunit()

    def _map_dataset_urn_components(
        self,
        platform_name: str,
        schema_name: str,
        table_name: str,
    ) -> AirbyteDatasetMapping:
        """Map dataset URN components using configuration.

        Args:
            platform_name: Source or destination platform name
            schema_name: Schema/namespace name (can be empty)
            table_name: Table/stream name

        Returns:
            Mapped dataset information for URN creation
        """
        platform_name = platform_name or ""
        schema_name = schema_name or ""

        mapping_config = self.source_config.platform_mapping.get_dataset_mapping(
            platform_name
        )
        # Sanitize platform name to remove spaces and special characters for URN compatibility
        mapped_platform = mapping_config.platform or _sanitize_platform_name(
            platform_name
        )
        mapped_env = mapping_config.env or "PROD"

        if (
            mapping_config.schema_mapping
            and schema_name in mapping_config.schema_mapping
        ):
            mapped_schema = mapping_config.schema_mapping[schema_name]
        else:
            prefix = mapping_config.schema_prefix or ""
            mapped_schema = f"{prefix}{schema_name}"

        # Determine the full dataset name
        if "." in table_name:
            mapped_dataset_name = table_name
        else:
            database_component = ""
            if (
                mapping_config.database_mapping
                and schema_name in mapping_config.database_mapping
            ):
                database_component = f"{mapping_config.database_mapping[schema_name]}."
            elif mapping_config.database_prefix:
                database_component = f"{mapping_config.database_prefix}."

            # Build the dataset name based on what components we have
            # For two-tier connectors (e.g., MySQL), mapped_schema may be empty
            if mapped_schema:
                mapped_dataset_name = (
                    f"{database_component}{mapped_schema}.{table_name}"
                )
            else:
                mapped_dataset_name = f"{database_component}{table_name}"

        return AirbyteDatasetMapping(
            platform=mapped_platform,
            name=mapped_dataset_name,
            env=mapped_env,
            platform_instance=mapping_config.platform_instance,
        )

    def _get_input_output_datasets(
        self,
        pipeline_info: AirbytePipelineInfo,
        source_platform: str,
        destination_platform: str,
    ) -> AirbyteInputOutputDatasets:
        """Get input and output dataset URNs for a connection.

        NOTE: This method does NOT apply per-source/per-destination platform_instance overrides.
        It only uses the platform mapping configuration. Per-source/per-destination overrides
        are applied in _create_dataset_urns which is used for per-stream lineage.
        """
        inputs: List[str] = []
        outputs: List[str] = []

        if (
            not pipeline_info.connection.sync_catalog
            or not pipeline_info.connection.sync_catalog.streams
        ):
            return AirbyteInputOutputDatasets(input_urns=inputs, output_urns=outputs)

        for stream_config in pipeline_info.connection.sync_catalog.streams:
            if not stream_config or not stream_config.stream:
                continue

            stream = stream_config.stream
            schema_name = stream.namespace or ""
            table_name = stream.name

            if source_platform and table_name:
                # Map source dataset components to URN
                source_mapping = self._map_dataset_urn_components(
                    source_platform, schema_name, table_name
                )

                # Use platform_instance from mapping only (no global fallback)
                source_urn = make_dataset_urn_with_platform_instance(
                    platform=source_mapping.platform,
                    name=source_mapping.name,
                    env=source_mapping.env
                    if source_mapping.env
                    else FabricTypeClass.PROD,
                    platform_instance=source_mapping.platform_instance,
                )
                inputs.append(source_urn)

            if destination_platform and table_name:
                # Map destination dataset components to URN
                dest_mapping = self._map_dataset_urn_components(
                    destination_platform, schema_name, table_name
                )

                # Use platform_instance from mapping only (no global fallback)
                destination_urn = make_dataset_urn_with_platform_instance(
                    platform=dest_mapping.platform,
                    name=dest_mapping.name,
                    env=dest_mapping.env if dest_mapping.env else FabricTypeClass.PROD,
                    platform_instance=dest_mapping.platform_instance,
                )
                outputs.append(destination_urn)

        return AirbyteInputOutputDatasets(input_urns=inputs, output_urns=outputs)

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

        # Use the configured start and end dates for job filtering
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

            # Process each job
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

                    # Process job attempts
                    attempts = job.get("attempts", [])
                    for attempt_idx, attempt in enumerate(attempts):
                        attempt_id = attempt.get("id", attempt_idx)
                        attempt_status = attempt.get("status", "").lower()

                        # Map status to result
                        result = AIRBYTE_JOB_STATUS_MAP.get(
                            attempt_status, InstanceRunResult.FAILURE
                        )

                        # Get timestamps
                        start_time_millis = attempt.get("createdAt")
                        end_time_millis = attempt.get("endedAt")

                        if not start_time_millis:
                            continue

                        # Create unique ID for this execution
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

                        # Create a DataProcessInstance for this execution
                        # URL links to connection's status page where job history is visible
                        job_url = f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}/connections/{connection_id}/status"

                        dpi = DataProcessInstance(
                            orchestrator=self.platform,
                            id=execution_id,
                            template_urn=datajob_urn,
                            properties=properties,
                            url=job_url,
                        )

                        # Generate the main entity MCPs
                        for mcp in dpi.generate_mcp(
                            created_ts_millis=start_time_millis,
                            materialize_iolets=False,
                        ):
                            yield mcp.as_workunit()

                        # Generate the start event MCP
                        for mcp in dpi.start_event_mcp(
                            start_timestamp_millis=start_time_millis,
                        ):
                            yield mcp.as_workunit()

                        # Generate the end event MCP if the job has completed
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
    ) -> List[AirbyteStreamDetails]:
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

        # Extract streams from connection sync catalog
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
                # Skip disabled streams
                if not stream_config.is_enabled():
                    logger.debug(
                        f"Skipping disabled stream: {stream_config.stream.name if stream_config.stream else 'unknown'}"
                    )
                    continue

                stream = stream_config.stream
                # Use the namespace from the stream if available, otherwise use source schema
                namespace = (
                    stream.namespace if stream.namespace else source_schema or ""
                )

                # Get property fields from the stream's JSON schema
                properties = {}
                if stream.json_schema and "properties" in stream.json_schema:
                    properties = stream.json_schema.get("properties", {})
                elif not stream.json_schema:
                    logger.debug(
                        f"Stream {stream.name} has no jsonSchema - column-level lineage will not be available"
                    )

                property_fields = []
                for field_name, _field_schema in properties.items():
                    # Check if field is selected for sync
                    if stream_config.is_field_selected(field_name):
                        property_fields.append([field_name])

                stream_details = {
                    "streamName": stream.name,
                    "namespace": namespace,
                    "propertyFields": property_fields,
                }
                streams.append(AirbyteStreamDetails.model_validate(stream_details))

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

        # Find tags associated with this connection
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

        # Create the connection DataFlow entity
        connection_dataflow_urn = DataFlowUrn(
            orchestrator=self.platform,
            flow_id=connection_id,
            cluster=self.source_config.env,
        )

        # Create dataflow info
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

        # Emit work units
        work_units = []

        # Add tags to the dataflow
        if tags:
            dataflow_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=tag) for tag in tags]
            )
            work_units.append(
                MetadataChangeProposalWrapper(
                    entityUrn=connection_dataflow_urn.urn(), aspect=dataflow_tags
                ).as_workunit()
            )

        # Emit the dataflow entity
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

        # Create a datajob for this stream
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

        # Create fine-grained lineage for column-level lineage (attached to DataJob, not dataset)
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

        # Create InputOutput aspect with fine-grained lineages
        input_output = DataJobInputOutputClass(
            inputDatasets=[source_urn],
            outputDatasets=[destination_urn],
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )

        # Emit the datajob entities
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

        # Add tags to the datajob
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
        tags: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit dataset lineage with column-level mappings. Marked as non-primary source."""
        work_units = []

        # Build fine-grained (column-level) lineages if enabled
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

        # Create dataset-level lineage with fine-grained lineages
        # Mark as non-primary since Airbyte is not authoritative for these datasets
        work_units.append(
            MetadataChangeProposalWrapper(
                entityUrn=destination_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=source_urn, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                    ],
                    fineGrainedLineages=fine_grained_lineages or None,
                ),
            ).as_workunit(is_primary_source=False)
        )

        # Add tags to datasets
        # Also marked as non-primary since Airbyte is not authoritative for these datasets
        if tags:
            logger.debug("Adding %d tags to source and destination datasets", len(tags))
            for dataset_urn in [source_urn, destination_urn]:
                global_tags = GlobalTagsClass(
                    tags=[TagAssociationClass(tag=tag) for tag in tags]
                )
                work_units.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=global_tags
                    ).as_workunit(is_primary_source=False)
                )

        return work_units

    def _create_dataset_urns(
        self,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        platform_instance: Optional[str],
    ) -> AirbyteDatasetUrns:
        source = pipeline_info.source
        destination = pipeline_info.destination

        # Get platform details with per-source/destination overrides
        source_platform, source_plat_instance, source_env = (
            self._get_platform_for_source(source)
        )
        (
            destination_platform,
            dest_plat_instance,
            dest_env,
        ) = self._get_platform_for_destination(destination)

        schema_name = stream.namespace or ""
        table_name = stream.stream_name

        # Get source and destination schema/database info
        source_schema = source.get_schema
        source_database = source.get_database
        destination_schema = destination.get_schema
        destination_database = destination.get_database

        if not schema_name and source_schema:
            schema_name = source_schema

        # Map source dataset components
        source_mapping = self._map_dataset_urn_components(
            source_platform, schema_name, table_name
        )

        # Prioritize per-source platform_instance, then mapping
        # NOTE: We do NOT fall back to the global platform_instance for dataset URNs
        # Following Fivetran's approach: only use per-source/per-destination overrides
        source_platform_instance = (
            source_plat_instance or source_mapping.platform_instance
        )

        # Prioritize per-source env, then mapping, then PROD
        source_env_final = source_env or source_mapping.env or FabricTypeClass.PROD

        # Create fully qualified dataset name including database
        source_mapped_name = source_mapping.name
        if source_database:
            # Check if database is already part of the mapped name
            if "." in source_mapped_name:
                # If it already contains dots (schema.table format), we need to prepend the database
                if not source_mapped_name.startswith(f"{source_database}."):
                    source_mapped_name = f"{source_database}.{source_mapped_name}"
            else:
                # If it's just a table name, add database and optionally schema
                # For two-tier connectors (e.g., MySQL), schema_name may be empty
                if schema_name:
                    source_mapped_name = f"{source_database}.{schema_name}.{table_name}"
                else:
                    source_mapped_name = f"{source_database}.{table_name}"
        else:
            # No database, ensure we at least have schema.table format (if schema exists)
            if "." not in source_mapped_name:
                if schema_name:
                    source_mapped_name = f"{schema_name}.{source_mapped_name}"
                # else: just use the mapped name as-is (already contains table name)

        # Validate URN components before creating URN
        validated_source_platform = _validate_urn_component(
            source_mapping.platform, "source_platform"
        )
        validated_source_name = _validate_urn_component(
            source_mapped_name, "source_dataset_name"
        )

        source_urn = make_dataset_urn_with_platform_instance(
            platform=validated_source_platform,
            name=validated_source_name,
            env=source_env_final,
            platform_instance=source_platform_instance,
        )

        # Use destination schema if specified
        dest_namespace = destination_schema if destination_schema else schema_name

        # Map the destination dataset components
        dest_mapping = self._map_dataset_urn_components(
            destination_platform, dest_namespace, table_name
        )

        # Prioritize per-destination platform_instance, then mapping
        # NOTE: We do NOT fall back to the global platform_instance for dataset URNs
        # Following Fivetran's approach: only use per-source/per-destination overrides
        dest_platform_instance = dest_plat_instance or dest_mapping.platform_instance

        # Prioritize per-destination env, then mapping, then PROD
        dest_env_final = dest_env or dest_mapping.env or FabricTypeClass.PROD

        # Create fully qualified dataset name including database
        dest_mapped_name = dest_mapping.name
        if destination_database:
            # Check if database is already part of the mapped name
            if "." in dest_mapped_name:
                # If it already contains dots (schema.table format), we need to prepend the database
                if not dest_mapped_name.startswith(f"{destination_database}."):
                    dest_mapped_name = f"{destination_database}.{dest_mapped_name}"
            else:
                # If it's just a table name, add database and optionally schema
                # For two-tier connectors (e.g., MySQL), dest_namespace may be empty
                if dest_namespace:
                    dest_mapped_name = (
                        f"{destination_database}.{dest_namespace}.{table_name}"
                    )
                else:
                    dest_mapped_name = f"{destination_database}.{table_name}"
        else:
            # No database, ensure we at least have schema.table format (if schema exists)
            if "." not in dest_mapped_name:
                if dest_namespace:
                    dest_mapped_name = f"{dest_namespace}.{dest_mapped_name}"
                # else: just use the mapped name as-is (already contains table name)

        # Validate destination URN components before creating URN
        validated_dest_platform = _validate_urn_component(
            dest_mapping.platform, "destination_platform"
        )
        validated_dest_name = _validate_urn_component(
            dest_mapped_name, "destination_dataset_name"
        )

        destination_urn = make_dataset_urn_with_platform_instance(
            platform=validated_dest_platform,
            name=validated_dest_name,
            env=dest_env_final,
            platform_instance=dest_platform_instance,
        )

        logger.debug("Created lineage from %s to %s", source_urn, destination_urn)

        return AirbyteDatasetUrns(
            source_urn=source_urn, destination_urn=destination_urn
        )

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

        # Get tags from Airbyte API
        airbyte_tags = self._fetch_tags_for_workspace(workspace_id)

        # Extract connection-specific tags
        tags = self._extract_connection_tags(pipeline_info, airbyte_tags)

        # Create the connection DataFlow entity
        dataflow_result = self._create_connection_dataflow(pipeline_info, tags)

        # Emit dataflow work units
        yield from dataflow_result.work_units

        # Determine platform instance
        platform_instance = None
        if (
            self.source_config.use_workspace_name_as_platform_instance
            and pipeline_info.workspace.name
        ):
            platform_instance = pipeline_info.workspace.name
        else:
            platform_instance = self.source_config.platform_instance

        # Get stream details
        streams = self._fetch_streams_for_source(pipeline_info)

        # Process each stream to create lineage
        for stream in streams:
            try:
                # Create source and destination URNs
                dataset_urns = self._create_dataset_urns(
                    pipeline_info, stream, platform_instance
                )

                # Create DataJob for this stream
                datajob_result = self._create_stream_datajob(
                    dataflow_result.dataflow_urn,
                    pipeline_info,
                    stream,
                    dataset_urns.source_urn,
                    dataset_urns.destination_urn,
                    tags,
                )

                # Emit datajob work units
                yield from datajob_result.work_units

                # Create job executions for this stream if enabled
                if self.source_config.include_statuses:
                    yield from self._create_job_executions_workunits(
                        pipeline_info=pipeline_info,
                        datajob_urn=datajob_result.datajob_urn,
                        stream_name=stream.stream_name,
                    )

                # Create dataset lineage
                lineage_workunits = self._create_dataset_lineage(
                    dataset_urns.source_urn, dataset_urns.destination_urn, stream, tags
                )

                # Emit lineage work units
                yield from lineage_workunits

            except Exception as e:
                # Get connection name for better context
                conn_name = getattr(pipeline_info.connection, "name", "unknown")
                ws_id = getattr(pipeline_info.workspace, "workspace_id", "unknown")
                self.report.report_failure(
                    message="Failed to process stream",
                    context=f"workspace-{ws_id}/connection-{connection_id}/{conn_name}/stream-{stream.stream_name}",
                    exc=e,
                )

    def get_report(self) -> SourceReport:
        return self.report
