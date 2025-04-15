import logging
from typing import Iterable, List, Optional, Tuple

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
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.client import (
    AirbyteBaseClient,
    create_airbyte_client,
)
from datahub.ingestion.source.airbyte.config import AirbyteSourceConfig
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDatasetMapping,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteStreamDetails,
    AirbyteTagInfo,
    AirbyteWorkspacePartial,
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

        # Log configuration information
        logger.info(
            f"Initialized Airbyte source with deployment type: {config.deployment_type}"
        )
        if config.platform_instance:
            logger.info(f"Using platform instance: {config.platform_instance}")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AirbyteSource":
        config = AirbyteSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def _get_pipelines(self) -> Iterable[AirbytePipelineInfo]:
        # Use the workspace pattern to filter workspaces
        for workspace_dict in self.client.list_workspaces(
            pattern=self.source_config.workspace_pattern
        ):
            try:
                # Use the partial model for initial parsing, as the API response may have missing fields
                workspace = AirbyteWorkspacePartial.parse_obj(workspace_dict)

                # Add null check for workspace_id
                if not workspace.workspace_id:
                    logger.warning(
                        f"Skipping workspace with missing ID: {workspace_dict}"
                    )
                    continue

                logger.info(f"Processing workspace {workspace.workspace_id}")

                # Use the connection pattern to filter connections
                for connection_dict in self.client.list_connections(
                    workspace.workspace_id,
                    pattern=self.source_config.connection_pattern,
                ):
                    try:
                        # Use the partial model for initial parsing
                        connection = AirbyteConnectionPartial.parse_obj(connection_dict)

                        # Add null check for connection_id
                        if not connection.connection_id:
                            logger.warning(
                                f"Skipping connection with missing ID: {connection_dict}"
                            )
                            continue

                        logger.info(f"Processing connection {connection.connection_id}")

                        if not connection.source_id or not connection.destination_id:
                            logger.warning(
                                f"Connection {connection.connection_id} is missing source or destination ID"
                            )
                            continue

                        source_dict = self.client.get_source(connection.source_id)
                        destination_dict = self.client.get_destination(
                            connection.destination_id
                        )

                        # Use the partial models since we're not sure if the API response is complete
                        source = AirbyteSourcePartial.parse_obj(source_dict)
                        destination = AirbyteDestinationPartial.parse_obj(
                            destination_dict
                        )

                        # Apply source pattern - add null check for name
                        if (
                            source.name
                            and not self.source_config.source_pattern.allowed(
                                source.name
                            )
                        ):
                            logger.info(
                                f"Skipping source {source.name} due to source pattern filter"
                            )
                            continue

                        # Apply destination pattern - add null check for name
                        if (
                            destination.name
                            and not self.source_config.destination_pattern.allowed(
                                destination.name
                            )
                        ):
                            logger.info(
                                f"Skipping destination {destination.name} due to destination pattern filter"
                            )
                            continue

                        yield AirbytePipelineInfo(
                            workspace=workspace,
                            connection=connection,
                            source=source,
                            destination=destination,
                        )
                    except Exception as e:
                        conn_id = connection_dict.get("connectionId", "unknown")
                        self.report.report_failure(
                            message="Failed to process connection",
                            context=f"connection-{conn_id}",
                            exc=e,
                        )
            except Exception as e:
                workspace_id = workspace_dict.get("workspaceId", "unknown")
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
        workspace_name = (
            workspace.name or "Unnamed Workspace"
        )  # Provide default if None

        # Skip if workspace_id is None
        if not workspace_id:
            logger.warning("Skipping dataflow creation for workspace with None ID")
            return

        dataflow_urn = make_data_flow_urn(
            orchestrator=self.platform,
            flow_id=workspace_id,
            cluster=self.source_config.env,
            platform_instance=self.source_config.platform_instance,
        )

        dataflow_info = DataFlowInfoClass(
            name=workspace_id,  # Use workspace_id which we know is not None
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
        workspace = pipeline_info.workspace
        connection = pipeline_info.connection
        workspace_id = workspace.workspace_id
        connection_id = connection.connection_id
        connection_name = connection.name or "Unnamed Connection"  # Provide default

        # Skip if required IDs are None
        if not workspace_id or not connection_id:
            logger.warning(
                "Skipping datajob creation - missing workspace_id or connection_id"
            )
            return

        # Safe access to source and destination
        source = pipeline_info.source
        destination = pipeline_info.destination

        source_service_name = source.name if source and source.name else ""
        destination_service_name = (
            destination.name if destination and destination.name else ""
        )

        # Prepare inputs and outputs from source and destination
        inputs: List[str] = []
        outputs: List[str] = []

        if source and destination and connection.sync_catalog:
            inputs, outputs = self._get_input_output_datasets(
                pipeline_info,
                source_service_name,
                destination_service_name,
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
        datajob.inlets = [DatasetUrn.from_string(input_urn) for input_urn in inputs]
        datajob.outlets = [DatasetUrn.from_string(output_urn) for output_urn in outputs]

        # Safely get source and destination IDs
        source_id = source.source_id if source and source.source_id else ""
        dest_id = (
            destination.destination_id
            if destination and destination.destination_id
            else ""
        )

        # Add custom properties
        datajob.properties = {
            "connection_id": connection_id,
            "source_id": source_id,
            "destination_id": dest_id,
            "source_name": source_service_name,
            "destination_name": destination_service_name,
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
        """Map dataset URN components using configuration."""
        safe_platform = platform_name or ""  # Ensure non-None value
        safe_schema = schema_name or ""  # Ensure non-None value

        mapping_config = self.source_config.platform_mapping.get_dataset_mapping(
            safe_platform
        )
        mapped_platform = mapping_config.platform or safe_platform.lower()
        mapped_env = mapping_config.env or "PROD"

        if (
            mapping_config.schema_mapping
            and safe_schema in mapping_config.schema_mapping
        ):
            mapped_schema = mapping_config.schema_mapping[safe_schema]
        else:
            # Apply prefix if specified
            prefix = mapping_config.schema_prefix or ""
            mapped_schema = f"{prefix}{safe_schema}"

        # Determine the full dataset name
        if "." in table_name:
            # If table_name already contains a schema, use it as is
            mapped_dataset_name = table_name
        else:
            # Map the database name if needed
            database_component = ""
            if (
                mapping_config.database_mapping
                and safe_schema in mapping_config.database_mapping
            ):
                database_component = f"{mapping_config.database_mapping[safe_schema]}."
            elif mapping_config.database_prefix:
                database_component = f"{mapping_config.database_prefix}."

            # Construct the full dataset name
            mapped_dataset_name = f"{database_component}{mapped_schema}.{table_name}"

        # Return as a model
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
    ) -> Tuple[List[str], List[str]]:
        inputs: List[str] = []
        outputs: List[str] = []

        if (
            not pipeline_info.connection.sync_catalog
            or not pipeline_info.connection.sync_catalog.streams
        ):
            return inputs, outputs

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

                # Override platform instance if specified
                platform_instance = source_mapping.platform_instance
                if not platform_instance and self.source_config.platform_instance:
                    platform_instance = self.source_config.platform_instance

                source_urn = make_dataset_urn_with_platform_instance(
                    platform=source_mapping.platform,
                    name=source_mapping.name,
                    env=source_mapping.env
                    if source_mapping.env
                    else FabricTypeClass.PROD,
                    platform_instance=platform_instance,
                )
                inputs.append(source_urn)

            if destination_platform and table_name:
                # Map destination dataset components to URN
                dest_mapping = self._map_dataset_urn_components(
                    destination_platform, schema_name, table_name
                )

                # Override platform instance if specified
                platform_instance = dest_mapping.platform_instance
                if not platform_instance and self.source_config.platform_instance:
                    platform_instance = self.source_config.platform_instance

                destination_urn = make_dataset_urn_with_platform_instance(
                    platform=dest_mapping.platform,
                    name=dest_mapping.name,
                    env=dest_mapping.env if dest_mapping.env else FabricTypeClass.PROD,
                    platform_instance=platform_instance,
                )
                outputs.append(destination_urn)

        return inputs, outputs

    def _create_job_executions_workunits(
        self,
        pipeline_info: AirbytePipelineInfo,
        datajob_urn: DataJobUrn,
        stream_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create job execution work units for a DataJob representing a specific stream

        Args:
            pipeline_info: Airbyte pipeline information
            datajob_urn: URN of the DataJob
            stream_name: Name of the stream

        Returns:
            Iterable of work units for job executions
        """
        # Ensure both IDs are not None
        connection_id = pipeline_info.connection.connection_id
        workspace_id = pipeline_info.workspace.workspace_id

        if not connection_id or not workspace_id:
            logger.warning(
                "Skipping job executions - missing connection_id or workspace_id"
            )
            return

        # Use the configured start and end dates for job filtering
        try:
            jobs = self.client.list_jobs(
                connection_id=connection_id,
                workspace_id=workspace_id,
                start_date=self.source_config.job_status_start_date,
                end_date=self.source_config.job_status_end_date,
                limit=self.source_config.job_statuses_limit,
            )

            logger.info(
                f"Found {len(jobs)} jobs for connection {connection_id} and stream {stream_name}"
            )

            # Define a mapping from Airbyte status to DataHub InstanceRunResult
            status_result_map = {
                "succeeded": InstanceRunResult.SUCCESS,
                "completed": InstanceRunResult.SUCCESS,
                "success": InstanceRunResult.SUCCESS,
                "failed": InstanceRunResult.FAILURE,
                "failure": InstanceRunResult.FAILURE,
                "error": InstanceRunResult.FAILURE,
                "cancelled": InstanceRunResult.SKIPPED,
                "canceled": InstanceRunResult.SKIPPED,
                "running": InstanceRunResult.SUCCESS,
                "incomplete": InstanceRunResult.SUCCESS,
                "pending": InstanceRunResult.SUCCESS,
            }

            # Process each job
            for job in jobs:
                try:
                    job_id = job.get("id")
                    if not job_id:
                        continue

                    # Process job attempts
                    attempts = job.get("attempts", [])
                    for attempt_idx, attempt in enumerate(attempts):
                        attempt_id = attempt.get("id", attempt_idx)
                        attempt_status = attempt.get("status", "").lower()

                        # Map status to result
                        result = status_result_map.get(
                            attempt_status, InstanceRunResult.FAILURE
                        )

                        # Get timestamps
                        start_time_millis = attempt.get("createdAt")
                        end_time_millis = attempt.get("endedAt")

                        if not start_time_millis:
                            continue

                        # Create unique ID for this execution
                        execution_id = f"{job_id}-{attempt_id}-{stream_name}"

                        # Create a DataProcessInstance for this execution
                        dpi = DataProcessInstance(
                            orchestrator=self.platform,
                            id=execution_id,
                            template_urn=datajob_urn,
                            properties={
                                "job_id": str(job_id),  # Ensure string
                                "attempt_id": str(attempt_id),
                                "connection_id": connection_id,
                                "stream_name": stream_name,
                                "status": attempt_status,
                            },
                            url=f"{getattr(self.source_config, 'host_port', 'https://cloud.airbyte.com')}/workspaces/{workspace_id}/connections/{connection_id}",
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
                    logger.warning(
                        f"Failed to process job {job.get('id', 'unknown')}: {str(e)}"
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
        """Fetch stream details from Airbyte API or connection object."""
        # Ensure source is not None
        if not pipeline_info.source:
            logger.warning("Cannot fetch streams - source is None")
            return []

        source_id = pipeline_info.source.source_id
        source_schema = pipeline_info.source.get_schema

        # Skip if source_id is None
        if not source_id:
            logger.warning("Cannot fetch streams - source_id is None")
            return []

        try:
            # Try to get streams directly from the Airbyte API
            streams_list = self.client.list_streams(source_id=source_id)
            logger.info(f"Found {len(streams_list)} streams for source {source_id}")

            if streams_list:
                return [
                    AirbyteStreamDetails.parse_obj(stream) for stream in streams_list
                ]
        except Exception as e:
            logger.error(f"Error fetching streams from API: {str(e)}")

        # Fall back to extracting streams from connection sync catalog
        streams = []
        if (
            pipeline_info.connection.sync_catalog
            and pipeline_info.connection.sync_catalog.streams
        ):
            for stream_config in pipeline_info.connection.sync_catalog.streams:
                if not stream_config or not stream_config.stream:
                    continue
                # Skip disabled streams
                if not stream_config.is_enabled():
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
                streams.append(AirbyteStreamDetails.parse_obj(stream_details))

        logger.info(f"Using {len(streams)} streams from connection sync catalog")
        return streams

    def _fetch_tags_for_workspace(self, workspace_id: str) -> List[AirbyteTagInfo]:
        """Fetch tags from Airbyte API for a workspace."""
        if not workspace_id:
            logger.warning("Cannot fetch tags - workspace_id is None")
            return []

        try:
            tags_list = self.client.list_tags(workspace_id)
            logger.info(f"Retrieved {len(tags_list)} tags from Airbyte")
            return [
                AirbyteTagInfo(
                    name=tag.get("name", ""),
                    id=tag.get("id"),
                    resourceId=tag.get("resourceId"),
                    resourceType=tag.get("resourceType"),
                )
                for tag in tags_list
            ]
        except Exception as e:
            logger.warning(f"Failed to retrieve tags from Airbyte: {str(e)}")
            return []

    def _extract_connection_tags(
        self, pipeline_info: AirbytePipelineInfo, airbyte_tags: List[AirbyteTagInfo]
    ) -> List[str]:
        """
        Extract tags for a connection

        Args:
            pipeline_info: Pipeline information
            airbyte_tags: Tags from Airbyte API

        Returns:
            List of tag URNs
        """
        tags: List[str] = []

        # Extract connection ID, with null check
        connection_id = pipeline_info.connection.connection_id
        if not connection_id:
            return tags

        # Find tags associated with this connection
        for tag_info in airbyte_tags:
            if tag_info.resource_id == connection_id:
                if tag_info.name:
                    tags.append(make_tag_urn(tag_info.name))

        return tags

    def _create_connection_dataflow(
        self, pipeline_info: AirbytePipelineInfo, tags: List[str]
    ) -> Tuple[DataFlowUrn, Iterable[MetadataWorkUnit]]:
        """Create connection DataFlow entity and return its URN and work units."""
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        # Handle potential None values
        connection_id = connection.connection_id
        source_id = source.source_id if source else ""
        destination_id = destination.destination_id if destination else ""
        source_name = source.name if source and source.name else "Unknown Source"
        destination_name = (
            destination.name
            if destination and destination.name
            else "Unknown Destination"
        )
        workspace_id = workspace.workspace_id or ""  # Default to empty string
        workspace_name = workspace.name or "Unnamed Workspace"
        connection_name = connection.name or "Unnamed Connection"

        # Ensure connection_id is not None
        if not connection_id:
            # Create a temporary URN and empty work units
            temp_urn = DataFlowUrn(
                orchestrator=self.platform,
                flow_id="unknown_connection",
                cluster=self.source_config.env,
            )
            return temp_urn, []

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

        return connection_dataflow_urn, work_units

    def _create_stream_datajob(
        self,
        connection_dataflow_urn: DataFlowUrn,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        source_urn: str,
        destination_urn: str,
        tags: List[str],
    ) -> Tuple[DataJobUrn, Iterable[MetadataWorkUnit]]:
        """Create DataJob for a stream and return its URN and work units."""
        work_units: List[MetadataWorkUnit] = []
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        # Handle potential None values
        connection_id = connection.connection_id or ""
        stream_name = stream.stream_name or ""
        source_name = source.name if source and source.name else "Unknown Source"
        destination_name = (
            destination.name
            if destination and destination.name
            else "Unknown Destination"
        )
        workspace_id = workspace.workspace_id or ""

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

        # Create InputOutput aspect to connect datasets with the datajob
        input_output = DataJobInputOutputClass(
            inputDatasets=[source_urn], outputDatasets=[destination_urn]
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

        return datajob_urn, work_units

    def _create_dataset_lineage(
        self,
        source_urn: str,
        destination_urn: str,
        stream: AirbyteStreamDetails,
        tags: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Create lineage between source and destination datasets."""
        work_units = []

        # Extract column names
        property_fields = stream.get_column_names()

        # Create fine-grained lineage for each column
        fine_grained_lineages = []
        if self.source_config.extract_column_level_lineage and property_fields:
            logger.info(
                f"Processing column-level lineage for {len(property_fields)} columns"
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

        # Create lineage metadata
        work_units.append(
            MetadataChangeProposalWrapper(
                entityUrn=destination_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=source_urn, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                    ],
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                ),
            ).as_workunit()
        )

        # Add tags to datasets
        if tags:
            logger.info(f"Adding {len(tags)} tags to source and destination datasets")
            for dataset_urn in [source_urn, destination_urn]:
                global_tags = GlobalTagsClass(
                    tags=[TagAssociationClass(tag=tag) for tag in tags]
                )
                work_units.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=global_tags
                    ).as_workunit()
                )

        return work_units

    def _create_dataset_urns(
        self,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        platform_instance: Optional[str],
    ) -> Tuple[str, str]:
        """Create source and destination URNs for a stream."""
        source = pipeline_info.source
        destination = pipeline_info.destination

        # Default values for potentially None fields
        source_name = source.name if source and source.name else ""
        destination_name = destination.name if destination and destination.name else ""

        schema_name = stream.namespace or ""
        table_name = stream.stream_name

        # Get source and destination schema/database info
        source_schema = source.get_schema if source else None
        source_database = source.get_database if source else None
        destination_schema = destination.get_schema if destination else None
        destination_database = destination.get_database if destination else None

        if not schema_name and source_schema:
            schema_name = source_schema

        # SOURCE DATASET URN CREATION
        # Map the source dataset components
        source_mapping = self._map_dataset_urn_components(
            source_name, schema_name, table_name
        )

        # Get platform instance if specified in the mapping
        source_platform_instance = source_mapping.platform_instance or platform_instance

        # Create fully qualified dataset name including database
        source_mapped_name = source_mapping.name
        if source_database:
            # Check if database is already part of the mapped name
            if "." in source_mapped_name:
                # If it already contains dots (schema.table format), we need to prepend the database
                if not source_mapped_name.startswith(f"{source_database}."):
                    source_mapped_name = f"{source_database}.{source_mapped_name}"
            else:
                # If it's just a table name, add both database and schema
                source_mapped_name = f"{source_database}.{schema_name}.{table_name}"
        else:
            # No database, ensure we at least have schema.table format
            if "." not in source_mapped_name:
                source_mapped_name = f"{schema_name}.{source_mapped_name}"

        source_urn = make_dataset_urn_with_platform_instance(
            platform=source_mapping.platform,
            name=source_mapped_name,
            env=source_mapping.env if source_mapping.env else FabricTypeClass.PROD,
            platform_instance=source_platform_instance,
        )

        # DESTINATION DATASET URN CREATION
        # For destination, use the destination schema if specified
        dest_namespace = destination_schema if destination_schema else schema_name

        # Map the destination dataset components
        dest_mapping = self._map_dataset_urn_components(
            destination_name, dest_namespace, table_name
        )

        # Get platform instance if specified in the mapping
        dest_platform_instance = dest_mapping.platform_instance or platform_instance

        # Create fully qualified dataset name including database
        dest_mapped_name = dest_mapping.name
        if destination_database:
            # Check if database is already part of the mapped name
            if "." in dest_mapped_name:
                # If it already contains dots (schema.table format), we need to prepend the database
                if not dest_mapped_name.startswith(f"{destination_database}."):
                    dest_mapped_name = f"{destination_database}.{dest_mapped_name}"
            else:
                # If it's just a table name, add both database and schema
                dest_mapped_name = (
                    f"{destination_database}.{dest_namespace}.{table_name}"
                )
        else:
            # No database, ensure we at least have schema.table format
            if "." not in dest_mapped_name:
                dest_mapped_name = f"{dest_namespace}.{dest_mapped_name}"

        destination_urn = make_dataset_urn_with_platform_instance(
            platform=dest_mapping.platform,
            name=dest_mapped_name,
            env=dest_mapping.env if dest_mapping.env else FabricTypeClass.PROD,
            platform_instance=dest_platform_instance,
        )

        logger.info(f"Created lineage from {source_urn} to {destination_urn}")

        return source_urn, destination_urn

    def _create_lineage_workunits(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create lineage work units between source and destination datasets based on stream information.

        Args:
            pipeline_info: Information about the Airbyte pipeline

        Returns:
            Iterable of work units for the lineage
        """
        # Get the connection_id safely
        connection_id = pipeline_info.connection.connection_id or "unknown"
        logger.info(f"Creating lineage for connection: {connection_id}")

        # Validate source and destination
        if not pipeline_info.source or not pipeline_info.destination:
            logger.warning(
                f"Missing source or destination for connection {connection_id}"
            )
            return

        workspace_id = pipeline_info.workspace.workspace_id or ""
        source_name = pipeline_info.source.name or ""
        destination_name = pipeline_info.destination.name or ""

        if not source_name or not destination_name:
            logger.warning(
                f"Missing source name or destination name for connection {connection_id}"
            )
            return

        # Get tags from Airbyte API
        airbyte_tags = self._fetch_tags_for_workspace(workspace_id)

        # Extract connection-specific tags
        tags = self._extract_connection_tags(pipeline_info, airbyte_tags)

        # Create the connection DataFlow entity
        connection_dataflow_urn, dataflow_workunits = self._create_connection_dataflow(
            pipeline_info, tags
        )

        # Emit dataflow work units
        yield from dataflow_workunits

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
                source_urn, destination_urn = self._create_dataset_urns(
                    pipeline_info, stream, platform_instance
                )

                # Create DataJob for this stream
                datajob_urn, datajob_workunits = self._create_stream_datajob(
                    connection_dataflow_urn,
                    pipeline_info,
                    stream,
                    source_urn,
                    destination_urn,
                    tags,
                )

                # Emit datajob work units
                yield from datajob_workunits

                # Create job executions for this stream if enabled
                if self.source_config.include_statuses:
                    yield from self._create_job_executions_workunits(
                        pipeline_info=pipeline_info,
                        datajob_urn=datajob_urn,
                        stream_name=stream.stream_name,
                    )

                # Create dataset lineage
                lineage_workunits = self._create_dataset_lineage(
                    source_urn, destination_urn, stream, tags
                )

                # Emit lineage work units
                yield from lineage_workunits

            except Exception as e:
                self.report.report_failure(
                    message="Failed to process stream",
                    context=f"stream-{connection_id}-{stream.stream_name}",
                    exc=e,
                )

    def get_report(self) -> SourceReport:
        return self.report
