import logging
from dataclasses import dataclass
from functools import partial
from typing import Dict, Iterable, List, Literal, Optional, Set

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import (
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
    PlatformInfo,
    PropertyFieldPath,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FabricTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sdk.datajob import DataFlow, DataJob
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


@dataclass(frozen=True)
class _PlatformResolutionRequest:
    """Inputs to the source/destination platform-resolution helper.

    Sources and destinations have nearly identical resolution logic; this
    dataclass lets us pass the relevant fields through a single helper
    instead of two near-duplicate methods (W4).
    """

    entity_id: str
    entity_type: Optional[str]
    name: Optional[str]
    definition_id: Optional[str]
    overrides: PlatformDetail
    kind: Literal["source", "destination"]


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
        self._warned_source_ids: Set[str] = set()
        self._warned_destination_ids: Set[str] = set()
        self._source_platform_cache: Dict[str, PlatformInfo] = {}
        self._dest_platform_cache: Dict[str, PlatformInfo] = {}
        self._warned_unknown_statuses: Set[str] = set()

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

    def _resolve_platform(self, request: _PlatformResolutionRequest) -> PlatformInfo:
        """Shared resolution logic for sources and destinations.

        Picks a DataHub platform name using the following precedence:
        1. Explicit override (`sources_to_platform_instance` / `destinations_to_platform_instance`).
        2. Airbyte's reported `sourceType` / `destinationType` mapped via
           `source_type_mapping` + `KNOWN_SOURCE_TYPE_MAPPING`.
        3. Heuristic fallback to the connector's display `name`.

        Cases (2) and (3) emit a per-id `report.warning` once so repeated
        connections sharing a source don't spam the report.
        """
        if request.kind == "source":
            cache = self._source_platform_cache
            warned = self._warned_source_ids
            id_label = "source_id"
            type_label = "source_type"
            name_label = "source_name"
            entity_label = "Source"
        else:
            cache = self._dest_platform_cache
            warned = self._warned_destination_ids
            id_label = "destination_id"
            type_label = "destination_type"
            name_label = "destination_name"
            entity_label = "Destination"

        if request.entity_id in cache:
            return cache[request.entity_id]

        overrides = request.overrides
        if overrides.platform:
            platform = overrides.platform
        elif request.entity_type and request.entity_type.strip():
            platform = _map_source_type_to_platform(
                request.entity_type, self.source_config.source_type_mapping
            )
        elif request.name:
            if request.entity_id not in warned:
                context_parts = [
                    f"{id_label}={request.entity_id}",
                    f"{name_label}={request.name}",
                ]
                if request.definition_id:
                    context_parts.append(f"definition_id={request.definition_id}")
                instance_setting = (
                    "sources_to_platform_instance"
                    if request.kind == "source"
                    else "destinations_to_platform_instance"
                )
                context_parts.append(
                    f"Configure '{instance_setting}' to specify correct platform"
                )
                self.report.warning(
                    title="Platform Detection Fallback",
                    message=(
                        f"{entity_label} {request.entity_id} missing "
                        f"{type_label}, using name '{request.name}' as fallback"
                    ),
                    context=", ".join(context_parts),
                )
                warned.add(request.entity_id)
            platform = _map_source_type_to_platform(
                request.name, self.source_config.source_type_mapping
            )
        else:
            if request.entity_id not in warned:
                self.report.warning(
                    title="Platform Detection Failed",
                    message=(
                        f"{entity_label} {request.entity_id} missing both "
                        f"{type_label} and name"
                    ),
                    context=f"{id_label}={request.entity_id}",
                )
                warned.add(request.entity_id)
            platform = ""

        result = PlatformInfo(
            platform=platform,
            platform_instance=overrides.platform_instance,
            env=overrides.env,
        )
        cache[request.entity_id] = result
        return result

    def _get_platform_for_source(self, source: AirbyteSourcePartial) -> PlatformInfo:
        """Return platform information for source."""
        overrides = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )
        return self._resolve_platform(
            _PlatformResolutionRequest(
                entity_id=source.source_id,
                entity_type=source.source_type,
                name=source.name,
                definition_id=source.source_definition_id,
                overrides=overrides,
                kind="source",
            )
        )

    def _get_platform_for_destination(
        self, destination: AirbyteDestinationPartial
    ) -> PlatformInfo:
        """Return platform information for destination."""
        overrides = self.source_config.destinations_to_platform_instance.get(
            destination.destination_id, PlatformDetail()
        )
        return self._resolve_platform(
            _PlatformResolutionRequest(
                entity_id=destination.destination_id,
                entity_type=destination.destination_type,
                name=destination.name,
                definition_id=destination.destination_definition_id,
                overrides=overrides,
                kind="destination",
            )
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Yield workunits for each Airbyte pipeline.

        Named `get_workunits_internal` (not `get_workunits`) so the base
        class's `get_workunits` runs every emission through
        `get_workunit_processors()` — including `auto_incremental_lineage`,
        `StaleEntityRemovalHandler`, and `AutoSystemMetadata.stamp`.
        Overriding `get_workunits` directly would silently disable all of
        them, breaking `incremental_lineage` and `DELETION_DETECTION`.
        """
        for pipeline_info in self._get_pipelines():
            try:
                yield from self._create_lineage_workunits(pipeline_info)
            except Exception as e:
                conn_id = pipeline_info.connection.connection_id or "unknown"
                self.report.report_failure(
                    message="Failed to process pipeline",
                    context=f"pipeline-{conn_id}",
                    exc=e,
                )

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
                        # Job-detail enrichment is best-effort, but the
                        # connector's reported metrics (bytesCommitted,
                        # streamStatuses) silently disappear when this swallow
                        # fires. Surface as a per-job warning so operators
                        # know enrichment is missing — not as logger.debug.
                        self.report.warning(
                            title="Job Details Unavailable",
                            message=(
                                "Failed to fetch detailed job info; "
                                "bytes/records committed metrics will be missing"
                            ),
                            context=f"job_id={job_id}, connection_id={connection_id}",
                            exc=e,
                        )

                    attempts = job.get("attempts", [])
                    for attempt_idx, attempt in enumerate(attempts):
                        attempt_id = attempt.get("id", attempt_idx)
                        attempt_status = attempt.get("status", "").lower()

                        if attempt_status not in AIRBYTE_JOB_STATUS_MAP:
                            # Defaulting unknown statuses to FAILURE makes
                            # every run look red if Airbyte ever renames the
                            # status field — warn once per unknown status so
                            # the operator can investigate.
                            if attempt_status not in self._warned_unknown_statuses:
                                self.report.warning(
                                    title="Unknown Airbyte Job Status",
                                    message=(
                                        f"Encountered unrecognized Airbyte job "
                                        f"status '{attempt_status}'; mapping to "
                                        "FAILURE. Update AIRBYTE_JOB_STATUS_MAP "
                                        "if this is a legitimate status."
                                    ),
                                    context=f"connection_id={connection_id}, job_id={job_id}",
                                )
                                self._warned_unknown_statuses.add(attempt_status)

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

                        job_url = (
                            f"{self.source_config.external_url_base}"
                            f"/workspaces/{workspace_id}/connections/{connection_id}/status"
                        )

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
            namespace = stream.namespace if stream.namespace else source_schema or ""

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
        """Extract tag URNs for a connection from both workspace tags and connection tags."""
        tags: List[str] = []
        tag_names_seen: Set[str] = set()
        connection_id = pipeline_info.connection.connection_id

        if pipeline_info.connection.tags:
            for tag_dict in pipeline_info.connection.tags:
                tag_name = tag_dict.get("name")
                if tag_name and tag_name not in tag_names_seen:
                    tags.append(make_tag_urn(tag_name))
                    tag_names_seen.add(tag_name)

        for tag_info in airbyte_tags:
            if (
                tag_info.resource_id == connection_id
                and tag_info.name
                and tag_info.name not in tag_names_seen
            ):
                tags.append(make_tag_urn(tag_info.name))
                tag_names_seen.add(tag_info.name)

        return tags

    def _build_fine_grained_lineages(
        self,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        source_urn: str,
        destination_urn: str,
    ) -> List[FineGrainedLineageClass]:
        """Build column-level lineage entries for a stream."""
        if not self.source_config.extract_column_level_lineage:
            return []

        property_fields = stream.get_column_names()
        if not property_fields:
            return []

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

        lineages = []
        for column_name in property_fields:
            # Snowflake lowercases columns when convert_urns_to_lowercase is true.
            # Other platforms keep column case as-is regardless of convert_urns_to_lowercase.
            source_column = (
                column_name.lower()
                if source_platform_info.platform == "snowflake"
                and source_details.convert_urns_to_lowercase
                else column_name
            )
            dest_column = (
                column_name.lower()
                if dest_platform_info.platform == "snowflake"
                and dest_details.convert_urns_to_lowercase
                else column_name
            )
            lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(source_urn, source_column)],
                    downstreams=[make_schema_field_urn(destination_urn, dest_column)],
                )
            )
        return lineages

    def _build_connection_custom_props(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Dict[str, str]:
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        custom_props: Dict[str, str] = {
            "connection_id": connection.connection_id,
            "source_id": source.source_id or "",
            "destination_id": destination.destination_id or "",
            "source_name": source.name or "Unknown Source",
            "destination_name": destination.name or "Unknown Destination",
            "workspace_id": workspace.workspace_id,
            "workspace_name": workspace.name or "Unnamed Workspace",
            "platform": "airbyte",
            "deployment_type": self.source_config.deployment_type.value,
        }

        if connection.created_at:
            custom_props["created_at"] = str(connection.created_at)
        if source.created_at:
            custom_props["source_created_at"] = str(source.created_at)
        if destination.created_at:
            custom_props["destination_created_at"] = str(destination.created_at)

        return custom_props

    def _build_connection_dataflow(
        self,
        pipeline_info: AirbytePipelineInfo,
        tags: List[str],
    ) -> DataFlow:
        """Build a SDK V2 `DataFlow` for the Airbyte connection.

        We previously emitted a workspace-level DataFlow + a connection-level
        DataFlow with no parent-child relationship, which left the workspace
        DataFlow dangling (see W7). Now we emit a single DataFlow per
        connection and surface workspace identity via `customProperties`.
        """
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        source_name = source.name or "Unknown Source"
        destination_name = destination.name or "Unknown Destination"
        external_url = (
            f"{self.source_config.external_url_base}"
            f"/workspaces/{workspace.workspace_id}"
            f"/connections/{connection.connection_id}"
        )

        return DataFlow(
            name=connection.connection_id,
            platform=self.platform,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            display_name=connection.name or "Unnamed Connection",
            description=(
                f"Airbyte connection from {source_name} to {destination_name}"
            ),
            external_url=external_url,
            custom_properties=self._build_connection_custom_props(pipeline_info),
            tags=tags or None,
        )

    def _build_stream_datajob(
        self,
        connection_dataflow: DataFlow,
        pipeline_info: AirbytePipelineInfo,
        stream: AirbyteStreamDetails,
        source_urn: str,
        destination_urn: str,
        tags: List[str],
    ) -> DataJob:
        """Build SDK V2 `DataJob` for a single stream within a connection."""
        connection = pipeline_info.connection
        source = pipeline_info.source
        destination = pipeline_info.destination
        workspace = pipeline_info.workspace

        connection_id = connection.connection_id
        stream_name = stream.stream_name or ""
        source_name = source.name or "Unknown Source"
        destination_name = destination.name or "Unknown Destination"

        job_id = f"{connection_id}_{stream_name}"

        custom_props: Dict[str, str] = {
            "stream_name": stream_name,
            "namespace": stream.namespace,
            "connection_id": connection_id,
            "connection_name": connection.name or "",
            "source_id": source.source_id if source else "",
            "source_name": source_name,
            "destination_id": destination.destination_id if destination else "",
            "destination_name": destination_name,
        }

        if stream.default_cursor_field:
            custom_props["cursor_field"] = ",".join(stream.default_cursor_field)
        if stream.source_defined_primary_key:
            custom_props["primary_key"] = str(stream.source_defined_primary_key)
        if stream.source_defined_cursor_field:
            custom_props["source_defined_cursor"] = "true"

        external_url = (
            f"{self.source_config.external_url_base}"
            f"/workspaces/{workspace.workspace_id}"
            f"/connections/{connection_id}"
        )

        fine_grained_lineages = self._build_fine_grained_lineages(
            pipeline_info, stream, source_urn, destination_urn
        )

        return DataJob(
            name=job_id,
            flow=connection_dataflow,
            display_name=stream_name,
            description=(
                f"Airbyte sync for stream {stream_name} from "
                f"{source_name} to {destination_name}"
            ),
            external_url=external_url,
            custom_properties=custom_props,
            inlets=[source_urn],
            outlets=[destination_urn],
            fine_grained_lineages=fine_grained_lineages or None,
            tags=tags or None,
        )

    def _emit_destination_upstream_lineage(
        self,
        pipeline_info: AirbytePipelineInfo,
        source_urn: str,
        destination_urn: str,
        stream: AirbyteStreamDetails,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit `UpstreamLineage` on the destination dataset.

        Marked with `is_primary_source=False` because Airbyte is enriching a
        dataset it does not own — the destination's primary connector
        (Postgres, Snowflake, etc.) owns the dataset and any other aspects.
        Lineage flows through fine-grained `FineGrainedLineageClass` entries
        so column-level relationships survive even if a downstream connector
        re-emits the dataset.
        """
        fine_grained_lineages = self._build_fine_grained_lineages(
            pipeline_info, stream, source_urn, destination_urn
        )

        if source_urn == destination_urn:
            # Self-lineage is meaningless and would noise up dataset views.
            logger.debug(
                "Skipping UpstreamLineage emission: source URN equals destination URN (%s).",
                destination_urn,
            )
            return

        yield MetadataChangeProposalWrapper(
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

        # For MSSQL and connectors with table-level schema, try table-specific lookup first
        table_level_schema = source.get_schema_for_table(stream.stream_name)
        config_schema = table_level_schema or source.get_schema

        schema_name = self._resolve_schema_name(
            stream.namespace,
            config_schema,
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
        connection: AirbyteConnectionPartial,
        source_schema: str,
        destination: AirbyteDestinationPartial,
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

            elif namespace_def in ("customformat", "custom_format"):
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

        if self.source_config.extract_tags:
            airbyte_tags = self._fetch_tags_for_workspace(workspace_id)
            tags = self._extract_connection_tags(pipeline_info, airbyte_tags)
        else:
            tags = []

        connection_dataflow = self._build_connection_dataflow(pipeline_info, tags)
        yield from connection_dataflow.as_workunits()

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

                datajob = self._build_stream_datajob(
                    connection_dataflow,
                    pipeline_info,
                    stream_info.details,
                    dataset_urns.source_urn,
                    dataset_urns.destination_urn,
                    tags,
                )

                yield from datajob.as_workunits()

                if self.source_config.include_statuses:
                    yield from self._create_job_executions_workunits(
                        pipeline_info=pipeline_info,
                        datajob_urn=datajob.urn,
                        stream_name=stream_info.details.stream_name,
                    )

                yield from self._emit_destination_upstream_lineage(
                    pipeline_info,
                    dataset_urns.source_urn,
                    dataset_urns.destination_urn,
                    stream_info.details,
                )

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
