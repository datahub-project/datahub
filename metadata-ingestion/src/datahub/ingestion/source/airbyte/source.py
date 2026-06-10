import logging
from typing import Dict, Iterable, List, Optional, Set

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
from datahub.ingestion.api.source import (
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
    PlatformKind,
    PlatformResolutionRequest,
    PropertyFieldPath,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
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
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
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
    return platform_name.lower().replace(" ", "-")


def _map_source_type_to_platform(
    source_type: str, source_type_mapping: Dict[str, str]
) -> str:
    if source_type in source_type_mapping:
        return source_type_mapping[source_type]

    source_type_lower = source_type.lower()
    if source_type_lower in KNOWN_SOURCE_TYPE_MAPPING:
        return KNOWN_SOURCE_TYPE_MAPPING[source_type_lower]

    return _sanitize_platform_name(source_type)


def _validate_urn_component(component: str, component_name: str) -> str:
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
@support_status(SupportStatus.INCUBATING)
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
        # Tags are workspace-scoped; cache per workspace so a workspace with N
        # connections doesn't make N identical `/tags?workspaceIds=...` calls.
        # Failures are cached as `[]` as well, so a transient error on the
        # first connection doesn't repeat the failing call for the rest.
        self._workspace_tags_cache: Dict[str, List[AirbyteTagInfo]] = {}
        self._warned_unknown_statuses: Set[str] = set()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AirbyteSource":
        config = AirbyteSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _resolve_platform(self, request: PlatformResolutionRequest) -> PlatformInfo:
        # Picks a DataHub platform using:
        #   1. explicit override via `*_to_platform_instance` config
        #   2. Airbyte's reported source/destination type mapped through
        #      `source_type_mapping` + `KNOWN_SOURCE_TYPE_MAPPING`
        #   3. heuristic fallback to the connector's display `name`
        # Cases 2 and 3 emit one warning per entity_id to avoid spam.
        if request.kind is PlatformKind.SOURCE:
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
                    if request.kind is PlatformKind.SOURCE
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
        overrides = self.source_config.sources_to_platform_instance.get(
            source.source_id, PlatformDetail()
        )
        return self._resolve_platform(
            PlatformResolutionRequest(
                entity_id=source.source_id,
                entity_type=source.source_type,
                name=source.name,
                definition_id=source.source_definition_id,
                overrides=overrides,
                kind=PlatformKind.SOURCE,
            )
        )

    def _get_platform_for_destination(
        self, destination: AirbyteDestinationPartial
    ) -> PlatformInfo:
        overrides = self.source_config.destinations_to_platform_instance.get(
            destination.destination_id, PlatformDetail()
        )
        return self._resolve_platform(
            PlatformResolutionRequest(
                entity_id=destination.destination_id,
                entity_type=destination.destination_type,
                name=destination.name,
                definition_id=destination.destination_definition_id,
                overrides=overrides,
                kind=PlatformKind.DESTINATION,
            )
        )

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
                            continue

                        if (
                            destination.name
                            and not self.source_config.destination_pattern.allowed(
                                destination.name
                            )
                        ):
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
        # Override `get_workunits_internal`, not `get_workunits`, so the base
        # class threads emissions through `get_workunit_processors()` —
        # otherwise `auto_incremental_lineage` and `StaleEntityRemovalHandler`
        # silently no-op.
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

            for job in jobs:
                try:
                    job_id = job.get("id")
                    if not job_id:
                        continue

                    job_details = None
                    try:
                        job_details = self.client.get_job(str(job_id))
                    except Exception as e:
                        # Enrichment is best-effort, but losing it silently
                        # would drop bytes/records-committed metrics from
                        # every run on this connection.
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
        # We read streams from the connection's sync_catalog instead of the
        # `/streams` endpoint — the latter is missing on older Airbyte (e.g.
        # 0.30.1) and sync_catalog works across all versions.
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

        for stream_config in pipeline_info.connection.sync_catalog.streams:
            if not stream_config or not stream_config.stream:
                continue
            if not stream_config.is_enabled():
                continue

            stream = stream_config.stream
            namespace = stream.namespace if stream.namespace else source_schema or ""

            properties = {}
            if stream.json_schema and "properties" in stream.json_schema:
                properties = stream.json_schema.get("properties", {})

            property_fields = [
                PropertyFieldPath(path=[field_name])
                for field_name in properties
                if stream_config.is_field_selected(field_name)
            ]

            stream_details = AirbyteStreamDetails(
                stream_name=stream.name,
                namespace=namespace,
                property_fields=property_fields,
            )
            streams.append(
                AirbyteStreamInfo(config=stream_config, details=stream_details)
            )

        return streams

    def _fetch_tags_for_workspace(self, workspace_id: str) -> List[AirbyteTagInfo]:
        if workspace_id in self._workspace_tags_cache:
            return self._workspace_tags_cache[workspace_id]
        try:
            tags_list = self.client.list_tags(workspace_id)
            tags = [AirbyteTagInfo.model_validate(tag) for tag in tags_list]
        except Exception as e:
            self.report.warning(
                title="Tag Retrieval Failed",
                message="Failed to retrieve tags from Airbyte",
                context=f"workspace_id={workspace_id}",
                exc=e,
            )
            tags = []
        self._workspace_tags_cache[workspace_id] = tags
        return tags

    def _extract_connection_tags(
        self, pipeline_info: AirbytePipelineInfo, airbyte_tags: List[AirbyteTagInfo]
    ) -> List[str]:
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
            # Snowflake folds identifiers to lowercase when
            # `convert_urns_to_lowercase` is set; other platforms preserve case.
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
        # One DataFlow per connection; workspace identity is folded into
        # custom properties rather than emitted as a separate (dangling)
        # workspace-level DataFlow.
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
        # `is_primary_source=False` because the destination dataset is owned
        # by its own platform connector (Postgres, Snowflake, etc.); we only
        # add the upstream + fine-grained edges, never overwrite other aspects.
        if source_urn == destination_urn:
            return

        fine_grained_lineages = self._build_fine_grained_lineages(
            pipeline_info, stream, source_urn, destination_urn
        )

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

        connection = pipeline_info.connection
        table_prefix = connection.get_prefix

        # MSSQL etc. carry per-table schemas in the source config; fall back
        # to the source-wide schema if no per-table override exists.
        config_schema = (
            source.get_schema_for_table(stream.stream_name) or source.get_schema
        )

        schema_name = self._resolve_schema_name(
            stream.namespace,
            config_schema,
            stream.stream_name,
            source.source_id,
        )
        table_name = stream.stream_name

        if table_prefix:
            table_name = f"{table_prefix}{table_name}"

        # Some connectors (Stripe, Hubspot) emit `<schema>.<table>` as the
        # stream name; we only want the leaf for URN composition.
        if "." in table_name:
            table_name = table_name.split(".")[-1]

        source_database = source.get_database

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
        # Per-stream namespace (when set by Airbyte) is more specific than
        # the connector-wide schema; fall back to the latter otherwise.
        if stream_namespace:
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
        # Airbyte's destination namespace precedence:
        #   1. per-stream `destinationNamespace`
        #   2. connection-level `namespace_definition` (source | destination |
        #      customformat with `${SOURCE_NAMESPACE}` interpolation)
        #   3. destination's configured default schema
        #   4. source schema as a last resort
        stream_dest_namespace = stream_config.get_destination_namespace()
        if stream_dest_namespace:
            return stream_dest_namespace

        namespace_def = connection.get_namespace_definition
        if namespace_def == "source":
            return source_schema
        if namespace_def == "destination":
            dest_config_schema = destination.get_schema
            if dest_config_schema:
                return dest_config_schema
        elif namespace_def in ("customformat", "custom_format"):
            namespace_fmt = connection.get_namespace_format
            if namespace_fmt:
                return namespace_fmt.replace("${SOURCE_NAMESPACE}", source_schema)

        dest_config_schema = destination.get_schema
        if dest_config_schema:
            return dest_config_schema

        return source_schema

    def _build_dataset_name(
        self,
        database: Optional[str],
        schema: str,
        table: str,
        include_schema_override: Optional[bool],
        entity_type: str,
    ) -> str:
        # Auto-detect 2-tier vs 3-tier: when `database == schema` we collapse
        # to `<database>.<table>` (typical for MySQL-style platforms where
        # Airbyte reports the database as both tiers).
        if not database:
            return f"{schema}.{table}" if schema else table

        if include_schema_override is None:
            include_schema = not (schema and schema == database)
        else:
            include_schema = include_schema_override

        if schema and include_schema:
            return f"{database}.{schema}.{table}"
        return f"{database}.{table}"

    def _create_lineage_workunits(
        self, pipeline_info: AirbytePipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        connection_id = pipeline_info.connection.connection_id
        workspace_id = pipeline_info.workspace.workspace_id
        source_name = pipeline_info.source.name or ""
        destination_name = pipeline_info.destination.name or ""

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

        streams = self._fetch_streams_for_source(pipeline_info)

        for stream_info in streams:
            try:
                dataset_urns = self._create_dataset_urns(
                    pipeline_info,
                    stream_info.config,
                    stream_info.details,
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
