import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.azure.constants import ADF_LINKED_SERVICE_PLATFORM_MAP
from datahub.ingestion.source.fabric.common.constants import (
    FABRIC_CONNECTION_PLATFORM_MAP,
)
from datahub.ingestion.source.fabric.common.models import FabricConnection
from datahub.ingestion.source.fabric.common.urn_generator import (
    FABRIC_ONELAKE_PLATFORM,
    make_activity_job_urn,
    make_onelake_urn,
    make_pipeline_flow_urn,
)
from datahub.ingestion.source.fabric.data_factory.models import (
    InvokePipelineActivityLineage,
    PipelineActivity,
)
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactorySourceReport,
)
from datahub.metadata.urns import DatasetUrn

logger = logging.getLogger(__name__)


class CopyActivityLineageExtractor:
    """Extracts dataset-level lineage from Fabric Data Factory Copy activities.

    Initialized with a connections cache and environment config, then called
    per-activity to produce (input_urns, output_urns).
    """

    def __init__(
        self,
        connections_cache: Dict[str, FabricConnection],
        report: FabricDataFactorySourceReport,
        env: str,
        platform_instance: Optional[str] = None,
        platform_instance_map: Optional[Dict[str, str]] = None,
    ) -> None:
        self._connections_cache = connections_cache
        self._report = report
        self._env = env
        self._platform_instance = platform_instance
        self._platform_instance_map = platform_instance_map or {}

    def extract_lineage(
        self,
        activity: PipelineActivity,
        workspace_id: str,
    ) -> tuple[List[str], List[str]]:
        """Return (input_urns, output_urns) for a Copy activity."""
        type_props = activity.type_properties
        source = type_props.get("source") or {}
        sink = type_props.get("sink") or type_props.get("destination") or {}

        source_ds = source.get("datasetSettings") or {}
        sink_ds = sink.get("datasetSettings") or {}

        input_urn = self._resolve_dataset_urn(source_ds, activity, workspace_id)
        output_urn = self._resolve_dataset_urn(sink_ds, activity, workspace_id)

        inputs = [input_urn] if input_urn else []
        outputs = [output_urn] if output_urn else []
        return inputs, outputs

    def _resolve_dataset_urn(
        self,
        dataset_settings: Dict[str, Any],
        activity: PipelineActivity,
        workspace_id: str,
    ) -> Optional[str]:
        """Resolve a datasetSettings dict to a dataset URN string.

        For Fabric-native types (Lakehouse, Warehouse), produces a URN
        matching the OneLake connector. For external platforms, produces
        a standard DatasetUrn.
        """
        connection_name, connection_type = self._resolve_connection_and_type(
            dataset_settings
        )
        if not connection_type:
            logger.debug(
                "Could not resolve connection type for activity '%s'. "
                "This may indicate an unsupported connection format.",
                activity.name,
            )
            return None

        platform = self._resolve_platform(connection_type)

        if platform == FABRIC_ONELAKE_PLATFORM:
            return self._resolve_onelake_urn(dataset_settings, activity, workspace_id)

        table_name = self._extract_table_name(
            dataset_settings.get("typeProperties", {}) or {}
        )
        if not table_name:
            logger.debug(
                "Could not extract table name from datasetSettings "
                "of activity '%s' (connection_type=%s).",
                activity.name,
                connection_type,
            )
            return None

        # Resolve platform instance: prefer per-connection mapping, fall back to global
        platform_instance = self._resolve_platform_instance(connection_name)

        return str(
            DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=table_name,
                env=self._env,
                platform_instance=platform_instance,
            )
        )

    def _resolve_connection_and_type(
        self,
        dataset_settings: Dict[str, Any],
    ) -> tuple[Optional[str], Optional[str]]:
        """Resolve the connection name and type from a datasetSettings dict.

        Returns (connection_name, connection_type). The name is extracted from
        whichever source resolves the type — FabricConnection.display_name for
        cached connections, connectionSettings.name, or linkedService.name.

        Resolution order:
        1. externalReferences.connection → cache → (display_name, type)
        2a. connectionSettings.properties.externalReferences.connection
            → cache → (display_name, type)
        2b. connectionSettings (name, properties.type)
        3. linkedService (name, properties.type)
        """
        conn_settings = dataset_settings.get("connectionSettings") or {}
        conn_settings_props = conn_settings.get("properties") or {}
        linked_service = dataset_settings.get("linkedService") or {}
        ls_props = linked_service.get("properties") or {}

        # 1. externalReferences.connection → cache
        ds_conn_id = (dataset_settings.get("externalReferences") or {}).get(
            "connection"
        )
        if ds_conn_id:
            conn = self._connections_cache.get(ds_conn_id)
            if conn:
                return conn.display_name, conn.connection_type
            logger.debug(
                "Connection ID '%s' from externalReferences not found in cache "
                "(cache has %d entries)",
                ds_conn_id,
                len(self._connections_cache),
            )

        # 2a. connectionSettings externalReferences → cache
        cs_conn_id = (conn_settings_props.get("externalReferences") or {}).get(
            "connection"
        )
        if cs_conn_id:
            conn = self._connections_cache.get(cs_conn_id)
            if conn:
                return conn.display_name, conn.connection_type
            logger.debug(
                "Connection ID '%s' from connectionSettings.externalReferences "
                "not found in cache",
                cs_conn_id,
            )

        # 2b. connectionSettings.properties.type (inline)
        cs_type: Optional[str] = conn_settings_props.get("type")
        if cs_type:
            cs_name: Optional[str] = conn_settings.get("name")
            return cs_name, cs_type

        # 3. linkedService (name, type)
        ls_type: Optional[str] = ls_props.get("type")
        if ls_type:
            ls_name: Optional[str] = linked_service.get("name")
            return ls_name, ls_type

        return None, None

    def _resolve_platform_instance(
        self, connection_name: Optional[str]
    ) -> Optional[str]:
        """Resolve the platform instance for a dataset.

        Checks platform_instance_map using the connection name,
        falling back to the global platform_instance.
        """
        if connection_name and self._platform_instance_map:
            mapped = self._platform_instance_map.get(connection_name)
            if mapped:
                return mapped
        return self._platform_instance

    def _resolve_platform(self, connection_type: str) -> str:
        """Map a connection type to a DataHub platform identifier."""
        platform = FABRIC_CONNECTION_PLATFORM_MAP.get(connection_type)
        if platform is None:
            # Fallback: connection type may be an ADF LinkedService type
            # (e.g. AzureBlobStorage, AzureSqlDatabase) rather than a
            # Fabric connection type.
            platform = ADF_LINKED_SERVICE_PLATFORM_MAP.get(connection_type)
        if platform is None:
            logger.warning(
                "Unmapped connection type '%s', defaulting to connection type as platform",
                connection_type,
            )
            self._report.report_unmapped_connection_type(connection_type)
            return connection_type
        return platform

    def _resolve_onelake_urn(
        self,
        dataset_settings: Dict[str, Any],
        activity: PipelineActivity,
        pipeline_workspace_id: str,
    ) -> Optional[str]:
        """Build a URN matching the OneLake connector for Fabric-native items.

        Handles two shapes:
        1. Structured (schema + table) → {workspace}.{item}.{schema}.{table}
        2. File-based (location block) → {workspace}.{item}.Files.{path}
        """
        conn_type_props = (dataset_settings.get("connectionSettings") or {}).get(
            "properties", {}
        ).get("typeProperties") or {}
        ls_type_props = (dataset_settings.get("linkedService") or {}).get(
            "properties", {}
        ).get("typeProperties") or {}
        ds_type_props = dataset_settings.get("typeProperties") or {}

        artifact_id: Optional[str] = (
            conn_type_props.get("artifactId")
            or ls_type_props.get("artifactId")
            or ds_type_props.get("artifactId")
        )
        if not artifact_id:
            logger.debug(
                "No artifactId found in OneLake datasetSettings for activity '%s'",
                activity.name,
            )
            return None

        resolved_workspace_id: str = (
            conn_type_props.get("workspaceId")
            or ls_type_props.get("workspaceId")
            or ds_type_props.get("workspaceId")
            or pipeline_workspace_id
        )

        # 1. Structured: schema + table
        table = ds_type_props.get("table")
        if table:
            return make_onelake_urn(
                workspace_id=resolved_workspace_id,
                item_id=artifact_id,
                table_name=table,
                schema_name=ds_type_props.get("schema"),
                env=self._env,
                platform_instance=self._platform_instance,
            )

        # 2. File-based: location block (Lakehouse "Files" section)
        # TODO: once file based datasets are supported in OneLake connector,
        # remove this log and return a URN of file
        logger.debug(
            "OneLake dataset for activity '%s' has artifactId "
            "but no table — file-based datasets not yet supported",
            activity.name,
        )
        return None

    @staticmethod
    def _extract_table_name(
        ds_type_properties: Dict[str, Any],
    ) -> Optional[str]:
        """Extract a qualified table or file path from datasetSettings."""
        schema = ds_type_properties.get("schema")
        table = ds_type_properties.get("table")
        if table:
            return f"{schema}.{table}" if schema else table

        location = ds_type_properties.get("location") or {}
        return CopyActivityLineageExtractor._extract_file_path(location)

    @staticmethod
    def _extract_file_path(location: Dict[str, Any]) -> Optional[str]:
        """Extract a file/folder path from a location block."""
        parts: list[str] = []
        for key in ("container", "fileSystem", "bucketName"):
            val = location.get(key)
            if val:
                parts.append(val)
                break  # mutually exclusive
        folder = location.get("folderPath")
        if folder:
            parts.append(folder.strip("/"))
        file_name = location.get("fileName")
        if file_name:
            parts.append(file_name)
        return "/".join(parts) if parts else None


class InvokePipelineLineageExtractor:
    """Resolves InvokePipeline activity references to child pipeline URNs.

    Initialized with the pipeline activities cache (populated during the
    first pass of pipeline processing) and environment config. Called
    per-activity to resolve the child pipeline's root activity URN.
    """

    SUPPORTED_OPERATION_TYPE = "InvokeFabricPipeline"

    def __init__(
        self,
        pipeline_activities_cache: dict[tuple[str, str], List[PipelineActivity]],
        report: FabricDataFactorySourceReport,
        platform: str,
        env: str,
        platform_instance: Optional[str] = None,
    ) -> None:
        self._pipeline_activities_cache = pipeline_activities_cache
        self._report = report
        self._platform = platform
        self._env = env
        self._platform_instance = platform_instance

    def extract_lineage(
        self,
        activity: PipelineActivity,
        parent_workspace_id: str,
    ) -> Optional[InvokePipelineActivityLineage]:
        """Resolve an InvokePipeline activity to the child's root activity URN.

        Dispatches to the appropriate handler based on ``operationType``.
        Returns ``None`` if the operation type is unsupported.
        """
        type_props = activity.type_properties or {}
        operation_type = type_props.get("operationType")

        if operation_type == self.SUPPORTED_OPERATION_TYPE:
            return self._resolve_fabric_pipeline(activity, parent_workspace_id)

        self._report.report_warning(
            title="InvokePipeline Lineage Not Resolved",
            message="Unsupported operationType. "
            "Only InvokeFabricPipeline is supported.",
            context=f"activity={activity.name}, operationType={operation_type}",
        )
        return None

    def _resolve_fabric_pipeline(
        self,
        activity: PipelineActivity,
        parent_workspace_id: str,
    ) -> Optional[InvokePipelineActivityLineage]:
        """Resolve an InvokeFabricPipeline operation type to a child pipeline URN."""
        type_props = activity.type_properties or {}
        child_pipeline_id = type_props.get("pipelineId")
        if not child_pipeline_id:
            logger.debug(
                "InvokePipeline activity '%s' has no pipelineId, "
                "skipping cross-pipeline lineage",
                activity.name,
            )
            return None

        child_workspace_id = type_props.get("workspaceId") or parent_workspace_id

        # Build child pipeline DataFlow URN
        child_flow_urn = make_pipeline_flow_urn(
            workspace_id=child_workspace_id,
            pipeline_id=child_pipeline_id,
            platform=self._platform,
            env=self._env,
            platform_instance=self._platform_instance,
        )

        # Resolve child's root activity from cache
        child_activities = self._pipeline_activities_cache.get(
            (child_workspace_id, child_pipeline_id)
        )
        root_activity_name: Optional[str] = None
        child_datajob_urn: Optional[str] = None

        if child_activities:
            root_activity_name = self._find_root_activity(child_activities)
            if root_activity_name:
                child_datajob_urn = str(
                    make_activity_job_urn(root_activity_name, child_flow_urn)
                )
        else:
            logger.debug(
                "InvokePipeline '%s' references pipeline %s in workspace %s "
                "which is not in the activities cache — skipping edge",
                activity.name,
                child_pipeline_id,
                child_workspace_id,
            )

        # Build custom properties for the InvokePipeline DataJob
        props: dict[str, str] = {
            "calls_pipeline_id": child_pipeline_id,
            "calls_workspace_id": child_workspace_id,
            "child_pipeline_urn": str(child_flow_urn),
            "operation_type": self.SUPPORTED_OPERATION_TYPE,
        }
        if root_activity_name:
            props["child_root_activity"] = root_activity_name

        return InvokePipelineActivityLineage(
            child_datajob_urn=child_datajob_urn,
            custom_properties=props,
        )

    @staticmethod
    def _find_root_activity(
        activities: List[PipelineActivity],
    ) -> Optional[str]:
        """Find the first root activity — one with no upstream dependencies.

        Root activities have an empty ``depends_on`` list, meaning they
        run first when the pipeline is triggered. If multiple roots exist
        (they run in parallel), the first one encountered is returned.

        Falls back to the first activity in the list if all activities
        have dependencies (shouldn't happen in valid pipelines but
        defensive).
        """
        for activity in activities:
            if not activity.depends_on:
                return activity.name
        # Fallback: all activities have dependencies (circular or malformed)
        return activities[0].name if activities else None
