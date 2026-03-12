import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.fabric.common.constants import (
    FABRIC_CONNECTION_PLATFORM_MAP,
)
from datahub.ingestion.source.fabric.common.models import FabricConnection
from datahub.ingestion.source.fabric.common.urn_generator import (
    FABRIC_ONELAKE_PLATFORM,
    make_onelake_urn,
)
from datahub.ingestion.source.fabric.data_factory.models import PipelineActivity
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
        env: str,
        platform_instance: Optional[str] = None,
    ) -> None:
        self._connections_cache = connections_cache
        self._env = env
        self._platform_instance = platform_instance

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
        connection_type = self._resolve_connection_type(dataset_settings)
        if not connection_type:
            logger.debug(
                f"Could not resolve connection type for activity '{activity.name}'"
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
                f"Could not extract table name from "
                f"datasetSettings of activity '{activity.name}'"
            )
            return None

        return str(
            DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=table_name,
                env=self._env,
                platform_instance=self._platform_instance,
            )
        )

    def _resolve_connection_type(
        self,
        dataset_settings: Dict[str, Any],
    ) -> Optional[str]:
        """Resolve the connection type from a datasetSettings dict.

        Resolution order:
        1. externalReferences.connection → cache → type
        2. connectionSettings.properties.externalReferences.connection
           → cache → type, then connectionSettings.properties.type
        3. linkedService.properties.type
        4. activity-level externalReferences → cache → type
        """
        conn_settings_props = (dataset_settings.get("connectionSettings") or {}).get(
            "properties"
        ) or {}
        ls_props = (dataset_settings.get("linkedService") or {}).get("properties") or {}

        # 1. externalReferences.connection → cache
        ds_conn_id = (dataset_settings.get("externalReferences") or {}).get(
            "connection"
        )
        if ds_conn_id:
            conn = self._connections_cache.get(ds_conn_id)
            if conn:
                return conn.connection_type

        # 2a. connectionSettings externalReferences → cache
        cs_conn_id = (conn_settings_props.get("externalReferences") or {}).get(
            "connection"
        )
        if cs_conn_id:
            conn = self._connections_cache.get(cs_conn_id)
            if conn:
                return conn.connection_type

        # 2b. connectionSettings.properties.type (inline)
        cs_type: Optional[str] = conn_settings_props.get("type")
        if cs_type:
            return cs_type

        # 3. linkedService.properties.type
        ls_type: Optional[str] = ls_props.get("type")
        if ls_type:
            return ls_type

        return None

    @staticmethod
    def _resolve_platform(connection_type: str) -> str:
        """Map a connection type to a DataHub platform identifier."""
        platform = FABRIC_CONNECTION_PLATFORM_MAP.get(connection_type)
        if platform is None:
            logger.debug(
                f"Unmapped connection type '{connection_type}', "
                f"defaulting to connection type as platform"
            )
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
        ds_type_props = dataset_settings.get("typeProperties") or {}

        artifact_id: Optional[str] = conn_type_props.get(
            "artifactId"
        ) or ds_type_props.get("artifactId")
        if not artifact_id:
            return None

        resolved_workspace_id: str = (
            conn_type_props.get("workspaceId")
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
        # file_path = self._extract_file_path(ds_type_props.get("location") or {})
        # TODO: once file based datsets are supported in OneLake connector, remove this log and return a URN of file
        logger.debug(
            f"OneLake dataset for activity '{activity.name}' has artifactId "
            f"but no table — skipping URN"
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
