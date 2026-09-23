import logging
from typing import Any, Callable, Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.azure.constants import ADF_LINKED_SERVICE_PLATFORM_MAP
from datahub.ingestion.source.azure.copy_translator import (
    TABULAR_TRANSLATOR,
    CopyColumnMapping,
    count_configured_mappings,
    get_translator_type,
    make_copy_fine_grained_lineage,
    parse_translator_mappings,
)
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
    DatasetColumns,
    InvokePipelineActivityLineage,
    PipelineActivity,
)
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactorySourceReport,
)
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

logger = logging.getLogger(__name__)

COPY_SOURCE_KEY = "source"
COPY_SINK_KEYS = ("sink", "destination")
DATASET_SETTINGS_KEY = "datasetSettings"
DATASET_SCHEMA_KEY = "schema"
DATASET_COLUMN_NAME_KEY = "name"
TRANSLATOR_KEY = "translator"
EXPRESSION_TYPE = "Expression"
SINK_TABLE_OPTION_KEY = "tableOption"
SINK_TABLE_OPTION_AUTO_CREATE = "autoCreate"
WORKSPACE_ID_KEY = "workspaceId"
# Placeholder Fabric writes for items living in the pipeline's own workspace.
SAME_WORKSPACE_PLACEHOLDER_ID = "00000000-0000-0000-0000-000000000000"

# Resolves a dataset URN to its columns (e.g. from the DataHub graph).
DatasetColumnsResolver = Callable[[str], Optional[DatasetColumns]]


def get_copy_sink(type_properties: Dict[str, Any]) -> Dict[str, Any]:
    """Return the sink block of a Copy activity (``sink`` or ``destination``)."""
    for key in COPY_SINK_KEYS:
        sink = type_properties.get(key)
        if isinstance(sink, dict) and sink:
            return sink
    return {}


def is_auto_create_sink(sink: Dict[str, Any]) -> bool:
    """Whether the Copy sink creates the destination table from the source schema.

    Set as ``sink.tableOption: "autoCreate"`` (e.g. DataWarehouseSink,
    AzureSqlSink, SqlServerSink); the table is created only if it does not
    already exist.
    """
    table_option = sink.get(SINK_TABLE_OPTION_KEY)
    return (
        isinstance(table_option, str)
        and table_option.lower() == SINK_TABLE_OPTION_AUTO_CREATE.lower()
    )


def get_copy_dataset_settings(
    type_properties: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Return the (source, sink) datasetSettings of a Copy activity."""
    source = type_properties.get(COPY_SOURCE_KEY) or {}
    sink = get_copy_sink(type_properties)
    return (
        source.get(DATASET_SETTINGS_KEY) or {},
        sink.get(DATASET_SETTINGS_KEY) or {},
    )


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
        source_ds, sink_ds = get_copy_dataset_settings(activity.type_properties)

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

        resolved_workspace_id = self._resolve_item_workspace_id(
            [conn_type_props, ls_type_props, ds_type_props],
            pipeline_workspace_id,
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
    def _resolve_item_workspace_id(
        type_properties_candidates: List[Dict[str, Any]],
        pipeline_workspace_id: str,
    ) -> str:
        """Return the workspace GUID of a referenced Fabric item.

        Exported pipeline JSON references items in the pipeline's own
        workspace with the all-zero GUID placeholder; that, like a missing or
        empty value, means "same workspace as the pipeline".
        """
        for type_properties in type_properties_candidates:
            workspace_id = type_properties.get(WORKSPACE_ID_KEY)
            if (
                isinstance(workspace_id, str)
                and workspace_id.strip()
                and workspace_id.strip() != SAME_WORKSPACE_PLACEHOLDER_ID
            ):
                return workspace_id
        return pipeline_workspace_id

    @staticmethod
    def _extract_table_name(
        ds_type_properties: Dict[str, Any],
    ) -> Optional[str]:
        """Extract a qualified table, object, or file path from datasetSettings."""
        schema = ds_type_properties.get("schema")
        table = ds_type_properties.get("table")
        if table:
            return f"{schema}.{table}" if schema else table

        # Salesforce-family datasets (SalesforceObject, SalesforceV2Object,
        # SalesforceServiceCloud(V2)Object) identify the sObject by its API
        # name, which is also the dataset name used by the salesforce connector.
        object_api_name = ds_type_properties.get("objectApiName")
        if isinstance(object_api_name, str) and object_api_name:
            return object_api_name

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


class DataHubDatasetColumnsResolver:
    """Looks up dataset columns from schemaMetadata in the DataHub graph."""

    def __init__(self, graph: DataHubGraph) -> None:
        self._graph = graph
        self._cache: Dict[str, Optional[DatasetColumns]] = {}

    def get_columns(self, dataset_urn: str) -> Optional[DatasetColumns]:
        if dataset_urn in self._cache:
            return self._cache[dataset_urn]
        columns: Optional[DatasetColumns] = None
        try:
            schema = self._graph.get_schema_metadata(dataset_urn)
            if schema is not None and schema.fields:
                columns = DatasetColumns(
                    field_paths=[f.fieldPath for f in schema.fields]
                )
        except Exception as e:
            logger.debug("Failed to fetch schemaMetadata for %s: %s", dataset_urn, e)
        self._cache[dataset_urn] = columns
        return columns


class CopyActivityColumnLineageExtractor:
    """Extracts column-level lineage from Fabric Data Factory Copy activities.

    Explicit translator mappings (``mappings`` / legacy ``columnMappings``)
    are emitted as-is, with column names normalized to the dataset schema's
    casing when that schema is known. Without explicit mappings, a
    ``TabularTranslator`` (or no translator) maps columns by name; that is
    only reproduced when both source and sink columns are known, from the
    inline datasetSettings ``schema`` or from DataHub. The exception is an
    ``autoCreate`` sink with unknown columns: it is created from the source
    schema, so sink columns are taken to equal the known source columns.
    """

    def __init__(
        self,
        report: FabricDataFactorySourceReport,
        columns_resolver: Optional[DatasetColumnsResolver] = None,
    ) -> None:
        self._report = report
        self._columns_resolver = columns_resolver

    def extract_column_lineage(
        self,
        activity: PipelineActivity,
        input_urn: str,
        output_urn: str,
        activity_key: str,
    ) -> List[FineGrainedLineageClass]:
        type_props = activity.type_properties
        source_ds, sink_ds = get_copy_dataset_settings(type_props)
        translator = type_props.get(TRANSLATOR_KEY)

        if translator is not None and not isinstance(translator, dict):
            self._report.report_column_lineage_unsupported_translator()
            return []

        translator_type = get_translator_type(translator) if translator else None
        if translator_type == EXPRESSION_TYPE:
            # Mappings supplied at runtime via dynamic content.
            self._report.report_column_lineage_dynamic_translator()
            return []

        configured = count_configured_mappings(translator) if translator else 0
        if translator and configured:
            explicit = parse_translator_mappings(translator)
            if not explicit:
                # e.g. ordinal-only mappings: Fabric applies those rather than
                # the default by-name mapping, so falling back to by-name
                # matching would emit wrong column lineage.
                self._report.report_column_lineage_unresolvable_mappings(activity_key)
                return []
            lineages = self._build_explicit(
                explicit, input_urn, output_urn, source_ds, sink_ds
            )
            self._report.report_column_lineage_explicit(
                len(lineages), num_skipped_mappings=configured - len(explicit)
            )
            return lineages

        if translator_type not in (None, TABULAR_TRANSLATOR):
            self._report.report_column_lineage_unsupported_translator()
            return []

        return self._build_auto_mapped(
            activity_key,
            input_urn,
            output_urn,
            source_ds,
            sink_ds,
            auto_create_sink=is_auto_create_sink(get_copy_sink(type_props)),
        )

    def _build_explicit(
        self,
        mappings: List[CopyColumnMapping],
        input_urn: str,
        output_urn: str,
        source_ds: Dict[str, Any],
        sink_ds: Dict[str, Any],
    ) -> List[FineGrainedLineageClass]:
        source_columns = self._get_columns(input_urn, source_ds)
        sink_columns = self._get_columns(output_urn, sink_ds)
        return [
            make_copy_fine_grained_lineage(
                input_urn,
                self._normalize(mapping.source_column, source_columns),
                output_urn,
                self._normalize(mapping.sink_column, sink_columns),
            )
            for mapping in mappings
        ]

    def _build_auto_mapped(
        self,
        activity_key: str,
        input_urn: str,
        output_urn: str,
        source_ds: Dict[str, Any],
        sink_ds: Dict[str, Any],
        auto_create_sink: bool,
    ) -> List[FineGrainedLineageClass]:
        source_columns = self._get_columns(input_urn, source_ds)
        sink_columns = (
            self._get_columns(output_urn, sink_ds) if source_columns else None
        )
        if source_columns and not sink_columns and auto_create_sink:
            # The sink table is created from the source schema, so its
            # columns are the source columns (as in the ADF connector).
            created_sink_lineages = [
                make_copy_fine_grained_lineage(
                    input_urn,
                    source_field,
                    output_urn,
                    get_simple_field_path_from_v2_field_path(source_field),
                )
                for source_field in source_columns.field_paths
            ]
            self._report.report_column_lineage_auto_created_sink(
                len(created_sink_lineages)
            )
            return created_sink_lineages

        if not source_columns or not sink_columns:
            logger.debug(
                "Skipping by-name column mapping for activity '%s': "
                "source or sink schema unavailable",
                activity_key,
            )
            self._report.report_column_lineage_no_schema(activity_key)
            return []

        lineages: List[FineGrainedLineageClass] = []
        for source_field in source_columns.field_paths:
            sink_field = sink_columns.lookup(source_field)
            if sink_field is None:
                continue
            lineages.append(
                make_copy_fine_grained_lineage(
                    input_urn, source_field, output_urn, sink_field
                )
            )
        self._report.report_column_lineage_auto_mapped(len(lineages))
        return lineages

    def _get_columns(
        self, dataset_urn: str, dataset_settings: Dict[str, Any]
    ) -> Optional[DatasetColumns]:
        inline = self._inline_columns(dataset_settings)
        if inline:
            return inline
        if self._columns_resolver is None:
            return None
        return self._columns_resolver(dataset_urn)

    @staticmethod
    def _inline_columns(dataset_settings: Dict[str, Any]) -> Optional[DatasetColumns]:
        """Columns from the design-time ``schema`` list on datasetSettings."""
        schema = dataset_settings.get(DATASET_SCHEMA_KEY)
        if not isinstance(schema, list):
            return None
        names: List[str] = []
        for column in schema:
            if not isinstance(column, dict):
                continue
            name = column.get(DATASET_COLUMN_NAME_KEY)
            if isinstance(name, str) and name:
                names.append(name)
        return DatasetColumns(field_paths=names) if names else None

    @staticmethod
    def _normalize(column: str, columns: Optional[DatasetColumns]) -> str:
        if columns is None:
            return column
        return columns.lookup(column) or column


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

        self._report.warning(
            title="InvokePipeline Lineage Not Resolved",
            message="Unsupported operationType. "
            "Only InvokeFabricPipeline is supported.",
            context=f"activity={activity.name}, operationType={operation_type}",
            log=False,
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
