import logging
from typing import Dict, Iterable, List, Optional, Set, Union

from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.hightouch.config import (
    Constant,
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    HIGHTOUCH_PLATFORM,
    KNOWN_DESTINATION_PLATFORM_MAPPING,
    KNOWN_SOURCE_PLATFORM_MAPPING,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_assertion import (
    HightouchAssertionsHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_schema import HightouchSchemaHandler
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchDestinationLineageInfo,
    HightouchModel,
    HightouchModelDatasetResult,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import (
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


class WorkspaceKey(ContainerKey):
    workspace_id: str


class FolderKey(ContainerKey):
    folder_id: str
    workspace_id: str


def normalize_column_name(name: str) -> str:
    return name.lower().replace("_", "").replace("-", "")


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, showing data flow from source -> model -> sync -> destination",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, emitting column-level lineage from field mappings",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase):
    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = "hightouch"

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(self.config.api_config)

        self.graph: Optional[DataHubGraph] = None
        if ctx.graph:
            self.graph = ctx.graph
            logger.info(
                "DataHub graph client available - will fetch schemas from DataHub when possible"
            )
        else:
            logger.debug(
                "No DataHub graph connection - schema fetching from DataHub disabled"
            )

        self._sources_cache: Dict[str, HightouchSourceConnection] = {}
        self._models_cache: Dict[str, HightouchModel] = {}
        self._destinations_cache: Dict[str, HightouchDestination] = {}
        self._users_cache: Dict[str, HightouchUser] = {}
        self._workspaces_cache: Dict[str, str] = {}  # workspace_id -> workspace_name
        self._folders_cache: Dict[str, str] = {}  # folder_id -> folder_name
        self._emitted_containers: set = set()
        self._registered_urns: Set[str] = set()  # Track URNs with loaded schemas
        self._model_schema_fields_cache: Dict[
            str, List[SchemaFieldClass]
        ] = {}  # Cache normalized schema fields

        self._urn_builder = HightouchUrnBuilder(self)

        self._schema_handler = HightouchSchemaHandler(
            report=self.report, graph=self.graph, urn_builder=self._urn_builder
        )

        self._assertions_handler = HightouchAssertionsHandler(
            source=self,
            config=self.config,
            report=self.report,
            api_client=self.api_client,
            urn_builder=self._urn_builder,
        )

        self._sql_aggregators: Dict[str, Optional[SqlParsingAggregator]] = {}

        self._destination_lineage: Dict[str, HightouchDestinationLineageInfo] = {}

        self._lineage_handler = HightouchLineageHandler(
            api_client=self.api_client,
            report=self.report,
            urn_builder=self._urn_builder,
            graph=self.graph,
            registered_urns=self._registered_urns,
            model_schema_fields_cache=self._model_schema_fields_cache,
            destination_lineage=self._destination_lineage,
            sql_aggregators=self._sql_aggregators,
        )

    def _get_aggregator_for_platform(
        self, source_platform: PlatformDetail
    ) -> Optional[SqlParsingAggregator]:
        # Returns None if platform is unknown - SQL parsing is optional, basic lineage still works
        platform = source_platform.platform
        if not platform:
            logger.debug("No platform specified, skipping SQL aggregator creation")
            return None

        if platform not in self._sql_aggregators:
            try:
                logger.info(
                    f"Creating SQL parsing aggregator for platform: {platform} "
                    f"(instance: {source_platform.platform_instance}, env: {source_platform.env})"
                )

                self._sql_aggregators[platform] = SqlParsingAggregator(
                    platform=platform,
                    platform_instance=source_platform.platform_instance,
                    env=source_platform.env or self.config.env,
                    graph=self.graph,
                    eager_graph_load=False,
                    generate_lineage=True,
                    generate_queries=False,
                    generate_usage_statistics=False,
                    generate_operations=False,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to create SQL aggregator for platform {platform}. "
                    f"SQL parsing will be disabled for this platform, but basic lineage will still be emitted. "
                    f"Error: {e}"
                )
                self._sql_aggregators[platform] = None
                return None

        return self._sql_aggregators[platform]

    def _get_source(self, source_id: str) -> Optional[HightouchSourceConnection]:
        if source_id not in self._sources_cache:
            self.report.report_api_call()
            source = self.api_client.get_source_by_id(source_id)
            if source:
                self._sources_cache[source_id] = source
        return self._sources_cache.get(source_id)

    def _get_model(self, model_id: str) -> Optional[HightouchModel]:
        if model_id not in self._models_cache:
            self.report.report_api_call()
            model = self.api_client.get_model_by_id(model_id)
            if model:
                self._models_cache[model_id] = model
        return self._models_cache.get(model_id)

    def _get_destination(self, destination_id: str) -> Optional[HightouchDestination]:
        if destination_id not in self._destinations_cache:
            self.report.report_api_call()
            destination = self.api_client.get_destination_by_id(destination_id)
            if destination:
                self._destinations_cache[destination_id] = destination
        return self._destinations_cache.get(destination_id)

    def _preload_workspaces_and_folders(self) -> None:
        """Preload all workspaces and folders once at the beginning for efficient container name resolution."""
        if not self.config.extract_workspaces_to_containers:
            return

        # Preload all workspaces
        try:
            self.report.report_api_call()
            workspaces = self.api_client.get_workspaces()
            for workspace in workspaces:
                self._workspaces_cache[workspace.id] = workspace.name
            logger.info(f"Preloaded {len(workspaces)} workspace names")
        except Exception as e:
            logger.warning(f"Could not preload workspaces: {e}")

        # Note: Hightouch API may not have a bulk folders endpoint
        # Individual folder fetches will be done on-demand with caching

    def _get_workspace_name(self, workspace_id: str) -> str:
        """Get workspace name from cache, falling back to workspace ID."""
        if workspace_id not in self._workspaces_cache:
            # Fallback if not preloaded (shouldn't normally happen)
            logger.warning(
                f"Workspace {workspace_id} not in preloaded cache, using ID as fallback"
            )
            self._workspaces_cache[workspace_id] = f"Workspace {workspace_id}"

        return self._workspaces_cache[workspace_id]

    def _get_folder_name(self, folder_id: str) -> str:
        """Get folder name from cache or API on-demand, falling back to folder ID."""
        if folder_id not in self._folders_cache:
            try:
                self.report.report_api_call()
                folder_data = self.api_client.get_folder_by_id(folder_id)
                if folder_data and "name" in folder_data:
                    self._folders_cache[folder_id] = folder_data["name"]
                else:
                    # Fallback to ID if folder not found or no name
                    self._folders_cache[folder_id] = f"Folder {folder_id}"
                    logger.debug(
                        f"Could not fetch folder name for {folder_id}, using ID as fallback"
                    )
            except Exception as e:
                # Fallback to ID on error
                self._folders_cache[folder_id] = f"Folder {folder_id}"
                logger.debug(f"Error fetching folder {folder_id}: {e}")

        return self._folders_cache[folder_id]

    def _get_user(self, user_id: str) -> Optional[HightouchUser]:
        if user_id not in self._users_cache:
            self.report.report_api_call()
            user = self.api_client.get_user_by_id(user_id)
            if user:
                self._users_cache[user_id] = user
        return self._users_cache.get(user_id)

    def _get_platform_for_source(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        source_details = self.config.sources_to_platform_instance.get(
            source.id, PlatformDetail()
        )

        if source_details.platform is None:
            if source.type.lower() in KNOWN_SOURCE_PLATFORM_MAPPING:
                source_details.platform = KNOWN_SOURCE_PLATFORM_MAPPING[
                    source.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown source platform type",
                    message=f"Source type '{source.type}' is not in the known platform mapping. "
                    f"Using source type as platform name. Consider adding a mapping in "
                    f"sources_to_platform_instance config for source_id '{source.id}'.",
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                source_details.platform = source.type.lower()

        if source_details.env is None:
            source_details.env = self.config.env

        if source_details.database is None and source.configuration:
            source_details.database = source.configuration.get("database")

        return source_details

    def _get_platform_for_destination(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        destination_details = self.config.destinations_to_platform_instance.get(
            destination.id, PlatformDetail()
        )

        if destination_details.platform is None:
            if destination.type.lower() in KNOWN_DESTINATION_PLATFORM_MAPPING:
                destination_details.platform = KNOWN_DESTINATION_PLATFORM_MAPPING[
                    destination.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown destination platform type",
                    message=f"Destination type '{destination.type}' is not in the known platform mapping. "
                    f"Using destination type as platform name. Consider adding a mapping in "
                    f"destinations_to_platform_instance config for destination_id '{destination.id}'.",
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                destination_details.platform = destination.type.lower()

        if destination_details.env is None:
            destination_details.env = self.config.env

        return destination_details

    def _generate_workspace_container(
        self, workspace_id: str
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = WorkspaceKey(
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        # Fetch actual workspace name from API
        workspace_name = self._get_workspace_name(workspace_id)

        container_workunits = gen_containers(
            container_key=container_key,
            name=workspace_name,
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_WORKSPACE)],
            extra_properties={
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            self.report.workspaces_emitted += 1
            yield workunit

    def _generate_folder_container(
        self,
        folder_id: str,
        workspace_id: str,
        parent_container_key: Optional[ContainerKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = FolderKey(
            folder_id=folder_id,
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        # Fetch actual folder name from API
        folder_name = self._get_folder_name(folder_id)

        container_workunits = gen_containers(
            container_key=container_key,
            name=folder_name,
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_FOLDER)],
            parent_container_key=parent_container_key,
            extra_properties={
                "folder_id": folder_id,
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            self.report.folders_emitted += 1
            yield workunit

    def _add_entity_to_container(
        self, entity_urn: str, container_key: ContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_workunits = add_dataset_to_container(container_key, entity_urn)
        for workunit in container_workunits:
            yield workunit

    def _build_model_custom_properties(
        self, model: HightouchModel, source: Optional[HightouchSourceConnection]
    ) -> Dict[str, str]:
        """Build custom properties dictionary for a model dataset."""
        custom_properties = {
            "model_id": model.id,
            "query_type": model.query_type,
            "is_schema": str(model.is_schema),
        }

        if model.primary_key:
            custom_properties["primary_key"] = model.primary_key

        if source:
            custom_properties["source_id"] = source.id
            custom_properties["source_name"] = source.name
            custom_properties["source_type"] = source.type

        if model.tags:
            for key, value in model.tags.items():
                custom_properties[f"tag_{key}"] = value

        if model.folder_id:
            custom_properties["folder_id"] = model.folder_id

        if model.raw_sql:
            sql_truncated = (
                model.raw_sql[:2000] if len(model.raw_sql) > 2000 else model.raw_sql
            )
            custom_properties["raw_sql"] = sql_truncated
            if len(model.raw_sql) > 2000:
                custom_properties["raw_sql_truncated"] = "true"
                custom_properties["raw_sql_length"] = str(len(model.raw_sql))

        return custom_properties

    def _build_model_schema_fields(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]],
    ) -> List[SchemaFieldClass]:
        """Build schema field classes with upstream casing normalization."""
        schema_fields = self._schema_handler.resolve_schema(
            model=model, source=source, referenced_columns=referenced_columns
        )

        if not schema_fields:
            return []

        # For table models, normalize field casing to match upstream table
        # Also extract table URNs from any models with SQL for single-table queries
        sql_table_urns = []
        if model.raw_sql and source:
            sql_table_urns = self._extract_table_urns_from_sql(model, source)

        upstream_field_casing = self._lineage_handler.get_upstream_field_casing(
            model, source, sql_table_urns
        )

        if upstream_field_casing:
            logger.info(
                f"Using upstream field casing for model {model.slug}: {len(upstream_field_casing)} fields mapped. "
                f"Casing map: {upstream_field_casing}"
            )
        else:
            logger.info(
                f"No upstream field casing available for model {model.slug} (query_type={model.query_type}, "
                f"name={model.name}, graph={'available' if self.graph else 'not available'}), "
                f"using original Hightouch casing"
            )

        schema_field_classes: List[SchemaFieldClass] = []
        for field in schema_fields:
            # Use upstream casing if available, otherwise use field name as-is
            normalized_name = normalize_column_name(field.name)
            field_path = upstream_field_casing.get(normalized_name, field.name)

            if upstream_field_casing and normalized_name in upstream_field_casing:
                if field_path != field.name:
                    logger.info(
                        f"Normalized field casing for model {model.slug}: '{field.name}' -> '{field_path}' "
                        f"(normalized lookup key: '{normalized_name}')"
                    )
            elif upstream_field_casing:
                logger.info(
                    f"No casing match found for field '{field.name}' in model {model.slug} "
                    f"(normalized: '{normalized_name}'). Available upstream fields: {list(upstream_field_casing.keys())}"
                )

            schema_field_classes.append(
                SchemaFieldClass(
                    fieldPath=field_path,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType=field.type,
                    description=field.description,
                    isPartOfKey=field.is_primary_key,
                )
            )

        if schema_field_classes:
            field_names = [f.fieldPath for f in schema_field_classes]
            logger.info(f"Final schema for model {model.slug}: {field_names}")

        return schema_field_classes

    def _register_model_schema_with_aggregator(
        self,
        model: HightouchModel,
        dataset_urn: str,
        source: Optional[HightouchSourceConnection],
        schema_field_classes: List[SchemaFieldClass],
    ) -> None:
        """Register model schema with SQL parsing aggregator if available."""
        if not source:
            return

        source_platform = self._get_platform_for_source(source)
        if not source_platform.platform:
            return

        aggregator = self._get_aggregator_for_platform(source_platform)
        if not aggregator:
            return

        try:
            aggregator.register_schema(
                dataset_urn,
                SchemaMetadataClass(
                    schemaName=model.slug,
                    platform=f"urn:li:dataPlatform:{HIGHTOUCH_PLATFORM}",
                    version=0,
                    hash="",
                    platformSchema=SchemalessClass(),
                    fields=schema_field_classes,
                ),
            )
            logger.debug(
                f"Registered model schema for {model.slug} with aggregator "
                f"for platform {source_platform.platform}"
            )
        except Exception as e:
            logger.debug(f"Failed to register model schema for {model.slug}: {e}")

    def _setup_table_model_upstream(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        custom_properties: Dict[str, str],
    ) -> Optional[Union[str, DatasetUrn]]:
        """Set up upstream lineage for table-type models, returns upstream URN."""
        if not source or model.query_type != "table" or not model.name:
            return None

        table_name = model.name

        if source.configuration:
            database = source.configuration.get("database", "")
            schema = source.configuration.get("schema", "")
            source_details = self._urn_builder._get_cached_source_details(source)

            if source_details.include_schema_in_urn and schema:
                table_name = f"{database}.{schema}.{table_name}"
            elif database and "." not in table_name:
                table_name = f"{database}.{table_name}"

        upstream_urn = self._urn_builder.make_upstream_table_urn(table_name, source)
        custom_properties["table_lineage"] = "true"
        custom_properties["upstream_table"] = table_name
        custom_properties["source_table_urn"] = str(upstream_urn)

        logger.debug(
            f"Set direct upstream lineage for table-type model {model.slug}: {upstream_urn}. "
            f"This ensures basic lineage is emitted even if SQL aggregator is unavailable."
        )

        return upstream_urn

    def _generate_model_dataset(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> HightouchModelDatasetResult:
        custom_properties = self._build_model_custom_properties(model, source)

        dataset = Dataset(
            name=model.slug,
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=custom_properties,
        )

        # Build and set schema fields
        schema_field_classes = self._build_model_schema_fields(
            model, source, referenced_columns
        )
        if schema_field_classes:
            dataset._set_schema(schema_field_classes)
            self._register_model_schema_with_aggregator(
                model, str(dataset.urn), source, schema_field_classes
            )

        # Set up upstream lineage for table models
        upstream_urn = self._setup_table_model_upstream(
            model, source, custom_properties
        )
        if upstream_urn:
            dataset.set_upstreams([upstream_urn])

        dataset.set_custom_properties(custom_properties)

        # Cache normalized schema fields for use in downstream column lineage
        if schema_field_classes:
            self._model_schema_fields_cache[model.id] = schema_field_classes

        return HightouchModelDatasetResult(
            dataset=dataset, schema_fields=schema_field_classes
        )

    def _generate_dataflow_from_sync(self, sync: HightouchSync) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=sync.id,
            env=self.config.env,
            display_name=sync.slug,
            platform_instance=self.config.platform_instance,
        )

    def _get_inlet_urn_for_model(
        self, model: HightouchModel, sync: HightouchSync
    ) -> Union[str, DatasetUrn, None]:
        source = self._get_source(model.source_id)
        if not source:
            self.report.warning(
                title="Failed to get source for model",
                message="Could not retrieve source information for lineage creation.",
                context=f"sync_slug: {sync.slug} (model_id: {model.source_id})",
            )
            return None

        if not self.config.emit_models_as_datasets:
            if model.query_type == "table" and model.name:
                table_name = model.name
                if source.configuration:
                    database = source.configuration.get("database", "")
                    schema = source.configuration.get("schema", "")
                    source_details = self._urn_builder._get_cached_source_details(
                        source
                    )
                    if source_details.include_schema_in_urn and schema:
                        table_name = f"{database}.{schema}.{table_name}"
                    elif database and "." not in table_name:
                        table_name = f"{database}.{table_name}"

                return self._urn_builder.make_upstream_table_urn(table_name, source)
            else:
                logger.warning(
                    f"Sync {sync.slug}: emit_models_as_datasets=False but model {model.slug} is not a table type. "
                    f"Cannot create direct lineage without model. Model query_type: {model.query_type}"
                )
                return None

        # Always return Hightouch model URN when emitting models as datasets
        # The sibling relationship connects the model to the upstream warehouse table
        return self._urn_builder.make_model_urn(model, source)

    def _get_outlet_urn_for_sync(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]:
        # Hightouch API doesn't guarantee config schema (per swagger.json).
        # Try common keys in priority order: object > tableName > table > destinationTable > objectName
        dest_table = None

        if sync.configuration:
            sync_type = sync.configuration.get("type")

            if sync_type == "event":
                event_name_value = sync.configuration.get("eventName")
                # eventName can be a string or a dict like {"from": "EVENT_NAME"}
                if isinstance(event_name_value, dict):
                    dest_table = event_name_value.get("from")
                elif isinstance(event_name_value, str):
                    dest_table = event_name_value
            else:
                for key in [
                    "object",
                    "tableName",
                    "table",
                    "destinationTable",
                    "objectName",
                ]:
                    value = sync.configuration.get(key)
                    if value:
                        # Values can be strings or dicts
                        if isinstance(value, str):
                            dest_table = value
                        elif isinstance(value, dict):
                            # Try to extract from common dict patterns
                            dest_table = value.get("from") or value.get("name")

                        if dest_table:
                            logger.debug(
                                f"Found destination table/object '{dest_table}' using config key '{key}' "
                                f"for sync {sync.slug} (destination type: {destination.type})"
                            )
                            break

        if not dest_table:
            dest_table = f"{sync.slug}_destination"
            logger.warning(
                f"Could not determine destination table name for sync {sync.slug} (id: {sync.id}). "
                f"Destination type: {destination.type}, Sync type: {sync.configuration.get('type') if sync.configuration else 'unknown'}. "
                f"Tried keys: object, tableName, table, destinationTable, objectName. "
                f"Using fallback name: {dest_table}"
            )

        return self._urn_builder.make_destination_urn(dest_table, destination)

    def _generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=sync.id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        datajob = DataJob(
            name=sync.id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=sync.slug,
        )

        model = self._get_model(sync.model_id)
        destination = self._get_destination(sync.destination_id)

        inlets: List[Union[str, DatasetUrn]] = []
        if model:
            inlet_urn = self._get_inlet_urn_for_model(model, sync)
            if inlet_urn:
                inlets.append(inlet_urn)

        datajob.set_inlets(inlets)

        outlet_urn = None
        if destination:
            outlet_urn = self._get_outlet_urn_for_sync(sync, destination)

        if outlet_urn:
            datajob.set_outlets([outlet_urn])

        if model and outlet_urn and inlets:
            # Use cached schema fields with normalized casing for column lineage
            model_schema_fields = self._model_schema_fields_cache.get(model.id)
            fine_grained_lineages = self._lineage_handler.generate_column_lineage(
                sync, model, inlets[0], outlet_urn, model_schema_fields
            )
            if fine_grained_lineages:
                datajob.set_fine_grained_lineages(fine_grained_lineages)

        custom_properties: Dict[str, str] = {
            "sync_id": sync.id,
            "model_id": sync.model_id,
            "destination_id": sync.destination_id,
            "disabled": str(sync.disabled),
        }

        if sync.schedule:
            custom_properties["schedule"] = str(sync.schedule)

        if model:
            custom_properties["model_name"] = model.name
            custom_properties["model_slug"] = model.slug
            custom_properties["query_type"] = model.query_type
            if model.description:
                custom_properties["model_description"] = model.description

        if destination:
            custom_properties["destination_name"] = destination.name
            custom_properties["destination_type"] = destination.type

        if sync.tags:
            # DataJob SDK doesn't directly support tags, so store as custom property
            tag_list = [
                f"{key}:{value}" for key, value in sync.tags.items() if key and value
            ]
            if tag_list:
                custom_properties["hightouch_tags"] = ", ".join(tag_list)
                self.report.tags_emitted += len(tag_list)

        datajob.set_custom_properties(custom_properties)

        return datajob

    def _generate_dpi_from_sync_run(
        self, sync_run: HightouchSyncRun, datajob: DataJob, sync_id: str
    ) -> DataProcessInstance:
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
        )

        dpi = DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=sync_run.id,
            clone_inlets=True,
            clone_outlets=True,
        )

        custom_props = {
            "sync_run_id": sync_run.id,
            "sync_id": sync_id,
            "status": sync_run.status,
        }
        if sync_run.planned_rows:
            custom_props["planned_rows_added"] = str(
                sync_run.planned_rows.get("added", 0)
            )
            custom_props["planned_rows_changed"] = str(
                sync_run.planned_rows.get("changed", 0)
            )
            custom_props["planned_rows_removed"] = str(
                sync_run.planned_rows.get("removed", 0)
            )
            custom_props["planned_rows_total"] = str(
                sum(sync_run.planned_rows.values())
            )

        if sync_run.successful_rows:
            custom_props["successful_rows_added"] = str(
                sync_run.successful_rows.get("added", 0)
            )
            custom_props["successful_rows_changed"] = str(
                sync_run.successful_rows.get("changed", 0)
            )
            custom_props["successful_rows_removed"] = str(
                sync_run.successful_rows.get("removed", 0)
            )
            custom_props["successful_rows_total"] = str(
                sum(sync_run.successful_rows.values())
            )

        if sync_run.failed_rows:
            failed_total = sum(sync_run.failed_rows.values())
            custom_props["failed_rows_total"] = str(failed_total)
            custom_props["failed_rows_added"] = str(
                sync_run.failed_rows.get("added", 0)
            )
            custom_props["failed_rows_changed"] = str(
                sync_run.failed_rows.get("changed", 0)
            )
            custom_props["failed_rows_removed"] = str(
                sync_run.failed_rows.get("removed", 0)
            )

        if sync_run.query_size:
            custom_props["query_size_bytes"] = str(sync_run.query_size)

        if sync_run.completion_ratio is not None:
            custom_props["completion_ratio"] = f"{sync_run.completion_ratio * 100:.1f}%"
        if sync_run.error:
            if isinstance(sync_run.error, str):
                custom_props["error_message"] = sync_run.error
            else:
                custom_props["error_message"] = sync_run.error.get(
                    "message", "Unknown error"
                )
                if "code" in sync_run.error:
                    custom_props["error_code"] = sync_run.error["code"]

        dpi.properties.update(custom_props)

        return dpi

    def _get_dpi_workunits(
        self, sync_run: HightouchSyncRun, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_map = {
            "success": InstanceRunResult.SUCCESS,
            "failed": InstanceRunResult.FAILURE,
            "cancelled": InstanceRunResult.SKIPPED,
            "interrupted": InstanceRunResult.SKIPPED,
            "warning": InstanceRunResult.SUCCESS,
        }

        status = status_map.get(sync_run.status.lower(), InstanceRunResult.SUCCESS)

        start_timestamp_millis = int(sync_run.started_at.timestamp() * 1000)

        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()

        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()

        end_timestamp_millis = (
            int(sync_run.finished_at.timestamp() * 1000)
            if sync_run.finished_at
            else start_timestamp_millis
        )

        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=status,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _emit_model_aspects(
        self,
        model: HightouchModel,
        model_dataset: Dataset,
        source: Optional[HightouchSourceConnection] = None,
        schema_fields: Optional[List[SchemaFieldClass]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = str(model_dataset.urn)

        subtypes: List[str] = [str(DatasetSubTypes.HIGHTOUCH_MODEL)]
        if model.raw_sql:
            subtypes.append(str(DatasetSubTypes.VIEW))
        elif model.query_type == "table":
            subtypes.append(str(DatasetSubTypes.TABLE))
        else:
            subtypes.append(str(DatasetSubTypes.VIEW))

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=subtypes),
        ).as_workunit()

        if model.raw_sql:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=model.raw_sql,
                ),
            ).as_workunit()

        if model.tags:
            tags_to_emit = [
                TagAssociationClass(tag=f"urn:li:tag:ht_{key}_{value}")
                for key, value in model.tags.items()
                if key and value
            ]
            if tags_to_emit:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags_to_emit),
                ).as_workunit()
                self.report.tags_emitted += len(tags_to_emit)
                logger.debug(f"Emitted {len(tags_to_emit)} tags for model {model.slug}")

        # Generate sibling relationships and column-level lineage for models with single upstream table
        # This applies to:
        # 1. table models (query_type == "table") - direct table reference via model.name
        # 2. raw_sql models with exactly one upstream table - determined via SQL parsing
        source_table_urn = None

        if model.query_type == "table" and model.name and source:
            table_name = model.name
            if source.configuration:
                database = source.configuration.get("database", "")
                schema = source.configuration.get("schema", "")
                source_details = self._urn_builder._get_cached_source_details(source)
                if source_details.include_schema_in_urn and schema:
                    table_name = f"{database}.{schema}.{table_name}"
                elif database and "." not in table_name:
                    table_name = f"{database}.{table_name}"

            source_table_urn = self._urn_builder.make_upstream_table_urn(
                table_name, source
            )
        elif model.raw_sql and source:
            # For raw_sql models, use SQL-parsed table URNs if exactly one table is referenced
            sql_table_urns = self._extract_table_urns_from_sql(model, source)
            if len(sql_table_urns) == 1:
                source_table_urn = sql_table_urns[0]
                logger.debug(
                    f"Using SQL-parsed upstream URN for model {model.slug} sibling/lineage: {source_table_urn}"
                )

        if source_table_urn:
            # Emit sibling aspects if configured
            if self.config.include_table_lineage_to_sibling:
                yield from self._lineage_handler.emit_sibling_aspects(
                    dataset_urn, str(source_table_urn)
                )
                logger.debug(
                    f"Created sibling relationship for model {model.slug} (query_type={model.query_type}): "
                    f"Hightouch model <-> {source_table_urn}"
                )

            # Generate and emit column-level lineage for models with single upstream table
            if model.query_type == "table":
                fine_grained_lineages = (
                    self._lineage_handler.generate_table_model_column_lineage(
                        model, dataset_urn, str(source_table_urn), schema_fields or []
                    )
                )
                if fine_grained_lineages:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=str(source_table_urn),
                                    type=DatasetLineageTypeClass.COPY,
                                )
                            ],
                            fineGrainedLineages=fine_grained_lineages,
                        ),
                    ).as_workunit()
                    self.report.column_lineage_emitted += len(fine_grained_lineages)
                    logger.debug(
                        f"Emitted {len(fine_grained_lineages)} column lineage edges for table model {model.slug}: "
                        f"{source_table_urn} -> {dataset_urn}"
                    )

    def _get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_syncs_scanned()

        model = None
        if self.config.emit_models_as_datasets:
            model = self._get_model(sync.model_id)
            if model and self.config.model_patterns.allowed(model.name):
                source = self._get_source(model.source_id)
                result = self._generate_model_dataset(
                    model, source, referenced_columns=sync.referenced_columns
                )
                self.report.report_models_emitted()
                yield result.dataset
                yield from self._emit_model_aspects(
                    model, result.dataset, source, result.schema_fields
                )

                if source:
                    self._lineage_handler.register_model_lineage(
                        model,
                        str(result.dataset.urn),
                        source,
                        self._get_platform_for_source,
                        self._get_aggregator_for_platform,
                    )

                # Organize models into workspace and folder containers
                if model.workspace_id:
                    workspace_key = WorkspaceKey(
                        workspace_id=model.workspace_id,
                        platform=HIGHTOUCH_PLATFORM,
                        instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                    yield from self._generate_workspace_container(model.workspace_id)

                    if model.folder_id:
                        # Model is in a folder - create folder container and add model to it
                        folder_key = FolderKey(
                            folder_id=model.folder_id,
                            workspace_id=model.workspace_id,
                            platform=HIGHTOUCH_PLATFORM,
                            instance=self.config.platform_instance,
                            env=self.config.env,
                        )
                        yield from self._generate_folder_container(
                            model.folder_id,
                            model.workspace_id,
                            parent_container_key=workspace_key,
                        )
                        yield from self._add_entity_to_container(
                            str(result.dataset.urn), folder_key
                        )
                    else:
                        # Model is directly in workspace - add model to workspace
                        yield from self._add_entity_to_container(
                            str(result.dataset.urn), workspace_key
                        )

        destination = self._get_destination(sync.destination_id)
        outlet_urn = None
        if destination:
            outlet_urn = self._get_outlet_urn_for_sync(sync, destination)
            if outlet_urn:
                self.report.report_destinations_emitted()

        dataflow = self._generate_dataflow_from_sync(sync)
        yield dataflow

        datajob = self._generate_datajob_from_sync(sync)
        yield datajob

        # Organize syncs (dataflows/datajobs) into workspace containers
        if sync.workspace_id:
            workspace_key = WorkspaceKey(
                workspace_id=sync.workspace_id,
                platform=HIGHTOUCH_PLATFORM,
                instance=self.config.platform_instance,
                env=self.config.env,
            )
            yield from self._generate_workspace_container(sync.workspace_id)
            yield from self._add_entity_to_container(str(dataflow.urn), workspace_key)
            yield from self._add_entity_to_container(str(datajob.urn), workspace_key)

        if outlet_urn and datajob.inlets:
            self._lineage_handler.accumulate_destination_lineage(
                str(outlet_urn),
                datajob.inlets,
                datajob.fine_grained_lineages,
            )

        if self.config.include_sync_runs:
            self.report.report_api_call()
            sync_runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )

            for sync_run in sync_runs:
                self.report.report_sync_runs_scanned()
                dpi = self._generate_dpi_from_sync_run(sync_run, datajob, sync.id)
                yield from self._get_dpi_workunits(sync_run, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _get_model_workunits(
        self, model: HightouchModel
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_models_scanned()

        source = self._get_source(model.source_id)
        result = self._generate_model_dataset(model, source)
        self.report.report_models_emitted()
        yield result.dataset

        yield from self._emit_model_aspects(
            model, result.dataset, source, result.schema_fields
        )

        if source:
            self._lineage_handler.register_model_lineage(
                model,
                str(result.dataset.urn),
                source,
                self._get_platform_for_source,
                self._get_aggregator_for_platform,
            )

        # Organize models into workspace and folder containers
        if model.workspace_id:
            workspace_key = WorkspaceKey(
                workspace_id=model.workspace_id,
                platform=HIGHTOUCH_PLATFORM,
                instance=self.config.platform_instance,
                env=self.config.env,
            )
            yield from self._generate_workspace_container(model.workspace_id)

            if model.folder_id:
                # Model is in a folder - create folder container and add model to it
                folder_key = FolderKey(
                    folder_id=model.folder_id,
                    workspace_id=model.workspace_id,
                    platform=HIGHTOUCH_PLATFORM,
                    instance=self.config.platform_instance,
                    env=self.config.env,
                )
                yield from self._generate_folder_container(
                    model.folder_id,
                    model.workspace_id,
                    parent_container_key=workspace_key,
                )
                yield from self._add_entity_to_container(
                    str(result.dataset.urn), folder_key
                )
            else:
                # Model is directly in workspace - add model to workspace
                yield from self._add_entity_to_container(
                    str(result.dataset.urn), workspace_key
                )

    def _extract_table_urns_from_sql(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
    ) -> List[str]:
        if not model.raw_sql:
            return []

        source_platform = self._get_platform_for_source(source)
        if not source_platform.platform:
            return []

        try:
            sql_result = create_lineage_sql_parsed_result(
                query=model.raw_sql,
                default_db=source_platform.database,
                platform=source_platform.platform,
                platform_instance=source_platform.platform_instance,
                env=source_platform.env,
                graph=None,
                schema_aware=False,
            )

            if sql_result.in_tables:
                logger.debug(
                    f"Extracted {len(sql_result.in_tables)} table references from SQL in model {model.slug}"
                )
                return [str(urn) for urn in sql_result.in_tables]
        except Exception as e:
            logger.debug(f"Could not extract tables from SQL for model {model.id}: {e}")

        return []

    def _fetch_and_register_schema(
        self,
        urn: str,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
        urn_description: str = "table",
    ) -> bool:
        if urn in registered_urns:
            return False

        if not self.graph:
            return False

        try:
            schema_metadata = self.graph.get_schema_metadata(urn)
            if schema_metadata and schema_metadata.fields:
                aggregator.register_schema(urn, schema_metadata)
                registered_urns.add(urn)
                logger.debug(
                    f"Preloaded schema for {urn_description} {urn} "
                    f"({len(schema_metadata.fields)} fields)"
                )
                return True
        except Exception as e:
            logger.debug(f"Could not preload schema for {urn}: {e}")

        return False

    def _preload_model_schemas(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
    ) -> None:
        if model.query_type == "table" and model.name:
            table_name = model.name
            if source.configuration:
                database = source.configuration.get("database", "")
                schema = source.configuration.get("schema", "")
                source_details = self._urn_builder._get_cached_source_details(source)
                if source_details.include_schema_in_urn and schema:
                    table_name = f"{database}.{schema}.{table_name}"
                elif database and "." not in table_name:
                    table_name = f"{database}.{table_name}"

            upstream_urn = str(
                self._urn_builder.make_upstream_table_urn(table_name, source)
            )
            self._fetch_and_register_schema(
                upstream_urn, aggregator, registered_urns, "upstream table"
            )

        if model.raw_sql and model.query_type == "raw_sql":
            sql_table_urns = self._extract_table_urns_from_sql(model, source)
            for table_urn in sql_table_urns:
                self._fetch_and_register_schema(
                    table_urn, aggregator, registered_urns, "SQL-referenced table"
                )

    def _preload_sync_schemas(
        self,
        sync: HightouchSync,
        registered_urns: Set[str],
    ) -> None:
        model = self._get_model(sync.model_id)
        if not model:
            return

        source = self._get_source(model.source_id)
        if not source:
            return

        source_platform = self._get_platform_for_source(source)
        if not source_platform.platform:
            return

        aggregator = self._get_aggregator_for_platform(source_platform)
        if not aggregator:
            return

        model_urn = str(self._urn_builder.make_model_urn(model, source))
        self._fetch_and_register_schema(model_urn, aggregator, registered_urns, "model")

        destination = self._get_destination(sync.destination_id)
        if not destination:
            return

        outlet_urn = str(self._get_outlet_urn_for_sync(sync, destination))
        if outlet_urn:
            self._fetch_and_register_schema(
                outlet_urn, aggregator, registered_urns, "destination"
            )

    def _preload_schemas_for_sql_parsing(
        self,
        models: List[HightouchModel],
        syncs: List[HightouchSync],
    ) -> None:
        # Fetch schemas from DataHub for SQL parsing: upstream tables, SQL refs, models, destinations
        if not self.graph:
            logger.debug("No DataHub graph available - skipping schema preloading")
            return

        logger.info("Preloading schemas from DataHub for SQL parsing")
        self._registered_urns.clear()  # Clear any previous run's data

        for model in models:
            source = self._get_source(model.source_id)
            if not source:
                continue

            source_platform = self._get_platform_for_source(source)
            if not source_platform.platform:
                continue

            aggregator = self._get_aggregator_for_platform(source_platform)
            if not aggregator:
                continue

            self._preload_model_schemas(
                model, source, aggregator, self._registered_urns
            )

        for sync in syncs:
            self._preload_sync_schemas(sync, self._registered_urns)

        logger.info(
            f"Preloaded {len(self._registered_urns)} schemas from DataHub for SQL parsing"
        )

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        logger.info("Starting Hightouch metadata extraction")

        # Preload workspace and folder names for efficient container organization
        self._preload_workspaces_and_folders()

        emitted_model_ids = set()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()
        logger.info(f"Found {len(syncs)} syncs")

        filtered_syncs = [
            sync for sync in syncs if self.config.sync_patterns.allowed(sync.slug)
        ]

        logger.info(f"Processing {len(filtered_syncs)} syncs after filtering")

        all_models = []
        if self.config.emit_models_as_datasets:
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            logger.info(f"Found {len(all_models)} models total")

        if self.graph and (filtered_syncs or all_models):
            self._preload_schemas_for_sql_parsing(all_models, filtered_syncs)

        for sync in filtered_syncs:
            try:
                if self.config.emit_models_as_datasets:
                    emitted_model_ids.add(sync.model_id)

                yield from self._get_sync_workunits(sync)
            except Exception as e:
                self.report.warning(
                    title="Failed to process sync",
                    message=f"An error occurred while processing sync: {str(e)}",
                    context=f"sync_slug: {sync.slug} (sync_id: {sync.id})",
                    exc=e,
                )

        if self.config.emit_models_as_datasets and all_models:
            logger.info("Processing standalone models")

            standalone_models = [
                model
                for model in all_models
                if model.id not in emitted_model_ids
                and self.config.model_patterns.allowed(model.name)
            ]

            logger.info(
                f"Processing {len(standalone_models)} standalone models after filtering"
            )

            for model in standalone_models:
                try:
                    yield from self._get_model_workunits(model)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process model",
                        message=f"An error occurred while processing model: {str(e)}",
                        context=f"model_name: {model.name} (model_id: {model.id})",
                        exc=e,
                    )

        if self.config.include_contracts:
            logger.info("Fetching event contracts")
            self.report.report_api_call()
            contracts = self.api_client.get_contracts()
            logger.info(f"Found {len(contracts)} contracts")

            yield from self._assertions_handler.get_assertion_workunits(
                contracts=contracts
            )

        if self._destination_lineage:
            logger.info(
                f"Emitting consolidated lineage for {len(self._destination_lineage)} destination(s)"
            )
            yield from self._lineage_handler.emit_all_destination_lineage()

        if self._sql_aggregators:
            active_aggregators = {
                platform: aggregator
                for platform, aggregator in self._sql_aggregators.items()
                if aggregator is not None
            }

            if active_aggregators:
                logger.info(
                    f"Generating lineage from {len(active_aggregators)} SQL aggregator(s)"
                )
                for platform, aggregator in active_aggregators.items():
                    logger.info(f"Generating lineage from {platform} aggregator")
                    try:
                        for mcp in aggregator.gen_metadata():
                            yield mcp.as_workunit()
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate metadata from {platform} aggregator: {e}",
                            exc_info=True,
                        )
                    finally:
                        try:
                            aggregator.close()
                        except Exception as e:
                            logger.debug(f"Error closing {platform} aggregator: {e}")
            else:
                logger.info(
                    "No active SQL aggregators - SQL-based lineage enrichment was not available. "
                    "Basic known lineage was still emitted."
                )
        else:
            logger.debug("No SQL aggregators created - SQL parsing was not used")

    def get_report(self) -> SourceReport:
        return self.report
