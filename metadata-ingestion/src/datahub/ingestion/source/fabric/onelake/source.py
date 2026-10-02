"""Microsoft Fabric OneLake ingestion source for DataHub.

This connector extracts metadata from Microsoft Fabric OneLake including:
- Workspaces as Containers
- Lakehouses as Containers
- Warehouses as Containers
- Schemas as Containers
- Tables as Datasets with schema metadata
- Views as Datasets with view definition and lineage parsed from the view SQL
- Notebooks as Datasets with subtype Notebook, including fabricGitSource contents
"""

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Iterable, Literal, Optional, Union

from typing_extensions import assert_never

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.schema_client import (
        SchemaExtractionClient,
    )

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import FabricWorkspace, WorkspaceKey
from datahub.ingestion.source.fabric.common.urn_generator import (
    make_lakehouse_name,
    make_notebook_name,
    make_schema_name,
    make_table_name,
    make_warehouse_name,
)
from datahub.ingestion.source.fabric.common.utils import build_workspace_container
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig
from datahub.ingestion.source.fabric.onelake.constants import (
    FABRIC_SQL_DEFAULT_SCHEMA,
)
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricLakehouse,
    FabricTable,
    FabricView,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.notebooks import (
    FabricNotebook,
    decode_notebook_definition,
    notebook_path,
)
from datahub.ingestion.source.fabric.onelake.profiling import (
    FabricProfileTarget,
    emit_dataset_profiles,
)
from datahub.ingestion.source.fabric.onelake.report import (
    FabricOneLakeClientReport,
    FabricOneLakeSourceReport,
)
from datahub.ingestion.source.fabric.onelake.shortcuts import (
    ORIGIN_ITEM_ID_PROPERTY,
    ORIGIN_ITEM_NAME_PROPERTY,
    ORIGIN_NAME_PROPERTY,
    ORIGIN_PATH_PROPERTY,
    ORIGIN_WORKSPACE_ID_PROPERTY,
    ORIGIN_WORKSPACE_NAME_PROPERTY,
    SHORTCUT_TAG,
    LakehouseTableShortcut,
    matching_column_pairs,
    parse_table_shortcut,
)
from datahub.ingestion.source.fabric.onelake.usage import FabricUsageExtractor
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset, parse_cll_mapping
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

# Platform identifier
PLATFORM = "fabric-onelake"


class LakehouseKey(WorkspaceKey):
    """Container key for Fabric lakehouses. Inherits from WorkspaceKey to enable parent_key() traversal."""

    platform: str = PLATFORM
    lakehouse_id: str

    def parent_key(self) -> Optional[ContainerKey]:
        if type(self) is LakehouseKey:
            # Default ContainerKey.parent_key() would preserve this key's
            # platform (`fabric-onelake`) when deriving WorkspaceKey.
            # For the lakehouse level, workspace parents must be `fabric`.
            return WorkspaceKey(
                instance=self.instance,
                env=self.env,
                workspace_id=self.workspace_id,
            )

        # For subclasses like LakehouseSchemaKey, keep standard traversal:
        # LakehouseSchemaKey -> LakehouseKey.
        return ContainerKey.parent_key(self)


class WarehouseKey(WorkspaceKey):
    """Container key for Fabric warehouses. Inherits from WorkspaceKey to enable parent_key() traversal."""

    platform: str = PLATFORM
    warehouse_id: str

    def parent_key(self) -> Optional[ContainerKey]:
        if type(self) is WarehouseKey:
            # Same rationale as LakehouseKey: workspace parents must be `fabric`,
            # not inherited as `fabric-onelake`.
            return WorkspaceKey(
                instance=self.instance,
                env=self.env,
                workspace_id=self.workspace_id,
            )

        # For subclasses like WarehouseSchemaKey, keep standard traversal:
        # WarehouseSchemaKey -> WarehouseKey.
        return ContainerKey.parent_key(self)


class LakehouseSchemaKey(LakehouseKey):
    """Container key for Fabric schemas under lakehouses. Inherits from LakehouseKey to enable parent_key() traversal."""

    schema_name: str


class WarehouseSchemaKey(WarehouseKey):
    """Container key for Fabric schemas under warehouses. Inherits from WarehouseKey to enable parent_key() traversal."""

    schema_name: str


@platform_name("Fabric OneLake")
@config_class(FabricOneLakeSourceConfig)
@support_status(SupportStatus.BETA)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via `profiling`. Uses the SQL Analytics Endpoint and the "
    "Microsoft ODBC Driver for SQL Server, the same profiler as `mssql-odbc`.",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Optionally enabled via `shortcuts.include_lineage`. A OneLake table shortcut "
    "target is emitted as an upstream of the shortcut table.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "View definitions are parsed when `extract_views` is enabled. When "
    "`shortcuts.include_lineage` is enabled and the origin table was ingested, "
    "shortcut columns are mapped onto the origin columns with the same name.",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Extracted from queryinsights.exec_requests_history (30-day retention) when "
    "`usage.include_usage_statistics` is enabled. Column-level usage is derived "
    "via SQL parsing of the query text.",
)
@capability(
    SourceCapability.OPERATION_CAPTURE,
    "Optionally enabled via `usage.include_usage_statistics` and `usage.include_operational_stats`",
)
class FabricOneLakeSource(StatefulIngestionSourceBase):
    """Extracts metadata from Microsoft Fabric OneLake."""

    config: FabricOneLakeSourceConfig
    report: FabricOneLakeSourceReport
    platform: str = PLATFORM

    def __init__(self, config: FabricOneLakeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FabricOneLakeSourceReport()

        # Initialize authentication and client
        auth_helper = FabricAuthHelper(config.credential)
        # Create client report instance that will be tracked by the client
        self.client_report = FabricOneLakeClientReport()

        # Initialize schema client if schema extraction is enabled
        self.client = OneLakeClient(
            auth_helper,
            timeout=config.api_timeout,
            report=self.client_report,
        )
        # Link client report to source report for reporting
        self.report.client_report = self.client_report

        # SQL parsing aggregator. Drives view lineage and (when usage is enabled)
        # the parsing of observed queries from queryinsights into usage and operation
        # aspects. Constructed unconditionally so view lineage works regardless of
        # the usage toggle; usage flags below decide what gets emitted.
        usage_enabled = config.usage.include_usage_statistics
        queries_enabled = usage_enabled and config.usage.include_queries
        self.aggregator = SqlParsingAggregator(
            platform=PLATFORM,
            platform_instance=config.platform_instance,
            env=config.env,
            graph=ctx.graph,
            generate_lineage=True,
            generate_queries=queries_enabled,
            generate_query_subject_fields=queries_enabled,
            generate_usage_statistics=usage_enabled,
            generate_operations=usage_enabled
            and config.usage.include_operational_stats,
            usage_config=config.usage if usage_enabled else None,
            eager_graph_load=False,
            # Query history includes the connector's own SQL (schema discovery,
            # profiling reflection). Only attach usage and lineage to datasets
            # this run actually ingested, so system objects and parser aliases
            # are not created as assets.
            is_allowed_table=self._is_usage_table_allowed,
        )
        self._ingested_dataset_names: set[str] = set()
        # dataset name (lowered) -> (name used in the URN, schema field paths)
        self._ingested_columns: dict[str, tuple[str, list[str]]] = {}
        # Shortcut datasets whose origin may be ingested later in the run.
        # Emitted after every workspace so column lineage can see that origin.
        self._deferred_shortcut_datasets: list[tuple[Dataset, list[str], str]] = []
        # Shortcut targets only carry GUIDs; display names are resolved once
        # per workspace/item and reused across all shortcuts in the run.
        self._workspace_display_names: dict[str, Optional[str]] = {}
        self._item_display_names: dict[tuple[str, str], Optional[str]] = {}
        self.report.sql_aggregator = self.aggregator.report

        # Stateful skip-handler for the usage time window. None when stateful
        # ingestion is not configured; the extractor handles that gracefully.
        self.redundant_usage_run_skip_handler: Optional[RedundantUsageRunSkipHandler]
        if config.stateful_ingestion is not None and config.stateful_ingestion.enabled:
            self.redundant_usage_run_skip_handler = RedundantUsageRunSkipHandler(
                source=self,
                config=config,
                pipeline_name=ctx.pipeline_name,
                run_id=ctx.run_id,
            )
        else:
            self.redundant_usage_run_skip_handler = None

        self.usage_extractor = FabricUsageExtractor(
            config=config.usage,
            aggregator=self.aggregator,
            report=self.report,
            redundant_run_skip_handler=self.redundant_usage_run_skip_handler,
        )

        # Resolved at the start of get_workunits_internal(); see comment there.
        self._skip_usage_run: bool = False

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FabricOneLakeSource":
        config = FabricOneLakeSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _register_ingested_dataset(self, dataset_name: str) -> None:
        # sqlglot's T-SQL dialect lowercases identifiers. Compare case-insensitively
        # so usage still attaches when convert_urns_to_lowercase is off.
        self._ingested_dataset_names.add(dataset_name.lower())

    def _is_usage_table_allowed(self, name: str) -> bool:
        return name.lower() in self._ingested_dataset_names

    def _norm(self, name: str) -> str:
        # Lowercase identifiers used in URNs and schema field paths so they match
        # what SqlParsingAggregator (sqlglot) emits for view lineage. Display
        # names keep their original case for the UI.
        return name.lower() if self.config.convert_urns_to_lowercase else name

    def get_report(self) -> FabricOneLakeSourceReport:
        """Return the ingestion report."""
        return self.report

    def close(self) -> None:
        self.client.close()
        self.aggregator.close()
        super().close()

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Generate workunits for all Fabric OneLake resources."""
        logger.info("Starting Fabric OneLake ingestion")

        # Resolve the skip-run decision once per ingestion.
        self._skip_usage_run = (
            self.config.usage.include_usage_statistics
            and self.usage_extractor.should_skip_run()
        )

        if self.config.usage.include_usage_statistics:
            if self._skip_usage_run:
                logger.info(
                    "Usage extraction skipped: configured window already covered "
                    "by a previous successful run."
                )
            else:
                logger.info(
                    f"Usage extraction enabled, window="
                    f"[{self.usage_extractor.start_time.isoformat()} -> "
                    f"{self.usage_extractor.end_time.isoformat()}]"
                )

        try:
            # List all workspaces
            workspaces = list(self.client.list_workspaces())

            for workspace in workspaces:
                self.report.report_api_call()

                # Filter workspaces
                if not self.config.workspace_pattern.allowed(workspace.name):
                    self.report.report_workspace_filtered(workspace.name)
                    continue

                self.report.report_workspace_scanned()
                logger.info(f"Processing workspace: {workspace.name} ({workspace.id})")

                try:
                    yield from build_workspace_container(
                        workspace=workspace,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    # Process items (lakehouses and warehouses)
                    yield from self._process_workspace_items(workspace)

                except Exception as e:
                    self.report.warning(
                        title="Failed to Process Workspace",
                        message="Error processing workspace. Skipping to next.",
                        context=f"workspace={workspace.name}",
                        exc=e,
                        log=False,
                    )

        except Exception as e:
            self.report.failure(
                title="Failed to List Workspaces",
                message="Unable to retrieve workspaces from Fabric.",
                context="",
                exc=e,
            )

        # Shortcut datasets wait until every table has been seen, so an origin
        # ingested later in the run can contribute column lineage.
        yield from self._emit_deferred_shortcut_datasets()

        # Drain the aggregator. Emits view lineage and (when usage is enabled)
        # datasetUsageStatistics / operation aspects. Deferred to the end so
        # cross-item view→table references resolve.
        logger.info(
            "Draining SQL aggregator (view lineage"
            f"{', usage' if self.config.usage.include_usage_statistics and not self._skip_usage_run else ''})"
        )
        aggregator_drain_succeeded = False
        emitted = 0
        try:
            for mcp in self.aggregator.gen_metadata():
                yield mcp.as_workunit()
                emitted += 1
            aggregator_drain_succeeded = True
            logger.info(f"SQL aggregator drained: emitted {emitted} MCPs")
        except Exception as e:
            self.report.failure(
                title="Failed to Generate Lineage / Usage",
                message="Error draining SQL aggregator for lineage and usage.",
                context=f"mcps_emitted_before_failure={emitted}",
                exc=e,
            )

        # Update the usage checkpoint only after a successful drain so a partial
        # run doesn't mark the window as covered.
        if (
            aggregator_drain_succeeded
            and self.config.usage.include_usage_statistics
            and not self.report.usage_run_skipped
        ):
            self.usage_extractor.update_state_on_success()

    def _process_workspace_items(
        self, workspace: FabricWorkspace
    ) -> Iterable[Union[Container, Dataset]]:
        """Process lakehouses and warehouses within a workspace."""
        # Process lakehouses
        if self.config.extract_lakehouses:
            try:
                for lakehouse in self.client.list_lakehouses(workspace.id):
                    # Filter lakehouses
                    if not self.config.lakehouse_pattern.allowed(lakehouse.name):
                        self.report.report_lakehouse_filtered(lakehouse.name)
                        continue

                    self.report.report_lakehouse_scanned()
                    logger.info(
                        f"Processing lakehouse: {lakehouse.name} ({lakehouse.id})"
                    )
                    yield from self._process_lakehouse(workspace, lakehouse)
            except Exception as e:
                self.report.warning(
                    title="Failed to List Lakehouses",
                    message="Unable to retrieve lakehouses from workspace.",
                    context=f"workspace={workspace.name}",
                    exc=e,
                    log=False,
                )

        # Process warehouses
        if self.config.extract_warehouses:
            try:
                for warehouse in self.client.list_warehouses(workspace.id):
                    # Filter warehouses
                    if not self.config.warehouse_pattern.allowed(warehouse.name):
                        self.report.report_warehouse_filtered(warehouse.name)
                        continue

                    self.report.report_warehouse_scanned()
                    logger.info(
                        f"Processing warehouse: {warehouse.name} ({warehouse.id})"
                    )
                    yield from self._process_warehouse(workspace, warehouse)
            except Exception as e:
                self.report.warning(
                    title="Failed to List Warehouses",
                    message="Unable to retrieve warehouses from workspace.",
                    context=f"workspace={workspace.name}",
                    exc=e,
                    log=False,
                )

        yield from self._process_notebooks(workspace)

    def _process_notebooks(self, workspace: FabricWorkspace) -> Iterable[Dataset]:
        """Ingest workspace notebooks as datasets with subtype Notebook."""
        if not self.config.include_notebooks:
            return

        folders: dict[str, tuple[str, Optional[str]]] = {}
        try:
            folders = self.client.list_folders(workspace.id)
        except Exception as e:
            self.report.warning(
                title="Failed to List Notebook Folders",
                message=(
                    "Unable to resolve notebook folder paths. "
                    "notebook_pattern will be applied to /<display name>."
                ),
                context=f"workspace={workspace.name}",
                exc=e,
                log=False,
            )

        try:
            notebooks = list(self.client.list_notebooks(workspace.id))
        except Exception as e:
            self.report.warning(
                title="Failed to List Notebooks",
                message="Unable to retrieve notebooks from workspace.",
                context=f"workspace={workspace.name}",
                exc=e,
                log=False,
            )
            return

        for notebook in notebooks:
            path = notebook_path(notebook.display_name, notebook.folder_id, folders)
            if not self.config.notebook_pattern.allowed(path):
                self.report.report_notebook_filtered(path)
                continue

            content: Optional[str] = None
            language: Optional[str] = None
            try:
                definition = self.client.get_notebook_definition(
                    workspace.id, notebook.id
                )
                content, language = decode_notebook_definition(definition)
            except Exception as e:
                self.report.warning(
                    title="Failed to Get Notebook Definition",
                    message="Notebook metadata will be ingested without contents.",
                    context=f"workspace={workspace.name}, notebook={path}",
                    exc=e,
                    log=False,
                )

            self.report.report_notebook_scanned()
            logger.info(f"Processing notebook: {path} ({notebook.id})")
            yield self._create_notebook_dataset(
                workspace, notebook, path, content, language
            )

    def _create_notebook_dataset(
        self,
        workspace: FabricWorkspace,
        notebook: FabricNotebook,
        path: str,
        content: Optional[str],
        language: Optional[str],
    ) -> Dataset:
        """Create a notebook dataset, including decoded fabricGitSource contents."""
        custom_properties = {"path": path}
        if language:
            custom_properties["language"] = language
        if content:
            custom_properties["content"] = content

        return Dataset(
            platform=PLATFORM,
            name=make_notebook_name(workspace.id, notebook.id),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=notebook.display_name,
            description=notebook.description,
            parent_container=WorkspaceKey(
                instance=self.config.platform_instance,
                env=self.config.env,
                workspace_id=workspace.id,
            ),
            subtype=DatasetSubTypes.NOTEBOOK,
            external_url=(
                f"{FABRIC_APP_BASE_URL}/groups/{workspace.id}"
                f"/synapsenotebooks/{notebook.id}"
            ),
            custom_properties=custom_properties,
        )

    def _process_lakehouse(
        self, workspace: FabricWorkspace, lakehouse: FabricLakehouse
    ) -> Iterable[Union[Container, Dataset, MetadataWorkUnit]]:
        """Process a lakehouse and its tables and views."""
        # Create lakehouse container
        lakehouse_key = LakehouseKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            workspace_id=workspace.id,
            lakehouse_id=lakehouse.id,
        )

        lakehouse_container = Container(
            container_key=lakehouse_key,
            display_name=lakehouse.name,
            description=lakehouse.description,
            subtype=DatasetContainerSubTypes.FABRIC_LAKEHOUSE,
            parent_container=lakehouse_key.parent_key(),
            qualified_name=make_lakehouse_name(workspace.id, lakehouse.id),
        )

        yield lakehouse_container

        schema_client = self._create_schema_client(
            workspace, lakehouse.id, "Lakehouse", lakehouse.name
        )
        schema_map = self._fetch_schema_map(
            schema_client, workspace, lakehouse.id, "Lakehouse"
        )

        # Track emitted schema containers to avoid duplicates
        emitted_schemas: set[str] = set()

        profile_targets: list[FabricProfileTarget] = []
        shortcuts = self._load_lakehouse_shortcuts(workspace.id, lakehouse.id)
        # Process tables
        yield from self._process_item_tables(
            workspace,
            lakehouse.id,
            "Lakehouse",
            lakehouse_key,
            item_display_name=lakehouse.name,
            schema_map=schema_map,
            emitted_schemas=emitted_schemas,
            profile_targets=profile_targets,
            shortcuts=shortcuts,
        )

        # Process views (requires SQL endpoint)
        if self.config.extract_views and schema_client is not None:
            yield from self._process_item_views(
                workspace,
                lakehouse.id,
                "Lakehouse",
                lakehouse_key,
                schema_client=schema_client,
                schema_map=schema_map,
                emitted_schemas=emitted_schemas,
            )

        self._extract_item_usage(
            workspace.id, lakehouse.id, lakehouse.name, schema_client
        )
        yield from self._profile_item(
            workspace.id, lakehouse.id, schema_client, profile_targets
        )

    def _process_warehouse(
        self, workspace: FabricWorkspace, warehouse: FabricWarehouse
    ) -> Iterable[Union[Container, Dataset, MetadataWorkUnit]]:
        """Process a warehouse and its tables and views."""
        # Create warehouse container
        warehouse_key = WarehouseKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            workspace_id=workspace.id,
            warehouse_id=warehouse.id,
        )

        warehouse_container = Container(
            container_key=warehouse_key,
            display_name=warehouse.name,
            description=warehouse.description,
            subtype=DatasetContainerSubTypes.FABRIC_WAREHOUSE,
            parent_container=warehouse_key.parent_key(),
            qualified_name=make_warehouse_name(workspace.id, warehouse.id),
        )

        yield warehouse_container

        schema_client = self._create_schema_client(
            workspace, warehouse.id, "Warehouse", warehouse.name
        )
        schema_map = self._fetch_schema_map(
            schema_client, workspace, warehouse.id, "Warehouse"
        )

        # Track emitted schema containers to avoid duplicates
        emitted_schemas: set[str] = set()

        profile_targets: list[FabricProfileTarget] = []
        # Process tables
        yield from self._process_item_tables(
            workspace,
            warehouse.id,
            "Warehouse",
            warehouse_key,
            item_display_name=warehouse.name,
            schema_map=schema_map,
            emitted_schemas=emitted_schemas,
            profile_targets=profile_targets,
        )

        # Process views (requires SQL endpoint)
        if self.config.extract_views and schema_client is not None:
            yield from self._process_item_views(
                workspace,
                warehouse.id,
                "Warehouse",
                warehouse_key,
                schema_client=schema_client,
                schema_map=schema_map,
                emitted_schemas=emitted_schemas,
            )

        self._extract_item_usage(
            workspace.id, warehouse.id, warehouse.name, schema_client
        )
        yield from self._profile_item(
            workspace.id, warehouse.id, schema_client, profile_targets
        )

    def _extract_item_usage(
        self,
        workspace_id: str,
        item_id: str,
        item_display_name: str,
        schema_client: Optional["SchemaExtractionClient"],
    ) -> None:
        """Stream queryinsights rows for one item into the aggregator.

        No-op when usage is disabled, when this run was already covered by a
        previous successful run, or when schema extraction failed for this item
        (we share the same SQL endpoint connection). Per-item extraction
        failures are caught inside the extractor.
        """
        if not self.config.usage.include_usage_statistics:
            return
        if self._skip_usage_run:
            return
        if schema_client is None:
            self.report.report_usage_query_skipped("no_sql_endpoint_for_item")
            logger.info(
                f"Skipping usage extraction for item {item_id} "
                f"({item_display_name}): SQL Analytics Endpoint unavailable."
            )
            return
        self.usage_extractor.extract(
            workspace_id=workspace_id,
            item_id=item_id,
            item_display_name=item_display_name,
            schema_client=schema_client,
        )

    def _profile_item(
        self,
        workspace_id: str,
        item_id: str,
        schema_client: Optional["SchemaExtractionClient"],
        profile_targets: list[FabricProfileTarget],
    ) -> Iterable[MetadataWorkUnit]:
        """Profile ingested tables on this item's SQL Analytics Endpoint."""
        if not self.config.is_profiling_enabled() or not profile_targets:
            return
        if schema_client is None:
            self.report.warning(
                title="Profiling Skipped",
                message=(
                    "SQL Analytics Endpoint is unavailable, so table and column "
                    "profiling was skipped for this item."
                ),
                context=f"item_id={item_id}",
            )
            return
        try:
            engine = schema_client.get_engine(workspace_id, item_id)
        except Exception as e:
            self.report.warning(
                title="Profiling Skipped",
                message="Failed to open the SQL Analytics Endpoint for profiling.",
                context=f"item_id={item_id}, error={e}",
                exc=e,
            )
            return
        yield from emit_dataset_profiles(
            engine=engine,
            report=self.report,
            profiling=self.config.profiling,
            profile_pattern=self.config.profile_pattern,
            targets=profile_targets,
            platform=PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            field_path_transform=self._norm,
        )

    def _process_item_tables(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_container_key: ContainerKey,
        item_display_name: str,
        schema_map: dict[tuple[str, str], list[FabricColumn]],
        emitted_schemas: set[str],
        profile_targets: list[FabricProfileTarget],
        shortcuts: Optional[dict[tuple[str, str], LakehouseTableShortcut]] = None,
    ) -> Iterable[Union[Container, Dataset]]:
        """Process tables in a lakehouse or warehouse."""
        try:
            # List tables
            if item_type == "Lakehouse":
                tables = list(self.client.list_lakehouse_tables(workspace.id, item_id))
            else:
                tables = list(self.client.list_warehouse_tables(workspace.id, item_id))

            # Group tables by schema
            tables_by_schema: dict[str, list[FabricTable]] = defaultdict(list)

            for table in tables:
                normalized_schema = (
                    table.schema_name
                    if table.schema_name
                    else FABRIC_SQL_DEFAULT_SCHEMA
                )

                # Filter schemas
                if not self.config.schema_pattern.allowed(normalized_schema):
                    self.report.report_schema_filtered(normalized_schema)
                    continue

                # Filter tables
                table_full_name = f"{normalized_schema}.{table.name}"

                if not self.config.table_pattern.allowed(table_full_name):
                    self.report.report_table_filtered(table_full_name)
                    continue

                self.report.report_table_scanned()
                tables_by_schema[normalized_schema].append(table)

            # Process each schema
            for schema_name, schema_tables in tables_by_schema.items():
                # Schema name as it goes into URNs (lowercased when configured),
                # vs. the original case kept for display and schema_map lookups.
                schema_urn_name = self._norm(schema_name)
                parent_container_key: ContainerKey
                if self.config.extract_schemas:
                    schema_key = self._make_schema_key(
                        workspace.id, item_id, item_type, schema_urn_name
                    )
                    if schema_urn_name not in emitted_schemas:
                        yield Container(
                            container_key=schema_key,
                            display_name=schema_name,
                            subtype=DatasetContainerSubTypes.FABRIC_SCHEMA,
                            parent_container=schema_key.parent_key(),
                            qualified_name=make_schema_name(
                                workspace.id, item_id, schema_urn_name
                            ),
                        )
                        emitted_schemas.add(schema_urn_name)
                        self.report.report_schema_scanned()
                    parent_container_key = schema_key
                else:
                    parent_container_key = item_container_key

                # Create table datasets
                for table in schema_tables:
                    columns = self._get_columns(schema_map, schema_name, table.name)
                    if self.config.is_profiling_enabled():
                        profile_targets.append(
                            FabricProfileTarget(
                                schema_name=schema_name,
                                table_name=table.name,
                                dataset_name=make_table_name(
                                    workspace.id,
                                    item_id,
                                    schema_urn_name,
                                    self._norm(table.name),
                                ),
                            )
                        )
                    shortcut = None
                    if shortcuts:
                        shortcut = shortcuts.get(
                            (schema_name.lower(), table.name.lower())
                        )
                    yield from self._create_table_dataset(
                        workspace,
                        item_id,
                        schema_name,
                        table,
                        parent_container_key,
                        columns,
                        shortcut=shortcut,
                    )

        except Exception as e:
            self.report.warning(
                title="Failed to Process Tables",
                message="Unable to retrieve tables from item.",
                context=f"item_id={item_id}, item_type={item_type}",
                exc=e,
                log=False,
            )

    def _get_columns(
        self,
        schema_map: dict[tuple[str, str], list[FabricColumn]],
        schema_name: str,
        table_name: str,
    ) -> list[FabricColumn]:
        """Get columns for a table from schema_map.

        Args:
            schema_map: Dictionary mapping (schema_name, table_name) to list of columns
            schema_name: Schema name (always non-empty, defaults to FABRIC_SQL_DEFAULT_SCHEMA for schemas-disabled lakehouses)
            table_name: Table name

        Returns:
            List of FabricColumn objects, or empty list if not found
        """
        if not schema_map:
            return []

        columns = schema_map.get((schema_name, table_name), [])

        if logger.isEnabledFor(logging.DEBUG):
            available_schemas = {schema for schema, _ in schema_map}
            logger.debug(
                f"Schema matching for table '{table_name}': "
                f"expected_schema='{schema_name}', "
                f"available_schemas_in_map={sorted(available_schemas)}, "
                f"tried_key=('{schema_name}', '{table_name}'), "
                f"found_columns={len(columns) if columns else 0}"
            )

        return columns

    def _create_table_dataset(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        schema_name: str,
        table: FabricTable,
        parent_container_key: ContainerKey,
        columns: list[FabricColumn],
        shortcut: Optional[LakehouseTableShortcut] = None,
    ) -> Iterable[Dataset]:
        """Create a table dataset with schema metadata."""
        table_name = make_table_name(
            workspace.id, item_id, self._norm(schema_name), self._norm(table.name)
        )
        self._register_ingested_dataset(table_name)

        # Build schema fields if available
        # Dataset SDK will automatically convert SQL Server types to DataHub types
        # using resolve_sql_type() from sql_types.py
        schema_fields = None
        if columns:
            # Schema is a list of tuples: (name, type) or (name, type, description)
            # Dataset SDK will handle type conversion via resolve_sql_type()
            schema_fields = [
                (
                    self._norm(col.name),
                    col.data_type,  # Raw SQL Server type string (e.g., "varchar", "int")
                    col.description or "",
                )
                for col in columns
            ]
        else:
            # No schema available - tables will be ingested without column metadata
            logger.debug(
                f"No schema metadata available for table {schema_name}.{table.name}"
            )
        field_paths = [field[0] for field in schema_fields] if schema_fields else []
        self._ingested_columns[table_name.lower()] = (table_name, field_paths)

        tags = None
        custom_properties = None
        upstreams = None
        if shortcut is not None:
            tags = [make_tag_urn(SHORTCUT_TAG)]
            custom_properties = self._shortcut_properties(shortcut)
            upstreams = self._shortcut_upstream(shortcut)
            self.report.shortcuts_found += 1

        dataset = Dataset(
            platform=PLATFORM,
            name=table_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=table.description,
            display_name=table.name,
            parent_container=parent_container_key,
            schema=schema_fields,
            subtype=DatasetSubTypes.TABLE,
            tags=tags,
            custom_properties=custom_properties,
            upstreams=upstreams,
        )

        upstream_name = (
            self._shortcut_upstream_dataset_name(shortcut) if shortcut else None
        )
        if upstreams is not None and upstream_name is not None:
            # Hold the dataset until the origin table may have been ingested.
            self._deferred_shortcut_datasets.append(
                (dataset, field_paths, upstream_name)
            )
            return

        yield dataset

    def _load_lakehouse_shortcuts(
        self, workspace_id: str, lakehouse_id: str
    ) -> dict[tuple[str, str], LakehouseTableShortcut]:
        """Map `(schema, table)` to shortcut metadata for one lakehouse."""
        if not self.config.shortcuts.enabled:
            return {}
        try:
            payloads = list(self.client.list_shortcuts(workspace_id, lakehouse_id))
        except Exception as e:
            self.report.warning(
                title="Failed to List Shortcuts",
                message=(
                    "Unable to list OneLake shortcuts for this lakehouse. "
                    "Tables will be ingested without shortcut tags."
                ),
                context=f"lakehouse_id={lakehouse_id}",
                exc=e,
                log=False,
            )
            return {}

        shortcuts: dict[tuple[str, str], LakehouseTableShortcut] = {}
        for payload in payloads:
            parsed = parse_table_shortcut(payload)
            if parsed is None:
                continue
            shortcuts[parsed.lookup_key()] = parsed
        logger.info(
            f"Found {len(shortcuts)} table shortcut(s) in lakehouse {lakehouse_id}"
        )
        return shortcuts

    def _shortcut_properties(self, shortcut: LakehouseTableShortcut) -> dict[str, str]:
        props: dict[str, str] = {}
        if shortcut.origin_name:
            props[ORIGIN_NAME_PROPERTY] = shortcut.origin_name
        if shortcut.origin_path:
            props[ORIGIN_PATH_PROPERTY] = shortcut.origin_path

        workspace_id = shortcut.upstream_workspace_id
        item_id = shortcut.upstream_item_id
        if workspace_id:
            props[ORIGIN_WORKSPACE_ID_PROPERTY] = workspace_id
            workspace_name = self._resolve_workspace_display_name(workspace_id)
            if workspace_name:
                props[ORIGIN_WORKSPACE_NAME_PROPERTY] = workspace_name
        if workspace_id and item_id:
            props[ORIGIN_ITEM_ID_PROPERTY] = item_id
            item_name = self._resolve_item_display_name(workspace_id, item_id)
            if item_name:
                props[ORIGIN_ITEM_NAME_PROPERTY] = item_name
        return props

    def _resolve_workspace_display_name(self, workspace_id: str) -> Optional[str]:
        if workspace_id not in self._workspace_display_names:
            self._workspace_display_names[workspace_id] = (
                self.client.get_workspace_display_name(workspace_id)
            )
        return self._workspace_display_names[workspace_id]

    def _resolve_item_display_name(
        self, workspace_id: str, item_id: str
    ) -> Optional[str]:
        key = (workspace_id, item_id)
        if key not in self._item_display_names:
            self._item_display_names[key] = self.client.get_item_display_name(
                workspace_id, item_id
            )
        return self._item_display_names[key]

    def _shortcut_upstream_dataset_name(
        self, shortcut: LakehouseTableShortcut
    ) -> Optional[str]:
        if (
            not shortcut.upstream_workspace_id
            or not shortcut.upstream_item_id
            or not shortcut.upstream_schema_name
            or not shortcut.upstream_table_name
        ):
            return None
        return make_table_name(
            shortcut.upstream_workspace_id,
            shortcut.upstream_item_id,
            self._norm(shortcut.upstream_schema_name),
            self._norm(shortcut.upstream_table_name),
        )

    def _shortcut_upstream(
        self, shortcut: LakehouseTableShortcut
    ) -> Optional[UpstreamLineageClass]:
        if not self.config.shortcuts.include_lineage:
            return None
        dataset_name = self._shortcut_upstream_dataset_name(shortcut)
        if dataset_name is None:
            return None
        return self._copy_lineage(dataset_name, column_pairs=None)

    def _copy_lineage(
        self,
        upstream_dataset_name: str,
        column_pairs: Optional[list[tuple[str, str]]],
        downstream_urn: Optional[str] = None,
    ) -> UpstreamLineageClass:
        dataset_urn = make_dataset_urn_with_platform_instance(
            PLATFORM,
            upstream_dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        fine_grained = None
        if column_pairs and downstream_urn is not None:
            fine_grained = parse_cll_mapping(
                upstream=dataset_urn,
                downstream=downstream_urn,
                cll_mapping={
                    downstream_field: [upstream_field]
                    for downstream_field, upstream_field in column_pairs
                },
            )
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=dataset_urn,
                    type=DatasetLineageTypeClass.COPY,
                )
            ],
            fineGrainedLineages=fine_grained,
        )

    def _emit_deferred_shortcut_datasets(self) -> Iterable[Dataset]:
        """Yield shortcut datasets once their origin table may have been ingested.

        Column lineage is added only when this run ingested the origin and both
        tables have schema. Table-level lineage is kept either way.
        """
        column_lineage_count = 0
        for (
            dataset,
            downstream_fields,
            upstream_name,
        ) in self._deferred_shortcut_datasets:
            ingested = self._ingested_columns.get(upstream_name.lower())
            if ingested is not None:
                canonical_name, upstream_fields = ingested
                pairs = matching_column_pairs(downstream_fields, upstream_fields)
                dataset.set_upstreams(
                    self._copy_lineage(
                        canonical_name,
                        pairs or None,
                        downstream_urn=str(dataset.urn),
                    )
                )
                if pairs:
                    column_lineage_count += 1
            yield dataset
        if column_lineage_count:
            logger.info(
                f"Added column lineage for {column_lineage_count} shortcut table(s)"
            )

    def _create_schema_client(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_display_name: str,
    ) -> Optional["SchemaExtractionClient"]:
        """Create a SQL Analytics Endpoint client, shared by column-schema,
        view extraction, and usage statistics. Returns None on failure; all
        three features skip this item.
        """
        needs_endpoint = (
            self.config.extract_schema.enabled
            or self.config.extract_views
            or self.config.usage.include_usage_statistics
            or self.config.is_profiling_enabled()
        )
        if not (needs_endpoint and self.config.sql_endpoint):
            return None

        try:
            from datahub.ingestion.source.fabric.onelake.schema_client import (
                SchemaExtractionClient,
                create_schema_extraction_client,
            )

            client: SchemaExtractionClient = create_schema_extraction_client(
                method=self.config.extract_schema.method,
                auth_helper=self.client.auth_helper,
                config=self.config.sql_endpoint,
                report=self.report.schema_report,
                workspace_id=workspace.id,
                item_id=item_id,
                item_type=item_type,
                base_client=self.client,
                item_display_name=item_display_name,
            )
            return client
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                f"Failed to initialize SQL Analytics Endpoint for item {item_id}: "
                f"{error_msg}. If enabled, column-schema, view extraction, "
                "usage statistics, and profiling will be skipped for this item.",
                exc_info=True,
            )
            self.report.warning(
                title="SQL Analytics Endpoint Initialization Failed",
                message=(
                    "Failed to initialize the SQL Analytics Endpoint client. "
                    "If enabled, column-schema, view extraction, usage "
                    "statistics, and profiling will be skipped for this item."
                ),
                context=f"item_id={item_id}, item_type={item_type}, error={error_msg}",
                exc=e,
                log=False,
            )
            return None

    def _fetch_schema_map(
        self,
        schema_client: Optional["SchemaExtractionClient"],
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
    ) -> dict[tuple[str, str], list[FabricColumn]]:
        """Fetch column metadata for all tables/views in the item. Failure only
        affects column-level schema; view discovery is unaffected.
        """
        if schema_client is None or not self.config.extract_schema.enabled:
            return {}

        try:
            return schema_client.get_all_table_columns(
                workspace_id=workspace.id,
                item_id=item_id,
            )
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                f"Failed to fetch column metadata for item {item_id}: {error_msg}. "
                "Tables and views will be emitted without column-level schema.",
                exc_info=True,
            )
            self.report.warning(
                title="Column Metadata Extraction Failed",
                message=(
                    "Failed to query INFORMATION_SCHEMA.COLUMNS. Tables and views "
                    "will be emitted without column-level schema."
                ),
                context=f"item_id={item_id}, item_type={item_type}, error={error_msg}",
                exc=e,
                log=False,
            )
            return {}

    def _make_schema_key(
        self,
        workspace_id: str,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        schema_name: str,
    ) -> Union[LakehouseSchemaKey, WarehouseSchemaKey]:
        """Create a schema container key for the given item type."""
        if item_type == "Lakehouse":
            return LakehouseSchemaKey(
                platform=PLATFORM,
                instance=self.config.platform_instance,
                env=self.config.env,
                workspace_id=workspace_id,
                lakehouse_id=item_id,
                schema_name=schema_name,
            )
        elif item_type == "Warehouse":
            return WarehouseSchemaKey(
                platform=PLATFORM,
                instance=self.config.platform_instance,
                env=self.config.env,
                workspace_id=workspace_id,
                warehouse_id=item_id,
                schema_name=schema_name,
            )
        else:
            assert_never(item_type)

    def _process_item_views(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_container_key: ContainerKey,
        schema_client: "SchemaExtractionClient",
        schema_map: dict[tuple[str, str], list[FabricColumn]],
        emitted_schemas: set[str],
    ) -> Iterable[Union[Container, Dataset]]:
        """Process views in a lakehouse or warehouse.

        Views are discovered via INFORMATION_SCHEMA.VIEWS on the SQL Analytics Endpoint.
        Column metadata comes from the shared schema_map (INFORMATION_SCHEMA.COLUMNS
        already includes view columns).
        """
        try:
            views = schema_client.get_all_views(
                workspace_id=workspace.id,
                item_id=item_id,
            )
        except Exception as e:
            logger.warning(
                f"Failed to discover views for item {item_id}: {e}. "
                "Views will be missing for this item.",
                exc_info=True,
            )
            self.report.warning(
                title="View Discovery Failed",
                message=(
                    "Failed to query INFORMATION_SCHEMA.VIEWS. Views will be missing for this item."
                ),
                context=f"item_id={item_id}, item_type={item_type}",
                exc=e,
                log=False,
            )
            return

        if not views:
            logger.debug(f"No views found in {item_type} {item_id}")
            return

        views_by_schema: dict[str, list[FabricView]] = defaultdict(list)
        for view in views:
            normalized_schema = (
                view.schema_name if view.schema_name else FABRIC_SQL_DEFAULT_SCHEMA
            )

            # Filter schemas
            if not self.config.schema_pattern.allowed(normalized_schema):
                self.report.report_schema_filtered(normalized_schema)
                continue

            view_full_name = f"{normalized_schema}.{view.name}"
            if not self.config.view_pattern.allowed(view_full_name):
                self.report.report_view_filtered(view_full_name)
                continue

            self.report.report_view_scanned()
            views_by_schema[normalized_schema].append(view)

        for schema_name, schema_views in views_by_schema.items():
            schema_urn_name = self._norm(schema_name)
            parent_container_key: ContainerKey
            if self.config.extract_schemas:
                schema_key = self._make_schema_key(
                    workspace.id, item_id, item_type, schema_urn_name
                )
                if schema_urn_name not in emitted_schemas:
                    yield Container(
                        container_key=schema_key,
                        display_name=schema_name,
                        subtype=DatasetContainerSubTypes.FABRIC_SCHEMA,
                        parent_container=schema_key.parent_key(),
                        qualified_name=make_schema_name(
                            workspace.id, item_id, schema_urn_name
                        ),
                    )
                    emitted_schemas.add(schema_urn_name)
                    self.report.report_schema_scanned()
                parent_container_key = schema_key
            else:
                parent_container_key = item_container_key

            for view in schema_views:
                columns = self._get_columns(schema_map, schema_name, view.name)
                yield from self._create_view_dataset(
                    workspace,
                    item_id,
                    schema_name,
                    view,
                    parent_container_key,
                    columns,
                )

    def _create_view_dataset(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        schema_name: str,
        view: FabricView,
        parent_container_key: ContainerKey,
        columns: list[FabricColumn],
    ) -> Iterable[Dataset]:
        """Create a view dataset with schema metadata and view definition."""
        # Views use the same URN pattern as tables
        view_name = make_table_name(
            workspace.id, item_id, self._norm(schema_name), self._norm(view.name)
        )
        self._register_ingested_dataset(view_name)

        schema_fields = None
        if columns:
            schema_fields = [
                (self._norm(col.name), col.data_type, col.description or "")
                for col in columns
            ]
        else:
            logger.debug(
                f"No schema metadata available for view {schema_name}.{view.name}"
            )

        dataset = Dataset(
            platform=PLATFORM,
            name=view_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=view.name,
            parent_container=parent_container_key,
            schema=schema_fields,
            subtype=DatasetSubTypes.VIEW,
            view_definition=view.view_definition,
            parse_view_lineage=False,
        )

        yield dataset

        if view.view_definition:
            self.aggregator.add_view_definition(
                view_urn=str(dataset.urn),
                view_definition=view.view_definition,
                default_db=f"{workspace.id}.{item_id}",
                default_schema=self._norm(schema_name),
            )
        else:
            self.report.report_view_missing_definition(f"{schema_name}.{view.name}")
            logger.debug(
                f"Skipping view lineage for {schema_name}.{view.name}: view_definition is unavailable."
            )
