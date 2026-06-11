"""Microsoft Fabric OneLake ingestion source for DataHub.

This connector extracts metadata from Microsoft Fabric OneLake including:
- Workspaces as Containers
- Lakehouses as Containers
- Warehouses as Containers
- Schemas as Containers
- Tables as Datasets with schema metadata
- Views as Datasets with view definition and lineage parsed from the view SQL
"""

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Iterable, Literal, Optional, Union

from typing_extensions import assert_never

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.schema_client import (
        SchemaExtractionClient,
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    GenericContainerSubTypes,
)
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.models import WorkspaceKey
from datahub.ingestion.source.fabric.common.urn_generator import (
    make_lakehouse_name,
    make_schema_name,
    make_table_name,
    make_warehouse_name,
    make_workspace_name,
)
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
    FabricWorkspace,
)
from datahub.ingestion.source.fabric.onelake.report import (
    FabricOneLakeClientReport,
    FabricOneLakeSourceReport,
)
from datahub.ingestion.source.fabric.onelake.usage import FabricUsageExtractor
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
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
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Extracted from view definitions via SQL parsing when `extract_views` is enabled",
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
        )
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

    def _norm(self, name: str) -> str:
        # Lowercase identifiers used in URNs and schema field paths so they match
        # what SqlParsingAggregator (sqlglot) emits for view lineage. Display
        # names keep their original case for the UI.
        return name.lower() if self.config.convert_urns_to_lowercase else name

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

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
                    # Create workspace container
                    yield from self._create_workspace_container(workspace)

                    # Process items (lakehouses and warehouses)
                    yield from self._process_workspace_items(workspace)

                except Exception as e:
                    self.report.report_warning(
                        title="Failed to Process Workspace",
                        message="Error processing workspace. Skipping to next.",
                        context=f"workspace={workspace.name}",
                        exc=e,
                    )

        except Exception as e:
            self.report.report_failure(
                title="Failed to List Workspaces",
                message="Unable to retrieve workspaces from Fabric.",
                context="",
                exc=e,
            )

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
            self.report.report_failure(
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

    def _create_workspace_container(
        self, workspace: FabricWorkspace
    ) -> Iterable[Container]:
        """Create a workspace container."""
        container_key = WorkspaceKey(
            instance=self.config.platform_instance,
            env=self.config.env,
            workspace_id=workspace.id,
        )

        container = Container(
            container_key=container_key,
            display_name=workspace.name,
            description=workspace.description,
            subtype=GenericContainerSubTypes.FABRIC_WORKSPACE,
            parent_container=None,  # Workspace is root container
            qualified_name=make_workspace_name(workspace.id),
        )

        yield container

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
                self.report.report_warning(
                    title="Failed to List Lakehouses",
                    message="Unable to retrieve lakehouses from workspace.",
                    context=f"workspace={workspace.name}",
                    exc=e,
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
                self.report.report_warning(
                    title="Failed to List Warehouses",
                    message="Unable to retrieve warehouses from workspace.",
                    context=f"workspace={workspace.name}",
                    exc=e,
                )

    def _process_lakehouse(
        self, workspace: FabricWorkspace, lakehouse: FabricLakehouse
    ) -> Iterable[Union[Container, Dataset]]:
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

        # Process tables
        yield from self._process_item_tables(
            workspace,
            lakehouse.id,
            "Lakehouse",
            lakehouse_key,
            item_display_name=lakehouse.name,
            schema_map=schema_map,
            emitted_schemas=emitted_schemas,
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

    def _process_warehouse(
        self, workspace: FabricWorkspace, warehouse: FabricWarehouse
    ) -> Iterable[Union[Container, Dataset]]:
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

        # Process tables
        yield from self._process_item_tables(
            workspace,
            warehouse.id,
            "Warehouse",
            warehouse_key,
            item_display_name=warehouse.name,
            schema_map=schema_map,
            emitted_schemas=emitted_schemas,
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

    def _process_item_tables(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_container_key: ContainerKey,
        item_display_name: str,
        schema_map: dict[tuple[str, str], list[FabricColumn]],
        emitted_schemas: set[str],
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
                    yield from self._create_table_dataset(
                        workspace,
                        item_id,
                        schema_name,
                        table,
                        parent_container_key,
                        columns,
                    )

        except Exception as e:
            self.report.report_warning(
                title="Failed to Process Tables",
                message="Unable to retrieve tables from item.",
                context=f"item_id={item_id}, item_type={item_type}",
                exc=e,
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
    ) -> Iterable[Dataset]:
        """Create a table dataset with schema metadata."""
        table_name = make_table_name(
            workspace.id, item_id, self._norm(schema_name), self._norm(table.name)
        )

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
        )

        yield dataset

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
                f"{error_msg}. If enabled, column-schema, view extraction, and "
                "usage statistics will be skipped for this item.",
                exc_info=True,
            )
            self.report.report_warning(
                title="SQL Analytics Endpoint Initialization Failed",
                message=(
                    "Failed to initialize the SQL Analytics Endpoint client. "
                    "If enabled, column-schema, view extraction, and usage "
                    "statistics will be skipped for this item."
                ),
                context=f"item_id={item_id}, item_type={item_type}, error={error_msg}",
                exc=e,
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
            self.report.report_warning(
                title="Column Metadata Extraction Failed",
                message=(
                    "Failed to query INFORMATION_SCHEMA.COLUMNS. Tables and views "
                    "will be emitted without column-level schema."
                ),
                context=f"item_id={item_id}, item_type={item_type}, error={error_msg}",
                exc=e,
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
            self.report.report_warning(
                title="View Discovery Failed",
                message=(
                    "Failed to query INFORMATION_SCHEMA.VIEWS. Views will be missing for this item."
                ),
                context=f"item_id={item_id}, item_type={item_type}",
                exc=e,
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
