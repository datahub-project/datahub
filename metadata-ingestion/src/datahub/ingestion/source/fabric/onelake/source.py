"""Microsoft Fabric OneLake ingestion source for DataHub.

This connector extracts metadata from Microsoft Fabric OneLake including:
- Workspaces as Containers
- Lakehouses as Containers
- Warehouses as Containers
- Schemas as Containers
- Tables as Datasets with schema metadata
"""

import logging
from collections import defaultdict
from typing import Iterable, Literal, Optional, Union

from typing_extensions import assert_never

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
    DEFAULT_SCHEMA_SCHEMALESS_LAKEHOUSE,
)
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
    FabricWorkspace,
)
from datahub.ingestion.source.fabric.onelake.report import (
    FabricOneLakeClientReport,
    FabricOneLakeSourceReport,
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

logger = logging.getLogger(__name__)

# Platform identifier
PLATFORM = "fabric-onelake"


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces."""

    workspace_id: str


class LakehouseKey(WorkspaceKey):
    """Container key for Fabric lakehouses. Inherits from WorkspaceKey to enable parent_key() traversal."""

    lakehouse_id: str


class WarehouseKey(WorkspaceKey):
    """Container key for Fabric warehouses. Inherits from WorkspaceKey to enable parent_key() traversal."""

    warehouse_id: str


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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FabricOneLakeSource":
        config = FabricOneLakeSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

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

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Generate workunits for all Fabric OneLake resources."""
        logger.info("Starting Fabric OneLake ingestion")

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
        finally:
            self.client.close()

    def _create_workspace_container(
        self, workspace: FabricWorkspace
    ) -> Iterable[Container]:
        """Create a workspace container."""
        container_key = WorkspaceKey(
            platform=PLATFORM,
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
        """Process a lakehouse and its tables."""
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

        # Process tables
        yield from self._process_item_tables(
            workspace,
            lakehouse.id,
            "Lakehouse",
            lakehouse_key,
            item_display_name=lakehouse.name,
        )

    def _process_warehouse(
        self, workspace: FabricWorkspace, warehouse: FabricWarehouse
    ) -> Iterable[Union[Container, Dataset]]:
        """Process a warehouse and its tables."""
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

        # Process tables
        yield from self._process_item_tables(
            workspace,
            warehouse.id,
            "Warehouse",
            warehouse_key,
            item_display_name=warehouse.name,
        )

    def _process_item_tables(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_container_key: ContainerKey,
        item_display_name: str,
    ) -> Iterable[Union[Container, Dataset]]:
        """Process tables in a lakehouse or warehouse."""
        try:
            # List tables
            if item_type == "Lakehouse":
                tables = list(self.client.list_lakehouse_tables(workspace.id, item_id))
            else:
                tables = list(self.client.list_warehouse_tables(workspace.id, item_id))

            # Extract schema metadata if enabled
            # Fetch all schemas at item level for efficiency (single query per item)
            schema_map: dict[tuple[str, str], list[FabricColumn]] = {}
            if self.config.extract_schema.enabled and self.config.sql_endpoint:
                if not tables:
                    logger.info(
                        f"Skipping schema extraction for {item_type} {item_id}: "
                        "0 tables found, nothing to describe."
                    )
                else:
                    try:
                        from datahub.ingestion.source.fabric.onelake.schema_client import (
                            create_schema_extraction_client,
                        )

                        # Create schema client for this specific workspace/item
                        # (endpoint URL is item-specific, so we create it here)
                        # Use the schema report from the source report
                        schema_client = create_schema_extraction_client(
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

                        # Get all table schemas for this item in a single batch query
                        schema_map = schema_client.get_all_table_columns(
                            workspace_id=workspace.id,
                            item_id=item_id,
                        )
                    except Exception as e:
                        error_msg = str(e)
                        logger.warning(
                            f"Failed to extract schema for item {item_id}: {error_msg}. "
                            "Tables will be ingested without column metadata."
                        )
                        # Report as warning in the main report
                        self.report.report_warning(
                            title="Schema Extraction Failed",
                            message="Failed to extract schema metadata from SQL Analytics Endpoint.",
                            context=f"item_id={item_id}, item_type={item_type}, error={error_msg}",
                        )
                        # Schema extraction failed, continue without schema metadata
                        schema_map = {}

            # Group tables by schema
            tables_by_schema: dict[str, list[FabricTable]] = defaultdict(list)

            for table in tables:
                normalized_schema = (
                    table.schema_name
                    if table.schema_name
                    else DEFAULT_SCHEMA_SCHEMALESS_LAKEHOUSE
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
                if self.config.extract_schemas:
                    # Create schema container key with proper inheritance
                    if item_type == "Lakehouse":
                        schema_key: Union[LakehouseSchemaKey, WarehouseSchemaKey] = (
                            LakehouseSchemaKey(
                                platform=PLATFORM,
                                instance=self.config.platform_instance,
                                env=self.config.env,
                                workspace_id=workspace.id,
                                lakehouse_id=item_id,
                                schema_name=schema_name,
                            )
                        )
                    elif item_type == "Warehouse":
                        schema_key = WarehouseSchemaKey(
                            platform=PLATFORM,
                            instance=self.config.platform_instance,
                            env=self.config.env,
                            workspace_id=workspace.id,
                            warehouse_id=item_id,
                            schema_name=schema_name,
                        )
                    else:
                        assert_never(item_type)

                    schema_container = Container(
                        container_key=schema_key,
                        display_name=schema_name,
                        subtype=DatasetContainerSubTypes.FABRIC_SCHEMA,
                        parent_container=schema_key.parent_key(),
                        qualified_name=make_schema_name(
                            workspace.id, item_id, schema_name
                        ),
                    )

                    yield schema_container
                    self.report.report_schema_scanned()

                    # Pass ContainerKey object, not URN string
                    parent_container_key: ContainerKey = schema_key
                else:
                    # Tables directly under item container (extract_schemas=false)
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
            schema_name: Schema name (always non-empty, defaults to DEFAULT_SCHEMA_SCHEMALESS_LAKEHOUSE for schemas-disabled lakehouses)
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
        table_name = make_table_name(workspace.id, item_id, schema_name, table.name)

        # Build schema fields if available
        # Dataset SDK will automatically convert SQL Server types to DataHub types
        # using resolve_sql_type() from sql_types.py
        schema_fields = None
        if columns:
            # Schema is a list of tuples: (name, type) or (name, type, description)
            # Dataset SDK will handle type conversion via resolve_sql_type()
            schema_fields = [
                (
                    col.name,
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
