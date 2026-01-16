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
from typing import Iterable, Literal, Optional

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
from datahub.ingestion.source.fabric.onelake.models import (
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

logger = logging.getLogger(__name__)

# Platform identifier
PLATFORM = "fabric-onelake"


class WorkspaceKey(ContainerKey):
    """Container key for Fabric workspaces."""

    workspace_id: str


class LakehouseKey(ContainerKey):
    """Container key for Fabric lakehouses."""

    workspace_id: str
    lakehouse_id: str


class WarehouseKey(ContainerKey):
    """Container key for Fabric warehouses."""

    workspace_id: str
    warehouse_id: str


class SchemaKey(ContainerKey):
    """Container key for Fabric schemas."""

    workspace_id: str
    item_id: str  # Lakehouse or Warehouse ID
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
        self.client = OneLakeClient(
            auth_helper, timeout=config.api_timeout, report=self.client_report
        )
        # Link client report to source report for reporting
        self.report.client_report = self.client_report

        # Create stale entity removal handler
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FabricOneLakeSource":
        config = FabricOneLakeSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_report(self) -> FabricOneLakeSourceReport:
        """Return the ingestion report."""
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
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
    ) -> Iterable[MetadataWorkUnit]:
        """Create a workspace container."""
        container_key = WorkspaceKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            workspace_id=workspace.id,
        )

        container = Container(
            container_key=container_key,
            display_name=workspace.name,
            description=workspace.description,
            subtype=GenericContainerSubTypes.FABRIC_WORKSPACE,
            qualified_name=make_workspace_name(workspace.id),
        )

        yield from container.as_workunits()

    def _process_workspace_items(
        self, workspace: FabricWorkspace
    ) -> Iterable[MetadataWorkUnit]:
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
    ) -> Iterable[MetadataWorkUnit]:
        """Process a lakehouse and its tables."""
        # Create lakehouse container
        workspace_key = WorkspaceKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            workspace_id=workspace.id,
        )

        lakehouse_key = LakehouseKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            workspace_id=workspace.id,
            lakehouse_id=lakehouse.id,
        )

        lakehouse_container = Container(
            container_key=lakehouse_key,
            display_name=lakehouse.name,
            description=lakehouse.description,
            subtype=DatasetContainerSubTypes.FABRIC_LAKEHOUSE,
            parent_container=workspace_key.as_urn(),
            qualified_name=make_lakehouse_name(workspace.id, lakehouse.id),
        )

        yield from lakehouse_container.as_workunits()

        # Process tables
        yield from self._process_item_tables(
            workspace, lakehouse.id, "Lakehouse", lakehouse_key
        )

    def _process_warehouse(
        self, workspace: FabricWorkspace, warehouse: FabricWarehouse
    ) -> Iterable[MetadataWorkUnit]:
        """Process a warehouse and its tables."""
        # Create warehouse container
        workspace_key = WorkspaceKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            workspace_id=workspace.id,
        )

        warehouse_key = WarehouseKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            workspace_id=workspace.id,
            warehouse_id=warehouse.id,
        )

        warehouse_container = Container(
            container_key=warehouse_key,
            display_name=warehouse.name,
            description=warehouse.description,
            subtype=DatasetContainerSubTypes.FABRIC_WAREHOUSE,
            parent_container=workspace_key.as_urn(),
            qualified_name=make_warehouse_name(workspace.id, warehouse.id),
        )

        yield from warehouse_container.as_workunits()

        # Process tables
        yield from self._process_item_tables(
            workspace, warehouse.id, "Warehouse", warehouse_key
        )

    def _process_item_tables(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        item_type: Literal["Lakehouse", "Warehouse"],
        item_container_key: ContainerKey,
    ) -> Iterable[MetadataWorkUnit]:
        """Process tables in a lakehouse or warehouse."""
        try:
            # List tables
            if item_type == "Lakehouse":
                tables = list(self.client.list_lakehouse_tables(workspace.id, item_id))
            else:
                tables = list(self.client.list_warehouse_tables(workspace.id, item_id))

            # TODO: Schema extraction is not yet implemented
            # See client.py for details on implementing SQL endpoint-based schema extraction
            schema_map: dict[tuple[str, str], list] = {}

            # Group tables by schema
            tables_by_schema: dict[str, list[FabricTable]] = defaultdict(list)

            for table in tables:
                # Filter tables
                # For schemas-disabled lakehouses, schema_name is empty, so use just table name
                table_full_name = (
                    f"{table.schema_name}.{table.name}"
                    if table.schema_name
                    else table.name
                )

                if not self.config.table_pattern.allowed(table_full_name):
                    self.report.report_table_filtered(table_full_name)
                    continue

                self.report.report_table_scanned()
                # Use empty string as key for schemas-disabled tables (schema_name is empty)
                schema_dict_key = table.schema_name if table.schema_name else ""
                tables_by_schema[schema_dict_key].append(table)

            # Process each schema
            for schema_name, schema_tables in tables_by_schema.items():
                # For schemas-disabled lakehouses, schema_name is empty string - skip schema container
                if self.config.extract_schemas and schema_name:
                    # Create schema container
                    schema_key = SchemaKey(
                        platform=PLATFORM,
                        instance=self.config.platform_instance,
                        workspace_id=workspace.id,
                        item_id=item_id,
                        schema_name=schema_name,
                    )

                    schema_container = Container(
                        container_key=schema_key,
                        display_name=schema_name,
                        subtype=DatasetContainerSubTypes.FABRIC_SCHEMA,
                        parent_container=item_container_key.as_urn(),
                        qualified_name=make_schema_name(
                            workspace.id, item_id, schema_name
                        ),
                    )

                    yield from schema_container.as_workunits()
                    self.report.report_schema_scanned()

                    parent_container_urn: str = schema_key.as_urn()
                else:
                    # Tables directly under item container (schemas-disabled or extract_schemas=false)
                    parent_container_urn = item_container_key.as_urn()

                # Create table datasets
                for table in schema_tables:
                    # TODO: Pass schema columns when schema extraction is implemented
                    columns = schema_map.get((schema_name, table.name), [])
                    yield from self._create_table_dataset(
                        workspace,
                        item_id,
                        schema_name,
                        table,
                        parent_container_urn,
                        columns,
                    )

        except Exception as e:
            self.report.report_warning(
                title="Failed to Process Tables",
                message="Unable to retrieve tables from item.",
                context=f"item_id={item_id}, item_type={item_type}",
                exc=e,
            )

    def _create_table_dataset(
        self,
        workspace: FabricWorkspace,
        item_id: str,
        schema_name: str,
        table: FabricTable,
        parent_container_urn: str,
        columns: list,  # TODO: Should be list[FabricColumn] when schema extraction is implemented
    ) -> Iterable[MetadataWorkUnit]:
        """Create a table dataset with schema metadata."""
        table_name = make_table_name(workspace.id, item_id, schema_name, table.name)

        # Build schema fields if available
        # TODO: Schema extraction is not yet implemented - see client.py for details
        schema_fields = None
        if columns:
            # Schema is a list of tuples: (name, type) or (name, type, description)
            schema_fields = [
                (
                    col.name,
                    col.data_type,
                    col.description or "",
                )
                for col in columns
            ]
            self.report.report_schema_extraction_success()
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
            parent_container=parent_container_urn,
            schema=schema_fields,
            subtype=DatasetSubTypes.TABLE,  # All Fabric tables are currently ingested as TABLE subtype
        )

        yield from dataset.as_workunits()
