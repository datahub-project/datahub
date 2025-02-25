import logging
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import WorkUnit
from datahub.ingestion.source.ms_fabric.config import AzureFabricSourceConfig
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.items import ReportManager
from datahub.ingestion.source.ms_fabric.lakehouse import LakehouseManager
from datahub.ingestion.source.ms_fabric.lineage_state import DatasetLineageState
from datahub.ingestion.source.ms_fabric.mirrored_database import (
    MirroredDatabasesManager,
)
from datahub.ingestion.source.ms_fabric.power_bi import PowerBIManager
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.semantic_model import SemanticModelManager
from datahub.ingestion.source.ms_fabric.types import Workspace
from datahub.ingestion.source.ms_fabric.warehouse import WarehouseManager

logger = logging.getLogger(__name__)


@platform_name("Microsoft Fabric")
@config_class(AzureFabricSourceConfig)
@support_status(SupportStatus.CERTIFIED)
class AzureFabricSource(Source):
    """Azure Fabric source for DataHub metadata ingestion"""

    config: AzureFabricSourceConfig
    report: AzureFabricSourceReport

    def __init__(self, config: AzureFabricSourceConfig, ctx):
        super().__init__(ctx)
        self.ctx = ctx
        self.platform = config.platform
        self.config = config
        self.report = AzureFabricSourceReport()
        self.source_config = config
        self.session = set_session(requests.Session(), self.config.azure_config)
        self.lineage_state = DatasetLineageState()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AzureFabricSource":
        config: AzureFabricSourceConfig = AzureFabricSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[WorkUnit]:
        """Generate work units for ingesting Azure Fabric metadata"""
        workspaces = self._get_workspaces()

        # Process Warehouses
        warehouse_manager = WarehouseManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            ctx=self.ctx,
            report=self.report,
        )
        yield from warehouse_manager.get_warehouse_wus()

        # Process Mirrored Databases
        mirrored_databases_manager = MirroredDatabasesManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            ctx=self.ctx,
            report=self.report,
        )
        yield from mirrored_databases_manager.get_mirrored_databases_wus()

        # Process Lakehouses
        lakehouse_manager = LakehouseManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            ctx=self.ctx,
            report=self.report,
            lineage_state=self.lineage_state,
        )
        yield from lakehouse_manager.get_lakehouse_wus()

        # Process semantic_models
        semantic_model_manager = SemanticModelManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            ctx=self.ctx,
            report=self.report,
            lineage_state=self.lineage_state,
        )
        yield from semantic_model_manager.get_semantic_model_wus()

        # Process PowerBI Dashboards
        powerbi_manager = PowerBIManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            ctx=self.ctx,
            report=self.report,
            lineage_state=self.lineage_state,
        )
        yield from powerbi_manager.get_powerbi_wus()

        report_manager = ReportManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            ctx=self.ctx,
            report=self.report,
            lineage_state=self.lineage_state,
        )
        yield from report_manager.get_report_wus()

    def _get_workspaces(
        self, continuation_token: Optional[str] = None
    ) -> List[Workspace]:
        """Get workspaces based on configuration"""
        workspaces: List[Workspace] = []

        # If specific workspaces are configured, fetch them directly
        if self.config.workspace_config.workspaces:
            for workspace_id in self.config.workspace_config.workspaces:
                try:
                    url = (
                        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
                    )
                    response = self.session.get(url, headers=self.session.headers)
                    response.raise_for_status()
                    workspace_data = response.json()
                    workspaces.append(Workspace.parse_obj(workspace_data))
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch workspace {workspace_id}: {str(e)}"
                    )

            # If we have workspace_names, we'll need to fetch those too
            if self.config.workspace_config.workspace_names:
                workspaces.extend(self._get_workspaces_by_name())

            return workspaces

        # If only workspace names are configured
        if self.config.workspace_config.workspace_names:
            return self._get_workspaces_by_name()

        # If no specific workspaces configured, fetch all workspaces
        return self._get_all_workspaces(continuation_token)

    def _get_workspaces_by_name(self) -> List[Workspace]:
        """Get workspaces by name"""
        workspaces: List[Workspace] = []
        workspace_names = set(self.config.workspace_config.workspace_names or [])

        if not workspace_names:
            return workspaces

        def fetch_page(
            continuation_token: Optional[str] = None,
        ) -> Tuple[List[Workspace], Optional[str]]:
            url = "https://api.fabric.microsoft.com/v1/workspaces"
            params = {}
            if continuation_token:
                params["continuation_token"] = continuation_token

            response = self.session.get(
                url, headers=self.session.headers, params=params
            )
            response.raise_for_status()
            response_data = response.json()

            page_workspaces = []
            for workspace in response_data.get("value", []):
                if workspace.get("displayName") in workspace_names:
                    page_workspaces.append(Workspace.parse_obj(workspace))

            return page_workspaces, response_data.get("continuationToken")

        try:
            continuation_token = None
            found_names = set()

            while True:
                page_workspaces, continuation_token = fetch_page(continuation_token)
                workspaces.extend(page_workspaces)

                # Track which workspace names we've found
                found_names.update(w.display_name for w in page_workspaces)

                # Stop if we've found all workspace names or no more pages
                if found_names.issuperset(workspace_names) or not continuation_token:
                    break

        except Exception as e:
            logger.error(f"Failed to fetch workspaces by name: {str(e)}")

        return workspaces

    def _get_all_workspaces(
        self, continuation_token: Optional[str] = None
    ) -> List[Workspace]:
        """Get all workspaces using pagination"""
        workspaces: List[Workspace] = []
        url = "https://api.fabric.microsoft.com/v1/workspaces"
        params = {}

        if continuation_token:
            params["continuation_token"] = continuation_token

        try:
            response = self.session.get(
                url, headers=self.session.headers, params=params
            )
            response.raise_for_status()

            response_data = response.json()
            workspaces_data = response_data.get("value", [])
            next_token = response_data.get("continuationToken")

            workspaces.extend([Workspace.parse_obj(w) for w in workspaces_data])

            if next_token:
                workspaces.extend(self._get_all_workspaces(next_token))

        except Exception as e:
            logger.error(f"Failed to fetch workspaces: {str(e)}")

        return workspaces

    def get_report(self):
        return self.report
