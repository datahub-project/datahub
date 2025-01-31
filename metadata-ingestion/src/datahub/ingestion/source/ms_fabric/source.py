import logging
from typing import Dict, Iterable, List, Optional

import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import WorkUnit
from datahub.ingestion.source.ms_fabric.config import AzureFabricSourceConfig
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.semantic_model import SemanticModelManager
from datahub.ingestion.source.ms_fabric.types import Workspace

logger = logging.getLogger(__name__)


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

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AzureFabricSource":
        config: AzureFabricSourceConfig = AzureFabricSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[WorkUnit]:
        """Generate work units for ingesting Azure Fabric metadata"""
        workspaces = self._get_workspaces()

        # Process semantic_models
        semantic_model_manager = SemanticModelManager(
            azure_config=self.config.azure_config,
            workspaces=workspaces,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            ctx=self.ctx,
            report=self.report,
        )
        for w in semantic_model_manager.get_semantic_model_wus():
            logger.error(w)
            yield w

        # # Process Warehouses
        # warehouse_manager = WarehouseManager(
        #     azure_config=self.config.azure_config,
        #     workspaces=workspaces,
        #     ctx=self.ctx,
        #     report=self.report,
        # )
        # yield from warehouse_manager.get_warehouse_wus()
        #
        # # Process Mirrored Databases
        # mirrored_databases_manager = MirroredDatabasesManager(
        #     azure_config=self.config.azure_config,
        #     workspaces=workspaces,
        #     ctx=self.ctx,
        #     report=self.report,
        # )
        # yield from mirrored_databases_manager.get_mirrored_databases_wus()
        #
        # # Process Lakehouses
        # lakehouse_manager = LakehouseManager(
        #     azure_config=self.config.azure_config,
        #     workspaces=workspaces,
        #     ctx=self.ctx,
        #     report=self.report,
        # )
        # yield from lakehouse_manager.get_lakehouse_wus()
        #
        # # Process PowerBI Dashboards
        # powerbi_manager = PowerBIManager(
        #     azure_config=self.config.azure_config,
        #     workspaces=workspaces,
        #     ctx=self.ctx,
        #     report=self.report,
        # )
        # yield from powerbi_manager.get_powerbi_wus()

    def _get_workspaces(
        self, continuation_token: Optional[str] = None
    ) -> List[Workspace]:
        workspaces: List[Workspace] = []
        url = "https://api.fabric.microsoft.com/v1/workspaces"
        params: Dict[str, str] = {}

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.session.get(url, headers=self.session.headers, params=params)
        response.raise_for_status()

        response_data = response.json()
        if response_data:
            workspaces_data = response_data.get("value")
            continuation_token = response_data.get("continuationToken")
            if continuation_token:
                workspaces.extend(self._get_workspaces(continuation_token))

            if workspaces_data:
                workspaces.extend(
                    [Workspace.parse_obj(workspace) for workspace in workspaces_data]
                )

        return workspaces

    def get_report(self):
        """Get ingestion report"""
        return self.report
