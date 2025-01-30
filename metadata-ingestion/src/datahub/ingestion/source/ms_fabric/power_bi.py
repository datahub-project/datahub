import copy
import logging
from typing import Iterable, List

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import Workspace
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource

logger = logging.getLogger(__name__)


class PowerBIManager:
    azure_config: AzureConnectionConfig
    workspaces: List[Workspace]
    ctx: PipelineContext
    report: AzureFabricSourceReport

    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
    ):
        self.azure_config = azure_config
        self.workspaces = workspaces
        self.ctx = ctx
        self.report = report

    def get_powerbi_wus(self) -> Iterable[WorkUnit]:
        workspace_ids = [workspace.id for workspace in self.workspaces]

        pbi_config = PowerBiDashboardSourceConfig(
            tenant_id=self.azure_config.tenant_id,
            client_id=self.azure_config.client_id,
            client_secret=self.azure_config.client_secret,
            admin_apis_only=False,
            extract_app=True,
            extract_column_level_lineage=True,
            extract_dataset_schema=True,
            extract_independent_datasets=True,
            extract_ownership=True,
            workspace_id_pattern=AllowDenyPattern(allow=workspace_ids),
        )

        try:
            ctx_copy = copy.deepcopy(self.ctx)
            source = PowerBiDashboardSource(config=pbi_config, ctx=ctx_copy)
            yield from source.get_workunits()
        except Exception as e:
            logger.error(f"Failed to process PowerBI dashboards: {str(e)}")
            self.report.report_failure(
                "powerbi_extraction", f"Failed to extract PowerBI dashboards: {str(e)}"
            )
