import copy
import json
import logging
from typing import Iterable, List

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.lineage_state import DatasetLineageState
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import Workspace
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.metadata.schema_classes import (
    GenericAspectClass,
)

logger = logging.getLogger(__name__)


class PowerBIManager:
    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
        lineage_state: DatasetLineageState,
    ):
        self.azure_config = azure_config
        self.workspaces = workspaces
        self.ctx = ctx
        self.report = report
        self.lineage_state = lineage_state

    def get_powerbi_wus(self) -> Iterable[WorkUnit]:
        """Generate workunits for PowerBI dashboards"""

        pbi_config = PowerBiDashboardSourceConfig(
            tenant_id=self.azure_config.tenant_id,
            client_id=self.azure_config.client_id,
            client_secret=self.azure_config.client_secret,
            extract_app=True,
            extract_column_level_lineage=True,
            extract_dataset_schema=True,
            extract_independent_datasets=True,
            extract_ownership=True,
            workspace_id_pattern=AllowDenyPattern(allow=[".*"]),
        )

        try:
            ctx_copy = copy.deepcopy(self.ctx)
            source = PowerBiDashboardSource(config=pbi_config, ctx=ctx_copy)

            for wu in source.get_workunits():
                if isinstance(
                    wu.metadata, MetadataChangeProposalWrapper
                ) and isinstance(wu.metadata.aspect, GenericAspectClass):
                    # Parse the JSON patch operations
                    values_json = json.loads(wu.metadata.aspect.value.decode("utf-8"))
                    logger.debug(f"Processing workunit with values: {values_json}")

                    # Extract dashboard ID from the entityUrn
                    dashboard_urn = wu.metadata.entityUrn
                    dashboard_id = (
                        dashboard_urn.split("(")[-1].split(")")[0].split(",")[-1]
                    )

                    # Initialize list to store chart URNs
                    chart_urns = []

                    # Extract any title for the dashboard
                    dashboard_title = None
                    for op in values_json:
                        if op["path"] == "/title" and "value" in op:
                            dashboard_title = op["value"]
                            break

                    # Process chart relationships
                    for op in values_json:
                        if op["path"].startswith("/charts"):
                            chart_urn = op["value"]
                            chart_id = (
                                chart_urn.split("(")[-1].split(")")[0].split(",")[-1]
                            )
                            chart_urns.append(chart_urn)

                            # Register the chart in lineage state
                            self.lineage_state.register_dataset(
                                table_name=chart_id,
                                urn=chart_urn,
                                columns=[],  # Charts don't have columns
                                platform="powerbi-chart",
                                additional_info={
                                    "dashboard_id": dashboard_id,
                                    "dashboard_urn": dashboard_urn,
                                },
                            )
                            logger.debug(
                                f"Registered chart {chart_id} with URN {chart_urn}"
                            )

                    # Register the dashboard if it has charts
                    if chart_urns:
                        self.lineage_state.register_dataset(
                            table_name=dashboard_title or dashboard_id,
                            urn=dashboard_urn,
                            columns=[],  # Dashboards don't have columns
                            platform="powerbi-dashboard",
                            additional_info={
                                "charts": chart_urns,
                                "title": dashboard_title,
                            },
                        )
                        logger.debug(
                            f"Registered dashboard {dashboard_id} with {len(chart_urns)} charts"
                        )

                # Always yield the original workunit
                yield wu

        except Exception as e:
            error_msg = f"Failed to process PowerBI dashboards: {str(e)}"
            logger.error(error_msg)
            self.report.report_failure("powerbi_extraction", error_msg)
