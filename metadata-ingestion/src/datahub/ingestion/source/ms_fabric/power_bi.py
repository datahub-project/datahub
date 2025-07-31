import copy
import json
import logging
from typing import Iterable, List

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.lineage_state import DatasetLineageState
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import Workspace
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.metadata.schema_classes import (
    GenericAspectClass,
    MetadataChangeProposalClass,
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

            chart_titles = {}  # Store chart URNs and their titles

            for wu in source.get_workunits():
                if isinstance(wu.metadata, MetadataChangeProposalClass) and isinstance(
                    wu.metadata.aspect, GenericAspectClass
                ):
                    # Parse the JSON patch operations
                    values_json = json.loads(wu.metadata.aspect.value.decode("utf-8"))
                    logger.debug(f"Processing workunit with values: {values_json}")

                    dashboard_urn = wu.metadata.entityUrn
                    if "reports." in dashboard_urn:
                        report_id = dashboard_urn.split("reports.")[-1].split(")")[0]
                        chart_urns = []
                        dashboard_title = None

                        for op in values_json:
                            if op["path"] == "/title" and "value" in op:
                                dashboard_title = op["value"]
                            elif op["path"].startswith("/charts/"):
                                chart_urn = op["value"]
                                chart_urns.append(chart_urn)
                            elif op["path"].startswith("/chartInfo/") and "value" in op:
                                # Extract chart title if present
                                try:
                                    chart_info = json.loads(op["value"])
                                    if "title" in chart_info:
                                        chart_path = op["path"].split("/")[
                                            2
                                        ]  # Gets the chart identifier
                                        chart_titles[chart_path] = chart_info["title"]
                                except Exception as e:
                                    logger.warning(e)
                                    continue

                        # Only register if we have charts
                        if chart_urns:
                            # Create a mapping of charts with their titles
                            chart_info = {
                                chart_urn: chart_titles.get(
                                    chart_urn.split("pages.")[-1].split(")")[0],
                                    "Untitled Chart",
                                )
                                for chart_urn in chart_urns
                            }

                            # Register using report_id as the key
                            self.lineage_state.register_dataset(
                                table_name=report_id,
                                urn=dashboard_urn,
                                columns=[],
                                platform="powerbi-dashboard",
                                additional_info={
                                    "title": dashboard_title,
                                    "charts": chart_urns,
                                    "chart_titles": chart_info,  # Add chart titles to additional_info
                                },
                            )
                            logger.debug(
                                f"Registered dashboard for report {report_id} with charts and titles: {chart_info}"
                            )

                # Always yield the original workunit
                yield wu

        except Exception as e:
            error_msg = f"Failed to process PowerBI dashboards: {str(e)}"
            logger.error(error_msg)
            self.report.report_failure("powerbi_extraction", error_msg)
