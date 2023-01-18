import json
import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.powerbi.config import (
    PowerBiAPIConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Dashboard,
    Page,
    PowerBIDataset,
    Report,
    Table,
    User,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_fetcher import (
    AdminFetcher,
    RegularFetcher,
)

# Logger instance
logger = logging.getLogger(__name__)


class PowerBiAPI:
    def __init__(self, config: PowerBiAPIConfig) -> None:
        self.__config: PowerBiAPIConfig = config

        self.__regular_fetcher = RegularFetcher(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret,
            tenant_id=self.__config.tenant_id,
        )

        self.__admin_fetcher = AdminFetcher(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret,
            tenant_id=self.__config.tenant_id,
        )

    def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
        """
        Return list of dashboard users
        """
        if self.__config.extract_ownership is False:
            logger.info(
                "Extract ownership capabilities is disabled from configuration and hence returning empty users list"
            )
            return []

        return self.__admin_fetcher.get_users(
            workspace_id=dashboard.workspace_id,
            entity="dashboards",
            entity_id=dashboard.id,
        )

    def get_pages_by_report(self, workspace_id: str, report_id: str) -> List[Page]:
        """
        Fetch the report from PowerBi for the given report identifier
        """
        if workspace_id is None or report_id is None:
            logger.info("workspace_id or report_id is None")
            return []

        return self.__regular_fetcher.get_pages_by_report(
            workspace_id=workspace_id, report_id=report_id
        )

    def get_reports(self, workspace: Workspace) -> List[Report]:
        """
        Fetch the report from PowerBi for the given Workspace
        """
        if workspace is None:
            logger.info("workspace is None")
            return []
        reports: List[Report] = self.__regular_fetcher.get_reports(workspace)
        if self.__config.extract_ownership is True:
            for report in reports:
                report.users = self.__admin_fetcher.get_users(
                    workspace_id=workspace.id,
                    entity="reports",
                    entity_id=report.id,
                )
        return reports

    def get_workspaces(self) -> List[Workspace]:
        groups = self.__regular_fetcher.get_groups()
        workspaces = [
            Workspace(
                id=workspace.get("id"),
                name=workspace.get("name"),
                datasets={},
                dashboards=[],
            )
            for workspace in groups.get("value", [])
            if workspace.get("type", None) == "Workspace"
        ]
        return workspaces

    # flake8: noqa: C901
    def fill_workspace(
        self, workspace: Workspace, reporter: PowerBiDashboardSourceReport
    ) -> Workspace:
        def json_to_dataset_map(scan_result: dict) -> dict:
            """
            Filter out "dataset" from scan_result and return Dataset instance set
            """
            datasets: Optional[Any] = scan_result.get("datasets")
            dataset_map: dict = {}

            if datasets is None or len(datasets) == 0:
                logger.warning(
                    f'Workspace {scan_result["name"]}({scan_result["id"]}) does not have datasets'
                )

                logger.info("Returning empty datasets")
                return dataset_map

            for dataset_dict in datasets:
                dataset_instance: PowerBIDataset = self.__regular_fetcher.get_dataset(
                    workspace_id=scan_result["id"],
                    dataset_id=dataset_dict["id"],
                )
                dataset_map[dataset_instance.id] = dataset_instance
                # set dataset-name
                dataset_name: str = (
                    dataset_instance.name
                    if dataset_instance.name is not None
                    else dataset_instance.id
                )

                for table in dataset_dict["tables"]:
                    expression: str = (
                        table["source"][0]["expression"]
                        if table.get("source") is not None and len(table["source"]) > 0
                        else None
                    )
                    dataset_instance.tables.append(
                        Table(
                            name=table["name"],
                            full_name="{}.{}".format(
                                dataset_name.replace(" ", "_"),
                                table["name"].replace(" ", "_"),
                            ),
                            expression=expression,
                        )
                    )

            return dataset_map

        def init_dashboard_tiles() -> None:
            for dashboard in workspace.dashboards:
                dashboard.tiles = self.__regular_fetcher.get_tiles(
                    workspace, dashboard=dashboard
                )

        workspace.dashboards = self.__regular_fetcher.get_dashboards(workspace)
        init_dashboard_tiles()

        datasets: Dict[str, PowerBIDataset] = {}
        # Fill datasets if Admin API is enabled
        if self.__config.enable_admin_api:
            scan_id = self.__admin_fetcher.create_scan_job(workspace_id=workspace.id)
            logger.info("Waiting for scan to complete")
            if (
                self.__admin_fetcher.wait_for_scan_to_complete(
                    scan_id=scan_id, timeout=self.__config.scan_timeout
                )
                is False
            ):
                raise ValueError(
                    "Workspace detail is not available. Please increase scan_timeout to wait."
                )

            # Scan is complete lets take the result
            scan_result = self.__admin_fetcher.get_scan_result(scan_id=scan_id)
            logger.debug(f"scan result = %s", json.dumps(scan_result, indent=1))
            datasets = json_to_dataset_map(scan_result)

        workspace.datasets = datasets

        return workspace
