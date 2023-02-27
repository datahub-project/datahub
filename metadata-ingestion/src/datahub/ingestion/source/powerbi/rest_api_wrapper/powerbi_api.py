import json
import logging
import sys
from typing import Any, Dict, List, Optional, cast

import requests

from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_resolver
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Dashboard,
    PowerBIDataset,
    Report,
    Table,
    User,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    RegularAPIResolver,
)

# Logger instance
logger = logging.getLogger(__name__)


class PowerBiAPI:
    def __init__(self, config: PowerBiDashboardSourceConfig) -> None:
        self.__config: PowerBiDashboardSourceConfig = config

        self.__regular_api_resolver = RegularAPIResolver(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret,
            tenant_id=self.__config.tenant_id,
        )

        self.__admin_api_resolver = AdminAPIResolver(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret,
            tenant_id=self.__config.tenant_id,
        )

    def log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")

        logger.debug(msg=message, exc_info=e)

        return e

    def _get_dashboard_endorsements(
        self, scan_result: Optional[dict]
    ) -> Dict[str, List[str]]:
        """
        Store saved dashboard endorsements into a dict with dashboard id as key and
        endorsements or tags as list of strings
        """
        results: Dict[str, List[str]] = {}
        if scan_result is None:
            return results

        for scanned_dashboard in scan_result.get(Constant.DASHBOARDS, []):
            # Iterate through response and create a list of PowerBiAPI.Dashboard
            dashboard_id = scanned_dashboard.get("id")
            tags = self._parse_endorsement(
                scanned_dashboard.get(Constant.ENDORSEMENT_DETAIL, None)
            )
            results[dashboard_id] = tags

        return results

    def _get_report_endorsements(
        self, scan_result: Optional[dict]
    ) -> Dict[str, List[str]]:
        results: Dict[str, List[str]] = {}

        if scan_result is None:
            return results

        reports: List[dict] = scan_result.get(Constant.REPORTS, [])

        for report in reports:
            report_id = report.get(Constant.ID, None)
            if report_id is None:
                logger.warning(
                    f"Report id is none. Skipping endorsement tag for report instance {report}"
                )
                continue
            endorsements = self._parse_endorsement(
                report.get(Constant.ENDORSEMENT_DETAIL, None)
            )
            results[report_id] = endorsements

        return results

    def _get_resolver(self):
        if self.__config.admin_apis_only:
            return self.__admin_api_resolver
        return self.__regular_api_resolver

    def _get_entity_users(
        self, workspace_id: str, entity_name: str, entity_id: str
    ) -> List[User]:
        """
        Return list of dashboard users
        """
        users: List[User] = []
        if self.__config.extract_ownership is False:
            logger.info(
                "Extract ownership capabilities is disabled from configuration and hence returning empty users list"
            )
            return users

        try:
            users = self.__admin_api_resolver.get_users(
                workspace_id=workspace_id,
                entity=entity_name,
                entity_id=entity_id,
            )
        except:  # It will catch all type of exception
            e = self.log_http_error(
                message=f"Unable to fetch users for {entity_name}({entity_id})."
            )
            if data_resolver.is_permission_error(cast(Exception, e)):
                logger.warning(
                    f"{entity_name} users would not get ingested as admin permission is not enabled on "
                    "configured Azure AD Application",
                )

        return users

    def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
        return self._get_entity_users(
            dashboard.workspace_id, Constant.DASHBOARDS, dashboard.id
        )

    def get_report_users(self, workspace_id: str, report_id: str) -> List[User]:
        return self._get_entity_users(workspace_id, Constant.REPORTS, report_id)

    def get_reports(self, workspace: Workspace) -> List[Report]:
        """
        Fetch the report from PowerBi for the given Workspace
        """
        reports: List[Report] = []
        try:
            reports = self._get_resolver().get_reports(workspace)
        except:
            self.log_http_error(
                message=f"Unable to fetch reports for workspace {workspace.name}"
            )

        def fill_ownership() -> None:
            if self.__config.extract_ownership is False:
                logger.info(
                    "Skipping user retrieval for report as extract_ownership is set to false"
                )
                return

            for report in reports:
                report.users = self.get_report_users(
                    workspace_id=workspace.id, report_id=report.id
                )

        def fill_tags() -> None:
            if self.__config.extract_endorsements_to_tags is False:
                logger.info(
                    "Skipping endorsements tags retrieval for report as extract_endorsements_to_tags is set to false"
                )
                return

            for report in reports:
                report.tags = workspace.report_endorsements.get(report.id, [])

        fill_ownership()
        fill_tags()

        return reports

    def get_workspaces(self) -> List[Workspace]:
        groups: List[dict] = []
        try:
            groups = self._get_resolver().get_groups()
        except:
            self.log_http_error(message="Unable to fetch list of workspaces")

        workspaces = [
            Workspace(
                id=workspace[Constant.ID],
                name=workspace[Constant.NAME],
                datasets={},
                dashboards=[],
                reports=[],
                report_endorsements={},
                dashboard_endorsements={},
                scan_result={},
            )
            for workspace in groups
        ]
        return workspaces

    def _get_scan_result(self, workspace: Workspace) -> Any:
        scan_id: Optional[str] = None
        try:
            scan_id = self.__admin_api_resolver.create_scan_job(
                workspace_id=workspace.id
            )
        except:
            e = self.log_http_error(
                message=f"Unable to fetch dataset lineage for {workspace.name}({workspace.id})."
            )
            if data_resolver.is_permission_error(cast(Exception, e)):
                logger.warning(
                    "Dataset lineage can not be ingestion because this user does not have access to the PowerBI Admin "
                    "API. "
                )
            return None

        logger.info("Waiting for scan to complete")
        if (
            self.__admin_api_resolver.wait_for_scan_to_complete(
                scan_id=scan_id, timeout=self.__config.scan_timeout
            )
            is False
        ):
            raise ValueError(
                "Workspace detail is not available. Please increase the scan_timeout configuration value to wait "
                "longer for the scan job to complete."
            )

        # Scan is complete lets take the result
        scan_result = self.__admin_api_resolver.get_scan_result(scan_id=scan_id)
        pretty_json: str = json.dumps(scan_result, indent=1)
        logger.debug(f"scan result = {pretty_json}")

        return scan_result

    @staticmethod
    def _parse_endorsement(endorsements: Optional[dict]) -> List[str]:
        if not endorsements:
            return []

        endorsement = endorsements.get(Constant.ENDORSEMENT, None)
        if not endorsement:
            return []

        return [endorsement]

    def _get_workspace_datasets(self, scan_result: Optional[dict]) -> dict:
        """
        Filter out "dataset" from scan_result and return Dataset instance set
        """
        dataset_map: dict = {}

        if scan_result is None:
            return dataset_map

        datasets: Optional[Any] = scan_result.get(Constant.DATASETS)
        if datasets is None or len(datasets) == 0:
            logger.warning(
                f"Workspace {scan_result[Constant.NAME]}({scan_result[Constant.ID]}) does not have datasets"
            )

            logger.info("Returning empty datasets")
            return dataset_map

        logger.debug("Processing scan result for datasets")

        for dataset_dict in datasets:
            dataset_instance: PowerBIDataset = self._get_resolver().get_dataset(
                workspace_id=scan_result[Constant.ID],
                dataset_id=dataset_dict[Constant.ID],
            )

            if self.__config.extract_endorsements_to_tags:
                dataset_instance.tags = self._parse_endorsement(
                    dataset_dict.get(Constant.ENDORSEMENT_DETAIL, None)
                )

            dataset_map[dataset_instance.id] = dataset_instance
            # set dataset-name
            dataset_name: str = (
                dataset_instance.name
                if dataset_instance.name is not None
                else dataset_instance.id
            )
            logger.debug(f"dataset_dict = {dataset_dict}")
            for table in dataset_dict.get(Constant.TABLES, []):
                expression: Optional[str] = (
                    table[Constant.SOURCE][0][Constant.EXPRESSION]
                    if table.get(Constant.SOURCE) is not None
                    and len(table[Constant.SOURCE]) > 0
                    else None
                )
                dataset_instance.tables.append(
                    Table(
                        name=table[Constant.NAME],
                        full_name="{}.{}".format(
                            dataset_name.replace(" ", "_"),
                            table[Constant.NAME].replace(" ", "_"),
                        ),
                        expression=expression,
                    )
                )

        return dataset_map

    def _fill_metadata_from_scan_result(self, workspace: Workspace) -> None:
        workspace.scan_result = self._get_scan_result(workspace)
        workspace.datasets = self._get_workspace_datasets(workspace.scan_result)
        # Fetch endorsements tag if it is enabled from configuration
        if self.__config.extract_endorsements_to_tags is False:
            logger.info(
                "Skipping endorsements tag as extract_endorsements_to_tags is set to "
                "false "
            )
            return

        workspace.dashboard_endorsements = self._get_dashboard_endorsements(
            workspace.scan_result
        )
        workspace.report_endorsements = self._get_report_endorsements(
            workspace.scan_result
        )

    def _fill_regular_metadata_detail(self, workspace: Workspace) -> None:
        def fill_dashboards() -> None:
            workspace.dashboards = self._get_resolver().get_dashboards(workspace)
            # set tiles of Dashboard
            for dashboard in workspace.dashboards:
                dashboard.tiles = self._get_resolver().get_tiles(
                    workspace, dashboard=dashboard
                )

        def fill_reports() -> None:
            if self.__config.extract_reports is False:
                logger.info(
                    "Skipping report retrieval as extract_reports is set to false"
                )
                return
            workspace.reports = self.get_reports(workspace)

        def fill_dashboard_tags() -> None:
            if self.__config.extract_endorsements_to_tags is False:
                logger.info(
                    "Skipping tag retrieval for dashboard as extract_endorsements_to_tags is set to false"
                )
                return
            for dashboard in workspace.dashboards:
                dashboard.tags = workspace.dashboard_endorsements.get(dashboard.id, [])

        fill_dashboards()
        fill_reports()
        fill_dashboard_tags()

    # flake8: noqa: C901
    def fill_workspace(
        self, workspace: Workspace, reporter: PowerBiDashboardSourceReport
    ) -> None:

        self._fill_metadata_from_scan_result(
            workspace=workspace
        )  # First try to fill the admin detail as some regular metadata contains lineage to admin metadata

        self._fill_regular_metadata_detail(workspace=workspace)
