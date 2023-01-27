import logging
from time import sleep
from typing import Any, List, Optional

import msal
import requests

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.powerbi.config import Constant
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Dashboard,
    Page,
    PowerBIDataset,
    Report,
    Tile,
    User,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)


class DataResolverBase:
    SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
    BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/groups"
    ADMIN_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/admin"
    AUTHORITY: str = "https://login.microsoftonline.com/"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ):
        self.__access_token: str = ""
        self.__tenant_id = tenant_id
        # Power-Bi Auth (Service Principal Auth)
        self.__msal_client = msal.ConfidentialClientApplication(
            client_id,
            client_credential=client_secret,
            authority=DataResolverBase.AUTHORITY + tenant_id,
        )

        # Test connection by generating access token
        logger.info("Trying to connect to {}".format(self.__get_authority_url()))
        self.get_access_token()
        logger.info("Connected to {}".format(self.__get_authority_url()))

    def get_access_token(self):
        if self.__access_token != "":
            logger.debug("Returning the cached access token")
            return self.__access_token

        logger.info("Generating PowerBi access token")

        auth_response = self.__msal_client.acquire_token_for_client(
            scopes=[DataResolverBase.SCOPE]
        )

        if not auth_response.get("access_token"):
            logger.warning(
                "Failed to generate the PowerBi access token. Please check input configuration"
            )
            raise ConfigurationError(
                "Powerbi authorization failed . Please check your input configuration."
            )

        logger.info("Generated PowerBi access token")

        self.__access_token = "Bearer {}".format(auth_response.get("access_token"))

        logger.debug(f"{Constant.PBIAccessToken}={self.__access_token}")

        return self.__access_token

    def __get_authority_url(self):
        return "{}{}".format(DataResolverBase.AUTHORITY, self.__tenant_id)


class RegularAPIResolver(DataResolverBase):
    # Regular access endpoints
    API_ENDPOINTS = {
        Constant.DASHBOARD_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards",
        Constant.TILE_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards/{DASHBOARD_ID}/tiles",
        Constant.DATASET_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}",
        Constant.DATASOURCE_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}/datasources",
        Constant.REPORT_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports/{REPORT_ID}",
        Constant.REPORT_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports",
        Constant.PAGE_BY_REPORT: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports/{REPORT_ID}/pages",
    }

    def _get_report(self, workspace_id: str, report_id: str) -> Optional[Report]:
        """
        Fetch the report from PowerBi for the given report identifier
        """
        if workspace_id is None or report_id is None:
            logger.info("Input values are None")
            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.ReportId}={report_id}")
            return None

        report_get_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.REPORT_GET]
        # Replace place holders
        report_get_endpoint = report_get_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace_id,
            REPORT_ID=report_id,
        )
        # Hit PowerBi
        logger.info(f"Request to report URL={report_get_endpoint}")
        response = requests.get(
            report_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi for"
            logger.warning(message)
            logger.warning(f"{Constant.WorkspaceId}={workspace_id}")
            logger.warning(f"{Constant.ReportId}={report_id}")
            raise ConnectionError(message)

        response_dict = response.json()

        return Report(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl=response_dict.get("webUrl"),
            embedUrl=response_dict.get("embedUrl"),
            description=response_dict.get("description"),
            users=[],
            pages=[],
            tags=[],
            dataset=self.get_dataset(
                workspace_id=workspace_id, dataset_id=response_dict.get("datasetId")
            ),
        )

    def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
        """
        Get the list of dashboard from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get
        -dashboards), there is no information available on pagination
        """
        dashboard_list_endpoint: str = RegularAPIResolver.API_ENDPOINTS[
            Constant.DASHBOARD_LIST
        ]
        # Replace place holders
        dashboard_list_endpoint = dashboard_list_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL, WORKSPACE_ID=workspace.id
        )
        # Hit PowerBi
        logger.info(f"Request to URL={dashboard_list_endpoint}")
        response = requests.get(
            dashboard_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            logger.warning("Failed to fetch dashboard list from power-bi for")
            logger.warning(f"{Constant.WorkspaceId}={workspace.id}")
            raise ConnectionError(
                "Failed to fetch the dashboard list from the power-bi"
            )

        dashboards_dict: List[Any] = response.json()[Constant.VALUE]

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        dashboards: List[Dashboard] = [
            Dashboard(
                id=instance.get("id"),
                isReadOnly=instance.get("isReadOnly"),
                displayName=instance.get("displayName"),
                embedUrl=instance.get("embedUrl"),
                webUrl=instance.get("webUrl"),
                workspace_id=workspace.id,
                workspace_name=workspace.name,
                tiles=[],
                users=[],
                tags=[],
            )
            for instance in dashboards_dict
            if instance is not None
        ]

        return dashboards

    def get_dataset(self, workspace_id: str, dataset_id: str) -> Any:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if workspace_id is None or dataset_id is None:
            logger.info("Input values are None")
            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.DatasetId}={dataset_id}")
            return None

        dataset_get_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.DATASET_GET]
        # Replace place holders
        dataset_get_endpoint = dataset_get_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace_id,
            DATASET_ID=dataset_id,
        )
        # Hit PowerBi
        logger.info(f"Request to dataset URL={dataset_get_endpoint}")
        response = requests.get(
            dataset_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch dataset from power-bi for"
            logger.warning(message)
            logger.warning(f"{Constant.WorkspaceId}={workspace_id}")
            logger.warning(f"{Constant.DatasetId}={dataset_id}")
            raise ConnectionError(message)

        response_dict = response.json()
        logger.debug("datasets = {}".format(response_dict))
        # PowerBi Always return the webURL, in-case if it is None then setting complete webURL to None instead of
        # None/details
        return PowerBIDataset(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl="{}/details".format(response_dict.get("webUrl"))
            if response_dict.get("webUrl") is not None
            else None,
            workspace_id=workspace_id,
            tables=[],
            tags=[],
        )

    def get_tiles(self, workspace: Workspace, dashboard: Dashboard) -> List[Tile]:

        """
        Get the list of tiles from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get
        -tiles), there is no information available on pagination

        """

        def new_dataset_or_report(tile_instance: Any) -> dict:
            """
            Find out which is the data source for tile. It is either REPORT or DATASET
            """
            report_fields = {
                "dataset": (
                    workspace.datasets.get(tile_instance.get("datasetId"))
                    if tile_instance.get("datasetId") is not None
                    else None
                ),
                "report": (
                    self._get_report(
                        workspace_id=workspace.id,
                        report_id=tile_instance.get("reportId"),
                    )
                    if tile_instance.get("reportId") is not None
                    else None
                ),
                "createdFrom": Tile.CreatedFrom.UNKNOWN,
            }

            # Tile is either created from report or dataset or from custom visualization
            if report_fields["report"] is not None:
                report_fields["createdFrom"] = Tile.CreatedFrom.REPORT
            elif report_fields["dataset"] is not None or (
                report_fields["dataset"] is None
                and tile_instance.get("datasetId") is not None
            ):  # Admin API is disabled and hence dataset instance is mising
                report_fields["createdFrom"] = Tile.CreatedFrom.DATASET
            else:
                report_fields["createdFrom"] = Tile.CreatedFrom.VISUALIZATION

            logger.info(
                "Tile %s(%s) is created from %s",
                tile_instance.get("title"),
                tile_instance.get("id"),
                report_fields["createdFrom"],
            )

            return report_fields

        tile_list_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.TILE_LIST]
        # Replace place holders
        tile_list_endpoint = tile_list_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=dashboard.workspace_id,
            DASHBOARD_ID=dashboard.id,
        )
        # Hit PowerBi
        logger.info("Request to URL={}".format(tile_list_endpoint))
        response = requests.get(
            tile_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            logger.warning("Failed to fetch tiles list from power-bi for")
            logger.warning("{}={}".format(Constant.WorkspaceId, workspace.id))
            logger.warning("{}={}".format(Constant.DashboardId, dashboard.id))
            raise ConnectionError("Failed to fetch the tile list from the power-bi")

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        tile_dict: List[Any] = response.json()[Constant.VALUE]
        logger.debug("Tile Dict = {}".format(tile_dict))
        tiles: List[Tile] = [
            Tile(
                id=instance.get("id"),
                title=instance.get("title"),
                embedUrl=instance.get("embedUrl"),
                **new_dataset_or_report(instance),
            )
            for instance in tile_dict
            if instance is not None
        ]

        return tiles

    def get_groups(self):
        group_endpoint = DataResolverBase.BASE_URL
        # Hit PowerBi
        logger.info(f"Request to get groups endpoint URL={group_endpoint}")
        response = requests.get(
            group_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )
        response.raise_for_status()
        return response.json()

    def _get_pages_by_report(self, workspace_id: str, report_id: str) -> List[Page]:
        pages_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.PAGE_BY_REPORT]
        # Replace place holders
        pages_endpoint = pages_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace_id,
            REPORT_ID=report_id,
        )
        # Hit PowerBi
        logger.info(f"Request to pages URL={pages_endpoint}")
        response = requests.get(
            pages_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch reports from power-bi for"
            logger.warning(message)
            logger.warning(f"{Constant.WorkspaceId}={workspace_id}")
            raise ConnectionError(message)

        response_dict = response.json()
        return [
            Page(
                id="{}.{}".format(report_id, raw_instance["name"].replace(" ", "_")),
                name=raw_instance["name"],
                displayName=raw_instance.get("displayName"),
                order=raw_instance.get("order"),
            )
            for raw_instance in response_dict["value"]
        ]

    def get_reports(self, workspace: Workspace) -> List[Report]:

        report_list_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.REPORT_LIST]
        # Replace place holders
        report_list_endpoint = report_list_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace.id,
        )
        # Hit PowerBi
        logger.info(f"Request to report URL={report_list_endpoint}")
        response = requests.get(
            report_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch reports from power-bi for"
            logger.warning(message)
            logger.warning(f"{Constant.WorkspaceId}={workspace.id}")
            raise ConnectionError(message)

        response_dict = response.json()
        reports: List[Report] = [
            Report(
                id=raw_instance["id"],
                name=raw_instance.get("name"),
                webUrl=raw_instance.get("webUrl"),
                embedUrl=raw_instance.get("embedUrl"),
                description=raw_instance.get("description"),
                pages=self._get_pages_by_report(
                    workspace_id=workspace.id, report_id=raw_instance["id"]
                ),
                users=[],  # It will be fetched using Admin Fetcher based on condition
                tags=[],  # It will be fetched using Admin Fetcher based on condition
                dataset=workspace.datasets.get(raw_instance.get("datasetId")),
            )
            for raw_instance in response_dict["value"]
        ]

        return reports


class AdminAPIResolver(DataResolverBase):
    # Admin access endpoints
    API_ENDPOINTS = {
        Constant.SCAN_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanStatus/{SCAN_ID}",
        Constant.SCAN_RESULT_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanResult/{SCAN_ID}",
        Constant.SCAN_CREATE: "{POWERBI_ADMIN_BASE_URL}/workspaces/getInfo",
        Constant.ENTITY_USER_LIST: "{POWERBI_ADMIN_BASE_URL}/{ENTITY}/{ENTITY_ID}/users",
    }

    def create_scan_job(self, workspace_id: str) -> str:
        """
        Create scan job on PowerBI for the workspace
        """
        request_body = {"workspaces": [workspace_id]}

        scan_create_endpoint = AdminAPIResolver.API_ENDPOINTS[Constant.SCAN_CREATE]
        scan_create_endpoint = scan_create_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL
        )

        logger.debug("Creating metadata scan job, request body (%s)", request_body)

        res = requests.post(
            scan_create_endpoint,
            data=request_body,
            params={
                "datasetExpressions": True,
                "datasetSchema": True,
                "datasourceDetails": True,
                "getArtifactUsers": True,
                "lineage": True,
            },
            headers={Constant.Authorization: self.get_access_token()},
        )

        if res.status_code not in (200, 202):
            message = f"API({scan_create_endpoint}) return error code {res.status_code} for workspace id({workspace_id})"

            logger.warning(message)

            raise ConnectionError(message)

        # Return scan_id of Scan created for the given workspace
        scan_id = res.json()["id"]

        logger.info("Scan id({})".format(scan_id))

        return scan_id

    @staticmethod
    def _calculate_max_trial(minimum_sleep: int, timeout: int) -> int:
        if timeout < minimum_sleep:
            logger.info(
                f"Setting timeout to minimum_sleep time {minimum_sleep} seconds"
            )
            timeout = minimum_sleep

        return timeout // minimum_sleep

    def wait_for_scan_to_complete(self, scan_id: str, timeout: int) -> Any:
        """
        Poll the PowerBi service for workspace scan to complete
        """
        minimum_sleep = 3
        max_trial: int = AdminAPIResolver._calculate_max_trial(minimum_sleep, timeout)
        logger.info(f"Max trial {max_trial}")

        scan_get_endpoint = AdminAPIResolver.API_ENDPOINTS[Constant.SCAN_GET]
        scan_get_endpoint = scan_get_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL, SCAN_ID=scan_id
        )
        logger.debug(f"Hitting URL={scan_get_endpoint}")
        trail = 1
        while True:
            logger.info(f"Trial = {trail}")
            res = requests.get(
                scan_get_endpoint,
                headers={Constant.Authorization: self.get_access_token()},
            )
            if res.status_code != 200:
                message = f"API({scan_get_endpoint}) return error code {res.status_code} for scan id({scan_id})"
                logger.warning(message)
                raise ConnectionError(message)

            if res.json()["status"].upper() == "Succeeded".upper():
                logger.info(f"Scan result is available for scan id({scan_id})")
                return True

            if trail == max_trial:
                break

            logger.info(f"Sleeping for {minimum_sleep} seconds")
            sleep(minimum_sleep)
            trail += 1

        # Result is not available
        return False

    def get_users(self, workspace_id: str, entity: str, entity_id: str) -> List[User]:
        """
        Get user for the given PowerBi entity
        """

        user_list_endpoint: str = AdminAPIResolver.API_ENDPOINTS[Constant.ENTITY_USER_LIST]
        # Replace place holders
        user_list_endpoint = user_list_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            ENTITY=entity,
            ENTITY_ID=entity_id,
        )
        # Hit PowerBi
        logger.info(f"Request to URL={user_list_endpoint}")
        response = requests.get(
            user_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            logger.warning(
                "Failed to fetch user list from power-bi. http_status=%s. message=%s",
                response.status_code,
                response.text,
            )

            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.ENTITY}={entity}")
            logger.info(f"{Constant.ID}={entity_id}")
            raise ConnectionError("Failed to fetch the user list from the power-bi")

        users_dict: List[Any] = response.json()[Constant.VALUE]

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        users: List[User] = [
            User(
                id=instance.get("identifier"),
                displayName=instance.get("displayName"),
                emailAddress=instance.get("emailAddress"),
                graphId=instance.get("graphId"),
                principalType=instance.get("principalType"),
            )
            for instance in users_dict
        ]

        return users

    def get_scan_result(self, scan_id: str) -> dict:
        logger.info("Fetching scan result")
        logger.info(f"{Constant.SCAN_ID}={scan_id}")
        scan_result_get_endpoint = AdminAPIResolver.API_ENDPOINTS[Constant.SCAN_RESULT_GET]
        scan_result_get_endpoint = scan_result_get_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL, SCAN_ID=scan_id
        )

        logger.debug(f"Hitting URL={scan_result_get_endpoint}")
        res = requests.get(
            scan_result_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )
        if res.status_code != 200:
            message = f"API({scan_result_get_endpoint}) return error code {res.status_code} for scan id({scan_id})"
            logger.warning(message)
            raise ConnectionError(message)

        return res.json()["workspaces"][0]
