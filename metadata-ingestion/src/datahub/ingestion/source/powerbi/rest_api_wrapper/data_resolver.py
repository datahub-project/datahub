import logging
from abc import ABC, abstractmethod
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
    new_powerbi_dataset,
)

# Logger instance
logger = logging.getLogger(__name__)


def is_permission_error(e: requests.exceptions.HTTPError) -> bool:
    return e.response.status_code == 401 or e.response.status_code == 403


class DataResolverBase(ABC):
    SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
    BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/groups"
    ADMIN_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/admin"
    AUTHORITY: str = "https://login.microsoftonline.com/"
    TOP: int = 1000

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
        logger.info("Trying to connect to {}".format(self._get_authority_url()))
        self.get_access_token()
        logger.info("Connected to {}".format(self._get_authority_url()))

    @abstractmethod
    def get_groups_endpoint(self) -> str:
        pass

    @abstractmethod
    def get_dashboards_endpoint(self, workspace: Workspace) -> str:
        pass

    @abstractmethod
    def get_reports_endpoint(self, workspace: Workspace) -> str:
        pass

    @abstractmethod
    def get_tiles_endpoint(self, workspace: Workspace, dashboard_id: str) -> str:
        pass

    @abstractmethod
    def _get_pages_by_report(self, workspace: Workspace, report_id: str) -> List[Page]:
        pass

    @abstractmethod
    def get_dataset(
        self, workspace_id: str, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        pass

    @abstractmethod
    def get_users(self, workspace_id: str, entity: str, entity_id: str) -> List[User]:
        pass

    def _get_authority_url(self):
        return "{}{}".format(DataResolverBase.AUTHORITY, self.__tenant_id)

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

    def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
        """
        Get the list of dashboard from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get
        -dashboards), there is no information available on pagination
        """
        dashboard_list_endpoint: str = self.get_dashboards_endpoint(workspace)

        # Hit PowerBi
        logger.info(f"Request to URL={dashboard_list_endpoint}")
        response = requests.get(
            dashboard_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        response.raise_for_status()

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

    def get_groups(self) -> List[dict]:
        group_endpoint = self.get_groups_endpoint()
        params: dict = {"$top": self.TOP, "$skip": 0, "$filter": "type eq 'Workspace'"}

        def fetch_page(page_number: int) -> dict:
            params["$skip"] = self.TOP * page_number
            logger.debug("Query parameters = %s", params)
            response = requests.get(
                group_endpoint,
                headers={Constant.Authorization: self.get_access_token()},
                params=params,
            )
            response.raise_for_status()
            return response.json()

        # Hit PowerBi
        logger.info("Request to groups endpoint URL=%s", group_endpoint)
        zeroth_page = fetch_page(0)
        logger.debug("Page 0 = %s", zeroth_page)
        if zeroth_page.get("@odata.count") is None:
            raise ValueError(
                "Required field @odata.count is missing. Not able to determine number of pages to fetch"
            )

        number_of_items = zeroth_page["@odata.count"]
        number_of_pages = round(number_of_items / self.TOP)
        output: List[dict] = zeroth_page["value"]
        for page in range(
            1, number_of_pages
        ):  # start from 1 as 0th index already fetched
            page_response = fetch_page(page)
            if len(page_response["value"]) == 0:
                break

            logger.debug("Page %d = %s", page, zeroth_page)

            output.extend(page_response["value"])

        return output

    def get_reports(
        self, workspace: Workspace, _filter: Optional[str] = None
    ) -> List[Report]:
        reports_endpoint = self.get_reports_endpoint(workspace)
        # Hit PowerBi
        logger.info(f"Request to report URL={reports_endpoint}")
        params: Optional[dict] = None
        if _filter is not None:
            params = {"$filter": _filter}

        response = requests.get(
            reports_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
            params=params,
        )
        response.raise_for_status()
        response_dict = response.json()
        reports: List[Report] = [
            Report(
                id=raw_instance["id"],
                name=raw_instance.get("name"),
                webUrl=raw_instance.get("webUrl"),
                embedUrl=raw_instance.get("embedUrl"),
                description=raw_instance.get("description"),
                pages=self._get_pages_by_report(
                    workspace=workspace, report_id=raw_instance["id"]
                ),
                users=[],  # It will be fetched using Admin Fetcher based on condition
                tags=[],  # It will be fetched using Admin Fetcher based on condition
                dataset=workspace.datasets.get(raw_instance.get("datasetId")),
            )
            for raw_instance in response_dict["value"]
        ]

        return reports

    def get_report(self, workspace: Workspace, report_id: str) -> Optional[Report]:
        reports: List[Report] = self.get_reports(
            workspace, _filter=f"id eq '{report_id}'"
        )

        if len(reports) == 0:
            return None

        return reports[0]

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
                    self.get_report(
                        workspace=workspace,
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
            ):  # Admin API is disabled and hence dataset instance is missing
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

        tile_list_endpoint: str = self.get_tiles_endpoint(
            workspace, dashboard_id=dashboard.id
        )
        # Hit PowerBi
        logger.info("Request to URL={}".format(tile_list_endpoint))
        response = requests.get(
            tile_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )
        response.raise_for_status()

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

    def get_dataset(
        self, workspace_id: str, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if workspace_id is None or dataset_id is None:
            logger.info("Input values are None")
            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.DatasetId}={dataset_id}")
            return None

        dataset_get_endpoint: str = RegularAPIResolver.API_ENDPOINTS[
            Constant.DATASET_GET
        ]
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
        response.raise_for_status()
        response_dict = response.json()
        logger.debug("datasets = {}".format(response_dict))
        # PowerBi Always return the webURL, in-case if it is None then setting complete webURL to None instead of
        # None/details
        return new_powerbi_dataset(workspace_id, response_dict)

    def get_groups_endpoint(self) -> str:
        return DataResolverBase.BASE_URL

    def get_dashboards_endpoint(self, workspace: Workspace) -> str:
        dashboards_endpoint: str = RegularAPIResolver.API_ENDPOINTS[
            Constant.DASHBOARD_LIST
        ]
        # Replace place holders
        return dashboards_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL, WORKSPACE_ID=workspace.id
        )

    def get_reports_endpoint(self, workspace: Workspace) -> str:
        reports_endpoint: str = self.API_ENDPOINTS[Constant.REPORT_LIST]
        return reports_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL, WORKSPACE_ID=workspace.id
        )

    def get_tiles_endpoint(self, workspace: Workspace, dashboard_id: str) -> str:
        tiles_endpoint: str = self.API_ENDPOINTS[Constant.TILE_LIST]
        # Replace place holders
        return tiles_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace.id,
            DASHBOARD_ID=dashboard_id,
        )

    def _get_pages_by_report(self, workspace: Workspace, report_id: str) -> List[Page]:
        pages_endpoint: str = RegularAPIResolver.API_ENDPOINTS[Constant.PAGE_BY_REPORT]
        # Replace place holders
        pages_endpoint = pages_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace.id,
            REPORT_ID=report_id,
        )
        # Hit PowerBi
        logger.info(f"Request to pages URL={pages_endpoint}")
        response = requests.get(
            pages_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        response.raise_for_status()
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

    def get_users(self, workspace_id: str, entity: str, entity_id: str) -> List[User]:
        return []  # User list is not available in regular access


class AdminAPIResolver(DataResolverBase):

    # Admin access endpoints
    API_ENDPOINTS = {
        Constant.DASHBOARD_LIST: "{POWERBI_ADMIN_BASE_URL}/groups/{WORKSPACE_ID}/dashboards",
        Constant.TILE_LIST: "{POWERBI_ADMIN_BASE_URL}/dashboards/{DASHBOARD_ID}/tiles",
        Constant.REPORT_LIST: "{POWERBI_ADMIN_BASE_URL}/groups/{WORKSPACE_ID}/reports",
        Constant.SCAN_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanStatus/{SCAN_ID}",
        Constant.SCAN_RESULT_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanResult/{SCAN_ID}",
        Constant.SCAN_CREATE: "{POWERBI_ADMIN_BASE_URL}/workspaces/getInfo",
        Constant.ENTITY_USER_LIST: "{POWERBI_ADMIN_BASE_URL}/{ENTITY}/{ENTITY_ID}/users",
        Constant.DATASET_LIST: "{POWERBI_ADMIN_BASE_URL}/groups/{WORKSPACE_ID}/datasets",
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

        res.raise_for_status()
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

        user_list_endpoint: str = AdminAPIResolver.API_ENDPOINTS[
            Constant.ENTITY_USER_LIST
        ]
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

        response.raise_for_status()

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
        scan_result_get_endpoint = AdminAPIResolver.API_ENDPOINTS[
            Constant.SCAN_RESULT_GET
        ]
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

    def get_groups_endpoint(self) -> str:
        return f"{AdminAPIResolver.ADMIN_BASE_URL}/groups"

    def get_dashboards_endpoint(self, workspace: Workspace) -> str:
        dashboard_list_endpoint: str = self.API_ENDPOINTS[Constant.DASHBOARD_LIST]
        # Replace place holders
        return dashboard_list_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            WORKSPACE_ID=workspace.id,
        )

    def get_reports_endpoint(self, workspace: Workspace) -> str:
        reports_endpoint: str = self.API_ENDPOINTS[Constant.REPORT_LIST]
        return reports_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            WORKSPACE_ID=workspace.id,
        )

    def get_tiles_endpoint(self, workspace: Workspace, dashboard_id: str) -> str:
        tiles_endpoint: str = self.API_ENDPOINTS[Constant.TILE_LIST]
        # Replace place holders
        return tiles_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            DASHBOARD_ID=dashboard_id,
        )

    def get_dataset(
        self, workspace_id: str, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        datasets_endpoint = self.API_ENDPOINTS[Constant.DATASET_LIST].format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            WORKSPACE_ID=workspace_id,
        )
        # Hit PowerBi
        logger.info(f"Request to datasets URL={datasets_endpoint}")
        params: dict = {"$filter": f"id eq '{dataset_id}'"}
        logger.debug("params = %s", params)
        response = requests.get(
            datasets_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
            params=params,  # urllib.parse.urlencode(params, doseq=True).replace('+', '%20'),
        )
        response.raise_for_status()
        response_dict = response.json()
        if len(response_dict["value"]) == 0:
            logger.warning(
                "Dataset not found. workspace_id = %s, dataset_id = %s",
                workspace_id,
                dataset_id,
            )
            return None

        raw_instance: dict = response_dict["value"][0]
        return new_powerbi_dataset(workspace_id, raw_instance)

    def _get_pages_by_report(self, workspace: Workspace, report_id: str) -> List[Page]:
        return []  # Report pages are not available in Admin API
