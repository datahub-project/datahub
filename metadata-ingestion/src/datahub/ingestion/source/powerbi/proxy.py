import json
import logging
from dataclasses import dataclass
from enum import Enum
from time import sleep
from typing import Any, Dict, List, Optional

import msal
import requests as requests

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp_builder import PlatformKey
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiAPIConfig,
    PowerBiDashboardSourceReport,
)

# Logger instance
logger = logging.getLogger(__name__)


class WorkspaceKey(PlatformKey):
    workspace: str


class PowerBiAPI:
    # API endpoints of PowerBi to fetch dashboards, tiles, datasets
    API_ENDPOINTS = {
        Constant.DASHBOARD_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards",
        Constant.ENTITY_USER_LIST: "{POWERBI_ADMIN_BASE_URL}/{ENTITY}/{ENTITY_ID}/users",
        Constant.TILE_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards/{DASHBOARD_ID}/tiles",
        Constant.DATASET_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}",
        Constant.DATASOURCE_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}/datasources",
        Constant.REPORT_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports/{REPORT_ID}",
        Constant.REPORT_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports",
        Constant.SCAN_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanStatus/{SCAN_ID}",
        Constant.SCAN_RESULT_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanResult/{SCAN_ID}",
        Constant.SCAN_CREATE: "{POWERBI_ADMIN_BASE_URL}/workspaces/getInfo",
        Constant.PAGE_BY_REPORT: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports/{REPORT_ID}/pages",
    }

    SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
    BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/groups"
    ADMIN_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/admin"
    AUTHORITY: str = "https://login.microsoftonline.com/"

    @dataclass
    class Workspace:
        """
        PowerBi Workspace
        """

        id: str
        name: str
        state: str
        dashboards: List[Any]
        datasets: Dict[str, "PowerBiAPI.PowerBIDataset"]
        report_endorsements: Dict[str, List[str]]
        dashboard_endorsements: Dict[str, List[str]]

        def get_urn_part(self):
            return self.name

        def get_workspace_key(self, platform_name: str) -> PlatformKey:
            return WorkspaceKey(
                workspace=self.get_urn_part(),
                platform=platform_name,
            )

    @dataclass
    class DataSource:
        """
        PowerBi
        """

        id: str
        type: str
        raw_connection_detail: Dict

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.DataSource)
                and self.__members() == instance.__members()
            )

        def __hash__(self):
            return hash(self.__members())

    @dataclass
    class Table:
        name: str
        full_name: str
        expression: Optional[str]

    # dataclasses for PowerBi Dashboard
    @dataclass
    class PowerBIDataset:
        id: str
        name: str
        webUrl: Optional[str]
        workspace_id: str
        workspace_name: str
        # Table in datasets
        tables: List["PowerBiAPI.Table"]
        tags: List[str]

        def get_urn_part(self):
            return f"datasets.{self.id}"

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.PowerBIDataset)
                and self.__members() == instance.__members()
            )

        def __hash__(self):
            return hash(self.__members())

    @dataclass
    class Page:
        id: str
        displayName: str
        name: str
        order: int

        def get_urn_part(self):
            return f"pages.{self.id}"

    @dataclass
    class User:
        id: str
        displayName: str
        emailAddress: str
        graphId: str
        principalType: str

        def get_urn_part(self):
            return f"users.{self.id}"

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.User)
                and self.__members() == instance.__members()
            )

        def __hash__(self):
            return hash(self.__members())

    @dataclass
    class Report:
        id: str
        name: str
        webUrl: str
        embedUrl: str
        description: str
        dataset: Optional["PowerBiAPI.PowerBIDataset"]
        pages: List["PowerBiAPI.Page"]
        users: List["PowerBiAPI.User"]
        tags: List[str]

        def get_urn_part(self):
            return f"reports.{self.id}"

    @dataclass
    class Tile:
        class CreatedFrom(Enum):
            REPORT = "Report"
            DATASET = "Dataset"
            VISUALIZATION = "Visualization"
            UNKNOWN = "UNKNOWN"

        id: str
        title: str
        embedUrl: str
        dataset: Optional["PowerBiAPI.PowerBIDataset"]
        report: Optional[Any]
        createdFrom: CreatedFrom

        def get_urn_part(self):
            return f"charts.{self.id}"

    @dataclass
    class Dashboard:
        id: str
        displayName: str
        embedUrl: str
        webUrl: str
        isReadOnly: Any
        workspace_id: str
        workspace_name: str
        tiles: List["PowerBiAPI.Tile"]
        users: List["PowerBiAPI.User"]
        tags: List[str]

        def get_urn_part(self):
            return f"dashboards.{self.id}"

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.Dashboard)
                and self.__members() == instance.__members()
            )

        def __hash__(self):
            return hash(self.__members())

    def __init__(self, config: PowerBiAPIConfig) -> None:
        self.__config: PowerBiAPIConfig = config
        self.__access_token: str = ""
        # Power-Bi Auth (Service Principal Auth)
        self.__msal_client = msal.ConfidentialClientApplication(
            self.__config.client_id,
            client_credential=self.__config.client_secret,
            authority=PowerBiAPI.AUTHORITY + self.__config.tenant_id,
        )

        # Test connection by generating a access token
        logger.info("Trying to connect to {}".format(self.__get_authority_url()))
        self.get_access_token()
        logger.info("Able to connect to {}".format(self.__get_authority_url()))

    def __get_authority_url(self):
        return "{}{}".format(PowerBiAPI.AUTHORITY, self.__config.tenant_id)

    def __get_users(self, workspace_id: str, entity: str, _id: str) -> List[User]:
        """
        Get user for the given PowerBi entity
        """
        users: List[PowerBiAPI.User] = []
        if self.__config.extract_ownership is False:
            logger.info(
                "Extract ownership capabilities is disabled from configuration and hence returning empty users list"
            )
            return users

        user_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.ENTITY_USER_LIST]
        # Replace place holders
        user_list_endpoint = user_list_endpoint.format(
            POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL,
            ENTITY=entity,
            ENTITY_ID=_id,
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
            logger.info(f"{Constant.ID}={_id}")
            raise ConnectionError("Failed to fetch the user list from the power-bi")

        users_dict: List[Any] = response.json()[Constant.VALUE]

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        users = [
            PowerBiAPI.User(
                id=instance.get("identifier"),
                displayName=instance.get("displayName"),
                emailAddress=instance.get("emailAddress"),
                graphId=instance.get("graphId"),
                principalType=instance.get("principalType"),
            )
            for instance in users_dict
        ]

        return users

    def _get_report(
        self, workspace_id: str, report_id: str, workspace_name: str
    ) -> Optional["PowerBiAPI.Report"]:
        """
        Fetch the report from PowerBi for the given report identifier
        """
        if workspace_id is None or report_id is None:
            logger.info("Input values are None")
            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.ReportId}={report_id}")
            return None

        report_get_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.REPORT_GET]
        # Replace place holders
        report_get_endpoint = report_get_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
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

        return PowerBiAPI.Report(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl=response_dict.get("webUrl"),
            embedUrl=response_dict.get("embedUrl"),
            description=response_dict.get("description"),
            users=[],
            pages=[],
            tags=[],
            dataset=self.get_dataset(
                workspace_id=workspace_id,
                dataset_id=response_dict.get("datasetId"),
                workspace_name=workspace_name,
            ),
        )

    def get_access_token(self):
        if self.__access_token != "":
            logger.debug("Returning the cached access token")
            return self.__access_token

        logger.info("Generating PowerBi access token")

        auth_response = self.__msal_client.acquire_token_for_client(
            scopes=[PowerBiAPI.SCOPE]
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

    def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
        """
        Return list of dashboard users
        """
        return self.__get_users(
            workspace_id=dashboard.workspace_id, entity="dashboards", _id=dashboard.id
        )

    def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
        """
        Get the list of dashboard from PowerBi for the given workspace identifier
        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get
        -dashboards), there is no information available on pagination
        """
        dashboard_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.DASHBOARD_LIST]
        # Replace place holders
        dashboard_list_endpoint = dashboard_list_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL, WORKSPACE_ID=workspace.id
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
        dashboards: List[PowerBiAPI.Dashboard] = [
            PowerBiAPI.Dashboard(
                id=instance.get("id"),
                isReadOnly=instance.get("isReadOnly"),
                displayName=instance.get("displayName"),
                embedUrl=instance.get("embedUrl"),
                webUrl=instance.get("webUrl"),
                workspace_id=workspace.id,
                workspace_name=workspace.name,
                tiles=[],
                users=[],
                tags=workspace.dashboard_endorsements.get(instance.get("id", None), []),
            )
            for instance in dashboards_dict
            if instance is not None
        ]

        return dashboards

    def get_dashboard_endorsements(self, scan_result: dict) -> Dict[str, List[str]]:
        """
        Store saved dashboard endorsements into a dict with dashboard id as key and
        endorsements or tags as list of strings
        """
        results = {}

        for scanned_dashboard in scan_result["dashboards"]:
            # Iterate through response and create a list of PowerBiAPI.Dashboard
            dashboard_id = scanned_dashboard.get("id")
            tags = self.parse_endorsement(
                scanned_dashboard.get("endorsementDetails", None)
            )
            results[dashboard_id] = tags

        return results

    @staticmethod
    def parse_endorsement(endorsements: Optional[dict]) -> List[str]:
        if not endorsements:
            return []

        endorsement = endorsements.get("endorsement", None)
        if not endorsement:
            return []

        return [endorsement]

    def get_dataset(
        self,
        workspace_id: str,
        dataset_id: str,
        workspace_name: str,
        endorsements: Optional[dict] = None,
    ) -> Any:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if workspace_id is None or dataset_id is None:
            logger.info("Input values are None")
            logger.info(f"{Constant.WorkspaceId}={workspace_id}")
            logger.info(f"{Constant.DatasetId}={dataset_id}")
            return None

        dataset_get_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.DATASET_GET]
        # Replace place holders
        dataset_get_endpoint = dataset_get_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
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
        tags = []
        if self.__config.extract_endorsements_to_tags:
            tags = self.parse_endorsement(endorsements)

        return PowerBiAPI.PowerBIDataset(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl="{}/details".format(response_dict.get("webUrl"))
            if response_dict.get("webUrl") is not None
            else None,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            tables=[],
            tags=tags,
        )

    def get_data_sources(
        self, dataset: PowerBIDataset
    ) -> Optional[Dict[str, "PowerBiAPI.DataSource"]]:
        """
        Fetch the data source from PowerBi for the given dataset
        """

        datasource_get_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.DATASOURCE_GET]
        # Replace place holders
        datasource_get_endpoint = datasource_get_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
            WORKSPACE_ID=dataset.workspace_id,
            DATASET_ID=dataset.id,
        )
        # Hit PowerBi
        logger.info(f"Request to datasource URL={datasource_get_endpoint}")
        response = requests.get(
            datasource_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch datasource from power-bi for"
            logger.warning(message)
            logger.warning("{}={}".format(Constant.WorkspaceId, dataset.workspace_id))
            logger.warning("{}={}".format(Constant.DatasetId, dataset.id))
            logger.warning("{}={}".format(Constant.HTTP_RESPONSE_TEXT, response.text))
            logger.warning(
                "{}={}".format(Constant.HTTP_RESPONSE_STATUS_CODE, response.status_code)
            )

            raise ConnectionError(message)

        res = response.json()
        value = res["value"]
        if len(value) == 0:
            logger.info(
                f"datasource is not found for dataset {dataset.name}({dataset.id})"
            )

            return None

        data_sources: Dict[str, "PowerBiAPI.DataSource"] = {}
        logger.debug("data-sources = {}".format(value))
        for datasource_dict in value:
            # Create datasource instance with basic detail available
            datasource = PowerBiAPI.DataSource(
                id=datasource_dict.get(
                    "datasourceId"
                ),  # datasourceId is not available in all cases
                type=datasource_dict["datasourceType"],
                raw_connection_detail=datasource_dict["connectionDetails"],
            )

            data_sources[datasource.id] = datasource

        return data_sources

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
                    workspace.datasets[tile_instance.get("datasetId")]
                    if tile_instance.get("datasetId") is not None
                    else None
                ),
                "report": (
                    self._get_report(
                        workspace_id=workspace.id,
                        report_id=tile_instance.get("reportId"),
                        workspace_name=workspace.name,
                    )
                    if tile_instance.get("reportId") is not None
                    else None
                ),
                "createdFrom": PowerBiAPI.Tile.CreatedFrom.UNKNOWN,
            }

            # Tile is either created from report or dataset or from custom visualization
            if report_fields["report"] is not None:
                report_fields["createdFrom"] = PowerBiAPI.Tile.CreatedFrom.REPORT
            elif report_fields["dataset"] is not None:
                report_fields["createdFrom"] = PowerBiAPI.Tile.CreatedFrom.DATASET
            else:
                report_fields["createdFrom"] = PowerBiAPI.Tile.CreatedFrom.VISUALIZATION

            logger.info(
                "Tile %s(%s) is created from %s",
                tile_instance.get("title"),
                tile_instance.get("id"),
                report_fields["createdFrom"],
            )

            return report_fields

        tile_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.TILE_LIST]
        # Replace place holders
        tile_list_endpoint = tile_list_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
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
        tiles: List[PowerBiAPI.Tile] = [
            PowerBiAPI.Tile(
                id=instance.get("id"),
                title=instance.get("title"),
                embedUrl=instance.get("embedUrl"),
                **new_dataset_or_report(instance),
            )
            for instance in tile_dict
            if instance is not None
        ]

        return tiles

    def get_pages_by_report(
        self, workspace_id: str, report_id: str
    ) -> List["PowerBiAPI.Page"]:
        """
        Fetch the report from PowerBi for the given report identifier
        """
        if workspace_id is None or report_id is None:
            logger.info("workspace_id or report_id is None")
            return []

        pages_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.PAGE_BY_REPORT]
        # Replace place holders
        pages_endpoint = pages_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
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
            PowerBiAPI.Page(
                id="{}.{}".format(report_id, raw_instance["name"].replace(" ", "_")),
                name=raw_instance["name"],
                displayName=raw_instance.get("displayName"),
                order=raw_instance.get("order"),
            )
            for raw_instance in response_dict["value"]
        ]

    def get_reports(
        self, workspace: "PowerBiAPI.Workspace"
    ) -> List["PowerBiAPI.Report"]:
        """
        Fetch the report from PowerBi for the given report identifier
        """
        if workspace is None:
            logger.info("workspace is None")
            return []

        report_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.REPORT_LIST]
        # Replace place holders
        report_list_endpoint = report_list_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
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
        reports: List["PowerBiAPI.Report"] = [
            PowerBiAPI.Report(
                id=raw_instance["id"],
                name=raw_instance.get("name"),
                webUrl=raw_instance.get("webUrl"),
                embedUrl=raw_instance.get("embedUrl"),
                description=raw_instance.get("description"),
                pages=self.get_pages_by_report(
                    workspace_id=workspace.id, report_id=raw_instance["id"]
                ),
                users=self.__get_users(
                    workspace_id=workspace.id, entity="reports", _id=raw_instance["id"]
                ),
                dataset=workspace.datasets.get(raw_instance.get("datasetId")),
                tags=workspace.report_endorsements.get(
                    raw_instance.get("id", None), []
                ),
            )
            for raw_instance in response_dict["value"]
        ]

        return reports

    def get_groups(self):
        group_endpoint = PowerBiAPI.BASE_URL
        # Hit PowerBi
        logger.info(f"Request to get groups endpoint URL={group_endpoint}")
        response = requests.get(
            group_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )
        response.raise_for_status()
        return response.json()

    def get_workspaces(self):
        groups = self.get_groups()
        workspaces = [
            PowerBiAPI.Workspace(
                id=workspace.get("id"),
                name=workspace.get("name"),
                state="",
                datasets={},
                dashboards=[],
                report_endorsements={},
                dashboard_endorsements={},
            )
            for workspace in groups.get("value", [])
            if workspace.get("type", None) == "Workspace"
        ]
        return workspaces

    # flake8: noqa: C901
    def get_workspace(
        self, workspace_id: str, reporter: PowerBiDashboardSourceReport
    ) -> Workspace:
        """
        Return Workspace for the given workspace identifier i.e. workspace_id
        """
        scan_create_endpoint = PowerBiAPI.API_ENDPOINTS[Constant.SCAN_CREATE]
        scan_create_endpoint = scan_create_endpoint.format(
            POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL
        )

        def create_scan_job():
            """
            Create scan job on PowerBi for the workspace
            """
            request_body = {"workspaces": [workspace_id]}

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
            # Return Id of Scan created for the given workspace
            id = res.json()["id"]
            logger.info("Scan id({})".format(id))
            return id

        def calculate_max_trial(minimum_sleep: int, timeout: int) -> int:
            if timeout < minimum_sleep:
                logger.info(
                    f"Setting timeout to minimum_sleep time {minimum_sleep} seconds"
                )
                timeout = minimum_sleep

            return timeout // minimum_sleep

        def wait_for_scan_to_complete(scan_id: str, timeout: int) -> Any:
            """
            Poll the PowerBi service for workspace scan to complete
            """
            minimum_sleep = 3
            max_trial: int = calculate_max_trial(minimum_sleep, timeout)
            logger.info(f"Max trial {max_trial}")

            scan_get_endpoint = PowerBiAPI.API_ENDPOINTS[Constant.SCAN_GET]
            scan_get_endpoint = scan_get_endpoint.format(
                POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL, SCAN_ID=scan_id
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

        def get_scan_result(scan_id: str) -> dict:
            logger.info("Fetching scan  result")
            logger.info(f"{Constant.SCAN_ID}={scan_id}")
            scan_result_get_endpoint = PowerBiAPI.API_ENDPOINTS[
                Constant.SCAN_RESULT_GET
            ]
            scan_result_get_endpoint = scan_result_get_endpoint.format(
                POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL, SCAN_ID=scan_id
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

        def json_to_dataset_map(scan_result: dict) -> dict:
            """
            Filter out "dataset" from scan_result and return PowerBiAPI.Dataset instance set
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
                dataset_instance: PowerBiAPI.PowerBIDataset = self.get_dataset(
                    workspace_id=scan_result["id"],
                    dataset_id=dataset_dict["id"],
                    workspace_name=scan_result["name"],
                    endorsements=dataset_dict.get("endorsementDetails", None),
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
                        PowerBiAPI.Table(
                            name=table["name"],
                            full_name="{}.{}".format(
                                dataset_name.replace(" ", "_"),
                                table["name"].replace(" ", "_"),
                            ),
                            expression=expression,
                        )
                    )

            return dataset_map

        def init_dashboard_tiles(workspace: PowerBiAPI.Workspace) -> None:
            for dashboard in workspace.dashboards:
                dashboard.tiles = self.get_tiles(workspace, dashboard=dashboard)

            return None

        def scan_result_to_report_endorsements(
            scan_result: dict,
        ) -> Dict[str, List[str]]:
            results = {}
            reports: List[dict] = scan_result.get("reports", [])

            for report in reports:
                report_id = report.get("id", "")
                endorsements = self.parse_endorsement(
                    report.get("endorsementDetails", None)
                )
                results[report_id] = endorsements
            return results

        logger.info("Creating scan job for workspace")
        logger.info("{}={}".format(Constant.WorkspaceId, workspace_id))
        logger.debug("Hitting URL={}".format(scan_create_endpoint))
        scan_id = create_scan_job()
        logger.info("Waiting for scan to complete")
        if (
            wait_for_scan_to_complete(
                scan_id=scan_id, timeout=self.__config.scan_timeout
            )
            is False
        ):
            raise ValueError(
                "Workspace detail is not available. Please increase scan_timeout to wait."
            )

        # Scan is complete lets take the result
        scan_result = get_scan_result(scan_id=scan_id)

        logger.debug(f"scan result = %s", json.dumps(scan_result, indent=1))
        workspace = PowerBiAPI.Workspace(
            id=scan_result["id"],
            name=scan_result["name"],
            state=scan_result["state"],
            datasets={},
            dashboards=[],
            report_endorsements={},
            dashboard_endorsements={},
        )

        if self.__config.extract_endorsements_to_tags:
            workspace.dashboard_endorsements = self.get_dashboard_endorsements(
                scan_result
            )
            workspace.report_endorsements = scan_result_to_report_endorsements(
                scan_result
            )

        # Get workspace dashboards
        workspace.dashboards = self.get_dashboards(workspace)

        workspace.datasets = json_to_dataset_map(scan_result)
        init_dashboard_tiles(workspace)

        return workspace
