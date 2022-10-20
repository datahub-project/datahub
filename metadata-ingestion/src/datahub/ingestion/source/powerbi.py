#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################

import logging
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xmlrpc.client import Boolean

import msal
import pydantic
import requests

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartKeyClass,
    CorpUserInfoClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DataPlatformInfoClass,
    DatasetKeyClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
)
from datahub.utilities.dedup_list import deduplicate_list

# Logger instance
LOGGER = logging.getLogger(__name__)


class Constant:
    """
    keys used in powerbi plugin
    """

    PBIAccessToken = "PBIAccessToken"
    DASHBOARD_LIST = "DASHBOARD_LIST"
    TILE_LIST = "TILE_LIST"
    DATASET_GET = "DATASET_GET"
    REPORT_GET = "REPORT_GET"
    DATASOURCE_GET = "DATASOURCE_GET"
    TILE_GET = "TILE_GET"
    ENTITY_USER_LIST = "ENTITY_USER_LIST"
    SCAN_CREATE = "SCAN_CREATE"
    SCAN_GET = "SCAN_GET"
    SCAN_RESULT_GET = "SCAN_RESULT_GET"
    Authorization = "Authorization"
    WorkspaceId = "WorkspaceId"
    DashboardId = "DashboardId"
    DatasetId = "DatasetId"
    ReportId = "ReportId"
    SCAN_ID = "ScanId"
    Dataset_URN = "DatasetURN"
    CHART_URN = "ChartURN"
    CHART = "chart"
    CORP_USER = "corpuser"
    CORP_USER_INFO = "corpUserInfo"
    CORP_USER_KEY = "corpUserKey"
    CHART_INFO = "chartInfo"
    STATUS = "status"
    CHART_ID = "powerbi.linkedin.com/charts/{}"
    CHART_KEY = "chartKey"
    DASHBOARD_ID = "powerbi.linkedin.com/dashboards/{}"
    DASHBOARD = "dashboard"
    DASHBOARD_KEY = "dashboardKey"
    OWNERSHIP = "ownership"
    BROWSERPATH = "browsePaths"
    DASHBOARD_INFO = "dashboardInfo"
    DATAPLATFORM_INSTANCE = "dataPlatformInstance"
    DATASET = "dataset"
    DATASET_ID = "powerbi.linkedin.com/datasets/{}"
    DATASET_KEY = "datasetKey"
    DATASET_PROPERTIES = "datasetProperties"
    VALUE = "value"
    ENTITY = "ENTITY"
    ID = "ID"
    HTTP_RESPONSE_TEXT = "HttpResponseText"
    HTTP_RESPONSE_STATUS_CODE = "HttpResponseStatusCode"


class PowerBiAPIConfig(EnvBasedSourceConfigBase):
    # Organsation Identifier
    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    # PowerBi workspace identifier
    workspace_id: str = pydantic.Field(description="PowerBI workspace identifier")
    # Dataset type mapping
    dataset_type_mapping: Dict[str, str] = pydantic.Field(
        description="Mapping of PowerBI datasource type to DataHub supported data-sources. See Quickstart Recipe for mapping"
    )
    # Azure app client identifier
    client_id: str = pydantic.Field(description="Azure app client identifier")
    # Azure app client secret
    client_secret: str = pydantic.Field(description="Azure app client secret")
    # timeout for meta-data scanning
    scan_timeout: int = pydantic.Field(
        default=60, description="timeout for PowerBI metadata scanning"
    )
    # Enable/Disable extracting ownership information of Dashboard
    extract_ownership: bool = pydantic.Field(
        default=True, description="Whether ownership should be ingested"
    )


class PowerBiDashboardSourceConfig(PowerBiAPIConfig):
    platform_name: str = "powerbi"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
    # Not supporting the pattern
    # dashboard_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    # chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


class PowerBiAPI:
    # API endpoints of PowerBi to fetch dashboards, tiles, datasets
    API_ENDPOINTS = {
        Constant.DASHBOARD_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards",
        Constant.ENTITY_USER_LIST: "{POWERBI_ADMIN_BASE_URL}/{ENTITY}/{ENTITY_ID}/users",
        Constant.TILE_LIST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/dashboards/{DASHBOARD_ID}/tiles",
        Constant.DATASET_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}",
        Constant.DATASOURCE_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}/datasources",
        Constant.REPORT_GET: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/reports/{REPORT_ID}",
        Constant.SCAN_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanStatus/{SCAN_ID}",
        Constant.SCAN_RESULT_GET: "{POWERBI_ADMIN_BASE_URL}/workspaces/scanResult/{SCAN_ID}",
        Constant.SCAN_CREATE: "{POWERBI_ADMIN_BASE_URL}/workspaces/getInfo",
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
        datasets: Dict

    @dataclass
    class DataSource:
        """
        PowerBi
        """

        @dataclass
        class MetaData:
            """
            MetaData about DataSource
            """

            is_relational: Boolean

        id: str
        type: str
        database: Optional[str]
        server: Optional[str]
        metadata: Any

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.DataSource)
                and self.__members() == instance.__members()
            )

        def __hash__(self):
            return hash(self.__members())

    # dataclasses for PowerBi Dashboard
    @dataclass
    class Dataset:
        @dataclass
        class Table:
            name: str
            schema_name: str

        id: str
        name: str
        webUrl: Optional[str]
        workspace_id: str
        datasource: Any
        # Table in datasets
        tables: List[Any]

        def get_urn_part(self):
            return f"datasets.{self.id}"

        def __members(self):
            return (self.id,)

        def __eq__(self, instance):
            return (
                isinstance(instance, PowerBiAPI.Dataset)
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
        dataset: Any

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
        dataset: Optional[Any]
        report: Optional[Any]
        createdFrom: CreatedFrom

        def get_urn_part(self):
            return f"charts.{self.id}"

    @dataclass
    class User:
        id: str
        displayName: str
        emailAddress: str
        dashboardUserAccessRight: str
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
    class Dashboard:
        id: str
        displayName: str
        embedUrl: str
        webUrl: str
        isReadOnly: Any
        workspace_id: str
        workspace_name: str
        tiles: List[Any]
        users: List[Any]

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
        LOGGER.info("Trying to connect to {}".format(self.__get_authority_url()))
        self.get_access_token()
        LOGGER.info("Able to connect to {}".format(self.__get_authority_url()))

    def __get_authority_url(self):
        return "{}{}".format(PowerBiAPI.AUTHORITY, self.__config.tenant_id)

    def __get_users(self, workspace_id: str, entity: str, id: str) -> List[User]:
        """
        Get user for the given PowerBi entity
        """
        users: List[PowerBiAPI.User] = []
        if self.__config.extract_ownership is False:
            LOGGER.info(
                "ExtractOwnership capabilities is disabled from configuration and hence returning empty users list"
            )
            return users

        user_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.ENTITY_USER_LIST]
        # Replace place holders
        user_list_endpoint = user_list_endpoint.format(
            POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL,
            ENTITY=entity,
            ENTITY_ID=id,
        )
        # Hit PowerBi
        LOGGER.info(f"Request to URL={user_list_endpoint}")
        response = requests.get(
            user_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            LOGGER.warning(
                f"Failed to fetch user list from power-bi for, http_status={response.status_code}, message={response.text}"
            )

            LOGGER.info(f"{Constant.WorkspaceId}={workspace_id}")
            LOGGER.info(f"{Constant.ENTITY}={entity}")
            LOGGER.info(f"{Constant.ID}={id}")
            raise ConnectionError("Failed to fetch the user list from the power-bi")

        users_dict: List[Any] = response.json()[Constant.VALUE]

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        users = [
            PowerBiAPI.User(
                id=instance.get("identifier"),
                displayName=instance.get("displayName"),
                emailAddress=instance.get("emailAddress"),
                dashboardUserAccessRight=instance.get("datasetUserAccessRight"),
                graphId=instance.get("graphId"),
                principalType=instance.get("principalType"),
            )
            for instance in users_dict
        ]

        return users

    def __get_report(self, workspace_id: str, report_id: str) -> Any:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if workspace_id is None or report_id is None:
            LOGGER.info("Input values are None")
            LOGGER.info(f"{Constant.WorkspaceId}={workspace_id}")
            LOGGER.info(f"{Constant.ReportId}={report_id}")
            return None

        report_get_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.REPORT_GET]
        # Replace place holders
        report_get_endpoint = report_get_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
            WORKSPACE_ID=workspace_id,
            REPORT_ID=report_id,
        )
        # Hit PowerBi
        LOGGER.info(f"Request to report URL={report_get_endpoint}")
        response = requests.get(
            report_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi for"
            LOGGER.warning(message)
            LOGGER.warning(f"{Constant.WorkspaceId}={workspace_id}")
            LOGGER.warning(f"{Constant.ReportId}={report_id}")
            raise ConnectionError(message)

        response_dict = response.json()

        return PowerBiAPI.Report(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl=response_dict.get("webUrl"),
            embedUrl=response_dict.get("embedUrl"),
            dataset=self.get_dataset(
                workspace_id=workspace_id, dataset_id=response_dict.get("datasetId")
            ),
        )

    def get_access_token(self):
        if self.__access_token != "":
            LOGGER.info("Returning the cached access token")
            return self.__access_token

        LOGGER.info("Generating PowerBi access token")

        auth_response = self.__msal_client.acquire_token_for_client(
            scopes=[PowerBiAPI.SCOPE]
        )

        if not auth_response.get("access_token"):
            LOGGER.warn(
                "Failed to generate the PowerBi access token. Please check input configuration"
            )
            raise ConfigurationError(
                "Powerbi authorization failed . Please check your input configuration."
            )

        LOGGER.info("Generated PowerBi access token")

        self.__access_token = "Bearer {}".format(auth_response.get("access_token"))

        LOGGER.debug(f"{Constant.PBIAccessToken}={self.__access_token}")

        return self.__access_token

    def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
        """
        Return list of dashboard users
        """
        return self.__get_users(
            workspace_id=dashboard.workspace_id, entity="dashboards", id=dashboard.id
        )

    def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
        """
        Get the list of dashboard from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get-dashboards), there is no information available on pagination
        """
        dashboard_list_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.DASHBOARD_LIST]
        # Replace place holders
        dashboard_list_endpoint = dashboard_list_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL, WORKSPACE_ID=workspace.id
        )
        # Hit PowerBi
        LOGGER.info(f"Request to URL={dashboard_list_endpoint}")
        response = requests.get(
            dashboard_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            LOGGER.warning("Failed to fetch dashboard list from power-bi for")
            LOGGER.warning(f"{Constant.WorkspaceId}={workspace.id}")
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
            LOGGER.info("Input values are None")
            LOGGER.info(f"{Constant.WorkspaceId}={workspace_id}")
            LOGGER.info(f"{Constant.DatasetId}={dataset_id}")
            return None

        dataset_get_endpoint: str = PowerBiAPI.API_ENDPOINTS[Constant.DATASET_GET]
        # Replace place holders
        dataset_get_endpoint = dataset_get_endpoint.format(
            POWERBI_BASE_URL=PowerBiAPI.BASE_URL,
            WORKSPACE_ID=workspace_id,
            DATASET_ID=dataset_id,
        )
        # Hit PowerBi
        LOGGER.info(f"Request to dataset URL={dataset_get_endpoint}")
        response = requests.get(
            dataset_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch dataset from power-bi for"
            LOGGER.warning(message)
            LOGGER.warning(f"{Constant.WorkspaceId}={workspace_id}")
            LOGGER.warning(f"{Constant.DatasetId}={dataset_id}")
            raise ConnectionError(message)

        response_dict = response.json()
        LOGGER.debug("datasets = {}".format(response_dict))
        # PowerBi Always return the webURL, in-case if it is None then setting complete webURL to None instead of None/details
        return PowerBiAPI.Dataset(
            id=response_dict.get("id"),
            name=response_dict.get("name"),
            webUrl="{}/details".format(response_dict.get("webUrl"))
            if response_dict.get("webUrl") is not None
            else None,
            workspace_id=workspace_id,
            tables=[],
            datasource=None,
        )

    def get_data_source(self, dataset: Dataset) -> Any:
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
        LOGGER.info(f"Request to datasource URL={datasource_get_endpoint}")
        response = requests.get(
            datasource_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch datasource from power-bi for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.WorkspaceId, dataset.workspace_id))
            LOGGER.warning("{}={}".format(Constant.DatasetId, dataset.id))
            LOGGER.warning("{}={}".format(Constant.HTTP_RESPONSE_TEXT, response.text))
            LOGGER.warning(
                "{}={}".format(Constant.HTTP_RESPONSE_STATUS_CODE, response.status_code)
            )

            raise ConnectionError(message)

        res = response.json()
        value = res["value"]
        if len(value) == 0:
            LOGGER.info(
                f"datasource is not found for dataset {dataset.name}({dataset.id})"
            )

            return None

        if len(value) > 1:
            # We are currently supporting data-set having single relational database
            LOGGER.warning(
                "More than one data-source found for {}({})".format(
                    dataset.name, dataset.id
                )
            )
            LOGGER.debug(value)
            return None

        # Consider only zero index datasource
        datasource_dict = value[0]
        LOGGER.debug("data-sources = {}".format(value))
        # Create datasource instance with basic detail available
        datasource = PowerBiAPI.DataSource(
            id=datasource_dict.get(
                "datasourceId"
            ),  # datasourceId is not available in all cases
            type=datasource_dict["datasourceType"],
            server=None,
            database=None,
            metadata=None,
        )

        # Check if datasource is relational as per our relation mapping
        if self.__config.dataset_type_mapping.get(datasource.type) is not None:
            # Now set the database detail as it is relational data source
            datasource.metadata = PowerBiAPI.DataSource.MetaData(is_relational=True)
            datasource.database = datasource_dict["connectionDetails"]["database"]
            datasource.server = datasource_dict["connectionDetails"]["server"]
        else:
            datasource.metadata = PowerBiAPI.DataSource.MetaData(is_relational=False)
            LOGGER.warning(
                "Non relational data-source found = {}".format(datasource_dict)
            )

        return datasource

    def get_tiles(self, workspace: Workspace, dashboard: Dashboard) -> List[Tile]:

        """
        Get the list of tiles from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get-tiles), there is no information available on pagination
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
                    self.__get_report(
                        workspace_id=workspace.id,
                        report_id=tile_instance.get("reportId"),
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

            LOGGER.info(
                f'Tile {tile_instance.get("title")}({tile_instance.get("id")}) is created from {report_fields["createdFrom"]}'
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
        LOGGER.info("Request to URL={}".format(tile_list_endpoint))
        response = requests.get(
            tile_list_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            LOGGER.warning("Failed to fetch tiles list from power-bi for")
            LOGGER.warning("{}={}".format(Constant.WorkspaceId, workspace.id))
            LOGGER.warning("{}={}".format(Constant.DashboardId, dashboard.id))
            raise ConnectionError("Failed to fetch the tile list from the power-bi")

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        tile_dict: List[Any] = response.json()[Constant.VALUE]
        LOGGER.debug("Tile Dict = {}".format(tile_dict))
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

    # flake8: noqa: C901
    def get_workspace(self, workspace_id: str) -> Workspace:
        """
        Return Workspace for the given workspace identifier i.e workspace_id
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

                LOGGER.warning(message)

                raise ConnectionError(message)
            # Return Id of Scan created for the given workspace
            id = res.json()["id"]
            LOGGER.info("Scan id({})".format(id))
            return id

        def wait_for_scan_to_complete(scan_id: str, timeout: int) -> Boolean:
            """
            Poll the PowerBi service for workspace scan to complete
            """
            minimum_sleep = 3
            if timeout < minimum_sleep:
                LOGGER.info(
                    f"Setting timeout to minimum_sleep time {minimum_sleep} seconds"
                )
                timeout = minimum_sleep

            max_trial = timeout // minimum_sleep
            LOGGER.info(f"Max trial {max_trial}")
            scan_get_endpoint = PowerBiAPI.API_ENDPOINTS[Constant.SCAN_GET]
            scan_get_endpoint = scan_get_endpoint.format(
                POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL, SCAN_ID=scan_id
            )

            LOGGER.info(f"Hitting URL={scan_get_endpoint}")

            trail = 1
            while True:
                LOGGER.info(f"Trial = {trail}")
                res = requests.get(
                    scan_get_endpoint,
                    headers={Constant.Authorization: self.get_access_token()},
                )
                if res.status_code != 200:
                    message = f"API({scan_get_endpoint}) return error code {res.status_code} for scan id({scan_id})"

                    LOGGER.warning(message)

                    raise ConnectionError(message)

                if res.json()["status"].upper() == "Succeeded".upper():
                    LOGGER.info(f"Scan result is available for scan id({scan_id})")
                    return True

                if trail == max_trial:
                    break
                LOGGER.info(f"Sleeping for {minimum_sleep} seconds")
                sleep(minimum_sleep)
                trail += 1

            # Result is not available
            return False

        def get_scan_result(scan_id: str) -> dict:
            LOGGER.info("Fetching scan  result")
            LOGGER.info(f"{Constant.SCAN_ID}={scan_id}")
            scan_result_get_endpoint = PowerBiAPI.API_ENDPOINTS[
                Constant.SCAN_RESULT_GET
            ]
            scan_result_get_endpoint = scan_result_get_endpoint.format(
                POWERBI_ADMIN_BASE_URL=PowerBiAPI.ADMIN_BASE_URL, SCAN_ID=scan_id
            )

            LOGGER.info(f"Hitting URL={scan_result_get_endpoint}")
            res = requests.get(
                scan_result_get_endpoint,
                headers={Constant.Authorization: self.get_access_token()},
            )
            if res.status_code != 200:
                message = f"API({scan_result_get_endpoint}) return error code {res.status_code} for scan id({scan_id})"

                LOGGER.warning(message)

                raise ConnectionError(message)

            return res.json()["workspaces"][0]

        def json_to_dataset_map(scan_result: dict) -> dict:
            """
            Filter out "dataset" from scan_result and return PowerBiAPI.Dataset instance set
            """
            datasets: Optional[Any] = scan_result.get("datasets")
            dataset_map: dict = {}

            if datasets is None or len(datasets) == 0:
                LOGGER.warning(
                    f'Workspace {scan_result["name"]}({scan_result["id"]}) does not have datasets'
                )

                LOGGER.info("Returning empty datasets")
                return dataset_map

            for dataset_dict in datasets:
                dataset_instance: PowerBiAPI.Dataset = self.get_dataset(
                    workspace_id=scan_result["id"],
                    dataset_id=dataset_dict["id"],
                )

                dataset_map[dataset_instance.id] = dataset_instance
                # set dataset's DataSource
                dataset_instance.datasource = self.get_data_source(dataset_instance)
                # Set table only if the datasource is relational and dataset is not created from custom SQL i.e Value.NativeQuery(
                # There are dataset which doesn't have DataSource
                if (
                    dataset_instance.datasource
                    and dataset_instance.datasource.metadata.is_relational is True
                ):
                    LOGGER.info(
                        f"Processing tables attribute for dataset {dataset_instance.name}({dataset_instance.id})"
                    )

                    for table in dataset_dict["tables"]:
                        if "Value.NativeQuery(" in table["source"][0]["expression"]:
                            LOGGER.warning(
                                f'Table {table["name"]} is created from Custom SQL. Ignoring in processing'
                            )

                            continue

                        # PowerBi table name contains schema name and table name. Format is <SchemaName> <TableName>
                        schema_and_name = table["name"].split(" ")
                        dataset_instance.tables.append(
                            PowerBiAPI.Dataset.Table(
                                schema_name=schema_and_name[0],
                                name=schema_and_name[1],
                            )
                        )

            return dataset_map

        def init_dashboard_tiles(workspace: PowerBiAPI.Workspace) -> None:
            for dashboard in workspace.dashboards:
                dashboard.tiles = self.get_tiles(workspace, dashboard=dashboard)

            return None

        LOGGER.info("Creating scan job for workspace")
        LOGGER.info("{}={}".format(Constant.WorkspaceId, workspace_id))
        LOGGER.info("Hitting URL={}".format(scan_create_endpoint))
        scan_id = create_scan_job()
        LOGGER.info("Waiting for scan to complete")
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
        LOGGER.debug("scan result = {}".format(scan_result))
        workspace = PowerBiAPI.Workspace(
            id=scan_result["id"],
            name=scan_result["name"],
            state=scan_result["state"],
            datasets={},
            dashboards=[],
        )
        # Get workspace dashboards
        workspace.dashboards = self.get_dashboards(workspace)
        workspace.datasets = json_to_dataset_map(scan_result)
        init_dashboard_tiles(workspace)

        return workspace


class Mapper:
    """
    Transfrom PowerBi concepts Dashboard, Dataset and Tile to DataHub concepts Dashboard, Dataset and Chart
    """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """
        We can add EquableMetadataWorkUnit to set.
        This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
        """

        def __eq__(self, instance):
            return self.id == instance.id

        def __hash__(self):
            return id(self.id)

    def __init__(self, config: PowerBiDashboardSourceConfig):
        self.__config = config

    def new_mcp(
        self,
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
            aspect=aspect,
        )

    def __to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def __to_datahub_dataset(
        self, dataset: Optional[PowerBiAPI.Dataset]
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
        In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.
        """

        dataset_mcps: List[MetadataChangeProposalWrapper] = []
        if dataset is None:
            return dataset_mcps

        # We are only supporting relation PowerBi DataSources
        if (
            dataset.datasource is None
            or dataset.datasource.metadata.is_relational is False
        ):
            LOGGER.warning(
                f"Dataset {dataset.name}({dataset.id}) is not created from relational datasource"
            )

            return dataset_mcps

        LOGGER.info(
            f"Converting dataset={dataset.name}(id={dataset.id}) to datahub dataset"
        )

        for table in dataset.tables:
            # Create an URN for dataset
            ds_urn = builder.make_dataset_urn(
                platform=self.__config.dataset_type_mapping[dataset.datasource.type],
                name=f"{dataset.datasource.database}.{table.schema_name}.{table.name}",
                env=self.__config.env,
            )

            LOGGER.info(f"{Constant.Dataset_URN}={ds_urn}")
            # Create datasetProperties mcp
            ds_properties = DatasetPropertiesClass(description=table.name)

            info_mcp = self.new_mcp(
                entity_type=Constant.DATASET,
                entity_urn=ds_urn,
                aspect_name=Constant.DATASET_PROPERTIES,
                aspect=ds_properties,
            )

            # Remove status mcp
            status_mcp = self.new_mcp(
                entity_type=Constant.DATASET,
                entity_urn=ds_urn,
                aspect_name=Constant.STATUS,
                aspect=StatusClass(removed=False),
            )

            dataset_mcps.extend([info_mcp, status_mcp])

        return dataset_mcps

    def __to_datahub_chart(
        self, tile: PowerBiAPI.Tile, ds_mcps: List[MetadataChangeProposalWrapper]
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi tile to datahub chart
        """
        LOGGER.info("Converting tile {}(id={}) to chart".format(tile.title, tile.id))
        # Create an URN for chart
        chart_urn = builder.make_chart_urn(
            self.__config.platform_name, tile.get_urn_part()
        )

        LOGGER.info("{}={}".format(Constant.CHART_URN, chart_urn))

        ds_input: List[str] = self.to_urn_set(ds_mcps)

        def tile_custom_properties(tile: PowerBiAPI.Tile) -> dict:
            custom_properties = {
                "datasetId": tile.dataset.id if tile.dataset else "",
                "reportId": tile.report.id if tile.report else "",
                "datasetWebUrl": tile.dataset.webUrl
                if tile.dataset is not None
                else "",
                "createdFrom": tile.createdFrom.value,
            }

            return custom_properties

        # Create chartInfo mcp
        # Set chartUrl only if tile is created from Report
        chart_info_instance = ChartInfoClass(
            title=tile.title or "",
            description=tile.title or "",
            lastModified=ChangeAuditStamps(),
            inputs=ds_input,
            externalUrl=tile.report.webUrl if tile.report else None,
            customProperties={**tile_custom_properties(tile)},
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_INFO,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # ChartKey status
        chart_key_instance = ChartKeyClass(
            dashboardTool=self.__config.platform_name,
            chartId=Constant.CHART_ID.format(tile.id),
        )

        chartkey_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_KEY,
            aspect=chart_key_instance,
        )

        return [info_mcp, status_mcp, chartkey_mcp]

    # written in this style to fix linter error
    def to_urn_set(self, mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
        return deduplicate_list(
            [
                mcp.entityUrn
                for mcp in mcps
                if mcp is not None and mcp.entityUrn is not None
            ]
        )

    def __to_datahub_dashboard(
        self,
        dashboard: PowerBiAPI.Dashboard,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dashboard to Datahub dashboard
        """

        dashboard_urn = builder.make_dashboard_urn(
            self.__config.platform_name, dashboard.get_urn_part()
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        def chart_custom_properties(dashboard: PowerBiAPI.Dashboard) -> dict:
            return {
                "chartCount": str(len(dashboard.tiles)),
                "workspaceName": dashboard.workspace_name,
                "workspaceId": dashboard.id,
            }

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=dashboard.displayName or "",
            title=dashboard.displayName or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=dashboard.webUrl,
            customProperties={**chart_custom_properties(dashboard)},
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_INFO,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_KEY,
            aspect=dashboard_key_cls,
        )

        # Dashboard Ownership
        owners = [
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            # Dashboard owner MCP
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.new_mcp(
                entity_type=Constant.DASHBOARD,
                entity_urn=dashboard_urn,
                aspect_name=Constant.OWNERSHIP,
                aspect=ownership,
            )

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=["/powerbi/{}".format(self.__config.workspace_id)]
        )
        browse_path_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.BROWSERPATH,
            aspect=browse_path,
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
        ]

        if owner_mcp is not None:
            list_of_mcps.append(owner_mcp)

        return list_of_mcps

    def to_datahub_user(
        self, user: PowerBiAPI.User
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi user to datahub user
        """

        LOGGER.info(
            f"Converting user {user.displayName}(id={user.id}) to datahub's user"
        )

        # Create an URN for user
        user_urn = builder.make_user_urn(user.get_urn_part())

        user_info_instance = CorpUserInfoClass(
            displayName=user.displayName,
            email=user.emailAddress,
            title=user.displayName,
            active=True,
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_INFO,
            aspect=user_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        user_key = CorpUserKeyClass(username=user.id)

        user_key_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_KEY,
            aspect=user_key,
        )

        return [info_mcp, status_mcp, user_key_mcp]

    def to_datahub_users(
        self, users: List[PowerBiAPI.User]
    ) -> List[MetadataChangeProposalWrapper]:
        user_mcps = []

        for user in users:
            user_mcps.extend(self.to_datahub_user(user))

        return user_mcps

    def to_datahub_chart(
        self, tiles: List[PowerBiAPI.Tile]
    ) -> Tuple[
        List[MetadataChangeProposalWrapper], List[MetadataChangeProposalWrapper]
    ]:
        ds_mcps = []
        chart_mcps = []

        # Return empty list if input list is empty
        if not tiles:
            return [], []

        LOGGER.info(f"Converting tiles(count={len(tiles)}) to charts")

        for tile in tiles:
            if tile is None:
                continue
            # First convert the dataset to MCP, because dataset mcp is used in input attribute of chart mcp
            dataset_mcps = self.__to_datahub_dataset(tile.dataset)
            # Now convert tile to chart MCP
            chart_mcp = self.__to_datahub_chart(tile, dataset_mcps)

            ds_mcps.extend(dataset_mcps)
            chart_mcps.extend(chart_mcp)

        # Return dataset and chart MCPs

        return ds_mcps, chart_mcps

    def to_datahub_work_units(
        self, dashboard: PowerBiAPI.Dashboard
    ) -> List[EquableMetadataWorkUnit]:
        mcps = []

        LOGGER.info(
            f"Converting dashboard={dashboard.displayName} to datahub dashboard"
        )

        # Convert user to CorpUser
        user_mcps = self.to_datahub_users(dashboard.users)
        # Convert tiles to charts
        ds_mcps, chart_mcps = self.to_datahub_chart(dashboard.tiles)
        # Lets convert dashboard to datahub dashboard
        dashboard_mcps = self.__to_datahub_dashboard(dashboard, chart_mcps, user_mcps)

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(dashboard_mcps)

        # Convert MCP to work_units
        work_units = map(self.__to_work_unit, mcps)
        # Return set of work_unit
        return deduplicate_list([wu for wu in work_units if wu is not None])


@dataclass
class PowerBiDashboardSourceReport(SourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)

    def report_dashboards_scanned(self, count: int = 1) -> None:
        self.dashboards_scanned += count

    def report_charts_scanned(self, count: int = 1) -> None:
        self.charts_scanned += count

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)


@platform_name("PowerBI")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.OWNERSHIP, "On by default but can disabled by configuration"
)
class PowerBiDashboardSource(Source):
    """
    This plugin extracts the following:
    - Power BI dashboards, tiles and datasets
    - Names, descriptions and URLs of dashboard and tile
    - Owners of dashboards
    """

    source_config: PowerBiDashboardSourceConfig
    reporter: PowerBiDashboardSourceReport
    accessed_dashboards: int = 0

    def __init__(self, config: PowerBiDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = PowerBiDashboardSourceReport()
        self.auth_token = PowerBiAPI(self.source_config).get_access_token()
        self.powerbi_client = PowerBiAPI(self.source_config)
        self.mapper = Mapper(config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        LOGGER.info("PowerBi plugin execution is started")

        # Fetch PowerBi workspace for given workspace identifier
        workspace = self.powerbi_client.get_workspace(self.source_config.workspace_id)

        for dashboard in workspace.dashboards:

            try:
                # Fetch PowerBi users for dashboards
                dashboard.users = self.powerbi_client.get_dashboard_users(dashboard)
                # Increase dashboard and tiles count in report
                self.reporter.report_dashboards_scanned()
                self.reporter.report_charts_scanned(count=len(dashboard.tiles))
            except Exception as e:
                message = f"Error ({e}) occurred while loading dashboard {dashboard.displayName}(id={dashboard.id}) tiles."

                LOGGER.exception(message, e)
                self.reporter.report_warning(dashboard.id, message)
            # Convert PowerBi Dashboard and child entities to Datahub work unit to ingest into Datahub
            workunits = self.mapper.to_datahub_work_units(dashboard)
            for workunit in workunits:
                # Add workunit to report
                self.reporter.report_workunit(workunit)
                # Return workunit to Datahub Ingestion framework
                yield workunit

    def get_report(self) -> SourceReport:
        return self.reporter
