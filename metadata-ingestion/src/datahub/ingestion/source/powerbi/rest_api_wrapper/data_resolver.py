import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Union

import msal
import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.source.powerbi.config import Constant
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    App,
    Column,
    Dashboard,
    Measure,
    MeasureProfile,
    Page,
    PowerBIDataset,
    Report,
    ReportType,
    Table,
    Tile,
    User,
    Workspace,
    new_powerbi_dataset,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.profiling_utils import (
    process_column_result,
    process_sample_result,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.query import DaxQuery

# Logger instance
logger = logging.getLogger(__name__)


def is_permission_error(e: Exception) -> bool:
    if not isinstance(e, requests.exceptions.HTTPError):
        return False

    return e.response.status_code == 401 or e.response.status_code == 403


def is_http_failure(response: Response, message: str) -> bool:
    if response.ok:
        # It is not failure so no need to log the message just return with False
        return False

    logger.info(message)
    logger.debug(f"HTTP Status Code = {response.status_code}")
    logger.debug(f"HTTP Error Message = {response.text}")
    return True


class SessionWithTimeout(requests.Session):
    timeout: int

    def __init__(self, timeout, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout

    def request(self, method, url, *args, **kwargs):
        # Set the default timeout if none is provided
        kwargs.setdefault("timeout", self.timeout)
        return super().request(method, url, *args, **kwargs)


class DataResolverBase(ABC):
    SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
    MY_ORG_URL = "https://api.powerbi.com/v1.0/myorg"
    BASE_URL: str = f"{MY_ORG_URL}/groups"
    ADMIN_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg/admin"
    AUTHORITY: str = "https://login.microsoftonline.com/"
    TOP: int = 1000

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        metadata_api_timeout: int,
    ):
        self._access_token: Optional[str] = None
        self._access_token_expiry_time: Optional[datetime] = None

        self._tenant_id = tenant_id
        # Test connection by generating access token
        logger.info(f"Trying to connect to {self._get_authority_url()}")
        # Power-Bi Auth (Service Principal Auth)
        self._msal_client = msal.ConfidentialClientApplication(
            client_id,
            client_credential=client_secret,
            authority=DataResolverBase.AUTHORITY + tenant_id,
        )
        self.get_access_token()

        logger.info(f"Connected to {self._get_authority_url()}")

        self._request_session = SessionWithTimeout(timeout=metadata_api_timeout)

        # set re-try parameter for request_session
        self._request_session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=1,
                    allowed_methods=None,
                    status_forcelist=[429, 500, 502, 503, 504],
                )
            ),
        )

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
    def profile_dataset(
        self,
        dataset: PowerBIDataset,
        table: Table,
        workspace_name: str,
        profile_pattern: Optional[AllowDenyPattern],
    ) -> None:
        pass

    @abstractmethod
    def get_dataset(
        self, workspace: Workspace, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        pass

    @abstractmethod
    def get_dataset_parameters(
        self, workspace_id: str, dataset_id: str
    ) -> Dict[str, str]:
        pass

    @abstractmethod
    def get_users(self, workspace_id: str, entity: str, entity_id: str) -> List[User]:
        pass

    @abstractmethod
    def _get_app(
        self,
        app_id: str,
    ) -> Optional[Dict]:
        pass

    def _get_authority_url(self):
        return f"{DataResolverBase.AUTHORITY}{self._tenant_id}"

    def get_authorization_header(self):
        return {Constant.Authorization: self.get_access_token()}

    def get_access_token(self) -> str:
        if self._access_token is not None and not self._is_access_token_expired():
            return self._access_token

        logger.info("Generating PowerBi access token")

        auth_response = self._msal_client.acquire_token_for_client(
            scopes=[DataResolverBase.SCOPE]
        )

        if not auth_response.get(Constant.ACCESS_TOKEN):
            logger.warning(
                "Failed to generate the PowerBi access token. Please check input configuration"
            )
            raise ConfigurationError(
                "Failed to retrieve access token for PowerBI principal. Please verify your configuration values"
            )

        logger.info("Generated PowerBi access token")

        self._access_token = "Bearer {}".format(
            auth_response.get(Constant.ACCESS_TOKEN)
        )
        safety_gap = 300
        self._access_token_expiry_time = datetime.now() + timedelta(
            seconds=(
                max(auth_response.get(Constant.ACCESS_TOKEN_EXPIRY, 0) - safety_gap, 0)
            )
        )

        logger.debug(f"{Constant.PBIAccessToken}={self._access_token}")

        return self._access_token

    def _is_access_token_expired(self) -> bool:
        if not self._access_token_expiry_time:
            return True
        return self._access_token_expiry_time < datetime.now()

    def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
        """
        Get the list of dashboard from PowerBi for the given workspace identifier

        TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get
        -dashboards), there is no information available on pagination
        """
        dashboard_list_endpoint: str = self.get_dashboards_endpoint(workspace)

        logger.debug(f"Request to URL={dashboard_list_endpoint}")
        response = self._request_session.get(
            dashboard_list_endpoint,
            headers=self.get_authorization_header(),
        )

        response.raise_for_status()

        dashboards_dict: List[Any] = response.json()[Constant.VALUE]

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        dashboards: List[Dashboard] = [
            Dashboard(
                id=instance.get(Constant.ID),
                isReadOnly=instance.get(Constant.IS_READ_ONLY),
                displayName=instance.get(Constant.DISPLAY_NAME),
                description=instance.get(Constant.DESCRIPTION, ""),
                embedUrl=instance.get(Constant.EMBED_URL),
                webUrl=instance.get(Constant.WEB_URL),
                workspace_id=workspace.id,
                workspace_name=workspace.name,
                tiles=[],
                users=[],
                tags=[],
            )
            for instance in dashboards_dict
            if (
                instance is not None
                and Constant.APP_ID
                not in instance  # As we add dashboards to the App, Power BI starts
                # providing duplicate dashboard information,
                # where the duplicate includes an AppId, while the original dashboard does not.
            )
        ]

        return dashboards

    def get_groups(self, filter_: Dict) -> List[dict]:
        group_endpoint = self.get_groups_endpoint()

        output: List[dict] = []

        for page in self.itr_pages(
            endpoint=group_endpoint,
            parameter_override=filter_,
        ):
            output.extend(page)

        return output

    def get_reports(
        self, workspace: Workspace, _filter: Optional[str] = None
    ) -> List[Report]:
        reports_endpoint = self.get_reports_endpoint(workspace)
        # Hit PowerBi
        logger.debug(f"Request to report URL={reports_endpoint}")
        params: Optional[dict] = None
        if _filter is not None:
            params = {"$filter": _filter}

        def fetch_reports():
            response = self._request_session.get(
                reports_endpoint,
                headers=self.get_authorization_header(),
                params=params,
            )
            response.raise_for_status()
            response_dict = response.json()
            logger.debug(f"Report Request response = {response_dict}")
            return response_dict.get(Constant.VALUE, [])

        reports: List[Report] = [
            Report(
                id=raw_instance.get(Constant.ID),
                name=raw_instance.get(Constant.NAME),
                type=ReportType[raw_instance.get(Constant.REPORT_TYPE)],
                webUrl=raw_instance.get(Constant.WEB_URL),
                embedUrl=raw_instance.get(Constant.EMBED_URL),
                description=raw_instance.get(Constant.DESCRIPTION, ""),
                pages=self._get_pages_by_report(
                    workspace=workspace, report_id=raw_instance[Constant.ID]
                ),
                dataset_id=raw_instance.get(Constant.DATASET_ID),
                users=[],  # It will be fetched using Admin Fetcher based on condition
                tags=[],  # It will be fetched using Admin Fetcher based on condition
                dataset=None,  # It will come from dataset_registry defined in powerbi_api.py
            )
            for raw_instance in fetch_reports()
            if Constant.APP_ID
            not in raw_instance  # As we add reports to the App, Power BI starts providing
            # duplicate report information,
            # where the duplicate includes an AppId,
            # while the original report does not.
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
        tile_list_endpoint: str = self.get_tiles_endpoint(
            workspace, dashboard_id=dashboard.id
        )
        # Hit PowerBi
        logger.debug(f"Request to URL={tile_list_endpoint}")
        response = self._request_session.get(
            tile_list_endpoint,
            headers=self.get_authorization_header(),
        )
        logger.debug(f"Request response = {response}")
        response.raise_for_status()

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        tile_dict: List[Any] = response.json().get(Constant.VALUE, [])
        logger.debug(f"Tile Dict = {tile_dict}")
        tiles: List[Tile] = [
            Tile(
                id=instance.get(Constant.ID),
                title=instance.get(Constant.TITLE),
                embedUrl=instance.get(Constant.EMBED_URL),
                dataset_id=instance.get(Constant.DATASET_ID),
                report_id=instance.get(Constant.REPORT_ID),
                dataset=None,
                report=None,
                createdFrom=(
                    # In the past we considered that only one of the two report_id or dataset_id would be present
                    # but we have seen cases where both are present. If both are present, we prioritize the report.
                    Tile.CreatedFrom.REPORT
                    if instance.get(Constant.REPORT_ID)
                    else Tile.CreatedFrom.DATASET
                    if instance.get(Constant.DATASET_ID)
                    else Tile.CreatedFrom.VISUALIZATION
                ),
            )
            for instance in tile_dict
            if instance is not None
        ]

        return tiles

    def itr_pages(
        self,
        endpoint: str,
        parameter_override: Optional[Dict] = None,
    ) -> Iterator[List[Dict]]:
        parameter_override = parameter_override or {}
        params: dict = {
            "$skip": 0,
            "$top": self.TOP,
            **parameter_override,
        }

        page_number: int = 0

        while True:
            params["$skip"] = self.TOP * page_number
            response = self._request_session.get(
                endpoint,
                headers=self.get_authorization_header(),
                params=params,
            )

            response.raise_for_status()

            assert Constant.VALUE in response.json(), (
                "'value' key is not present in paginated response"
            )

            if not response.json()[Constant.VALUE]:  # if it is an empty list then break
                break

            yield response.json()[Constant.VALUE]

            page_number += 1

    def get_app(
        self,
        app_id: str,
    ) -> Optional[App]:
        raw_app: Optional[Dict] = self._get_app(
            app_id=app_id,
        )

        if raw_app is None:
            return None

        assert Constant.ID in raw_app, (
            f"{Constant.ID} is required field not present in server response"
        )

        assert Constant.NAME in raw_app, (
            f"{Constant.NAME} is required field not present in server response"
        )

        return App(
            id=raw_app[Constant.ID],
            name=raw_app[Constant.NAME],
            description=raw_app.get(Constant.DESCRIPTION),
            last_update=raw_app.get(Constant.LAST_UPDATE),
            dashboards=[],  # dashboards and reports of App are available in scan-result response
            reports=[],  # There is an App section in documentation https://learn.microsoft.com/en-us/rest/api/power-bi/dashboards/get-dashboards-in-group#code-try-0
            # However the report API mentioned in that section is not returning the reports
            # We will collect these details from the scan-result.
        )


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
        Constant.DATASET_EXECUTE_QUERIES: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries",
        Constant.GET_WORKSPACE_APP: "{MY_ORG_URL}/apps/{APP_ID}",
    }

    def get_dataset(
        self, workspace: Workspace, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if workspace.id is None or dataset_id is None:
            logger.debug("Input values are None")
            logger.debug(f"{Constant.WorkspaceId}={workspace.id}")
            logger.debug(f"{Constant.DatasetId}={dataset_id}")
            return None

        dataset_get_endpoint: str = RegularAPIResolver.API_ENDPOINTS[
            Constant.DATASET_GET
        ]
        # Replace place holders
        dataset_get_endpoint = dataset_get_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace.id,
            DATASET_ID=dataset_id,
        )
        # Hit PowerBi
        logger.debug(f"Request to dataset URL={dataset_get_endpoint}")
        response = self._request_session.get(
            dataset_get_endpoint,
            headers=self.get_authorization_header(),
        )
        # Check if we got a response from PowerBi
        response.raise_for_status()
        response_dict = response.json()
        logger.debug(f"datasets = {response_dict}")
        # PowerBi Always return the webURL, in-case if it is None, then setting complete webURL to None instead of
        # None/details
        return new_powerbi_dataset(workspace, response_dict)

    def get_dataset_parameters(
        self, workspace_id: str, dataset_id: str
    ) -> Dict[str, str]:
        dataset_get_endpoint: str = RegularAPIResolver.API_ENDPOINTS[
            Constant.DATASET_GET
        ]
        dataset_get_endpoint = dataset_get_endpoint.format(
            POWERBI_BASE_URL=DataResolverBase.BASE_URL,
            WORKSPACE_ID=workspace_id,
            DATASET_ID=dataset_id,
        )
        logger.debug(f"Request to dataset URL={dataset_get_endpoint}")
        params_get_endpoint = dataset_get_endpoint + "/parameters"

        params_response = self._request_session.get(
            params_get_endpoint,
            headers=self.get_authorization_header(),
        )
        params_response.raise_for_status()
        params_dict = params_response.json()

        params_values: List[dict] = params_dict.get(Constant.VALUE, [])

        logger.debug(f"dataset {dataset_id} parameters = {params_values}")

        return {
            value[Constant.NAME]: value[Constant.CURRENT_VALUE]
            for value in params_values
        }

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
        logger.debug(f"Request to pages URL={pages_endpoint}")
        response = self._request_session.get(
            pages_endpoint,
            headers=self.get_authorization_header(),
        )

        if is_http_failure(response, f"Unable to fetch pages for report {report_id}"):
            return []

        response_dict = response.json()
        return [
            Page(
                id="{}.{}".format(
                    report_id, raw_instance[Constant.NAME].replace(" ", "_")
                ),
                name=raw_instance[Constant.NAME],
                displayName=raw_instance.get(Constant.DISPLAY_NAME),
                order=raw_instance.get(Constant.ORDER),
            )
            for raw_instance in response_dict.get(Constant.VALUE, [])
        ]

    def get_users(self, workspace_id: str, entity: str, entity_id: str) -> List[User]:
        return []  # User list is not available in regular access

    def _execute_profiling_query(self, dataset: PowerBIDataset, query: str) -> dict:
        dataset_query_endpoint: str = self.API_ENDPOINTS[
            Constant.DATASET_EXECUTE_QUERIES
        ]
        # Replace place holders
        dataset_query_endpoint = dataset_query_endpoint.format(
            POWERBI_BASE_URL=self.BASE_URL,
            WORKSPACE_ID=dataset.workspace_id,
            DATASET_ID=dataset.id,
        )
        # Hit PowerBi
        logger.info(f"Request to query endpoint URL={dataset_query_endpoint}")

        # Serializer is configured to include nulls so that the queried fields
        # exist in the returned payloads. Only failed queries will result in KeyError
        payload = {
            "queries": [
                {
                    "query": query,
                }
            ],
            "serializerSettings": {
                "includeNulls": True,
            },
        }
        response = self._request_session.post(
            dataset_query_endpoint,
            json=payload,
            headers=self.get_authorization_header(),
        )
        response.raise_for_status()
        return response.json()

    def _get_row_count(self, dataset: PowerBIDataset, table: Table) -> int:
        query = DaxQuery.row_count_query(table.name)
        try:
            data = self._execute_profiling_query(dataset, query)
            rows = data["results"][0]["tables"][0]["rows"]
            count = rows[0]["[count]"]
            return count
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Profiling failed for getting row count for dataset {dataset.id}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Profiling failed for getting row count for dataset {dataset.id}, with {ex}"
            )
        return 0

    def _get_data_sample(self, dataset: PowerBIDataset, table: Table) -> dict:
        try:
            query = DaxQuery.data_sample_query(table.name)
            data = self._execute_profiling_query(dataset, query)
            return process_sample_result(data)
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Getting sample with TopN failed for dataset {dataset.id}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Getting sample with TopN failed for dataset {dataset.id}, with {ex}"
            )
        return {}

    def _get_column_data(
        self, dataset: PowerBIDataset, table: Table, column: Union[Column, Measure]
    ) -> dict:
        try:
            logger.debug(f"Column data query for {dataset.name}, {column.name}")
            query = DaxQuery.column_data_query(table.name, column.name)
            data = self._execute_profiling_query(dataset, query)
            return process_column_result(data)
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Getting column statistics failed for dataset {dataset.name}, {column.name}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Getting column statistics failed for dataset {dataset.name}, {column.name}, with {ex}"
            )
        return {}

    def profile_dataset(
        self,
        dataset: PowerBIDataset,
        table: Table,
        workspace_name: str,
        profile_pattern: Optional[AllowDenyPattern],
    ) -> None:
        if not profile_pattern:
            logger.info("Profile pattern not configured, not profiling")
            return

        if not profile_pattern.allowed(f"{workspace_name}.{dataset.name}.{table.name}"):
            logger.info(
                f"Table {table.name} in {dataset.name}, not allowed for profiling"
            )
            return

        logger.info(f"Profiling table: {table.name}")
        row_count = self._get_row_count(dataset, table)
        sample = self._get_data_sample(dataset, table)

        table.row_count = row_count
        column_count = 0

        columns: List[Union[Column, Measure]] = [
            *(table.columns or []),
            *(table.measures or []),
        ]
        for column in columns:
            if column.isHidden:
                continue

            column_sample = sample.get(column.name, None) if sample else None
            column_stats = self._get_column_data(dataset, table, column)

            column.measure_profile = MeasureProfile(
                sample_values=column_sample, **column_stats
            )
            column_count += 1

        table.column_count = column_count

    def _get_app(
        self,
        app_id: str,
    ) -> Optional[Dict]:
        # [Date: 2024/10/18] As per API doc, the service principal approach is not supported for regular API
        # https://learn.microsoft.com/en-us/rest/api/power-bi/apps/get-app

        return None


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
        Constant.WORKSPACE_MODIFIED_LIST: "{POWERBI_ADMIN_BASE_URL}/workspaces/modified",
        Constant.GET_WORKSPACE_APP: "{POWERBI_ADMIN_BASE_URL}/apps",
    }

    def create_scan_job(self, workspace_ids: List[str]) -> str:
        """
        Create scan job on PowerBI for the workspace
        """
        request_body = {"workspaces": workspace_ids}

        scan_create_endpoint = AdminAPIResolver.API_ENDPOINTS[Constant.SCAN_CREATE]
        scan_create_endpoint = scan_create_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL
        )

        logger.debug(
            f"Creating metadata scan job, request body {request_body}",
        )

        res = self._request_session.post(
            scan_create_endpoint,
            data=request_body,
            params={
                "datasetExpressions": True,
                "datasetSchema": True,
                "datasourceDetails": True,
                "getArtifactUsers": True,
                "lineage": True,
            },
            headers=self.get_authorization_header(),
        )

        res.raise_for_status()
        # Return scan_id of Scan created for the given workspace
        scan_id = res.json()["id"]

        logger.debug(f"Scan id({scan_id})")

        return scan_id

    @staticmethod
    def _calculate_max_retry(minimum_sleep: int, timeout: int) -> int:
        if timeout < minimum_sleep:
            logger.info(
                f"Setting timeout to minimum_sleep time {minimum_sleep} seconds"
            )
            timeout = minimum_sleep

        return timeout // minimum_sleep

    def _is_scan_result_ready(
        self,
        scan_get_endpoint: str,
        max_retry: int,
        minimum_sleep_seconds: int,
        scan_id: str,
    ) -> bool:
        logger.debug(f"Hitting URL={scan_get_endpoint}")
        retry = 1
        while True:
            logger.debug(f"retry = {retry}")
            res = self._request_session.get(
                scan_get_endpoint,
                headers=self.get_authorization_header(),
            )

            logger.debug(f"Request response = {res}")

            res.raise_for_status()

            if res.json()[Constant.STATUS].upper() == Constant.SUCCEEDED:
                logger.debug(f"Scan result is available for scan id({scan_id})")
                return True

            if retry == max_retry:
                logger.warning(
                    "Max retry reached when polling for scan job (lineage) result. Scan job is not "
                    "available! Try increasing your max retry using config option scan_timeout"
                )
                break

            logger.debug(
                f"Waiting to check for scan job completion for {minimum_sleep_seconds} seconds."
            )
            sleep(minimum_sleep_seconds)
            retry += 1

        return False

    def wait_for_scan_to_complete(self, scan_id: str, timeout: int) -> Any:
        """
        Poll the PowerBi service for workspace scan to complete
        """
        minimum_sleep_seconds = 3
        max_retry: int = AdminAPIResolver._calculate_max_retry(
            minimum_sleep_seconds, timeout
        )
        # logger.info(f"Max trial {max_retry}")

        scan_get_endpoint = AdminAPIResolver.API_ENDPOINTS[Constant.SCAN_GET]
        scan_get_endpoint = scan_get_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL, SCAN_ID=scan_id
        )

        return self._is_scan_result_ready(
            scan_get_endpoint=scan_get_endpoint,
            max_retry=max_retry,
            minimum_sleep_seconds=minimum_sleep_seconds,
            scan_id=scan_id,
        )

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
        logger.debug(f"Request to URL={user_list_endpoint}")
        response = self._request_session.get(
            user_list_endpoint,
            headers=self.get_authorization_header(),
        )
        logger.debug(f"Response = {response}")

        response.raise_for_status()

        users_dict: List[Any] = response.json().get(Constant.VALUE, [])

        # Iterate through response and create a list of PowerBiAPI.Dashboard
        users: List[User] = [
            User(
                id=instance.get(Constant.IDENTIFIER),
                displayName=instance.get(Constant.DISPLAY_NAME),
                emailAddress=instance.get(Constant.EMAIL_ADDRESS),
                graphId=instance.get(Constant.GRAPH_ID),
                principalType=instance.get(Constant.PRINCIPAL_TYPE),
                datasetUserAccessRight=instance.get(Constant.DATASET_USER_ACCESS_RIGHT),
                reportUserAccessRight=instance.get(Constant.REPORT_USER_ACCESS_RIGHT),
                dashboardUserAccessRight=instance.get(
                    Constant.DASHBOARD_USER_ACCESS_RIGHT
                ),
                groupUserAccessRight=instance.get(Constant.GROUP_USER_ACCESS_RIGHT),
            )
            for instance in users_dict
        ]

        return users

    def get_scan_result(self, scan_id: str) -> Optional[dict]:
        logger.debug("Fetching scan result")
        logger.debug(f"{Constant.SCAN_ID}={scan_id}")
        scan_result_get_endpoint = AdminAPIResolver.API_ENDPOINTS[
            Constant.SCAN_RESULT_GET
        ]
        scan_result_get_endpoint = scan_result_get_endpoint.format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL, SCAN_ID=scan_id
        )

        logger.debug(f"Hitting URL={scan_result_get_endpoint}")
        res = self._request_session.get(
            scan_result_get_endpoint,
            headers=self.get_authorization_header(),
        )
        if res.status_code != 200:
            message = f"API({scan_result_get_endpoint}) return error code {res.status_code} for scan id({scan_id})"
            logger.warning(message)
            raise ConnectionError(message)

        if (
            res.json().get("workspaces") is None
            or len(res.json().get("workspaces")) == 0
        ):
            logger.warning(
                f"Scan result is not available for scan identifier = {scan_id}"
            )
            return None

        return res.json()

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
        self, workspace: Workspace, dataset_id: str
    ) -> Optional[PowerBIDataset]:
        datasets_endpoint = self.API_ENDPOINTS[Constant.DATASET_LIST].format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            WORKSPACE_ID=workspace.id,
        )
        # Hit PowerBi
        logger.debug(f"Request to datasets URL={datasets_endpoint}")
        params: dict = {"$filter": f"id eq '{dataset_id}'"}
        logger.debug("params = %s", params)
        response = self._request_session.get(
            datasets_endpoint,
            headers=self.get_authorization_header(),
            params=params,
        )
        response.raise_for_status()
        response_dict = response.json()
        if len(response_dict.get(Constant.VALUE, [])) == 0:
            logger.warning(
                "Dataset not found. workspace_id = %s, dataset_id = %s",
                workspace.id,
                dataset_id,
            )
            return None

        raw_instance: dict = response_dict[Constant.VALUE][0]
        return new_powerbi_dataset(workspace, raw_instance)

    def _get_pages_by_report(self, workspace: Workspace, report_id: str) -> List[Page]:
        return []  # Report pages are not available in Admin API

    def get_modified_workspaces(self, modified_since: str) -> List[str]:
        """
        Get a list of modified workspaces
        """
        modified_workspaces_endpoint = self.API_ENDPOINTS[
            Constant.WORKSPACE_MODIFIED_LIST
        ].format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
        )
        parameters: Dict[str, Any] = {
            "excludePersonalWorkspaces": False,
            "excludeInActiveWorkspaces": True,
            "modifiedSince": modified_since,
        }

        res = self._request_session.get(
            modified_workspaces_endpoint,
            params=parameters,
            headers=self.get_authorization_header(),
        )
        if res.status_code == 400:
            error_msg_json = res.json()
            if (
                error_msg_json.get("error")
                and error_msg_json["error"]["code"] == "InvalidRequest"
            ):
                raise ConfigurationError(
                    "Please check if modified_since is within last 30 days."
                )
            else:
                raise ConfigurationError(
                    f"Please resolve the following error: {res.text}"
                )
        res.raise_for_status()

        # Return scan_id of Scan created for the given workspace
        workspace_ids = [row["id"] for row in res.json()]
        logger.debug(f"modified workspace_ids: {workspace_ids}")
        return workspace_ids

    def get_dataset_parameters(
        self, workspace_id: str, dataset_id: str
    ) -> Dict[str, str]:
        logger.debug("Get dataset parameter is unsupported in Admin API")
        return {}

    def profile_dataset(
        self,
        dataset: PowerBIDataset,
        table: Table,
        workspace_name: str,
        profile_pattern: Optional[AllowDenyPattern],
    ) -> None:
        logger.debug("Profile dataset is unsupported in Admin API")
        return None

    def _get_app(
        self,
        app_id: str,
    ) -> Optional[Dict]:
        app_endpoint = self.API_ENDPOINTS[Constant.GET_WORKSPACE_APP].format(
            POWERBI_ADMIN_BASE_URL=DataResolverBase.ADMIN_BASE_URL,
            APP_ID=app_id,
        )
        # Hit PowerBi
        logger.debug(f"Request to app URL={app_endpoint}")

        for page in self.itr_pages(endpoint=app_endpoint):
            for app in page:
                if Constant.ID in app and app_id == app[Constant.ID]:
                    return app

        return None
