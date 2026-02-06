import functools
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.data_classes import (
    DatasetSource,
    Element,
    File,
    Page,
    SigmaDataset,
    Workbook,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)

# Retry constants for 429/503 errors
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 2.0  # Exponential backoff: 2s, 4s, 8s


class SigmaAPI:
    def __init__(self, config: SigmaSourceConfig, report: SigmaSourceReport) -> None:
        self.config = config
        self.report = report
        self.workspaces: Dict[str, Workspace] = {}
        self.users: Dict[str, str] = {}
        self.connections: Dict[
            str, Dict[str, Any]
        ] = {}  # connectionId -> connection info
        self.session = requests.Session()
        self.refresh_token: Optional[str] = None

        # Test connection by generating access token
        logger.info(f"Trying to connect to {self.config.api_url}")
        self._generate_token()

    def _generate_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        response = self.session.post(f"{self.config.api_url}/auth/token", data=data)
        response.raise_for_status()
        response_dict = response.json()
        self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
        self.session.headers.update(
            {
                "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                "Content-Type": "application/json",
            }
        )

    def _log_http_error(self, message: str) -> Any:
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def _refresh_access_token(self):
        try:
            data = {
                "grant_type": Constant.REFRESH_TOKEN,
                "refresh_token": self.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            }
            post_response = self.session.post(
                f"{self.config.api_url}/auth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
            post_response.raise_for_status()
            response_dict = post_response.json()
            self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                    "Content-Type": "application/json",
                }
            )
        except Exception as e:
            self._log_http_error(
                message=f"Unable to refresh access token. Exception: {e}"
            )

    def _get_api_call(self, url: str) -> requests.Response:
        """Make an API call with retry on 429/503 errors."""
        for attempt in range(RETRY_MAX_ATTEMPTS):
            get_response = self.session.get(url)

            # Handle token refresh on 401
            if get_response.status_code == 401 and self.refresh_token:
                logger.debug("Access token might expired. Refreshing access token.")
                self._refresh_access_token()
                get_response = self.session.get(url)

            # Retry on 429 (rate limit) or 503 (service unavailable) with exponential backoff
            if get_response.status_code in (429, 503):
                if attempt < RETRY_MAX_ATTEMPTS - 1:
                    delay = RETRY_BASE_DELAY_SECONDS * (2**attempt)
                    logger.debug(
                        f"Retryable error ({get_response.status_code}) on {url}, "
                        f"retrying in {delay}s (attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    logger.warning(
                        f"Retryable error ({get_response.status_code}) on {url}, "
                        f"max retries exceeded"
                    )

            return get_response

        return get_response

    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        if workspace_id in self.workspaces:
            return self.workspaces[workspace_id]

        logger.debug(f"Fetching workspace metadata with id '{workspace_id}'")
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workspaces/{workspace_id}"
            )
            if response.status_code == 403:
                logger.debug(f"Workspace {workspace_id} not accessible.")
                self.report.non_accessible_workspaces_count += 1
                return None
            response.raise_for_status()
            workspace = Workspace.model_validate(response.json())
            self.workspaces[workspace.workspaceId] = workspace
            return workspace
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workspace '{workspace_id}'. Exception: {e}"
            )
        return None

    def fill_workspaces(self) -> None:
        logger.debug("Fetching all accessible workspaces metadata.")
        workspace_url = url = f"{self.config.api_url}/workspaces?limit=50"
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workspace_dict in response_dict[Constant.ENTRIES]:
                    self.workspaces[workspace_dict[Constant.WORKSPACEID]] = (
                        Workspace.model_validate(workspace_dict)
                    )
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workspace_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch workspaces. Exception: {e}")

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        logger.debug("Fetching all accessible users metadata.")
        try:
            users: Dict[str, str] = {}
            members_url = url = f"{self.config.api_url}/members?limit=50"
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for user_dict in response_dict[Constant.ENTRIES]:
                    users[user_dict[Constant.MEMBERID]] = (
                        f"{user_dict[Constant.FIRSTNAME]}_{user_dict[Constant.LASTNAME]}"
                    )
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{members_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return users
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch users details. Exception: {e}"
            )
            return {}

    def get_user_name(self, user_id: str) -> Optional[str]:
        return self._get_users().get(user_id)

    def fill_connections(self) -> None:
        """Fetch all connections and cache them."""
        logger.debug("Fetching all accessible connections metadata.")
        connections_url = url = f"{self.config.api_url}/connections?limit=50"
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for connection in response_dict.get(Constant.ENTRIES, []):
                    conn_id = connection.get(Constant.CONNECTION_ID)
                    if conn_id:
                        self.connections[conn_id] = connection
                        logger.debug(
                            f"Cached connection: {connection.get(Constant.NAME)} "
                            f"(id={conn_id}, type={connection.get(Constant.TYPE)})"
                        )
                if response_dict.get(Constant.NEXTPAGE):
                    url = f"{connections_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            logger.info(f"Fetched {len(self.connections)} connections")
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch connections. Exception: {e}")

    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get connection info by ID."""
        return self.connections.get(connection_id)

    def get_connection_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get connection info by name."""
        for conn in self.connections.values():
            if conn.get(Constant.NAME) == name:
                return conn
        return None

    @functools.lru_cache()
    def get_workspace_id_from_file_path(
        self, parent_id: str, path: str
    ) -> Optional[str]:
        try:
            path_list = path.split("/")
            while len(path_list) != 1:  # means current parent id is folder's id
                response = self._get_api_call(
                    f"{self.config.api_url}/files/{parent_id}"
                )
                response.raise_for_status()
                parent_id = response.json()[Constant.PARENTID]
                path_list.pop()
            return parent_id
        except Exception as e:
            logger.error(
                f"Unable to find workspace id using file path '{path}'. Exception: {e}"
            )
            return None

    @functools.lru_cache
    def _get_files_metadata(self, file_type: str) -> Dict[str, File]:
        logger.debug(f"Fetching file metadata with type {file_type}.")
        file_url = url = (
            f"{self.config.api_url}/files?permissionFilter=view&typeFilters={file_type}"
        )
        try:
            files_metadata: Dict[str, File] = {}
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for file_dict in response_dict[Constant.ENTRIES]:
                    file = File.model_validate(file_dict)
                    file.workspaceId = self.get_workspace_id_from_file_path(
                        file.parentId, file.path
                    )
                    files_metadata[file_dict[Constant.ID]] = file
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{file_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            self.report.number_of_files_metadata[file_type] = len(files_metadata)
            return files_metadata
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch files metadata. Exception: {e}"
            )
            return {}

    def get_sigma_datasets(self) -> List[SigmaDataset]:
        logger.debug("Fetching all accessible datasets metadata.")
        dataset_url = url = f"{self.config.api_url}/datasets"
        dataset_files_metadata = self._get_files_metadata(file_type=Constant.DATASET)
        try:
            datasets: List[SigmaDataset] = []
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for dataset_dict in response_dict[Constant.ENTRIES]:
                    dataset = SigmaDataset.model_validate(dataset_dict)

                    if dataset.datasetId not in dataset_files_metadata:
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) (missing file metadata)"
                        )
                        continue

                    dataset.workspaceId = dataset_files_metadata[
                        dataset.datasetId
                    ].workspaceId

                    dataset.path = dataset_files_metadata[dataset.datasetId].path
                    dataset.badge = dataset_files_metadata[dataset.datasetId].badge

                    workspace = None
                    if dataset.workspaceId:
                        workspace = self.get_workspace(dataset.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.datasets.processed(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                            datasets.append(dataset)
                        else:
                            self.report.datasets.dropped(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for dataset we can consider it as shared entity
                        self.report.datasets_without_workspace += 1
                        self.report.datasets.processed(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )
                        datasets.append(dataset)
                    else:
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{dataset_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break

            return datasets
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma datasets. Exception: {e}"
            )
            return []

    def get_dataset_source(self, dataset_id: str) -> Optional[DatasetSource]:
        """
        Fetch source information for a dataset (underlying warehouse table).

        Uses the /datasets/{datasetId}/sources endpoint to get source table info.
        """
        try:
            # Use the /sources endpoint to get source information
            response = self._get_api_call(
                f"{self.config.api_url}/datasets/{dataset_id}/sources"
            )
            if response.status_code == 404:
                logger.debug(f"Dataset sources not found for {dataset_id}")
                return None
            if response.status_code == 403:
                logger.debug(f"Dataset sources not accessible for {dataset_id}")
                return None
            response.raise_for_status()
            sources_response = response.json()

            # Log the full response for debugging
            logger.debug(f"Dataset {dataset_id} sources response: {sources_response}")

            # The response contains source entries with inode IDs and type info
            # Look for sources that are tables (not other datasets)
            # API may return either {"entries": [...]} or just [...]
            if isinstance(sources_response, list):
                entries = sources_response
            else:
                entries = sources_response.get("entries", [])

            for source in entries:
                # Log each source entry
                logger.debug(f"Dataset {dataset_id} source entry: {source}")

                # Extract source info - the structure may vary
                connection_id = source.get("connectionId")

                # If this is a table source with connection info
                if connection_id:
                    table = (
                        source.get("table")
                        or source.get("tableName")
                        or source.get("name")
                        or source.get("path")
                    )
                    schema_name = source.get("schema") or source.get("schemaName")
                    database = source.get("database") or source.get("databaseName")

                    logger.debug(
                        f"Dataset {dataset_id} found source: connection={connection_id}, "
                        f"table={table}, schema={schema_name}, database={database}"
                    )

                    return DatasetSource(
                        connectionId=connection_id,
                        table=table,
                        schema_name=schema_name,
                        database=database,
                    )

            logger.debug(f"Dataset {dataset_id} has no table sources in response")
            return None

        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch source for dataset {dataset_id}. Exception: {e}"
            )
            return None

    def _get_element_upstream_sources(
        self, element: Element, workbook: Workbook
    ) -> Dict[str, str]:
        """
        Returns upstream dataset sources with keys as id and values as name of that dataset
        """
        try:
            upstream_sources: Dict[str, str] = {}
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/lineage/elements/{element.elementId}"
            )
            if response.status_code == 500:
                logger.debug(
                    f"Lineage metadata not present for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 403:
                logger.debug(
                    f"Lineage metadata not accessible for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 400:
                logger.debug(
                    f"Lineage not supported for element {element.name} of workbook '{workbook.name}' (400 Bad Request)"
                )
                return upstream_sources

            response.raise_for_status()
            response_dict = response.json()
            for edge in response_dict[Constant.EDGES]:
                source_type = response_dict[Constant.DEPENDENCIES][
                    edge[Constant.SOURCE]
                ][Constant.TYPE]
                if source_type == "dataset":
                    upstream_sources[edge[Constant.SOURCE]] = response_dict[
                        Constant.DEPENDENCIES
                    ][edge[Constant.SOURCE]][Constant.NAME]
            return upstream_sources
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch lineage for element {element.name} of workbook '{workbook.name}'. Exception: {e}"
            )
            return {}

    def _get_element_sql_query(
        self, element: Element, workbook: Workbook
    ) -> Optional[str]:
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/elements/{element.elementId}/query"
            )
            if response.status_code == 404:
                logger.debug(
                    f"Query not present for element {element.name} of workbook '{workbook.name}'"
                )
                return None
            response.raise_for_status()
            response_dict = response.json()
            if "sql" in response_dict:
                return response_dict["sql"]
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sql query for element {element.name} of workbook '{workbook.name}'. Exception: {e}"
            )
        return None

    def _fetch_element_lineage_data(
        self, element: Element, workbook: Workbook
    ) -> Tuple[str, Dict[str, str], Optional[str]]:
        """Fetch upstream sources and SQL query for an element. Used for parallel fetching."""
        upstream_sources = self._get_element_upstream_sources(element, workbook)
        query = self._get_element_sql_query(element, workbook)
        return (element.elementId, upstream_sources, query)

    def get_page_elements(self, workbook: Workbook, page: Page) -> List[Element]:
        try:
            elements: List[Element] = []
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()

            # First pass: create all elements without lineage data
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                if not element_dict.get(Constant.NAME):
                    element_dict[Constant.NAME] = (
                        f"Element {i + 1} of Page '{page.name}'"
                    )
                element_dict[Constant.URL] = (
                    f"{workbook.url}?:nodeId={element_dict[Constant.ELEMENTID]}&:fullScreen=true"
                )
                element = Element.model_validate(element_dict)
                elements.append(element)

            # Second pass: fetch lineage data in parallel if needed
            if (
                self.config.extract_lineage
                and self.config.workbook_lineage_pattern.allowed(workbook.name)
                and elements
            ):
                # Fetch lineage data sequentially to avoid rate limiting
                for element in elements:
                    element_id, upstream_sources, query = (
                        self._fetch_element_lineage_data(element, workbook)
                    )
                    element.upstream_sources = upstream_sources
                    element.query = query

            return elements
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page '{page.name}', workbook '{workbook.name}'. Exception: {e}"
            )
            return []

    def get_workbook_pages(self, workbook: Workbook) -> List[Page]:
        try:
            pages: List[Page] = []
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.model_validate(page_dict)
                page.elements = self.get_page_elements(workbook, page)
                pages.append(page)
            return pages
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch pages of workbook '{workbook.name}'. Exception: {e}"
            )
            return []

    def get_sigma_workbooks(self) -> List[Workbook]:
        logger.debug("Fetching all accessible workbooks metadata.")
        workbook_url = url = f"{self.config.api_url}/workbooks"
        workbook_files_metadata = self._get_files_metadata(file_type=Constant.WORKBOOK)
        try:
            workbooks: List[Workbook] = []
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workbook_dict in response_dict[Constant.ENTRIES]:
                    workbook = Workbook.model_validate(workbook_dict)

                    if workbook.workbookId not in workbook_files_metadata:
                        # Due to a bug in the Sigma API, it seems like the /files endpoint does not
                        # return file metadata when the user has access via admin permissions. In
                        # those cases, the user associated with the token needs to be manually added
                        # to the workspace.
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) (missing file metadata; path: {workbook.path}; likely need to manually add user to workspace)"
                        )
                        continue

                    workbook.workspaceId = workbook_files_metadata[
                        workbook.workbookId
                    ].workspaceId

                    workbook.badge = workbook_files_metadata[workbook.workbookId].badge

                    workspace = None
                    if workbook.workspaceId:
                        workspace = self.get_workspace(workbook.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.workbooks.processed(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                            workbook.pages = self.get_workbook_pages(workbook)
                            workbooks.append(workbook)
                        else:
                            self.report.workbooks.dropped(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for workbook we can consider it as shared entity
                        self.report.workbooks_without_workspace += 1
                        self.report.workbooks.processed(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )
                        workbook.pages = self.get_workbook_pages(workbook)
                        workbooks.append(workbook)
                    else:
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workbook_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return workbooks
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma workbooks. Exception: {e}"
            )
            return []
