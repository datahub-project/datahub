import functools
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Rate limiting constants
RATE_LIMIT_THRESHOLD = 3  # Enable rate limiting when max_workers > this value
RATE_LIMIT_REQUESTS_PER_SECOND = 5.0  # Max requests per second when rate limiting

# Retry constants for 429 errors
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 2.0  # Exponential backoff: 2s, 4s, 8s


class RateLimiter:
    """Thread-safe rate limiter using token bucket algorithm."""

    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        self.tokens = requests_per_second  # Start with full bucket
        self.last_update = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request can be made."""
        with self.lock:
            now = time.monotonic()
            # Add tokens based on time passed
            time_passed = now - self.last_update
            self.tokens = min(
                self.requests_per_second,
                self.tokens + time_passed * self.requests_per_second,
            )
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
            else:
                # Wait for enough time to get a token
                wait_time = (1 - self.tokens) / self.requests_per_second
                time.sleep(wait_time)
                self.tokens = 0
                self.last_update = time.monotonic()


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

        # Enable rate limiting when max_workers > threshold to avoid API rate limits
        if self.config.max_workers > RATE_LIMIT_THRESHOLD:
            self.rate_limiter: Optional[RateLimiter] = RateLimiter(
                RATE_LIMIT_REQUESTS_PER_SECOND
            )
            logger.info(
                f"Rate limiting enabled: {RATE_LIMIT_REQUESTS_PER_SECOND} requests/sec "
                f"(max_workers={self.config.max_workers} > threshold={RATE_LIMIT_THRESHOLD})"
            )
        else:
            self.rate_limiter = None

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
        """Make an API call with rate limiting and retry on 429 errors."""
        for attempt in range(RETRY_MAX_ATTEMPTS):
            # Apply rate limiting if enabled
            if self.rate_limiter:
                self.rate_limiter.acquire()

            get_response = self.session.get(url)

            # Handle token refresh on 401
            if get_response.status_code == 401 and self.refresh_token:
                logger.debug("Access token might expired. Refreshing access token.")
                self._refresh_access_token()
                if self.rate_limiter:
                    self.rate_limiter.acquire()
                get_response = self.session.get(url)

            # Retry on 429 with exponential backoff
            if get_response.status_code == 429:
                if attempt < RETRY_MAX_ATTEMPTS - 1:
                    delay = RETRY_BASE_DELAY_SECONDS * (2**attempt)
                    logger.debug(
                        f"Rate limited (429) on {url}, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    logger.warning(f"Rate limited (429) on {url}, max retries exceeded")

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

    def _get_connection_id_from_name(self, connection_name: str) -> Optional[str]:
        """Look up a connection ID by its name in the cached connections."""
        for conn_id, conn in self.connections.items():
            if conn.get(Constant.NAME) == connection_name:
                return conn_id
        return None

    def _get_table_info_from_inode(self, inode_id: str) -> Optional[DatasetSource]:
        """
        Fetch table details from an inode ID using the /files/{inodeId} endpoint.

        The API returns file metadata including:
        - name: table name (e.g., "salesforce_account")
        - path: folder path (e.g., "Connection Name/schema" where first part is connection)
        - connectionId: usually not present for tables (only for connection roots)

        We extract table name, schema, and resolve connectionId from the path.
        """
        try:
            response = self._get_api_call(f"{self.config.api_url}/files/{inode_id}")
            if response.status_code in (404, 403):
                logger.debug(f"File/table {inode_id} not accessible")
                return None
            response.raise_for_status()
            file_info = response.json()

            logger.debug(f"Table inode {inode_id} file info: {file_info}")

            # Extract table name from the file info
            table = file_info.get("name")
            if not table:
                logger.debug(f"Table inode {inode_id} has no name")
                return None

            # Extract connection name and schema from path
            # Path format: "Connection Name/schema" or "Connection Name/database/schema"
            path = file_info.get("path") or ""
            schema_name = None
            connection_name = None

            if path:
                path_parts = path.split("/")
                if len(path_parts) >= 1:
                    # First part of path is the connection name
                    connection_name = path_parts[0]
                if len(path_parts) >= 2:
                    # Last part of path is usually the schema
                    schema_name = path_parts[-1]

            # connectionId is usually not present for individual tables
            # Try to resolve it from the connection name
            connection_id = file_info.get("connectionId")
            if not connection_id and connection_name:
                connection_id = self._get_connection_id_from_name(connection_name)
                if connection_id:
                    logger.debug(
                        f"Resolved connectionId={connection_id} from connection name '{connection_name}'"
                    )

            logger.debug(
                f"Table inode {inode_id} extracted: table={table}, "
                f"schema={schema_name}, connectionId={connection_id}, "
                f"connectionName={connection_name}"
            )

            return DatasetSource(
                connectionId=connection_id,
                table=table,
                schema_name=schema_name,
                database=None,  # Database not available from this API
            )

        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch table info for inode {inode_id}. Exception: {e}"
            )
            return None

    def get_dataset_source(self, dataset_id: str) -> Optional[DatasetSource]:
        """
        Fetch source information for a dataset (underlying warehouse table).

        Uses the /datasets/{datasetId}/sources endpoint to get source inode IDs,
        then resolves each table inode to get actual connection/table details.
        """
        try:
            # Use the /sources endpoint to get source inode IDs
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

            # The response is a list of source entries with type and inodeId
            entries = sources_response if isinstance(sources_response, list) else []

            for source in entries:
                source_type = source.get("type")
                inode_id = source.get("inodeId")

                logger.debug(
                    f"Dataset {dataset_id} source: type={source_type}, inodeId={inode_id}"
                )

                # Only process table sources (not dataset sources)
                if source_type == "table" and inode_id:
                    table_info = self._get_table_info_from_inode(inode_id)
                    if table_info:
                        if table_info.connectionId:
                            logger.debug(
                                f"Dataset {dataset_id} resolved table source: "
                                f"connection={table_info.connectionId}, table={table_info.table}, "
                                f"schema={table_info.schema_name}, database={table_info.database}"
                            )
                            return table_info
                        else:
                            logger.debug(
                                f"Dataset {dataset_id} table source has no connectionId: "
                                f"table={table_info.table}, schema={table_info.schema_name}"
                            )

            logger.debug(f"Dataset {dataset_id} has no resolvable table sources")
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
                element_map = {e.elementId: e for e in elements}
                num_threads = min(self.config.max_workers, len(elements))

                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    futures = {
                        executor.submit(
                            self._fetch_element_lineage_data, element, workbook
                        ): element
                        for element in elements
                    }
                    for future in as_completed(futures):
                        element_id, upstream_sources, query = future.result()
                        element_map[element_id].upstream_sources = upstream_sources
                        element_map[element_id].query = query

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
