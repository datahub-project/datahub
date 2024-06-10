import functools
import logging
import sys
from typing import Any, Dict, List, Optional

import requests

from datahub.ingestion.source.sigma.config import Constant, SigmaSourceConfig
from datahub.ingestion.source.sigma.data_classes import (
    Element,
    File,
    Page,
    SigmaDataset,
    Workbook,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)


class SigmaAPI:
    def __init__(self, config: SigmaSourceConfig) -> None:
        self.config = config
        self.workspaces: Dict[str, Workspace] = {}
        self.users: Dict[str, str] = {}
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
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def _get_api_call(self, url: str) -> requests.Response:
        get_response = self.session.get(url)

        if get_response.status_code == 401 and self.refresh_token:
            logger.debug("Access token might expired. Refreshing access token.")
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
                get_response = self.session.get(url)
            except Exception as e:
                self._log_http_error(
                    message=f"Unable to refresh access token. Exception: {e}"
                )
        return get_response

    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        try:
            if workspace_id in self.workspaces:
                return self.workspaces[workspace_id]
            else:
                response = self._get_api_call(
                    f"{self.config.api_url}/workspaces/{workspace_id}"
                )
                if response.status_code == 403:
                    logger.debug(f"Workspace {workspace_id} not accessible.")
                    return None
                response.raise_for_status()
                workspace = Workspace.parse_obj(response.json())
                self.workspaces[workspace.workspaceId] = workspace
                return workspace
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workspace {workspace_id}. Exception: {e}"
            )
        return None

    def fill_workspaces(self) -> None:
        url = f"{self.config.api_url}/workspaces?limit=50"
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workspace_dict in response_dict[Constant.ENTRIES]:
                    self.workspaces[
                        workspace_dict[Constant.WORKSPACEID]
                    ] = Workspace.parse_obj(workspace_dict)
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch workspaces. Exception: {e}")

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        users: Dict[str, str] = {}
        try:
            response = self._get_api_call(f"{self.config.api_url}/members")
            response.raise_for_status()
            for user_dict in response.json():
                users[
                    user_dict[Constant.MEMBERID]
                ] = f"{user_dict[Constant.FIRSTNAME]}_{user_dict[Constant.LASTNAME]}"
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch users details. Exception: {e}"
            )
        return users

    def get_user_name(self, user_id: str) -> Optional[str]:
        return self._get_users().get(user_id)

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
        files_metadata: Dict[str, File] = {}
        url = f"{self.config.api_url}/files?typeFilters={file_type}"
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for file_dict in response_dict[Constant.ENTRIES]:
                    file = File.parse_obj(file_dict)
                    file.workspaceId = self.get_workspace_id_from_file_path(
                        file.parentId, file.path
                    )
                    files_metadata[file_dict[Constant.ID]] = file
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch files metadata. Exception: {e}"
            )
        return files_metadata

    def get_sigma_datasets(self) -> List[SigmaDataset]:
        datasets: List[SigmaDataset] = []
        url = f"{self.config.api_url}/datasets"
        dataset_files_metadata = self._get_files_metadata(file_type=Constant.DATASET)
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for dataset_dict in response_dict[Constant.ENTRIES]:
                    dataset = SigmaDataset.parse_obj(dataset_dict)

                    if dataset.datasetId in dataset_files_metadata:
                        dataset.path = dataset_files_metadata[dataset.datasetId].path
                        dataset.badge = dataset_files_metadata[dataset.datasetId].badge

                        # Get workspace for dataset
                        workspace_id = dataset_files_metadata[
                            dataset.datasetId
                        ].workspaceId
                        if workspace_id:
                            dataset.workspaceId = workspace_id
                            workspace = self.get_workspace(dataset.workspaceId)
                            if workspace:
                                if self.config.workspace_pattern.allowed(
                                    workspace.name
                                ):
                                    datasets.append(dataset)
                            elif self.config.ingest_shared_entities:
                                # If no workspace for dataset we can consider it as shared entity
                                datasets.append(dataset)

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma datasets. Exception: {e}"
            )
        return datasets

    def _get_element_upstream_sources(
        self, element: Element, page: Page, workbook: Workbook
    ) -> Dict[str, str]:
        """
        Returns upstream dataset sources with keys as id and values as name of that dataset
        """
        upstream_sources: Dict[str, str] = {}
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/lineage/elements/{element.elementId}"
            )
            if response.status_code == 500:
                logger.debug(
                    f"Lineage metadata not present for element {element.name} of page {page.name} in workbook {workbook.name}"
                )
                return upstream_sources
            if response.status_code == 403:
                logger.debug(
                    f"Lineage metadata not accessible for element {element.name} of page {page.name} in workbook {workbook.name}"
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
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch lineage for element {element.name} of page {page.name} from workbook {workbook.name}. Exception: {e}"
            )
        return upstream_sources

    def _get_element_sql_query(
        self, element: Element, page: Page, workbook: Workbook
    ) -> Optional[str]:
        query: Optional[str] = None
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/elements/{element.elementId}/query"
            )
            if response.status_code == 404:
                logger.debug(
                    f"Query not present for element {element.name} of page {page.name} in workbook {workbook.name}"
                )
                return query
            response.raise_for_status()
            response_dict = response.json()
            if "sql" in response_dict:
                query = response_dict["sql"]
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sql query for element {Element.elementId} of page {page.name} in workbook {workbook.name}. Exception: {e}"
            )
        return query

    def get_page_elements(self, workbook: Workbook, page: Page) -> List[Element]:
        elements: List[Element] = []
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                if not element_dict.get(Constant.NAME):
                    element_dict[Constant.NAME] = f"Element {i+1} of Page '{page.name}'"
                element_dict[
                    Constant.URL
                ] = f"{workbook.url}?:nodeId={element_dict[Constant.ELEMENTID]}&:fullScreen=true"
                element = Element.parse_obj(element_dict)
                if (
                    self.config.extract_lineage
                    and self.config.workbook_lineage_pattern.allowed(workbook.name)
                ):
                    element.upstream_sources = self._get_element_upstream_sources(
                        element, page, workbook
                    )
                    element.query = self._get_element_sql_query(element, page, workbook)
                elements.append(element)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page {page.pageId}, workbook {workbook.workbookId}. Exception: {e}"
            )
        return elements

    def get_workbook_pages(self, workbook: Workbook) -> List[Page]:
        pages: List[Page] = []
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.parse_obj(page_dict)
                page.elements = self.get_page_elements(workbook, page)
                pages.append(page)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch pages of workbook {workbook.workbookId}. Exception: {e}"
            )
        return pages

    def get_sigma_workbooks(self) -> List[Workbook]:
        workbooks: List[Workbook] = []
        url = f"{self.config.api_url}/workbooks"
        workbook_files_metadata = self._get_files_metadata(file_type=Constant.WORKBOOK)
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workbook_dict in response_dict[Constant.ENTRIES]:
                    workbook = Workbook.parse_obj(workbook_dict)

                    if workbook.workbookId in workbook_files_metadata:
                        workbook.badge = workbook_files_metadata[
                            workbook.workbookId
                        ].badge

                        # Get workspace for workbook
                        workspace_id = workbook_files_metadata[
                            workbook.workbookId
                        ].workspaceId
                        if workspace_id:
                            workbook.workspaceId = workspace_id
                            workspace = self.get_workspace(workbook.workspaceId)
                            if workspace:
                                if self.config.workspace_pattern.allowed(
                                    workspace.name
                                ):
                                    workbook.pages = self.get_workbook_pages(workbook)
                                    workbooks.append(workbook)
                            elif self.config.ingest_shared_entities:
                                # If no workspace for workbook we can consider it as shared entity
                                workbook.pages = self.get_workbook_pages(workbook)
                                workbooks.append(workbook)

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma workbooks. Exception: {e}"
            )
        return workbooks
