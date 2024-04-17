import logging
import sys
from typing import Any, Dict, List, Optional, Union

import requests

from datahub.ingestion.source.sigma.config import Constant, SigmaSourceConfig
from datahub.ingestion.source.sigma.data_classes import (
    Element,
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
        # Test connection by generating access token
        logger.info("Trying to connect to {}".format(self.config.api_url))
        self._generate_token()

    def _generate_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        response = self.session.post(f"{self.config.api_url}/auth/token", data=data)
        response.raise_for_status()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {response.json()['access_token']}",
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

    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        workspace: Optional[Workspace] = None
        try:
            response = self.session.get(
                f"{self.config.api_url}/workspaces/{workspace_id}"
            )
            response.raise_for_status()
            workspace_dict = response.json()
            workspace = Workspace.parse_obj(workspace_dict)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workspace {workspace_id}. Exception: {e}"
            )
        return workspace

    def get_user_name(self, user_id: str) -> Optional[str]:
        try:
            if user_id in self.users:
                # To avoid fetching same user details again
                return self.users[user_id]
            else:
                response = self.session.get(f"{self.config.api_url}/members/{user_id}")
                response.raise_for_status()
                user_dict = response.json()
                user_name = (
                    f"{user_dict[Constant.FIRSTNAME]}_{user_dict[Constant.LASTNAME]}"
                )
                self.users[user_id] = user_name
                return user_name
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch user with id {user_id}. Exception: {e}"
            )
        return None

    def get_sigma_dataset(
        self, dataset_id: str, workspace_id: str, path: str
    ) -> Optional[SigmaDataset]:
        dataset: Optional[SigmaDataset] = None
        try:
            response = self.session.get(f"{self.config.api_url}/datasets/{dataset_id}")
            response.raise_for_status()
            dataset_dict = response.json()
            dataset_dict[Constant.WORKSPACEID] = workspace_id
            dataset_dict[Constant.PATH] = path
            dataset = SigmaDataset.parse_obj(dataset_dict)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma dataset {dataset_id}. Exception: {e}"
            )
        return dataset

    def _get_element_upstream_sources(
        self, element_id: str, workbook_id: str
    ) -> Dict[str, str]:
        """
        Returns upstream dataset sources with keys as id and values as name of that dataset
        """
        upstream_sources: Dict[str, str] = {}
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}/lineage/elements/{element_id}"
            )
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
                message=f"Unable to fetch lineage of element {element_id}. Exception: {e}"
            )
        return upstream_sources

    def _get_element_sql_query(
        self, element_id: str, workbook_id: str
    ) -> Optional[str]:
        query: Optional[str] = None
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}/elements/{element_id}/query"
            )
            response.raise_for_status()
            response_dict = response.json()
            if "sql" in response_dict:
                query = response_dict["sql"]
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sql query for a element {element_id}. Exception: {e}"
            )
        return query

    def get_page_elements(self, workbook: Workbook, page: Page) -> List[Element]:
        elements: List[Element] = []
        try:
            response = self.session.get(
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
                element.upstream_sources = self._get_element_upstream_sources(
                    element.elementId, workbook.workbookId
                )
                element.query = self._get_element_sql_query(
                    element.elementId, workbook.workbookId
                )
                elements.append(element)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page {page.pageId}, workbook {workbook.workbookId}. Exception: {e}"
            )
        return elements

    def get_workbook_pages(self, workbook: Workbook) -> List[Page]:
        pages: List[Page] = []
        try:
            response = self.session.get(
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

    def get_workbook(self, workbook_id: str, workspace_id: str) -> Optional[Workbook]:
        workbook: Optional[Workbook] = None
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}"
            )
            response.raise_for_status()
            workbook_dict = response.json()
            workbook_dict[Constant.WORKSPACEID] = workspace_id
            workbook = Workbook.parse_obj(workbook_dict)
            workbook.pages = self.get_workbook_pages(workbook)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workbook {workbook_id}. Exception: {e}"
            )
        return workbook

    def get_workspace_id(self, parent_id: str, path: str) -> str:
        path_list = path.split("/")
        while len(path_list) != 1:  # means current parent id is folder's id
            response = self.session.get(f"{self.config.api_url}/files/{parent_id}")
            parent_id = response.json()[Constant.PARENTID]
            path_list.pop()
        return parent_id

    def get_sigma_entities(self) -> List[Union[Workbook, SigmaDataset]]:
        entities: List[Union[Workbook, SigmaDataset]] = []
        url = f"{self.config.api_url}/files"
        while True:
            response = self.session.get(url)
            response.raise_for_status()
            response_dict = response.json()
            for entity in response_dict[Constant.ENTRIES]:
                workspace_id = self.get_workspace_id(
                    entity[Constant.PARENTID], entity[Constant.PATH]
                )
                if workspace_id not in self.workspaces:
                    workspace = self.get_workspace(workspace_id)
                    if workspace:
                        self.workspaces[workspace.workspaceId] = workspace

                if self.workspaces.get(
                    workspace_id
                ) and self.config.workspace_pattern.allowed(
                    self.workspaces[workspace_id].name
                ):
                    type = entity[Constant.TYPE]
                    if type == Constant.DATASET:
                        dataset = self.get_sigma_dataset(
                            entity[Constant.ID],
                            workspace_id,
                            entity[Constant.PATH],
                        )
                        if dataset:
                            dataset.badge = entity[Constant.BADGE]
                            entities.append(dataset)
                    elif type == Constant.WORKBOOK:
                        workbook = self.get_workbook(entity[Constant.ID], workspace_id)
                        if workbook:
                            workbook.badge = entity[Constant.BADGE]
                            entities.append(workbook)
            if response_dict[Constant.NEXTPAGE]:
                url = f"{url}?page={response_dict[Constant.NEXTPAGE]}"
            else:
                break
        return entities
