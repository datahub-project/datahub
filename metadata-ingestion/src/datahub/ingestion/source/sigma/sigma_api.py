import logging
import sys
from typing import Any, Dict, List, Optional

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
        self.users: Dict = {}
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

    def get_workspaces(self) -> List[Workspace]:
        workspaces: List[Workspace] = []
        try:
            response = self.session.get(f"{self.config.api_url}/workspaces")
            response.raise_for_status()
            for workspace_dict in response.json():
                workspaces.append(Workspace.parse_obj(workspace_dict))
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch workspaces. Exception: {e}")
        return workspaces

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

    def get_sigma_datasets(self) -> List[SigmaDataset]:
        datasets: List[SigmaDataset] = []
        try:
            response = self.session.get(f"{self.config.api_url}/datasets")
            response.raise_for_status()
            for dataset_dict in response.json()[Constant.ENTRIES]:
                datasets.append(SigmaDataset.parse_obj(dataset_dict))
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma datasets. Exception: {e}"
            )
        return datasets

    def _get_element_lineage(self, element_id: str, workbook_id: str) -> List[str]:
        upstream_datasets: List[str] = []
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}/lineage/elements/{element_id}"
            )
            response.raise_for_status()
            for edge in response.json()[Constant.EDGES]:
                upstream_datasets.append(edge[Constant.SOURCE].split("-")[-1])
        except Exception:
            self._log_http_error(
                message=f"Unable to fetch lineage of element {element_id}."
            )
        return upstream_datasets

    def get_page_elements(self, workbook_id: str, page: Page) -> List[Element]:
        elements: List[Element] = []
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                if not element_dict.get("name"):
                    element_dict["name"] = f"Element {i+1} of Page '{page.name}'"
                element = Element.parse_obj(element_dict)
                element.upstream_datasets = self._get_element_lineage(
                    element.elementId, workbook_id
                )
                elements.append(element)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page {page.pageId}, workbook {workbook_id}. Exception: {e}"
            )
        return elements

    def get_workbook_pages(self, workbook_id: str) -> List[Page]:
        pages: List[Page] = []
        try:
            response = self.session.get(
                f"{self.config.api_url}/workbooks/{workbook_id}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.parse_obj(page_dict)
                page.elements = self.get_page_elements(workbook_id, page)
                pages.append(page)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch pages of workbook {workbook_id}. Exception: {e}"
            )
        return pages

    def get_workbooks(self) -> List[Workbook]:
        workbooks: List[Workbook] = []
        try:
            response = self.session.get(f"{self.config.api_url}/workbooks")
            response.raise_for_status()
            for workbook_dict in response.json()[Constant.ENTRIES]:
                workbook = Workbook.parse_obj(workbook_dict)
                workbook.pages = self.get_workbook_pages(workbook.workbookId)
                workbooks.append(workbook)
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch workbooks. Exception: {e}")
        return workbooks
