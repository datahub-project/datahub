"""API client for Grafana metadata extraction"""

import logging
from typing import Dict, List, Optional, Sequence, Union

import requests
import urllib3.exceptions
from pydantic import SecretStr, ValidationError

from datahub.ingestion.source.grafana.constants import (
    FOLDER_FETCH_ERROR_MESSAGE,
    FOLDER_FETCH_ERROR_TITLE,
    FOLDER_PARSE_ERROR_MESSAGE,
    FOLDER_PARSE_ERROR_TITLE,
    FOLDER_UNEXPECTED_RESPONSE_MESSAGE,
    HTTP_PERMISSION_STATUS_CODES,
)
from datahub.ingestion.source.grafana.models import Dashboard, Folder, FolderListing
from datahub.ingestion.source.grafana.report import GrafanaSourceReport

logger = logging.getLogger(__name__)


def _is_permission_error(e: requests.exceptions.RequestException) -> bool:
    # A requests.Response is falsy for 4xx/5xx, so compare against None.
    return (
        e.response is not None
        and e.response.status_code in HTTP_PERMISSION_STATUS_CODES
    )


class GrafanaAPIClient:
    """Client for making requests to Grafana API"""

    def __init__(
        self,
        base_url: str,
        token: SecretStr,
        verify_ssl: bool,
        page_size: int,
        report: GrafanaSourceReport,
        skip_text_panels: bool = False,
    ) -> None:
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.page_size = page_size
        self.report = report
        self.skip_text_panels = skip_text_panels
        self.session = self._create_session(token)

    def _create_session(self, token: SecretStr) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {token.get_secret_value()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        session.verify = self.verify_ssl

        # If SSL verification is disabled, suppress the warnings
        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.report.warning(
                title="SSL Configuration Warning",
                message="SSL Verification is recommended.",
            )

        return session

    def get_folders(self) -> FolderListing:
        """Fetch all folders from Grafana with pagination."""
        listing = FolderListing()
        page = 1
        per_page = self.page_size

        while True:
            try:
                response = self.session.get(
                    f"{self.base_url}/api/folders",
                    params={"page": page, "limit": per_page},
                )
                response.raise_for_status()

                batch = response.json()
                # Check the shape before emptiness: a non-list page (e.g. a
                # proxy error body) is a failure even when it is {} or null,
                # and a non-empty one would otherwise never end paging.
                if not isinstance(batch, list):
                    self.report.failure(
                        title=FOLDER_FETCH_ERROR_TITLE,
                        message=FOLDER_UNEXPECTED_RESPONSE_MESSAGE,
                        context=f"page {page}: {batch}",
                    )
                    break
                if not batch:
                    break

                self._parse_folders(batch, listing)
                page += 1
            except requests.exceptions.RequestException as e:
                self.report.failure(
                    title=FOLDER_FETCH_ERROR_TITLE,
                    message=FOLDER_FETCH_ERROR_MESSAGE,
                    context=f"page {page}",
                    exc=e,
                )
                if _is_permission_error(e):
                    self.report.report_permission_warning()
                break

        return listing

    def _parse_folders(self, batch: Sequence[object], listing: FolderListing) -> None:
        """Validate each folder separately so one bad entry costs only itself."""
        for folder in batch:
            try:
                listing.folders.append(Folder.model_validate(folder))
            except ValidationError as e:
                listing.unparseable += 1
                self.report.report_folder_unparseable()
                self.report.warning(
                    title=FOLDER_PARSE_ERROR_TITLE,
                    message=FOLDER_PARSE_ERROR_MESSAGE,
                    context=str(folder),
                    exc=e,
                )

    def get_dashboard(self, uid: str) -> Optional[Dashboard]:
        """Fetch a specific dashboard by UID"""
        try:
            response = self.session.get(f"{self.base_url}/api/dashboards/uid/{uid}")
            response.raise_for_status()
            dashboard_data = response.json()
            if self.skip_text_panels:
                dashboard_data["_skip_text_panels"] = True
            return Dashboard.model_validate(dashboard_data)
        except requests.exceptions.RequestException as e:
            self.report.warning(
                title="Dashboard Fetch Error",
                message="Failed to fetch dashboard",
                context=uid,
                exc=e,
            )
            if _is_permission_error(e):
                self.report.report_permission_warning()
            return None

    def get_dashboards(self) -> List[Dashboard]:
        """Fetch all dashboards from search endpoint with pagination."""
        dashboards: List[Dashboard] = []
        page = 1
        per_page = self.page_size

        while True:
            try:
                params: Dict[str, Union[str, int]] = {
                    "type": "dash-db",
                    "page": page,
                    "limit": per_page,
                }
                response = self.session.get(
                    f"{self.base_url}/api/search",
                    params=params,
                )
                response.raise_for_status()

                batch = response.json()
                if not batch:
                    break

                for result in batch:
                    dashboard = self.get_dashboard(result["uid"])
                    if dashboard:
                        dashboards.append(dashboard)
                page += 1
            except requests.exceptions.RequestException as e:
                self.report.failure(
                    title="Dashboard Search Error",
                    message="Failed to fetch dashboards on page",
                    context=str(page),
                    exc=e,
                )
                if _is_permission_error(e):
                    self.report.report_permission_warning()
                break

        return dashboards
