"""API client for Grafana metadata extraction"""

import logging
from typing import Dict, List, Optional, Union

import requests
import urllib3.exceptions
from pydantic import SecretStr

from datahub.ingestion.source.grafana.models import Dashboard, Folder
from datahub.ingestion.source.grafana.report import GrafanaSourceReport

logger = logging.getLogger(__name__)


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

    def get_folders(self) -> List[Folder]:
        """Fetch all folders from Grafana with pagination."""
        folders: List[Folder] = []
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
                if not batch:
                    break

                folders.extend(Folder.model_validate(folder) for folder in batch)
                page += 1
            except requests.exceptions.RequestException as e:
                self.report.report_failure(
                    title="Folder Fetch Error",
                    message="Failed to fetch folders on page",
                    context=str(page),
                    exc=e,
                )
                self.report.report_permission_warning()  # Likely a permission issue
                break

        return folders

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
            if e.response and e.response.status_code in (401, 403):
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
                self.report.report_failure(
                    title="Dashboard Search Error",
                    message="Failed to fetch dashboards on page",
                    context=str(page),
                    exc=e,
                )
                if e.response and e.response.status_code in (401, 403):
                    self.report.report_permission_warning()
                break

        return dashboards
