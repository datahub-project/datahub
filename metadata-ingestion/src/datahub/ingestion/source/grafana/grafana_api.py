"""API client for Grafana metadata extraction"""

import logging
from typing import List, Optional

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
        report: GrafanaSourceReport,
    ) -> None:
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.session = self._create_session(token)
        self.report = report

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

        self.report.warning("SSL Verification is recommended.")

        return session

    def get_folders(self) -> List[Folder]:
        """Fetch all folders from Grafana"""
        try:
            response = self.session.get(f"{self.base_url}/api/folders")
            response.raise_for_status()
            return [Folder.from_dict(folder) for folder in response.json()]
        except requests.exceptions.RequestException as e:
            self.report.report_exc(
                message="Failed to fetch folders",
                exc=e,
            )
            return []

    def get_dashboard(self, uid: str) -> Optional[Dashboard]:
        """Fetch a specific dashboard by UID"""
        try:
            response = self.session.get(f"{self.base_url}/api/dashboards/uid/{uid}")
            response.raise_for_status()
            return Dashboard.from_dict(response.json())
        except requests.exceptions.RequestException as e:
            self.report.warning(
                message="Failed to fetch dashboard",
                context=uid,
                exc=e,
            )
            return None

    def get_dashboards(self) -> List[Dashboard]:
        """Fetch all dashboards from search endpoint"""
        try:
            response = self.session.get(f"{self.base_url}/api/search?type=dash-db")
            response.raise_for_status()

            dashboards = []
            for result in response.json():
                dashboard = self.get_dashboard(result["uid"])
                if dashboard:
                    dashboards.append(dashboard)

            return dashboards
        except requests.exceptions.RequestException as e:
            self.report.report_exc(
                message="Failed to fetch dashboards",
                exc=e,
            )
            return []
