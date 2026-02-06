"""Base REST client for Microsoft Fabric workloads."""

import logging
from abc import ABC, abstractmethod
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.report import FabricClientReport

logger = logging.getLogger(__name__)

# Fabric REST API base URL
FABRIC_API_BASE_URL = "https://api.fabric.microsoft.com/v1"

# Retry configuration
RETRY_MAX_TIMES = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_BACKOFF_FACTOR = 1
RETRY_ALLOWED_METHODS = ["GET", "POST"]


class BaseFabricClient(ABC):
    """Base class for all Microsoft Fabric REST API clients.

    Provides common functionality:
    - Authentication via Azure TokenCredential
    - HTTP session management with retry logic
    - Error handling and logging
    """

    @abstractmethod
    def get_base_endpoint(self) -> str:
        """Get the base API endpoint for this client.

        Returns:
            Base endpoint path (e.g., 'workspaces' for OneLake)
        """
        pass

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricClientReport] = None,
    ):
        """Initialize the base client.

        Args:
            auth_helper: Authentication helper for getting Bearer tokens
            timeout: Request timeout in seconds
            report: Optional client report for tracking metrics
        """
        self.auth_helper = auth_helper
        self.timeout = timeout
        self._session = self._create_session()
        self.report = report or FabricClientReport()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.

        Returns:
            Configured requests.Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=RETRY_MAX_TIMES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=RETRY_ALLOWED_METHODS,
            raise_on_status=False,  # We'll handle status codes ourselves
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        return session

    def _get_headers(self) -> dict:
        """Get headers for API requests, including authorization.

        Returns:
            Dictionary of headers
        """
        headers = {}
        try:
            headers["Authorization"] = self.auth_helper.get_authorization_header()
        except Exception as e:
            logger.error(f"Failed to get authorization header: {e}")
            raise
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """Make an authenticated HTTP request.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            json: JSON body for POST requests

        Returns:
            Response object

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{FABRIC_API_BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        self.report.report_request()

        logger.debug(f"Making {method} request to {endpoint}")
        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=self.timeout,
            )

            # Raise for status codes
            response.raise_for_status()

            logger.debug(f"Successfully completed {method} request to {endpoint}")
            return response

        except requests.exceptions.HTTPError as e:
            self.report.report_error()
            logger.error(
                f"HTTP error {e.response.status_code} for {method} {url}: {e.response.text}"
            )
            raise
        except requests.exceptions.RequestException as e:
            self.report.report_error()
            logger.error(f"Request error for {method} {url}: {e}")
            raise

    def _list_workspaces_raw(self) -> Iterator[dict]:
        """List all accessible Fabric workspaces (raw data).

        This method is common to all Fabric workloads.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/workspaces/list

        Yields:
            Workspace data dictionaries
        """
        logger.info("Listing Fabric workspaces")
        try:
            response = self.get("workspaces")
            data = response.json()

            workspaces = data.get("value", [])
            logger.info(f"Found {len(workspaces)} workspace(s)")

            for workspace_data in workspaces:
                logger.debug(
                    f"Processing workspace: {workspace_data.get('displayName', 'Unknown')}"
                )
                yield workspace_data

            # TODO: Handle continuation token if present in response
            # continuation_token = data.get("continuationToken")

        except Exception as e:
            logger.error(f"Failed to list workspaces: {e}")
            raise

    def get(self, endpoint: str, params: Optional[dict] = None) -> requests.Response:
        """Make a GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object
        """
        return self._request("GET", endpoint, params=params)

    def post(
        self, endpoint: str, json: Optional[dict] = None, params: Optional[dict] = None
    ) -> requests.Response:
        """Make a POST request.

        Args:
            endpoint: API endpoint
            json: JSON body
            params: Query parameters

        Returns:
            Response object
        """
        return self._request("POST", endpoint, json=json, params=params)

    def close(self) -> None:
        """Close the session and release resources."""
        if self._session:
            self._session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
