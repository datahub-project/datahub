import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
)

logger = logging.getLogger(__name__)


class AirbyteBaseClient(ABC):
    """
    Abstract base client for interacting with the Airbyte API
    """

    def __init__(self, config: AirbyteClientConfig):
        """
        Initialize the Airbyte base client

        Args:
            config: Client configuration
        """
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create and configure a requests Session with retry logic

        Returns:
            Configured requests Session
        """
        session = requests.Session()

        # Set up base headers
        session.headers.update({"Content-Type": "application/json"})

        # Add custom headers if provided
        if self.config.extra_headers:
            session.headers.update(self.config.extra_headers)

        # Configure SSL verification
        session.verify = self.config.verify_ssl
        if self.config.verify_ssl and self.config.ssl_ca_cert:
            if os.path.isfile(self.config.ssl_ca_cert):
                session.verify = self.config.ssl_ca_cert
                logger.debug(f"Using custom CA certificate: {self.config.ssl_ca_cert}")
            else:
                logger.warning(
                    f"CA certificate file not found: {self.config.ssl_ca_cert}. Using default verification."
                )

        if not self.config.verify_ssl:
            logger.warning("SSL certificate verification is disabled")
            # Suppress insecure request warnings when verify is False
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Configure retry logic
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @staticmethod
    def _clean_uri(uri: str) -> str:
        """
        Remove trailing / from URI if present

        Args:
            uri: URI to clean

        Returns:
            Cleaned URI
        """
        return uri.rstrip("/")

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        """
        Make a request to the Airbyte API with retry handling

        Args:
            endpoint: API endpoint
            method: HTTP method
            data: Request payload
            params: URL parameters

        Returns:
            Response as a dictionary

        Raises:
            Exception: If the request fails after retries
        """
        url = self._get_full_url(endpoint)
        logger.debug(f"Making {method} request to {url}")

        try:
            if method == "GET":
                response = self.session.get(
                    url, params=params, timeout=self.config.request_timeout
                )
            elif method == "POST":
                response = self.session.post(
                    url, json=data, params=params, timeout=self.config.request_timeout
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            error_message = f"Airbyte API request failed: {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('message', e.response.text)}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"

            logger.error(error_message)
            raise Exception(error_message) from e
        except requests.RequestException as e:
            error_message = f"Error connecting to Airbyte API: {str(e)}"
            logger.error(error_message)
            raise Exception(error_message) from e

    def _paginate_results(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        result_key: str = "data",
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        next_page_token_key: str = "next",
        offset_param: str = "offset",
    ) -> Iterator[dict]:
        """
        Handle pagination for API endpoints that return paginated results

        Args:
            endpoint: API endpoint
            method: HTTP method
            data: Request payload
            params: URL query parameters
            result_key: Key in the response that contains the results
            page_size: Number of items per page
            limit: Maximum number of items to return (None for all)
            next_page_token_key: Key in the response that contains the next page token
            offset_param: Parameter name for offset-based pagination

        Returns:
            Iterator of result items
        """
        if not page_size:
            page_size = self.config.page_size

        if not params:
            params = {}

        params["limit"] = page_size
        offset = 0
        total_items = 0

        while True:
            if offset > 0:
                params[offset_param] = offset

            response = self._make_request(endpoint, method, data, params)

            # Extract items from the response
            items = response.get(result_key, [])
            if isinstance(items, list):
                for item in items:
                    yield item
                    total_items += 1

                    # Check if we've reached the requested limit
                    if limit and total_items >= limit:
                        return
            else:
                # Not a list, might be just one item or an error
                logger.warning(f"Expected list for {result_key} but got {type(items)}")

            # Check if there are more pages
            next_token = response.get(next_page_token_key)
            if not next_token or next_token == "":
                # No more pages
                break

            # Update offset for the next page
            offset += page_size

    @staticmethod
    def _apply_pattern(
        items: List[Dict[str, Any]], pattern: AllowDenyPattern, name_key: str = "name"
    ) -> List[Dict[str, Any]]:
        """
        Filter a list of items based on a DataHub AllowDenyPattern

        Args:
            items: List of items to filter
            pattern: AllowDenyPattern configuration to apply
            name_key: The key in the item dict that contains the name to match against

        Returns:
            Filtered list of items
        """
        if not items:
            return []

        # If allow_all pattern is used, return all items
        if pattern.allow_all():
            return items

        filtered_items = []
        for item in items:
            name = item.get(name_key, "")
            if name and pattern.allowed(name):
                filtered_items.append(item)

        return filtered_items

    @abstractmethod
    def _get_full_url(self, endpoint: str) -> str:
        """
        Get the full URL for the API endpoint

        Args:
            endpoint: API endpoint

        Returns:
            Full URL for the API endpoint
        """
        pass

    @abstractmethod
    def _check_auth_before_request(self) -> None:
        """
        Check and refresh authentication if needed before making a request
        """
        pass

    def list_workspaces(self, pattern: Optional[AllowDenyPattern] = None) -> List[dict]:
        """
        List all workspaces in Airbyte with pagination and filtering

        Args:
            pattern: AllowDenyPattern to filter workspaces by name

        Returns:
            List of workspaces
        """
        self._check_auth_before_request()
        workspaces = list(
            self._paginate_results(
                endpoint="/workspaces", method="GET", result_key="data"
            )
        )

        if pattern:
            workspaces = self._apply_pattern(workspaces, pattern)

        return workspaces

    def list_connections(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[dict]:
        """
        List all connections in a workspace with filtering

        Args:
            workspace_id: Workspace ID
            pattern: AllowDenyPattern to filter connections by name

        Returns:
            List of connections
        """
        self._check_auth_before_request()
        params = {"workspaceId": workspace_id}
        connections = list(
            self._paginate_results(
                endpoint="/connections", method="GET", params=params, result_key="data"
            )
        )

        # Filter out disabled connections
        active_connections = [
            conn for conn in connections if conn.get("status") != "inactive"
        ]

        if pattern:
            active_connections = self._apply_pattern(active_connections, pattern)

        return active_connections

    def list_jobs(
        self,
        connection_id: str,
        workspace_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[dict]:
        """
        List jobs for a connection

        Args:
            connection_id: Connection ID
            workspace_id: Optional workspace ID to filter jobs
            start_date: Optional start date for job filtering (format: yyyy-mm-ddTHH:MM:SSZ)
            end_date: Optional end date for job filtering (format: yyyy-mm-ddTHH:MM:SSZ)
            limit: Maximum number of jobs to return

        Returns:
            List of jobs
        """
        self._check_auth_before_request()

        params = {
            "configId": connection_id,
            "configTypes": ["sync", "reset_connection"],
            "limit": limit,
        }

        if workspace_id:
            params["workspaceId"] = workspace_id

        if start_date:
            params["updatedAtStart"] = start_date

        if end_date:
            params["updatedAtEnd"] = end_date

        response = self._make_request("/jobs", method="GET", params=params)
        return response.get("jobs", [])

    def get_source(self, source_id: str) -> dict:
        """
        Get source details

        Args:
            source_id: Source ID

        Returns:
            Source details
        """
        self._check_auth_before_request()
        return self._make_request(f"/sources/{source_id}", method="GET")

    def list_sources(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[dict]:
        """
        List all sources in a workspace with filtering

        Args:
            workspace_id: Workspace ID
            pattern: AllowDenyPattern to filter sources by name

        Returns:
            List of sources
        """
        self._check_auth_before_request()

        params = {"workspaceId": workspace_id}
        sources = list(
            self._paginate_results(
                endpoint="/sources", method="GET", params=params, result_key="data"
            )
        )

        if pattern:
            sources = self._apply_pattern(sources, pattern)

        return sources

    def get_destination(self, destination_id: str) -> dict:
        """
        Get destination details

        Args:
            destination_id: Destination ID

        Returns:
            Destination details
        """
        self._check_auth_before_request()
        return self._make_request(f"/destinations/{destination_id}", method="GET")

    def list_destinations(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[dict]:
        """
        List all destinations in a workspace with filtering

        Args:
            workspace_id: Workspace ID
            pattern: AllowDenyPattern to filter destinations by name

        Returns:
            List of destinations
        """
        self._check_auth_before_request()

        params = {"workspaceId": workspace_id}
        destinations = list(
            self._paginate_results(
                endpoint="/destinations", method="GET", params=params, result_key="data"
            )
        )

        if pattern:
            destinations = self._apply_pattern(destinations, pattern)

        return destinations

    def get_connection(self, connection_id: str) -> Dict:
        """
        Get connection details including sync catalog

        Args:
            connection_id: Connection ID

        Returns:
            Connection details including sync catalog
        """
        self._check_auth_before_request()
        return self._make_request(f"/connections/{connection_id}", method="GET")

    def list_streams(
        self, source_id: Optional[str] = None, destination_id: Optional[str] = None
    ) -> List[dict]:
        """
        List streams available in Airbyte.
        Can be filtered by source_id or destination_id.

        Args:
            source_id: Optional source ID to filter streams
            destination_id: Optional destination ID to filter streams

        Returns:
            List of stream metadata
        """
        self._check_auth_before_request()
        query_params = []
        if source_id:
            query_params.append(f"sourceId={source_id}")
        if destination_id:
            query_params.append(f"destinationId={destination_id}")

        endpoint = "/streams"
        if query_params:
            endpoint = f"{endpoint}?{'&'.join(query_params)}"

        response = self._make_request(endpoint, method="GET")
        return response if isinstance(response, list) else response.get("streams", [])

    def get_job(self, job_id: str) -> Dict:
        """
        Get job details

        Args:
            job_id: Job ID

        Returns:
            Job details
        """
        self._check_auth_before_request()
        return self._make_request(f"/jobs/{job_id}", method="GET")

    def list_tags(self, workspace_id: str) -> List[dict]:
        """
        List all available tags in Airbyte

        Returns:
            List of tags for the given workspace_id
        """
        self._check_auth_before_request()
        response = self._make_request(
            f"/tags?workspaceIds={workspace_id}", method="GET"
        )
        return response.get("tags", [])


class AirbyteOSSClient(AirbyteBaseClient):
    """
    Client for interacting with the Airbyte Open Source API
    """

    def __init__(self, config: AirbyteClientConfig):
        """
        Initialize the Airbyte Open Source client

        Args:
            config: Client configuration
        """
        super().__init__(config)

        # Ensure host_port is not None for OSS
        if not config.host_port:
            raise ValueError("host_port is required for open_source deployment")

        # Set the base URL for API requests - OSS uses /api/public/v1
        self.base_url = f"{self._clean_uri(config.host_port)}/api/public/v1"

        # Configure authentication
        self._setup_authentication()

    def _setup_authentication(self) -> None:
        """
        Set up the appropriate authentication method based on configuration.
        Prioritizes API key/token over username/password if both are provided.
        """
        # Check for API key/token first (PAT)
        if self.config.api_key:
            # Add as Bearer token in the Authorization header
            token = self.config.api_key.get_secret_value()
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.debug("Using API key/token authentication")
        # Fall back to basic auth if username is provided
        elif self.config.username:
            password = (
                self.config.password.get_secret_value() if self.config.password else ""
            )
            self.session.auth = HTTPBasicAuth(self.config.username, password)
            logger.debug("Using basic authentication")
        else:
            logger.debug("No authentication credentials provided")

    def _get_full_url(self, endpoint: str) -> str:
        """
        Get the full URL for the API endpoint

        Args:
            endpoint: API endpoint

        Returns:
            Full URL for the API endpoint
        """
        return f"{self.base_url}{endpoint}"

    def _check_auth_before_request(self) -> None:
        """
        Check authentication before making a request - no-op for OSS client
        """
        pass


class AirbyteCloudClient(AirbyteBaseClient):
    """
    Client for interacting with the Airbyte Cloud API
    """

    # Constants for Airbyte Cloud
    CLOUD_BASE_URL = "https://api.airbyte.com/v1"
    TOKEN_URL = "https://auth.airbyte.com/oauth/token"

    def __init__(self, config: AirbyteClientConfig):
        """
        Initialize the Airbyte Cloud client

        Args:
            config: Client configuration
        """
        super().__init__(config)

        # Set the workspace ID
        self.workspace_id = config.cloud_workspace_id
        if not self.workspace_id:
            raise ValueError("Workspace ID is required for Airbyte Cloud")

        # Set the base URL for API requests
        self.base_url = self.CLOUD_BASE_URL

        # Set up OAuth2 authentication
        self._setup_oauth_authentication()

    def _setup_oauth_authentication(self) -> None:
        """
        Set up OAuth2 authentication for Airbyte Cloud
        """
        if not self.config.oauth2_client_id or not self.config.oauth2_refresh_token:
            raise ValueError(
                "OAuth2 client ID and refresh token are required for Airbyte Cloud"
            )

        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required for Airbyte Cloud")

        # Set up initial token
        self._refresh_oauth_token()

    def _refresh_oauth_token(self) -> None:
        """
        Refresh the OAuth2 token
        """
        logger.debug("Refreshing OAuth2 token")

        # Ensure oauth2_client_secret is not None
        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required for token refresh")

        # Ensure oauth2_refresh_token is not None
        if not self.config.oauth2_refresh_token:
            raise ValueError("OAuth2 refresh token is required for token refresh")

        data = {
            "client_id": self.config.oauth2_client_id,
            "client_secret": self.config.oauth2_client_secret.get_secret_value(),
            "refresh_token": self.config.oauth2_refresh_token.get_secret_value(),
            "grant_type": "refresh_token",
        }

        try:
            response = requests.post(
                self.TOKEN_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.token_expiry = (
                time.time() + token_data.get("expires_in", 3600) - 300
            )  # 5 min buffer

            # Update session headers with the new token
            self.session.headers.update(
                {"Authorization": f"Bearer {self.access_token}"}
            )

            logger.debug("OAuth2 token refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh OAuth2 token: {str(e)}")
            raise

    def _check_token_expiry(self) -> None:
        """
        Check if the token is about to expire and refresh if necessary
        """
        if hasattr(self, "token_expiry") and time.time() >= self.token_expiry:
            self._refresh_oauth_token()

    def _get_full_url(self, endpoint: str) -> str:
        """
        Get the full URL for the API endpoint

        Args:
            endpoint: API endpoint

        Returns:
            Full URL for the API endpoint
        """
        return urljoin(self.base_url, endpoint.lstrip("/"))

    def _check_auth_before_request(self) -> None:
        """
        Check and refresh authentication if needed before making a request
        """
        self._check_token_expiry()

    def list_workspaces(self, pattern: Optional[AllowDenyPattern] = None) -> List[dict]:
        """
        List all workspaces in Airbyte Cloud
        For Cloud, we only return the configured workspace

        Returns:
            List of workspaces containing the configured workspace
        """
        self._check_auth_before_request()
        try:
            workspace = self._make_request(
                f"/workspaces/{self.workspace_id}", method="GET"
            )
            workspaces = [workspace] if workspace else []

            if pattern:
                workspaces = self._apply_pattern(workspaces, pattern)

            return workspaces
        except Exception as e:
            logger.warning(f"Failed to get workspace information: {str(e)}")
            # If we can't get the specific workspace, return empty list
            return []


def create_airbyte_client(config: AirbyteClientConfig) -> AirbyteBaseClient:
    """
    Factory function to create the appropriate Airbyte client based on deployment type

    Args:
        config: Airbyte client configuration

    Returns:
        Appropriate Airbyte client implementation

    Raises:
        ValueError: If the deployment type is invalid
    """
    if config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE:
        return AirbyteOSSClient(config)
    elif config.deployment_type == AirbyteDeploymentType.CLOUD:
        return AirbyteCloudClient(config)
    else:
        raise ValueError(f"Invalid deployment type: {config.deployment_type}")
