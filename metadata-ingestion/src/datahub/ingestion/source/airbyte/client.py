import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypedDict
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


class StreamConfigDict(TypedDict):
    """Type definition for stream configuration returned by _build_stream_config."""

    selected: bool
    syncMode: str
    destinationSyncMode: str
    primaryKey: List[List[str]]
    cursorField: List[str]


class StreamSchemaDict(TypedDict):
    """Type definition for stream schema in syncCatalog."""

    name: str
    namespace: Optional[str]
    jsonSchema: Dict[str, object]


class StreamSyncDict(TypedDict):
    """Type definition for stream in syncCatalog with config."""

    stream: StreamSchemaDict
    config: StreamConfigDict


class SyncCatalogDict(TypedDict):
    """Type definition for syncCatalog structure."""

    streams: List[StreamSyncDict]


class JsonSchemaDict(TypedDict, total=False):
    """Type definition for JSON Schema structure.

    Uses total=False since properties and other fields are optional.
    """

    type: str
    properties: Dict[str, Dict[str, object]]


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

        session.headers.update({"Content-Type": "application/json"})

        if self.config.extra_headers:
            session.headers.update(self.config.extra_headers)

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
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

            items = response.get(result_key, [])
            if isinstance(items, list):
                for item in items:
                    yield item
                    total_items += 1

                    if limit and total_items >= limit:
                        return
            else:
                logger.warning(f"Expected list for {result_key} but got {type(items)}")

            next_token = response.get(next_page_token_key)
            if not next_token or next_token == "":
                break

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

    def get_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Get connection details including sync catalog

        Args:
            connection_id: Connection ID

        Returns:
            Connection details including sync catalog
        """
        self._check_auth_before_request()
        connection_data = self._make_request(
            f"/connections/{connection_id}", method="GET"
        )

        # Airbyte 1.x public API returns 'configurations.streams' instead of 'syncCatalog.streams'
        # Transform to the format expected by DataHub connector for backward compatibility
        if not connection_data.get("syncCatalog") and connection_data.get(
            "configurations"
        ):
            configurations = connection_data.get("configurations", {})
            if "streams" in configurations:
                # Fetch column schema information from /streams endpoint
                stream_property_fields = self._fetch_stream_property_fields(
                    connection_data.get("sourceId")
                )

                # Build syncCatalog from configurations.streams
                connection_data["syncCatalog"] = self._build_sync_catalog(
                    configurations["streams"], stream_property_fields
                )

        return connection_data

    def _fetch_stream_property_fields(
        self, source_id: Optional[str]
    ) -> Dict[Tuple[str, str], List[List[str]]]:
        """
        Fetch propertyFields (column names) from /streams endpoint.

        Args:
            source_id: Source ID to fetch streams for

        Returns:
            Dict mapping (stream_name, namespace) tuple to list of property field paths.
            Each property field is represented as a list of strings (e.g., [["column_name"]] for simple columns).
        """
        if not source_id:
            return {}

        try:
            detailed_streams = self.list_streams(source_id=source_id)
            stream_property_fields = {}

            for stream in detailed_streams:
                if not isinstance(stream, dict) or not stream.get("propertyFields"):
                    continue

                # Extract stream name (handle field name variations)
                stream_name = stream.get("streamName") or stream.get("name")
                if not stream_name or not isinstance(stream_name, str):
                    continue

                # Extract namespace (handle field name variations)
                # Default to empty string to match return type Dict[Tuple[str, str], ...]
                namespace = (
                    stream.get("namespace")
                    or stream.get("streamnamespace")
                    or stream.get("streamNamespace")
                    or ""
                )
                if not isinstance(namespace, str):
                    namespace = ""

                stream_property_fields[(stream_name, namespace)] = stream.get(
                    "propertyFields", []
                )

            if stream_property_fields:
                logger.info(
                    f"Retrieved propertyFields for {len(stream_property_fields)} streams from /streams endpoint"
                )

            return stream_property_fields

        except Exception as e:
            logger.debug(
                f"/streams endpoint not available or failed: {e}. Using schema from configurations."
            )
            return {}

    def _build_sync_catalog(
        self,
        config_streams: List[Dict[str, Any]],
        stream_property_fields: Dict[Tuple[str, str], List[List[str]]],
    ) -> SyncCatalogDict:
        """
        Build syncCatalog structure from configurations.streams.

        Args:
            config_streams: List of stream dicts from configurations (raw API response)
            stream_property_fields: Property fields from /streams endpoint, mapping (stream_name, namespace) to column paths

        Returns:
            syncCatalog dict with streams array
        """
        streams: List[StreamSyncDict] = []
        for stream in config_streams:
            # Extract name and namespace with type safety
            name = stream.get("name")
            namespace = stream.get("namespace")

            # Ensure types match TypedDict requirements
            if not isinstance(name, str):
                name = str(name) if name is not None else ""
            if not isinstance(namespace, str):
                namespace = str(namespace) if namespace is not None else ""

            # Get property fields using properly typed tuple key
            property_fields = stream_property_fields.get((name, namespace))

            stream_schema: StreamSchemaDict = {
                "name": name,
                "namespace": namespace if namespace else None,
                "jsonSchema": self._get_json_schema_for_stream(stream, property_fields),
            }

            stream_sync: StreamSyncDict = {
                "stream": stream_schema,
                "config": self._build_stream_config(stream),
            }

            streams.append(stream_sync)

        return {"streams": streams}

    def _build_stream_config(self, stream: Dict[str, Any]) -> StreamConfigDict:
        """
        Build stream config from configurations stream data.

        Args:
            stream: Stream data dict from configurations (raw API response)

        Returns:
            Stream config dict with syncMode, destinationSyncMode, etc.
        """
        sync_mode = stream.get("syncMode", "")

        return {
            "selected": True,
            "syncMode": sync_mode.split("_")[0] if sync_mode else "full_refresh",
            "destinationSyncMode": (
                sync_mode.split("_")[1] if "_" in sync_mode else "overwrite"
            ),
            "primaryKey": stream.get("primaryKey", []),
            "cursorField": stream.get("cursorField", []),
        }

    def _get_json_schema_for_stream(
        self,
        stream: Dict[str, Any],
        property_fields: Optional[List[List[str]]] = None,
    ) -> Dict[str, object]:
        """Get jsonSchema for a stream, converting propertyFields if needed.

        Args:
            stream: Stream data dict from configurations (raw API response)
            property_fields: propertyFields from /streams endpoint (if available), where each field is a list of strings representing the path

        Returns:
            jsonSchema dict with properties (structure varies by schema), or empty dict if not available
        """
        # Priority 1: Convert propertyFields from /streams endpoint to jsonSchema format
        # This is the primary source in modern Airbyte (1.8+) public API
        if property_fields:
            properties = {}
            for field in property_fields:
                # propertyFields is [[field_name], ...] format
                field_name = field[0] if isinstance(field, list) and field else field
                if field_name:
                    properties[field_name] = {
                        "type": ["null", "string"]
                    }  # Generic type

            if properties:
                return {"type": "object", "properties": properties}

        # Priority 2: Check if configurations already has jsonSchema
        # This is rare/non-existent in public API but may exist in other deployments
        if stream.get("jsonSchema"):
            return stream["jsonSchema"]
        if stream.get("json_schema"):
            return stream["json_schema"]

        # Priority 3: Empty schema (no column-level lineage available)
        return {}

    def list_streams(
        self, source_id: Optional[str] = None, destination_id: Optional[str] = None
    ) -> List[dict]:
        """
        List streams available in Airbyte with detailed schema information.
        Can be filtered by source_id or destination_id.

        This endpoint provides propertyFields (column names) which are converted to
        jsonSchema format for extracting column-level lineage. This is the primary
        source of schema information in modern Airbyte (1.8+) public API.

        Note: This endpoint may not be available in older Airbyte versions.
        The connector gracefully handles failures and falls back to other sources.

        Args:
            source_id: Optional source ID to filter streams
            destination_id: Optional destination ID to filter streams

        Returns:
            List of stream metadata with propertyFields (column names)

        Raises:
            Exception: If the endpoint is not available (handled gracefully by caller)
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

    def get_job(self, job_id: str) -> Dict[str, Any]:
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

        if not config.host_port:
            raise ValueError("host_port is required for open_source deployment")

        # OSS uses /api/public/v1 per https://docs.airbyte.com/developers/api-documentation
        self.base_url = f"{self._clean_uri(config.host_port)}/api/public/v1"

        self._setup_authentication()

    def _setup_authentication(self) -> None:
        """
        Set up the appropriate authentication method based on configuration.
        Prioritizes API key/token over username/password if both are provided.
        """
        if self.config.api_key:
            token = self.config.api_key.get_secret_value()
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.debug("Using API key/token authentication")
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

        self.base_url = self.CLOUD_BASE_URL
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

        self._refresh_oauth_token()

    def _refresh_oauth_token(self) -> None:
        """
        Refresh the OAuth2 token
        """
        logger.debug("Refreshing OAuth2 token")

        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required for token refresh")

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
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Workspace {self.workspace_id} not found")
                return []
            else:
                logger.error(f"Failed to get workspace: HTTP {e.response.status_code}")
                raise
        except requests.RequestException as e:
            logger.error(f"Network error: {str(e)}")
            raise


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
