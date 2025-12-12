import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, TypedDict
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
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbyteSourcePartial,
    AirbyteWorkspacePartial,
    PropertyFieldPath,
    StreamIdentifier,
)

"""
Exception Handling Strategy:

The client layer (this module) raises custom exceptions for all API errors:
- AirbyteApiError: For HTTP errors, connection failures, JSON parsing errors
- AirbyteAuthenticationError: For OAuth token refresh failures (Cloud only)

The source layer (source.py) catches these exceptions and:
- Logs them as warnings/errors with context
- Reports them as failures via the reporting API
- Continues processing other entities (non-fatal)

This allows the ingestion to continue even when individual API calls fail,
while still providing visibility into the errors that occurred.

API Response Validation:

- Client methods return Dict[str, Any] (raw API responses)
- Source layer validates responses using Pydantic models via model_validate()
- This keeps the client layer thin and the validation logic centralized
"""

logger = logging.getLogger(__name__)

# HTTP Header Constants
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"

# OAuth Token Constants
DEFAULT_TOKEN_EXPIRY_SECONDS = 3600  # 1 hour default from OAuth providers
TOKEN_REFRESH_BUFFER_SECONDS = (
    600  # Refresh 10 minutes before expiry to avoid race conditions
)


class AirbyteApiError(Exception):
    """Raised when Airbyte API request fails.

    This exception is raised for HTTP errors, connection errors, or any other
    issues encountered while communicating with the Airbyte API.
    """

    pass


class AirbyteAuthenticationError(AirbyteApiError):
    """Raised when authentication with Airbyte API fails."""

    pass


class StreamConfigDict(TypedDict, total=False):
    """Type definition for stream configuration returned by _build_stream_config."""

    selected: bool
    syncMode: str
    destinationSyncMode: str
    primaryKey: List[List[str]]
    cursorField: List[str]
    destinationNamespace: str  # Per-stream destination schema override
    aliasName: str  # Per-stream table name override
    selectedFields: List[str]  # Per-stream column selection
    fieldSelectionEnabled: bool  # Whether field selection is enabled


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
    """Abstract base client for interacting with the Airbyte API."""

    def __init__(self, config: AirbyteClientConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create and configure requests Session with retry logic."""
        session = requests.Session()

        session.headers.update({"Content-Type": CONTENT_TYPE_JSON})

        if self.config.extra_headers:
            session.headers.update(self.config.extra_headers)

        session.verify = self.config.verify_ssl
        if self.config.verify_ssl and self.config.ssl_ca_cert:
            if os.path.isfile(self.config.ssl_ca_cert):
                session.verify = self.config.ssl_ca_cert
                logger.debug("Using custom CA certificate: %s", self.config.ssl_ca_cert)
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
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a GET request to the Airbyte API with automatic retry handling.

        Args:
            endpoint: API endpoint (e.g., "/workspaces")
            params: URL query parameters

        Returns:
            Parsed JSON response

        Raises:
            AirbyteApiError: For HTTP errors, connection failures, or JSON parsing errors
        """
        url = self._get_full_url(endpoint)
        logger.debug("Making GET request to %s", url)

        try:
            response = self.session.get(
                url, params=params, timeout=self.config.request_timeout
            )
            response.raise_for_status()
            response_data = response.json()
            logger.debug(
                "Airbyte API response from %s: %s",
                endpoint,
                response_data,
            )
            return response_data
        except requests.HTTPError as e:
            error_message = f"Airbyte API request failed: {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('message', e.response.text)}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"

            logger.error(error_message)
            raise AirbyteApiError(error_message) from e
        except requests.RequestException as e:
            error_message = f"Error connecting to Airbyte API: {str(e)}"
            logger.error(error_message)
            raise AirbyteApiError(error_message) from e

    def _paginate_results(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        result_key: str = "data",
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        next_page_token_key: str = "next",
        offset_param: str = "offset",
    ) -> Iterator[dict]:
        """Handle pagination for API endpoints."""
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

            response = self._make_request(endpoint, params=params)

            items = response.get(result_key, [])
            if isinstance(items, list):
                for item in items:
                    yield item
                    total_items += 1

                    if limit and total_items >= limit:
                        return
            else:
                logger.warning(
                    "Expected list for %s but got %s", result_key, type(items)
                )

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
        """Get full URL for API endpoint."""
        pass

    @abstractmethod
    def _check_auth_before_request(self) -> None:
        """Check and refresh authentication before request."""
        pass

    def list_workspaces(
        self, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteWorkspacePartial]:
        """List all workspaces with optional filtering."""
        self._check_auth_before_request()
        workspaces_data = list(
            self._paginate_results(endpoint="/workspaces", result_key="data")
        )

        if pattern:
            workspaces_data = self._apply_pattern(workspaces_data, pattern)

        return [AirbyteWorkspacePartial.model_validate(w) for w in workspaces_data]

    def list_connections(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteConnectionPartial]:
        """List connections in workspace with optional filtering."""
        self._check_auth_before_request()
        params = {"workspaceId": workspace_id}
        connections = list(
            self._paginate_results(
                endpoint="/connections", params=params, result_key="data"
            )
        )

        active_connections = [
            conn for conn in connections if conn.get("status") != "inactive"
        ]

        if pattern:
            active_connections = self._apply_pattern(active_connections, pattern)

        return [AirbyteConnectionPartial.model_validate(c) for c in active_connections]

    def list_jobs(
        self,
        connection_id: str,
        workspace_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
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

        response = self._make_request("/jobs", params=params)
        return response.get("jobs", [])

    def get_source(self, source_id: str) -> AirbyteSourcePartial:
        """Get source details.

        Args:
            source_id: Source ID

        Returns:
            Validated source model
        """
        self._check_auth_before_request()
        source_data = self._make_request(f"/sources/{source_id}")
        return AirbyteSourcePartial.model_validate(source_data)

    def list_sources(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteSourcePartial]:
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
        sources_data = list(
            self._paginate_results(
                endpoint="/sources", params=params, result_key="data"
            )
        )

        if pattern:
            sources_data = self._apply_pattern(sources_data, pattern)

        return [AirbyteSourcePartial.model_validate(s) for s in sources_data]

    def get_destination(self, destination_id: str) -> AirbyteDestinationPartial:
        """Get destination details.

        Args:
            destination_id: Destination ID

        Returns:
            Validated destination model
        """
        self._check_auth_before_request()
        dest_data = self._make_request(f"/destinations/{destination_id}")
        return AirbyteDestinationPartial.model_validate(dest_data)

    def list_destinations(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteDestinationPartial]:
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
        destinations_data = list(
            self._paginate_results(
                endpoint="/destinations", params=params, result_key="data"
            )
        )

        if pattern:
            destinations_data = self._apply_pattern(destinations_data, pattern)

        return [AirbyteDestinationPartial.model_validate(d) for d in destinations_data]

    def get_connection(self, connection_id: str) -> AirbyteConnectionPartial:
        """Get connection details including sync catalog.

        Args:
            connection_id: Connection ID

        Returns:
            Validated connection model
        """
        self._check_auth_before_request()
        connection_data = self._make_request(f"/connections/{connection_id}")

        # Airbyte 1.x public API returns 'configurations.streams' instead of 'syncCatalog.streams'
        # Transform to the format expected by DataHub connector for backward compatibility
        if not connection_data.get("syncCatalog") and connection_data.get(
            "configurations"
        ):
            configurations = connection_data.get("configurations", {})
            if "streams" in configurations:
                stream_property_fields = self._fetch_stream_property_fields(
                    connection_data.get("sourceId")
                )

                connection_data["syncCatalog"] = self._build_sync_catalog(
                    configurations["streams"], stream_property_fields
                )

        return AirbyteConnectionPartial.model_validate(connection_data)

    def _fetch_stream_property_fields(
        self, source_id: Optional[str]
    ) -> Dict[StreamIdentifier, List[PropertyFieldPath]]:
        """
        Fetch propertyFields (column names) from /streams endpoint.

        Args:
            source_id: Source ID to fetch streams for

        Returns:
            Dict mapping StreamIdentifier to list of PropertyFieldPath objects.
        """
        if not source_id:
            return {}

        try:
            detailed_streams = self.list_streams(source_id=source_id)
            stream_property_fields = {}

            for stream in detailed_streams:
                if not isinstance(stream, dict) or not stream.get("propertyFields"):
                    continue

                stream_name = stream.get("streamName") or stream.get("name")
                if not stream_name or not isinstance(stream_name, str):
                    continue

                namespace = (
                    stream.get("namespace")
                    or stream.get("streamnamespace")
                    or stream.get("streamNamespace")
                    or ""
                )
                if not isinstance(namespace, str):
                    namespace = ""

                stream_id = StreamIdentifier(
                    stream_name=stream_name, namespace=namespace
                )
                raw_fields = stream.get("propertyFields", [])
                property_paths = [
                    PropertyFieldPath(
                        path=field if isinstance(field, list) else [field]
                    )
                    for field in raw_fields
                    if field
                ]
                stream_property_fields[stream_id] = property_paths

            if stream_property_fields:
                logger.debug(
                    "Retrieved propertyFields for %d streams from /streams endpoint",
                    len(stream_property_fields),
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
        stream_property_fields: Dict[StreamIdentifier, List[PropertyFieldPath]],
    ) -> SyncCatalogDict:
        """
        Build syncCatalog structure from configurations.streams.

        Args:
            config_streams: List of stream dicts from configurations (raw API response)
            stream_property_fields: Property fields from /streams endpoint, mapping StreamIdentifier to PropertyFieldPath objects

        Returns:
            syncCatalog dict with streams array
        """
        streams: List[StreamSyncDict] = []
        for stream in config_streams:
            name = stream.get("name")
            namespace = stream.get("namespace")

            if not isinstance(name, str):
                name = str(name) if name is not None else ""
            if not isinstance(namespace, str):
                namespace = str(namespace) if namespace is not None else ""

            stream_id = StreamIdentifier(stream_name=name, namespace=namespace)
            property_fields = stream_property_fields.get(stream_id)

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

        config: StreamConfigDict = {
            "selected": True,
            "syncMode": sync_mode.split("_")[0] if sync_mode else "full_refresh",
            "destinationSyncMode": (
                sync_mode.split("_")[1] if "_" in sync_mode else "overwrite"
            ),
            "primaryKey": stream.get("primaryKey", []),
            "cursorField": stream.get("cursorField", []),
        }

        # Preserve per-stream overrides if present
        if "destinationNamespace" in stream:
            config["destinationNamespace"] = stream["destinationNamespace"]

        if "aliasName" in stream:
            config["aliasName"] = stream["aliasName"]

        if "selectedFields" in stream:
            config["selectedFields"] = stream["selectedFields"]

        if "fieldSelectionEnabled" in stream:
            config["fieldSelectionEnabled"] = stream["fieldSelectionEnabled"]

        return config

    def _get_json_schema_for_stream(
        self,
        stream: Dict[str, Any],
        property_fields: Optional[List[PropertyFieldPath]] = None,
    ) -> Dict[str, object]:
        """Get jsonSchema for a stream, converting propertyFields if needed.

        Args:
            stream: Stream data dict from configurations (raw API response)
            property_fields: Property fields from /streams endpoint (if available)

        Returns:
            jsonSchema dict with properties (structure varies by schema), or empty dict if not available
        """
        # Priority 1: propertyFields from /streams endpoint (Airbyte 1.8+)
        if property_fields:
            properties = {}
            for field_path in property_fields:
                field_name = field_path.field_name
                if field_name:
                    properties[field_name] = {
                        "type": ["null", "string"]
                    }  # Generic type

            if properties:
                return {"type": "object", "properties": properties}

        # Priority 2: jsonSchema from configurations (rare in public API)
        if stream.get("jsonSchema"):
            return stream["jsonSchema"]
        if stream.get("json_schema"):
            return stream["json_schema"]

        # Priority 3: Empty schema (no column-level lineage)
        return {}

    def list_streams(
        self, source_id: Optional[str] = None, destination_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
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

        response = self._make_request(endpoint)
        return response if isinstance(response, list) else response.get("streams", [])

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed job information including sync statistics

        Args:
            job_id: Job ID

        Returns:
            Job details including bytesCommitted, recordsCommitted, streamStatuses, etc.
        """
        self._check_auth_before_request()
        return self._make_request(f"/jobs/{job_id}")

    def list_tags(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List all available tags in Airbyte

        Returns:
            List of tags for the given workspace_id
        """
        self._check_auth_before_request()
        response = self._make_request(f"/tags?workspaceIds={workspace_id}")
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

    def __init__(self, config: AirbyteClientConfig):
        """
        Initialize the Airbyte Cloud client

        Args:
            config: Client configuration
        """
        super().__init__(config)

        self.workspace_id = config.cloud_workspace_id
        if not self.workspace_id:
            raise ValueError("Workspace ID is required for Airbyte Cloud")

        self.base_url = config.cloud_api_url
        self.token_url = config.cloud_oauth_token_url

        # OAuth tokens are stored in memory and refreshed automatically.
        # For multi-hour ingestion jobs, tokens are refreshed before each API request
        # via _check_auth_before_request() which checks token expiry with a 10-min buffer.
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
                self.token_url,
                data=data,
                headers={"Content-Type": CONTENT_TYPE_FORM_URLENCODED},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", DEFAULT_TOKEN_EXPIRY_SECONDS)
            self.token_expiry = time.time() + expires_in

            self.session.headers.update(
                {"Authorization": f"Bearer {self.access_token}"}
            )

            logger.debug("OAuth2 token refreshed successfully")
        except Exception as e:
            logger.error("Failed to refresh OAuth2 token: %s", str(e))
            raise

    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> Any:
        """Override to add OAuth token retry logic for 401/403 responses."""
        url = self._get_full_url(endpoint)
        logger.debug("Making GET request to %s", url)

        try:
            response = self.session.get(
                url, params=params, timeout=self.config.request_timeout
            )
            response.raise_for_status()
            response_data = response.json()
            logger.debug(
                "Airbyte API response from %s: %s",
                endpoint,
                response_data,
            )
            return response_data
        except requests.HTTPError as e:
            if e.response.status_code in (401, 403):
                logger.warning(
                    f"Received {e.response.status_code}, attempting token refresh"
                )
                try:
                    logger.info("Refreshing token after server invalidation")
                    self._refresh_oauth_token()

                    response = self.session.get(
                        url, params=params, timeout=self.config.request_timeout
                    )
                    response.raise_for_status()
                    response_data = response.json()
                    logger.debug(
                        "Airbyte API response from %s (after token refresh): %s",
                        endpoint,
                        response_data,
                    )
                    return response_data
                except Exception:
                    logger.error(
                        "Token refresh failed, authentication error is terminal"
                    )

            # Fall through to raise original error
            error_message = f"Airbyte API request failed: {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('message', e.response.text)}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"

            logger.error(error_message)
            raise AirbyteApiError(error_message) from e
        except requests.RequestException as e:
            error_message = f"Error connecting to Airbyte API: {str(e)}"
            logger.error(error_message)
            raise AirbyteApiError(error_message) from e

    def _check_token_expiry(self) -> None:
        """Check token expiry with 10-minute buffer to avoid race conditions."""
        if hasattr(self, "token_expiry") and time.time() >= (
            self.token_expiry - TOKEN_REFRESH_BUFFER_SECONDS
        ):
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

    def list_workspaces(
        self, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteWorkspacePartial]:
        """List all workspaces in Airbyte Cloud.

        For Cloud, we only return the configured workspace.

        Returns:
            List of validated workspace models
        """
        self._check_auth_before_request()
        try:
            workspace_data = self._make_request(f"/workspaces/{self.workspace_id}")
            workspaces_data = [workspace_data] if workspace_data else []

            if pattern:
                workspaces_data = self._apply_pattern(workspaces_data, pattern)

            return [AirbyteWorkspacePartial.model_validate(w) for w in workspaces_data]
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.info("Workspace %s not found", self.workspace_id)
                return []
            else:
                logger.error("Failed to get workspace: HTTP %s", e.response.status_code)
                raise
        except requests.RequestException as e:
            logger.error("Network error: %s", str(e))
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
