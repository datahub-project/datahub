import logging
from typing import Any, Dict, Iterator, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
)
from datahub.ingestion.source.fivetran.response_models import (
    FivetranColumn,
    FivetranConnectionDetails,
    FivetranConnectionSchemas,
    FivetranDestinationDetails,
    FivetranGroup,
    FivetranListConnectionsResponse,
    FivetranListedConnection,
    FivetranListedUser,
    FivetranListGroupsResponse,
    FivetranListUsersResponse,
)

logger = logging.getLogger(__name__)


# Retry configuration constants
RETRY_MAX_TIMES = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_BACKOFF_FACTOR = 1
RETRY_ALLOWED_METHODS = ["GET"]


def _extract_data(payload: Dict[str, Any], *, context: str) -> Any:
    """Validate a Fivetran REST envelope and return its `data` field.

    Fivetran wraps every response as `{"code": "...", "message": "...", "data": ...}`.
    Direct `payload["data"]` access raises KeyError on a malformed-but-200
    response; using this helper turns that into a clear ValueError that
    surfaces in the ingest report.
    """
    code = payload.get("code")
    if code != "Success":
        raise ValueError(
            f"Fivetran API returned non-success code {code!r} for {context}: "
            f"{payload.get('message')}"
        )
    data = payload.get("data")
    if data is None:
        raise ValueError(f"Fivetran API response missing 'data' field for {context}")
    return data


class FivetranAPIClient:
    """Client for interacting with the Fivetran REST API."""

    def __init__(self, config: FivetranAPIConfig) -> None:
        self.config = config
        self._session = self._create_session()
        self._destination_cache: Dict[str, FivetranDestinationDetails] = {}

    def _create_session(self) -> requests.Session:
        """
        Create a session with retry logic and basic authentication
        """
        requests_session = requests.Session()

        # Configure retry strategy for transient failures
        retry_strategy = Retry(
            total=RETRY_MAX_TIMES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=RETRY_ALLOWED_METHODS,
            raise_on_status=True,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        requests_session.mount("http://", adapter)
        requests_session.mount("https://", adapter)

        # Set up basic authentication
        requests_session.auth = (
            self.config.api_key.get_secret_value(),
            self.config.api_secret.get_secret_value(),
        )
        requests_session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        return requests_session

    def get_connection_details_by_id(
        self, connection_id: str
    ) -> FivetranConnectionDetails:
        """
        Get details for a specific connection from the Fivetran API.

        Args:
            connection_id: The Fivetran connection ID to fetch details for.

        Returns:
            FivetranConnectionDetails: The parsed connection details.

        Raises:
            requests.HTTPError: If the API returns an HTTP error.
            ValueError: If the response is missing required fields or has non-success code.
            pydantic.ValidationError: If the response data doesn't match the expected schema.
        """
        response = self._session.get(
            f"{self.config.base_url}/v1/connections/{connection_id}",
            timeout=self.config.request_timeout_sec,
        )

        # Check for HTTP errors and raise HTTPError if needed
        response.raise_for_status()

        response_json = response.json()

        # Check response code at top level (e.g., "code": "Success")
        response_code = response_json.get("code")
        if response_code and response_code.lower() != "success":
            raise ValueError(
                f"Response code is not 'success' for connection_id {connection_id}. "
                f"Code: {response_code}, Response: {response_json}"
            )

        data = response_json.get("data", {})
        if not data:
            raise ValueError(
                f"Response missing 'data' field for connection_id {connection_id}"
            )

        # Use Pydantic's built-in parsing with extra="ignore" configured in the model
        # ValidationError will propagate if required fields are missing
        return FivetranConnectionDetails(**data)

    def get_destination_details_by_id(
        self, destination_id: str
    ) -> FivetranDestinationDetails:
        """Fetch destination metadata from `GET /v1/destinations/{id}`.

        Cached per instance so repeated lookups for the same destination during
        a single ingest issue at most one HTTP call.

        Raises:
            requests.HTTPError: transport-level failure (after retries).
            ValueError: API returned a non-Success code.
            pydantic.ValidationError: response shape doesn't match expectations.
        """
        cached = self._destination_cache.get(destination_id)
        if cached is not None:
            return cached

        response = self._session.get(
            f"{self.config.base_url}/v1/destinations/{destination_id}",
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()

        data = _extract_data(payload, context=f"destination {destination_id!r}")
        details = FivetranDestinationDetails.model_validate(data)
        self._destination_cache[destination_id] = details
        return details

    def list_groups(self, page_size: int = 500) -> Iterator[FivetranGroup]:
        """Yield every group visible to the API key, traversing pagination.

        Used by REST-mode log readers to enumerate destinations to scan.
        Validates the response shape via Pydantic so a malformed-but-Success
        payload raises `pydantic.ValidationError` instead of `KeyError`.
        """
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Union[str, int]] = {"limit": page_size}
            if cursor is not None:
                params["cursor"] = cursor
            resp = self._session.get(
                f"{self.config.base_url}/v1/groups",
                params=params,
                timeout=self.config.request_timeout_sec,
            )
            resp.raise_for_status()
            payload = resp.json()
            page = FivetranListGroupsResponse.model_validate(
                _extract_data(payload, context="list_groups")
            )
            yield from page.items
            cursor = page.next_cursor
            if cursor is None:
                return

    def list_connections(
        self, group_id: str, page_size: int = 500
    ) -> Iterator[FivetranListedConnection]:
        """Yield every connection in a Fivetran group, traversing pagination.

        Cursor-based pagination follows Fivetran's standard contract: `data.next_cursor`
        is the token for the next page; `None` ends iteration.
        """
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Union[str, int]] = {"limit": page_size}
            if cursor is not None:
                params["cursor"] = cursor
            resp = self._session.get(
                f"{self.config.base_url}/v1/groups/{group_id}/connections",
                params=params,
                timeout=self.config.request_timeout_sec,
            )
            resp.raise_for_status()
            payload = resp.json()
            page = FivetranListConnectionsResponse.model_validate(
                _extract_data(
                    payload, context=f"list_connections (group_id={group_id})"
                )
            )
            yield from page.items
            cursor = page.next_cursor
            if cursor is None:
                return

    def get_connection_schemas(self, connection_id: str) -> FivetranConnectionSchemas:
        resp = self._session.get(
            f"{self.config.base_url}/v1/connections/{connection_id}/schemas",
            timeout=self.config.request_timeout_sec,
        )
        resp.raise_for_status()
        payload = resp.json()
        return FivetranConnectionSchemas.model_validate(
            _extract_data(
                payload,
                context=f"get_connection_schemas (connection_id={connection_id})",
            )
        )

    def get_table_columns(
        self,
        connection_id: str,
        schema: str,
        table: str,
    ) -> Dict[str, FivetranColumn]:
        """Return the full column dict for a single table.

        The bulk `/v1/connections/{id}/schemas` endpoint only includes
        columns the user has explicitly modified (per Fivetran's OpenAPI
        spec for `TableConfigResponse.columns`). To get every column for
        a default-configured table, this per-table endpoint is required.

        Returns: `{<source_column_name>: FivetranColumn}`. Dict keys are
        the source-side names per the OpenAPI spec ("Each key is the
        column name as stored in the connection schema config").
        """
        resp = self._session.get(
            f"{self.config.base_url}/v1/connections/{connection_id}"
            f"/schemas/{schema}/tables/{table}/columns",
            timeout=self.config.request_timeout_sec,
        )
        resp.raise_for_status()
        payload = resp.json()
        data = _extract_data(
            payload,
            context=(
                f"get_table_columns (connection_id={connection_id}, "
                f"schema={schema}, table={table})"
            ),
        )
        columns_data = data.get("columns", {}) or {}
        return {
            name: FivetranColumn.model_validate(col_data)
            for name, col_data in columns_data.items()
        }

    def list_users(
        self, group_id: str, page_size: int = 500
    ) -> Iterator[FivetranListedUser]:
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Union[str, int]] = {"limit": page_size}
            if cursor is not None:
                params["cursor"] = cursor
            resp = self._session.get(
                f"{self.config.base_url}/v1/groups/{group_id}/users",
                params=params,
                timeout=self.config.request_timeout_sec,
            )
            resp.raise_for_status()
            payload = resp.json()
            page = FivetranListUsersResponse.model_validate(
                _extract_data(payload, context=f"list_users (group_id={group_id})")
            )
            yield from page.items
            cursor = page.next_cursor
            if cursor is None:
                return
