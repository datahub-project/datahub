"""Base REST client for Microsoft Fabric workloads."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Iterator, Mapping, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.constants import (
    ITEM_INCLUDE_DEFAULT_IDENTITY,
)
from datahub.ingestion.source.fabric.common.models import (
    FabricItemAnnotations,
    FabricJobInstance,
    item_has_principal,
    item_owner_user_principal_name,
    sensitivity_label_id,
)
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
        include_owners: bool = True,
        include_sensitivity_labels: bool = False,
    ):
        """Initialize the base client.

        Args:
            auth_helper: Authentication helper for getting Bearer tokens
            timeout: Request timeout in seconds
            report: Optional client report for tracking metrics
            include_owners: When true, look up each item's default identity
            include_sensitivity_labels: When true, read each item's sensitivity label id
        """
        self.auth_helper = auth_helper
        self.timeout = timeout
        self.include_owners = include_owners
        self.include_sensitivity_labels = include_sensitivity_labels
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

    def _paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Iterator[dict]:
        """Yield all items from a paginated Fabric API endpoint.

        Handles continuation token pagination transparently — callers
        just iterate and get all items across all pages.

        Args:
            endpoint: API endpoint (relative to base URL)
            params: Initial query parameters

        Yields:
            Item dictionaries from each page's "value" array
        """
        request_params: Dict[str, str] = dict(params or {})
        page = 1
        while True:
            response = self.get(endpoint, params=request_params)
            data = response.json()
            items = data.get("value", [])
            logger.debug(f"Page {page}: got {len(items)} item(s) from {endpoint}")
            yield from items

            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
            request_params["continuationToken"] = continuation_token
            page += 1

    def _paginate_post(
        self,
        endpoint: str,
        body: dict,
    ) -> Iterator[dict]:
        """Yield all items from a paginated Fabric POST API endpoint.

        Some Fabric APIs (e.g. queryActivityRuns) use POST with a
        continuationToken in both the request and response body.

        Args:
            endpoint: API endpoint (relative to base URL)
            body: JSON request body (will be mutated to add continuationToken)

        Yields:
            Item dictionaries from each page's "value" array
        """
        page = 1
        while True:
            response = self.post(endpoint, json=body)
            data = response.json()

            items = data if isinstance(data, list) else data.get("value", [])
            logger.debug(f"Page {page}: got {len(items)} item(s) from {endpoint}")
            yield from items

            continuation_token = (
                data.get("continuationToken") if isinstance(data, dict) else None
            )
            if not continuation_token:
                break
            body["continuationToken"] = continuation_token
            page += 1

    def _list_workspaces_raw(self) -> Iterator[dict]:
        """List all accessible Fabric workspaces (raw data).

        This method is common to all Fabric workloads.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/workspaces/list

        Yields:
            Workspace data dictionaries
        """
        logger.info("Listing Fabric workspaces")
        yield from self._paginate("workspaces")

    def get_item(self, workspace_id: str, item_id: str) -> dict:
        """Get one workspace item, including its default identity.

        GET /workspaces/{workspaceId}/items/{itemId}?include=DefaultIdentity

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item
        """
        response = self.get(
            f"workspaces/{workspace_id}/items/{item_id}",
            params={"include": ITEM_INCLUDE_DEFAULT_IDENTITY},
        )
        data = response.json()
        if not isinstance(data, dict):
            return {}
        return data

    def _consume_item_payload(
        self,
        payload: Mapping[str, object],
        annotations: FabricItemAnnotations,
        *,
        include_labels: bool,
        owner_blocked: list[bool],
    ) -> None:
        if self.include_owners and not annotations.owner_user_principal_name:
            if not owner_blocked[0]:
                annotations.owner_user_principal_name = item_owner_user_principal_name(
                    payload
                )
                if not annotations.owner_user_principal_name and item_has_principal(
                    payload
                ):
                    # An identity is present but it is not a user. Do not
                    # replace it with the item creator.
                    owner_blocked[0] = True
        if include_labels and not annotations.sensitivity_label_id:
            annotations.sensitivity_label_id = sensitivity_label_id(payload)

    def get_item_annotations(
        self,
        workspace_id: str,
        item_id: str,
        *,
        resource_path: Optional[str] = None,
        listed: Optional[Mapping[str, object]] = None,
    ) -> FabricItemAnnotations:
        """Read the item owner and sensitivity label id.

        The label GUID comes from GET /workspaces/{workspaceId}/items/{itemId}
        when a payload already fetched does not include it. Warehouse owners
        still try the warehouse GET, then the items endpoint, then
        ``creatorPrincipal`` on the admin item API. A missing value or a
        failed lookup does not fail item listing.
        """
        include_labels = getattr(self, "include_sensitivity_labels", False)
        annotations = FabricItemAnnotations()
        owner_blocked = [False]

        def consume(payload: object) -> None:
            if isinstance(payload, dict):
                self._consume_item_payload(
                    payload,
                    annotations,
                    include_labels=include_labels,
                    owner_blocked=owner_blocked,
                )

        if listed is not None:
            consume(listed)
        if not item_id:
            return annotations

        def need_owner() -> bool:
            return (
                self.include_owners
                and not annotations.owner_user_principal_name
                and not owner_blocked[0]
            )

        def need_label() -> bool:
            return include_labels and not annotations.sensitivity_label_id

        if resource_path and need_owner():
            try:
                response = self.get(resource_path)
                consume(response.json())
            except Exception as e:
                logger.debug(f"Owner lookup failed for {resource_path}: {e}")

        if need_owner() or need_label():
            # include=DefaultIdentity only while the owner is still missing.
            # A labels-only lookup is the plain items GET.
            params = (
                {"include": ITEM_INCLUDE_DEFAULT_IDENTITY} if need_owner() else None
            )
            try:
                response = self.get(
                    f"workspaces/{workspace_id}/items/{item_id}",
                    params=params,
                )
                consume(response.json())
            except Exception as e:
                logger.warning(
                    f"Failed to read item {item_id} in workspace {workspace_id}: {e}"
                )

        if need_owner():
            try:
                response = self.get(f"admin/workspaces/{workspace_id}/items/{item_id}")
                consume(response.json())
            except Exception as e:
                logger.warning(
                    f"Failed to read owner for item {item_id} in workspace "
                    f"{workspace_id} from the admin item API: {e}"
                )
        return annotations

    def get_item_owner_user_principal_name(
        self,
        workspace_id: str,
        item_id: str,
        *,
        resource_path: Optional[str] = None,
    ) -> Optional[str]:
        """Return the item owner's userPrincipalName, if the API provides one.

        A missing identity or a failed lookup does not fail item listing.
        ``resource_path`` is a type-specific GET (a warehouse) tried before
        the generic items endpoint. When the item has no identity at all, the
        admin item API supplies ``creatorPrincipal``. A service principal
        identity is left without an owner.
        """
        if not self.include_owners:
            return None
        return self.get_item_annotations(
            workspace_id,
            item_id,
            resource_path=resource_path,
        ).owner_user_principal_name

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

    def list_item_job_instances(
        self, workspace_id: str, item_id: str
    ) -> Iterator[FabricJobInstance]:
        """List job execution instances for a Fabric item.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances
        Limitations: Most items have a limit of 100 recently completed entities, and there is not limit for active entities.

        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID (pipeline, copy job, dataflow, notebook, etc.)

        Yields:
            FabricJobInstance objects (newest first)
        """
        endpoint = f"workspaces/{workspace_id}/items/{item_id}/jobs/instances"
        logger.debug(f"Listing job instances for item {item_id}")
        for data in self._paginate(endpoint):
            try:
                # failureReason is an ErrorResponse object; extract the message
                raw_failure = data.get("failureReason")
                failure_reason = raw_failure.get("message") if raw_failure else None

                yield FabricJobInstance(
                    id=data["id"],
                    item_id=item_id,
                    workspace_id=workspace_id,
                    status=data["status"],
                    start_time_utc=data.get("startTimeUtc"),
                    end_time_utc=data.get("endTimeUtc"),
                    invoke_type=data.get("invokeType"),
                    failure_reason=failure_reason,
                )
            except KeyError as e:
                self.report.report_parse_failure(
                    f"Skipping malformed job instance for item "
                    f"{item_id}: missing required field {e}"
                )

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
