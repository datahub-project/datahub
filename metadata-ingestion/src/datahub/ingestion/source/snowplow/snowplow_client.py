"""
Snowplow BDP Console API client.

Handles authentication, API requests, retry logic, and pagination for the
Snowplow BDP (Behavioral Data Platform) Console API.

API Documentation: https://console.snowplowanalytics.com/api/msc/v1/docs/
"""

import json
import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport

from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataModel,
    DataStructure,
    DataStructureDeployment,
    Destination,
    Enrichment,
    EventSpecification,
    EventSpecificationsResponse,
    Organization,
    Pipeline,
    PipelinesResponse,
    TokenResponse,
    TrackingPlan,
    TrackingPlansResponse,
    User,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
)

logger = logging.getLogger(__name__)


class ResourceNotFoundError(Exception):
    """Raised when API resource returns 404 Not Found."""

    pass


class SnowplowBDPClient:
    """
    API client for Snowplow BDP Console.

    Handles:
    - v3 Authentication (API Key → JWT token)
    - Data structures (schemas) extraction
    - Event specifications extraction
    - Tracking scenarios extraction
    - Automatic token refresh on expiry
    - Retry logic with exponential backoff
    - Error handling
    """

    def __init__(
        self,
        config: SnowplowBDPConnectionConfig,
        report: Optional["SnowplowSourceReport"] = None,
    ):
        """
        Initialize BDP Console API client.

        Args:
            config: BDP connection configuration
            report: Optional report for tracking API metrics
        """
        self.config = config
        self.base_url = config.console_api_url
        self.organization_id = config.organization_id
        self.report = report  # For API metrics tracking

        # JWT token (obtained via authentication)
        self._jwt_token: Optional[str] = None

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,  # Exponential backoff: 1, 2, 4 seconds
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT"],  # Retry safe methods
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Authenticate immediately
        self._authenticate()

    def close(self) -> None:
        """Close the HTTP session and release resources."""
        if hasattr(self, "session") and self.session:
            self.session.close()

    def __enter__(self) -> "SnowplowBDPClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.close()

    def _authenticate(self) -> None:
        """
        Authenticate with BDP Console API using v3 flow.

        Exchanges API credentials for JWT token. Credentials (api_key_id, api_key) are
        sent as headers, and the server returns a short-lived JWT for subsequent requests.

        API Reference: https://docs.snowplow.io/docs/using-the-snowplow-console/managing-console-api-authentication/

        The token endpoint uses GET (not POST). This is Snowplow's API convention,
        confirmed by snowplow-cli source code. Credentials are passed via
        X-API-Key-ID and X-API-Key headers rather than a request body.
        """
        # GET /organizations/{organizationId}/credentials/v3/token
        url = (
            f"{self.base_url}/organizations/{self.organization_id}/credentials/v3/token"
        )

        headers = {
            "X-API-Key-ID": self.config.api_key_id,
            "X-API-Key": self.config.api_key.get_secret_value(),
        }

        try:
            logger.info(f"Authenticating with Snowplow BDP Console API: {url}")
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()

            # Parse response using Pydantic model
            response_data = response.json()
            try:
                token_response = TokenResponse.model_validate(response_data)
                self._jwt_token = token_response.access_token

                # Set JWT in session headers for future requests
                self.session.headers.update(
                    {"Authorization": f"Bearer {self._jwt_token}"}
                )

                logger.info("Successfully authenticated with Snowplow BDP Console API")
            except ValidationError as parse_error:
                logger.error(f"Failed to parse authentication response: {parse_error}")
                logger.debug(
                    f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
                )
                raise ValueError(
                    f"Invalid authentication response format: {parse_error}"
                ) from parse_error

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise ValueError(
                    f"Authentication failed: Invalid API credentials. "
                    f"Check api_key_id and api_key. Status: {e.response.status_code}"
                ) from e
            elif e.response.status_code == 403:
                raise ValueError(
                    f"Authentication failed: Forbidden. Check organization_id and API key permissions. "
                    f"Status: {e.response.status_code}"
                ) from e
            else:
                raise ValueError(
                    f"Authentication failed with status {e.response.status_code}: {e}"
                ) from e
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Authentication failed due to network error: {e}") from e
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Authentication failed: invalid JSON response: {e}"
            ) from e

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        retry_auth: bool = True,
    ) -> dict:
        """
        Make authenticated API request with error handling and timing metrics.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (without base URL)
            params: Query parameters
            json_data: JSON request body
            retry_auth: Whether to retry with re-authentication on 401

        Returns:
            Response JSON as dictionary

        Raises:
            requests.exceptions.HTTPError: For HTTP errors
            ValueError: For authentication errors
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        endpoint_name = self._extract_endpoint_name(endpoint)
        start_time = time.perf_counter()
        is_error = False

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and retry_auth:
                # JWT token expired, re-authenticate and retry
                logger.warning("JWT token expired, re-authenticating...")
                self._authenticate()
                return self._request(
                    method, endpoint, params, json_data, retry_auth=False
                )
            elif e.response.status_code == 403:
                is_error = True
                logger.error(f"Permission denied: {url}")
                raise PermissionError(
                    f"Permission denied for {url}. Check API key permissions."
                ) from e
            elif e.response.status_code == 404:
                # 404 is not always an error (e.g., checking if resource exists)
                logger.debug(f"Resource not found: {url}")
                raise ResourceNotFoundError(f"Resource not found: {url}") from e
            else:
                is_error = True
                logger.error(f"HTTP error {e.response.status_code}: {url}")
                raise
        except requests.exceptions.Timeout:
            is_error = True
            logger.error(f"Request timeout: {url}")
            raise
        except requests.exceptions.RequestException as e:
            is_error = True
            logger.error(f"Request failed: {url}: {e}")
            raise
        finally:
            # Record API metrics
            latency_ms = (time.perf_counter() - start_time) * 1000
            self._record_api_call(endpoint_name, latency_ms, is_error)

    def _extract_endpoint_name(self, endpoint: str) -> str:
        """
        Extract a normalized endpoint name for metrics grouping.

        Converts paths like 'organizations/{id}/data-structures' to 'data-structures'.
        This groups API calls by logical endpoint rather than full path.
        """
        parts = endpoint.strip("/").split("/")
        # Skip organization ID parts and return the meaningful endpoint name
        meaningful_parts = [
            p for p in parts if p not in ("organizations", self.organization_id, "v1")
        ]
        if meaningful_parts:
            # Return last meaningful part (e.g., 'data-structures', 'event-specs')
            return meaningful_parts[-1]
        return "unknown"

    def _record_api_call(
        self, endpoint: str, latency_ms: float, is_error: bool
    ) -> None:
        """Record API call metrics if report is available."""
        if self.report is not None:
            self.report.record_api_call("bdp", endpoint, latency_ms, is_error)

    # ============================================
    # Organization API
    # ============================================

    def get_organization(self) -> Optional[Organization]:
        """
        Get organization details including warehouse destination configuration.

        Returns:
            Organization details with warehouse source information, or None if error
        """
        endpoint = f"organizations/{self.organization_id}"

        logger.debug("Fetching organization details from Snowplow")

        try:
            response_data = self._request("GET", endpoint)

            if not response_data:
                logger.warning("Empty response from organization endpoint")
                return None

            try:
                organization = Organization.model_validate(response_data)
                logger.info(f"Found organization: {organization.name}")

                if organization.source:
                    logger.info(
                        f"Organization has warehouse destination: {organization.source.name}"
                    )

                return organization
            except ValidationError as parse_error:
                error_msg = f"Failed to parse organization response: {parse_error}"
                logger.error(error_msg)
                if self.report:
                    self.report.report_warning("organization_parsing", error_msg)
                logger.debug(
                    f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
                )
                return None

        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch organization details: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("organization_fetch", error_msg)
            return None

    # ============================================
    # Data Structures API
    # ============================================

    def get_data_structures(
        self,
        vendor: Optional[str] = None,
        name: Optional[str] = None,
        page_size: int = 100,
    ) -> List[DataStructure]:
        """
        Get all data structures (schemas) from organization with pagination support.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/

        Args:
            vendor: Optional filter by schema vendor
            name: Optional filter by schema name
            page_size: Number of results per page (default: 100)

        Returns:
            List of data structures
        """
        # GET /organizations/{organizationId}/data-structures/v1
        # Supports pagination via 'from' and 'size' parameters
        endpoint = f"organizations/{self.organization_id}/data-structures/v1"

        params: Dict[str, str] = {}
        if vendor:
            params["vendor"] = vendor
        if name:
            params["name"] = name

        logger.debug(
            f"Fetching data structures from Snowplow (vendor={vendor}, name={name})"
        )

        all_structures: List[DataStructure] = []
        offset = 0

        while True:
            # Add pagination parameters
            params["from"] = str(offset)
            params["size"] = str(page_size)

            logger.debug(f"Fetching page: offset={offset}, size={page_size}")

            try:
                response_data = self._request("GET", endpoint, params=params)
            except (
                requests.exceptions.RequestException,
                ResourceNotFoundError,
            ) as e:
                error_msg = (
                    f"Failed to fetch data structures page at offset {offset}: {e}"
                )
                logger.warning(error_msg)
                if self.report:
                    self.report.report_warning("data_structures_pagination", error_msg)
                break  # Return partial results instead of losing everything

            # BDP API returns direct array (per Swagger spec)
            if not isinstance(response_data, list):
                error_msg = f"Expected list from BDP API, got {type(response_data)}"
                logger.error(error_msg)
                if self.report:
                    self.report.report_warning("data_structures_format", error_msg)
                break

            # No more results
            if not response_data:
                break

            try:
                page_structures = [
                    DataStructure.model_validate(ds) for ds in response_data
                ]
                all_structures.extend(page_structures)

                # If we got fewer results than page_size, we've reached the end
                if len(page_structures) < page_size:
                    break

                offset += len(page_structures)

            except ValidationError as e:
                error_msg = (
                    f"Failed to parse data structures page at offset {offset}: {e}"
                )
                logger.error(error_msg)
                logger.debug(
                    f"Raw response data (first item): {json.dumps(response_data[0] if response_data else 'empty', indent=2, default=str)}"
                )
                if self.report:
                    self.report.report_warning("data_structures_parsing", error_msg)
                break

        logger.info(f"Found {len(all_structures)} data structures total")
        return all_structures

    def get_data_structure(self, data_structure_hash: str) -> Optional[DataStructure]:
        """
        Get specific data structure by hash.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/

        Args:
            data_structure_hash: Data structure hash identifier

        Returns:
            Data structure or None if not found
        """
        # GET /organizations/{organizationId}/data-structures/v1/{dataStructureHash}
        # Docs: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/
        endpoint = f"organizations/{self.organization_id}/data-structures/v1/{data_structure_hash}"

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch data structure {data_structure_hash}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("data_structure_fetch", error_msg)
            return None

        if not response_data:
            return None

        try:
            # Real BDP API returns data structure directly, not wrapped
            return DataStructure.model_validate(response_data)
        except ValidationError as e:
            error_msg = f"Failed to parse data structure {data_structure_hash}: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("data_structure_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    def get_data_structure_version(
        self, data_structure_hash: str, version: str, env: Optional[str] = None
    ) -> Optional[DataStructure]:
        """
        Get specific version of a data structure with full schema definition.

        This endpoint returns the actual JSON Schema definition with properties/fields,
        unlike the list/get endpoints which only return metadata.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/

        Args:
            data_structure_hash: Data structure hash identifier
            version: Schema version (e.g., "1-0-0")
            env: Optional environment filter (e.g., "PROD", "DEV")

        Returns:
            Data structure with full schema definition, or None if not found
        """
        # GET /organizations/{organizationId}/data-structures/v1/{schemaHash}/versions/{version}
        # This returns SelfDescribingSchema_Json with the actual schema definition
        endpoint = f"organizations/{self.organization_id}/data-structures/v1/{data_structure_hash}/versions/{version}"

        params = {}
        if env:
            params["env"] = env

        logger.debug(
            f"Fetching schema definition for {data_structure_hash} version {version}"
        )

        try:
            response_data = self._request("GET", endpoint, params=params)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = (
                f"Failed to fetch schema version {data_structure_hash}/{version}: {e}"
            )
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("schema_version_fetch", error_msg)
            return None

        if not response_data:
            return None

        try:
            # Response format: The JSON Schema properties are at root level, with 'self' merged in
            # Example: {"self": {...}, "properties": {...}, "type": "object", "required": [...]}
            # Not nested like {"self": {...}, "schema": {...}}

            # Extract self-descriptor info
            self_desc = response_data.get("self", {})

            if not self_desc:
                logger.warning(
                    f"Invalid schema response for {data_structure_hash}/{version}: missing 'self' field"
                )
                return None

            # Schema definition is everything except the 'self' field
            schema_def = {k: v for k, v in response_data.items() if k != "self"}

            if not schema_def:
                logger.warning(
                    f"Invalid schema response for {data_structure_hash}/{version}: no schema properties"
                )
                return None

            # Build DataStructure with the schema definition
            data_structure_dict = {
                "hash": data_structure_hash,
                "vendor": self_desc.get("vendor"),
                "name": self_desc.get("name"),
                "data": {
                    "self": self_desc,
                    **schema_def,  # Merge schema properties
                },
            }

            return DataStructure.model_validate(data_structure_dict)
        except ValidationError as e:
            error_msg = (
                f"Failed to parse schema version {data_structure_hash}/{version}: {e}"
            )
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("schema_version_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    def get_data_structure_deployments(
        self, data_structure_hash: str
    ) -> List[DataStructureDeployment]:
        """
        Get deployment information for data structure.

        Returns ALL historical deployments, not just current deployments per environment.
        Uses pagination to retrieve complete deployment history.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/

        Args:
            data_structure_hash: Data structure hash identifier

        Returns:
            List of all deployments (historical and current)
        """
        # GET /organizations/{organizationId}/data-structures/v1/{dataStructureHash}/deployments
        # IMPORTANT: Must use pagination params (from, size) to get full deployment history
        # Without pagination, only returns CURRENT deployment per environment
        endpoint = f"organizations/{self.organization_id}/data-structures/v1/{data_structure_hash}/deployments"

        # Use large page size to get all deployments in one call
        # Typical schemas have <100 deployments, 1000 is more than sufficient
        params = {"from": 0, "size": 1000}

        try:
            response_data = self._request("GET", endpoint, params=params)
        except ResourceNotFoundError:
            logger.info(
                f"Deployments not found for data structure {data_structure_hash} (404)"
            )
            return []
        except (requests.exceptions.RequestException, PermissionError) as e:
            error_msg = f"Failed to fetch deployments for {data_structure_hash}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("deployment_fetch", error_msg)
            return []

        if not response_data:
            return []

        try:
            # With pagination params, response is a direct array
            # Without pagination, response is {"data": [...]}
            if isinstance(response_data, list):
                deployments_list = response_data
            else:
                deployments_list = response_data.get("data", [])

            return [DataStructureDeployment.model_validate(d) for d in deployments_list]
        except ValidationError as e:
            error_msg = f"Failed to parse deployments for {data_structure_hash}: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("deployment_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    # ============================================
    # Event Specifications API
    # ============================================

    def get_event_specifications(self) -> List[EventSpecification]:
        """
        Get all event specifications from organization.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/

        Returns:
            List of event specifications
        """
        # GET /organizations/{organizationId}/event-specs/v1
        # Docs: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/
        endpoint = f"organizations/{self.organization_id}/event-specs/v1"

        logger.debug("Fetching event specifications from Snowplow")

        try:
            response_data = self._request("GET", endpoint)
        except ResourceNotFoundError:
            # Endpoint may not be available for some organizations
            logger.info(
                "Event specifications endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Handle empty response
        if not response_data:
            logger.debug("Event specifications endpoint returned empty response")
            return []

        # Event specifications API uses wrapped format (unlike data structures API)
        # Response structure: {"data": [...], "includes": [...], "errors": [...]}
        if not isinstance(response_data, dict):
            error_msg = f"Expected dict (wrapped response) from event specs API, got {type(response_data)}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("event_specs_format", error_msg)
            return []

        try:
            response = EventSpecificationsResponse.model_validate(response_data)
            logger.info(f"Found {len(response.data)} event specifications")

            # Log any errors/warnings from API
            if response.errors:
                logger.warning(
                    f"API returned {len(response.errors)} errors/warnings for event specifications"
                )

            return response.data
        except ValidationError as e:
            error_msg = f"Failed to parse event specifications: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("event_specs_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    def get_event_specification(
        self, event_spec_id: str
    ) -> Optional[EventSpecification]:
        """
        Get specific event specification by ID.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/

        Args:
            event_spec_id: Event specification ID

        Returns:
            Event specification or None if not found
        """
        # GET /organizations/{organizationId}/event-specs/v1/{eventSpecId}
        # Docs: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/
        endpoint = (
            f"organizations/{self.organization_id}/event-specs/v1/{event_spec_id}"
        )

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch event specification {event_spec_id}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("event_spec_fetch", error_msg)
            return None

        if not response_data:
            return None

        try:
            return EventSpecification.model_validate(response_data.get("data", {}))
        except ValidationError as e:
            error_msg = f"Failed to parse event specification {event_spec_id}: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("event_spec_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    # ============================================
    # Tracking Plans API
    # ============================================

    def get_tracking_plans(self) -> List[TrackingPlan]:
        """
        Get all tracking plans from organization.

        Uses the /data-products/v2 API which provides richer metadata than the
        legacy /tracking-scenarios/v1 endpoint. Snowplow originally called
        tracking plans "data products" — the v2 API still uses that path.

        Returns:
            List of tracking plans
        """
        # GET /organizations/{organizationId}/data-products/v2
        # Note: API path uses legacy 'data-products' name (Snowplow renamed to tracking plans)
        endpoint = f"organizations/{self.organization_id}/data-products/v2"

        logger.debug("Fetching tracking plans from Snowplow")

        try:
            response_data = self._request("GET", endpoint)
        except ResourceNotFoundError:
            logger.info(
                "Tracking plans endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Handle empty response
        if not response_data:
            logger.debug("Tracking plans endpoint returned empty response")
            return []

        # Tracking plans API uses wrapped format
        # Response structure: {"data": [...], "includes": {...}, "errors": [...]}
        if not isinstance(response_data, dict):
            error_msg = f"Expected dict (wrapped response) from tracking plans API, got {type(response_data)}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("tracking_plans_format", error_msg)
            return []

        try:
            response = TrackingPlansResponse.model_validate(response_data)
            logger.info(f"Found {len(response.data)} tracking plans")

            # Log any errors/warnings from API
            if response.errors:
                logger.warning(
                    f"API returned {len(response.errors)} errors/warnings for tracking plans"
                )

            return response.data
        except ValidationError as e:
            error_msg = f"Failed to parse tracking plans: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("tracking_plans_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    def get_tracking_plan(self, plan_id: str) -> Optional[TrackingPlan]:
        """
        Get specific tracking plan by ID.

        Args:
            plan_id: Tracking plan ID

        Returns:
            Tracking plan or None if not found
        """
        # GET /organizations/{organizationId}/data-products/v2/{id}
        # Note: API path uses legacy 'data-products' name
        endpoint = f"organizations/{self.organization_id}/data-products/v2/{plan_id}"

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch tracking plan {plan_id}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("tracking_plan_fetch", error_msg)
            return None

        if not response_data:
            return None

        try:
            return TrackingPlan.model_validate(response_data.get("data", {}))
        except ValidationError as e:
            error_msg = f"Failed to parse tracking plan {plan_id}: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("tracking_plan_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    def get_data_models(self, tracking_plan_id: str) -> List[DataModel]:
        """
        Get data models for a tracking plan.

        Data models define transformations and outputs to warehouse destinations.
        Each model references a destination (warehouse) and specifies the table name.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/data-models/api/

        Args:
            tracking_plan_id: Tracking plan UUID

        Returns:
            List of data models for the tracking plan
        """
        # GET /organizations/{organizationId}/data-products/v2/{id}/data-models/v1
        # Note: API path uses legacy 'data-products' name
        endpoint = f"organizations/{self.organization_id}/data-products/v2/{tracking_plan_id}/data-models/v1"

        logger.debug(f"Fetching data models for tracking plan {tracking_plan_id}")

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = (
                f"Failed to fetch data models for tracking plan {tracking_plan_id}: {e}"
            )
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("data_models_fetch", error_msg)
            return []

        # Handle 404 or empty response
        if not response_data:
            logger.debug(
                f"No data models found for tracking plan {tracking_plan_id} (empty response)"
            )
            return []

        # Response is an array of data models
        if not isinstance(response_data, list):
            logger.warning(
                f"Expected list from data models API, got {type(response_data)}"
            )
            return []

        # Parse data models
        data_models = []
        for model_data in response_data:
            try:
                data_model = DataModel.model_validate(model_data)
                data_models.append(data_model)
            except ValidationError as e:
                logger.warning(f"Failed to parse data model: {e}")
                logger.debug(
                    f"Raw model data: {json.dumps(model_data, indent=2, default=str)}"
                )
                continue

        logger.debug(
            f"Found {len(data_models)} data models for tracking plan {tracking_plan_id}"
        )

        return data_models

    # ============================================
    # Pipeline Methods
    # ============================================

    def get_pipelines(self) -> List[Pipeline]:
        """
        Get all pipelines from organization.

        API Reference: Discovered via testing - not yet officially documented

        Returns:
            List of pipelines
        """
        # GET /organizations/{organizationId}/pipelines/v1
        endpoint = f"organizations/{self.organization_id}/pipelines/v1"

        logger.debug("Fetching pipelines from Snowplow")

        try:
            response_data = self._request("GET", endpoint)
        except ResourceNotFoundError:
            # Endpoint may not be available for some organizations
            logger.info(
                "Pipelines endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Handle empty response
        if not response_data:
            logger.debug("Pipelines endpoint returned empty response")
            return []

        try:
            response = PipelinesResponse.model_validate(response_data)
            logger.info(f"Found {len(response.pipelines)} pipelines")

            return response.pipelines
        except ValidationError as e:
            error_msg = f"Failed to parse pipelines: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("pipelines_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """
        Get specific pipeline by ID.

        Args:
            pipeline_id: Pipeline ID

        Returns:
            Pipeline or None if not found
        """
        # GET /organizations/{organizationId}/pipelines/v1/{pipelineId}
        endpoint = f"organizations/{self.organization_id}/pipelines/v1/{pipeline_id}"

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch pipeline {pipeline_id}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("pipeline_fetch", error_msg)
            return None

        if not response_data:
            return None

        try:
            return Pipeline.model_validate(response_data)
        except ValidationError as e:
            error_msg = f"Failed to parse pipeline {pipeline_id}: {e}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("pipeline_parsing", error_msg)
            logger.debug(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    def get_enrichments(self, pipeline_id: str) -> List[Enrichment]:
        """
        Get all enrichments for a pipeline.

        API Reference: Discovered via testing - resources/v1 path

        Args:
            pipeline_id: Pipeline ID

        Returns:
            List of enrichments
        """
        # GET /organizations/{organizationId}/resources/v1/pipelines/{pipelineId}/configuration/enrichments
        endpoint = f"organizations/{self.organization_id}/resources/v1/pipelines/{pipeline_id}/configuration/enrichments"

        logger.debug(f"Fetching enrichments for pipeline {pipeline_id}")

        try:
            response_data = self._request("GET", endpoint)
        except ResourceNotFoundError:
            # Endpoint may not be available for some organizations
            logger.info(
                f"Enrichments endpoint not available for pipeline {pipeline_id} (404)"
            )
            return []

        # Handle empty response
        if not response_data:
            logger.debug(f"No enrichments found for pipeline {pipeline_id}")
            return []

        # Parse enrichments individually so one bad item doesn't lose all
        enrichments: List[Enrichment] = []
        for enrichment_data in response_data:
            try:
                enrichments.append(Enrichment.model_validate(enrichment_data))
            except ValidationError as e:
                error_msg = f"Failed to parse enrichment in pipeline {pipeline_id}: {e}"
                logger.warning(error_msg)
                if self.report:
                    self.report.report_warning("enrichment_parsing", error_msg)
                continue

        logger.info(f"Found {len(enrichments)} enrichments")
        return enrichments

    def get_destinations(self) -> List[Destination]:
        """
        Get all warehouse destinations (loaders) from organization.

        API Reference: /destinations/v3 endpoint (discovered via UI inspection)

        Destinations define how enriched events are loaded into warehouses,
        including database, schema, and table configuration.

        Returns:
            List of destinations
        """
        # GET /organizations/{organizationId}/destinations/v3
        endpoint = f"organizations/{self.organization_id}/destinations/v3"

        logger.debug("Fetching destinations from Snowplow")

        try:
            response_data = self._request("GET", endpoint)
        except ResourceNotFoundError:
            # Endpoint may not be available for some organizations
            logger.info(
                "Destinations endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Handle empty response
        if not response_data:
            logger.debug("Destinations endpoint returned empty response")
            return []

        # Response is a direct array of destinations
        if not isinstance(response_data, list):
            error_msg = (
                f"Expected list from destinations API, got {type(response_data)}"
            )
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("destinations_format", error_msg)
            return []

        # Parse destinations individually so one bad item doesn't lose all
        destinations: List[Destination] = []
        for dest_data in response_data:
            try:
                destinations.append(Destination.model_validate(dest_data))
            except ValidationError as e:
                error_msg = f"Failed to parse destination: {e}"
                logger.warning(error_msg)
                if self.report:
                    self.report.report_warning("destination_parsing", error_msg)
                continue

        logger.info(f"Found {len(destinations)} destinations")
        return destinations

    # ============================================
    # Helper Methods
    # ============================================

    def test_connection(self) -> bool:
        """
        Test API connectivity and authentication.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to fetch data structures as connectivity test
            self.get_data_structures()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_users(self) -> List[User]:
        """
        Get all users in the organization.

        API Reference: https://console.snowplowanalytics.com/api/msc/v1/docs/#/Users/getOrganizationsOrganizationidUsers

        This is used to map initiatorId from deployments to user email addresses
        for ownership extraction.

        Returns:
            List of users with id, email, and name fields
        """
        # GET /organizations/{organizationId}/users
        endpoint = f"organizations/{self.organization_id}/users"

        logger.debug("Fetching users from Snowplow BDP")

        try:
            response_data = self._request("GET", endpoint)
        except (requests.exceptions.RequestException, ResourceNotFoundError) as e:
            error_msg = f"Failed to fetch users: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_warning("users_fetch", error_msg)
            return []

        # BDP API returns direct array (per Swagger spec)
        if not isinstance(response_data, list):
            error_msg = f"Expected list from users API, got {type(response_data)}"
            logger.error(error_msg)
            if self.report:
                self.report.report_warning("users_format", error_msg)
            return []

        # Parse users individually so one bad item doesn't lose all
        users: List[User] = []
        for user_data in response_data:
            try:
                users.append(User.model_validate(user_data))
            except ValidationError as e:
                error_msg = f"Failed to parse user: {e}"
                logger.warning(error_msg)
                if self.report:
                    self.report.report_warning("user_parsing", error_msg)
                continue

        logger.info(f"Found {len(users)} users")
        return users
