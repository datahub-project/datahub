"""
Snowplow BDP Console API client.

Handles authentication, API requests, retry logic, and pagination for the
Snowplow BDP (Behavioral Data Platform) Console API.

API Documentation: https://console.snowplowanalytics.com/api/msc/v1/docs/
"""

import logging
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
)
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataModel,
    DataProduct,
    DataProductsResponse,
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
    TrackingScenario,
    TrackingScenariosResponse,
    User,
)

logger = logging.getLogger(__name__)


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

    def __init__(self, config: SnowplowBDPConnectionConfig):
        """
        Initialize BDP Console API client.

        Args:
            config: BDP connection configuration
        """
        self.config = config
        self.base_url = config.console_api_url
        self.organization_id = config.organization_id

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

    def _authenticate(self) -> None:
        """
        Authenticate with BDP Console API using v3 flow.

        Exchanges API credentials for JWT token.

        API Reference: https://docs.snowplow.io/docs/using-the-snowplow-console/managing-console-api-authentication/

        Note: The token endpoint uses GET method, not POST (as confirmed by snowplow-cli).
        """
        # GET /organizations/{organizationId}/credentials/v3/token
        # Docs: https://docs.snowplow.io/docs/using-the-snowplow-console/managing-console-api-authentication/
        # Note: Despite being a token endpoint, this uses GET, not POST
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
            token_response = TokenResponse.model_validate(response.json())
            self._jwt_token = token_response.access_token

            # Set JWT in session headers for future requests
            self.session.headers.update({"Authorization": f"Bearer {self._jwt_token}"})

            logger.info("Successfully authenticated with Snowplow BDP Console API")

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
        except Exception as e:
            raise ValueError(f"Authentication failed: {e}") from e

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        retry_auth: bool = True,
    ) -> dict:
        """
        Make authenticated API request with error handling.

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
                logger.error(f"Permission denied: {url}")
                raise PermissionError(
                    f"Permission denied for {url}. Check API key permissions."
                ) from e
            elif e.response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return {}  # Return empty dict for not found
            else:
                logger.error(f"HTTP error {e.response.status_code}: {url}")
                raise
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {url}: {e}")
            raise

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

        logger.info("Fetching organization details from Snowplow")

        try:
            response_data = self._request("GET", endpoint)

            if not response_data:
                logger.warning("Empty response from organization endpoint")
                return None

            organization = Organization.model_validate(response_data)
            logger.info(f"Found organization: {organization.name}")

            if organization.source:
                logger.info(
                    f"Organization has warehouse destination: {organization.source.name}"
                )

            return organization

        except Exception as e:
            logger.warning(f"Failed to fetch organization details: {e}")
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

        params = {}
        if vendor:
            params["vendor"] = vendor
        if name:
            params["name"] = name

        logger.info(
            f"Fetching data structures from Snowplow (vendor={vendor}, name={name})"
        )

        all_structures = []
        offset = 0

        while True:
            # Add pagination parameters
            params["from"] = offset
            params["size"] = page_size

            logger.debug(f"Fetching page: offset={offset}, size={page_size}")

            response_data = self._request("GET", endpoint, params=params)

            # BDP API returns direct array (per Swagger spec)
            if not isinstance(response_data, list):
                logger.error(f"Expected list from BDP API, got {type(response_data)}")
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

            except Exception as e:
                import json

                logger.error(f"Failed to parse data structures page: {e}")
                logger.error(
                    f"Raw response data (first item): {json.dumps(response_data[0] if response_data else 'empty', indent=2, default=str)}"
                )
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

        response_data = self._request("GET", endpoint)

        if not response_data:
            return None

        try:
            # Real BDP API returns data structure directly, not wrapped
            return DataStructure.model_validate(response_data)
        except Exception as e:
            import json

            logger.error(f"Failed to parse data structure {data_structure_hash}: {e}")
            logger.error(
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

        response_data = self._request("GET", endpoint, params=params)

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
        except Exception as e:
            import json

            logger.error(
                f"Failed to parse schema version {data_structure_hash}/{version}: {e}"
            )
            logger.error(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return None

    def get_data_structure_deployments(
        self, data_structure_hash: str
    ) -> List[DataStructureDeployment]:
        """
        Get deployment information for data structure.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/

        Args:
            data_structure_hash: Data structure hash identifier

        Returns:
            List of deployments
        """
        # GET /organizations/{organizationId}/data-structures/v1/{dataStructureHash}/deployments
        # Docs: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/
        endpoint = f"organizations/{self.organization_id}/data-structures/v1/{data_structure_hash}/deployments"

        response_data = self._request("GET", endpoint)

        if not response_data:
            return []

        try:
            deployments_list = response_data.get("data", [])
            return [DataStructureDeployment.model_validate(d) for d in deployments_list]
        except Exception as e:
            logger.error(f"Failed to parse deployments for {data_structure_hash}: {e}")
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

        logger.info("Fetching event specifications from Snowplow")

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Event specifications endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Event specifications API uses wrapped format (unlike data structures API)
        # Response structure: {"data": [...], "includes": [...], "errors": [...]}
        if not isinstance(response_data, dict):
            logger.error(
                f"Expected dict (wrapped response) from BDP API, got {type(response_data)}"
            )
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
        except Exception as e:
            import json

            logger.error(f"Failed to parse event specifications: {e}")
            logger.error(
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

        response_data = self._request("GET", endpoint)

        if not response_data:
            return None

        try:
            return EventSpecification.model_validate(response_data.get("data", {}))
        except Exception as e:
            logger.error(f"Failed to parse event specification {event_spec_id}: {e}")
            return None

    # ============================================
    # Tracking Scenarios API
    # ============================================

    def get_tracking_scenarios(self) -> List[TrackingScenario]:
        """
        Get all tracking scenarios from organization.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/

        Returns:
            List of tracking scenarios
        """
        # GET /organizations/{organizationId}/tracking-scenarios/v1
        # Docs: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/
        endpoint = f"organizations/{self.organization_id}/tracking-scenarios/v1"

        logger.info("Fetching tracking scenarios from Snowplow")

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Tracking scenarios endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Tracking scenarios API uses wrapped format (unlike data structures API)
        # Response structure: {"data": [...], "includes": [...], "errors": [...]}
        if not isinstance(response_data, dict):
            logger.error(
                f"Expected dict (wrapped response) from BDP API, got {type(response_data)}"
            )
            return []

        try:
            response = TrackingScenariosResponse.model_validate(response_data)
            logger.info(f"Found {len(response.data)} tracking scenarios")

            # Log any errors/warnings from API
            if response.errors:
                logger.warning(
                    f"API returned {len(response.errors)} errors/warnings for tracking scenarios"
                )

            return response.data
        except Exception as e:
            import json

            logger.error(f"Failed to parse tracking scenarios: {e}")
            logger.error(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    def get_tracking_scenario(self, scenario_id: str) -> Optional[TrackingScenario]:
        """
        Get specific tracking scenario by ID.

        API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/

        Args:
            scenario_id: Tracking scenario ID

        Returns:
            Tracking scenario or None if not found
        """
        # GET /organizations/{organizationId}/tracking-scenarios/v1/{scenarioId}
        # Docs: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/
        endpoint = (
            f"organizations/{self.organization_id}/tracking-scenarios/v1/{scenario_id}"
        )

        response_data = self._request("GET", endpoint)

        if not response_data:
            return None

        try:
            return TrackingScenario.model_validate(response_data.get("data", {}))
        except Exception as e:
            logger.error(f"Failed to parse tracking scenario {scenario_id}: {e}")
            return None

    # ============================================
    # Data Products API
    # ============================================

    def get_data_products(self) -> List[DataProduct]:
        """
        Get all data products from BDP Console.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/data-products/api/

        Data products are groupings of event specifications with ownership, domain,
        and access information. They help organize tracking design at a higher level.

        Returns:
            List of data products
        """
        # GET /organizations/{organizationId}/data-products/v2
        # Docs: https://docs.snowplow.io/docs/data-product-studio/data-products/api/
        # Note: Uses v2 API endpoint (newer version)
        endpoint = f"organizations/{self.organization_id}/data-products/v2"

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Data products endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Data products API uses wrapped format (similar to event specs)
        # Response structure: {"data": [...], "includes": [...], "errors": [...]}
        if not isinstance(response_data, dict):
            logger.error(
                f"Expected dict (wrapped response) from BDP API, got {type(response_data)}"
            )
            return []

        try:
            response = DataProductsResponse.model_validate(response_data)
            logger.info(f"Found {len(response.data)} data products")

            # Log any errors/warnings from API
            if response.errors:
                logger.warning(
                    f"API returned {len(response.errors)} errors/warnings for data products"
                )

            return response.data
        except Exception as e:
            import json

            logger.error(f"Failed to parse data products: {e}")
            logger.error(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

    def get_data_product(self, product_id: str) -> Optional[DataProduct]:
        """
        Get specific data product by ID.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/data-products/api/

        Args:
            product_id: Data product ID

        Returns:
            Data product or None if not found
        """
        # GET /organizations/{organizationId}/data-products/v2/{dataProductId}
        endpoint = f"organizations/{self.organization_id}/data-products/v2/{product_id}"

        response_data = self._request("GET", endpoint)

        if not response_data:
            return None

        try:
            return DataProduct.model_validate(response_data.get("data", {}))
        except Exception as e:
            logger.error(f"Failed to parse data product {product_id}: {e}")
            return None

    def get_data_models(self, data_product_id: str) -> List[DataModel]:
        """
        Get data models for a data product.

        Data models define transformations and outputs to warehouse destinations.
        Each model references a destination (warehouse) and specifies the table name.

        API Reference: https://docs.snowplow.io/docs/data-product-studio/data-models/api/

        Args:
            data_product_id: Data product UUID

        Returns:
            List of data models for the data product
        """
        # GET /organizations/{organizationId}/data-products/v2/{dataProductId}/data-models/v1
        endpoint = f"organizations/{self.organization_id}/data-products/v2/{data_product_id}/data-models/v1"

        logger.debug(f"Fetching data models for data product {data_product_id}")

        response_data = self._request("GET", endpoint)

        # Handle 404 or empty response
        if not response_data:
            logger.debug(
                f"No data models found for data product {data_product_id} (empty response)"
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
            except Exception as e:
                logger.warning(f"Failed to parse data model: {e}")
                continue

        logger.debug(
            f"Found {len(data_models)} data models for data product {data_product_id}"
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

        logger.info("Fetching pipelines from Snowplow")

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Pipelines endpoint not available (404) - this is normal for some organizations"
            )
            return []

        try:
            response = PipelinesResponse.model_validate(response_data)
            logger.info(f"Found {len(response.pipelines)} pipelines")

            return response.pipelines
        except Exception as e:
            import json

            logger.error(f"Failed to parse pipelines: {e}")
            logger.error(
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

        response_data = self._request("GET", endpoint)

        if not response_data:
            return None

        try:
            return Pipeline.model_validate(response_data)
        except Exception as e:
            logger.error(f"Failed to parse pipeline {pipeline_id}: {e}")
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

        logger.info(f"Fetching enrichments for pipeline {pipeline_id}")

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Enrichments endpoint not available (404) - this is normal for some organizations"
            )
            return []

        try:
            # Response is a direct array of enrichments
            enrichments = [Enrichment.model_validate(e) for e in response_data]
            logger.info(f"Found {len(enrichments)} enrichments")

            return enrichments
        except Exception as e:
            import json

            logger.error(f"Failed to parse enrichments: {e}")
            logger.error(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

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

        logger.info("Fetching destinations from Snowplow")

        response_data = self._request("GET", endpoint)

        # Handle 404 (resource not found) - endpoint may not be available
        if not response_data or response_data == {}:
            logger.info(
                "Destinations endpoint not available (404) - this is normal for some organizations"
            )
            return []

        # Response is a direct array of destinations
        if not isinstance(response_data, list):
            logger.error(
                f"Expected list from destinations API, got {type(response_data)}"
            )
            return []

        try:
            destinations = [Destination.model_validate(d) for d in response_data]
            logger.info(f"Found {len(destinations)} destinations")

            return destinations
        except Exception as e:
            import json

            logger.error(f"Failed to parse destinations: {e}")
            logger.error(
                f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
            )
            return []

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

    def get_available_permissions(self) -> List[str]:
        """
        Get available API permissions for current credentials.

        Note: This endpoint may not be available on all BDP deployments.

        Returns:
            List of permission strings, or empty list if unavailable
        """
        try:
            # GET /organizations/{organizationId}/permissions (hypothetical endpoint)
            # Note: This endpoint may not exist in actual API
            endpoint = f"organizations/{self.organization_id}/permissions"
            response_data = self._request("GET", endpoint)
            return response_data.get("permissions", [])
        except Exception:
            # Permissions endpoint may not be available
            logger.debug("Permissions endpoint not available")
            return []

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

        logger.info("Fetching users from Snowplow BDP")

        try:
            response_data = self._request("GET", endpoint)

            # BDP API returns direct array (per Swagger spec)
            if not isinstance(response_data, list):
                logger.error(f"Expected list from BDP API, got {type(response_data)}")
                return []

            users = [User.model_validate(user) for user in response_data]
            logger.info(f"Found {len(users)} users")
            return users
        except Exception as e:
            import json

            logger.warning(f"Failed to fetch users: {e}")
            if isinstance(response_data, list) and response_data:
                logger.error(
                    f"Raw response data (first item): {json.dumps(response_data[0], indent=2, default=str)}"
                )
            return []
