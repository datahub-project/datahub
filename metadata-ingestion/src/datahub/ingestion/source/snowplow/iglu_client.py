"""
Iglu Schema Registry API client.

Handles communication with Iglu schema registry for open-source Snowplow deployments
or as a fallback for BDP deployments.

API Documentation: https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/
"""

import json
import logging
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.snowplow.snowplow_config import IgluConnectionConfig
from datahub.ingestion.source.snowplow.snowplow_models import IgluSchema

logger = logging.getLogger(__name__)


class IgluClient:
    """
    API client for Iglu Schema Registry.

    Handles:
    - Schema retrieval by vendor/name/version
    - Optional authentication for private registries
    - Retry logic with exponential backoff
    - Error handling
    """

    def __init__(self, config: IgluConnectionConfig):
        """
        Initialize Iglu Schema Registry client.

        Args:
            config: Iglu connection configuration
        """
        self.config = config
        self.base_url = config.iglu_server_url

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # Fixed retry count for Iglu
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set authentication if provided
        if config.api_key:
            # Iglu uses UUID API key in query parameter or header
            self.session.params = {"apikey": config.api_key.get_secret_value()}  # type: ignore

    def get_schema(
        self,
        vendor: str,
        name: str,
        format: str,
        version: str,
    ) -> Optional[IgluSchema]:
        """
        Get schema by vendor/name/format/version.

        API Reference: https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/

        Args:
            vendor: Schema vendor (e.g., 'com.snowplowanalytics.snowplow')
            name: Schema name (e.g., 'page_view')
            format: Schema format (typically 'jsonschema')
            version: SchemaVer version (e.g., '1-0-0')

        Returns:
            Schema or None if not found
        """
        # GET /api/schemas/{vendor}/{name}/{format}/{version}
        # Docs: https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/
        # Example: /api/schemas/com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0
        endpoint = f"/api/schemas/{vendor}/{name}/{format}/{version}"
        url = f"{self.base_url}{endpoint}"

        try:
            logger.debug(f"Fetching schema from Iglu: {vendor}/{name}/{version}")
            response = self.session.get(url, timeout=self.config.timeout_seconds)
            response.raise_for_status()

            # Parse response using Pydantic model
            response_data = response.json()
            try:
                schema = IgluSchema.model_validate(response_data)
                return schema
            except Exception as parse_error:
                logger.error(f"Failed to parse Iglu schema response: {parse_error}")
                logger.error(
                    f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
                )
                return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"Schema not found in Iglu: {vendor}/{name}/{format}/{version}"
                )
                return None
            elif e.response.status_code == 403:
                logger.error(f"Permission denied for Iglu schema: {url}")
                raise PermissionError(
                    f"Permission denied for {url}. Check Iglu API key."
                ) from e
            else:
                logger.error(
                    f"HTTP error {e.response.status_code} fetching schema: {url}"
                )
                raise
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout fetching schema: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching schema: {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse schema from Iglu: {url}: {e}")
            return None

    def list_schemas(self) -> List[str]:
        """
        List all schemas in registry (if supported).

        Note: Not all Iglu servers support listing schemas.
        This is primarily for Iglu Server instances with the list endpoint enabled.

        Returns:
            List of schema identifiers in format 'iglu:vendor/name/format/version'
            Example: ['iglu:com.acme/page_view/jsonschema/1-0-0']
        """
        # GET /api/schemas
        # Docs: https://github.com/snowplow/iglu-server/blob/master/src/main/scala/com/snowplowanalytics/iglu/server/service/SchemaService.scala#L80
        # Returns: Array of schema URIs in format "iglu:vendor/name/format/version"
        endpoint = "/api/schemas"
        url = f"{self.base_url}{endpoint}"

        try:
            logger.info("Listing schemas from Iglu registry")
            response = self.session.get(url, timeout=self.config.timeout_seconds)
            response.raise_for_status()

            # Response format: Array of schema URIs
            # Example: ["iglu:com.test.event/page_view/jsonschema/1-0-0", ...]
            data = response.json()

            if isinstance(data, list):
                logger.info(f"Found {len(data)} schemas in Iglu registry")
                return data
            elif isinstance(data, dict) and "schemas" in data:
                # Alternative format (some Iglu implementations)
                schemas = data["schemas"]
                logger.info(f"Found {len(schemas)} schemas in Iglu registry")
                return schemas
            else:
                logger.warning(
                    f"Unexpected response format from Iglu list endpoint: {type(data)}"
                )
                return []

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    "Iglu server does not support listing schemas (404). "
                    "You must manually specify schemas_to_extract in config."
                )
                return []
            else:
                logger.warning(
                    f"Failed to list schemas from Iglu ({e.response.status_code}): {e}"
                )
                return []
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout listing schemas from Iglu: {url}")
            return []
        except Exception as e:
            logger.warning(f"Failed to list schemas from Iglu: {e}")
            return []

    @staticmethod
    def parse_iglu_uri(iglu_uri: str) -> Optional[dict]:
        """
        Parse Iglu URI into components.

        Args:
            iglu_uri: Schema URI in format 'iglu:vendor/name/format/version'
                     Example: 'iglu:com.acme/page_view/jsonschema/1-0-0'

        Returns:
            Dict with keys: vendor, name, format, version
            Or None if URI format is invalid
        """
        try:
            # Remove 'iglu:' prefix
            uri_path = iglu_uri[5:] if iglu_uri.startswith("iglu:") else iglu_uri

            # Split by '/' to get vendor/name/format/version
            parts = uri_path.split("/")
            if len(parts) != 4:
                logger.warning(
                    f"Invalid Iglu URI format: {iglu_uri} (expected 4 parts, got {len(parts)})"
                )
                return None

            return {
                "vendor": parts[0],
                "name": parts[1],
                "format": parts[2],
                "version": parts[3],
            }
        except Exception as e:
            logger.warning(f"Failed to parse Iglu URI '{iglu_uri}': {e}")
            return None

    def test_connection(self) -> bool:
        """
        Test Iglu server connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try a well-known public schema as connectivity test
            # This works for both public and private registries
            _ = self.get_schema(
                vendor="com.snowplowanalytics.snowplow",
                name="atomic",
                format="jsonschema",
                version="1-0-0",
            )
            # If we get any response (even 404), connection is working
            return True
        except PermissionError:
            # Permission error means we connected but auth failed
            logger.error("Iglu connection test: Permission denied (check API key)")
            return False
        except Exception as e:
            logger.error(f"Iglu connection test failed: {e}")
            return False

    def validate_schema(self, schema: dict) -> bool:
        """
        Validate a schema against Iglu validation rules.

        Note: This endpoint may not be available on all Iglu servers.

        Args:
            schema: Schema definition to validate

        Returns:
            True if valid, False otherwise
        """
        # POST /api/schemas/validate
        # Docs: https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/
        endpoint = "/api/schemas/validate"
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.post(
                url,
                json=schema,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()

            result = response.json()
            return result.get("valid", False)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug("Iglu validation endpoint not available")
                return True  # Assume valid if validation not supported
            else:
                logger.warning(f"Schema validation failed: {e}")
                return False
        except Exception as e:
            logger.warning(f"Schema validation error: {e}")
            return True  # Assume valid on error
