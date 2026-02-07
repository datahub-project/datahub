"""
Iglu Schema Registry API client.

Handles communication with Iglu schema registry for open-source Snowplow deployments
or as a fallback for BDP deployments.

API Documentation: https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/
"""

import json
import logging
import time
from typing import TYPE_CHECKING, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport

from datahub.ingestion.source.snowplow.models.snowplow_models import IgluSchema
from datahub.ingestion.source.snowplow.snowplow_config import IgluConnectionConfig

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

    def __init__(
        self,
        config: IgluConnectionConfig,
        report: Optional["SnowplowSourceReport"] = None,
    ):
        """
        Initialize Iglu Schema Registry client.

        Args:
            config: Iglu connection configuration
            report: Optional report for tracking API metrics
        """
        self.config = config
        self.base_url = config.iglu_server_url
        self.report = report  # For API metrics tracking

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
            # Store as instance variable to use in requests
            self._api_key_param: dict[str, str] = {
                "apikey": config.api_key.get_secret_value()
            }
        else:
            self._api_key_param = {}

    def close(self) -> None:
        """Close the HTTP session and release resources."""
        if hasattr(self, "session") and self.session:
            self.session.close()

    def __enter__(self) -> "IgluClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.close()

    def _record_api_call(
        self, endpoint: str, latency_ms: float, is_error: bool
    ) -> None:
        """Record API call metrics if report is available."""
        if self.report is not None:
            self.report.record_api_call("iglu", endpoint, latency_ms, is_error)

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
        start_time = time.perf_counter()
        is_error = False

        try:
            logger.debug(f"Fetching schema from Iglu: {vendor}/{name}/{version}")
            response = self.session.get(
                url, params=self._api_key_param, timeout=self.config.timeout_seconds
            )
            response.raise_for_status()

            # Parse response using Pydantic model
            response_data = response.json()
            try:
                schema = IgluSchema.model_validate(response_data)
                return schema
            except Exception as parse_error:
                is_error = True
                logger.error(f"Failed to parse Iglu schema response: {parse_error}")
                logger.error(
                    f"Raw response data: {json.dumps(response_data, indent=2, default=str)}"
                )
                return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # 404 is expected for missing schemas, not an error
                logger.warning(
                    f"Schema not found in Iglu: {vendor}/{name}/{format}/{version}"
                )
                return None
            elif e.response.status_code == 403:
                is_error = True
                logger.error(f"Permission denied for Iglu schema: {url}")
                raise PermissionError(
                    f"Permission denied for {url}. Check Iglu API key."
                ) from e
            else:
                is_error = True
                logger.error(
                    f"HTTP error {e.response.status_code} fetching schema: {url}"
                )
                raise
        except requests.exceptions.Timeout:
            is_error = True
            logger.error(f"Request timeout fetching schema: {url}")
            raise
        except requests.exceptions.RequestException as e:
            is_error = True
            logger.error(f"Request failed fetching schema: {url}: {e}")
            raise
        except Exception as e:
            is_error = True
            logger.error(f"Failed to parse schema from Iglu: {url}: {e}")
            return None
        finally:
            latency_ms = (time.perf_counter() - start_time) * 1000
            self._record_api_call("get_schema", latency_ms, is_error)

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
        start_time = time.perf_counter()
        is_error = False

        try:
            logger.info("Listing schemas from Iglu registry")
            response = self.session.get(
                url, params=self._api_key_param, timeout=self.config.timeout_seconds
            )
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
                # 404 is expected for older Iglu servers without list support
                logger.warning(
                    "Iglu server does not support listing schemas (404). "
                    "You must manually specify schemas_to_extract in config."
                )
                return []
            elif e.response.status_code in (401, 403):
                is_error = True
                # Auth failures should not be silent - reraise as fatal
                logger.error(
                    f"Authentication failed for Iglu schema list ({e.response.status_code}): {e}"
                )
                raise PermissionError(
                    f"Authentication failed for Iglu schema list. "
                    f"Check your Iglu API key configuration. Status: {e.response.status_code}"
                ) from e
            else:
                is_error = True
                # Other HTTP errors - reraise to let caller handle
                logger.error(
                    f"HTTP error listing schemas from Iglu ({e.response.status_code}): {e}"
                )
                raise
        except requests.exceptions.Timeout:
            is_error = True
            logger.error(f"Timeout listing schemas from Iglu: {url}")
            raise
        except requests.exceptions.ConnectionError as e:
            is_error = True
            logger.error(f"Connection error listing schemas from Iglu: {url}: {e}")
            raise
        except Exception as e:
            is_error = True
            logger.error(f"Unexpected error listing schemas from Iglu: {e}")
            raise
        finally:
            latency_ms = (time.perf_counter() - start_time) * 1000
            self._record_api_call("list_schemas", latency_ms, is_error)

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
                params=self._api_key_param,
                json=schema,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()

            result = response.json()
            return result.get("valid", False)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Validation endpoint not available on this server - assume valid
                # since we can't validate (this is expected for some Iglu servers)
                logger.debug("Iglu validation endpoint not available (404)")
                return True
            elif e.response.status_code in (401, 403):
                logger.error(
                    f"Schema validation failed: permission denied ({e.response.status_code}). "
                    "Check Iglu API key permissions."
                )
                return False
            else:
                logger.warning(
                    f"Schema validation failed with HTTP {e.response.status_code}: {e}"
                )
                return False
        except requests.exceptions.Timeout:
            logger.warning(
                f"Schema validation timeout after {self.config.timeout_seconds}s - "
                "assuming schema is invalid"
            )
            return False
        except requests.exceptions.RequestException as e:
            logger.warning(f"Schema validation request failed: {e}")
            return False
        except Exception as e:
            # Fail closed on unexpected errors - don't assume validity
            logger.warning(f"Schema validation error (assuming invalid): {e}")
            return False
