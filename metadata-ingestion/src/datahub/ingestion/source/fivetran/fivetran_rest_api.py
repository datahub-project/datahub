import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
)
from datahub.ingestion.source.fivetran.response_models import FivetranConnectionDetails

logger = logging.getLogger(__name__)

# Retry configuration constants
RETRY_MAX_TIMES = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_BACKOFF_FACTOR = 1
RETRY_ALLOWED_METHODS = ["GET"]


class FivetranAPIClient:
    """Client for interacting with the Fivetran REST API."""

    def __init__(self, config: FivetranAPIConfig) -> None:
        self.config = config
        self._session = self._create_session()

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
        requests_session.auth = (self.config.api_key, self.config.api_secret)
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
        """Get details for a specific connection."""
        logger.debug(
            f"Fetching connection details for connection_id: {connection_id} from {self.config.base_url}/v1/connections/{connection_id}"
        )

        connection_details = self._session.get(
            f"{self.config.base_url}/v1/connections/{connection_id}",
            timeout=self.config.request_timeout_sec,
        )

        logger.debug(
            f"API response status code: {connection_details.status_code} for connection_id: {connection_id}"
        )

        response_json = connection_details.json()
        logger.debug(
            f"Raw API response for connection_id {connection_id}: {response_json}"
        )

        data = response_json.get("data", {})
        logger.debug(
            f"Extracted 'data' field for connection_id {connection_id}. Keys present: {list(data.keys())}"
        )

        # Log specific fields that are expected
        expected_fields = [
            "id",
            "group_id",
            "service",
            "created_at",
            "succeeded_at",
            "paused",
            "sync_frequency",
            "status",
            "config",
            "source_sync_details",
        ]
        missing_fields = [field for field in expected_fields if field not in data]
        if missing_fields:
            logger.warning(
                f"Missing expected fields in API response for connection_id {connection_id}: {missing_fields}"
            )

        # Log the presence/absence of source_sync_details specifically
        if "source_sync_details" not in data:
            logger.warning(
                f"source_sync_details field is missing in API response for connection_id {connection_id}. "
                f"Available fields: {list(data.keys())}"
            )
        else:
            logger.debug(
                f"source_sync_details for connection_id {connection_id}: {data.get('source_sync_details')}"
            )

        # Log config field if present
        if "config" in data:
            logger.debug(
                f"config field for connection_id {connection_id}: {data.get('config')}"
            )

        # Log status field if present
        if "status" in data:
            logger.debug(
                f"status field for connection_id {connection_id}: {data.get('status')}"
            )

        try:
            return FivetranConnectionDetails(**data)
        except Exception as e:
            logger.error(
                f"Failed to parse FivetranConnectionDetails for connection_id {connection_id}. "
                f"Error: {e}. "
                f"Response data: {data}"
            )
            raise
