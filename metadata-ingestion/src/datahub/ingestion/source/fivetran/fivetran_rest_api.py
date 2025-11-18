import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
)
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionConfig,
    FivetranConnectionDetails,
)

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
        """
        Get details for a specific connection from the Fivetran API.

        Args:
            connection_id: The Fivetran connection ID to fetch details for.

        Returns:
            FivetranConnectionDetails: The parsed connection details.

        """
        try:
            connection_details = self._session.get(
                f"{self.config.base_url}/v1/connections/{connection_id}",
                timeout=self.config.request_timeout_sec,
            )

            # Check for HTTP errors and raise HTTPError if needed
            connection_details.raise_for_status()

            response_json = connection_details.json()

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

            # Manually extract and construct nested models with only required fields
            try:
                config_data = data.get("config", {})
                config = FivetranConnectionConfig(
                    auth_type=config_data.get("auth_type"),
                    sheet_id=config_data.get("sheet_id"),
                    named_range=config_data.get("named_range"),
                )

                # Parse into FivetranConnectionDetails with manually extracted fields
                return FivetranConnectionDetails(
                    id=data.get("id"),
                    group_id=data.get("group_id"),
                    service=data.get("service"),
                    created_at=data.get("created_at"),
                    succeeded_at=data.get("succeeded_at"),
                    paused=data.get("paused"),
                    sync_frequency=data.get("sync_frequency"),
                    config=config,
                )
            except Exception as e:
                logger.debug(
                    f"Failed to parse FivetranConnectionDetails for connection_id: {connection_id}. "
                    f"Response data: {data}",
                    exc_info=True,
                )
                # The error message is in the data field
                raise ValueError(
                    f"Failed to parse FivetranConnectionDetails for connection_id {connection_id}. Data: {data}"
                ) from e

        except Exception as e:
            logger.debug(
                f"Request error occurred while fetching connection details for connection_id: {connection_id}",
                exc_info=True,
            )
            raise e
