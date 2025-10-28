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
        connection_details = self._session.get(
            f"{self.config.base_url}/v1/connections/{connection_id}",
            timeout=self.config.request_timeout_sec,
        )
        return FivetranConnectionDetails(**connection_details.json().get("data", {}))
