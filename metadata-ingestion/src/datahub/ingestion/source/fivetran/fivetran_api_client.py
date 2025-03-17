import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fivetran.config import Constant, FivetranAPIConfig
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)

logger = logging.getLogger(__name__)


class FivetranAPIClient:
    """Client for interacting with the Fivetran REST API."""

    def __init__(self, config: FivetranAPIConfig) -> None:
        self.config = config
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic and auth."""
        session = requests.Session()

        # Configure retry logic for resilience
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Set up basic authentication
        session.auth = (self.config.api_key, self.config.api_secret)
        return session

    def _make_request(self, method: str, endpoint: str, **kwargs: Any) -> Dict:
        """Make a request to the Fivetran API."""
        url = f"{self.config.base_url}/v1{endpoint}"

        # Set default timeout
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.config.request_timeout_sec

        # Add standard headers
        headers = kwargs.get("headers", {})
        headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        kwargs["headers"] = headers

        try:
            logger.debug(f"Making {method} request to {url}")
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            # If we get a 405 (Method Not Allowed) error, log additional information
            if e.response.status_code == 405:
                logger.error(f"Method {method} not allowed for {url}")
                allowed_methods = e.response.headers.get("Allow", "unknown")
                logger.error(f"Allowed methods: {allowed_methods}")
            raise

    def list_connectors(self) -> List[Dict]:
        """Retrieve all connectors from the Fivetran API."""
        connectors = []
        next_cursor = None

        while True:
            params = {"limit": 100}
            if next_cursor:
                params["cursor"] = next_cursor

            response = self._make_request("GET", "/connectors", params=params)
            items = response.get("data", {}).get("items", [])
            connectors.extend(items)

            next_cursor = response.get("data", {}).get("next_cursor")
            if not next_cursor:
                break

        return connectors

    def get_connector(self, connector_id: str) -> Dict:
        """Get details for a specific connector."""
        response = self._make_request("GET", f"/connectors/{connector_id}")
        return response.get("data", {})

    def list_connector_schemas(self, connector_id: str) -> List[Dict]:
        """Get schema information for a connector."""
        response = self._make_request("GET", f"/connectors/{connector_id}/schemas")
        return response.get("data", {}).get("schemas", [])

    def list_users(self) -> List[Dict]:
        """Get all users in the Fivetran account."""
        response = self._make_request("GET", "/users")
        return response.get("data", {}).get("items", [])

    def get_destination_details(self, group_id: str) -> Dict:
        """Get details about a destination group"""
        if not group_id:
            return {}

        try:
            response = self._make_request("GET", f"/groups/{group_id}")
            return response.get("data", {})
        except Exception as e:
            logger.warning(
                f"Failed to get destination details for group ID {group_id}: {e}"
            )
            return {}

    def detect_destination_platform(self, group_id: str) -> str:
        """Attempt to detect the destination platform from group information"""
        destination = self.get_destination_details(group_id)

        # Get the destination service if available
        service = destination.get("service")

        # Map Fivetran service names to DataHub platform names
        if service:
            service = service.lower()
            if "snowflake" in service:
                return "snowflake"
            elif "bigquery" in service:
                return "bigquery"
            elif "redshift" in service:
                return "redshift"
            elif "postgres" in service:
                return "postgres"
            elif "mysql" in service:
                return "mysql"
            elif "databricks" in service:
                return "databricks"

        return "snowflake"

    def get_user(self, user_id: str) -> Dict:
        """Get details for a specific user."""
        response = self._make_request("GET", f"/users/{user_id}")
        return response.get("data", {})

    def list_groups(self) -> List[Dict]:
        """Get all destination groups in the Fivetran account."""
        groups = []
        next_cursor = None

        while True:
            params = {"limit": 100}
            if next_cursor:
                params["cursor"] = next_cursor

            response = self._make_request("GET", "/groups", params=params)
            items = response.get("data", {}).get("items", [])
            groups.extend(items)

            next_cursor = response.get("data", {}).get("next_cursor")
            if not next_cursor:
                break

        return groups

    def list_connector_sync_history(
        self, connector_id: str, days: int = 7
    ) -> List[Dict]:
        """Get the sync history for a connector."""
        try:
            # First, try the "synchronization_history" endpoint (most likely correct)
            since_time = int(time.time()) - (days * 24 * 60 * 60)
            params = {"limit": 100, "since": since_time}

            response = self._make_request(
                "GET",
                f"/connectors/{connector_id}/synchronization_history",
                params=params,
            )
            return response.get("data", {}).get("items", [])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    "synchronization_history endpoint not found, trying alternative endpoints"
                )

                # Try alternative endpoints
                try:
                    # Try "history" endpoint
                    response = self._make_request(
                        "GET", f"/connectors/{connector_id}/history", params=params
                    )
                    return response.get("data", {}).get("items", [])
                except requests.exceptions.HTTPError:
                    # As a last resort, try getting runs directly
                    logger.warning(
                        "history endpoint not found, trying to get connector runs"
                    )
                    try:
                        response = self._make_request(
                            "GET", f"/connectors/{connector_id}/runs", params=params
                        )
                        return response.get("data", {}).get("items", [])
                    except requests.exceptions.HTTPError as e:
                        logger.error(f"Failed to get sync history: {e}")
                        # Return empty list if all attempts fail
                        return []
            else:
                logger.error(f"Failed to get sync history: {e}")
                return []

    def _parse_timestamp(self, iso_timestamp: Optional[str]) -> Optional[int]:
        """Parse ISO timestamp to Unix timestamp."""
        if not iso_timestamp:
            return None

        try:
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse timestamp {iso_timestamp}: {e}")
            return None

    def extract_connector_metadata(
        self, api_connector: Dict, sync_history: List[Dict]
    ) -> Connector:
        """
        Convert API connector data to our internal Connector model.
        """
        connector_id = api_connector.get("id")
        if not connector_id:
            raise ValueError(f"Connector is missing required id field: {api_connector}")

        connector_name = api_connector.get("name", "")
        connector_service = api_connector.get("service", "")
        paused = api_connector.get("paused", False)

        # Get sync frequency in minutes
        schedule = api_connector.get("schedule", {})
        sync_frequency = schedule.get("sync_frequency", 360)  # Default to 6 hours

        # Convert sync jobs to our Job model
        jobs = []
        for job in sync_history:
            # Try different possible field names for start and end times
            started_at = None
            completed_at = None

            # Check for different possible field names
            for start_field in ["started_at", "start_time", "created_at"]:
                if start_field in job:
                    started_at = self._parse_timestamp(job.get(start_field))
                    if started_at:
                        break

            for end_field in ["completed_at", "end_time", "finished_at", "updated_at"]:
                if end_field in job:
                    completed_at = self._parse_timestamp(job.get(end_field))
                    if completed_at:
                        break

            # Only include completed jobs
            if started_at and completed_at:
                # Try different possible field names for status
                status = None
                for status_field in ["status", "state", "result"]:
                    if status_field in job:
                        status = job.get(status_field, "").upper()
                        if status:
                            break

                if not status:
                    continue

                # Map Fivetran API status to our constants
                # API returns: "COMPLETED", "FAILED", "CANCELLED", etc.
                if status in ["COMPLETED", "SUCCESS", "SUCCEEDED"]:
                    status = Constant.SUCCESSFUL
                elif status in ["FAILED", "FAILURE", "ERROR"]:
                    status = Constant.FAILURE_WITH_TASK
                elif status in ["CANCELLED", "CANCELED", "ABORTED", "STOPPED"]:
                    status = Constant.CANCELED
                else:
                    # Skip unknown statuses
                    continue

                jobs.append(
                    Job(
                        job_id=job.get(
                            "id", str(hash(str(job)))
                        ),  # Use hash as fallback ID
                        start_time=started_at,
                        end_time=completed_at,
                        status=status,
                    )
                )

        destination_id = api_connector.get("group", {}).get("id", "")
        destination_platform = self.detect_destination_platform(destination_id)

        return Connector(
            connector_id=connector_id,
            connector_name=connector_name,
            connector_type=connector_service,
            paused=paused,
            sync_frequency=sync_frequency,
            destination_id=destination_id,
            user_id=api_connector.get("created_by", ""),
            lineage=[],  # Will be filled later
            jobs=jobs,
            # Add additional properties - we can access these later
            additional_properties={"destination_platform": destination_platform},
        )

    def extract_table_lineage(self, connector_id: str) -> List[TableLineage]:
        """
        Extract table lineage information from connector schemas.
        This is a simplified approach, as standard Fivetran doesn't provide direct lineage info.
        """
        schemas = self.list_connector_schemas(connector_id)
        lineage_list = []

        for schema in schemas:
            schema_name = schema.get("name", "")
            tables = schema.get("tables", [])

            for table in tables:
                table_name = table.get("name", "")
                enabled = table.get("enabled", False)

                if not enabled:
                    continue

                # For each source table, we identify the destination
                # This is a simplification - in reality we would need to analyze the actual data flows
                source_table = f"{schema_name}.{table_name}"

                # The destination naming follows Fivetran's standard pattern
                # In a real implementation, we'd need to fetch this from the connector configuration
                destination_schema = schema_name
                destination_table = table_name

                # Extract column information
                columns = table.get("columns", [])
                column_lineage = []

                for column in columns:
                    column_name = column.get("name", "")
                    # In standard Fivetran, column names are typically preserved
                    column_lineage.append(
                        ColumnLineage(
                            source_column=column_name, destination_column=column_name
                        )
                    )

                lineage_list.append(
                    TableLineage(
                        source_table=source_table,
                        destination_table=f"{destination_schema}.{destination_table}",
                        column_lineage=column_lineage,
                    )
                )

        return lineage_list
