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

# Known DataHub-supported platforms to map from Fivetran connector types
KNOWN_PLATFORMS = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "google_bigquery": "bigquery",
    "databricks": "databricks",
    "oracle": "oracle",
    "mssql": "mssql",
    "sql_server": "mssql",
    "synapse": "mssql",
    "salesforce": "salesforce",
    "mongodb": "mongodb",
    "kafka": "kafka",
    "s3": "s3",
    "azure_blob_storage": "abs",
    "gcs": "gcs",
    "google_cloud_storage": "gcs",
}


class FivetranAPIClient:
    """Client for interacting with the Fivetran REST API."""

    def __init__(self, config: FivetranAPIConfig) -> None:
        self.config = config
        self._session = self._create_session()
        # Cache for connector schemas
        self._schema_cache = {}
        # Cache for destination details
        self._destination_cache = {}

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic and auth."""
        session = requests.Session()

        # Configure retry logic for resilience
        retries = Retry(
            total=5,
            backoff_factor=0.5,
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
            # For 404 errors, return an empty response structure instead of raising
            if e.response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                if "/schemas" in endpoint:
                    return {"data": {"schemas": []}}
                return {"data": {"items": []}}
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

    def get_connector_details(self, connector_id: str) -> Dict:
        """Get additional details about a connector."""
        # Get the basic connector information
        connector_data = self.get_connector(connector_id)

        # Try to get metadata about connector type
        try:
            connector_type = connector_data.get("service", "").lower()
            # Get configuration details for the connector
            config_response = self._make_request(
                "GET", f"/connectors/{connector_id}/config"
            )
            config = config_response.get("data", {})

            # Add additional info to connector data
            connector_data["config"] = config

            # For known connector types, extract specific useful metadata
            if connector_type == "salesforce":
                self._enrich_salesforce_connector(connector_id, connector_data)
            elif connector_type == "google_bigquery":
                self._enrich_bigquery_connector(connector_id, connector_data)

            return connector_data
        except Exception as e:
            logger.warning(
                f"Failed to get additional details for connector {connector_id}: {e}"
            )
            return connector_data

    def _enrich_salesforce_connector(
        self, connector_id: str, connector_data: Dict
    ) -> None:
        """Add Salesforce-specific details to connector data."""
        try:
            # Try to get Salesforce schema information
            schema_info = self.list_connector_schemas(connector_id)
            salesforce_objects = []

            for schema in schema_info:
                schema_name = schema.get("name", "")
                for table in schema.get("tables", []):
                    if table.get("enabled", False):
                        salesforce_objects.append(
                            f"{schema_name}.{table.get('name', '')}"
                        )

            if salesforce_objects:
                connector_data["salesforce_objects"] = salesforce_objects
        except Exception as e:
            logger.warning(f"Failed to enrich Salesforce connector {connector_id}: {e}")

    def _enrich_bigquery_connector(
        self, connector_id: str, connector_data: Dict
    ) -> None:
        """Add BigQuery-specific details to connector data."""
        try:
            config = connector_data.get("config", {})
            project = config.get("project_id", "")
            dataset = config.get("dataset", "")

            if project and dataset:
                connector_data["bigquery_location"] = f"{project}.{dataset}"
        except Exception as e:
            logger.warning(f"Failed to enrich BigQuery connector {connector_id}: {e}")

    def list_connector_schemas(self, connector_id: str) -> List[Dict]:
        """Get schema information for a connector."""
        # Check cache first
        if connector_id in self._schema_cache:
            return self._schema_cache[connector_id]

        response = self._make_request("GET", f"/connectors/{connector_id}/schemas")
        schemas = response.get("data", {}).get("schemas", [])

        # Cache the response
        self._schema_cache[connector_id] = schemas
        return schemas

    def list_users(self) -> List[Dict]:
        """Get all users in the Fivetran account."""
        response = self._make_request("GET", "/users")
        return response.get("data", {}).get("items", [])

    def get_destination_details(self, group_id: str) -> Dict:
        """Get details about a destination group"""
        if not group_id:
            return {}

        # Check cache first
        if group_id in self._destination_cache:
            return self._destination_cache[group_id]

        try:
            response = self._make_request("GET", f"/groups/{group_id}")
            destination_data = response.get("data", {})

            # Additional destination details
            try:
                # Try to get destination config
                config_response = self._make_request(
                    "GET", f"/groups/{group_id}/config"
                )
                destination_data["config"] = config_response.get("data", {})
            except Exception as e:
                logger.debug(f"Could not get destination config for {group_id}: {e}")
                pass

            # Cache the result
            self._destination_cache[group_id] = destination_data
            return destination_data
        except Exception as e:
            logger.warning(
                f"Failed to get destination details for group ID {group_id}: {e}"
            )
            return {}

    def detect_destination_platform(self, group_id: str) -> str:
        """Attempt to detect the destination platform from group information"""
        destination = self.get_destination_details(group_id)

        # Get the destination service if available
        service = destination.get("service", "")

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
            elif "synapse" in service:
                return "synapse"
            elif "azure_sql_database" in service:
                return "mssql"

        return "snowflake"  # Default to snowflake if detection failed

    def get_destination_database(self, group_id: str) -> str:
        """Get the database name for a destination."""
        destination = self.get_destination_details(group_id)

        # Check config for database information
        config = destination.get("config", {})

        # Try different fields based on destination type
        service = destination.get("service", "").lower()
        if "snowflake" in service:
            return config.get("database", "")
        elif "bigquery" in service:
            return config.get("dataset", "")
        elif "redshift" in service or "postgres" in service or "mysql" in service:
            return config.get("database", "")

        # Fall back to generic field
        return config.get("database", "")

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
            # First, try the specific connector's logs endpoint
            since_time = int(time.time()) - (days * 24 * 60 * 60)
            params = {"limit": 100, "since": since_time}

            # Try v1/connectors/{connector_id}/logs (most likely endpoint)
            try:
                response = self._make_request(
                    "GET", f"/connectors/{connector_id}/logs", params=params
                )
                logs = response.get("data", {}).get("items", [])
                if logs:
                    # If we have logs, try to extract sync history from them
                    return self._extract_sync_history_from_logs(logs)
            except Exception as e:
                logger.warning(f"Failed to get connector logs: {e}")

            # Try connector metadata to see if it has any sync information
            try:
                connector_details = self.get_connector(connector_id)
                if (
                    "succeeded_at" in connector_details
                    or "failed_at" in connector_details
                ):
                    # Create a synthetic job record
                    sync_time = None
                    status = None
                    if "succeeded_at" in connector_details:
                        sync_time = connector_details.get("succeeded_at")
                        status = "COMPLETED"
                    elif "failed_at" in connector_details:
                        sync_time = connector_details.get("failed_at")
                        status = "FAILED"

                    if sync_time:
                        return [
                            {
                                "id": f"{connector_id}-latest",
                                "created_at": sync_time,
                                "succeeded_at": sync_time
                                if status == "COMPLETED"
                                else None,
                                "failed_at": sync_time if status == "FAILED" else None,
                                "status": status,
                            }
                        ]
            except Exception as e:
                logger.warning(f"Failed to get connector details for sync history: {e}")

            # If all attempts fail, return empty list
            return []

        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to get sync history: {e}")
            return []

    def _extract_sync_history_from_logs(self, logs: List[Dict]) -> List[Dict]:
        """Extract sync history entries from connector logs."""
        # Group logs by sync_id
        sync_groups = {}
        for log in logs:
            if "sync_id" in log:
                sync_id = log.get("sync_id")
                if sync_id not in sync_groups:
                    sync_groups[sync_id] = []
                sync_groups[sync_id].append(log)

        # Create sync history entries from log groups
        sync_history = []
        for sync_id, group_logs in sync_groups.items():
            # Find start and end logs
            start_log = None
            end_log = None

            for log in group_logs:
                message = log.get("message", "").lower()
                if "sync started" in message or "started sync" in message:
                    start_log = log
                elif "sync completed" in message:
                    end_log = log
                    end_log["status"] = "COMPLETED"
                elif "sync failed" in message or "failed" in message:
                    end_log = log
                    end_log["status"] = "FAILED"

            # Only create history entry if we have both start and end
            if start_log and end_log:
                entry = {
                    "id": sync_id,
                    "created_at": start_log.get("created_at"),
                    "status": end_log.get("status", "UNKNOWN"),
                }

                if entry["status"] == "COMPLETED":
                    entry["succeeded_at"] = end_log.get("created_at")
                else:
                    entry["failed_at"] = end_log.get("created_at")

                sync_history.append(entry)

        return sync_history

    def _parse_timestamp(self, iso_timestamp: Optional[str]) -> Optional[int]:
        """Parse ISO timestamp to Unix timestamp."""
        if not iso_timestamp:
            return None

        try:
            # Handle different timestamp formats
            if "T" in iso_timestamp:
                # ISO format with T separator
                dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
                return int(dt.timestamp())
            else:
                # Try simple format
                dt = datetime.strptime(iso_timestamp, "%Y-%m-%d %H:%M:%S")
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

        # Extract additional properties to include in the connector
        additional_properties = {}

        # Extract jobs from sync history
        jobs = self._extract_jobs_from_sync_history(sync_history)

        destination_id = api_connector.get("group", {}).get("id", "")
        destination_platform = self.detect_destination_platform(destination_id)
        destination_database = self.get_destination_database(destination_id)

        # Add destination info to properties
        additional_properties["destination_platform"] = destination_platform
        if destination_database:
            additional_properties["destination_database"] = destination_database

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
            additional_properties=additional_properties,
        )

    def _get_connector_details_safely(self, connector_id: str) -> Dict:
        """Extract connector details safely with error handling."""
        additional_properties = {}
        try:
            connector_details = self.get_connector_details(connector_id)

            # Add configuration details if available
            if "config" in connector_details:
                config = connector_details.get("config", {})
                # Extract relevant config fields based on connector type
                for key, value in config.items():
                    if isinstance(value, (str, int, bool, float)):
                        additional_properties[f"config.{key}"] = str(value)

            # Add Salesforce objects if available
            if "salesforce_objects" in connector_details:
                additional_properties["salesforce_objects"] = ", ".join(
                    connector_details.get("salesforce_objects", [])
                )

            # Add BigQuery location if available
            if "bigquery_location" in connector_details:
                additional_properties["bigquery_location"] = connector_details.get(
                    "bigquery_location"
                )
        except Exception as e:
            logger.warning(f"Failed to extract additional connector metadata: {e}")

        return additional_properties

    def _extract_jobs_from_sync_history(self, sync_history: List[Dict]) -> List[Job]:
        """Extract jobs from sync history with proper error handling."""
        jobs = []
        for job in sync_history:
            # Try different possible field names for start and end times
            started_at = self._find_timestamp(
                job, ["started_at", "start_time", "created_at", "timestamp"]
            )

            # Get status if available
            status = self._find_status(job)

            # Find completion timestamp based on status and available fields
            completed_at = self._find_completion_timestamp(job, status)

            # If we're missing end time but have start time, use current time for recent jobs
            if started_at and not completed_at:
                if (time.time() - started_at) < (24 * 60 * 60):  # Within last 24 hours
                    completed_at = int(time.time())
                else:
                    # Skip old jobs without completion times
                    continue

            # Only include jobs with both timestamps
            if started_at and completed_at:
                # Map status to constants
                status_mapped = self._map_status_to_constant(status)

                jobs.append(
                    Job(
                        job_id=job.get(
                            "id", str(hash(str(job)))
                        ),  # Use hash as fallback ID
                        start_time=started_at,
                        end_time=completed_at,
                        status=status_mapped,
                    )
                )

        return jobs

    def _find_timestamp(self, job: Dict, field_names: List[str]) -> Optional[int]:
        """Find timestamp in job data from a list of possible field names."""
        for field in field_names:
            if field in job:
                timestamp = self._parse_timestamp(job.get(field))
                if timestamp:
                    return timestamp
        return None

    def _find_status(self, job: Dict) -> Optional[str]:
        """Find status in job data."""
        for status_field in ["status", "state", "result"]:
            if status_field in job:
                status = job.get(status_field, "").upper()
                if status:
                    return status
        return None

    def _find_completion_timestamp(
        self, job: Dict, status: Optional[str]
    ) -> Optional[int]:
        """Find completion timestamp based on status and available fields."""
        # If we have status, determine completion field to check
        if status in ["COMPLETED", "SUCCESS", "SUCCEEDED"]:
            return self._find_timestamp(
                job, ["completed_at", "end_time", "finished_at", "succeeded_at"]
            )
        elif status in ["FAILED", "FAILURE", "ERROR"]:
            return self._find_timestamp(job, ["failed_at", "end_time", "finished_at"])
        else:
            # If no explicit status, check all end fields
            return self._find_timestamp(
                job, ["completed_at", "end_time", "finished_at", "updated_at"]
            )

    def _map_status_to_constant(self, status: Optional[str]) -> str:
        """Map API status to internal constants."""
        if not status:
            return Constant.SUCCESSFUL  # Default to success

        if status in ["COMPLETED", "SUCCESS", "SUCCEEDED"]:
            return Constant.SUCCESSFUL
        elif status in ["FAILED", "FAILURE", "ERROR"]:
            return Constant.FAILURE_WITH_TASK
        elif status in ["CANCELLED", "CANCELED", "ABORTED", "STOPPED"]:
            return Constant.CANCELED
        else:
            # Default to success for unknown status
            return Constant.SUCCESSFUL

    def extract_table_lineage(self, connector_id: str) -> List[TableLineage]:
        """
        Extract table lineage information for a connector.
        Uses a generic approach that works for any connector type.
        """
        try:
            # Get the connector details first
            connector_details = self.get_connector(connector_id)

            # Get destination information
            destination_id = connector_details.get("group", {}).get("id", "")
            destination_platform = self.detect_destination_platform(destination_id)

            # Get schema information
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

                    # Create source table name
                    source_table = f"{schema_name}.{table_name}"

                    # Create destination table name based on destination platform
                    destination_schema = self._get_destination_schema_name(
                        schema_name, destination_platform
                    )
                    destination_table = self._get_destination_table_name(
                        table_name, destination_platform
                    )
                    destination_table_full = f"{destination_schema}.{destination_table}"

                    # Extract column information
                    columns = table.get("columns", [])
                    column_lineage = []

                    for column in columns:
                        column_name = column.get("name", "")
                        # Adjust destination column name based on platform
                        dest_column_name = self._get_destination_column_name(
                            column_name, destination_platform
                        )

                        column_lineage.append(
                            ColumnLineage(
                                source_column=column_name,
                                destination_column=dest_column_name,
                            )
                        )

                    lineage_list.append(
                        TableLineage(
                            source_table=source_table,
                            destination_table=destination_table_full,
                            column_lineage=column_lineage,
                        )
                    )

            return lineage_list
        except Exception as e:
            logger.error(f"Failed to extract lineage for connector {connector_id}: {e}")
            return []

    def _extract_lineage_with_special_handling(
        self, connector: Connector, source_platform: str
    ) -> List[TableLineage]:
        """
        Extract lineage with special handling for certain source platforms
        like Salesforce that need additional processing.
        """
        connector_id = connector.connector_id
        lineage_list = []

        try:
            schemas = self.list_connector_schemas(connector_id)

            # Get destination details
            destination_platform = connector.additional_properties.get(
                "destination_platform", ""
            )

            for schema in schemas:
                schema_name = schema.get("name", "")
                tables = schema.get("tables", [])

                for table in tables:
                    table_name = table.get("name", "")
                    enabled = table.get("enabled", False)

                    if not enabled:
                        continue

                    # Create source table name for Salesforce
                    source_table = f"{schema_name}.{table_name}"

                    # Create destination table name based on destination platform
                    destination_schema = self._get_destination_schema_name(
                        schema_name, destination_platform
                    )
                    destination_table = self._get_destination_table_name(
                        table_name, destination_platform
                    )
                    destination_table_full = f"{destination_schema}.{destination_table}"

                    # Extract column information
                    columns = table.get("columns", [])
                    column_lineage = []

                    for column in columns:
                        column_name = column.get("name", "")
                        # Adjust destination column name based on platform
                        dest_column_name = self._get_destination_column_name(
                            column_name, destination_platform
                        )

                        column_lineage.append(
                            ColumnLineage(
                                source_column=column_name,
                                destination_column=dest_column_name,
                            )
                        )

                    lineage_list.append(
                        TableLineage(
                            source_table=source_table,
                            destination_table=destination_table_full,
                            column_lineage=column_lineage,
                        )
                    )

            return lineage_list
        except Exception as e:
            logger.error(f"Failed to extract special lineage for {connector_id}: {e}")
            return []

    def _get_destination_schema_name(
        self, schema_name: str, destination_platform: str
    ) -> str:
        """Get the destination schema name based on the platform."""
        if destination_platform.lower() == "bigquery":
            return schema_name.lower()
        else:
            return schema_name.upper()

    def _get_destination_table_name(
        self, table_name: str, destination_platform: str
    ) -> str:
        """Get the destination table name based on the platform."""
        if destination_platform.lower() == "bigquery":
            return table_name.lower()
        else:
            return table_name.upper()

    def _get_destination_column_name(
        self, column_name: str, destination_platform: str
    ) -> str:
        """Get the destination column name based on the platform."""
        if destination_platform.lower() == "bigquery":
            return column_name.lower()
        else:
            return column_name.upper()

    def _build_lineage_from_schemas(
        self, schemas: List[Dict], connector: Connector
    ) -> List[TableLineage]:
        """
        Build lineage information from schemas for a generic connector.
        """
        destination_platform = connector.additional_properties.get(
            "destination_platform", ""
        )

        lineage_list = []

        for schema in schemas:
            schema_name = schema.get("name", "")
            tables = schema.get("tables", [])

            for table in tables:
                table_name = table.get("name", "")
                enabled = table.get("enabled", False)

                if not enabled:
                    continue

                # Create source table name
                source_table = f"{schema_name}.{table_name}"

                # Create destination table name based on destination platform
                destination_schema = self._get_destination_schema_name(
                    schema_name, destination_platform
                )
                destination_table = self._get_destination_table_name(
                    table_name, destination_platform
                )
                destination_table_full = f"{destination_schema}.{destination_table}"

                # Extract column information
                columns = table.get("columns", [])
                column_lineage = []

                for column in columns:
                    column_name = column.get("name", "")
                    # Adjust destination column name based on platform
                    dest_column_name = self._get_destination_column_name(
                        column_name, destination_platform
                    )

                    column_lineage.append(
                        ColumnLineage(
                            source_column=column_name,
                            destination_column=dest_column_name,
                        )
                    )

                lineage_list.append(
                    TableLineage(
                        source_table=source_table,
                        destination_table=destination_table_full,
                        column_lineage=column_lineage,
                    )
                )

        return lineage_list
