import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
)
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
        self._schema_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._destination_cache: Dict[str, Dict[str, Any]] = {}
        self._connector_name_to_id_cache: Dict[str, str] = {}
        self._column_cache: Dict[str, List[Dict[str, Any]]] = {}

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

            if e.response.status_code == 404:
                # If we get a 404 for a connector endpoint, it might be because we're using name instead of ID
                if "/connectors/" in endpoint:
                    # Extract the connector name or ID from the endpoint
                    parts = endpoint.split("/connectors/")
                    if len(parts) > 1:
                        connector_identifier = parts[1].split("/")[0]
                        logger.warning(
                            f"404 error for connector identifier: {connector_identifier}"
                        )

                        # Try to get the connector ID if we used a name
                        if connector_identifier:
                            try:
                                correct_id = self.get_connector_id_by_name(
                                    connector_identifier
                                )
                                if correct_id and correct_id != connector_identifier:
                                    logger.info(
                                        f"Found correct connector ID: {correct_id} for name: {connector_identifier}"
                                    )
                                    # Modify the endpoint with the correct ID
                                    new_endpoint = endpoint.replace(
                                        f"/connectors/{connector_identifier}",
                                        f"/connectors/{correct_id}",
                                    )
                                    logger.info(
                                        f"Retrying with correct endpoint: {new_endpoint}"
                                    )
                                    # Retry the request with the correct ID
                                    return self._make_request(
                                        method, new_endpoint, **kwargs
                                    )
                            except Exception as id_error:
                                logger.warning(
                                    f"Error trying to find connector ID: {id_error}"
                                )

                # Return an empty response for 404 errors
                logger.warning(f"Resource not found: {url}")
                if "/schemas" in endpoint:
                    return {"data": {"schemas": []}}
                if "/logs" in endpoint:
                    return {"data": {"items": []}}
                return {"data": {"items": []}}

            # For other HTTP errors, provide more context
            if e.response.status_code == 405:
                logger.error(f"Method {method} not allowed for {url}")
                allowed_methods = e.response.headers.get("Allow", "unknown")
                logger.error(f"Allowed methods: {allowed_methods}")

            raise

    def get_connector_id_by_name(self, connector_name_or_id: str) -> Optional[str]:
        """
        Find the connector ID by its name.
        If the input is already an ID, it will be returned as is if it exists.

        Args:
            connector_name_or_id: Either the connector name or ID

        Returns:
            The connector ID or None if not found
        """
        # Check if this is already an ID that exists
        try:
            # Try to get the connector directly by ID first
            self._make_request("GET", f"/connectors/{connector_name_or_id}")
            # If successful, it's a valid ID
            return connector_name_or_id
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                # For errors other than 404, re-raise
                raise
            # For 404, continue with name lookup
            pass

        # If we already have this name cached, return the cached ID
        if connector_name_or_id in self._connector_name_to_id_cache:
            return self._connector_name_to_id_cache[connector_name_or_id]

        # List all connectors to find the ID by name
        try:
            connectors = self.list_connectors()

            # First check for exact name match
            for connector in connectors:
                if connector.get("name") == connector_name_or_id:
                    connector_id = connector.get("id")
                    if connector_id:
                        # Cache the mapping for future use
                        self._connector_name_to_id_cache[connector_name_or_id] = (
                            connector_id
                        )
                        return connector_id

            # If no exact match, try case-insensitive match
            for connector in connectors:
                if connector.get("name", "").lower() == connector_name_or_id.lower():
                    connector_id = connector.get("id")
                    if connector_id:
                        # Cache the mapping for future use
                        self._connector_name_to_id_cache[connector_name_or_id] = (
                            connector_id
                        )
                        return connector_id

            # Log diagnostic info if not found
            logger.warning(
                f"Could not find connector with name: {connector_name_or_id}"
            )
            logger.debug(f"Available connectors: {[c.get('name') for c in connectors]}")
            return None

        except Exception as e:
            logger.warning(f"Error trying to find connector ID by name: {e}")
            return None

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
            # Get configuration details for the connector
            config_response = self._make_request(
                "GET", f"/connectors/{connector_id}/config"
            )
            config = config_response.get("data", {})

            # Add additional info to connector data
            connector_data["config"] = config

            return connector_data
        except Exception as e:
            logger.warning(
                f"Failed to get additional details for connector {connector_id}: {e}"
            )
            return connector_data

    def get_table_columns(
        self, connector_id: str, schema_name: str, table_name: str
    ) -> List[Dict]:
        """
        Get detailed column information for a specific table using the direct columns endpoint.

        Args:
            connector_id: The Fivetran connector ID
            schema_name: The schema name
            table_name: The table name to find columns for

        Returns:
            List of column dictionaries with name, type, and other properties
        """
        try:
            # Check if we've already cached these columns
            cache_key = f"{connector_id}:{schema_name}.{table_name}"
            if cache_key in self._column_cache:
                logger.debug(f"Using cached columns for {schema_name}.{table_name}")
                return self._column_cache[cache_key]

            # URL-encode the schema name and table name to handle special characters
            import urllib.parse

            encoded_schema = urllib.parse.quote(schema_name)
            encoded_table = urllib.parse.quote(table_name)

            logger.info(
                f"Fetching column info for {schema_name}.{table_name} using direct columns endpoint"
            )

            # Make direct request to the columns endpoint
            url = f"/connectors/{connector_id}/schemas/{encoded_schema}/tables/{encoded_table}/columns"
            response = self._make_request("GET", url)

            # Extract columns from the response
            columns_data = response.get("data", {}).get("columns", [])

            # Cache the columns
            self._column_cache[cache_key] = columns_data

            logger.info(
                f"Retrieved {len(columns_data)} columns directly from columns endpoint for {schema_name}.{table_name}"
            )
            return columns_data

        except Exception as e:
            logger.warning(
                f"Failed to get columns from direct endpoint for {schema_name}.{table_name}: {e}"
            )
            return []

    def list_connector_schemas(self, connector_id: str) -> List[Dict]:
        """
        Get schema information for a connector with improved error handling and format normalization.
        """
        if connector_id in self._schema_cache:
            return self._schema_cache[connector_id]

        try:
            # First, try the standard schema endpoint
            response = self._make_request("GET", f"/connectors/{connector_id}/schemas")

            logger.debug(f"Schema response for connector {connector_id}: {response}")

            # Process raw schemas based on format received
            raw_schemas = response.get("data", {}).get("schemas", [])
            schemas = self._normalize_schema_format(raw_schemas, connector_id)

            # If schemas are empty, try alternate schema retrieval methods
            if not schemas:
                logger.info(
                    f"No schemas found from primary endpoint for {connector_id}, trying alternate methods"
                )
                schemas = self._try_alternate_schema_methods(connector_id)

            # Cache and return the results
            self._schema_cache[connector_id] = schemas
            logger.info(
                f"Processed {len(schemas)} schemas for connector {connector_id}"
            )

            # Ensure we have column information
            if schemas:
                self._ensure_column_information(schemas, connector_id)

            return schemas

        except Exception as e:
            logger.warning(
                f"Error fetching schemas for connector {connector_id}: {e}",
                exc_info=True,
            )
            # Return an empty but well-structured schema list that can be used by the lineage builder
            return []

    def _normalize_schema_format(
        self, raw_schemas: Any, connector_id: str
    ) -> List[Dict]:
        """
        Normalize schema information into a consistent format regardless of API response structure.
        """
        schemas = []

        # Log what we're working with
        logger.debug(f"Raw schema response type: {type(raw_schemas)}")
        if isinstance(raw_schemas, dict):
            logger.debug(f"Schema keys: {list(raw_schemas.keys())}")
        elif isinstance(raw_schemas, list):
            logger.debug(f"Schema list length: {len(raw_schemas)}")
            if raw_schemas:
                logger.debug(f"First schema item type: {type(raw_schemas[0])}")
        else:
            logger.debug(f"Unexpected schema format: {str(raw_schemas)[:100]}...")

        # Handle different response formats
        if isinstance(raw_schemas, dict):
            # Handle nested object format (older API versions)
            logger.info(f"Converting nested schema format for connector {connector_id}")
            for schema_name, schema_data in raw_schemas.items():
                # Convert to the expected format
                schema_obj = {
                    "name": schema_name,
                    "name_in_destination": schema_data.get(
                        "name_in_destination", schema_name
                    ),
                    "enabled": schema_data.get("enabled", True),
                    "tables": [],
                }

                # Convert tables from dict to list format
                tables_dict = schema_data.get("tables", {})
                if isinstance(tables_dict, dict):
                    for table_name, table_data in tables_dict.items():
                        table_obj = {
                            "name": table_name,
                            "name_in_destination": table_data.get(
                                "name_in_destination", table_name
                            ),
                            "enabled": table_data.get("enabled", False),
                        }

                        # Handle columns if present
                        columns_dict = table_data.get("columns", {})
                        columns = []
                        if isinstance(columns_dict, dict):
                            for column_name, column_data in columns_dict.items():
                                column_obj = {
                                    "name": column_name,
                                    "name_in_destination": column_data.get(
                                        "name_in_destination", column_name
                                    ),
                                    "enabled": column_data.get("enabled", True),
                                    "type": column_data.get("type", ""),
                                }
                                columns.append(column_obj)
                        elif isinstance(columns_dict, list):
                            columns = columns_dict

                        if columns:
                            table_obj["columns"] = columns

                        schema_obj["tables"].append(table_obj)

                schemas.append(schema_obj)
        elif isinstance(raw_schemas, list):
            # Already in the expected list format
            schemas = raw_schemas

            # Ensure each schema has the expected structure
            for schema in schemas:
                if "tables" not in schema:
                    schema["tables"] = []

                for table in schema.get("tables", []):
                    if "columns" not in table:
                        table["columns"] = []
        else:
            logger.warning(
                f"Unexpected schema format type for connector {connector_id}: {type(raw_schemas)}"
            )
            schemas = []

        return schemas

    def _try_alternate_schema_methods(self, connector_id: str) -> List[Dict]:
        """
        Try alternate methods to retrieve schema information when the primary method fails.
        """
        schemas = []

        try:
            # Try connector details endpoint, which sometimes has schema information
            connector_details = self.get_connector_details(connector_id)

            # Check if we have config with schema info
            if "config" in connector_details:
                config = connector_details.get("config", {})
                schema_config = config.get("schema", {}) or config.get("schemas", {})

                if schema_config:
                    logger.info(
                        f"Found schema information in connector config for {connector_id}"
                    )
                    # Try to extract schema information from config
                    schemas = self._extract_schemas_from_config(
                        schema_config, connector_id
                    )

            # If we still don't have schemas, try metadata endpoint if available
            if not schemas:
                try:
                    metadata_response = self._make_request(
                        "GET", f"/connectors/{connector_id}/metadata"
                    )
                    schemas = self._extract_schemas_from_metadata(
                        metadata_response, connector_id
                    )
                except Exception as metadata_err:
                    logger.debug(
                        f"No metadata endpoint for {connector_id}: {metadata_err}"
                    )

            # Try to retrieve information about connector setup itself
            if not schemas:
                try:
                    setup_response = self._make_request(
                        "GET", f"/connectors/{connector_id}/setup_tests"
                    )
                    if "data" in setup_response and "results" in setup_response["data"]:
                        for result in setup_response["data"]["results"]:
                            if (
                                "source" in result.get("title", "").lower()
                                and "schema" in result
                            ):
                                logger.info(
                                    f"Found schema info in setup tests for {connector_id}"
                                )
                                source_schema = result.get("schema", {})
                                schemas = self._extract_schemas_from_setup(
                                    source_schema, connector_id
                                )
                except Exception as setup_err:
                    logger.debug(
                        f"No setup test information for {connector_id}: {setup_err}"
                    )

        except Exception as e:
            logger.warning(
                f"Error in alternate schema retrieval for {connector_id}: {e}"
            )

        return schemas

    def _extract_schemas_from_config(
        self, schema_config: Dict, connector_id: str
    ) -> List[Dict]:
        """
        Extract schema information from connector config.
        """
        schemas = []
        try:
            # Handle various config formats we might encounter
            if isinstance(schema_config, dict):
                # Format: {"schema_name": {"tables": {"table_name": {...}}}}
                for schema_name, schema_data in schema_config.items():
                    schema_obj = {"name": schema_name, "tables": []}

                    tables = schema_data.get("tables", {})
                    if isinstance(tables, dict):
                        for table_name, table_data in tables.items():
                            # Skip if table is explicitly disabled
                            if table_data.get("enabled") is False:
                                continue

                            table_obj = {
                                "name": table_name,
                                "enabled": True,
                                "columns": [],
                            }

                            # Extract column information if available
                            columns = table_data.get("columns", {})
                            if isinstance(columns, dict):
                                for col_name, col_data in columns.items():
                                    if col_data.get("enabled") is False:
                                        continue
                                    table_obj["columns"].append(
                                        {
                                            "name": col_name,
                                            "enabled": True,
                                            "type": col_data.get("type", ""),
                                        }
                                    )

                            schema_obj["tables"].append(table_obj)

                    schemas.append(schema_obj)

            logger.info(
                f"Extracted {len(schemas)} schemas from config for connector {connector_id}"
            )

        except Exception as e:
            logger.warning(
                f"Error extracting schemas from config for {connector_id}: {e}"
            )

        return schemas

    def _extract_schemas_from_metadata(
        self, metadata_response: Dict, connector_id: str
    ) -> List[Dict]:
        """
        Extract schema information from connector metadata response.
        """
        schemas: List[Dict] = []
        try:
            # Try to extract from various metadata formats
            metadata = metadata_response.get("data", {})

            # If metadata contains schema information directly
            if "schemas" in metadata:
                raw_schemas = metadata.get("schemas", [])
                return self._normalize_schema_format(raw_schemas, connector_id)

            # If metadata contains source_objects which might have schema information
            if "source_objects" in metadata:
                source_objects = metadata.get("source_objects", [])
                schema_obj = {"name": "default", "tables": []}

                for obj in source_objects:
                    if isinstance(obj, dict) and "name" in obj:
                        table_name = obj.get("name")
                        schema_name = obj.get("schema", "default")

                        # If we found a schema name, update or create a schema object
                        if schema_name != "default":
                            schema_obj = next(
                                (s for s in schemas if s["name"] == schema_name),
                                {"name": schema_name, "tables": []},
                            )
                            if not schema_obj:
                                schema_obj = {"name": schema_name, "tables": []}
                                schemas.append(schema_obj)

                        table_obj: Dict[str, Any] = {
                            "name": table_name,
                            "enabled": True,
                            "columns": [],
                        }

                        # Extract column information if available
                        if "columns" in obj:
                            for col in obj.get("columns", []):
                                if isinstance(col, dict) and "name" in col:
                                    table_obj["columns"].append(
                                        {
                                            "name": col.get("name"),
                                            "enabled": True,
                                            "type": col.get("type", ""),
                                        }
                                    )

                        if isinstance(schema_obj["tables"], list):
                            schema_obj["tables"].append(table_obj)

                # Add the default schema if it has tables and isn't already in schemas
                if schema_obj["name"] == "default" and schema_obj["tables"]:
                    schemas.append(schema_obj)

            logger.info(
                f"Extracted {len(schemas)} schemas from metadata for connector {connector_id}"
            )

        except Exception as e:
            logger.warning(
                f"Error extracting schemas from metadata for {connector_id}: {e}"
            )

        return schemas

    def _extract_schemas_from_setup(
        self, setup_schema: Dict, connector_id: str
    ) -> List[Dict]:
        """
        Extract schema information from connector setup test results.
        """
        schemas = []
        try:
            # Setup tests might contain information about available schemas and tables
            if isinstance(setup_schema, dict):
                for schema_name, tables in setup_schema.items():
                    schema_obj = {"name": schema_name, "tables": []}

                    if isinstance(tables, list):
                        for table_name in tables:
                            table_obj = {
                                "name": table_name,
                                "enabled": True,
                                "columns": [],  # We likely won't have column info here
                            }
                            schema_obj["tables"].append(table_obj)

                    schemas.append(schema_obj)

            logger.info(
                f"Extracted {len(schemas)} schemas from setup tests for connector {connector_id}"
            )

        except Exception as e:
            logger.warning(
                f"Error extracting schemas from setup tests for {connector_id}: {e}"
            )

        return schemas

    def _ensure_column_information(
        self, schemas: List[Dict], connector_id: str
    ) -> None:
        """
        Ensure we have column information for tables by fetching additional details if needed.
        Uses multiple strategies to get complete column information:
        1. Check existing schema data first
        2. Try dedicated table API endpoint for tables missing columns
        3. Attempt to infer columns from metadata if available
        """
        # First identify which tables need column information
        tables_missing_columns, tables_with_columns, total_tables = (
            self._identify_tables_missing_columns(schemas)
        )

        # Log statistics about column availability
        logger.info(
            f"Column information stats for connector {connector_id}: "
            f"{tables_with_columns} tables have columns, "
            f"{len(tables_missing_columns)} tables missing columns, "
            f"out of {total_tables} total tables"
        )

        if not tables_missing_columns:
            return

        # Process tables in batches
        self._process_tables_in_batches(tables_missing_columns, connector_id)

        # Count how many tables still don't have column info for
        tables_still_missing = self._count_tables_still_missing_columns(schemas)

        # Try bulk retrieval approach as a final fallback for any remaining tables
        if tables_still_missing > 0:
            logger.info(
                f"Still missing columns for {tables_still_missing} tables. Attempting bulk schema retrieval as final fallback."
            )
            self._try_bulk_column_retrieval(connector_id, schemas)

        # Final count of tables still missing column information
        tables_still_missing = self._count_tables_still_missing_columns(schemas)
        logger.info(
            f"After all retrieval attempts, {tables_still_missing} tables still missing column information"
        )

    def _identify_tables_missing_columns(
        self, schemas: List[Dict]
    ) -> Tuple[List[Dict], int, int]:
        """
        Identify tables that are missing column information.

        Returns:
            Tuple containing:
            - List of tables missing columns (with schema, table name, and table object)
            - Count of tables that have columns
            - Total number of tables
        """
        tables_missing_columns = []
        tables_with_columns = 0
        total_tables = 0

        for schema in schemas:
            schema_name = schema.get("name", "")
            for table in schema.get("tables", []):
                total_tables += 1
                table_name = table.get("name", "")

                # Skip tables that aren't enabled
                if not table.get("enabled", True):
                    continue

                # Check if table has column information
                columns = table.get("columns", [])
                if not columns:
                    # Add to list of tables needing column info
                    tables_missing_columns.append(
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "table_obj": table,  # Keep reference to the table object for updates
                        }
                    )
                else:
                    tables_with_columns += 1

        return tables_missing_columns, tables_with_columns, total_tables

    def _count_tables_still_missing_columns(self, schemas: List[Dict]) -> int:
        """Count how many tables still don't have column information."""
        tables_still_missing = 0
        for schema in schemas:
            for table in schema.get("tables", []):
                if table.get("enabled", True) and not table.get("columns"):
                    tables_still_missing += 1
        return tables_still_missing

    def _process_tables_in_batches(
        self, tables_missing_columns: List[Dict], connector_id: str
    ) -> None:
        """Process tables in batches to avoid overwhelming the API."""
        batch_size = 20
        total_batches = (len(tables_missing_columns) + batch_size - 1) // batch_size

        # Add retry mechanism for API calls
        max_retries = 3
        retry_delay = 5  # seconds

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(tables_missing_columns))
            batch = tables_missing_columns[start_idx:end_idx]

            logger.info(
                f"Processing batch {batch_num + 1}/{total_batches}: "
                f"Fetching column information for {len(batch)} tables "
                f"(tables {start_idx + 1}-{end_idx} out of {len(tables_missing_columns)} missing column info)"
            )

            # Process each table in the batch
            batch_success = self._process_batch(
                batch, connector_id, max_retries, retry_delay
            )

            # Add a small delay between batches to avoid rate limiting
            if batch_num < total_batches - 1:
                logger.info(
                    f"Batch {batch_num + 1} complete with {batch_success}/{len(batch)} successful retrievals. Pausing briefly before next batch."
                )
                time.sleep(2)  # 2 second pause between batches

    def _process_batch(
        self, batch: List[Dict], connector_id: str, max_retries: int, retry_delay: int
    ) -> int:
        """
        Process a batch of tables to retrieve column information.

        Returns:
            Number of tables successfully updated with column information.
        """
        batch_success = 0

        for table_info in batch:
            schema_name = table_info["schema"]
            table_name = table_info["table"]
            table_obj = table_info["table_obj"]

            # Try to get columns with retry logic
            if self._get_columns_with_retry(
                connector_id,
                schema_name,
                table_name,
                table_obj,
                max_retries,
                retry_delay,
            ):
                batch_success += 1

        return batch_success

    def _get_columns_with_retry(
        self,
        connector_id: str,
        schema_name: str,
        table_name: str,
        table_obj: Dict,
        max_retries: int,
        retry_delay: int,
    ) -> bool:
        """
        Attempt to get columns for a table with retry logic.

        Returns:
            True if columns were successfully retrieved, False otherwise.
        """
        for retry in range(max_retries):
            try:
                # Get columns using the dedicated table API endpoint
                columns = self.get_table_columns(connector_id, schema_name, table_name)

                if columns:
                    # Update the table object directly with these columns
                    table_obj["columns"] = columns
                    logger.info(
                        f"Updated {schema_name}.{table_name} with {len(columns)} columns from table API"
                    )
                    return True

                elif retry < max_retries - 1:
                    logger.warning(
                        f"No columns returned for {schema_name}.{table_name}, retrying ({retry + 1}/{max_retries})..."
                    )
                    time.sleep(retry_delay)
                else:
                    # If API doesn't return columns after all retries, try to infer from metadata
                    logger.warning(
                        f"Could not get columns for {schema_name}.{table_name} from API after {max_retries} attempts, trying fallback methods"
                    )
                    if self._try_fallback_column_methods(
                        connector_id, schema_name, table_name, table_obj
                    ):
                        return True

            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(
                        f"Error fetching columns for {schema_name}.{table_name}, retrying ({retry + 1}/{max_retries}): {e}"
                    )
                    time.sleep(retry_delay)
                else:
                    logger.warning(
                        f"Failed to get columns for {schema_name}.{table_name} after {max_retries} attempts: {e}"
                    )
                    # Try fallback methods
                    if self._try_fallback_column_methods(
                        connector_id, schema_name, table_name, table_obj
                    ):
                        return True

        return False

    def _try_fallback_column_methods(
        self, connector_id: str, schema_name: str, table_name: str, table_obj: Dict
    ) -> bool:
        """
        Try fallback methods to get column information for a table.

        Returns:
            True if columns were successfully retrieved, False otherwise.
        """
        # Try metadata approach first
        if self._try_metadata_approach(
            connector_id, schema_name, table_name, table_obj
        ):
            return True

        # If metadata approach failed, try similar table approach
        if self._try_infer_columns_from_similar_tables(
            connector_id, schema_name, table_name, table_obj
        ):
            return True

        return False

    def _try_metadata_approach(
        self, connector_id: str, schema_name: str, table_name: str, table_obj: Dict
    ) -> bool:
        """
        Try to get column information from connector metadata.

        Returns:
            True if columns were successfully retrieved, False otherwise.
        """
        try:
            metadata_path = f"/connectors/{connector_id}/metadata"
            metadata_response = self._make_request("GET", metadata_path)
            metadata = metadata_response.get("data", {})

            # Look for column information in metadata
            source_objects = metadata.get("source_objects", [])
            for obj in source_objects:
                if (
                    isinstance(obj, dict)
                    and obj.get("name") == table_name
                    and obj.get("schema") == schema_name
                ):
                    metadata_columns = obj.get("columns", [])
                    if metadata_columns:
                        # Convert to our expected format
                        formatted_columns = []
                        for col in metadata_columns:
                            if isinstance(col, dict) and "name" in col:
                                formatted_columns.append(
                                    {
                                        "name": col["name"],
                                        "type": col.get("type", ""),
                                        "enabled": True,
                                    }
                                )

                        if formatted_columns:
                            table_obj["columns"] = formatted_columns
                            logger.info(
                                f"Inferred {len(formatted_columns)} columns for {schema_name}.{table_name} from metadata"
                            )
                            return True
        except Exception as e:
            logger.warning(f"Failed to get metadata for {connector_id}: {e}")

        return False

    def _try_infer_columns_from_similar_tables(
        self, connector_id: str, schema_name: str, table_name: str, table_obj: Dict
    ) -> bool:
        """
        Try to infer columns by looking for similar tables in the same schema.

        Returns:
            True if columns were successfully retrieved, False otherwise.
        """
        try:
            # Try to get schema information again with focus on this schema
            schemas_response = self._make_request(
                "GET", f"/connectors/{connector_id}/schemas"
            )
            schemas = schemas_response.get("data", {}).get("schemas", [])

            if not schemas:
                return False

            # First look for exact matches in different schemas
            for schema in schemas:
                schema_name_check = schema.get("name", "")
                if schema_name_check != schema_name:  # Different schema
                    for table in schema.get("tables", []):
                        if table.get("name") == table_name and table.get("columns"):
                            # Found exact name match in different schema
                            table_obj["columns"] = table.get("columns", [])
                            logger.info(
                                f"Used columns from exact name match in different schema {schema_name_check} for {table_name}"
                            )
                            return True

            # Then look for similar tables in same schema
            target_schema = next(
                (s for s in schemas if s.get("name") == schema_name), None
            )
            if not target_schema:
                return False

            import difflib

            best_match = None
            best_score = 0.7  # Minimum similarity threshold
            best_columns = []

            for table in target_schema.get("tables", []):
                if not table.get("columns"):
                    continue

                check_name = table.get("name", "")
                if check_name == table_name:
                    continue

                # Calculate similarity between table names
                similarity = difflib.SequenceMatcher(
                    None, table_name, check_name
                ).ratio()
                if similarity > best_score:
                    best_match = check_name
                    best_score = similarity
                    best_columns = table.get("columns", [])

            if best_columns:
                table_obj["columns"] = best_columns
                logger.info(
                    f"Inferred {len(best_columns)} columns for {schema_name}.{table_name} from similar table {best_match} (similarity: {best_score:.2f})"
                )
                return True
        except Exception as e:
            logger.warning(f"Failed to infer columns from similar tables: {e}")

        return False

    def _try_bulk_column_retrieval(
        self, connector_id: str, schemas: List[Dict]
    ) -> None:
        """Attempt to retrieve column information in bulk by fetching all schema information at once"""
        try:
            # Make a single request to get all schemas with their tables and columns
            response = self._make_request(
                "GET",
                f"/connectors/{connector_id}/schemas",
                params={"include_columns": "true"},
            )
            api_schemas = response.get("data", {}).get("schemas", [])

            if not api_schemas:
                logger.warning("Bulk schema retrieval returned no schemas")
                return

            # Create a lookup map for quick access
            schema_table_columns = {}
            for api_schema in api_schemas:
                schema_name = api_schema.get("name")
                if not schema_name:
                    continue

                for api_table in api_schema.get("tables", []):
                    table_name = api_table.get("name")
                    if not table_name:
                        continue

                    key = f"{schema_name}.{table_name}"
                    schema_table_columns[key] = api_table.get("columns", [])

            # Update our schemas with the retrieved column information
            tables_updated = 0
            for schema in schemas:
                schema_name = schema.get("name", "")
                for table in schema.get("tables", []):
                    table_name = table.get("name", "")
                    if not table_name or table.get("columns"):
                        continue  # Skip if already has columns

                    key = f"{schema_name}.{table_name}"
                    if key in schema_table_columns and schema_table_columns[key]:
                        table["columns"] = schema_table_columns[key]
                        tables_updated += 1

            logger.info(
                f"Bulk retrieval updated {tables_updated} tables with column information"
            )
        except Exception as e:
            logger.warning(f"Bulk schema retrieval failed: {e}")

    def list_users(self) -> List[Dict]:
        """Get all users in the Fivetran account."""
        response = self._make_request("GET", "/users")
        return response.get("data", {}).get("items", [])

    def _get_group_id_by_name_or_id(self, group_name_or_id: str) -> Optional[str]:
        """
        Find the group ID by its name or verify that an ID is valid.

        Args:
            group_name_or_id: Either a group name or ID

        Returns:
            The group ID if found, or None if not found
        """
        # Check if this is already an ID that exists
        try:
            # Try to get the group directly by ID first
            self._make_request("GET", f"/groups/{group_name_or_id}")
            # If successful, it's a valid ID
            logger.debug(f"Confirmed {group_name_or_id} is a valid group ID")
            return group_name_or_id
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                # For errors other than 404, re-raise
                raise
            # For 404, continue with name lookup
            logger.debug(
                f"{group_name_or_id} is not a valid group ID, trying as name lookup"
            )
            pass

        # If we get here, the ID wasn't valid, so try to find by name
        try:
            # List all groups to find the ID by name
            groups = self.list_groups()

            # First check for exact name match
            for group in groups:
                if group.get("name") == group_name_or_id:
                    group_id = group.get("id")
                    if group_id:
                        logger.info(
                            f"Found group ID {group_id} for name: {group_name_or_id}"
                        )
                        return group_id

            # If no exact match, try case-insensitive match
            for group in groups:
                if group.get("name", "").lower() == group_name_or_id.lower():
                    group_id = group.get("id")
                    if group_id:
                        logger.info(
                            f"Found group ID {group_id} for name (case-insensitive): {group_name_or_id}"
                        )
                        return group_id

            # If still no match, check if the value is a destination ID used in any connector
            connectors = self.list_connectors()
            for connector in connectors:
                group_field = connector.get("group", {})
                if (
                    isinstance(group_field, dict)
                    and group_field.get("id") == group_name_or_id
                ):
                    logger.info(
                        f"Confirmed {group_name_or_id} is a valid group ID referenced in connector"
                    )
                    return group_name_or_id

            logger.warning(f"Could not find group with name: {group_name_or_id}")
            return None

        except Exception as e:
            logger.warning(f"Error trying to find group ID by name: {e}")
            return None

    def get_destination_details(self, group_id: str) -> Dict[str, Any]:
        """Get details about a destination group with enhanced error handling and logging"""
        if not group_id:
            logger.warning("Empty group_id provided to get_destination_details")
            return {}

        # Check cache first
        if group_id in self._destination_cache:
            logger.debug(f"Using cached destination details for {group_id}")
            return self._destination_cache[group_id]

        try:
            # First, try to find the right group ID if this might be a name
            real_group_id = self._get_group_id_by_name_or_id(group_id)
            if not real_group_id:
                logger.warning(f"Could not find a valid group ID for: {group_id}")
                real_group_id = group_id  # Fall back to the original value
            elif real_group_id != group_id:
                logger.info(
                    f"Resolved group name/ID '{group_id}' to ID '{real_group_id}'"
                )
                group_id = real_group_id  # Use the resolved ID

            logger.debug(f"Fetching destination details for group ID: {group_id}")
            response = self._make_request("GET", f"/groups/{group_id}")
            destination_data = response.get("data", {})
            logger.debug(f"Raw destination data for {group_id}: {destination_data}")

            # Additional destination details
            try:
                # Try to get destination config
                logger.debug(f"Fetching config for destination {group_id}")
                config_response = self._make_request(
                    "GET", f"/groups/{group_id}/config"
                )
                config_data = config_response.get("data", {})
                logger.debug(f"Destination config for {group_id}: {config_data}")
                destination_data["config"] = config_data
            except Exception as config_e:
                logger.debug(
                    f"Could not get destination config for {group_id}: {config_e}"
                )
                # Continue without config data

            # Check for essential destination info
            if "service" in destination_data:
                logger.info(
                    f"Destination {group_id} has service: {destination_data['service']}"
                )
            else:
                logger.warning(
                    f"No service field found in destination details for {group_id}"
                )
                # Try to infer from other fields
                if "name" in destination_data:
                    name = destination_data["name"].lower()
                    logger.debug(f"Checking destination name for clues: {name}")
                    if "bigquery" in name:
                        logger.info(f"Found 'bigquery' in destination name: {name}")
                        destination_data["service"] = "bigquery"
                    elif "snowflake" in name:
                        logger.info(f"Found 'snowflake' in destination name: {name}")
                        destination_data["service"] = "snowflake"

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
        if not group_id:
            logger.warning("Empty group_id provided to detect_destination_platform")
            return "snowflake"  # Default if no group_id is provided

        try:
            # First, verify that we have the correct ID
            real_group_id = self._get_group_id_by_name_or_id(group_id)
            if real_group_id and real_group_id != group_id:
                logger.info(
                    f"Using resolved group ID {real_group_id} instead of {group_id}"
                )
                group_id = real_group_id

            destination = self.get_destination_details(group_id)
            logger.debug(f"Destination details for {group_id}: {destination}")

            # Get the destination service if available
            service = destination.get("service", "")
            logger.debug(f"Service value from API: '{service}'")

            # Map Fivetran service names to DataHub platform names
            if service:
                service = service.lower()
                logger.debug(f"Lowercase service: '{service}'")

                if "snowflake" in service:
                    logger.info(
                        f"Detected Snowflake destination from service '{service}'"
                    )
                    return "snowflake"
                elif "bigquery" in service:
                    logger.info(
                        f"Detected BigQuery destination from service '{service}'"
                    )
                    return "bigquery"
                elif "redshift" in service:
                    logger.info(
                        f"Detected Redshift destination from service '{service}'"
                    )
                    return "redshift"
                elif "postgres" in service:
                    logger.info(
                        f"Detected Postgres destination from service '{service}'"
                    )
                    return "postgres"
                elif "mysql" in service:
                    logger.info(f"Detected MySQL destination from service '{service}'")
                    return "mysql"
                elif "databricks" in service:
                    logger.info(
                        f"Detected Databricks destination from service '{service}'"
                    )
                    return "databricks"
                elif "synapse" in service:
                    logger.info(
                        f"Detected Synapse destination from service '{service}'"
                    )
                    return "synapse"
                elif "azure_sql_database" in service:
                    logger.info(
                        f"Detected Azure SQL destination from service '{service}'"
                    )
                    return "mssql"
                else:
                    logger.warning(
                        f"Unknown service type: '{service}', defaulting to snowflake"
                    )
            else:
                logger.warning(
                    f"No service field found in destination details for {group_id}"
                )

            # Check for other clues in the destination details if service is not available
            if "config" in destination:
                config = destination.get("config", {})
                logger.debug(f"Destination config: {config}")

                # Look for platform-specific fields
                if "warehouse" in config:
                    logger.info("Found 'warehouse' in config, likely Snowflake")
                    return "snowflake"
                elif "dataset" in config:
                    logger.info("Found 'dataset' in config, likely BigQuery")
                    return "bigquery"

            logger.warning(
                f"Could not determine platform for destination {group_id}, defaulting to snowflake"
            )
            return "snowflake"  # Default to snowflake if detection failed
        except Exception as e:
            logger.warning(f"Error in detect_destination_platform for {group_id}: {e}")
            return "snowflake"

    def get_destination_database(self, group_id: str) -> str:
        """Get the database name for a destination."""
        # First, verify that we have the correct ID
        real_group_id = self._get_group_id_by_name_or_id(group_id)
        if real_group_id and real_group_id != group_id:
            logger.info(
                f"Using resolved group ID {real_group_id} instead of {group_id}"
            )
            group_id = real_group_id

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
        if not user_id:
            logger.warning("Empty user_id provided to get_user")
            return {}

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
        """
        Get the sync history for a connector with improved error handling.

        This method first tries to use the connector ID and falls back to
        finding the ID by name if needed. It also tries multiple endpoints
        to maximize the chance of finding history data.
        """
        try:
            # Ensure we have a valid connector ID
            valid_connector_id = self.get_connector_id_by_name(connector_id)
            if not valid_connector_id:
                logger.warning(
                    f"Could not find a valid ID for connector: {connector_id}"
                )
                valid_connector_id = (
                    connector_id  # Use what we were given as a last resort
                )

            # Calculate the since timestamp (days ago)
            since_date = datetime.now() - timedelta(days=days)
            since_timestamp = int(since_date.timestamp())

            logger.info(
                f"Getting sync history for connector {valid_connector_id} from the past {days} days"
            )

            # First try: Use the sync-history endpoint (most reliable)
            try:
                response = self._make_request(
                    "GET",
                    f"/connectors/{valid_connector_id}/sync-history",
                    params={"limit": 100, "since": since_timestamp},
                )
                sync_items = response.get("data", {}).get("items", [])

                if sync_items:
                    logger.info(
                        f"Found {len(sync_items)} sync history records via sync-history endpoint"
                    )
                    return sync_items

                logger.info(
                    "No sync history found via sync-history endpoint, trying logs endpoint"
                )
            except Exception as e:
                logger.warning(f"Error fetching from sync-history endpoint: {e}")

            # Second try: Use the logs endpoint
            try:
                # Format as ISO string for the logs API
                since_str = f"{days}d"

                response = self._make_request(
                    "GET",
                    f"/connectors/{valid_connector_id}/logs",
                    params={"limit": 100, "since": since_str},
                )
                logs = response.get("data", {}).get("items", [])

                if logs:
                    logger.info(
                        f"Found {len(logs)} logs, extracting sync history from them"
                    )
                    # Process logs to extract sync events and build history
                    sync_history = self._extract_sync_history_from_logs(logs)
                    if sync_history:
                        logger.info(
                            f"Extracted {len(sync_history)} sync history items from logs"
                        )
                        return sync_history

                logger.info("No usable sync history found in logs")
            except Exception as e:
                logger.warning(f"Error fetching from logs endpoint: {e}")

            # Third try: Use connector metadata (last resort)
            try:
                connector_details = self.get_connector(valid_connector_id)

                # Check for last sync timestamp fields
                if (
                    "succeeded_at" in connector_details
                    or "failed_at" in connector_details
                    or "sync_state" in connector_details
                ):
                    logger.info(
                        "Creating synthetic sync history from connector metadata"
                    )

                    # Determine status and timestamp
                    status = "COMPLETED"
                    timestamp = None

                    if "succeeded_at" in connector_details:
                        timestamp = connector_details.get("succeeded_at")
                    elif "failed_at" in connector_details:
                        timestamp = connector_details.get("failed_at")
                        status = "FAILED"
                    elif "sync_state" in connector_details:
                        sync_state = connector_details.get("sync_state", {})
                        if isinstance(sync_state, dict):
                            if "last_sync" in sync_state:
                                timestamp = sync_state.get("last_sync")
                            if "status" in sync_state:
                                state_status = sync_state.get("status", "").upper()
                                if state_status in ["FAILED", "ERROR"]:
                                    status = "FAILED"
                                elif state_status in ["CANCELLED", "CANCELED"]:
                                    status = "CANCELLED"

                    if timestamp:
                        # Create synthetic history entry
                        sync_id = f"{valid_connector_id}-latest-{int(time.time())}"
                        return [
                            {
                                "id": sync_id,
                                "started_at": timestamp,  # Use same timestamp for both
                                "completed_at": timestamp,
                                "status": status,
                            }
                        ]
            except Exception as e:
                logger.warning(f"Error creating synthetic history from metadata: {e}")

            # If we got here, we couldn't find any history
            logger.warning(
                f"No sync history found for connector {connector_id} after trying all methods"
            )
            return []

        except Exception as e:
            logger.error(
                f"Failed to get sync history for connector {connector_id}: {e}"
            )
            return []

    def _extract_sync_history_from_logs(self, logs: List[Dict]) -> List[Dict]:
        """Extract sync history entries from connector logs."""
        if not logs:
            return []

        # Group logs by sync_id
        sync_groups: Dict[str, List[Dict[str, Any]]] = {}
        for log in logs:
            if "sync_id" in log:
                sync_id = log.get("sync_id")
                if sync_id is not None:  # Ensure sync_id is not None
                    str_sync_id = str(sync_id)  # Convert to string to be safe
                    if str_sync_id not in sync_groups:
                        sync_groups[str_sync_id] = []
                    sync_groups[str_sync_id].append(log)

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
                elif "cancelled" in message or "canceled" in message:
                    end_log = log
                    end_log["status"] = "CANCELLED"

            # Only create history entry if we have both start and end
            if start_log and end_log:
                start_time = start_log.get("created_at")
                end_time = end_log.get("created_at")

                entry = {
                    "id": sync_id,
                    "started_at": start_time,
                    "completed_at": end_time,
                    "status": end_log.get("status", "UNKNOWN"),
                }

                sync_history.append(entry)

        return sync_history

    def extract_connector_metadata(
        self, api_connector: Dict, sync_history: List[Dict]
    ) -> Connector:
        """
        Convert API connector data to our internal Connector model with improved column lineage.
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

        # Enhanced destination ID detection
        destination_id = ""

        # Try different ways to get the destination ID
        group_field = api_connector.get("group", {})
        if isinstance(group_field, dict) and "id" in group_field:
            destination_id = group_field.get("id", "")
            logger.debug(f"Found destination_id={destination_id} from group.id")

        # Alternative fields if group.id doesn't exist
        if not destination_id:
            if "destination_id" in api_connector:
                destination_id = api_connector.get("destination_id", "")
                logger.debug(
                    f"Found destination_id={destination_id} from destination_id field"
                )

        if not destination_id:
            if "group_id" in api_connector:
                destination_id = api_connector.get("group_id", "")
                logger.debug(
                    f"Found destination_id={destination_id} from group_id field"
                )

        # Log the issue and create a fallback ID if still empty
        if not destination_id:
            logger.warning(
                f"Could not find destination ID for connector {connector_id}. Available fields: {list(api_connector.keys())}"
            )
            destination_id = f"destination_for_{connector_id}"
            logger.warning(f"Using generated destination ID: {destination_id}")

        # Get destination platform and database information with better logging
        # Try to detect the destination platform and database
        try:
            logger.debug(f"Detecting platform for destination ID: {destination_id}")
            destination_platform = self.detect_destination_platform(destination_id)

            # Log more details about why this platform was detected
            logger.info(
                f"API detected destination platform '{destination_platform}' for destination ID: {destination_id}"
            )

            destination_database = self.get_destination_database(destination_id)

            # Add destination info to properties with clear key names
            additional_properties["destination_platform"] = destination_platform
            if destination_database:
                additional_properties["destination_database"] = destination_database

        except Exception as e:
            logger.warning(
                f"Failed to detect destination details for {destination_id}: {e}"
            )
            # Default to snowflake if detection fails but log clearly that this is a fallback
            logger.warning(
                "Using fallback platform 'snowflake' due to detection failure"
            )
            additional_properties["destination_platform"] = "snowflake"

        # Add any other useful metadata from the API response
        for key, value in api_connector.items():
            if key not in [
                "id",
                "name",
                "service",
                "paused",
                "schedule",
                "group",
            ] and isinstance(value, (str, int, bool, float)):
                additional_properties[f"api.{key}"] = str(value)

        return Connector(
            connector_id=connector_id,
            connector_name=connector_name,
            connector_type=connector_service,
            paused=paused,
            sync_frequency=sync_frequency,
            destination_id=destination_id,
            user_id=api_connector.get("created_by", ""),
            lineage=[],  # Will be filled later by extract_table_lineage
            jobs=jobs,
            additional_properties=additional_properties,
        )

    def _extract_jobs_from_sync_history(self, sync_history: List[Dict]) -> List[Job]:
        """
        Extract jobs from sync history with improved error handling.
        Handles both enterprise and standard API formats.
        """
        jobs = []

        for job in sync_history:
            try:
                # Try different possible field names for start and end times
                started_at = None
                completed_at = None

                # Extract timestamps depending on available fields
                if "started_at" in job:
                    started_at = self._parse_api_timestamp(job.get("started_at"))
                elif "start_time" in job:
                    # Handle datetime object from enterprise mode
                    start_time = job.get("start_time")
                    if start_time is not None and hasattr(start_time, "timestamp"):
                        started_at = int(start_time.timestamp())
                    else:
                        started_at = self._parse_api_timestamp(start_time)

                if "completed_at" in job:
                    completed_at = self._parse_api_timestamp(job.get("completed_at"))
                elif "end_time" in job:
                    # Handle datetime object from enterprise mode
                    end_time = job.get("end_time")
                    if end_time is not None and hasattr(end_time, "timestamp"):
                        completed_at = int(end_time.timestamp())
                    else:
                        completed_at = self._parse_api_timestamp(end_time)

                # Get status from appropriate field
                status = self._map_job_status(
                    job.get("status"), job.get("end_message_data")
                )

                # Only include jobs with valid timestamps
                if started_at and completed_at:
                    jobs.append(
                        Job(
                            job_id=job.get("id", str(hash(str(job)))),
                            start_time=started_at,
                            end_time=completed_at,
                            status=status,
                        )
                    )
            except Exception as e:
                logger.warning(f"Error processing sync history entry: {e}")
                continue

        return jobs

    def _map_job_status(
        self, status: Optional[str], message_data: Optional[str]
    ) -> str:
        """Map various status representations to our standard statuses."""
        # Default to success if we can't determine
        if not status and not message_data:
            return "SUCCESSFUL"

        # First check explicit status
        if status:
            status_upper = status.upper()

            if status_upper in ["COMPLETED", "SUCCEEDED", "SUCCESS"]:
                return "SUCCESSFUL"
            elif status_upper in ["FAILED", "FAILURE", "ERROR"]:
                return "FAILURE_WITH_TASK"
            elif status_upper in ["CANCELLED", "CANCELED"]:
                return "CANCELED"

        # If no clear status, try to parse from message data
        if message_data:
            try:
                # Sometimes message_data is a JSON string
                if isinstance(message_data, str):
                    if message_data.startswith('"') and message_data.endswith('"'):
                        # Double-encoded JSON string
                        message_data = json.loads(message_data)

                    if isinstance(message_data, str):
                        # Try to parse as JSON
                        try:
                            message_obj = json.loads(message_data)
                            if (
                                isinstance(message_obj, dict)
                                and "status" in message_obj
                            ):
                                status_str = message_obj.get("status", "").upper()
                                if status_str == "SUCCESSFUL":
                                    return "SUCCESSFUL"
                                elif status_str in ["FAILURE", "FAILURE_WITH_TASK"]:
                                    return "FAILURE_WITH_TASK"
                                elif status_str == "CANCELED":
                                    return "CANCELED"
                        except json.JSONDecodeError:
                            # Not valid JSON, continue with string analysis
                            pass

                        # Try to infer from text
                        message_lower = message_data.lower() if message_data else ""
                        if "success" in message_lower:
                            return "SUCCESSFUL"
                        elif "fail" in message_lower or "error" in message_lower:
                            return "FAILURE_WITH_TASK"
                        elif "cancel" in message_lower:
                            return "CANCELED"

            except Exception as e:
                logger.warning(f"Error parsing message data: {e}")

        # Default to successful if we couldn't determine
        return "SUCCESSFUL"

    def _parse_api_timestamp(self, timestamp_value: Any) -> Optional[int]:
        """Parse a timestamp from API responses into Unix timestamp (seconds)."""
        if not timestamp_value:
            return None

        try:
            # If it's already a number, return it directly
            if isinstance(timestamp_value, (int, float)):
                return int(timestamp_value)

            # If it's a string, try to parse it
            if isinstance(timestamp_value, str):
                # Parse ISO format (2023-09-20T06:37:32.606Z)
                if "T" in timestamp_value:
                    dt = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
                    return int(dt.timestamp())

                # Try various other formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        dt = datetime.strptime(timestamp_value, fmt)
                        return int(dt.timestamp())
                    except ValueError:
                        continue

            logger.warning(f"Could not parse timestamp: {timestamp_value}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing timestamp {timestamp_value}: {e}")
            return None

    def extract_table_lineage(self, connector_id: str) -> List[TableLineage]:
        """
        Extract table and column lineage information for a connector.
        """
        try:
            # Get schemas to extract tables and columns
            schemas = self.list_connector_schemas(connector_id)
            if not schemas:
                logger.warning(
                    f"No schema information found for connector {connector_id}"
                )
                return []

            # Get destination platform information
            destination_platform = self._get_destination_platform_for_connector(
                connector_id
            )

            # Collect source columns information
            source_table_columns = self._collect_source_columns(schemas)

            # Process schemas to extract lineage
            lineage_list = self._process_schemas(
                schemas, destination_platform, source_table_columns
            )

            # Log statistics about what we found
            self._log_lineage_stats(lineage_list, connector_id)

            return lineage_list

        except Exception as e:
            logger.error(
                f"Failed to extract lineage for connector {connector_id}: {e}",
                exc_info=True,
            )
            return []

    def _get_destination_platform_for_connector(self, connector_id: str) -> str:
        """Get destination platform for a connector."""
        # Get destination information
        connector_details = self.get_connector(connector_id)
        group_id = None
        if "group" in connector_details and isinstance(
            connector_details["group"], dict
        ):
            group_id = connector_details["group"].get("id")

        # Detect destination platform
        destination_platform = "snowflake"  # Default
        if group_id:
            destination_platform = self.detect_destination_platform(group_id)
            logger.info(
                f"Detected destination platform: {destination_platform} for group {group_id}"
            )

        return destination_platform

    def _collect_source_columns(self, schemas: List[Dict]) -> Dict[str, Dict[str, str]]:
        """Collect all source columns from schemas."""
        source_table_columns: Dict[str, Dict[str, str]] = {}
        for schema in schemas:
            schema_name = schema.get("name", "")
            for table in schema.get("tables", []):
                if not isinstance(table, dict):
                    continue

                table_name = table.get("name", "")
                if not table_name:
                    continue

                # Create source table name
                source_table = f"{schema_name}.{table_name}"

                # Initialize column dictionary for this table
                source_table_columns[source_table] = {}

                # Collect columns
                columns = table.get("columns", [])
                if not columns:
                    continue

                if isinstance(columns, list):
                    # Process list of column objects
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = col.get("name")
                            # Only add entries with a valid string name
                            if col_name and isinstance(col_name, str):
                                col_type = col.get("type", "")
                                # Ensure col_type is a string
                                if not isinstance(col_type, str):
                                    col_type = (
                                        str(col_type) if col_type is not None else ""
                                    )
                                source_table_columns[source_table][col_name] = col_type
                elif isinstance(columns, dict):
                    # Process dictionary of columns
                    for col_name, col_data in columns.items():
                        # Skip if col_name is not a string
                        if not isinstance(col_name, str):
                            continue

                        col_type = ""
                        if isinstance(col_data, dict):
                            type_value = col_data.get("type", "")
                            # Ensure col_type is a string
                            col_type = str(type_value) if type_value is not None else ""
                        elif col_data is not None:
                            # Handle case where col_data is a direct type string
                            col_type = str(col_data)

                        source_table_columns[source_table][col_name] = col_type

        return source_table_columns

    def _process_schemas(
        self,
        schemas: List[Dict],
        destination_platform: str,
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[TableLineage]:
        """Process schemas to extract table lineage information."""
        lineage_list = []

        for schema in schemas:
            schema_name = schema.get("name", "")
            if not schema_name:
                continue

            # Use name_in_destination if available for schema
            schema_name_in_destination = schema.get("name_in_destination")

            # Get destination schema name based on platform
            dest_schema = self._get_destination_schema_name(
                schema_name, schema_name_in_destination, destination_platform
            )

            logger.debug(f"Processing schema {schema_name} -> {dest_schema}")

            # Process each table in the schema
            for table in schema.get("tables", []):
                lineage_entry = self._process_table_lineage(
                    table,
                    schema_name,
                    dest_schema,
                    destination_platform,
                    source_table_columns,
                )

                if lineage_entry:
                    lineage_list.append(lineage_entry)

        return lineage_list

    def _get_destination_schema_name(
        self,
        schema_name: str,
        schema_name_in_destination: Optional[str],
        destination_platform: str,
    ) -> str:
        """Get destination schema name based on source schema and platform."""
        if schema_name_in_destination:
            return schema_name_in_destination

        # Apply platform-specific transformations
        if destination_platform.lower() == "bigquery":
            return schema_name.lower()
        else:
            return schema_name.upper()

    def _process_table_lineage(
        self,
        table: Dict,
        schema_name: str,
        dest_schema: str,
        destination_platform: str,
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> Optional[TableLineage]:
        """Process a single table to extract lineage information."""
        if not isinstance(table, dict):
            return None

        table_name = table.get("name", "")
        if not table_name or not table.get("enabled", True):
            return None

        # Create source table identifier
        source_table = f"{schema_name}.{table_name}"

        # Get destination table name
        dest_table = self._get_destination_table_name(
            table_name, table.get("name_in_destination"), destination_platform
        )

        # Create full destination table name
        destination_table = f"{dest_schema}.{dest_table}"

        logger.debug(f"Creating lineage: {source_table} -> {destination_table}")

        # Extract column lineage
        column_lineage = self._extract_column_lineage(
            table, source_table, destination_platform, source_table_columns
        )

        # Create and return the lineage entry
        return TableLineage(
            source_table=source_table,
            destination_table=destination_table,
            column_lineage=column_lineage,
        )

    def _get_destination_table_name(
        self,
        table_name: str,
        table_name_in_destination: Optional[str],
        destination_platform: str,
    ) -> str:
        """Get destination table name based on source table and platform."""
        if table_name_in_destination:
            return table_name_in_destination

        # Apply platform-specific transformations
        if destination_platform.lower() == "bigquery":
            return table_name.lower()
        else:
            return table_name.upper()

    def _extract_column_lineage(
        self,
        table: Dict,
        source_table: str,
        destination_platform: str,
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[ColumnLineage]:
        """Extract column lineage for a table."""
        column_lineage = []
        columns = table.get("columns", [])

        # First try with columns from the table itself
        if columns:
            if isinstance(columns, list):
                column_lineage = self._process_column_list(
                    columns, destination_platform
                )
            elif isinstance(columns, dict):
                column_lineage = self._process_column_dict(
                    columns, destination_platform
                )
        # Try with source_table_columns as fallback
        elif source_table in source_table_columns:
            column_lineage = self._process_source_columns(
                source_table_columns[source_table], destination_platform
            )

        return column_lineage

    def _process_column_list(
        self, columns: List[Dict], destination_platform: str
    ) -> List[ColumnLineage]:
        """Process a list of column dictionaries."""
        column_lineage = []

        for column in columns:
            if not isinstance(column, dict):
                continue

            col_name = column.get("name", "")
            if not col_name or col_name.startswith("_fivetran"):
                continue

            # Get destination column name
            dest_col = column.get("name_in_destination", col_name)

            # Apply platform-specific transformations if not explicitly set
            if dest_col == col_name:
                dest_col = self._transform_column_name(col_name, destination_platform)

            # Log column mapping for debugging
            logger.debug(f"Column mapping: {col_name} -> {dest_col}")

            column_lineage.append(
                ColumnLineage(source_column=col_name, destination_column=dest_col)
            )

        return column_lineage

    def _process_column_dict(
        self, columns: Dict[str, Any], destination_platform: str
    ) -> List[ColumnLineage]:
        """Process a dictionary of columns."""
        column_lineage = []

        for col_name, col_data in columns.items():
            if col_name.startswith("_fivetran"):
                continue

            # Get destination column name - ensure it's always a string
            if isinstance(col_data, dict) and "name_in_destination" in col_data:
                # Get the name_in_destination value and ensure it's a string
                name_in_dest = col_data.get("name_in_destination")
                if name_in_dest is not None and isinstance(name_in_dest, str):
                    dest_col = name_in_dest
                else:
                    # Fall back to transformation if name_in_destination is not a valid string
                    dest_col = self._transform_column_name(
                        col_name, destination_platform
                    )
            else:
                # Apply platform-specific transformations
                dest_col = self._transform_column_name(col_name, destination_platform)

            # At this point, dest_col is guaranteed to be a string
            column_lineage.append(
                ColumnLineage(source_column=col_name, destination_column=dest_col)
            )

        return column_lineage

    def _process_source_columns(
        self, columns: Dict[str, str], destination_platform: str
    ) -> List[ColumnLineage]:
        """Process source columns from collected information."""
        column_lineage = []

        for col_name in columns:
            if col_name.startswith("_fivetran"):
                continue

            # Apply platform-specific transformations
            dest_col = self._transform_column_name(col_name, destination_platform)

            column_lineage.append(
                ColumnLineage(source_column=col_name, destination_column=dest_col)
            )

        return column_lineage

    def _transform_column_name(self, col_name: str, destination_platform: str) -> str:
        """Transform column name based on destination platform."""
        if destination_platform.lower() == "bigquery":
            # Convert camelCase to snake_case for BigQuery
            import re

            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", col_name)
            s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
            return s2.lower()
        else:
            # For most other systems, uppercase
            return col_name.upper()

    def _log_lineage_stats(
        self, lineage_list: List[TableLineage], connector_id: str
    ) -> None:
        """Log statistics about lineage extraction."""
        total_tables = len(lineage_list)
        tables_with_column_lineage = sum(
            1 for entry in lineage_list if entry.column_lineage
        )
        total_column_mappings = sum(len(entry.column_lineage) for entry in lineage_list)

        logger.info(
            f"Extracted {total_tables} table lineage entries for connector {connector_id}: "
            f"{tables_with_column_lineage} tables with column mappings, "
            f"{total_column_mappings} total column mappings"
        )
