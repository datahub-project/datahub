# This is the improved implementation for fivetran_standard_api.py
# These modifications enhance column lineage extraction and job history in standard mode

import difflib
import logging
import re
from typing import Dict, List, Optional, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    ColumnNamingPattern,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran_access import FivetranAccessInterface
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_query import (
    MAX_JOBS_PER_CONNECTOR,
)

logger = logging.getLogger(__name__)


class FivetranStandardAPI(FivetranAccessInterface):
    """
    API adapter for the standard version of Fivetran using the REST API
    instead of querying log tables directly.
    """

    def __init__(
        self,
        api_client: FivetranAPIClient,
        config: Optional[FivetranSourceConfig] = None,
    ) -> None:
        """Initialize with a FivetranAPIClient instance."""
        self.api_client = api_client
        self.config = config

        # Cache for connector information
        self._connector_cache: Dict[str, Connector] = {}

        # Determine the fivetran_log_database from config if available
        self._fivetran_log_database = None
        if (
            self.config
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            if (
                hasattr(self.config.fivetran_log_config, "bigquery_destination_config")
                and self.config.fivetran_log_config.bigquery_destination_config
            ):
                self._fivetran_log_database = (
                    self.config.fivetran_log_config.bigquery_destination_config.dataset
                )
            elif (
                hasattr(self.config.fivetran_log_config, "snowflake_destination_config")
                and self.config.fivetran_log_config.snowflake_destination_config
            ):
                self._fivetran_log_database = self.config.fivetran_log_config.snowflake_destination_config.database

    @property
    def fivetran_log_database(self) -> Optional[str]:
        """Get the log database name from config if available."""
        return self._fivetran_log_database

    def get_user_email(self, user_id: str) -> Optional[str]:
        """Get a user's email from their user ID."""
        if not user_id:
            return None

        try:
            user_data = self.api_client.get_user(user_id)
            return user_data.get("email")
        except Exception as e:
            logger.warning(f"Failed to get user email for user ID {user_id}: {e}")
            return None

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> List[Connector]:
        """
        Get a list of connectors filtered by the provided patterns.
        """
        connectors: List[Connector] = []

        with report.metadata_extraction_perf.connectors_metadata_extraction_sec:
            logger.info("Fetching connector list from Fivetran API")
            connector_list = self.api_client.list_connectors()

            # Process each connector
            for api_connector in connector_list:
                connector_id = api_connector.get("id", "")
                if not connector_id:
                    continue

                connector_name = api_connector.get("name", "")

                # Apply connector pattern filter (skip explicitly included connectors)
                explicitly_included = False
                if (
                    self.config
                    and hasattr(self.config, "sources_to_platform_instance")
                    and connector_id in self.config.sources_to_platform_instance
                ):
                    explicitly_included = True
                    logger.info(
                        f"Connector {connector_name} (ID: {connector_id}) explicitly included via sources_to_platform_instance"
                    )

                if not explicitly_included and not connector_patterns.allowed(
                    connector_name
                ):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
                    )
                    continue

                # Get destination ID
                destination_id = self._extract_destination_id(api_connector)

                # Apply destination filter
                if not destination_patterns.allowed(destination_id):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
                    )
                    continue

                # Get sync history
                sync_history = self.api_client.list_connector_sync_history(
                    connector_id, syncs_interval
                )

                # Create connector object
                try:
                    connector = self.api_client.extract_connector_metadata(
                        api_connector, sync_history
                    )
                    connectors.append(connector)

                    # Cache for later use
                    self._connector_cache[connector_id] = connector

                    # Report scanned connector
                    report.report_connectors_scanned()

                except Exception as e:
                    logger.error(
                        f"Error extracting metadata for connector {connector_id}: {e}"
                    )
                    report.report_failure(
                        f"Error extracting metadata for connector {connector_id}: {e}"
                    )

        if not connectors:
            logger.info("No allowed connectors found")
            return []

        logger.info(f"Found {len(connectors)} allowed connectors")

        # Process lineage for all connectors
        with report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info("Extracting lineage from Fivetran API")
            for connector in connectors:
                try:
                    # Extract table-level and column-level lineage
                    lineage = self.api_client.extract_table_lineage(
                        connector.connector_id
                    )

                    # Ensure we have non-empty lineage and log it
                    if lineage:
                        logger.info(
                            f"Extracted {len(lineage)} table lineage entries for connector {connector.connector_id}"
                        )

                        # Log some column lineage statistics
                        total_column_mappings = sum(
                            len(table.column_lineage) for table in lineage
                        )
                        logger.info(
                            f"Extracted {total_column_mappings} column mappings across {len(lineage)} tables"
                        )
                    else:
                        logger.warning(
                            f"No lineage extracted for connector {connector.connector_id}"
                        )

                    connector.lineage = lineage

                except Exception as e:
                    logger.error(
                        f"Error extracting lineage for connector {connector.connector_id}: {e}"
                    )
                    report.report_failure(
                        f"Error extracting lineage for connector {connector.connector_id}: {e}"
                    )

        # Clean up job history
        with report.metadata_extraction_perf.connectors_jobs_extraction_sec:
            logger.info("Processing job history")
            for connector in connectors:
                if len(connector.jobs) > MAX_JOBS_PER_CONNECTOR:
                    # Sort by end time to ensure we keep the most recent
                    connector.jobs.sort(key=lambda job: job.end_time, reverse=True)
                    logger.info(
                        f"Truncating jobs for connector {connector.connector_id} from {len(connector.jobs)} to {MAX_JOBS_PER_CONNECTOR}"
                    )
                    connector.jobs = connector.jobs[:MAX_JOBS_PER_CONNECTOR]

                    report.warning(
                        title="Job history truncated",
                        message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs. "
                        f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                        context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                    )

        return connectors

    def _create_synthetic_lineage(
        self, connector: Connector, schemas: List[dict], destination_platform: str
    ) -> None:
        """Create synthetic lineage for a connector based just on schema and table information."""
        lineage_list = []

        for schema in schemas:
            schema_name = schema.get("name", "")
            if not schema_name:
                continue

            for table in schema.get("tables", []):
                if not isinstance(table, dict):
                    continue

                table_name = table.get("name", "")
                if not table_name or not table.get("enabled", True):
                    continue

                # Create source table identifier
                source_table = f"{schema_name}.{table_name}"

                # Get destination names
                dest_schema = self._get_destination_schema_name(
                    schema_name, destination_platform
                )
                dest_table = self._get_destination_table_name(
                    table_name, destination_platform
                )
                destination_table = f"{dest_schema}.{dest_table}"

                # Create synthetic column lineage if we have column info
                column_lineage = []
                columns = table.get("columns", [])

                if isinstance(columns, list):
                    for column in columns:
                        col_name = None
                        if isinstance(column, dict):
                            col_name = column.get("name")
                        elif isinstance(column, str):
                            col_name = column

                        if col_name and not col_name.startswith("_fivetran"):
                            is_bigquery = destination_platform.lower() == "bigquery"
                            dest_col = self._transform_column_name_for_platform(
                                col_name, is_bigquery
                            )
                            column_lineage.append(
                                ColumnLineage(
                                    source_column=col_name, destination_column=dest_col
                                )
                            )

                # Add this table's lineage
                lineage_list.append(
                    TableLineage(
                        source_table=source_table,
                        destination_table=destination_table,
                        column_lineage=column_lineage,
                    )
                )

        if lineage_list:
            logger.info(
                f"Created {len(lineage_list)} synthetic table lineage entries for connector {connector.connector_id}"
            )
            # Set the lineage directly on the connector instead of using _lineage_cache
            connector.lineage = lineage_list

    def _process_connector(
        self,
        api_connector: Dict,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        syncs_interval: int,
        report: FivetranSourceReport,
    ) -> Optional[Connector]:
        """Process a single connector and return the Connector object if applicable."""
        connector_id = api_connector.get("id", "")
        if not connector_id:
            logger.warning(f"Skipping connector with missing id: {api_connector}")
            return None

        connector_name = api_connector.get("name", "")
        if not connector_name:
            connector_name = f"connector-{connector_id}"

        # Extract destination ID
        destination_id = self._extract_destination_id(api_connector)

        # Check if this connector ID is explicitly specified in sources_to_platform_instance
        # If it is, we should include it regardless of connector_patterns
        explicitly_included = False
        if (
            self.config
            and hasattr(self.config, "sources_to_platform_instance")
            and connector_id in self.config.sources_to_platform_instance
        ):
            explicitly_included = True
            logger.info(
                f"Connector {connector_name} (ID: {connector_id}) explicitly included via sources_to_platform_instance"
            )

        # Apply connector pattern filter only if not explicitly included
        if not explicitly_included and not connector_patterns.allowed(connector_name):
            report.report_connectors_dropped(
                f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
            )
            return None

        # Apply destination filter
        if not destination_patterns.allowed(destination_id):
            report.report_connectors_dropped(
                f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
            )
            return None

        try:
            # Get sync history for this connector
            sync_history = self._get_sync_history(
                connector_id=connector_id, days=syncs_interval
            )

            # Convert from API format to our internal model
            connector = self.api_client.extract_connector_metadata(
                api_connector=api_connector, sync_history=sync_history
            )
        except Exception as e:
            logger.warning(
                f"Error processing connector {connector_id} metadata: {e}",
                exc_info=True,
            )
            # Create a basic connector instance with minimal information
            connector = Connector(
                connector_id=connector_id,
                connector_name=connector_name,
                connector_type=api_connector.get("service", "unknown"),
                paused=api_connector.get("paused", False),
                sync_frequency=api_connector.get("schedule", {}).get(
                    "sync_frequency", 1440
                ),
                destination_id=destination_id,
                user_id=api_connector.get("created_by", ""),
                lineage=[],
                jobs=[],
            )

        # Ensure destination_id is set in the connector
        if not connector.destination_id:
            connector.destination_id = destination_id
            logger.info(
                f"Set destination_id={destination_id} for connector {connector_id}"
            )

        # Log connector details for easier configuration
        logger.info(
            f"Found connector: {connector.connector_name} (ID={connector.connector_id}, Type={connector.connector_type}, Destination ID={connector.destination_id})"
        )

        # Determine destination platform from config
        if (
            self.config
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            destination_platform = self.config.fivetran_log_config.destination_platform
            connector.additional_properties["destination_platform"] = (
                destination_platform
            )
            logger.info(
                f"Setting destination platform to {destination_platform} from config for connector {connector_id}"
            )

        # Special handling for kafka and streaming-type connectors
        if connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]:
            if (
                self.config
                and hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config
            ):
                # Use fivetran_log_config.destination_platform
                connector.additional_properties["destination_platform"] = (
                    self.config.fivetran_log_config.destination_platform
                )
            else:
                # Default to snowflake if not specified
                connector.additional_properties["destination_platform"] = "snowflake"

        report.report_connectors_scanned()
        return connector

    def _get_sync_history(self, connector_id: str, days: int) -> List[Dict]:
        try:
            # First, try the standard connector sync-history endpoint
            sync_history = self.api_client.list_connector_sync_history(
                connector_id=connector_id, days=days
            )

            if sync_history:
                logger.info(
                    f"Found {len(sync_history)} sync history entries for connector {connector_id}"
                )
                return sync_history

            # If no history from primary endpoint, try alternative approach
            logger.warning(
                f"No sync history found for connector {connector_id} via primary endpoint, trying alternatives"
            )

            # Try direct logs endpoint
            try:
                connector_logs = self._fetch_connector_logs(connector_id, days)
                if connector_logs:
                    # Convert logs to sync history format
                    sync_history = self._convert_logs_to_sync_history(connector_logs)
                    if sync_history:
                        logger.info(
                            f"Extracted {len(sync_history)} sync history entries from logs for connector {connector_id}"
                        )
                        return sync_history
            except Exception as logs_error:
                logger.warning(
                    f"Failed to fetch logs for connector {connector_id}: {logs_error}"
                )

            # Try connector details as last resort
            try:
                connector_details = self.api_client.get_connector(connector_id)
                # Create synthetic history from latest sync
                if any(
                    x in connector_details
                    for x in ["succeeded_at", "failed_at", "completed_at"]
                ):
                    # Find the timestamp and status
                    sync_time = None
                    status = None
                    for field, status_value in [
                        ("succeeded_at", "COMPLETED"),
                        ("completed_at", "COMPLETED"),
                        ("failed_at", "FAILED"),
                    ]:
                        if field in connector_details:
                            sync_time = connector_details.get(field)
                            status = status_value
                            break

                    if sync_time:
                        sync_id = f"{connector_id}-latest"
                        logger.info(
                            f"Creating synthetic sync history for connector {connector_id} with status {status}"
                        )
                        return [
                            {
                                "id": sync_id,
                                "started_at": sync_time,  # Use same time as approximation
                                "completed_at": sync_time,
                                "status": status,
                            }
                        ]
            except Exception as details_error:
                logger.warning(
                    f"Failed to get connector details for sync history fallback: {details_error}"
                )

            # Return empty list if all methods fail
            return []

        except Exception as e:
            logger.error(
                f"Error retrieving sync history for connector {connector_id}: {e}"
            )
            return []

    def _fetch_connector_logs(self, connector_id: str, days: int) -> List[Dict]:
        """Fetch raw connector logs as an alternative source of sync information."""
        try:
            # Try the logs endpoint directly
            response = self.api_client._make_request(
                "GET",
                f"/connectors/{connector_id}/logs",
                params={"limit": 100, "since": f"{days}d"},
            )
            logs = response.get("data", {}).get("items", [])
            if logs:
                logger.info(f"Retrieved {len(logs)} logs for connector {connector_id}")
                return logs
        except Exception as e:
            logger.warning(f"Failed to retrieve logs: {e}")

        return []

    def _convert_logs_to_sync_history(self, logs: List[Dict]) -> List[Dict]:
        """Convert raw logs to sync history format."""
        # Group logs by sync_id
        sync_groups: Dict[str, List[Dict]] = {}
        for log in logs:
            if "sync_id" in log:
                sync_id = log.get("sync_id")
                # Fix: Ensure sync_id is not None and convert to string
                if sync_id is not None:
                    str_sync_id = str(
                        sync_id
                    )  # Ensure it's a string to use as a dict key
                    if str_sync_id not in sync_groups:
                        sync_groups[str_sync_id] = []
                    sync_groups[str_sync_id].append(log)
                else:
                    logger.warning("Found log entry with None sync_id, skipping")

        # Create sync history entries
        sync_history = []
        for sync_id, group_logs in sync_groups.items():
            # Find start and end logs
            start_log = None
            end_log = None
            for log in group_logs:
                message = log.get("message", "").lower()
                if "sync started" in message or "starting sync" in message:
                    start_log = log
                elif any(
                    x in message
                    for x in ["sync completed", "sync finished", "sync succeeded"]
                ):
                    end_log = log
                    end_log["status"] = "COMPLETED"
                elif any(
                    x in message
                    for x in ["sync failed", "failed", "error", "cancelled"]
                ):
                    end_log = log
                    status = "FAILED"
                    if "cancelled" in message:
                        status = "CANCELLED"
                    end_log["status"] = status

            # Create history entry if we have both start and end
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

    def _extract_destination_id(self, api_connector: Dict) -> str:
        """Extract destination ID from connector data with robust error handling."""
        connector_id = api_connector.get("id", "unknown")

        # Try different ways of getting the destination ID
        group_field = api_connector.get("group", {})
        destination_id = None

        if isinstance(group_field, dict):
            destination_id = group_field.get("id", "")
            if destination_id:
                logger.debug(f"Found destination_id={destination_id} from group.id")
                return destination_id

        # Try alternate fields if group.id doesn't work
        if "destination_id" in api_connector:
            destination_id = api_connector.get("destination_id", "")
            if destination_id:
                logger.debug(
                    f"Found destination_id={destination_id} from destination_id field"
                )
                return destination_id

        if "group_id" in api_connector:
            destination_id = api_connector.get("group_id", "")
            if destination_id:
                logger.debug(
                    f"Found destination_id={destination_id} from group_id field"
                )
                return destination_id

        # Generate a fallback ID based on connector ID if all else fails
        logger.warning(f"Could not find destination ID for connector {connector_id}")
        destination_id = f"destination_for_{connector_id}"
        logger.warning(f"Using generated destination ID: {destination_id}")
        return destination_id

    def _process_connector_jobs(
        self,
        connectors: List[Connector],
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> None:
        for connector in connectors:
            # If no jobs were found, try to fetch more with extended date range
            if not connector.jobs and syncs_interval < 30:
                try:
                    logger.info(
                        f"No jobs found for connector {connector.connector_id} with {syncs_interval} day lookback. "
                        f"Trying extended lookback of 30 days."
                    )
                    extended_history = self._get_sync_history(
                        connector_id=connector.connector_id, days=30
                    )
                    if extended_history:
                        # Create new jobs with the extended history
                        connector.jobs = (
                            self.api_client._extract_jobs_from_sync_history(
                                extended_history
                            )
                        )
                        logger.info(
                            f"Found {len(connector.jobs)} jobs with extended lookback"
                        )
                except Exception as e:
                    logger.warning(f"Failed to get extended history: {e}")

            # Normalize job statuses for consistency with enterprise mode
            for job in connector.jobs:
                # Convert API statuses to the expected status constants
                status_mapping = {
                    "COMPLETED": "SUCCESSFUL",
                    "SUCCEEDED": "SUCCESSFUL",
                    "SUCCESS": "SUCCESSFUL",
                    "FAILED": "FAILURE_WITH_TASK",
                    "FAILURE": "FAILURE_WITH_TASK",
                    "ERROR": "FAILURE_WITH_TASK",
                    "CANCELLED": "CANCELED",
                    "CANCELED": "CANCELED",
                }

                if job.status in status_mapping:
                    job.status = status_mapping[job.status]

            if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
                report.warning(
                    title="Job history truncated",
                    message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs in the past {syncs_interval} days. "
                    f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                    context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                )
                # Sort by end_time to keep the most recent jobs
                connector.jobs = sorted(
                    connector.jobs, key=lambda j: j.end_time, reverse=True
                )[:MAX_JOBS_PER_CONNECTOR]

    def _generate_config_example(
        self, destination_details: Dict[str, Dict[str, str]]
    ) -> str:
        """Generate configuration example for destination to platform instance mapping."""
        example_config = "\ndestination_to_platform_instance:\n"
        for dest_id, details in destination_details.items():
            # More scalable platform detection based on service name
            known_platforms = {
                "bigquery": "bigquery",
                "snowflake": "snowflake",
                "redshift": "redshift",
                "postgres": "postgres",
                "mysql": "mysql",
                "databricks": "databricks",
                "synapse": "mssql",
                "sql_server": "mssql",
                "azure_sql": "mssql",
                "oracle": "oracle",
                "kafka": "kafka",
                "s3": "s3",
                "gcs": "gcs",
            }

            service = details.get("service", "").lower()
            platform_suggestion = None

            # Find best matching platform
            for key, platform in known_platforms.items():
                if key in service:
                    platform_suggestion = platform
                    break

            # Default to snowflake if no match found
            if not platform_suggestion:
                platform_suggestion = "snowflake"

            example_config += f"  {dest_id}:  # {details['name']}\n"
            example_config += f'    platform: "{platform_suggestion}"\n'
            example_config += '    database: "your_database_name"\n'
            example_config += '    env: "PROD"\n'

        return example_config

    def _process_schemas_for_lineage(
        self,
        connector: Connector,
        schemas: List[Dict],
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[TableLineage]:
        """
        Process schemas to extract lineage information for a connector.
        """
        lineage_list = []
        destination_platform = self._get_destination_platform(connector)

        for schema in schemas:
            schema_name = schema.get("name", "")
            if not schema_name:
                continue

            # Get destination schema name (use name_in_destination if available)
            dest_schema = schema.get("name_in_destination", schema_name)

            # Apply platform-specific transformations if no explicit destination name
            if dest_schema == schema_name:
                if destination_platform.lower() == "bigquery":
                    dest_schema = schema_name.lower()
                else:
                    dest_schema = schema_name.upper()

            # Process each table in the schema
            for table in schema.get("tables", []):
                table_name = table.get("name", "")
                if not table_name or not table.get("enabled", True):
                    continue

                # Create source table name
                source_table = f"{schema_name}.{table_name}"

                # Get destination table name (use name_in_destination if available)
                dest_table = table.get("name_in_destination", table_name)

                # Apply platform-specific transformations if no explicit destination name
                if dest_table == table_name:
                    if destination_platform.lower() == "bigquery":
                        dest_table = table_name.lower()
                    else:
                        dest_table = table_name.upper()

                # Create full destination table name
                destination_table = f"{dest_schema}.{dest_table}"

                # Extract column lineage
                column_lineage = self._extract_column_lineage(
                    table=table,
                    source_table=source_table,
                    destination_platform=destination_platform,
                    source_table_columns=source_table_columns,
                )

                # Add this table's lineage to the list
                lineage_list.append(
                    TableLineage(
                        source_table=source_table,
                        destination_table=destination_table,
                        column_lineage=column_lineage,
                    )
                )

        logger.info(
            f"Extracted {len(lineage_list)} table lineage entries with {sum(len(tl.column_lineage) for tl in lineage_list)} column mappings"
        )
        return lineage_list

    def _extract_column_lineage(
        self,
        table: Dict,
        source_table: str,
        destination_platform: str,
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[ColumnLineage]:
        """
        Extract column-level lineage for a table.
        Args:
            table: Table data from API
            source_table: Full source table name (schema.table)
            destination_platform: Destination platform type
            source_table_columns: Dict mapping table names to column information
        Returns:
            List of ColumnLineage objects
        """
        logger.debug(
            f"Extracting column lineage for {source_table} to {destination_platform}"
        )

        # 1. Get columns from the table data
        columns = table.get("columns", [])

        # Handle different column formats
        if isinstance(columns, dict):
            # Convert dict format to list
            columns_list = []
            for col_name, col_data in columns.items():
                if isinstance(col_data, dict):
                    col_data = col_data.copy()
                    col_data["name"] = col_name
                    columns_list.append(col_data)
                else:
                    columns_list.append({"name": col_name, "type": str(col_data)})
            columns = columns_list
            logger.debug(f"Converted dict format to list with {len(columns)} columns")

        # Create column lineage from the columns we have
        column_lineage = []
        is_bigquery = destination_platform.lower() == "bigquery"

        if not columns:
            logger.warning(f"No column information available for {source_table}")
            return []

        for column in columns:
            col_name = None
            if isinstance(column, dict):
                col_name = column.get("name")
            elif isinstance(column, str):
                col_name = column

            if not col_name or col_name.startswith("_fivetran"):
                continue

            # Get destination column name - prefer name_in_destination if available
            dest_col_name = None
            if isinstance(column, dict) and "name_in_destination" in column:
                dest_col_name = column.get("name_in_destination")
                logger.debug(
                    f"Using name_in_destination: {col_name} -> {dest_col_name}"
                )

            # If no name_in_destination, transform based on platform
            if not dest_col_name:
                dest_col_name = self._transform_column_name_for_platform(
                    col_name, is_bigquery
                )
                logger.debug(f"Transformed column name: {col_name} -> {dest_col_name}")

            column_lineage.append(
                ColumnLineage(
                    source_column=col_name,
                    destination_column=dest_col_name,
                )
            )

        if column_lineage:
            logger.debug(
                f"Created {len(column_lineage)} column lineage entries for {source_table}"
            )
        else:
            logger.warning(f"No column lineage created for {source_table}")

        return column_lineage

    def _get_columns_from_api(self, source_table: str) -> List[Dict]:
        """Get columns directly from Fivetran API for a table."""
        # Parse schema and table name
        if "." not in source_table:
            logger.warning(
                f"Source table {source_table} doesn't contain schema name, cannot query API"
            )
            return []

        schema_name, table_name = source_table.split(".", 1)

        # Find the connector ID for this source table
        connector_id = self._find_connector_id_for_source_table(source_table)
        if not connector_id:
            logger.warning(
                f"Could not find connector ID for source table {source_table}"
            )
            return []

        logger.info(
            f"Found connector ID {connector_id} for source table {source_table}"
        )
        logger.info(f"Querying API for columns of {schema_name}.{table_name}")

        # Call the API to get columns using the direct columns endpoint
        try:
            columns = self.api_client.get_table_columns(
                connector_id, schema_name, table_name
            )
            if columns:
                logger.info(
                    f"Retrieved {len(columns)} columns from API for {source_table}"
                )
                return columns
            else:
                logger.warning(f"No columns returned from API for {source_table}")
        except Exception as e:
            logger.warning(f"Failed to get columns from API for {source_table}: {e}")

        return []

    def _find_connector_id_for_source_table(self, source_table: str) -> Optional[str]:
        """Find the connector ID for a source table with improved matching."""
        # Normalize the source table name for more flexible matching
        normalized_source = source_table.lower()

        # Try to find in the connector cache
        for conn in getattr(self, "_connector_cache", []):
            if not hasattr(conn, "connector_id"):
                continue

            # Check if it's a Salesforce connector if the source table name has "salesforce"
            if (
                "salesforce" in normalized_source
                and conn.connector_type.lower() == "salesforce"
            ):
                logger.info(
                    f"Matching Salesforce table {source_table} to connector {conn.connector_id}"
                )
                return conn.connector_id

            # Check in lineage explicitly
            for lineage in getattr(conn, "lineage", []):
                if (
                    hasattr(lineage, "source_table")
                    and lineage.source_table.lower() == normalized_source
                ):
                    return conn.connector_id

            # Try partial matching - especially helpful for Salesforce objects
            for lineage in getattr(conn, "lineage", []):
                if (
                    hasattr(lineage, "source_table")
                    and source_table.split(".")[0] == lineage.source_table.split(".")[0]
                ):
                    # If schema matches, this is probably the right connector
                    logger.info(
                        f"Found schema match for {source_table} in connector {conn.connector_id}"
                    )
                    return conn.connector_id

        # If no match found, look for connector type matching the schema name
        schema_name = source_table.split(".")[0] if "." in source_table else ""
        if schema_name:
            for conn in getattr(self, "_connector_cache", []):
                if conn.connector_type.lower() == schema_name.lower():
                    logger.info(
                        f"Matched {source_table} to connector {conn.connector_id} based on schema/type match"
                    )
                    return conn.connector_id

        # No match found
        return None

    def _find_columns_from_similar_tables(
        self, schemas: List[Dict], target_schema: str, target_table: str
    ) -> List[Dict]:
        """
        Find columns by looking at tables with similar names in the same schema
        or tables with the same name in different schemas.
        """
        # First, try exact name match in different schemas
        for schema in schemas:
            schema_name = schema.get("name", "")
            if schema_name == target_schema:
                continue  # Skip the target schema itself

            for table in schema.get("tables", []):
                table_name = table.get("name", "")
                if table_name == target_table and table.get("enabled", True):
                    columns = table.get("columns", [])
                    if columns:
                        logger.info(
                            f"Found columns from exact name match in different schema: {schema_name}.{table_name}"
                        )
                        return columns

        # Next, try similar tables in the same schema
        best_match = None
        best_score = 0.6  # Minimum similarity threshold
        best_columns = []

        for schema in schemas:
            schema_name = schema.get("name", "")
            if schema_name != target_schema:
                continue

            for table in schema.get("tables", []):
                table_name = table.get("name", "")
                if table_name == target_table or not table.get("enabled", True):
                    continue

                # Calculate similarity between table names
                similarity = difflib.SequenceMatcher(
                    None, target_table, table_name
                ).ratio()
                if similarity > best_score:
                    columns = table.get("columns", [])
                    if columns:
                        best_match = table_name
                        best_score = similarity
                        best_columns = columns

        if best_match:
            logger.info(
                f"Found columns from similar table: {target_schema}.{best_match} "
                f"(similarity: {best_score:.2f})"
            )
            return best_columns

        # No matching table found
        return []

    def _log_lineage_stats(
        self, lineage_list: List[TableLineage], connector_id: str
    ) -> None:
        """Log statistics about lineage processing."""
        if not lineage_list:
            logger.warning(f"No lineage entries found for connector {connector_id}")
            return

        tables_with_columns = len(
            [
                table_lineage
                for table_lineage in lineage_list
                if table_lineage.column_lineage
            ]
        )
        total_column_mappings = sum(
            len(table_lineage.column_lineage) for table_lineage in lineage_list
        )
        logger.info(
            f"Lineage stats for connector {connector_id}: "
            f"{len(lineage_list)} table lineages, {tables_with_columns} tables with column lineage, "
            f"{total_column_mappings} total column mappings"
        )

    def _collect_source_columns(self, schemas: List[Dict]) -> Dict[str, Dict[str, str]]:
        """
        Collect all source columns with their types for each table.

        Returns:
            Dict mapping table names to Dict of column names and their types
        """
        source_columns: Dict[str, Dict[str, str]] = {}
        for schema in schemas:
            schema_name = schema.get("name", "")
            if not schema_name:
                continue

            tables = schema.get("tables", [])
            if not isinstance(tables, list):
                continue

            for table in tables:
                if not isinstance(table, dict):
                    continue

                table_name = table.get("name", "")
                if not table_name:
                    continue

                full_table_name = f"{schema_name}.{table_name}"
                source_columns[full_table_name] = {}

                columns = table.get("columns", [])
                if not columns:
                    continue

                # Handle different column formats
                if isinstance(columns, list):
                    for column in columns:
                        if not isinstance(column, dict):
                            continue

                        column_name = column.get("name", "")
                        if not column_name:
                            continue

                        column_type = column.get("type", "")
                        source_columns[full_table_name][column_name] = column_type
                elif isinstance(columns, dict):
                    for column_name, column_data in columns.items():
                        if isinstance(column_data, dict):
                            column_type = column_data.get("type", "")
                        else:
                            column_type = str(column_data)
                        source_columns[full_table_name][column_name] = column_type

        return source_columns

    def _transform_column_name_with_pattern(
        self, column_name: str, naming_pattern: ColumnNamingPattern
    ) -> str:
        """
        Transform a column name according to the specified naming pattern.

        Args:
            column_name: The original column name
            naming_pattern: The target naming pattern

        Returns:
            Transformed column name
        """
        import re

        if not column_name:
            return ""

        # First normalize by removing special characters (except underscores)
        normalized = re.sub(r"[^\w]", "", column_name)

        if naming_pattern == ColumnNamingPattern.AUTO:
            # In auto mode, try to determine the current pattern and preserve it
            return column_name

        elif naming_pattern == ColumnNamingPattern.SNAKE_CASE:
            # Convert camelCase or PascalCase to snake_case
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", normalized)
            s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
            return s2.lower()

        elif naming_pattern == ColumnNamingPattern.CAMEL_CASE:
            # Convert to camelCase
            # First convert to snake_case if it contains underscores
            if "_" in normalized:
                parts = normalized.lower().split("_")
                return parts[0] + "".join(p.capitalize() for p in parts[1:])
            # If already camelCase or PascalCase, ensure first letter is lowercase
            return normalized[0].lower() + normalized[1:]

        elif naming_pattern == ColumnNamingPattern.PASCAL_CASE:
            # Convert to PascalCase
            # First convert to snake_case if it contains underscores
            if "_" in normalized:
                parts = normalized.lower().split("_")
                return "".join(p.capitalize() for p in parts)
            # If already camelCase, capitalize first letter
            return normalized[0].upper() + normalized[1:]

        elif naming_pattern == ColumnNamingPattern.UPPER_CASE:
            # Convert to UPPER_CASE
            # First convert to snake_case if it's camelCase or PascalCase
            if re.search(r"[a-z][A-Z]", normalized):
                s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", normalized)
                s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
                return s2.upper()
            return normalized.upper()

        elif naming_pattern == ColumnNamingPattern.LOWER_CASE:
            # Convert to lowercase, preserving underscores
            return normalized.lower()

        # Default case (should not happen)
        return column_name

    def _detect_column_naming_pattern(
        self, column_names: List[str]
    ) -> ColumnNamingPattern:
        """
        Detect the naming pattern from a list of column names.

        Args:
            column_names: List of column names

        Returns:
            The detected naming pattern
        """
        if not column_names:
            return ColumnNamingPattern.AUTO

        # Count occurrences of each pattern
        pattern_counts = {
            ColumnNamingPattern.SNAKE_CASE: 0,
            ColumnNamingPattern.CAMEL_CASE: 0,
            ColumnNamingPattern.PASCAL_CASE: 0,
            ColumnNamingPattern.UPPER_CASE: 0,
            ColumnNamingPattern.LOWER_CASE: 0,
        }

        for name in column_names:
            if not name:
                continue

            # Check for snake_case
            if "_" in name and name.islower():
                pattern_counts[ColumnNamingPattern.SNAKE_CASE] += 1
            # Check for camelCase
            elif name[0].islower() and any(c.isupper() for c in name):
                pattern_counts[ColumnNamingPattern.CAMEL_CASE] += 1
            # Check for PascalCase
            elif name[0].isupper() and any(c.islower() for c in name):
                pattern_counts[ColumnNamingPattern.PASCAL_CASE] += 1
            # Check for UPPER_CASE
            elif name.isupper():
                pattern_counts[ColumnNamingPattern.UPPER_CASE] += 1
            # Check for lower_case
            elif name.islower():
                pattern_counts[ColumnNamingPattern.LOWER_CASE] += 1

        # Return the most common pattern
        if not pattern_counts:
            return ColumnNamingPattern.AUTO

        return max(pattern_counts.items(), key=lambda x: x[1])[0]

    def _get_source_naming_pattern(self, connector_id: str) -> ColumnNamingPattern:
        """Get the column naming pattern for a source connector."""
        if (
            self.config
            and hasattr(self.config, "sources_to_platform_instance")
            and connector_id in self.config.sources_to_platform_instance
        ):
            platform_detail = self.config.sources_to_platform_instance[connector_id]
            if hasattr(platform_detail, "column_naming_pattern"):
                return platform_detail.column_naming_pattern

        return ColumnNamingPattern.AUTO

    def _get_destination_naming_pattern(
        self, destination_id: str, platform: str
    ) -> ColumnNamingPattern:
        """Get the column naming pattern for a destination platform."""
        # First check if there's an explicit configuration
        if (
            self.config
            and hasattr(self.config, "destination_to_platform_instance")
            and destination_id in self.config.destination_to_platform_instance
        ):
            platform_detail = self.config.destination_to_platform_instance[
                destination_id
            ]
            if hasattr(platform_detail, "column_naming_pattern"):
                return platform_detail.column_naming_pattern

        # If no explicit config, use platform-based defaults
        platform_defaults = {
            "bigquery": ColumnNamingPattern.SNAKE_CASE,
            "snowflake": ColumnNamingPattern.UPPER_CASE,
            "redshift": ColumnNamingPattern.UPPER_CASE,
            "postgres": ColumnNamingPattern.SNAKE_CASE,
            "mysql": ColumnNamingPattern.SNAKE_CASE,
            "oracle": ColumnNamingPattern.UPPER_CASE,
            "mssql": ColumnNamingPattern.PASCAL_CASE,
            "databricks": ColumnNamingPattern.SNAKE_CASE,
        }

        return platform_defaults.get(platform.lower(), ColumnNamingPattern.AUTO)

    def _convert_column_dict_to_list(self, columns_dict: Dict) -> List[Dict]:
        """Convert column dictionary to list format."""
        columns_list = []
        for col_name, col_data in columns_dict.items():
            if isinstance(col_data, dict):
                col_data = col_data.copy()
                col_data["name"] = col_name
                columns_list.append(col_data)
            else:
                columns_list.append({"name": col_name, "type": str(col_data)})
        return columns_list

    def _normalize_column_name(self, column_name: str) -> str:
        """Normalize column name for comparison by removing non-alphanumeric chars and converting to lowercase."""
        # Remove non-alphanumeric characters and convert to lowercase
        normalized = re.sub(r"[^a-zA-Z0-9]", "", column_name).lower()
        return normalized

    def _find_best_fuzzy_match(
        self, source_col: str, source_norm: str, dest_columns: List[Tuple[str, str]]
    ) -> Optional[str]:
        """
        Find the best match for a source column in the destination columns.

        Args:
            source_col: Original source column name
            source_norm: Normalized source column name
            dest_columns: List of (column_name, normalized_name) tuples

        Returns:
            Best matching destination column name or None
        """
        # First, check for exact match with normalized names
        for dest_col, dest_norm in dest_columns:
            if source_norm == dest_norm:
                return dest_col

        # If no exact match, try converting source column from camelCase to snake_case
        if re.search(r"[A-Z]", source_col):
            snake_case = self._transform_column_name_for_platform(source_col, True)
            snake_norm = self._normalize_column_name(snake_case)

            for dest_col, dest_norm in dest_columns:
                if snake_norm == dest_norm:
                    return dest_col

        # Use difflib to find close matches
        all_dest_norms = [norm for _, norm in dest_columns]
        matches = difflib.get_close_matches(
            source_norm, all_dest_norms, n=1, cutoff=0.8
        )

        if matches:
            matched_norm = matches[0]
            for dest_col, dest_norm in dest_columns:
                if dest_norm == matched_norm:
                    return dest_col

        # Try fallback to original column name in proper case for destination
        if not dest_columns:
            return source_col

        return None

    def _transform_column_name_for_platform(
        self, column_name: str, is_bigquery: bool
    ) -> str:
        """
        Transform column name based on the destination platform with better handling of edge cases.

        Args:
            column_name: Source column name
            is_bigquery: Whether the destination is BigQuery

        Returns:
            Transformed column name
        """
        if not column_name:
            return ""

        if is_bigquery:
            # For BigQuery:
            # 1. Convert to lowercase
            # 2. Replace camelCase with snake_case
            # 3. Clean up any invalid characters
            import re

            # Step 1: Convert camelCase to snake_case with regex
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", column_name)
            s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

            # Step 2: lowercase and replace non-alphanumeric with underscore
            transformed = re.sub(r"[^a-zA-Z0-9_]", "_", s2.lower())

            # Step 3: Remove leading/trailing underscores and collapse multiple underscores
            transformed = re.sub(r"_+", "_", transformed).strip("_")

            # Log the transformation for debugging
            if transformed != column_name.lower():
                logger.debug(f"Transformed column: {column_name} -> {transformed}")

            return transformed
        else:
            # For other platforms like Snowflake, typically uppercase
            return column_name.upper()

    def _get_destination_platform(self, connector: Connector) -> str:
        """
        Determine the destination platform based on the configuration and connector details.

        Order of precedence:
        1. Check if there's a specific setting in destination_to_platform_instance for this destination
        2. Check if destination platform is in connector's additional properties (from API detection)
        3. Only then fall back to fivetran_log_config platform
        4. Default to snowflake if nothing else is available
        """
        # Check if we have a specific mapping for this destination
        if self.config and hasattr(self.config, "destination_to_platform_instance"):
            destination_details = self.config.destination_to_platform_instance.get(
                connector.destination_id
            )
            if destination_details and destination_details.platform:
                logger.info(
                    f"Using platform '{destination_details.platform}' from destination_to_platform_instance for {connector.destination_id}"
                )
                return destination_details.platform

        # Check if destination platform is in connector's additional properties from API detection
        if "destination_platform" in connector.additional_properties:
            destination_platform = str(
                connector.additional_properties.get("destination_platform")
            )
            if destination_platform:
                logger.info(
                    f"Using platform '{destination_platform}' from API-detected properties"
                )
                return destination_platform

        # Only fall back to fivetran_log_config if no platform was detected from the API
        if (
            self.config
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            destination_platform = self.config.fivetran_log_config.destination_platform
            logger.info(
                f"Falling back to platform '{destination_platform}' from fivetran_log_config"
            )
            return destination_platform

        # Special handling for specific connector types
        if connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]:
            logger.info(
                f"Special handling for {connector.connector_type} connector: defaulting destination to 'kafka'"
            )
            return "kafka"

        # Default to snowflake if no platform information is available
        logger.info("No destination platform specified, defaulting to 'snowflake'")
        return "snowflake"

    def _get_destination_schema_name(
        self, schema_name: str, destination_platform: str
    ) -> str:
        """
        Get the destination schema name based on the platform.
        This is a helper method that applies appropriate case transformations.
        """
        if destination_platform.lower() == "bigquery":
            # BigQuery schema names are case-sensitive and typically lowercase
            return schema_name.lower()
        else:
            # For most other systems (Snowflake, Redshift, etc.), schema names are uppercased
            return schema_name.upper()

    def _get_destination_table_name(
        self, table_name: str, destination_platform: str
    ) -> str:
        """
        Get the destination table name based on the platform.
        This is a helper method that applies appropriate case transformations.
        """
        if destination_platform.lower() == "bigquery":
            # BigQuery table names are case-sensitive and typically lowercase
            return table_name.lower()
        else:
            # For most other systems (Snowflake, Redshift, etc.), table names are uppercased
            return table_name.upper()
