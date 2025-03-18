"""
This is an updated version of fivetran_standard_api.py with the fix for BigQuery case handling.
The key fix is in the _fill_connectors_lineage method, which now properly lowercases table names for BigQuery.
"""

import logging
from typing import Dict, List, Optional, Set, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
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
    MAX_TABLE_LINEAGE_PER_CONNECTOR,
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
        This is the standard version replacement for querying log tables.
        """
        connectors: List[Connector] = []

        with report.metadata_extraction_perf.connectors_metadata_extraction_sec:
            logger.info("Fetching connector list from Fivetran API")
            connector_list = self.api_client.list_connectors()

            # Debug raw connector list to see structure (uncomment if needed)
            # logger.debug(f"Raw connector list from API (first 2 items): {connector_list[:2]}")

            # Collect destination information
            destinations_seen, destination_details = self._collect_destination_info(
                connector_list
            )

            # Log a configuration example for the user
            if destinations_seen:
                example_config = self._generate_config_example(destination_details)
                logger.info(
                    f"Configuration example for destination_to_platform_instance:{example_config}"
                )
            else:
                logger.warning(
                    "No destinations found. This may indicate an issue with the API response structure or permissions."
                )

            # Process each connector
            for api_connector in connector_list:
                connector = self._process_connector(
                    api_connector=api_connector,
                    connector_patterns=connector_patterns,
                    destination_patterns=destination_patterns,
                    syncs_interval=syncs_interval,
                    report=report,
                )

                if connector:
                    connectors.append(connector)

        if not connectors:
            logger.info("No allowed connectors found")
            return []

        logger.info(f"Found {len(connectors)} allowed connectors")

        with report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info("Fetching connector lineage from Fivetran API")
            self._fill_connectors_lineage(connectors)

        # Process jobs for each connector
        self._process_connector_jobs(connectors, report)

        return connectors

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

        # Apply filters
        if not connector_patterns.allowed(connector_name):
            report.report_connectors_dropped(
                f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
            )
            return None

        if not destination_patterns.allowed(destination_id):
            report.report_connectors_dropped(
                f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
            )
            return None

        # Get sync history for this connector
        sync_history = self.api_client.list_connector_sync_history(
            connector_id=connector_id, days=syncs_interval
        )

        # Convert from API format to our internal model
        connector = self.api_client.extract_connector_metadata(
            api_connector=api_connector, sync_history=sync_history
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

    def _extract_destination_id(self, api_connector: Dict) -> str:
        """Extract destination ID from connector data with robust error handling."""
        connector_id = api_connector.get("id", "unknown")

        # Try different ways of getting the destination ID
        group_field = api_connector.get("group", {})
        destination_id = None

        if isinstance(group_field, dict):
            destination_id = group_field.get("id", "")
            logger.debug(f"Found destination_id={destination_id} from group.id")
        else:
            logger.debug(f"group field is not a dictionary: {group_field}")

        # Try alternate fields if group.id doesn't work
        if not destination_id:
            destination_id = api_connector.get("destination_id", "")
            if destination_id:
                logger.debug(
                    f"Found destination_id={destination_id} from destination_id field"
                )

        if not destination_id:
            destination_id = api_connector.get("group_id", "")
            if destination_id:
                logger.debug(
                    f"Found destination_id={destination_id} from group_id field"
                )

        # If destination_id is still empty, log a warning and try to extract any identifier
        if not destination_id:
            logger.warning(
                f"Empty destination ID found for connector: {connector_id}, name: {api_connector.get('name', 'unknown')}"
            )
            logger.warning(f"Available fields: {list(api_connector.keys())}")

            # As a fallback, check if there's any field that might contain destination info
            for key, value in api_connector.items():
                if isinstance(value, str) and (
                    "destination" in key.lower() or "group" in key.lower()
                ):
                    logger.info(f"Potential destination field: {key}={value}")

            # Generate a dummy destination ID based on connector ID if all else fails
            destination_id = f"destination_for_{connector_id}"
            logger.warning(f"Using generated destination ID: {destination_id}")

        return destination_id

    def _process_connector_jobs(
        self, connectors: List[Connector], report: FivetranSourceReport
    ) -> None:
        """Process jobs for each connector, limiting to the maximum allowed."""
        for connector in connectors:
            if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
                report.warning(
                    title="Job history truncated",
                    message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs in the past {self.config.history_sync_lookback_period if self.config else 7} days. "
                    f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                    context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                )
                connector.jobs = sorted(
                    connector.jobs, key=lambda j: j.end_time, reverse=True
                )[:MAX_JOBS_PER_CONNECTOR]

    def _generate_config_example(
        self, destination_details: Dict[str, Dict[str, str]]
    ) -> str:
        """Generate configuration example for destination to platform instance mapping."""
        example_config = "\ndestination_to_platform_instance:\n"
        for dest_id, details in destination_details.items():
            platform_suggestion = (
                "bigquery"
                if details.get("service") and "bigquery" in details["service"].lower()
                else "snowflake"
            )
            example_config += f"  {dest_id}:  # {details['name']}\n"
            example_config += f'    platform: "{platform_suggestion}"\n'
            example_config += '    database: "your_database_name"\n'
            example_config += '    env: "PROD"\n'

        return example_config

    def _collect_destination_info(
        self, connector_list: List[Dict]
    ) -> Tuple[Set[str], Dict[str, Dict[str, str]]]:
        """Collect information about all destinations from connector list."""
        destinations_seen = set()
        destination_details = {}

        # First pass: collect all destination IDs to log them for configuration
        for api_connector in connector_list:
            # Debug raw connector structure if needed
            # logger.debug(f"Connector structure: {json.dumps(api_connector, indent=2)}")

            destination_id = self._extract_destination_id(api_connector)

            if destination_id and destination_id not in destinations_seen:
                destinations_seen.add(destination_id)

                # Fetch destination details with better error handling
                try:
                    destination_data = self.api_client.get_destination_details(
                        destination_id
                    )
                    destination_name = destination_data.get("name", "unknown")
                    destination_service = destination_data.get("service", "unknown")

                    destination_details[destination_id] = {
                        "name": destination_name,
                        "service": destination_service,
                    }

                    logger.info(
                        f"Found destination: ID={destination_id}, Name={destination_name}, Service={destination_service}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Error fetching details for destination {destination_id}: {e}"
                    )
                    destination_details[destination_id] = {
                        "name": "unknown (error fetching details)",
                        "service": "unknown",
                    }

        return destinations_seen, destination_details

    def _fill_connectors_lineage(self, connectors: List[Connector]) -> None:
        """
        Fill in lineage information for connectors by calling the API.
        """
        for connector in connectors:
            try:
                logger.info(
                    f"Extracting lineage for connector {connector.connector_id}"
                )

                # Determine destination platform from the configuration
                destination_platform = self._get_destination_platform(connector)

                # Update the connector's additional properties with the correct destination platform
                connector.additional_properties["destination_platform"] = (
                    destination_platform
                )
                logger.info(
                    f"Using destination platform {destination_platform} for connector {connector.connector_id}"
                )

                # Get schema information
                schemas = self.api_client.list_connector_schemas(connector.connector_id)
                logger.info(
                    f"Got {len(schemas)} schemas for connector {connector.connector_id}"
                )

                lineage_list = []

                # Process each schema
                for schema in schemas:
                    try:
                        schema_name = schema.get("name", "")
                        if not schema_name:
                            logger.warning(
                                f"Skipping schema with no name in connector {connector.connector_id}"
                            )
                            continue

                        tables = schema.get("tables", [])
                        if not isinstance(tables, list):
                            logger.warning(
                                f"Schema {schema_name} has non-list tables: {type(tables)}"
                            )
                            continue

                        # Process each table in the schema
                        for table in tables:
                            try:
                                if not isinstance(table, dict):
                                    continue

                                table_name = table.get("name", "")
                                enabled = table.get("enabled", False)

                                if not enabled or not table_name:
                                    continue

                                # Create source and destination table identifiers
                                source_table = f"{schema_name}.{table_name}"

                                # Adjust case based on destination platform
                                # FIX: Use the helper method from api_client for consistent case handling
                                dest_schema = (
                                    self.api_client._get_destination_schema_name(
                                        schema_name, destination_platform
                                    )
                                )
                                dest_table = (
                                    self.api_client._get_destination_table_name(
                                        table_name, destination_platform
                                    )
                                )
                                destination_table = f"{dest_schema}.{dest_table}"

                                # Process columns for lineage
                                column_lineage = []
                                columns = table.get("columns", [])

                                if isinstance(columns, list):
                                    for column in columns:
                                        try:
                                            if not isinstance(column, dict):
                                                continue

                                            col_name = column.get("name", "")
                                            if not col_name:
                                                continue

                                            # FIX: Use the helper method for consistent case handling
                                            dest_col_name = self.api_client._get_destination_column_name(
                                                col_name, destination_platform
                                            )

                                            column_lineage.append(
                                                ColumnLineage(
                                                    source_column=col_name,
                                                    destination_column=dest_col_name,
                                                )
                                            )
                                        except Exception as col_e:
                                            logger.warning(
                                                f"Error processing column in table {table_name}: {col_e}"
                                            )

                                # Add this table's lineage
                                lineage_list.append(
                                    TableLineage(
                                        source_table=source_table,
                                        destination_table=destination_table,
                                        column_lineage=column_lineage,
                                    )
                                )

                                logger.debug(
                                    f"Added lineage: {source_table} -> {destination_table} with {len(column_lineage)} columns"
                                )
                            except Exception as table_e:
                                logger.warning(
                                    f"Error processing table {table.get('name', 'unknown')}: {table_e}"
                                )
                    except Exception as schema_e:
                        logger.warning(
                            f"Error processing schema {schema.get('name', 'unknown')}: {schema_e}"
                        )

                # Truncate if necessary
                if len(lineage_list) > MAX_TABLE_LINEAGE_PER_CONNECTOR:
                    logger.warning(
                        f"Connector {connector.connector_name} has {len(lineage_list)} tables, "
                        f"truncating to {MAX_TABLE_LINEAGE_PER_CONNECTOR}"
                    )
                    lineage_list = lineage_list[:MAX_TABLE_LINEAGE_PER_CONNECTOR]

                connector.lineage = lineage_list

                logger.info(
                    f"Successfully extracted {len(lineage_list)} table lineages for connector {connector.connector_id}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to extract lineage for connector {connector.connector_name}: {e}",
                    exc_info=True,
                )
                connector.lineage = []

    def _get_destination_platform(self, connector: Connector) -> str:
        """
        Determine the destination platform based on the configuration and connector details.

        1. First, check if there's a specific setting in destination_to_platform_instance for this destination
        2. Next, check if the fivetran_log_config has a destination_platform specified
        3. For confluent_cloud connectors, don't override destination platform from their additional properties
        4. Finally, fall back to the additional properties in the connector
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

        # Check if destination platform is specified in fivetran_log_config
        if (
            self.config
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            destination_platform: str = (
                self.config.fivetran_log_config.destination_platform
            )
            logger.info(
                f"Using platform '{destination_platform}' from fivetran_log_config"
            )
            return destination_platform

        # Special handling for specific connector types - don't let their connector type affect destination type
        if connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]:
            # For these streaming connectors, we need to rely on the other settings, not on connector type
            # Default to snowflake if we can't determine otherwise
            logger.info(
                f"Special handling for {connector.connector_type} connector: defaulting destination to 'snowflake'"
            )
            return "snowflake"

        # Check if destination platform is in connector's additional properties
        if "destination_platform" in connector.additional_properties:
            destination_platform = str(
                connector.additional_properties.get("destination_platform")
            )
            if destination_platform:
                logger.info(
                    f"Using platform '{destination_platform}' from connector properties"
                )
                return destination_platform

        # Default to snowflake if no platform information is available
        logger.info("No destination platform specified, defaulting to 'snowflake'")
        return "snowflake"
