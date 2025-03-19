import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

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

    def _process_schemas_for_lineage(
        self,
        connector: Connector,
        schemas: List[Dict],
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[TableLineage]:
        """
        Process schemas to extract lineage information for a connector.
        This was extracted from _fill_connectors_lineage to reduce complexity.
        """
        lineage_list = []
        destination_platform = self._get_destination_platform(connector)

        for schema in schemas:
            try:
                schema_name = schema.get("name", "")
                if not schema_name:
                    logger.warning(
                        f"Skipping schema with no name in connector {connector.connector_id}"
                    )
                    continue

                # Use name_in_destination if available for schema
                schema_name_in_destination = schema.get("name_in_destination")

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

                        # Create source table identifier
                        source_table = f"{schema_name}.{table_name}"

                        # Get destination schema name - prefer name_in_destination if available
                        dest_schema = None
                        if schema_name_in_destination:
                            dest_schema = schema_name_in_destination
                        else:
                            # Fall back to case transformation if name_in_destination not available
                            dest_schema = self._get_destination_schema_name(
                                schema_name, destination_platform
                            )

                        # Get destination table name - prefer name_in_destination if available
                        dest_table = None
                        table_name_in_destination = table.get("name_in_destination")
                        if table_name_in_destination:
                            dest_table = table_name_in_destination
                            logger.debug(
                                f"Using provided name_in_destination '{dest_table}' for table {table_name}"
                            )
                        else:
                            # Fall back to case transformation if name_in_destination not available
                            dest_table = self._get_destination_table_name(
                                table_name, destination_platform
                            )
                            logger.debug(
                                f"No name_in_destination found for table {table_name}, using transformed name '{dest_table}'"
                            )

                        # Combine to create full destination table name
                        destination_table = f"{dest_schema}.{dest_table}"

                        # Process columns for lineage
                        column_lineage = self._extract_column_lineage(
                            table=table,
                            source_table=source_table,
                            destination_platform=destination_platform,
                            source_table_columns=source_table_columns,
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

        return lineage_list

    def _fill_connectors_lineage(self, connectors: List[Connector]) -> None:
        """
        Fill in lineage information for connectors by calling the API with enhanced diagnostics and robust error handling.
        """
        for connector in connectors:
            try:
                logger.info(
                    f"Extracting lineage for connector {connector.connector_id}"
                )

                # Determine destination platform
                destination_platform = self._get_destination_platform(connector)
                connector.additional_properties["destination_platform"] = (
                    destination_platform
                )
                logger.info(
                    f"Using destination platform {destination_platform} for connector {connector.connector_id}"
                )

                # Get schema information from API
                schemas = self.api_client.list_connector_schemas(connector.connector_id)

                # DIAGNOSTIC: Log detailed schema information
                self._log_schema_diagnostics(schemas)

                # If we have no columns at all, try direct fetching for each table
                if self._should_fetch_missing_columns(schemas):
                    logger.warning(
                        "No columns found in initial schema fetch. Attempting direct table column fetching."
                    )
                    self._fetch_missing_columns(connector.connector_id, schemas)
                    self._log_schema_diagnostics(schemas)  # Log updated stats

                # First, collect all source columns with their types for each table
                # This will help with generating column-level lineage
                source_table_columns = self._collect_source_columns(schemas)

                # Process schemas to extract lineage information
                lineage_list = self._process_schemas_for_lineage(
                    connector, schemas, source_table_columns
                )

                # Truncate if necessary
                if len(lineage_list) > MAX_TABLE_LINEAGE_PER_CONNECTOR:
                    logger.warning(
                        f"Connector {connector.connector_name} has {len(lineage_list)} tables, "
                        f"truncating to {MAX_TABLE_LINEAGE_PER_CONNECTOR}"
                    )
                    lineage_list = lineage_list[:MAX_TABLE_LINEAGE_PER_CONNECTOR]

                connector.lineage = lineage_list

                # Final stats logging
                self._log_lineage_stats(lineage_list, connector.connector_id)

            except Exception as e:
                logger.error(
                    f"Failed to extract lineage for connector {connector.connector_name}: {e}",
                    exc_info=True,
                )
                connector.lineage = []

    def _log_schema_diagnostics(self, schemas: List[Dict]) -> None:
        """Log diagnostic information about schemas and their columns."""
        total_columns = 0
        total_tables_with_columns = 0
        total_tables = 0

        for schema in schemas:
            schema_name = schema.get("name", "")
            for table in schema.get("tables", []):
                total_tables += 1
                table_name = table.get("name", "")
                columns = table.get("columns", [])

                if columns:
                    total_tables_with_columns += 1
                    total_columns += len(columns)
                    logger.info(
                        f"Table {schema_name}.{table_name} has {len(columns)} columns"
                    )

                    # DIAGNOSTIC: Print a sample of column names
                    column_names = [col.get("name", "unknown") for col in columns[:5]]
                    logger.info(f"Sample columns: {column_names}")
                else:
                    logger.warning(f"Table {schema_name}.{table_name} has NO columns")

        logger.info(
            f"SCHEMA STATS: {total_tables_with_columns}/{total_tables} tables have columns, total {total_columns} columns"
        )

    def _should_fetch_missing_columns(self, schemas: List[Dict]) -> bool:
        """Determine if we need to fetch missing columns based on schema content."""
        total_columns = 0
        total_tables = 0

        for schema in schemas:
            for table in schema.get("tables", []):
                total_tables += 1
                columns = table.get("columns", [])
                if columns:
                    total_columns += len(columns)

        return total_columns == 0 and total_tables > 0

    def _log_lineage_stats(
        self, lineage_list: List[TableLineage], connector_id: str
    ) -> None:
        """Log statistics about lineage processing."""
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

    def _fetch_missing_columns(self, connector_id: str, schemas: List[Dict]) -> None:
        """Attempt to fetch missing column information directly for each table."""
        for schema in schemas:
            schema_name = schema.get("name", "")
            for table in schema.get("tables", []):
                table_name = table.get("name", "")
                if not table.get("columns") and table.get("enabled", True):
                    try:
                        # Try direct column fetching
                        columns = self.api_client.get_table_columns(
                            connector_id, schema_name, table_name
                        )
                        if columns:
                            table["columns"] = columns
                            logger.info(
                                f"Directly fetched {len(columns)} columns for {schema_name}.{table_name}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to directly fetch columns for {schema_name}.{table_name}: {e}"
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
                if not isinstance(columns, list):
                    continue

                for column in columns:
                    if not isinstance(column, dict):
                        continue

                    column_name = column.get("name", "")
                    if not column_name:
                        continue

                    column_type = column.get("type", "")
                    source_columns[full_table_name][column_name] = column_type

        return source_columns

    def _process_columns_from_list(
        self, columns: List[Any], is_bigquery: bool, source_table: str
    ) -> List[ColumnLineage]:
        """
        Process columns from list format and create column lineage.
        Extracted from _extract_column_lineage to reduce complexity.
        """
        column_lineage = []

        for column in columns:
            col_name = None
            if isinstance(column, dict):
                col_name = column.get("name")
            elif isinstance(column, str):
                col_name = column

            if not col_name:
                continue

            # Skip Fivetran system columns
            if col_name.startswith("_fivetran"):
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
                logger.debug(f"Transformed name: {col_name} -> {dest_col_name}")

            # Add to lineage
            column_lineage.append(
                ColumnLineage(
                    source_column=col_name,
                    destination_column=dest_col_name,
                )
            )

        logger.info(
            f"Created {len(column_lineage)} column lineage entries for {source_table}"
        )
        return column_lineage

    def _process_columns_from_source(
        self, source_columns: Dict[str, str], is_bigquery: bool, source_table: str
    ) -> List[ColumnLineage]:
        """
        Create column lineage from source column information.
        Extracted from _extract_column_lineage to reduce complexity.
        """
        column_lineage = []

        logger.info(f"Using {len(source_columns)} columns from source table schema")

        # Create lineage for each source column
        for col_name in source_columns:
            if col_name.startswith("_fivetran"):
                continue

            # Transform destination column name based on platform
            dest_col_name = self._transform_column_name_for_platform(
                col_name, is_bigquery
            )

            column_lineage.append(
                ColumnLineage(
                    source_column=col_name,
                    destination_column=dest_col_name,
                )
            )

        logger.info(f"Created {len(column_lineage)} fallback column lineage entries")
        return column_lineage

    def _extract_column_lineage(
        self,
        table: Dict,
        source_table: str,
        destination_platform: str,
        source_table_columns: Dict[str, Dict[str, str]],
    ) -> List[ColumnLineage]:
        """
        Extract column-level lineage for a table with improved debugging and fallback.

        Args:
            table: Table data from API
            source_table: Full source table name (schema.table)
            destination_platform: Destination platform type
            source_table_columns: Dict mapping table names to column information

        Returns:
            List of ColumnLineage objects
        """
        # Get detailed logging for debugging
        logger.info(
            f"Extracting column lineage for {source_table} to {destination_platform}"
        )
        logger.debug(f"Table data keys: {list(table.keys())}")

        # Get columns from the API response
        columns = table.get("columns", [])

        # DIAGNOSTIC: Print details about what we got
        self._log_column_diagnostics(columns)

        # CRITICAL FIX: Convert dict format to list if needed
        if isinstance(columns, dict):
            columns = self._convert_column_dict_to_list(columns)

        is_bigquery = destination_platform.lower() == "bigquery"
        column_lineage = []

        # If we have columns, create lineage mappings
        if isinstance(columns, list) and columns:
            column_lineage = self._process_columns_from_list(
                columns, is_bigquery, source_table
            )
        else:
            # No column information from API, try other methods
            logger.warning(f"No usable column information for {source_table} from API")

            # Use source_table_columns if available (fallback method)
            source_columns = source_table_columns.get(source_table, {})
            if source_columns:
                column_lineage = self._process_columns_from_source(
                    source_columns, is_bigquery, source_table
                )
            else:
                # Try to use column mapping from config if available
                column_lineage = self._try_get_column_mapping_from_config(source_table)

        # Final check and log
        if column_lineage:
            logger.info(
                f"Returning {len(column_lineage)} column lineage mappings for {source_table}"
            )
        else:
            logger.warning(f"No column lineage mappings created for {source_table}")

        return column_lineage

    def _log_column_diagnostics(self, columns: Any) -> None:
        """Log diagnostic information about column data."""
        if isinstance(columns, list):
            logger.info(f"Found {len(columns)} columns in list format")
            if columns:
                sample = columns[:2]
                logger.debug(f"Sample columns: {sample}")
        elif isinstance(columns, dict):
            logger.info(f"Found {len(columns)} columns in dict format")
            if columns:
                sample_keys = list(columns.keys())[:2]
                logger.debug(f"Sample column keys: {sample_keys}")
        else:
            logger.warning(f"Columns in unexpected format: {type(columns)}")

    def _convert_column_dict_to_list(self, columns_dict: Dict) -> List[Dict]:
        """Convert column dictionary to list format for consistent processing."""
        columns_list = []
        for col_name, col_data in columns_dict.items():
            if isinstance(col_data, dict):
                col_data = col_data.copy()
                col_data["name"] = col_name
                columns_list.append(col_data)
            else:
                columns_list.append({"name": col_name})

        logger.info(f"Converted dict format to list with {len(columns_list)} columns")
        return columns_list

    def _try_get_column_mapping_from_config(
        self, source_table: str
    ) -> List[ColumnLineage]:
        """Try to get column mapping from configuration if available."""
        column_lineage = []

        # Check if there's a config attribute with column mapping info
        if hasattr(self, "config") and self.config:
            # Check for any attribute that might have column mappings
            for attr_name in dir(self.config):
                if "column_mapping" in attr_name.lower() and hasattr(
                    self.config, attr_name
                ):
                    mapping_attr = getattr(self.config, attr_name)
                    if isinstance(mapping_attr, dict) and source_table in mapping_attr:
                        column_mapping = mapping_attr.get(source_table, {})
                        logger.info(
                            f"Found config column mapping for {source_table}: {len(column_mapping)} columns"
                        )

                        for source_col, dest_col in column_mapping.items():
                            column_lineage.append(
                                ColumnLineage(
                                    source_column=source_col,
                                    destination_column=dest_col,
                                )
                            )

        return column_lineage

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
                f"Special handling for {connector.connector_type} connector: defaulting destination to 'snowflake'"
            )
            return "snowflake"

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

    def _transform_column_name_for_platform(
        self, column_name: str, is_bigquery: bool
    ) -> str:
        """
        Transform column name based on the destination platform with better handling of edge cases.
        """
        if not column_name:
            return ""

        if is_bigquery:
            # For BigQuery:
            # 1. Convert to lowercase
            # 2. Replace camelCase with snake_case
            # 3. Clean up any invalid characters

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
