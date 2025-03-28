import concurrent.futures
import logging
import time
from typing import Any, Dict, List, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector, TableLineage
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_standard_api import FivetranStandardAPI

logger = logging.getLogger(__name__)


def enhance_with_parallel_processing(source_config, api_client):
    """
    Factory function to create a parallelized version of the Fivetran API client.

    Args:
        source_config: The FivetranSourceConfig instance
        api_client: The original FivetranAPIClient instance

    Returns:
        A ParallelFivetranAPI instance that can be used in place of FivetranStandardAPI
    """
    # If the user has specified a custom max_workers value, use it
    if hasattr(source_config, "max_workers"):
        max_workers = source_config.max_workers
    else:
        # Default to a reasonable number based on the system
        import multiprocessing

        max_workers = min(32, multiprocessing.cpu_count() * 2)

    source_config.max_workers = max_workers

    # Create and return the parallel API implementation
    return ParallelFivetranAPI(api_client, source_config)


class ParallelFivetranAPI(FivetranStandardAPI):
    """
    Enhanced implementation of FivetranStandardAPI with parallel processing capabilities
    to improve performance for schema retrieval and column lineage extraction.
    """

    def __init__(
        self,
        api_client: FivetranAPIClient,
        config: FivetranSourceConfig,
    ):
        """Initialize with a FivetranAPIClient instance."""
        super().__init__(api_client, config)
        # Additional caches to improve performance
        self._schema_processing_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._column_processing_cache: Dict[str, List[Dict[str, Any]]] = {}
        # Configure thread pool
        self._max_workers = min(
            32, (config.max_workers if hasattr(config, "max_workers") else 10)
        )
        logger.info(f"Initialized parallel API with {self._max_workers} workers")

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> List[Connector]:
        """
        Get a list of connectors filtered by the provided patterns.
        Enhanced with parallel processing for schema and lineage extraction.
        """
        # Get the basic connector list as in the original implementation
        connectors = super().get_allowed_connectors_list(
            connector_patterns, destination_patterns, report, syncs_interval
        )

        if not connectors:
            return []

        # Process lineage for all connectors in parallel
        with report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info(
                f"Extracting lineage in parallel for {len(connectors)} connectors"
            )
            self._process_lineage_in_parallel(connectors, report)

        return connectors

    def _process_lineage_in_parallel(
        self, connectors: List[Connector], report: FivetranSourceReport
    ) -> None:
        """Process lineage for multiple connectors in parallel."""
        start_time = time.time()
        connector_ids = [connector.connector_id for connector in connectors]

        # Create a mapping for easy lookup
        connector_map = {connector.connector_id: connector for connector in connectors}

        # Define the worker function that will be executed in parallel
        def process_connector_lineage(
            connector_id: str,
        ) -> Tuple[str, List[TableLineage]]:
            try:
                lineage = self.api_client.extract_table_lineage(connector_id)
                return connector_id, lineage
            except Exception as e:
                logger.error(
                    f"Error extracting lineage for connector {connector_id}: {e}"
                )
                report.report_failure(
                    message="Error extracting lineage for connector",
                    context=connector_id,
                    exc=e,
                )
                return connector_id, []

        # Process connectors in parallel batches for better control
        batch_size = 5  # Process 5 connectors at a time to avoid overwhelming the API
        results = []

        for i in range(0, len(connector_ids), batch_size):
            batch = connector_ids[i : i + batch_size]
            logger.info(
                f"Processing lineage batch {i // batch_size + 1}/{(len(connector_ids) + batch_size - 1) // batch_size}"
            )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            ) as executor:
                batch_results = list(executor.map(process_connector_lineage, batch))
                results.extend(batch_results)

            # Small delay between batches to avoid overwhelming the API
            if i + batch_size < len(connector_ids):
                time.sleep(1)

        # Update connectors with lineage results
        for connector_id, lineage in results:
            connector = connector_map.get(connector_id)
            if connector:
                connector.lineage = lineage
                # Log statistics
                if lineage:
                    total_column_mappings = sum(
                        len(table.column_lineage) for table in lineage
                    )
                    logger.info(
                        f"Extracted {len(lineage)} table lineage entries with {total_column_mappings} "
                        f"column mappings for connector {connector_id}"
                    )
                else:
                    logger.warning(f"No lineage extracted for connector {connector_id}")

        end_time = time.time()
        logger.info(
            f"Completed parallel lineage extraction in {end_time - start_time:.2f} seconds"
        )

    def _process_schemas_in_parallel(
        self, connectors: List[Connector]
    ) -> Dict[str, List[Dict]]:
        """Fetch and process schemas for multiple connectors in parallel."""
        schemas_by_connector = {}

        def get_schemas(connector_id: str) -> Tuple[str, List[Dict]]:
            try:
                if connector_id in self._schema_processing_cache:
                    logger.debug(f"Using cached schemas for connector {connector_id}")
                    return connector_id, self._schema_processing_cache[connector_id]

                schemas = self.api_client.list_connector_schemas(connector_id)
                self._schema_processing_cache[connector_id] = schemas
                return connector_id, schemas
            except Exception as e:
                logger.error(
                    f"Error fetching schemas for connector {connector_id}: {e}"
                )
                return connector_id, []

        connector_ids = [connector.connector_id for connector in connectors]

        # Process in batches
        batch_size = 5
        for i in range(0, len(connector_ids), batch_size):
            batch = connector_ids[i : i + batch_size]
            logger.info(
                f"Processing schema batch {i // batch_size + 1}/{(len(connector_ids) + batch_size - 1) // batch_size}"
            )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            ) as executor:
                for connector_id, schemas in executor.map(get_schemas, batch):
                    schemas_by_connector[connector_id] = schemas

            # Small delay between batches
            if i + batch_size < len(connector_ids):
                time.sleep(1)

        return schemas_by_connector

    def _batch_process_table_columns(self, tables_to_process: List[Dict]) -> None:
        """Process column information for multiple tables in parallel."""
        start_time = time.time()

        def process_table_columns(table_info: Dict) -> Tuple[str, str, str, List[Dict]]:
            connector_id = table_info["connector_id"]
            schema_name = table_info["schema"]
            table_name = table_info["table"]
            table_obj = table_info["table_obj"]

            # Create a cache key
            cache_key = f"{connector_id}:{schema_name}.{table_name}"

            if cache_key in self._column_processing_cache:
                logger.debug(f"Using cached columns for {cache_key}")
                return (
                    connector_id,
                    schema_name,
                    table_name,
                    self._column_processing_cache[cache_key],
                )

            try:
                columns = self.api_client.get_table_columns(
                    connector_id, schema_name, table_name
                )
                if columns:
                    # Update the table object with these columns
                    table_obj["columns"] = columns
                    logger.info(
                        f"Retrieved {len(columns)} columns for {schema_name}.{table_name}"
                    )
                    # Cache the results
                    self._column_processing_cache[cache_key] = columns
                    return connector_id, schema_name, table_name, columns

                # If no columns were found, try fallback methods
                logger.warning(
                    f"No columns found for {schema_name}.{table_name}, trying fallback"
                )
                fallback_columns = self._try_fallback_column_methods(
                    connector_id, schema_name, table_name, table_obj
                )
                if fallback_columns:
                    self._column_processing_cache[cache_key] = fallback_columns
                    return connector_id, schema_name, table_name, fallback_columns
            except Exception as e:
                logger.warning(
                    f"Error retrieving columns for {schema_name}.{table_name}: {e}"
                )

            return connector_id, schema_name, table_name, []

        # Process tables in smaller batches
        batch_size = 10
        for i in range(0, len(tables_to_process), batch_size):
            batch = tables_to_process[i : i + batch_size]
            logger.info(
                f"Processing column batch {i // batch_size + 1}/{(len(tables_to_process) + batch_size - 1) // batch_size}"
            )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            ) as executor:
                futures = [
                    executor.submit(process_table_columns, table_info)
                    for table_info in batch
                ]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        connector_id, schema_name, table_name, columns = future.result()
                        if columns:
                            # Success is logged in the worker function
                            pass
                        else:
                            logger.warning(
                                f"No columns retrieved for {schema_name}.{table_name}"
                            )
                    except Exception as e:
                        logger.error(f"Error processing table columns: {e}")

            # Small delay between batches
            if i + batch_size < len(tables_to_process):
                time.sleep(1)

        end_time = time.time()
        logger.info(
            f"Completed batch column processing in {end_time - start_time:.2f} seconds"
        )

    def _try_fallback_column_methods(
        self, connector_id: str, schema_name: str, table_name: str, table_obj: Dict
    ) -> List[Dict]:
        """
        Try fallback methods to get column information for a table.
        This is the same as in the original implementation but enhanced with caching.
        """
        # Try using the implementation from the parent class but with caching
        cache_key = f"{connector_id}:{schema_name}.{table_name}"
        if cache_key in self._column_processing_cache:
            logger.debug(f"Using cached fallback columns for {cache_key}")
            return self._column_processing_cache[cache_key]

        # Try metadata approach first
        try:
            metadata_path = f"/connectors/{connector_id}/metadata"
            metadata_response = self.api_client._make_request("GET", metadata_path)
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
                            self._column_processing_cache[cache_key] = formatted_columns
                            return formatted_columns
        except Exception as e:
            logger.warning(f"Failed to get metadata for {connector_id}: {e}")

        # Try to infer columns from similar tables as a last resort
        try:
            schemas = self.api_client.list_connector_schemas(connector_id)

            import difflib

            # First look for exact matches in different schemas
            for schema in schemas:
                schema_name_check = schema.get("name", "")
                if schema_name_check != schema_name:  # Different schema
                    for table in schema.get("tables", []):
                        if table.get("name") == table_name and table.get("columns"):
                            columns = table.get("columns", [])
                            table_obj["columns"] = columns
                            logger.info(
                                f"Used columns from exact name match in different schema {schema_name_check} for {table_name}"
                            )
                            self._column_processing_cache[cache_key] = columns
                            return columns

            # Look for similar tables in same schema
            target_schema = next(
                (s for s in schemas if s.get("name") == schema_name), None
            )
            if target_schema:
                best_match = None
                best_score = 0.7  # Minimum similarity threshold
                best_columns = []

                for table in target_schema.get("tables", []):
                    if not table.get("columns"):
                        continue

                    check_name = table.get("name", "")
                    if check_name == table_name:
                        continue

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
                        f"Inferred {len(best_columns)} columns for {schema_name}.{table_name} "
                        f"from similar table {best_match} (similarity: {best_score:.2f})"
                    )
                    self._column_processing_cache[cache_key] = best_columns
                    return best_columns
        except Exception as e:
            logger.warning(f"Failed to infer columns from similar tables: {e}")

        return []

    def _ensure_column_information(
        self, schemas: List[Dict], connector_id: str
    ) -> None:
        """
        Ensure we have column information for tables by fetching additional details if needed.
        Enhanced with parallel processing.
        """
        # First identify which tables need column information
        tables_missing_columns = []
        tables_with_columns = 0
        total_tables = 0

        for schema in schemas:
            schema_name = schema.get("name", "")
            for table in schema.get("tables", []):
                total_tables += 1

                # Skip tables that aren't enabled
                if not table.get("enabled", True):
                    continue

                # Check if table has column information
                columns = table.get("columns", [])
                if not columns:
                    # Add to list of tables needing column info
                    tables_missing_columns.append(
                        {
                            "connector_id": connector_id,
                            "schema": schema_name,
                            "table": table.get("name", ""),
                            "table_obj": table,  # Keep reference to the table object for updates
                        }
                    )
                else:
                    tables_with_columns += 1

        # Log statistics about column availability
        logger.info(
            f"Column information stats for connector {connector_id}: "
            f"{tables_with_columns} tables have columns, "
            f"{len(tables_missing_columns)} tables missing columns, "
            f"out of {total_tables} total tables"
        )

        if not tables_missing_columns:
            return

        # Process tables in parallel batches
        self._batch_process_table_columns(tables_missing_columns)
