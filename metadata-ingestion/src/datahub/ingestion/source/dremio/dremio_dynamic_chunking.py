"""
Dynamic chunking system for Dremio that detects system limits and uses intelligent two-phase processing.

This module implements a smart chunking approach that:
1. Detects Dremio's single_field_size_bytes limit from system tables
2. Uses two-phase processing: bulk query for small views, individual chunked queries for large views
3. Uses hash-based identification for reassembly to minimize memory footprint
"""

import hashlib
import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class DremioEditionType(Enum):
    """Dremio edition types for capability detection."""

    COMMUNITY = "COMMUNITY"
    ENTERPRISE = "ENTERPRISE"
    CLOUD = "CLOUD"


class DremioSystemLimits(BaseModel):
    """Represents detected Dremio system limits."""

    single_field_size_bytes: int
    detection_method: str
    is_detected: bool
    fallback_used: bool = False


class ViewProcessingPlan(BaseModel):
    """Plan for processing views based on size analysis."""

    small_views_query: str  # Single query for views under limit
    large_view_ids: List[str]  # List of view IDs that need individual processing
    total_small_views: int
    total_large_views: int
    size_limit_bytes: int


class DremioSystemDetector:
    """Detects Dremio system capabilities and limits."""

    def __init__(self, dremio_api):
        self.dremio_api = dremio_api
        self.edition = dremio_api.edition
        self._cached_limits: Optional[DremioSystemLimits] = None

    def detect_system_limits(self) -> DremioSystemLimits:
        """
        Detect Dremio's single_field_size_bytes limit from system configuration.

        Returns:
            DremioSystemLimits with detected or fallback values
        """
        if self._cached_limits:
            return self._cached_limits

        # Try different detection methods based on edition
        detection_methods = [
            self._detect_from_sys_options,
            self._detect_from_information_schema,
            self._detect_from_system_tables,
        ]

        for method in detection_methods:
            try:
                limits = method()
                if limits and limits.is_detected:
                    logger.info(
                        f"Detected Dremio limits using {limits.detection_method}: {limits.single_field_size_bytes} bytes"
                    )
                    self._cached_limits = limits
                    return limits
            except Exception as e:
                logger.debug(f"Detection method {method.__name__} failed: {e}")
                continue

        # Fallback to 32KB
        fallback_limits = DremioSystemLimits(
            single_field_size_bytes=32000,  # 32KB fallback
            detection_method="fallback",
            is_detected=False,
            fallback_used=True,
        )

        logger.warning(
            f"Could not detect Dremio single_field_size_bytes limit. "
            f"Using fallback: {fallback_limits.single_field_size_bytes} bytes"
        )

        self._cached_limits = fallback_limits
        return fallback_limits

    def _detect_from_sys_options(self) -> Optional[DremioSystemLimits]:
        """Try to detect limits from sys.options (Enterprise/Software/Community Editions)."""
        try:
            query = "SELECT name, string_val, num_val FROM sys.options WHERE name = 'planner.single_field_size_bytes'"
            results = self.dremio_api.execute_query(query)

            if results and len(results) > 0:
                result = results[0]
                # Try num_val first, then string_val
                size_bytes = result.get("num_val") or int(result.get("string_val", 0))

                if size_bytes > 0:
                    return DremioSystemLimits(
                        single_field_size_bytes=size_bytes,
                        detection_method="sys.options",
                        is_detected=True,
                    )
        except Exception as e:
            logger.debug(f"sys.options detection failed: {e}")

        return None

    def _detect_from_information_schema(self) -> Optional[DremioSystemLimits]:
        """Try to detect limits from information_schema tables."""
        # Try different possible table names
        possible_tables = [
            "information_schema.options",
            "information_schema.system_options",
            "information_schema.configuration",
            "information_schema.settings",
        ]

        for table in possible_tables:
            try:
                # First check if table exists
                check_query = f"SELECT * FROM {table} LIMIT 1"
                self.dremio_api.execute_query(check_query)

                # If table exists, look for the setting
                query = f"""
                SELECT * FROM {table} 
                WHERE LOWER(name) LIKE '%single%field%size%' 
                   OR LOWER(name) LIKE '%field%size%bytes%'
                   OR LOWER(name) LIKE '%planner%single%field%'
                """
                results = self.dremio_api.execute_query(query)

                if results:
                    for result in results:
                        # Try to extract size from various possible column names
                        size_bytes = self._extract_size_from_result(result)
                        if size_bytes:
                            return DremioSystemLimits(
                                single_field_size_bytes=size_bytes,
                                detection_method=f"{table}",
                                is_detected=True,
                            )

            except Exception as e:
                logger.debug(f"Table {table} detection failed: {e}")
                continue

        return None

    def _detect_from_system_tables(self) -> Optional[DremioSystemLimits]:
        """Try to detect from other system tables."""
        try:
            # First, discover available system tables
            show_tables_query = "SHOW TABLES IN sys"
            tables_result = self.dremio_api.execute_query(show_tables_query)

            available_tables = [
                row.get("TABLE_NAME", "") for row in tables_result or []
            ]
            logger.debug(f"Available sys tables: {available_tables}")

            # Look for configuration-related tables
            config_tables = [
                table
                for table in available_tables
                if any(
                    keyword in table.lower()
                    for keyword in ["option", "config", "setting", "param"]
                )
            ]

            for table in config_tables:
                try:
                    query = f"SELECT * FROM sys.{table} LIMIT 10"
                    results = self.dremio_api.execute_query(query)

                    if results:
                        for result in results:
                            size_bytes = self._extract_size_from_result(result)
                            if size_bytes:
                                return DremioSystemLimits(
                                    single_field_size_bytes=size_bytes,
                                    detection_method=f"sys.{table}",
                                    is_detected=True,
                                )
                except Exception as e:
                    logger.debug(f"sys.{table} detection failed: {e}")
                    continue

        except Exception as e:
            logger.debug(f"System tables discovery failed: {e}")

        return None

    def _extract_size_from_result(self, result: Dict[str, Any]) -> Optional[int]:
        """Extract size bytes from a configuration result row."""
        # Look for single_field_size_bytes in various formats
        for key, value in result.items():
            key_lower = key.lower()

            # Check if this looks like our setting
            if (
                "single" in key_lower and "field" in key_lower and "size" in key_lower
            ) or (
                "field" in key_lower and "size" in key_lower and "bytes" in key_lower
            ):
                try:
                    # Try to convert value to int
                    if isinstance(value, (int, float)):
                        return int(value)
                    elif isinstance(value, str):
                        # Try to parse string value
                        if value.isdigit():
                            return int(value)
                        # Handle values like "32000 bytes" or "32KB"
                        import re

                        match = re.search(r"(\d+)", value)
                        if match:
                            return int(match.group(1))
                except (ValueError, TypeError):
                    continue

        return None


class DremioSmartChunker:
    """Smart chunker that uses two-phase processing based on detected limits."""

    def __init__(self, dremio_api):
        self.dremio_api = dremio_api
        self.detector = DremioSystemDetector(dremio_api)
        self.system_limits = self.detector.detect_system_limits()

    def create_processing_plan(self, containers: List[Any]) -> ViewProcessingPlan:
        """
        Create a processing plan that separates small and large views.

        Args:
            containers: List of container objects to process

        Returns:
            ViewProcessingPlan with optimized queries
        """
        # First, get view size information
        view_sizes = self._analyze_view_sizes(containers)

        # Separate views by size
        size_limit = self.system_limits.single_field_size_bytes
        small_views = []
        large_view_ids = []

        for view_info in view_sizes:
            if view_info["estimated_size"] <= size_limit:
                small_views.append(view_info)
            else:
                large_view_ids.append(view_info["view_id"])

        # Create optimized query for small views
        small_views_query = self._create_bulk_query_for_small_views(
            containers, small_views
        )

        return ViewProcessingPlan(
            small_views_query=small_views_query,
            large_view_ids=large_view_ids,
            total_small_views=len(small_views),
            total_large_views=len(large_view_ids),
            size_limit_bytes=size_limit,
        )

    def _analyze_view_sizes(self, containers: List[Any]) -> List[Dict[str, Any]]:
        """
        Analyze view sizes to determine processing strategy.

        This uses a lightweight query to get view definition lengths without
        retrieving the full content.
        """
        try:
            # Build schema list from containers
            schema_names = [
                f"'{container.container_name.lower()}'" for container in containers
            ]
            if not schema_names:
                return []

            # Use the intelligent query from DremioSQLQueries
            from datahub.ingestion.source.dremio.dremio_sql_queries import (
                DremioSQLQueries,
            )

            formatted_query = DremioSQLQueries.get_view_size_analysis_query(
                ", ".join(schema_names)
            )

            results = self.dremio_api.execute_query(formatted_query)

            view_info = []
            for result in results or []:
                # Create a hash for identification
                view_id = self._create_view_hash(
                    result.get("TABLE_SCHEMA", ""),
                    result.get("TABLE_NAME", ""),
                    result.get("VIEW_DEFINITION_PREVIEW", ""),
                )

                view_info.append(
                    {
                        "view_id": view_id,
                        "table_schema": result.get("TABLE_SCHEMA"),
                        "table_name": result.get("TABLE_NAME"),
                        "full_table_path": result.get("FULL_TABLE_PATH"),
                        "estimated_size": result.get("VIEW_DEFINITION_LENGTH", 0),
                    }
                )

            logger.info(f"Analyzed {len(view_info)} views for size-based processing")
            return view_info

        except Exception as e:
            logger.warning(f"View size analysis failed: {e}. Using fallback approach.")
            return []

    def _create_bulk_query_for_small_views(
        self, containers: List[Any], small_views: List[Dict]
    ) -> str:
        """Create optimized bulk query for views under the size limit."""
        if not small_views:
            return ""

        # Get the base query template
        from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

        if self.dremio_api.edition.name == "COMMUNITY":
            base_query = DremioSQLQueries.QUERY_DATASETS_CE
        else:
            base_query = DremioSQLQueries.QUERY_DATASETS_EE

        # For small views, we can use the original single VIEW_DEFINITION column
        # since they're under the limit
        optimized_query = base_query.replace(
            # Replace the chunked columns with simple VIEW_DEFINITION
            """CASE 
                WHEN LENGTH(V.VIEW_DEFINITION) <= 32000 THEN V.VIEW_DEFINITION
                ELSE SUBSTRING(V.VIEW_DEFINITION, 1, 32000)
            END AS VIEW_DEFINITION_CHUNK_0,
            CASE 
                WHEN LENGTH(V.VIEW_DEFINITION) > 32000 THEN SUBSTRING(V.VIEW_DEFINITION, 32001, 32000)
                ELSE NULL
            END AS VIEW_DEFINITION_CHUNK_1,
            CASE 
                WHEN LENGTH(V.VIEW_DEFINITION) > 64000 THEN SUBSTRING(V.VIEW_DEFINITION, 64001, 32000)
                ELSE NULL
            END AS VIEW_DEFINITION_CHUNK_2,
            CASE 
                WHEN LENGTH(V.VIEW_DEFINITION) > 96000 THEN SUBSTRING(V.VIEW_DEFINITION, 96001, 32000)
                ELSE NULL
            END AS VIEW_DEFINITION_CHUNK_3,
            LENGTH(V.VIEW_DEFINITION) AS VIEW_DEFINITION_TOTAL_LENGTH,""",
            "V.VIEW_DEFINITION,",
        )

        return optimized_query

    def process_large_view_individually(
        self, view_id: str, table_schema: str, table_name: str
    ) -> Dict[str, Any]:
        """
        Process a large view individually using chunked single-column queries.

        Args:
            view_id: Hash-based identifier for the view
            table_schema: Schema name
            table_name: Table name

        Returns:
            Dictionary with reassembled view data
        """
        try:
            # Get view definition in chunks
            chunks = []
            chunk_size = self.system_limits.single_field_size_bytes

            for chunk_index in range(4):  # Support up to 4 chunks (128KB total)
                start_pos = chunk_index * chunk_size + 1
                (chunk_index + 1) * chunk_size

                chunk_query = f"""
                SELECT 
                    '{table_schema}' AS TABLE_SCHEMA,
                    '{table_name}' AS TABLE_NAME,
                    CONCAT('{table_schema}', '.', '{table_name}') AS FULL_TABLE_PATH,
                    SUBSTRING(V.VIEW_DEFINITION, {start_pos}, {chunk_size}) AS VIEW_DEFINITION_CHUNK,
                    LENGTH(V.VIEW_DEFINITION) AS VIEW_DEFINITION_TOTAL_LENGTH,
                    {chunk_index} AS CHUNK_INDEX
                FROM INFORMATION_SCHEMA.VIEWS V
                WHERE V.TABLE_SCHEMA = '{table_schema}' 
                  AND V.TABLE_NAME = '{table_name}'
                  AND LENGTH(V.VIEW_DEFINITION) >= {start_pos}
                """

                result = self.dremio_api.execute_query(chunk_query)

                if result and len(result) > 0:
                    chunk_data = result[0]
                    chunk_content = chunk_data.get("VIEW_DEFINITION_CHUNK", "")

                    if chunk_content:
                        chunks.append(
                            {
                                f"VIEW_DEFINITION_CHUNK_{chunk_index}": chunk_content,
                                "VIEW_DEFINITION_TOTAL_LENGTH": chunk_data.get(
                                    "VIEW_DEFINITION_TOTAL_LENGTH", 0
                                ),
                            }
                        )
                    else:
                        break  # No more chunks
                else:
                    break  # No more chunks

            # Reassemble the view definition
            if chunks:
                # Merge all chunk data
                merged_data = {
                    "TABLE_SCHEMA": table_schema,
                    "TABLE_NAME": table_name,
                    "FULL_TABLE_PATH": f"{table_schema}.{table_name}",
                    "VIEW_DEFINITION_TOTAL_LENGTH": chunks[0].get(
                        "VIEW_DEFINITION_TOTAL_LENGTH", 0
                    ),
                }

                # Add all chunks
                for chunk in chunks:
                    merged_data.update(chunk)

                logger.info(
                    f"Successfully processed large view {table_schema}.{table_name} in {len(chunks)} chunks"
                )
                return merged_data

        except Exception as e:
            logger.error(
                f"Failed to process large view {table_schema}.{table_name}: {e}"
            )

        return {}

    def _create_view_hash(self, schema: str, table: str, preview: str) -> str:
        """Create a hash-based identifier for a view."""
        identifier = f"{schema}.{table}.{preview}"
        return hashlib.md5(identifier.encode()).hexdigest()[:16]

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get statistics about the chunking system."""
        return {
            "system_limits": {
                "single_field_size_bytes": self.system_limits.single_field_size_bytes,
                "detection_method": self.system_limits.detection_method,
                "is_detected": self.system_limits.is_detected,
                "fallback_used": self.system_limits.fallback_used,
            },
            "dremio_edition": self.dremio_api.edition.name
            if hasattr(self.dremio_api.edition, "name")
            else str(self.dremio_api.edition),
        }
