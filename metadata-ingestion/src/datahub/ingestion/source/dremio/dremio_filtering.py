"""
Dremio filtering utilities that provide consistent AllowDenyPattern behavior
while handling Dremio's unique hierarchical folder structure.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from pydantic import BaseModel

from datahub.configuration.common import AllowDenyPattern

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig

logger = logging.getLogger(__name__)


class DremioFilterHelper(BaseModel):
    """
    Helper class to provide consistent filtering behavior for Dremio that matches
    other DataHub connectors while handling Dremio's unique folder discovery.
    """

    schema_pattern: AllowDenyPattern
    dataset_pattern: AllowDenyPattern
    include_system_tables: bool = False

    class Config:
        # Allow methods to be defined
        arbitrary_types_allowed = True

    def is_schema_allowed(self, schema_name: str) -> bool:
        """
        Check if a schema should be included based on schema_pattern.

        Args:
            schema_name: The schema name to check

        Returns:
            True if schema should be included
        """
        return self.schema_pattern.allowed(schema_name)

    def is_dataset_allowed(
        self, dataset_name: str, dataset_type: str = "table"
    ) -> bool:
        """
        Check if a dataset should be included based on dataset_pattern.

        Args:
            dataset_name: Full dataset name in format "source.schema.table"
            dataset_type: Either "table" or "view" (not used, kept for compatibility)

        Returns:
            True if dataset should be included
        """
        return self.dataset_pattern.allowed(dataset_name)

    def is_system_table(self, schema_name: str, table_name: str) -> bool:
        """
        Check if a table is a system table that should be filtered.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            True if this is a system table
        """
        schema_lower = schema_name.lower()
        table_lower = table_name.lower()

        # Common system schema patterns
        system_schemas = [
            "information_schema",
            "sys",
            "system",
            "__system",
        ]

        # Check if schema is a system schema
        for sys_schema in system_schemas:
            if schema_lower == sys_schema or schema_lower.startswith(f"{sys_schema}."):
                return True

        # Check for system table patterns
        system_table_prefixes = [
            "__",
            "sys_",
            "system_",
            "information_schema_",
        ]

        return any(table_lower.startswith(prefix) for prefix in system_table_prefixes)

    def should_include_dataset(
        self,
        source_name: str,
        schema_name: str,
        table_name: str,
        dataset_type: str = "table",
    ) -> bool:
        """
        Comprehensive check if a dataset should be included.

        Args:
            source_name: Dremio source name
            schema_name: Schema name
            table_name: Table name
            dataset_type: Either "table" or "view"

        Returns:
            True if dataset should be included
        """
        # Check system tables first
        if self.is_system_table(schema_name, table_name):
            if not self.include_system_tables:
                logger.debug(
                    f"Excluding system table: {source_name}.{schema_name}.{table_name}"
                )
                return False

        # Check schema pattern
        if not self.is_schema_allowed(schema_name):
            logger.debug(f"Schema {schema_name} excluded by schema_pattern")
            return False

        # Build full dataset name for pattern matching
        full_dataset_name = f"{source_name}.{schema_name}.{table_name}"

        # Check dataset pattern
        if not self.is_dataset_allowed(full_dataset_name, dataset_type):
            logger.debug(
                f"Dataset {full_dataset_name} excluded by {dataset_type}_pattern"
            )
            return False

        return True

    def generate_sql_filters(self, container_name: str) -> Dict[str, str]:
        """
        Generate SQL filter conditions for Dremio queries.

        Args:
            container_name: The container name to filter for

        Returns:
            Dictionary with SQL filter strings
        """
        filters = {
            "schema_pattern": "",
            "deny_schema_pattern": "",
            "system_table_filter": "",
        }

        # Generate schema filters
        if self.schema_pattern.allow and self.schema_pattern.allow != [".*"]:
            allow_conditions = []
            for pattern in self.schema_pattern.allow:
                if pattern != ".*":  # Skip default allow-all pattern
                    allow_conditions.append(
                        f"REGEXP_LIKE(T.TABLE_SCHEMA, '{pattern}', 'i')"
                    )

            if allow_conditions:
                filters["schema_pattern"] = f"AND ({' OR '.join(allow_conditions)})"

        if self.schema_pattern.deny:
            deny_conditions = []
            for pattern in self.schema_pattern.deny:
                deny_conditions.append(f"REGEXP_LIKE(T.TABLE_SCHEMA, '{pattern}', 'i')")

            if deny_conditions:
                filters["deny_schema_pattern"] = (
                    f"AND NOT ({' OR '.join(deny_conditions)})"
                )

        # Generate system table filter
        if not self.include_system_tables:
            system_conditions = [
                "LOWER(T.TABLE_SCHEMA) NOT IN ('information_schema', 'sys', 'system')",
                "LOWER(T.TABLE_SCHEMA) NOT LIKE '__system%'",
                "LOWER(T.TABLE_NAME) NOT LIKE '__%'",
                "LOWER(T.TABLE_NAME) NOT LIKE 'sys_%'",
                "LOWER(T.TABLE_NAME) NOT LIKE 'system_%'",
            ]
            filters["system_table_filter"] = f"AND ({' AND '.join(system_conditions)})"

        return filters

    def get_filtered_containers(self, containers: List[Any]) -> List[Any]:
        """
        Filter containers based on schema patterns.

        Args:
            containers: List of Dremio container objects

        Returns:
            Filtered list of containers
        """
        filtered_containers = []

        for container in containers:
            container_name = getattr(container, "container_name", str(container))

            if self.is_schema_allowed(container_name):
                filtered_containers.append(container)
            else:
                logger.debug(f"Container {container_name} excluded by schema_pattern")

        return filtered_containers

    def log_filtering_summary(
        self,
        total_schemas: int,
        total_datasets: int,
        filtered_schemas: int,
        filtered_datasets: int,
    ) -> None:
        """
        Log a summary of filtering results.

        Args:
            total_schemas: Total number of schemas discovered
            total_datasets: Total number of datasets discovered
            filtered_schemas: Number of schemas after filtering
            filtered_datasets: Number of datasets after filtering
        """
        logger.info(
            f"Dremio filtering summary: "
            f"Schemas: {filtered_schemas}/{total_schemas} included, "
            f"Datasets: {filtered_datasets}/{total_datasets} included"
        )

        if not self.include_system_tables:
            logger.info(
                "System tables excluded (set include_system_tables=true to include)"
            )

        # Log pattern information
        if self.schema_pattern.allow != [".*"]:
            logger.info(f"Schema allow patterns: {self.schema_pattern.allow}")
        if self.schema_pattern.deny:
            logger.info(f"Schema deny patterns: {self.schema_pattern.deny}")
        if self.dataset_pattern.allow != [".*"]:
            logger.info(f"Dataset allow patterns: {self.dataset_pattern.allow}")
        if self.dataset_pattern.deny:
            logger.info(f"Dataset deny patterns: {self.dataset_pattern.deny}")


def create_dremio_filter_helper(config: "DremioSourceConfig") -> DremioFilterHelper:
    """
    Factory function to create a DremioFilterHelper from configuration.

    Args:
        config: DremioSourceConfig object

    Returns:
        Configured DremioFilterHelper instance
    """
    return DremioFilterHelper(
        schema_pattern=config.schema_pattern,
        dataset_pattern=config.dataset_pattern,
        include_system_tables=config.include_system_tables,
    )
