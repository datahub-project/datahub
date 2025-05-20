import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class BigQueryProfilerConfig:
    """
    Configuration settings for BigQuery profiling operations.
    Centralizes all configuration settings previously embedded directly in the profiler.
    """

    # Sample size and query limits
    sample_size: int = 100_000
    query_timeout: int = 60  # Default timeout in seconds
    max_partition_retries: int = 3
    max_queries_per_table: int = 50

    # Cache configuration
    max_cache_size: int = 1000
    enable_caching: bool = True

    # Performance optimization
    enable_partition_optimization: bool = True
    external_table_sampling_percent: float = 0.1
    large_table_sampling_percent: float = 1.0
    partition_batch_size: int = 5

    # Profiling scope
    include_table_lineage: bool = True
    include_column_lineage: bool = True
    include_column_descriptions: bool = True

    # Profiling depth
    profile_table_level_only: bool = False
    profile_partition_columns_only: bool = False

    # Filtering
    tables_pattern: Optional[List[str]] = None
    schema_pattern: Optional[List[str]] = None
    profile_pattern: Optional[Dict[str, List[str]]] = None

    # Custom configuration
    custom_sql: Optional[Dict[str, str]] = None
    column_type_overrides: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Error handling
    raise_on_error: bool = False
    ignore_errors_on_tables: Optional[List[str]] = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        # Ensure sampling percentages are between 0 and 100
        self.external_table_sampling_percent = min(
            max(self.external_table_sampling_percent, 0.01), 100.0
        )
        self.large_table_sampling_percent = min(
            max(self.large_table_sampling_percent, 0.01), 100.0
        )

        # Default empty lists if None
        self.ignore_errors_on_tables = self.ignore_errors_on_tables or []
        self.tables_pattern = self.tables_pattern or []
        self.schema_pattern = self.schema_pattern or []

        if self.query_timeout < 10:
            logger.warning(
                f"Very low query timeout ({self.query_timeout}s) may cause profiling failures. "
                f"Setting minimum of 10 seconds."
            )
            self.query_timeout = max(self.query_timeout, 10)

    def get_column_type_override(
        self, table_name: str, column_name: str
    ) -> Optional[str]:
        """
        Get the type override for a specific column if one exists.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            Override type or None if no override exists
        """
        if table_name in self.column_type_overrides:
            return self.column_type_overrides[table_name].get(column_name)
        return None

    def should_profile_table(
        self, project_id: str, dataset_name: str, table_name: str
    ) -> bool:
        """
        Determine if a table should be profiled based on configured patterns.

        Args:
            project_id: BigQuery project ID
            dataset_name: Dataset name
            table_name: Table name

        Returns:
            True if the table should be profiled, False otherwise
        """
        # If no patterns specified, include everything
        if (
            not self.tables_pattern
            and not self.schema_pattern
            and not self.profile_pattern
        ):
            return True

        # Check tables pattern
        if self.tables_pattern:
            import re

            for pattern in self.tables_pattern:
                if re.match(pattern, table_name):
                    return True

        # Check schema pattern
        if self.schema_pattern:
            import re

            for pattern in self.schema_pattern:
                if re.match(pattern, dataset_name):
                    return True

        # Check profile pattern (dataset.table combinations)
        if self.profile_pattern:
            dataset_patterns = self.profile_pattern.get(dataset_name, [])
            if dataset_patterns:
                import re

                for pattern in dataset_patterns:
                    if re.match(pattern, table_name):
                        return True

        return False

    def get_custom_sql_for_table(self, table_name: str) -> Optional[str]:
        """
        Get custom SQL for profiling a specific table if one exists.

        Args:
            table_name: Name of the table

        Returns:
            Custom SQL query string or None
        """
        if not self.custom_sql:
            return None

        # Try exact match first
        if table_name in self.custom_sql:
            return self.custom_sql[table_name]

        # Then try with regex patterns
        import re

        for pattern, sql in self.custom_sql.items():
            # Check if the pattern has regex special characters
            if any(c in pattern for c in ".*+?[](){}^$\\|"):
                if re.match(pattern, table_name):
                    return sql

        return None

    def get_timeout_for_table(self, table_size_bytes: Optional[int] = None) -> int:
        """
        Get an appropriate timeout value based on table size.

        Args:
            table_size_bytes: Size of the table in bytes, if known

        Returns:
            Timeout in seconds
        """
        if table_size_bytes is None:
            return self.query_timeout

        # Scale timeout based on table size, up to 5x the base timeout
        size_gb = table_size_bytes / 1_000_000_000
        if size_gb > 100:  # > 100 GB
            return min(self.query_timeout * 5, 300)
        elif size_gb > 10:  # > 10 GB
            return min(self.query_timeout * 3, 180)
        elif size_gb > 1:  # > 1 GB
            return min(self.query_timeout * 2, 120)
        else:
            return self.query_timeout

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryProfilerConfig":
        """
        Create a config object from a dictionary.

        Args:
            config_dict: Dictionary containing configuration values

        Returns:
            BigQueryProfilerConfig instance
        """
        # Filter out keys that aren't in the dataclass
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_dict = {k: v for k, v in config_dict.items() if k in valid_keys}

        return cls(**filtered_dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert config to dictionary.

        Returns:
            Dictionary representation of the config
        """
        return {key: getattr(self, key) for key in self.__dataclass_fields__}
