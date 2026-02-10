"""Data structure for tracking profiling state across platform adapters."""

from dataclasses import dataclass, field
from typing import List, Optional

import sqlalchemy as sa


@dataclass
class ProfilingContext:
    """
    Encapsulates all state for profiling a single table.

    This context is passed through the profiling pipeline and modified
    by platform adapters to handle platform-specific setup (temp tables,
    sampling, etc.).
    """

    # Input parameters
    schema: Optional[str]
    table: Optional[str]
    custom_sql: Optional[str]
    pretty_name: str
    partition: Optional[str] = None

    # Working state (populated during profiling)
    sql_table: Optional[sa.Table] = None
    row_count: Optional[int] = None

    # Platform-specific resources (need cleanup)
    temp_table: Optional[str] = None
    temp_view: Optional[str] = None
    temp_schema: Optional[str] = None

    # Sampling information
    is_sampled: bool = False
    sample_percentage: Optional[float] = None

    # Resource tracking for cleanup
    resources_to_cleanup: List[str] = field(default_factory=list)

    def add_temp_resource(self, resource_type: str, resource_name: str) -> None:
        """Track a temporary resource that needs cleanup."""
        self.resources_to_cleanup.append(f"{resource_type}:{resource_name}")

    def get_table_identifier(self) -> str:
        """Get fully qualified table identifier."""
        if self.temp_table:
            # Use temp table if created
            return (
                f"{self.temp_schema}.{self.temp_table}"
                if self.temp_schema
                else self.temp_table
            )
        elif self.temp_view:
            # Use temp view if created
            return f"{self.schema}.{self.temp_view}" if self.schema else self.temp_view
        else:
            # Use original table
            if not self.table:
                raise ValueError(f"No table specified for {self.pretty_name}")
            return f"{self.schema}.{self.table}" if self.schema else self.table
