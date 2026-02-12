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

    def __post_init__(self) -> None:
        """Validate profiling context invariants."""
        # Must have either table or custom_sql
        if not self.table and not self.custom_sql:
            raise ValueError(
                "ProfilingContext requires either 'table' or 'custom_sql' to be specified"
            )

        # Sample percentage must be in range [0, 100]
        if self.sample_percentage is not None:
            if not (0 <= self.sample_percentage <= 100):
                raise ValueError(
                    f"sample_percentage must be in [0, 100], got {self.sample_percentage}"
                )

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
