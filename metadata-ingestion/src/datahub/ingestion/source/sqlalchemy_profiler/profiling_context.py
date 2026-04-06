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

    Requirements:
    - table: Always required, non-empty string (also when using custom_sql for context/naming)
    - schema: Optional (None for two-tier databases like MySQL/SQLite/Athena)
    - custom_sql: Optional profiling query override

    When both table and custom_sql are provided:
    - table provides the original table context for naming and logging
    - custom_sql takes precedence for actual profiling
    - Adapters create temp views/tables from custom_sql and use those for profiling
    """

    # Input parameters
    pretty_name: str
    table: str
    schema: Optional[str] = None
    custom_sql: Optional[str] = None
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
        # Table must be non-empty string (min_length=1 equivalent)
        if not self.table:
            raise ValueError(f"table must be a non-empty string, got: {self.table!r}")

        # Sample percentage must be in range [0, 100] (ge=0, le=100 equivalent)
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
            return f"{self.schema}.{self.table}" if self.schema else self.table
