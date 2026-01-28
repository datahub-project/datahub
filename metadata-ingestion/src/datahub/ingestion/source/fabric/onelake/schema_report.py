"""Report class for SQL Analytics Endpoint schema extraction."""

from dataclasses import dataclass


@dataclass
class SqlAnalyticsEndpointReport:
    """Report for SQL Analytics Endpoint schema extraction operations."""

    successes: int = 0
    failures: int = 0
    skipped: int = 0
