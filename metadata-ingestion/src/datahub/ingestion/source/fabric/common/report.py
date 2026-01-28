"""Report classes for Fabric clients."""

from dataclasses import dataclass


@dataclass
class FabricClientReport:
    """Report for Fabric REST API client operations.

    Tracks metrics for API calls, errors, and other client-level operations.
    This can be aggregated into workload-specific source reports.
    """

    request_count: int = 0
    error_count: int = 0

    def report_request(self) -> None:
        """Track a successful API request."""
        self.request_count += 1

    def report_error(self) -> None:
        """Track an API error."""
        self.error_count += 1
