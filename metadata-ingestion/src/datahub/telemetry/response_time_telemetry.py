"""
Response Time Telemetry Utility

A utility for recording and aggregating response time telemetry using
t-digest algorithm for efficient percentile calculation.
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from tdigest import TDigest

from datahub.configuration.telemetry_config import TelemetryConfig


@dataclass
class ResponseTimeTelemetry:
    """Telemetry for response times using t-digest algorithm."""

    min_time: float = float("inf")
    max_time: float = 0.0
    mean_time: float = 0.0
    count: int = 0
    total_time: float = 0.0

    # T-digest for streaming percentile calculation
    _tdigest: TDigest = field(default_factory=lambda: TDigest())
    _compression_threshold: int = 1000  # Compress when we reach this many points

    # Context information
    max_time_context: Optional[Dict[str, Any]] = None
    min_time_context: Optional[Dict[str, Any]] = None
    recent_contexts: List[Dict[str, Any]] = field(default_factory=list)
    max_recent_contexts_count: int = 10  # Store last N recent contexts

    # Configurable percentiles
    percentiles_list: List[int] = field(
        default_factory=lambda: [50, 90, 95, 99]
    )  # Default percentiles to calculate
    percentiles: Dict[int, float] = field(
        default_factory=dict
    )  # Calculated percentiles

    def add_time(
        self, response_time: float, context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a response time measurement with optional context."""
        # Add to t-digest for streaming percentile calculation
        self._tdigest.update(response_time)

        # Update min/max with context
        if response_time < self.min_time:
            self.min_time = response_time
            self.min_time_context = context.copy() if context else None

        if response_time > self.max_time:
            self.max_time = response_time
            self.max_time_context = context.copy() if context else None

        # Update basic statistics
        self.count += 1
        self.total_time += response_time
        self.mean_time = self.total_time / self.count

        # Store context information
        if context is not None:
            context_entry = {
                "response_time": response_time,
                "context": context,
                "timestamp": time.time(),
            }

            # Store in recent contexts (rolling window)
            self.recent_contexts.append(context_entry)
            if len(self.recent_contexts) > self.max_recent_contexts_count:
                self.recent_contexts.pop(0)

        # Calculate percentiles using t-digest
        if self.count > 0:
            self.percentiles = {
                p: self._tdigest.percentile(p) for p in self.percentiles_list
            }

        # Compress t-digest when we reach the threshold
        if self.count % self._compression_threshold == 0:
            self._tdigest.compress()

    def get_custom_percentile(self, percentile: float) -> float:
        """Calculate a custom percentile using t-digest."""
        if self.count == 0:
            return 0.0

        return self._tdigest.percentile(percentile)

    def get_context_stats(self) -> Dict[str, Any]:
        """Get statistics about the context storage."""
        return {
            "total_calls": self.count,
            "recent_contexts_count": len(self.recent_contexts),
            "max_recent_contexts_count": self.max_recent_contexts_count,
        }

    def configure_context_windows(
        self,
        max_recent_contexts_count: Optional[int] = None,
    ) -> None:
        """Configure context window size."""
        if max_recent_contexts_count is not None:
            self.max_recent_contexts_count = max(1, max_recent_contexts_count)

    def configure_percentiles(self, percentiles: List[int]) -> None:
        """Configure which percentiles to calculate."""
        if not percentiles:
            raise ValueError("Percentiles list cannot be empty")

        # Validate percentiles are between 0 and 100
        for p in percentiles:
            if not 0 <= p <= 100:
                raise ValueError(f"Percentile {p} must be between 0 and 100")

        self.percentiles_list = sorted(set(percentiles))  # Remove duplicates and sort
        # Recalculate percentiles if we have data
        if self.count > 0:
            self.percentiles = {
                p: self._tdigest.percentile(p) for p in self.percentiles_list
            }

    def get_percentiles(self) -> Dict[int, float]:
        """Get all calculated percentiles."""
        return self.percentiles.copy()

    def get_percentile(self, percentile: int) -> float:
        """Get a specific percentile value."""
        return self.percentiles.get(percentile, 0.0)

    def clear(self) -> None:
        """Clear all data and reset to initial state."""
        self.min_time = float("inf")
        self.max_time = 0.0
        self.mean_time = 0.0
        self.count = 0
        self.total_time = 0.0
        self.max_time_context = None
        self.min_time_context = None
        self.recent_contexts.clear()
        self.percentiles.clear()
        # Reset t-digest
        self._tdigest = TDigest()

    def __str__(self) -> str:
        """Return a formatted string representation of the telemetry."""
        lines = [
            f"Count: {self.count}",
            f"Min: {self.min_time:.3f}s",
            f"Max: {self.max_time:.3f}s",
            f"Mean: {self.mean_time:.3f}s",
        ]

        # Add percentiles
        percentiles = self.get_percentiles()
        for p in sorted(percentiles.keys()):
            lines.append(f"P{p}: {percentiles[p]:.3f}s")

        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """Convert telemetry to dictionary."""
        return {
            "min_time": self.min_time,
            "max_time": self.max_time,
            "mean_time": self.mean_time,
            "count": self.count,
            "total_time": self.total_time,
            "max_time_context": self.max_time_context,
            "min_time_context": self.min_time_context,
            "recent_contexts": self.recent_contexts,
            "percentiles": self.percentiles,
            "context_info": {
                "max_recent_contexts_count": self.max_recent_contexts_count,
                "total_stored": len(self.recent_contexts),
                "percentiles_list": self.percentiles_list,
            },
        }


class ResponseTimeMetrics:
    """Main metrics aggregator for different API call types."""

    def __init__(
        self,
        config: TelemetryConfig,
        group_name: Optional[str] = None,
    ):
        self._stats: Dict[str, ResponseTimeTelemetry] = defaultdict(
            ResponseTimeTelemetry
        )
        self._lock = threading.Lock()
        self._config = config
        self._group_name = group_name

    def _get_full_api_type(self, api_type: str) -> str:
        """Get the full API type with group name prefix if configured."""
        return f"{self._group_name}.{api_type}" if self._group_name else api_type

    def record_time(
        self,
        api_type: str,
        response_time: float,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a response time for a specific API type with optional context."""
        full_api_type = self._get_full_api_type(api_type)

        if (
            self._config.disable_response_time_collection
            or not self._config.capture_response_times_pattern.allowed(full_api_type)
            or self._config.capture_response_times_pattern.denied(full_api_type)
        ):
            return

        with self._lock:
            self._stats[full_api_type].add_time(response_time, context)

    def get_stats(self, api_type: str) -> Optional[ResponseTimeTelemetry]:
        """Get telemetry for a specific API type."""
        full_api_type = self._get_full_api_type(api_type)
        return self._stats.get(full_api_type)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get all telemetry as dictionaries."""
        return {api_type: stats.to_dict() for api_type, stats in self._stats.items()}

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all metrics."""
        summary = {}
        for api_type, stats in self._stats.items():
            summary[api_type] = {
                "min": stats.min_time,
                "max": stats.max_time,
                "mean": stats.mean_time,
                "p99": stats.p99_time,
                "p95": stats.p95_time,
                "p90": stats.p90_time,
                "p50": stats.p50_time,
                "count": stats.count,
            }
        return summary

    def reset_stats(self, api_type: Optional[str] = None) -> None:
        """Reset telemetry for a specific API type or all types."""
        with self._lock:
            if api_type:
                if api_type in self._stats:
                    del self._stats[api_type]
            else:
                self._stats.clear()

    def set_config(self, config: TelemetryConfig) -> None:
        """Set the telemetry config for filtering API types."""
        self._config = config

    def configure_context_windows(
        self,
        max_recent_contexts_count: Optional[int] = None,
    ) -> None:
        """Configure context window size for all API types."""
        with self._lock:
            for stats in self._stats.values():
                stats.configure_context_windows(
                    max_recent_contexts_count=max_recent_contexts_count,
                )

    def configure_percentiles(self, percentiles: List[int]) -> None:
        """Configure percentiles for all API types."""
        with self._lock:
            for stats in self._stats.values():
                stats.configure_percentiles(percentiles)

    def track_response_time(
        self, api_type: str, context: Optional[Dict[str, Any]] = None
    ) -> "ResponseTimeTracker":
        """
        Create a context manager for tracking response times.

        Args:
            api_type: The type of API call being tracked
            context: Optional context information to attach to this call

        Returns:
            ResponseTimeTracker context manager

        Usage:
            metrics = create_metrics_instance()
            with metrics.track_response_time("api_call", {"user_id": "123"}):
                result = api_client.get_data()
        """
        return ResponseTimeTracker(self, api_type, context)

    def __str__(self) -> str:
        """Return a formatted summary of all metrics."""
        lines = ["=== Response Time Telemetry Summary ==="]
        for api_type, stats in self._stats.items():
            lines.append(f"\n{api_type}:")
            lines.append(f"  Count: {stats.count}")
            lines.append(f"  Min: {stats.min_time:.3f}s")
            lines.append(f"  Max: {stats.max_time:.3f}s")
            lines.append(f"  Mean: {stats.mean_time:.3f}s")

            # Print configured percentiles
            percentiles = stats.get_percentiles()
            for p in sorted(percentiles.keys()):
                lines.append(f"  P{p}: {percentiles[p]:.3f}s")

        return "\n".join(lines)


class ResponseTimeTracker:
    """Context manager for tracking response times."""

    def __init__(
        self,
        metrics: ResponseTimeMetrics,
        api_type: str,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.metrics = metrics
        self.api_type = api_type
        self.context = context
        self.start_time: Optional[float] = None

    def __enter__(self) -> "ResponseTimeTracker":
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.start_time is not None:
            response_time = time.time() - self.start_time
            # Add exception info to context if there was an exception
            final_context = self.context.copy() if self.context else {}
            if exc_type is not None:
                final_context["exception"] = {
                    "type": exc_type.__name__ if exc_type else None,
                    "value": str(exc_val) if exc_val else None,
                }
            self.metrics.record_time(self.api_type, response_time, final_context)


def create_response_time_metrics_instance(
    config: TelemetryConfig,
    group_name: Optional[str] = None,
) -> ResponseTimeMetrics:
    """Create a new metrics instance."""
    return ResponseTimeMetrics(config=config, group_name=group_name)
