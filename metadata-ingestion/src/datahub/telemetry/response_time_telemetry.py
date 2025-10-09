"""
Response Time Telemetry Utility

A utility for recording and aggregating response time telemetry using
t-digest algorithm for efficient percentile calculation without storing all data.
Beneficial for high-volume API call use cases where we cannot store all response time data.
"""

import threading
import time
from collections import defaultdict, deque
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
    recent_contexts: deque = field(default_factory=deque)
    max_recent_contexts_count: int = 10  # Store last N recent contexts

    # Configurable percentiles
    percentiles_list: List[int] = field(
        default_factory=lambda: [50, 90, 95, 99]
    )  # Default percentiles to calculate
    percentiles: Dict[int, float] = field(
        default_factory=dict
    )  # Calculated percentiles

    # Recent context control
    disable_recent_contexts: bool = False  # Disable recent context storage

    def add_time(
        self,
        response_time: float,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a response time measurement with optional context.

        Args:
            response_time: The response time in seconds
            context: Optional context information to store with this measurement
        """
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

        # Store context information (only if recent contexts are not disabled)
        if context is not None and not self.disable_recent_contexts:
            context_entry = {
                "response_time": response_time,
                "context": context,
                "timestamp": time.time(),
            }

            # Fixed window size
            self.recent_contexts.append(context_entry)
            if len(self.recent_contexts) > self.max_recent_contexts_count:
                self.recent_contexts.popleft()

        # Calculate percentiles using t-digest
        if self.count > 0:
            self.percentiles = {
                p: self._tdigest.percentile(p) for p in self.percentiles_list
            }

        # Compress t-digest when we reach the threshold
        if self.count % self._compression_threshold == 0:
            self._tdigest.compress()

    def configure_context_windows(
        self,
        max_recent_contexts_count: Optional[int],
    ) -> "ResponseTimeTelemetry":
        """Configure context window size.

        Args:
            max_recent_contexts_count: Maximum number of recent contexts to store

        Returns:
            Self for method chaining

        Raises:
            ValueError: If called after data has been added
        """
        if self.count > 0:
            raise ValueError(
                "Context windows can only be configured before any data is added"
            )

        if max_recent_contexts_count:
            self.max_recent_contexts_count = max(1, max_recent_contexts_count)

        return self

    def configure_percentiles(self, percentiles: List[int]) -> "ResponseTimeTelemetry":
        """Configure which percentiles to calculate.

        Args:
            percentiles: List of percentile values (0-100) to calculate

        Returns:
            Self for method chaining

        Raises:
            ValueError: If called after data has been added or invalid percentiles
        """
        if self.count > 0:
            raise ValueError(
                "Percentiles can only be configured before any data is added"
            )

        if percentiles:
            # Validate percentiles are between 0 and 100
            for p in percentiles:
                if not 0 <= p <= 100:
                    raise ValueError(f"Percentile {p} must be between 0 and 100")

            self.percentiles_list = sorted(
                set(percentiles)
            )  # Remove duplicates and sort

        return self

    def configure_disable_recent_contexts(
        self, disable_recent_contexts: Optional[bool]
    ) -> "ResponseTimeTelemetry":
        """Configure whether to disable recent context storage.

        Args:
            disable_recent_contexts: Whether to disable recent context storage

        Returns:
            Self for method chaining

        Raises:
            ValueError: If called after data has been added
        """
        if self.count > 0:
            raise ValueError(
                "Recent context storage can only be configured before any data is added"
            )

        if disable_recent_contexts is not None:
            self.disable_recent_contexts = disable_recent_contexts

        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert telemetry to dictionary.

        Returns:
            Dictionary containing all telemetry data
        """
        return {
            "min": {
                "time_in_secs": round(self.min_time, 3),
                "context": self.min_time_context,
            },
            "max": {
                "time_in_secs": round(self.max_time, 3),
                "context": self.max_time_context,
            },
            "mean": round(self.mean_time, 3),
            "count": self.count,
            "total_time_in_secs": round(self.total_time, 3),
            **(
                {"recent_contexts": list(self.recent_contexts)}
                if not self.disable_recent_contexts
                else {}
            ),
            "percentiles_in_secs": {
                p: round(v, 3) for p, v in self.percentiles.items()
            },
        }


class ResponseTimeMetrics:
    """Main metrics aggregator for different API call types."""

    def __init__(
        self,
        config: TelemetryConfig,
        percentiles: Optional[List[int]],
        recent_contexts_window_size: Optional[int],
        disable_recent_contexts: Optional[bool],
    ) -> None:
        """Initialize ResponseTimeMetrics.

        Args:
            config: Telemetry configuration for filtering API types
            percentiles: List of percentiles to calculate (default: [50, 90, 95, 99])
            recent_contexts_window_size: Maximum number of recent contexts to store
            disable_recent_contexts: Whether to disable recent context storage
        """
        self._stats: Dict[str, ResponseTimeTelemetry] = defaultdict(
            lambda: ResponseTimeTelemetry()
            .configure_percentiles(percentiles)
            .configure_context_windows(recent_contexts_window_size)
            .configure_disable_recent_contexts(disable_recent_contexts)
        )
        self._lock = threading.Lock()
        self._config = config

    def record_time(
        self,
        api_type: str,
        response_time: float,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a response time for a specific API type with optional context.

        Args:
            api_type: The type of API call being recorded
            response_time: The response time in seconds
            context: Optional context information to store with this measurement
        """
        if (
            self._config.disable_response_time_collection
            or not self._config.capture_response_times_pattern.allowed(api_type)
            or self._config.capture_response_times_pattern.denied(api_type)
        ):
            return

        with self._lock:
            self._stats[api_type].add_time(response_time, context)

    def get_stats(self, api_type: str) -> Optional[ResponseTimeTelemetry]:
        """Get telemetry for a specific API type.

        Args:
            api_type: The type of API call to get stats for

        Returns:
            ResponseTimeTelemetry object or None if not found
        """
        return self._stats.get(api_type)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get all telemetry as dictionaries.

        Returns:
            Dictionary mapping API types to their telemetry data
        """
        # Create a snapshot to avoid holding locks while converting to dict
        stats_snapshot = {}
        for api_type in list(self._stats.keys()):
            with self._lock:
                stats_snapshot[api_type] = self._stats[api_type].to_dict()
        return stats_snapshot

    def track_response_time(
        self,
        api_type: str,
        context: Optional[Dict[str, Any]] = None,
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


class ResponseTimeTracker:
    """Context manager for tracking response times."""

    def __init__(
        self,
        metrics: ResponseTimeMetrics,
        api_type: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize ResponseTimeTracker.

        Args:
            metrics: The ResponseTimeMetrics instance to record to
            api_type: The type of API call being tracked
            context: Optional context information to attach to this call
        """
        self.metrics = metrics
        self.api_type = api_type
        self.context = context
        self.start_time: Optional[float] = None

    def __enter__(self) -> "ResponseTimeTracker":
        """Enter the context manager and start timing.

        Returns:
            Self for use in with statement
        """
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and record the response time.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self.start_time is not None:
            response_time = time.time() - self.start_time
            self.metrics.record_time(self.api_type, response_time, self.context)


def create_response_time_metrics_instance(
    config: TelemetryConfig,
    percentiles: Optional[List[int]] = None,
    recent_contexts_window_size: Optional[int] = None,
    disable_recent_contexts: Optional[bool] = None,
) -> ResponseTimeMetrics:
    """Create a new metrics instance.

    Args:
        config: Telemetry configuration for filtering API types
        percentiles: List of percentiles to calculate (default: [50, 90, 95, 99])
        recent_contexts_window_size: Maximum number of recent contexts to store
        disable_recent_contexts: Whether to disable recent context storage

    Returns:
        New ResponseTimeMetrics instance
    """
    return ResponseTimeMetrics(
        config=config,
        percentiles=percentiles,
        recent_contexts_window_size=recent_contexts_window_size,
        disable_recent_contexts=disable_recent_contexts,
    )
