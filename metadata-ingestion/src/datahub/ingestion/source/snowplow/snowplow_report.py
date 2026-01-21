"""
Custom report class for Snowplow source.

Tracks extraction statistics, errors, warnings, and API metrics.
"""

import logging
import random
import time
from dataclasses import dataclass, field
from types import TracebackType
from typing import Dict, List, Optional, Type

from datahub.ingestion.source.snowplow.constants import SchemaType
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class APIMetrics:
    """
    Tracks API call metrics for a single endpoint.

    Used for performance monitoring and tuning of API interactions.
    Stores individual latencies to compute percentiles at report time.
    """

    call_count: int = 0
    total_latency_ms: float = 0.0
    latencies_ms: List[float] = field(default_factory=list)
    error_count: int = 0

    # Keep limited history for percentile calculation (avoid unbounded memory)
    MAX_LATENCY_SAMPLES: int = 1000

    def record_call(self, latency_ms: float, is_error: bool = False) -> None:
        """Record a single API call."""
        self.call_count += 1
        self.total_latency_ms += latency_ms
        if is_error:
            self.error_count += 1

        # Keep only recent samples for percentile calculation
        if len(self.latencies_ms) < self.MAX_LATENCY_SAMPLES:
            self.latencies_ms.append(latency_ms)
        else:
            # Reservoir sampling: randomly replace to maintain representative sample
            idx = random.randint(0, self.call_count - 1)
            if idx < self.MAX_LATENCY_SAMPLES:
                self.latencies_ms[idx] = latency_ms

    @property
    def avg_latency_ms(self) -> float:
        """Average latency in milliseconds."""
        return self.total_latency_ms / self.call_count if self.call_count > 0 else 0.0

    def get_percentile(self, percentile: float) -> float:
        """
        Get latency at given percentile (0-100).

        Args:
            percentile: Percentile value (e.g., 50 for p50, 99 for p99)

        Returns:
            Latency at that percentile in milliseconds
        """
        if not self.latencies_ms:
            return 0.0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int((percentile / 100.0) * (len(sorted_latencies) - 1))
        return sorted_latencies[idx]

    @property
    def p50_ms(self) -> float:
        """50th percentile latency (median)."""
        return self.get_percentile(50)

    @property
    def p95_ms(self) -> float:
        """95th percentile latency."""
        return self.get_percentile(95)

    @property
    def p99_ms(self) -> float:
        """99th percentile latency."""
        return self.get_percentile(99)


class APICallTimer:
    """
    Context manager for timing API calls.

    Usage:
        with APICallTimer(report, "endpoint_name") as timer:
            result = make_api_call()
            if failed:
                timer.mark_error()

    The timer automatically records the latency when exiting the context.
    """

    def __init__(
        self, report: "SnowplowSourceReport", endpoint: str, client: str = "bdp"
    ):
        """
        Initialize timer.

        Args:
            report: Report to record metrics to
            endpoint: API endpoint name (e.g., "data_structures", "event_specs")
            client: Client type ("bdp" or "iglu")
        """
        self.report = report
        self.endpoint = endpoint
        self.client = client
        self.start_time: Optional[float] = None
        self.is_error = False

    def __enter__(self) -> "APICallTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.start_time is not None:
            latency_ms = (time.perf_counter() - self.start_time) * 1000
            # Mark as error if exception occurred or manually marked
            is_error = self.is_error or exc_type is not None
            self.report.record_api_call(
                self.client, self.endpoint, latency_ms, is_error
            )

    def mark_error(self) -> None:
        """Mark this call as an error (for non-exception failures)."""
        self.is_error = True


@dataclass
class SnowplowSourceReport(StaleEntityRemovalSourceReport):
    """
    Report for Snowplow source ingestion.

    Tracks:
    - Schemas extracted (events and entities)
    - Event specifications extracted
    - Tracking scenarios extracted
    - Lineage extracted
    - Errors and warnings
    """

    # Connection info
    connection_mode: str = "unknown"  # "bdp", "iglu", or "both"
    organization_id: str = ""

    # Schema extraction stats
    num_event_schemas_found: int = 0
    num_entity_schemas_found: int = 0
    num_event_schemas_extracted: int = 0
    num_entity_schemas_extracted: int = 0
    num_event_schemas_filtered: int = 0
    num_entity_schemas_filtered: int = 0

    # Event specifications stats
    num_event_specs_found: int = 0
    num_event_specs_extracted: int = 0
    num_event_specs_filtered: int = 0

    # Tracking scenarios stats
    num_tracking_scenarios_found: int = 0
    num_tracking_scenarios_extracted: int = 0
    num_tracking_scenarios_filtered: int = 0

    # Data products stats
    num_data_products_found: int = 0
    num_data_products_extracted: int = 0
    num_data_products_filtered: int = 0

    # Pipeline stats (DataFlow in DataHub)
    num_pipelines_found: int = 0
    num_pipelines_extracted: int = 0
    num_pipelines_filtered: int = 0

    # Enrichment stats (DataJob in DataHub)
    num_enrichments_found: int = 0
    num_enrichments_extracted: int = 0
    num_enrichments_filtered: int = 0

    # Lineage stats
    num_warehouse_lineage_extracted: int = 0

    # Deployment stats
    num_schemas_deployed: int = 0
    num_schemas_not_deployed: int = 0
    num_deployment_fetch_failures: int = 0
    failed_deployment_fetches: List[str] = field(default_factory=list)

    # Hidden schemas
    num_hidden_schemas: int = 0
    num_hidden_schemas_skipped: int = 0

    # Schema parsing errors
    schema_parsing_errors: List[str] = field(default_factory=list)
    schema_parsing_warnings: List[str] = field(default_factory=list)

    # API errors
    api_errors: Dict[str, List[str]] = field(default_factory=dict)

    # API metrics tracking (for performance tuning)
    # Structure: {client_type: {endpoint: APIMetrics}}
    # Example: {"bdp": {"data_structures": APIMetrics(...), "event_specs": APIMetrics(...)}}
    api_metrics: Dict[str, Dict[str, APIMetrics]] = field(default_factory=dict)

    # Filtered items (for debugging)
    filtered_schemas: List[str] = field(default_factory=list)
    filtered_event_specs: List[str] = field(default_factory=list)
    filtered_tracking_scenarios: List[str] = field(default_factory=list)

    def report_schema_found(self, schema_type: str) -> None:
        """Record that a schema was found."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_found += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_found += 1

    def report_schema_extracted(self, schema_type: str) -> None:
        """Record that a schema was successfully extracted."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_extracted += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_extracted += 1

    def report_schema_filtered(self, schema_type: str, schema_name: str) -> None:
        """Record that a schema was filtered out."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_filtered += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_filtered += 1
        self.filtered_schemas.append(schema_name)

    def report_event_spec_found(self) -> None:
        """Record that an event specification was found."""
        self.num_event_specs_found += 1

    def report_event_spec_extracted(self) -> None:
        """Record that an event specification was extracted."""
        self.num_event_specs_extracted += 1

    def report_event_spec_filtered(self, spec_name: str) -> None:
        """Record that an event specification was filtered out."""
        self.num_event_specs_filtered += 1
        self.filtered_event_specs.append(spec_name)

    def report_tracking_scenario_found(self) -> None:
        """Record that a tracking scenario was found."""
        self.num_tracking_scenarios_found += 1

    def report_tracking_scenario_extracted(self) -> None:
        """Record that a tracking scenario was extracted."""
        self.num_tracking_scenarios_extracted += 1

    def report_tracking_scenario_filtered(self, scenario_name: str) -> None:
        """Record that a tracking scenario was filtered out."""
        self.num_tracking_scenarios_filtered += 1
        self.filtered_tracking_scenarios.append(scenario_name)

    def report_pipeline_found(self) -> None:
        """Record that a pipeline was found."""
        self.num_pipelines_found += 1

    def report_pipeline_extracted(self) -> None:
        """Record that a pipeline was extracted."""
        self.num_pipelines_extracted += 1

    def report_pipeline_filtered(self, pipeline_name: str) -> None:
        """Record that a pipeline was filtered out."""
        self.num_pipelines_filtered += 1

    def report_enrichment_found(self) -> None:
        """Record that an enrichment was found."""
        self.num_enrichments_found += 1

    def report_enrichment_extracted(self) -> None:
        """Record that an enrichment was extracted."""
        self.num_enrichments_extracted += 1

    def report_enrichment_filtered(self, enrichment_name: str) -> None:
        """Record that an enrichment was filtered out."""
        self.num_enrichments_filtered += 1

    def report_warehouse_lineage_extracted(self) -> None:
        """Record that warehouse lineage was extracted."""
        self.num_warehouse_lineage_extracted += 1

    def report_schema_deployed(self) -> None:
        """Record that a schema is deployed."""
        self.num_schemas_deployed += 1

    def report_schema_not_deployed(self) -> None:
        """Record that a schema is not deployed."""
        self.num_schemas_not_deployed += 1

    def report_deployment_fetch_failure(self, schema_key: str, error: str) -> None:
        """Record that a deployment fetch failed for a schema."""
        self.num_deployment_fetch_failures += 1
        if len(self.failed_deployment_fetches) < 20:  # Limit stored failures
            self.failed_deployment_fetches.append(f"{schema_key}: {error}")

    def report_hidden_schema(self, skipped: bool = False) -> None:
        """Record a hidden schema."""
        self.num_hidden_schemas += 1
        if skipped:
            self.num_hidden_schemas_skipped += 1

    def report_schema_parsing_error(self, error: str) -> None:
        """Record a schema parsing error."""
        self.schema_parsing_errors.append(error)

    def report_schema_parsing_warning(self, warning: str) -> None:
        """Record a schema parsing warning."""
        self.schema_parsing_warnings.append(warning)

    def report_api_error(self, api_name: str, error: str) -> None:
        """Record an API error."""
        if api_name not in self.api_errors:
            self.api_errors[api_name] = []
        self.api_errors[api_name].append(error)

    def record_api_call(
        self, client: str, endpoint: str, latency_ms: float, is_error: bool = False
    ) -> None:
        """
        Record an API call with timing metrics.

        Args:
            client: Client type ("bdp" or "iglu")
            endpoint: API endpoint name (e.g., "data_structures", "event_specs")
            latency_ms: Call latency in milliseconds
            is_error: Whether the call resulted in an error
        """
        if client not in self.api_metrics:
            self.api_metrics[client] = {}
        if endpoint not in self.api_metrics[client]:
            self.api_metrics[client][endpoint] = APIMetrics()
        self.api_metrics[client][endpoint].record_call(latency_ms, is_error)

    def get_api_metrics_summary(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Get summary of API metrics for all clients and endpoints.

        Returns:
            Nested dict: {client: {endpoint: {metric_name: value}}}
        """
        summary: Dict[str, Dict[str, Dict[str, float]]] = {}
        for client, endpoints in self.api_metrics.items():
            summary[client] = {}
            for endpoint, metrics in endpoints.items():
                summary[client][endpoint] = {
                    "call_count": metrics.call_count,
                    "error_count": metrics.error_count,
                    "avg_latency_ms": round(metrics.avg_latency_ms, 2),
                    "p50_latency_ms": round(metrics.p50_ms, 2),
                    "p95_latency_ms": round(metrics.p95_ms, 2),
                    "p99_latency_ms": round(metrics.p99_ms, 2),
                    "total_latency_ms": round(metrics.total_latency_ms, 2),
                }
        return summary

    def compute_stats(self) -> None:
        """Compute final statistics (called at end of ingestion)."""
        # Total schemas (computed for potential future use)
        # total_schemas_found = self.num_event_schemas_found + self.num_entity_schemas_found
        # total_schemas_extracted = self.num_event_schemas_extracted + self.num_entity_schemas_extracted
        pass

        # Add to warnings if significant filtering occurred
        if self.num_event_schemas_filtered > 0:
            self.report_warning(
                "schema_filtering",
                f"Filtered out {self.num_event_schemas_filtered} event schemas. "
                f"Check schema_pattern configuration.",
            )

        if self.num_entity_schemas_filtered > 0:
            self.report_warning(
                "schema_filtering",
                f"Filtered out {self.num_entity_schemas_filtered} entity schemas. "
                f"Check schema_pattern configuration.",
            )

        # Report hidden schemas
        if self.num_hidden_schemas_skipped > 0:
            self.report_warning(
                "hidden_schemas",
                f"Skipped {self.num_hidden_schemas_skipped} hidden schemas. "
                f"Set include_hidden_schemas=true to include them.",
            )

        # Report schema parsing errors
        if self.schema_parsing_errors:
            for error in self.schema_parsing_errors[:5]:  # Limit to 5 errors
                self.report_warning("schema_parsing", error)

        # Report API errors
        for api_name, errors in self.api_errors.items():
            for error in errors[:3]:  # Limit to 3 errors per API
                self.report_failure(api_name, error)

        # Log API metrics summary for performance tuning
        self._log_api_metrics_summary()

    def _log_api_metrics_summary(self) -> None:
        """Log API metrics summary for performance monitoring."""
        logger = logging.getLogger(__name__)

        if not self.api_metrics:
            return

        # Calculate totals across all endpoints
        total_calls = 0
        total_errors = 0
        total_latency_ms = 0.0

        for _client, endpoints in self.api_metrics.items():
            for _endpoint, metrics in endpoints.items():
                total_calls += metrics.call_count
                total_errors += metrics.error_count
                total_latency_ms += metrics.total_latency_ms

        logger.info(
            f"API Metrics Summary: {total_calls} calls, "
            f"{total_errors} errors, {total_latency_ms:.0f}ms total"
        )

        # Log per-endpoint details at debug level
        for client, endpoints in self.api_metrics.items():
            for endpoint, metrics in endpoints.items():
                if metrics.call_count > 0:
                    logger.debug(
                        f"  [{client}] {endpoint}: "
                        f"{metrics.call_count} calls, "
                        f"avg={metrics.avg_latency_ms:.0f}ms, "
                        f"p50={metrics.p50_ms:.0f}ms, "
                        f"p99={metrics.p99_ms:.0f}ms"
                    )
