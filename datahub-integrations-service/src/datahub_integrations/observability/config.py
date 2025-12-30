"""Configuration for OpenTelemetry observability.

This module provides configuration matching the patterns established in the
Java metadata-service for consistency across DataHub services.
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ObservabilityConfig(BaseSettings):
    """OpenTelemetry configuration for datahub-integrations-service.

    Follows the patterns established in Java's metadata-service for consistency.
    All settings can be overridden via environment variables with the OTEL_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="OTEL_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    # Service identification
    service_name: str = Field(
        default="datahub-integrations-service",
        description="Service name for resource attributes",
    )
    service_version: str = Field(
        default="auto",
        description="Service version (auto-detected from package)",
    )
    service_namespace: str = Field(
        default="datahub",
        description="Service namespace for resource grouping",
    )

    # Prometheus exporter (pull-based)
    prometheus_enabled: bool = Field(
        default=True,
        description="Enable Prometheus exporter for /metrics endpoint",
    )

    # OTLP exporter (push-based)
    otlp_enabled: bool = Field(
        default=False,
        description="Enable OTLP exporter for push to collectors",
    )
    otlp_endpoint: str = Field(
        default="http://localhost:4317",
        description="OTLP collector endpoint (gRPC or HTTP)",
    )
    otlp_protocol: Literal["grpc", "http"] = Field(
        default="grpc",
        description="OTLP protocol (grpc or http)",
    )
    otlp_timeout_ms: int = Field(
        default=10000,
        ge=1000,
        le=60000,
        description="OTLP export timeout in milliseconds",
    )
    otlp_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Additional headers for OTLP exporter (e.g., API keys)",
    )

    # Metrics configuration
    metrics_export_interval_ms: int = Field(
        default=60000,
        ge=1000,
        le=300000,
        description="Metric export interval in milliseconds",
    )
    metrics_exemplars_enabled: bool = Field(
        default=True,
        description="Enable exemplars to link metrics to traces",
    )

    # High cardinality protection
    high_cardinality_limit: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Maximum unique label values before dropping metrics",
    )

    # SLO-based histogram buckets (in seconds)
    # Dual bucket strategy: separate configurations for fast vs slow operations

    # Fast operations: API calls, queries, routing (sub-5s typical)
    slo_buckets_fast_seconds: list[float] = Field(
        default_factory=lambda: [
            0.010,  # 10ms  - Very fast (cache hits, routing)
            0.050,  # 50ms  - Fast (simple queries)
            0.250,  # 250ms - Acceptable (complex queries)
            1.000,  # 1s    - Slow threshold for APIs
            5.000,  # 5s    - Very slow for APIs
        ],
        description="SLO buckets for fast operations (APIs, queries, routing) - 5 buckets",
    )

    # Slow operations: LLM calls, batch jobs, long-running (multi-second typical)
    slo_buckets_slow_seconds: list[float] = Field(
        default_factory=lambda: [
            1.000,  # 1s    - Fast for LLM
            5.000,  # 5s    - Typical single LLM call
            15.000,  # 15s   - Multi-step or slow LLM
            60.000,  # 1min  - Long-running operations
            300.000,  # 5min - Timeout boundary
        ],
        description="SLO buckets for slow operations (LLM, batch, long-running) - 5 buckets",
    )

    # Percentiles for summary metrics
    percentiles: list[float] = Field(
        default_factory=lambda: [0.5, 0.95, 0.99, 0.999],
        description="Percentiles to calculate for metrics",
    )

    # Tracing configuration
    tracing_enabled: bool = Field(
        default=True,
        description="Enable distributed tracing",
    )
    traces_sample_rate: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        description="Trace sampling rate (0.0 to 1.0, 0.1 = 10%)",
    )

    # System metrics
    system_metrics_enabled: bool = Field(
        default=True,
        description="Enable system resource metrics (CPU, memory, etc.)",
    )

    # Kafka lag monitoring for action subprocesses
    kafka_lag_enabled: bool = Field(
        default=True,
        description="Enable Kafka consumer lag monitoring in action subprocesses",
    )
    kafka_lag_interval_seconds: float = Field(
        default=30.0,
        ge=5.0,
        le=300.0,
        description="Kafka lag check interval in seconds",
    )
    kafka_lag_timeout_seconds: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Kafka API timeout for lag checks in seconds",
    )

    def validate_config(self) -> None:
        """Validate configuration consistency.

        Raises:
            ValueError: If configuration is invalid.
        """
        if self.otlp_enabled and not self.otlp_endpoint:
            raise ValueError("otlp_endpoint must be set when otlp_enabled=True")

        if not self.prometheus_enabled and not self.otlp_enabled:
            raise ValueError(
                "At least one exporter (Prometheus or OTLP) must be enabled"
            )

        # Validate fast buckets
        if len(self.slo_buckets_fast_seconds) == 0:
            raise ValueError("slo_buckets_fast_seconds must have at least one value")
        if sorted(self.slo_buckets_fast_seconds) != self.slo_buckets_fast_seconds:
            raise ValueError(
                "slo_buckets_fast_seconds must be sorted in ascending order"
            )

        # Validate slow buckets
        if len(self.slo_buckets_slow_seconds) == 0:
            raise ValueError("slo_buckets_slow_seconds must have at least one value")
        if sorted(self.slo_buckets_slow_seconds) != self.slo_buckets_slow_seconds:
            raise ValueError(
                "slo_buckets_slow_seconds must be sorted in ascending order"
            )

        # Validate percentiles
        for p in self.percentiles:
            if not 0.0 <= p <= 1.0:
                raise ValueError(f"Percentile {p} must be between 0.0 and 1.0")
