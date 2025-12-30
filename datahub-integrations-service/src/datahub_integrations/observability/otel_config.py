"""OpenTelemetry SDK initialization for datahub-integrations-service.

This module handles the setup of OpenTelemetry metrics and tracing,
following the patterns established in Java's metadata-service.
"""

from typing import Any

from loguru import logger
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,  # type: ignore[import-not-found]
)
from opentelemetry.exporter.prometheus import (
    PrometheusMetricReader,  # type: ignore[import-not-found]
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

import datahub_integrations as di
from datahub_integrations.observability.config import ObservabilityConfig
from datahub_integrations.observability.metrics_registry import (
    BucketProfile,
    get_metrics_by_profile,
)

# Track initialization state to prevent re-initialization in tests
_observability_initialized = False


class OTELConfigurationError(Exception):
    """Raised when OpenTelemetry configuration fails."""


def _create_resource(config: ObservabilityConfig) -> Resource:
    """Create OpenTelemetry resource with service metadata.

    Args:
        config: Observability configuration.

    Returns:
        Resource with service attributes.
    """
    version = (
        di.__version__ if config.service_version == "auto" else config.service_version
    )

    return Resource.create(
        {
            "service.name": config.service_name,
            "service.version": version,
            "service.namespace": config.service_namespace,
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.22.0",  # Will be set by SDK
        }
    )


def _create_duration_view(
    instrument_name: str,
    buckets: list[float],
) -> View:
    """Create view with custom SLO-based histogram buckets for duration metrics.

    Args:
        instrument_name: Name of the instrument to match (supports wildcards).
        buckets: Histogram bucket boundaries in seconds.

    Returns:
        View with explicit bucket boundaries.
    """
    from opentelemetry.sdk.metrics import Histogram
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation

    return View(
        instrument_type=Histogram,
        instrument_name=instrument_name,
        aggregation=ExplicitBucketHistogramAggregation(boundaries=buckets),
    )


def _create_prometheus_reader(config: ObservabilityConfig) -> PrometheusMetricReader:
    """Create Prometheus metric reader for pull-based scraping.

    Args:
        config: Observability configuration.

    Returns:
        Configured PrometheusMetricReader.

    Raises:
        OTELConfigurationError: If Prometheus reader creation fails.
    """
    try:
        return PrometheusMetricReader()
    except Exception as e:
        logger.error(f"Failed to create Prometheus metric reader: {e}")
        raise OTELConfigurationError("Prometheus exporter initialization failed") from e


def _create_otlp_reader(config: ObservabilityConfig) -> PeriodicExportingMetricReader:
    """Create OTLP metric reader for push-based export.

    Args:
        config: Observability configuration.

    Returns:
        Configured PeriodicExportingMetricReader with OTLP exporter.

    Raises:
        OTELConfigurationError: If OTLP reader creation fails.
    """
    try:
        # Parse headers from dict to tuple format expected by OTLP exporter
        headers: tuple[tuple[str, str], ...] | None = (
            tuple((k, v) for k, v in config.otlp_headers.items())
            if config.otlp_headers
            else None
        )

        exporter = OTLPMetricExporter(
            endpoint=config.otlp_endpoint,
            timeout=config.otlp_timeout_ms // 1000,
            headers=headers,
        )

        return PeriodicExportingMetricReader(
            exporter=exporter,
            export_interval_millis=config.metrics_export_interval_ms,
        )
    except Exception as e:
        logger.error(f"Failed to create OTLP metric reader: {e}")
        raise OTELConfigurationError("OTLP exporter initialization failed") from e


def setup_meter_provider(config: ObservabilityConfig) -> MeterProvider:
    """Initialize MeterProvider with configured exporters and views.

    This sets up the global MeterProvider with:
    - Resource attributes (service name, version, etc.)
    - Prometheus exporter (pull-based, if enabled)
    - OTLP exporter (push-based, if enabled)
    - Custom histogram views with SLO-based buckets

    Args:
        config: Observability configuration.

    Returns:
        Configured MeterProvider.

    Raises:
        OTELConfigurationError: If setup fails.
    """
    try:
        config.validate_config()
    except ValueError as e:
        raise OTELConfigurationError(f"Invalid configuration: {e}") from e

    resource = _create_resource(config)
    readers: list[Any] = []

    # Add Prometheus reader (pull-based)
    if config.prometheus_enabled:
        try:
            readers.append(_create_prometheus_reader(config))
            logger.info("Prometheus metric reader enabled")
        except OTELConfigurationError:
            # If Prometheus is the only exporter, re-raise
            if not config.otlp_enabled:
                raise
            logger.warning("Prometheus exporter failed, continuing with OTLP only")

    # Add OTLP reader (push-based)
    if config.otlp_enabled:
        try:
            readers.append(_create_otlp_reader(config))
            logger.info(
                f"OTLP metric reader enabled (endpoint: {config.otlp_endpoint})"
            )
        except OTELConfigurationError:
            # If OTLP is the only exporter, re-raise
            if not config.prometheus_enabled:
                raise
            logger.warning("OTLP exporter failed, continuing with Prometheus only")

    if not readers:
        raise OTELConfigurationError("No metric readers could be initialized")

    # Create custom views for duration metrics with SLO-based buckets
    # Dual bucket strategy: fast operations vs slow operations
    # Views are automatically generated from the metric registry

    # Fast operations (APIs, queries, routing) - use fast buckets
    fast_metrics = get_metrics_by_profile(BucketProfile.FAST)
    fast_views = [
        _create_duration_view(metric.name, config.slo_buckets_fast_seconds)
        for metric in fast_metrics
    ]

    # Slow operations (LLM, batch, long-running) - use slow buckets
    slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)
    slow_views = [
        _create_duration_view(metric.name, config.slo_buckets_slow_seconds)
        for metric in slow_metrics
    ]

    # Combine all views
    views = fast_views + slow_views

    logger.info(
        f"Generated {len(views)} OpenTelemetry Views from metric registry "
        f"({len(fast_views)} fast, {len(slow_views)} slow)"
    )

    provider = MeterProvider(
        resource=resource,
        metric_readers=readers,
        views=views,
    )

    # Set as global meter provider
    metrics.set_meter_provider(provider)

    logger.info(
        f"OpenTelemetry MeterProvider initialized for {config.service_name} "
        f"v{resource.attributes.get('service.version')}"
    )

    return provider


def setup_tracer_provider(config: ObservabilityConfig) -> TracerProvider | None:
    """Initialize TracerProvider with sampling and OTLP export.

    This sets up distributed tracing with:
    - Parent-based sampling with configurable rate
    - OTLP trace exporter (if OTLP enabled)
    - W3C Trace Context propagation

    Args:
        config: Observability configuration.

    Returns:
        Configured TracerProvider, or None if tracing disabled.

    Raises:
        OTELConfigurationError: If setup fails when tracing is enabled.
    """
    if not config.tracing_enabled:
        logger.info("Distributed tracing disabled")
        return None

    resource = _create_resource(config)

    # Parent-based sampling with trace ID ratio
    sampler = ParentBasedTraceIdRatio(config.traces_sample_rate)

    provider = TracerProvider(resource=resource, sampler=sampler)

    # Add OTLP span exporter if enabled
    if config.otlp_enabled:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # type: ignore[import-not-found]
                OTLPSpanExporter,
            )

            # Parse headers from dict to tuple format
            trace_headers: tuple[tuple[str, str], ...] | None = (
                tuple((k, v) for k, v in config.otlp_headers.items())
                if config.otlp_headers
                else None
            )

            span_exporter = OTLPSpanExporter(
                endpoint=config.otlp_endpoint,
                timeout=config.otlp_timeout_ms // 1000,
                headers=trace_headers,
            )

            # Use batch processor for performance
            span_processor = BatchSpanProcessor(span_exporter)
            provider.add_span_processor(span_processor)

            logger.info(
                f"OTLP trace exporter enabled (sample rate: {config.traces_sample_rate:.1%})"
            )
        except Exception as e:
            logger.warning(f"Failed to initialize OTLP trace exporter: {e}")
            # Continue without tracing rather than failing

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    logger.info("OpenTelemetry TracerProvider initialized")

    return provider


def setup_system_metrics(config: ObservabilityConfig) -> None:
    """Enable system resource metrics collection.

    Collects CPU, memory, and process metrics automatically.

    Args:
        config: Observability configuration.
    """
    if not config.system_metrics_enabled:
        logger.info("System metrics disabled")
        return

    try:
        from opentelemetry.instrumentation.system_metrics import (  # type: ignore[import-not-found]
            SystemMetricsInstrumentor,
        )

        SystemMetricsInstrumentor().instrument()
        logger.info("System metrics instrumentation enabled")
    except ImportError:
        logger.warning(
            "opentelemetry-instrumentation-system-metrics not installed, skipping system metrics"
        )
    except Exception as e:
        logger.warning(f"Failed to enable system metrics: {e}")


def initialize_observability(config: ObservabilityConfig | None = None) -> None:
    """Initialize OpenTelemetry observability stack.

    This is the main entry point for setting up metrics and tracing.
    Call this once at application startup.

    Args:
        config: Observability configuration. If None, loads from environment.

    Raises:
        OTELConfigurationError: If initialization fails.
    """
    global _observability_initialized

    # Check if already initialized to avoid re-initialization in tests
    if _observability_initialized:
        logger.debug("OpenTelemetry already initialized, skipping re-initialization")
        return

    if config is None:
        config = ObservabilityConfig()

    logger.info("Initializing OpenTelemetry observability...")

    try:
        setup_meter_provider(config)
        setup_tracer_provider(config)
        setup_system_metrics(config)

        _observability_initialized = True
        logger.info("✓ OpenTelemetry observability initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize observability: {e}")
        raise OTELConfigurationError("Observability initialization failed") from e
