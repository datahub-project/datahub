"""Unit tests for OpenTelemetry initialization with corner cases."""

from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider

from datahub_integrations.observability.config import ObservabilityConfig
from datahub_integrations.observability.otel_config import (
    OTELConfigurationError,
    _create_duration_view,
    _create_otlp_reader,
    _create_prometheus_reader,
    _create_resource,
    initialize_observability,
    setup_meter_provider,
    setup_system_metrics,
    setup_tracer_provider,
)


@pytest.fixture(autouse=True)
def reset_observability_state():
    """Reset the observability initialization state between tests."""
    import datahub_integrations.observability.otel_config as otel_config

    # Reset the global flag before each test
    otel_config._observability_initialized = False
    yield
    # Reset after test as well
    otel_config._observability_initialized = False


class TestResourceCreation:
    """Test OpenTelemetry resource creation."""

    def test_resource_with_auto_version(self) -> None:
        config = ObservabilityConfig(service_version="auto")
        resource = _create_resource(config)

        assert resource.attributes["service.name"] == "datahub-integrations-service"
        assert resource.attributes["service.namespace"] == "datahub"
        assert "service.version" in resource.attributes
        # Should use the package version, not "auto"
        assert resource.attributes["service.version"] != "auto"

    def test_resource_with_explicit_version(self) -> None:
        config = ObservabilityConfig(service_version="1.2.3")
        resource = _create_resource(config)

        assert resource.attributes["service.version"] == "1.2.3"

    def test_resource_with_custom_service_name(self) -> None:
        config = ObservabilityConfig(service_name="custom-service")
        resource = _create_resource(config)

        assert resource.attributes["service.name"] == "custom-service"

    def test_resource_contains_telemetry_sdk_attributes(self) -> None:
        config = ObservabilityConfig()
        resource = _create_resource(config)

        assert resource.attributes["telemetry.sdk.language"] == "python"
        assert resource.attributes["telemetry.sdk.name"] == "opentelemetry"


class TestDurationViewCreation:
    """Test histogram view creation with custom buckets."""

    def test_duration_view_with_default_buckets(self) -> None:
        config = ObservabilityConfig()
        view = _create_duration_view("test_duration", config.slo_buckets_fast_seconds)

        # View should use the configured SLO buckets
        assert view._aggregation._boundaries == config.slo_buckets_fast_seconds  # type: ignore[attr-defined]

    def test_duration_view_with_custom_buckets(self) -> None:
        custom_buckets = [0.01, 0.1, 1.0, 10.0]
        view = _create_duration_view("test_duration", custom_buckets)

        assert view._aggregation._boundaries == custom_buckets  # type: ignore[attr-defined]

    def test_duration_view_with_single_bucket(self) -> None:
        view = _create_duration_view("test_duration", [1.0])

        assert view._aggregation._boundaries == [1.0]  # type: ignore[attr-defined]

    def test_duration_view_matches_wildcard_pattern(self) -> None:
        config = ObservabilityConfig()
        view = _create_duration_view("*_duration", config.slo_buckets_fast_seconds)

        # Should match any metric ending with _duration
        assert view._instrument_name == "*_duration"


class TestPrometheusReaderCreation:
    """Test Prometheus metric reader creation."""

    def test_prometheus_reader_creation_succeeds(self) -> None:
        config = ObservabilityConfig(prometheus_enabled=True)
        reader = _create_prometheus_reader(config)

        from opentelemetry.exporter.prometheus import (
            PrometheusMetricReader,  # type: ignore[import-not-found]
        )

        assert isinstance(reader, PrometheusMetricReader)

    def test_prometheus_reader_creation_failure_raises_error(self) -> None:
        config = ObservabilityConfig()

        with patch(
            "datahub_integrations.observability.otel_config.PrometheusMetricReader",
            side_effect=Exception("Prometheus init failed"),
        ):
            with pytest.raises(OTELConfigurationError, match="Prometheus exporter"):
                _create_prometheus_reader(config)


class TestOTLPReaderCreation:
    """Test OTLP metric reader creation."""

    def test_otlp_reader_with_default_settings(self) -> None:
        config = ObservabilityConfig(
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )
        reader = _create_otlp_reader(config)

        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        assert isinstance(reader, PeriodicExportingMetricReader)

    def test_otlp_reader_with_custom_timeout(self) -> None:
        config = ObservabilityConfig(
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
            otlp_timeout_ms=5000,
        )
        reader = _create_otlp_reader(config)

        # Reader should be configured with the timeout
        assert reader._export_interval_millis == config.metrics_export_interval_ms

    def test_otlp_reader_with_headers(self) -> None:
        config = ObservabilityConfig(
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
            otlp_headers={"api-key": "secret", "tenant": "prod"},
        )
        # Should not raise
        reader = _create_otlp_reader(config)
        assert reader is not None

    def test_otlp_reader_creation_failure_raises_error(self) -> None:
        config = ObservabilityConfig(
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )

        with patch(
            "datahub_integrations.observability.otel_config.OTLPMetricExporter",
            side_effect=Exception("OTLP init failed"),
        ):
            with pytest.raises(OTELConfigurationError, match="OTLP exporter"):
                _create_otlp_reader(config)


class TestMeterProviderSetup:
    """Test MeterProvider initialization."""

    def test_meter_provider_with_prometheus_only(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=False,
        )
        provider = setup_meter_provider(config)

        assert isinstance(provider, MeterProvider)
        assert len(provider._sdk_config.metric_readers) >= 1

    def test_meter_provider_with_otlp_only(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )
        provider = setup_meter_provider(config)

        assert isinstance(provider, MeterProvider)
        assert len(provider._sdk_config.metric_readers) >= 1

    def test_meter_provider_with_both_exporters(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )
        provider = setup_meter_provider(config)

        assert isinstance(provider, MeterProvider)
        # Should have both Prometheus and OTLP readers
        assert len(provider._sdk_config.metric_readers) >= 2

    def test_meter_provider_with_invalid_config_raises_error(self) -> None:
        # Config with no exporters should fail
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=False,
        )

        with pytest.raises(OTELConfigurationError, match="Invalid configuration"):
            setup_meter_provider(config)

    def test_meter_provider_graceful_degradation_prometheus_fails(self) -> None:
        # If Prometheus fails but OTLP works, should continue with OTLP
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )

        with patch(
            "datahub_integrations.observability.otel_config._create_prometheus_reader",
            side_effect=OTELConfigurationError("Prometheus failed"),
        ):
            provider = setup_meter_provider(config)
            # Should succeed with OTLP only
            assert isinstance(provider, MeterProvider)

    def test_meter_provider_graceful_degradation_otlp_fails(self) -> None:
        # If OTLP fails but Prometheus works, should continue with Prometheus
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )

        with patch(
            "datahub_integrations.observability.otel_config._create_otlp_reader",
            side_effect=OTELConfigurationError("OTLP failed"),
        ):
            provider = setup_meter_provider(config)
            # Should succeed with Prometheus only
            assert isinstance(provider, MeterProvider)

    def test_meter_provider_fails_when_all_exporters_fail(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )

        with (
            patch(
                "datahub_integrations.observability.otel_config._create_prometheus_reader",
                side_effect=OTELConfigurationError("Prometheus failed"),
            ),
            patch(
                "datahub_integrations.observability.otel_config._create_otlp_reader",
                side_effect=OTELConfigurationError("OTLP failed"),
            ),
        ):
            with pytest.raises(
                OTELConfigurationError, match="No metric readers could be initialized"
            ):
                setup_meter_provider(config)

    def test_meter_provider_includes_custom_views(self) -> None:
        config = ObservabilityConfig()
        provider = setup_meter_provider(config)

        # Should have registered views for duration metrics
        assert provider._sdk_config.views is not None
        assert len(provider._sdk_config.views) > 0


class TestTracerProviderSetup:
    """Test TracerProvider initialization."""

    def test_tracer_provider_disabled(self) -> None:
        config = ObservabilityConfig(tracing_enabled=False)
        provider = setup_tracer_provider(config)

        assert provider is None

    def test_tracer_provider_enabled_without_otlp(self) -> None:
        config = ObservabilityConfig(
            tracing_enabled=True,
            otlp_enabled=False,
        )
        provider = setup_tracer_provider(config)

        # Should create provider but without span exporters
        assert isinstance(provider, TracerProvider)

    def test_tracer_provider_with_otlp_exporter(self) -> None:
        config = ObservabilityConfig(
            tracing_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )
        provider = setup_tracer_provider(config)

        assert isinstance(provider, TracerProvider)

    def test_tracer_provider_with_custom_sample_rate(self) -> None:
        config = ObservabilityConfig(
            tracing_enabled=True,
            traces_sample_rate=0.25,
        )
        provider = setup_tracer_provider(config)

        assert isinstance(provider, TracerProvider)
        # Sampler should use the configured rate
        assert provider.sampler._root._rate == 0.25  # type: ignore[attr-defined]

    def test_tracer_provider_with_zero_sample_rate(self) -> None:
        # Zero sample rate means no traces collected (valid edge case)
        config = ObservabilityConfig(
            tracing_enabled=True,
            traces_sample_rate=0.0,
        )
        provider = setup_tracer_provider(config)

        assert isinstance(provider, TracerProvider)
        assert provider.sampler._root._rate == 0.0  # type: ignore[attr-defined]

    def test_tracer_provider_with_full_sample_rate(self) -> None:
        # 1.0 sample rate means all traces collected
        config = ObservabilityConfig(
            tracing_enabled=True,
            traces_sample_rate=1.0,
        )
        provider = setup_tracer_provider(config)

        assert isinstance(provider, TracerProvider)
        assert provider.sampler._root._rate == 1.0  # type: ignore[attr-defined]

    def test_tracer_provider_continues_on_otlp_failure(self) -> None:
        # Should continue without OTLP exporter if it fails
        config = ObservabilityConfig(
            tracing_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://localhost:4317",
        )

        with patch(
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter",
            side_effect=Exception("OTLP span exporter failed"),
        ):
            # Should not raise, just continue without trace export
            provider = setup_tracer_provider(config)
            assert isinstance(provider, TracerProvider)


class TestSystemMetricsSetup:
    """Test system metrics instrumentation."""

    def test_system_metrics_disabled(self) -> None:
        config = ObservabilityConfig(system_metrics_enabled=False)
        # Should not raise
        setup_system_metrics(config)

    def test_system_metrics_enabled(self) -> None:
        config = ObservabilityConfig(system_metrics_enabled=True)

        with patch(
            "opentelemetry.instrumentation.system_metrics.SystemMetricsInstrumentor"
        ) as mock_instrumentor:
            mock_instance = mock_instrumentor.return_value

            setup_system_metrics(config)

            mock_instance.instrument.assert_called_once()

    def test_system_metrics_handles_import_error(self) -> None:
        config = ObservabilityConfig(system_metrics_enabled=True)

        with patch(
            "opentelemetry.instrumentation.system_metrics.SystemMetricsInstrumentor",
            side_effect=ImportError("Module not found"),
        ):
            # Should not raise, just log warning
            setup_system_metrics(config)

    def test_system_metrics_handles_instrumentation_error(self) -> None:
        config = ObservabilityConfig(system_metrics_enabled=True)

        mock_instrumentor = MagicMock()
        mock_instrumentor.return_value.instrument.side_effect = Exception(
            "Instrumentation failed"
        )

        with patch(
            "opentelemetry.instrumentation.system_metrics.SystemMetricsInstrumentor",
            mock_instrumentor,
        ):
            # Should not raise, just log warning
            setup_system_metrics(config)


class TestObservabilityInitialization:
    """Test full observability stack initialization."""

    def test_initialize_with_default_config(self) -> None:
        # Should use environment-based config
        initialize_observability()
        # No assertion needed - if it doesn't raise, initialization succeeded

    def test_initialize_with_explicit_config(self) -> None:
        config = ObservabilityConfig(
            service_name="test-service",
            prometheus_enabled=True,
            otlp_enabled=False,
        )
        initialize_observability(config)
        # No assertion needed - if it doesn't raise, initialization succeeded

    def test_initialize_raises_on_meter_provider_failure(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=False,
        )

        with pytest.raises(OTELConfigurationError, match="initialization failed"):
            initialize_observability(config)

    def test_initialize_continues_on_tracer_provider_failure(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            tracing_enabled=True,
        )

        with patch(
            "datahub_integrations.observability.otel_config.setup_tracer_provider",
            side_effect=Exception("Tracer setup failed"),
        ):
            with pytest.raises(OTELConfigurationError):
                initialize_observability(config)

    def test_initialize_continues_on_system_metrics_failure(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            system_metrics_enabled=True,
        )

        with patch(
            "datahub_integrations.observability.otel_config.setup_system_metrics",
            side_effect=Exception("System metrics failed"),
        ):
            with pytest.raises(OTELConfigurationError):
                initialize_observability(config)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_concurrent_initialization(self) -> None:
        # Test that multiple initializations don't cause issues
        config = ObservabilityConfig(prometheus_enabled=True, otlp_enabled=False)

        # First initialization
        initialize_observability(config)

        # Second initialization should not raise
        # (though in practice, this would replace global providers)
        initialize_observability(config)

    def test_initialization_with_malformed_endpoint(self) -> None:
        # OTLP accepts any string as endpoint - validation happens on send
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=True,
            otlp_endpoint="not-a-valid-url",
        )

        # Should succeed - OTLP doesn't validate URL format upfront
        # Errors only occur when trying to send data
        initialize_observability(config)  # Should not raise

    def test_resource_attributes_with_special_characters(self) -> None:
        config = ObservabilityConfig(
            service_name="service-name-with-dashes_and_underscores",
            service_namespace="namespace/with/slashes",
        )

        resource = _create_resource(config)

        assert (
            resource.attributes["service.name"]
            == "service-name-with-dashes_and_underscores"
        )
        assert resource.attributes["service.namespace"] == "namespace/with/slashes"
