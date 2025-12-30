"""Unit tests for ObservabilityConfig with comprehensive corner cases."""

import pytest

from datahub_integrations.observability.config import ObservabilityConfig


class TestObservabilityConfigDefaults:
    """Test default configuration values."""

    def test_default_service_name(self) -> None:
        config = ObservabilityConfig()
        assert config.service_name == "datahub-integrations-service"

    def test_default_service_version_is_auto(self) -> None:
        config = ObservabilityConfig()
        assert config.service_version == "auto"

    def test_default_service_namespace(self) -> None:
        config = ObservabilityConfig()
        assert config.service_namespace == "datahub"

    def test_prometheus_enabled_by_default(self) -> None:
        config = ObservabilityConfig()
        assert config.prometheus_enabled is True

    def test_otlp_disabled_by_default(self) -> None:
        config = ObservabilityConfig()
        assert config.otlp_enabled is False

    def test_tracing_enabled_by_default(self) -> None:
        config = ObservabilityConfig()
        assert config.tracing_enabled is True

    def test_system_metrics_enabled_by_default(self) -> None:
        config = ObservabilityConfig()
        assert config.system_metrics_enabled is True


class TestObservabilityConfigEnvironmentVariables:
    """Test configuration from environment variables."""

    def test_service_name_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_SERVICE_NAME", "test-service")
        config = ObservabilityConfig()
        assert config.service_name == "test-service"

    def test_service_version_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_SERVICE_VERSION", "1.2.3")
        config = ObservabilityConfig()
        assert config.service_version == "1.2.3"

    def test_otlp_endpoint_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_OTLP_ENDPOINT", "http://collector:4317")
        config = ObservabilityConfig()
        assert config.otlp_endpoint == "http://collector:4317"

    def test_otlp_protocol_http(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_OTLP_PROTOCOL", "http")
        config = ObservabilityConfig()
        assert config.otlp_protocol == "http"

    def test_otlp_protocol_grpc(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_OTLP_PROTOCOL", "grpc")
        config = ObservabilityConfig()
        assert config.otlp_protocol == "grpc"

    def test_metrics_export_interval_from_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_METRICS_EXPORT_INTERVAL_MS", "30000")
        config = ObservabilityConfig()
        assert config.metrics_export_interval_ms == 30000

    def test_high_cardinality_limit_from_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_HIGH_CARDINALITY_LIMIT", "500")
        config = ObservabilityConfig()
        assert config.high_cardinality_limit == 500

    def test_traces_sample_rate_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_TRACES_SAMPLE_RATE", "0.25")
        config = ObservabilityConfig()
        assert config.traces_sample_rate == 0.25

    def test_case_insensitive_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Pydantic settings should handle case-insensitive env vars
        monkeypatch.setenv("otel_service_name", "lowercase-service")
        config = ObservabilityConfig()
        assert config.service_name == "lowercase-service"


class TestObservabilityConfigValidation:
    """Test configuration validation logic."""

    def test_validation_passes_with_prometheus_only(self) -> None:
        config = ObservabilityConfig(prometheus_enabled=True, otlp_enabled=False)
        # Should not raise
        config.validate_config()

    def test_validation_passes_with_otlp_only(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=True,
            otlp_endpoint="http://collector:4317",
        )
        # Should not raise
        config.validate_config()

    def test_validation_passes_with_both_exporters(self) -> None:
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=True,
            otlp_endpoint="http://collector:4317",
        )
        # Should not raise
        config.validate_config()

    def test_validation_fails_with_no_exporters(self) -> None:
        config = ObservabilityConfig(prometheus_enabled=False, otlp_enabled=False)
        with pytest.raises(
            ValueError,
            match="At least one exporter.*must be enabled",
        ):
            config.validate_config()

    def test_validation_fails_with_otlp_enabled_but_no_endpoint(self) -> None:
        config = ObservabilityConfig(otlp_enabled=True, otlp_endpoint="")
        with pytest.raises(ValueError, match="otlp_endpoint must be set"):
            config.validate_config()

    def test_validation_fails_with_empty_buckets(self) -> None:
        config = ObservabilityConfig(slo_buckets_fast_seconds=[])
        with pytest.raises(ValueError, match="must have at least one value"):
            config.validate_config()

    def test_validation_fails_with_unsorted_buckets(self) -> None:
        config = ObservabilityConfig(slo_buckets_fast_seconds=[1.0, 0.5, 2.0])
        with pytest.raises(ValueError, match="must be sorted in ascending order"):
            config.validate_config()

    def test_validation_fails_with_invalid_percentile_below_zero(self) -> None:
        config = ObservabilityConfig(percentiles=[-0.1, 0.5, 0.99])
        with pytest.raises(ValueError, match="must be between 0.0 and 1.0"):
            config.validate_config()

    def test_validation_fails_with_invalid_percentile_above_one(self) -> None:
        config = ObservabilityConfig(percentiles=[0.5, 0.99, 1.5])
        with pytest.raises(ValueError, match="must be between 0.0 and 1.0"):
            config.validate_config()

    def test_validation_passes_with_percentile_exactly_zero(self) -> None:
        config = ObservabilityConfig(percentiles=[0.0, 0.5, 1.0])
        # Should not raise
        config.validate_config()

    def test_validation_passes_with_percentile_exactly_one(self) -> None:
        config = ObservabilityConfig(percentiles=[0.5, 0.99, 1.0])
        # Should not raise
        config.validate_config()


class TestObservabilityConfigBoundaries:
    """Test boundary conditions for numeric fields."""

    def test_otlp_timeout_minimum_boundary(self) -> None:
        config = ObservabilityConfig(otlp_timeout_ms=1000)
        assert config.otlp_timeout_ms == 1000

    def test_otlp_timeout_maximum_boundary(self) -> None:
        config = ObservabilityConfig(otlp_timeout_ms=60000)
        assert config.otlp_timeout_ms == 60000

    def test_otlp_timeout_below_minimum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(otlp_timeout_ms=999)

    def test_otlp_timeout_above_maximum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(otlp_timeout_ms=60001)

    def test_metrics_export_interval_minimum_boundary(self) -> None:
        config = ObservabilityConfig(metrics_export_interval_ms=1000)
        assert config.metrics_export_interval_ms == 1000

    def test_metrics_export_interval_maximum_boundary(self) -> None:
        config = ObservabilityConfig(metrics_export_interval_ms=300000)
        assert config.metrics_export_interval_ms == 300000

    def test_metrics_export_interval_below_minimum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(metrics_export_interval_ms=999)

    def test_metrics_export_interval_above_maximum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(metrics_export_interval_ms=300001)

    def test_high_cardinality_limit_minimum_boundary(self) -> None:
        config = ObservabilityConfig(high_cardinality_limit=10)
        assert config.high_cardinality_limit == 10

    def test_high_cardinality_limit_maximum_boundary(self) -> None:
        config = ObservabilityConfig(high_cardinality_limit=1000)
        assert config.high_cardinality_limit == 1000

    def test_high_cardinality_limit_below_minimum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(high_cardinality_limit=9)

    def test_high_cardinality_limit_above_maximum_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(high_cardinality_limit=1001)

    def test_traces_sample_rate_zero(self) -> None:
        config = ObservabilityConfig(traces_sample_rate=0.0)
        assert config.traces_sample_rate == 0.0

    def test_traces_sample_rate_one(self) -> None:
        config = ObservabilityConfig(traces_sample_rate=1.0)
        assert config.traces_sample_rate == 1.0

    def test_traces_sample_rate_below_zero_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(traces_sample_rate=-0.1)

    def test_traces_sample_rate_above_one_fails(self) -> None:
        with pytest.raises(ValueError):
            ObservabilityConfig(traces_sample_rate=1.1)


class TestObservabilityConfigSLOBuckets:
    """Test SLO bucket configuration."""

    def test_default_slo_buckets_are_sorted(self) -> None:
        config = ObservabilityConfig()
        buckets = config.slo_buckets_fast_seconds
        assert buckets == sorted(buckets)

    def test_default_slo_buckets_cover_common_latencies(self) -> None:
        config = ObservabilityConfig()
        buckets = config.slo_buckets_fast_seconds

        # Should have buckets for common SLO targets
        assert 0.010 in buckets  # 10ms
        assert 0.050 in buckets  # 50ms
        assert 0.250 in buckets  # 250ms
        assert 1.000 in buckets  # 1s
        assert 5.000 in buckets  # 5s

    def test_custom_slo_buckets(self) -> None:
        custom_buckets = [0.01, 0.05, 0.1, 0.5, 1.0]
        config = ObservabilityConfig(slo_buckets_fast_seconds=custom_buckets)
        assert config.slo_buckets_fast_seconds == custom_buckets

    def test_single_bucket_is_valid(self) -> None:
        config = ObservabilityConfig(slo_buckets_fast_seconds=[1.0])
        config.validate_config()  # Should not raise


class TestObservabilityConfigPercentiles:
    """Test percentile configuration."""

    def test_default_percentiles_include_common_values(self) -> None:
        config = ObservabilityConfig()
        percentiles = config.percentiles

        # Should have standard percentiles
        assert 0.5 in percentiles  # p50 (median)
        assert 0.95 in percentiles  # p95
        assert 0.99 in percentiles  # p99
        assert 0.999 in percentiles  # p99.9

    def test_custom_percentiles(self) -> None:
        custom_percentiles = [0.5, 0.90, 0.99]
        config = ObservabilityConfig(percentiles=custom_percentiles)
        assert config.percentiles == custom_percentiles

    def test_extreme_percentiles(self) -> None:
        # Test edge cases
        config = ObservabilityConfig(percentiles=[0.0, 0.9999, 1.0])
        config.validate_config()  # Should not raise


class TestObservabilityConfigOTLPHeaders:
    """Test OTLP headers configuration."""

    def test_default_headers_empty(self) -> None:
        config = ObservabilityConfig()
        assert config.otlp_headers == {}

    def test_custom_headers(self) -> None:
        headers = {"api-key": "secret123", "tenant-id": "abc"}
        config = ObservabilityConfig(otlp_headers=headers)
        assert config.otlp_headers == headers

    def test_headers_with_special_characters(self) -> None:
        # Test headers with special characters (realistic for auth tokens)
        headers = {"authorization": "Bearer eyJhbGc.iOiJIUz.I1NiIsInR5"}
        config = ObservabilityConfig(otlp_headers=headers)
        assert config.otlp_headers == headers


class TestObservabilityConfigEdgeCases:
    """Test edge cases and unusual configurations."""

    def test_config_with_all_features_disabled(self) -> None:
        # System should fail validation if no exporters
        config = ObservabilityConfig(
            prometheus_enabled=False,
            otlp_enabled=False,
            tracing_enabled=False,
            system_metrics_enabled=False,
        )
        with pytest.raises(ValueError):
            config.validate_config()

    def test_config_with_minimal_valid_settings(self) -> None:
        # Minimal valid config - just Prometheus
        config = ObservabilityConfig(
            prometheus_enabled=True,
            otlp_enabled=False,
            tracing_enabled=False,
            system_metrics_enabled=False,
        )
        config.validate_config()  # Should not raise

    def test_config_serialization(self) -> None:
        # Test that config can be serialized (useful for debugging)
        config = ObservabilityConfig()
        config_dict = config.model_dump()
        assert isinstance(config_dict, dict)
        assert "service_name" in config_dict

    def test_config_with_very_long_service_name(self) -> None:
        # Test with unusually long service name
        long_name = "a" * 200
        config = ObservabilityConfig(service_name=long_name)
        assert config.service_name == long_name

    def test_config_with_unicode_service_name(self) -> None:
        # Test with unicode characters
        config = ObservabilityConfig(service_name="service-测试-🚀")
        assert config.service_name == "service-测试-🚀"

    def test_config_with_url_encoded_endpoint(self) -> None:
        # Test with URL-encoded characters in endpoint
        config = ObservabilityConfig(
            otlp_endpoint="http://host:4317/v1/metrics?key=value%20with%20spaces"
        )
        assert "value%20with%20spaces" in config.otlp_endpoint
