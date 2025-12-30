"""Test that observability gracefully degrades when disabled."""

import pytest
from opentelemetry import metrics

from datahub_integrations.observability.config import ObservabilityConfig
from datahub_integrations.observability.cost import TokenUsage, get_cost_tracker
from datahub_integrations.observability.decorators import otel_duration, otel_instrument
from datahub_integrations.observability.metrics_constants import AIModule
from datahub_integrations.observability.otel_config import (
    OTELConfigurationError,
    initialize_observability,
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


def test_graceful_degradation_with_both_exporters_disabled():
    """Test that system works when both Prometheus and OTLP are disabled."""
    # This should raise an error during initialization
    config = ObservabilityConfig(
        prometheus_enabled=False,
        otlp_enabled=False,
    )

    with pytest.raises(
        OTELConfigurationError, match="Observability initialization failed"
    ):
        initialize_observability(config)

    # But decorators should still work with NoOp implementations
    @otel_duration("test_metric", "Test metric")
    def test_function():
        return "success"

    # Should not raise even though OTEL wasn't initialized
    result = test_function()
    assert result == "success"


def test_graceful_degradation_cost_tracker_without_otel():
    """Test that CostTracker works without OTEL initialization."""
    # Don't initialize OTEL at all
    tracker = get_cost_tracker()

    # Should not raise even without OTEL
    usage = TokenUsage(
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
    )

    cost = tracker.record_llm_call(
        provider="anthropic",
        model="claude-3-5-sonnet",
        usage=usage,
        ai_module=AIModule.CHAT,
        success=True,
    )

    # Cost calculation should still work (doesn't require OTEL)
    assert cost is not None
    assert cost.total_cost > 0


def test_graceful_degradation_decorators_without_otel():
    """Test that all decorators work without OTEL initialization."""

    @otel_instrument(metric_prefix="test", description="Test function")
    def instrumented_function():
        return "success"

    @otel_duration("test_duration", "Test duration")
    def timed_function():
        return "success"

    # Should not raise even without OTEL initialization
    result1 = instrumented_function()
    result2 = timed_function()

    assert result1 == "success"
    assert result2 == "success"


def test_graceful_degradation_metrics_get_meter_without_init():
    """Test that OpenTelemetry NoOp meter works correctly."""
    # Don't initialize OTEL - this tests the fallback behavior
    meter = metrics.get_meter("test")

    # Should return a NoOp meter, not raise
    assert meter is not None

    # NoOp meter should allow creating instruments
    counter = meter.create_counter("test_counter")
    histogram = meter.create_histogram("test_histogram")

    # Should not raise when recording
    counter.add(1, {"label": "value"})
    histogram.record(1.0, {"label": "value"})
