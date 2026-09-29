"""Unit tests for API metrics tracking in Snowplow connector."""

import pytest

from datahub.ingestion.source.snowplow.snowplow_report import (
    APICallTimer,
    APIMetrics,
    SnowplowSourceReport,
)


class TestAPIMetrics:
    """Test APIMetrics dataclass functionality."""

    def test_record_call_updates_counts(self) -> None:
        """Test that recording calls updates count and latency."""
        metrics = APIMetrics()

        metrics.record_call(100.0)
        assert metrics.call_count == 1
        assert metrics.total_latency_ms == 100.0
        assert metrics.error_count == 0

        metrics.record_call(200.0, is_error=True)
        assert metrics.call_count == 2
        assert metrics.total_latency_ms == 300.0
        assert metrics.error_count == 1

    def test_avg_latency_calculation(self) -> None:
        """Test average latency calculation."""
        metrics = APIMetrics()
        assert metrics.avg_latency_ms == 0.0  # No calls yet

        metrics.record_call(100.0)
        metrics.record_call(200.0)
        metrics.record_call(300.0)

        assert metrics.avg_latency_ms == 200.0

    def test_percentile_calculation(self) -> None:
        """Test percentile calculation."""
        metrics = APIMetrics()

        # Add 10 samples: 10, 20, 30, ..., 100
        for i in range(1, 11):
            metrics.record_call(i * 10.0)

        # p50 (median) should be around 50-60
        p50 = metrics.p50_ms
        assert 40 <= p50 <= 60

        # p99 should be close to 100
        p99 = metrics.p99_ms
        assert p99 >= 90

    def test_percentile_empty_list(self) -> None:
        """Test percentile returns 0 for empty list."""
        metrics = APIMetrics()
        assert metrics.p50_ms == 0.0
        assert metrics.p95_ms == 0.0
        assert metrics.p99_ms == 0.0

    def test_reservoir_sampling_limits_memory(self) -> None:
        """Test that reservoir sampling keeps samples bounded."""
        metrics = APIMetrics()

        # Record more than MAX_LATENCY_SAMPLES calls
        for i in range(metrics.MAX_LATENCY_SAMPLES + 500):
            metrics.record_call(float(i))

        # Samples should be capped at MAX_LATENCY_SAMPLES
        assert len(metrics.latencies_ms) == metrics.MAX_LATENCY_SAMPLES


class TestAPICallTimer:
    """Test APICallTimer context manager."""

    def test_timer_records_successful_call(self) -> None:
        """Test timer records latency for successful call."""
        report = SnowplowSourceReport()

        with APICallTimer(report, "test_endpoint", client="bdp"):
            pass  # Simulated API call

        assert "bdp" in report.api_metrics
        assert "test_endpoint" in report.api_metrics["bdp"]
        metrics = report.api_metrics["bdp"]["test_endpoint"]
        assert metrics.call_count == 1
        assert metrics.error_count == 0
        assert metrics.total_latency_ms > 0

    def test_timer_records_error_on_exception(self) -> None:
        """Test timer marks error when exception occurs."""
        report = SnowplowSourceReport()

        with (
            pytest.raises(ValueError),
            APICallTimer(report, "test_endpoint", client="bdp"),
        ):
            raise ValueError("Test error")

        metrics = report.api_metrics["bdp"]["test_endpoint"]
        assert metrics.call_count == 1
        assert metrics.error_count == 1

    def test_timer_mark_error_manually(self) -> None:
        """Test timer can be manually marked as error."""
        report = SnowplowSourceReport()

        with APICallTimer(report, "test_endpoint", client="iglu") as timer:
            timer.mark_error()

        metrics = report.api_metrics["iglu"]["test_endpoint"]
        assert metrics.call_count == 1
        assert metrics.error_count == 1


class TestSnowplowSourceReportAPIMetrics:
    """Test API metrics methods on SnowplowSourceReport."""

    def test_record_api_call(self) -> None:
        """Test recording API calls creates correct structure."""
        report = SnowplowSourceReport()

        report.record_api_call("bdp", "data_structures", 150.0)
        report.record_api_call("bdp", "event_specs", 200.0)
        report.record_api_call("iglu", "get_schema", 50.0)

        assert "bdp" in report.api_metrics
        assert "iglu" in report.api_metrics
        assert "data_structures" in report.api_metrics["bdp"]
        assert "event_specs" in report.api_metrics["bdp"]
        assert "get_schema" in report.api_metrics["iglu"]

    def test_get_api_metrics_summary(self) -> None:
        """Test summary generation includes all metrics."""
        report = SnowplowSourceReport()

        # Add some API calls
        report.record_api_call("bdp", "data_structures", 100.0)
        report.record_api_call("bdp", "data_structures", 200.0)
        report.record_api_call("bdp", "data_structures", 300.0, is_error=True)

        summary = report.get_api_metrics_summary()

        assert "bdp" in summary
        assert "data_structures" in summary["bdp"]

        endpoint_summary = summary["bdp"]["data_structures"]
        assert endpoint_summary["call_count"] == 3
        assert endpoint_summary["error_count"] == 1
        assert endpoint_summary["avg_latency_ms"] == 200.0
        assert endpoint_summary["total_latency_ms"] == 600.0
        assert "p50_latency_ms" in endpoint_summary
        assert "p95_latency_ms" in endpoint_summary
        assert "p99_latency_ms" in endpoint_summary

    def test_multiple_endpoints_tracked_separately(self) -> None:
        """Test that different endpoints are tracked independently."""
        report = SnowplowSourceReport()

        report.record_api_call("bdp", "endpoint_a", 100.0)
        report.record_api_call("bdp", "endpoint_b", 200.0)
        report.record_api_call("bdp", "endpoint_a", 150.0)

        assert report.api_metrics["bdp"]["endpoint_a"].call_count == 2
        assert report.api_metrics["bdp"]["endpoint_b"].call_count == 1

    def test_different_clients_tracked_separately(self) -> None:
        """Test that BDP and Iglu clients are tracked separately."""
        report = SnowplowSourceReport()

        report.record_api_call("bdp", "schemas", 100.0)
        report.record_api_call("iglu", "schemas", 50.0)

        assert report.api_metrics["bdp"]["schemas"].call_count == 1
        assert report.api_metrics["iglu"]["schemas"].call_count == 1
        assert report.api_metrics["bdp"]["schemas"].total_latency_ms == 100.0
        assert report.api_metrics["iglu"]["schemas"].total_latency_ms == 50.0
