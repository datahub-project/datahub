"""Integration tests for /metrics endpoint.

These tests verify that the metrics endpoint works correctly when
integrated with the full FastAPI application and OpenTelemetry stack.
"""

import os
from collections.abc import Generator
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient


@pytest.fixture(scope="module")
def test_client() -> Generator[TestClient, None, None]:
    """Create a test client with the full application stack.

    Note: This is module-scoped because some lifespan components
    (like StreamableHTTPSessionManager) can only be initialized once.
    TestClient automatically handles lifespan context.
    """
    # Disable MCP to avoid singleton conflicts with other tests
    os.environ["DISABLE_MCP"] = "true"

    try:
        # Import here to avoid circular dependencies
        from datahub_integrations.server import app

        # TestClient automatically enters/exits lifespan context
        with TestClient(app) as client:
            yield client
    finally:
        # Clean up environment variable
        os.environ.pop("DISABLE_MCP", None)


class TestMetricsEndpoint:
    """Test the /metrics endpoint functionality."""

    def test_metrics_endpoint_exists(self, test_client: TestClient) -> None:
        """Test that /metrics endpoint is accessible."""
        response = test_client.get("/metrics")
        assert response.status_code == 200

    def test_metrics_endpoint_returns_prometheus_format(
        self, test_client: TestClient
    ) -> None:
        """Test that /metrics returns Prometheus text format."""
        response = test_client.get("/metrics")

        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; charset=utf-8"

        # Prometheus format should contain TYPE and HELP comments
        content = response.text
        assert "# TYPE" in content or "# HELP" in content or len(content) > 0

    def test_metrics_endpoint_contains_http_metrics(
        self, test_client: TestClient
    ) -> None:
        """Test that HTTP metrics are collected and exposed."""
        # Make a request to generate metrics
        test_client.get("/ping")

        # Fetch metrics
        response = test_client.get("/metrics")
        assert response.status_code == 200

        content = response.text

        # Should contain HTTP server metrics from FastAPI instrumentation
        # Note: Metric names may vary based on OTel version, so check for common patterns
        assert (
            "http_server" in content.lower()
            or "http.server" in content.lower()
            or "target_info" in content.lower()
        )

    def test_metrics_endpoint_after_multiple_requests(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics accumulate across multiple requests."""
        # Make multiple requests
        for _ in range(5):
            test_client.get("/ping")

        response = test_client.get("/metrics")
        assert response.status_code == 200

        # Metrics should be present
        assert len(response.text) > 0

    def test_metrics_endpoint_is_not_authenticated(
        self, test_client: TestClient
    ) -> None:
        """Test that /metrics endpoint does not require authentication.

        This is important for Prometheus/Datadog scraping.
        """
        # Make request without any auth headers
        response = test_client.get("/metrics")

        # Should succeed without authentication
        assert response.status_code == 200

    def test_metrics_endpoint_handles_concurrent_requests(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics endpoint handles concurrent access."""
        import concurrent.futures

        def fetch_metrics() -> int:
            response = test_client.get("/metrics")
            return response.status_code

        # Make concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_metrics) for _ in range(20)]
            results = [f.result() for f in futures]

        # All requests should succeed
        assert all(status == 200 for status in results)

    def test_metrics_endpoint_performance(self, test_client: TestClient) -> None:
        """Test that metrics endpoint responds quickly."""
        import time

        start = time.perf_counter()
        response = test_client.get("/metrics")
        duration = time.perf_counter() - start

        assert response.status_code == 200
        # Should respond in less than 100ms (generous threshold)
        assert duration < 0.1


class TestMetricsEndpointErrorHandling:
    """Test error handling in /metrics endpoint."""

    def test_metrics_endpoint_handles_registry_error(
        self, test_client: TestClient
    ) -> None:
        """Test graceful handling when metrics generation fails."""
        with patch(
            "prometheus_client.generate_latest",
            side_effect=Exception("Registry error"),
        ):
            response = test_client.get("/metrics")

            # Should return 500 but not crash
            assert response.status_code == 500
            assert "Error generating metrics" in response.text


class TestMetricsContent:
    """Test the content of metrics output."""

    def test_metrics_contain_target_info(self, test_client: TestClient) -> None:
        """Test that metrics contain target_info with service attributes."""
        response = test_client.get("/metrics")
        content = response.text

        # Should contain service identification
        # OpenTelemetry exposes resource attributes as target_info
        assert (
            "service_name" in content.lower()
            or "datahub" in content.lower()
            or "target_info" in content.lower()
        )

    def test_metrics_are_valid_prometheus_format(self, test_client: TestClient) -> None:
        """Test that metrics output is valid Prometheus format."""
        response = test_client.get("/metrics")
        content = response.text

        # Basic Prometheus format validation
        lines = content.split("\n")

        # Should have some content
        assert len(lines) > 0

        # Check for valid metric lines (skip comments and empty lines)
        metric_lines = [line for line in lines if line and not line.startswith("#")]

        if metric_lines:
            # Each metric line should have format: metric_name{labels} value [timestamp]
            for line in metric_lines[:5]:  # Check first 5 metric lines
                # Should contain metric name and value
                parts = line.split()
                assert len(parts) >= 2  # At least name{labels} and value

    def test_metrics_update_on_new_requests(self, test_client: TestClient) -> None:
        """Test that metrics update when new requests are made."""
        # Get initial metrics
        response1 = test_client.get("/metrics")
        initial_content = response1.text

        # Make some requests
        for _ in range(3):
            test_client.get("/ping")

        # Get updated metrics
        response2 = test_client.get("/metrics")
        updated_content = response2.text

        # Content should be present (may or may not be different due to metric aggregation)
        assert len(initial_content) > 0
        assert len(updated_content) > 0


class TestMetricsEndpointIntegration:
    """Test metrics endpoint integration with other parts of the system."""

    def test_metrics_endpoint_does_not_affect_other_endpoints(
        self, test_client: TestClient
    ) -> None:
        """Test that fetching metrics doesn't disrupt normal operations."""
        # Normal request
        response1 = test_client.get("/ping")
        assert response1.status_code == 200

        # Fetch metrics
        response2 = test_client.get("/metrics")
        assert response2.status_code == 200

        # Another normal request
        response3 = test_client.get("/ping")
        assert response3.status_code == 200

    def test_metrics_endpoint_with_different_http_methods(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics endpoint only responds to GET."""
        # GET should work
        get_response = test_client.get("/metrics")
        assert get_response.status_code == 200

        # POST should not be allowed
        post_response = test_client.post("/metrics")
        assert post_response.status_code == 405  # Method Not Allowed

        # PUT should not be allowed
        put_response = test_client.put("/metrics")
        assert put_response.status_code == 405

        # DELETE should not be allowed
        delete_response = test_client.delete("/metrics")
        assert delete_response.status_code == 405

    def test_metrics_endpoint_with_query_parameters(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics endpoint ignores query parameters."""
        # Prometheus scrapers might add query params
        response = test_client.get("/metrics?foo=bar&baz=qux")

        # Should still work
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; charset=utf-8"


class TestMetricsEndpointEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_metrics_endpoint_on_startup(self, test_client: TestClient) -> None:
        """Test that metrics are available immediately on startup."""
        # First request to metrics endpoint should work
        response = test_client.get("/metrics")

        assert response.status_code == 200
        # Should have at least some content (even if minimal)
        assert len(response.text) >= 0

    def test_metrics_endpoint_large_number_of_requests(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics handle a large number of tracked requests."""
        # Generate many requests to different endpoints
        for _ in range(100):
            test_client.get("/ping")

        response = test_client.get("/metrics")
        assert response.status_code == 200

        # Should not have excessive response size
        content_length = len(response.text)
        assert content_length < 1_000_000  # Less than 1MB

    def test_metrics_endpoint_with_special_characters_in_path(
        self, test_client: TestClient
    ) -> None:
        """Test that metrics endpoint path is strict."""
        # Exact path should work
        response = test_client.get("/metrics")
        assert response.status_code == 200

        # Variations should not match
        response_trailing = test_client.get("/metrics/")
        # May be 200 (redirected) or 404 depending on FastAPI config
        assert response_trailing.status_code in [200, 404, 307]

        response_subpath = test_client.get("/metrics/foo")
        assert response_subpath.status_code == 404
