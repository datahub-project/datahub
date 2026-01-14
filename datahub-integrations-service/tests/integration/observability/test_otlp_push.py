"""Integration tests for OTLP push from SubprocessMetricsBridge.

These tests verify that metrics scraped from subprocesses are actually
pushed via OTLP to a collector. Uses a mock gRPC server to capture
and validate OTLP payloads.
"""

import asyncio
import time
from concurrent import futures
from unittest.mock import patch

import grpc  # type: ignore[import-untyped]
import pytest
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from datahub_integrations.observability.subprocess_metrics_bridge import (
    SubprocessMetricsBridge,
)


class MockOTLPCollector(metrics_service_pb2_grpc.MetricsServiceServicer):
    """Mock OTLP gRPC collector that captures export requests."""

    def __init__(self):
        self.export_requests = []
        self.export_count = 0

    def Export(self, request, context):
        """Handle Export RPC - capture the request."""
        self.export_requests.append(request)
        self.export_count += 1
        return metrics_service_pb2.ExportMetricsServiceResponse()


@pytest.fixture
async def mock_otlp_server():
    """Start a mock OTLP gRPC server for testing."""
    collector = MockOTLPCollector()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(collector, server)

    # Use a random available port
    port = server.add_insecure_port("[::]:0")
    server.start()

    yield collector, f"localhost:{port}"

    server.stop(grace=1.0)


@pytest.fixture
async def mock_subprocess_server(unused_tcp_port):
    """Start a mock HTTP server that returns Prometheus metrics."""
    from aiohttp import web

    metrics_text = """
# HELP kafka_consumer_lag Kafka consumer lag per topic
# TYPE kafka_consumer_lag gauge
kafka_consumer_lag{topic="test_topic",pipeline_name="test_pipeline"} 1250.0

# HELP kafka_offset Kafka offsets per topic, partition
# TYPE kafka_offset gauge
kafka_offset{topic="test_topic",partition="0",pipeline_name="test_pipeline"} 5000.0

# HELP actions_running Number of currently running actions
# TYPE actions_running gauge
actions_running{stage="BOOTSTRAP",action_urn="urn:li:dataHubAction:test"} 1.0
"""

    async def metrics_handler(request):
        return web.Response(text=metrics_text, content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", unused_tcp_port)
    await site.start()

    yield f"http://localhost:{unused_tcp_port}"

    await runner.cleanup()


class TestOTLPPushIntegration:
    """Integration tests for OTLP push functionality."""

    @pytest.mark.anyio
    @pytest.mark.integration
    async def test_metrics_pushed_to_otlp_collector(
        self, mock_otlp_server, mock_subprocess_server
    ):
        """Test that scraped metrics are pushed to OTLP collector.

        This is the main end-to-end test:
        1. Start mock subprocess with Prometheus metrics
        2. Start mock OTLP collector
        3. Configure bridge to scrape subprocess and push to OTLP
        4. Verify metrics arrive at collector with correct format
        """
        collector, otlp_endpoint = mock_otlp_server

        # Configure OpenTelemetry to push to our mock collector
        resource = Resource.create(
            {
                "service.name": "datahub-integrations-service-test",
            }
        )

        otlp_exporter = OTLPMetricExporter(
            endpoint=otlp_endpoint,
            insecure=True,  # No TLS for testing
        )

        reader = PeriodicExportingMetricReader(
            otlp_exporter,
            export_interval_millis=1000,  # Export every 1s for fast testing
        )

        provider = MeterProvider(resource=resource, metric_readers=[reader])

        # Get meter from provider and pass to bridge
        meter = provider.get_meter(__name__)

        # Create bridge with short scrape interval for testing
        bridge = SubprocessMetricsBridge(
            subprocess_urls=[mock_subprocess_server],
            scrape_interval_seconds=0.5,  # Scrape every 500ms
            meter=meter,
        )

        # Start bridge
        await bridge.start()

        # Wait for metrics to be scraped and exported
        # Need to wait for: scrape (500ms) + export interval (1000ms) + buffer
        await asyncio.sleep(2.5)

        # Stop bridge
        await bridge.stop()

        # Force flush to ensure all metrics exported
        provider.force_flush()

        # Shutdown provider to stop background threads cleanly
        provider.shutdown()

        # Verify metrics were pushed to collector
        assert collector.export_count > 0, "No metrics were exported to OTLP collector"

        # Verify the exported metrics contain our subprocess metrics
        found_kafka_lag = False
        found_kafka_offset = False
        found_actions_running = False

        for request in collector.export_requests:
            for resource_metrics in request.resource_metrics:
                for scope_metrics in resource_metrics.scope_metrics:
                    for metric in scope_metrics.metrics:
                        if metric.name == "actions.kafka_consumer_lag":
                            found_kafka_lag = True
                            # Verify it has data points
                            assert len(metric.gauge.data_points) > 0
                            # Verify attributes
                            dp = metric.gauge.data_points[0]
                            attrs = {attr.key: attr.value for attr in dp.attributes}
                            assert "topic" in attrs
                            assert "pipeline_name" in attrs

                        elif metric.name == "actions.kafka_offset":
                            found_kafka_offset = True

                        elif metric.name == "actions.actions_running":
                            found_actions_running = True

        assert found_kafka_lag, "kafka_consumer_lag metric not found in OTLP export"
        assert found_kafka_offset, "kafka_offset metric not found in OTLP export"
        assert found_actions_running, "actions_running metric not found in OTLP export"

    @pytest.mark.anyio
    @pytest.mark.integration
    async def test_otlp_push_with_collector_unavailable(self, mock_subprocess_server):
        """Test that bridge handles OTLP collector being unavailable gracefully."""
        # Point to a non-existent OTLP endpoint
        bad_endpoint = "localhost:9999"

        resource = Resource.create({"service.name": "test"})
        otlp_exporter = OTLPMetricExporter(
            endpoint=bad_endpoint,
            insecure=True,
        )
        reader = PeriodicExportingMetricReader(
            otlp_exporter,
            export_interval_millis=500,
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        meter = provider.get_meter(__name__)
        bridge = SubprocessMetricsBridge(
            subprocess_urls=[mock_subprocess_server],
            scrape_interval_seconds=0.5,
            meter=meter,
        )

        # Should not raise exception even though OTLP endpoint is down
        await bridge.start()
        await asyncio.sleep(1.5)

        # Bridge should still be running and scraping
        assert bridge._running
        assert len(bridge._latest_metrics) > 0

        await bridge.stop()

        # Shutdown provider to stop background threads cleanly
        provider.shutdown()

    @pytest.mark.anyio
    @pytest.mark.integration
    async def test_metric_attributes_preserved_in_otlp(
        self, mock_otlp_server, mock_subprocess_server
    ):
        """Test that metric labels are preserved as OTLP attributes."""
        collector, otlp_endpoint = mock_otlp_server

        resource = Resource.create({"service.name": "test"})
        otlp_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(
            otlp_exporter, export_interval_millis=1000
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        meter = provider.get_meter(__name__)
        bridge = SubprocessMetricsBridge(
            subprocess_urls=[mock_subprocess_server],
            scrape_interval_seconds=0.5,
            meter=meter,
        )

        await bridge.start()
        await asyncio.sleep(2.5)
        await bridge.stop()
        provider.force_flush()

        # Shutdown provider to stop background threads cleanly
        provider.shutdown()

        # Find kafka_consumer_lag metric and verify attributes
        for request in collector.export_requests:
            for resource_metrics in request.resource_metrics:
                for scope_metrics in resource_metrics.scope_metrics:
                    for metric in scope_metrics.metrics:
                        if metric.name == "actions.kafka_consumer_lag":
                            dp = metric.gauge.data_points[0]
                            attrs = {
                                attr.key: (
                                    attr.value.string_value
                                    if attr.value.HasField("string_value")
                                    else None
                                )
                                for attr in dp.attributes
                            }

                            # Verify expected attributes
                            assert attrs.get("topic") == "test_topic"
                            assert attrs.get("pipeline_name") == "test_pipeline"
                            assert attrs.get("service.name") == "datahub-actions"
                            return

        pytest.fail("kafka_consumer_lag metric not found with expected attributes")


class TestOTLPExportTiming:
    """Test timing and interval behavior of OTLP exports."""

    @pytest.mark.anyio
    @pytest.mark.integration
    async def test_metrics_exported_periodically(
        self, mock_otlp_server, mock_subprocess_server
    ):
        """Test that metrics are exported at regular intervals."""
        collector, otlp_endpoint = mock_otlp_server

        resource = Resource.create({"service.name": "test"})
        otlp_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)

        # Export every 500ms
        reader = PeriodicExportingMetricReader(
            otlp_exporter, export_interval_millis=500
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        meter = provider.get_meter(__name__)
        bridge = SubprocessMetricsBridge(
            subprocess_urls=[mock_subprocess_server],
            scrape_interval_seconds=0.2,  # Scrape frequently
            meter=meter,
        )

        await bridge.start()

        # Record export counts over time
        initial_count = collector.export_count
        await asyncio.sleep(1.0)
        mid_count = collector.export_count
        await asyncio.sleep(1.0)
        final_count = collector.export_count

        await bridge.stop()

        # Shutdown provider to stop background threads cleanly
        provider.shutdown()

        # Should have multiple exports
        assert mid_count > initial_count, "No exports in first interval"
        assert final_count > mid_count, "No exports in second interval"

        # Roughly 2 exports per second (every 500ms)
        # Allow some variance due to timing
        assert final_count >= 2, "Expected at least 2 exports over 2 seconds"

    @pytest.mark.anyio
    @pytest.mark.integration
    async def test_scrape_interval_respected(self, mock_subprocess_server):
        """Test that scrape interval is respected."""
        scrape_times = []

        original_scrape = SubprocessMetricsBridge._scrape_subprocess

        async def tracking_scrape(self, url):
            scrape_times.append(time.time())
            return await original_scrape(self, url)

        with patch.object(
            SubprocessMetricsBridge, "_scrape_subprocess", tracking_scrape
        ):
            bridge = SubprocessMetricsBridge(
                subprocess_urls=[mock_subprocess_server],
                scrape_interval_seconds=0.5,  # 500ms interval
            )

            await bridge.start()
            await asyncio.sleep(2.5)  # Run for 2.5 seconds
            await bridge.stop()

        # Should have ~5 scrapes (2.5s / 0.5s = 5)
        # Allow variance: 4-6 scrapes is acceptable
        assert 4 <= len(scrape_times) <= 6, f"Got {len(scrape_times)} scrapes"

        # Verify intervals between scrapes are roughly 500ms
        if len(scrape_times) >= 2:
            intervals = [
                scrape_times[i] - scrape_times[i - 1]
                for i in range(1, len(scrape_times))
            ]
            avg_interval = sum(intervals) / len(intervals)

            # Allow 20% variance
            assert 0.4 <= avg_interval <= 0.6, (
                f"Average interval {avg_interval}s not close to 0.5s"
            )


@pytest.fixture
def unused_tcp_port():
    """Get an unused TCP port for testing."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port
