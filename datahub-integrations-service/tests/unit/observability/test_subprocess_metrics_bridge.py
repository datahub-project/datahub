"""Unit tests for SubprocessMetricsBridge.

Tests the core functionality of scraping Prometheus metrics from subprocesses
and converting them to OpenTelemetry observations for OTLP export.
"""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from datahub_integrations.observability.subprocess_metrics_bridge import (
    SubprocessMetric,
    SubprocessMetricsBridge,
    create_subprocess_bridge_if_enabled,
)


class TestPrometheusMetricsParsing:
    """Test parsing of Prometheus text format."""

    def test_parse_simple_gauge(self):
        """Test parsing a simple gauge metric."""
        metrics_text = """
# HELP kafka_consumer_lag Kafka consumer lag per topic
# TYPE kafka_consumer_lag gauge
kafka_consumer_lag{topic="MetadataChangeLog_Versioned_v1",pipeline_name="tag-propagation"} 1250
"""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])
        metrics = bridge._parse_prometheus_metrics(metrics_text)

        assert len(metrics) == 1
        metric = metrics[0]
        assert metric.name == "kafka_consumer_lag"
        assert metric.value == 1250
        assert metric.labels == {
            "topic": "MetadataChangeLog_Versioned_v1",
            "pipeline_name": "tag-propagation",
        }
        assert metric.metric_type == "gauge"

    def test_parse_counter_with_total_suffix(self):
        """Test parsing counter with _total suffix."""
        metrics_text = """
# TYPE kafka_messages_total counter
kafka_messages_total{pipeline_name="test",error="False"} 42
"""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])
        metrics = bridge._parse_prometheus_metrics(metrics_text)

        assert len(metrics) == 1
        assert metrics[0].name == "kafka_messages_total"
        assert metrics[0].value == 42
        assert metrics[0].metric_type == "counter"

    def test_parse_multiple_metrics(self):
        """Test parsing multiple metrics with different labels."""
        metrics_text = """
kafka_consumer_lag{topic="topic1",pipeline_name="pipe1"} 100
kafka_consumer_lag{topic="topic2",pipeline_name="pipe1"} 200
kafka_offset{topic="topic1",partition="0",pipeline_name="pipe1"} 1000
"""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])
        metrics = bridge._parse_prometheus_metrics(metrics_text)

        assert len(metrics) == 3
        assert metrics[0].value == 100
        assert metrics[1].value == 200
        assert metrics[2].value == 1000

    def test_parse_empty_metrics(self):
        """Test parsing empty metrics text."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])
        metrics = bridge._parse_prometheus_metrics("")

        assert metrics == []

    def test_parse_metrics_with_comments_only(self):
        """Test parsing metrics with only comments."""
        metrics_text = """
# HELP some_metric Some metric
# TYPE some_metric gauge
"""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])
        metrics = bridge._parse_prometheus_metrics(metrics_text)

        assert metrics == []


class TestSubprocessScraping:
    """Test scraping metrics from subprocess /metrics endpoints."""

    @pytest.mark.anyio
    async def test_scrape_subprocess_success(self):
        """Test successful scraping of subprocess metrics."""
        metrics_text = """
kafka_consumer_lag{topic="test",pipeline_name="test"} 42
"""
        mock_response = Mock()
        mock_response.text = metrics_text
        mock_response.raise_for_status = Mock()

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            bridge = SubprocessMetricsBridge(subprocess_urls=["http://localhost:8000"])
            metrics = await bridge._scrape_subprocess("http://localhost:8000")

            assert len(metrics) == 1
            assert metrics[0].value == 42
            mock_client.get.assert_called_once_with(
                "http://localhost:8000/metrics", timeout=5.0
            )

    @pytest.mark.anyio
    async def test_scrape_subprocess_http_error(self):
        """Test handling of HTTP errors during scraping."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(
                side_effect=httpx.HTTPError("Connection failed")
            )
            mock_client_class.return_value = mock_client

            bridge = SubprocessMetricsBridge(subprocess_urls=["http://localhost:8000"])

            with pytest.raises(httpx.HTTPError):
                await bridge._scrape_subprocess("http://localhost:8000")

    @pytest.mark.anyio
    async def test_scrape_subprocess_timeout(self):
        """Test handling of timeout during scraping."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
            mock_client_class.return_value = mock_client

            bridge = SubprocessMetricsBridge(
                subprocess_urls=["http://localhost:8000"], timeout_seconds=1.0
            )

            with pytest.raises(httpx.TimeoutException):
                await bridge._scrape_subprocess("http://localhost:8000")

    @pytest.mark.anyio
    async def test_scrape_all_subprocesses_with_failure(self):
        """Test scraping multiple subprocesses with one failure."""
        success_metrics = """
kafka_consumer_lag{topic="test",pipeline_name="test"} 100
"""
        mock_response = Mock()
        mock_response.text = success_metrics
        mock_response.raise_for_status = Mock()

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client

            # First subprocess succeeds, second fails
            mock_client.get = AsyncMock(
                side_effect=[
                    mock_response,  # First call succeeds
                    httpx.HTTPError("Connection failed"),  # Second fails
                ]
            )
            mock_client_class.return_value = mock_client

            bridge = SubprocessMetricsBridge(
                subprocess_urls=["http://localhost:8000", "http://localhost:8001"]
            )

            await bridge._scrape_all_subprocesses()

            # Should have metrics from first subprocess only
            assert "http://localhost:8000" in bridge._latest_metrics
            assert "http://localhost:8001" not in bridge._latest_metrics
            assert len(bridge._latest_metrics["http://localhost:8000"]) == 1


class TestMetricsRetrieval:
    """Test retrieving specific metrics by name."""

    def test_get_metrics_by_name_exact_match(self):
        """Test getting metrics by exact name match."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        # Populate with test metrics
        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=100,
                labels={"topic": "test"},
                metric_type="gauge",
            ),
            SubprocessMetric(
                name="kafka_offset",
                value=1000,
                labels={"topic": "test"},
                metric_type="gauge",
            ),
        ]

        metrics = bridge._get_metrics_by_name("kafka_consumer_lag")
        assert len(metrics) == 1
        assert metrics[0].value == 100

    def test_get_metrics_by_name_prefix_match(self):
        """Test getting metrics by prefix match."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_messages_total",
                value=42,
                labels={"error": "False"},
                metric_type="counter",
            ),
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=100,
                labels={"topic": "test"},
                metric_type="gauge",
            ),
        ]

        # Get all metrics starting with "kafka_"
        metrics = bridge._get_metrics_by_name("kafka_")
        assert len(metrics) == 2

    def test_get_metrics_adds_subprocess_url_label(self):
        """Test that subprocess URL is added as a label."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=100,
                labels={"topic": "test"},
                metric_type="gauge",
            ),
        ]

        metrics = bridge._get_metrics_by_name("kafka_consumer_lag")
        assert metrics[0].labels["subprocess_url"] == "http://localhost:8000"

    def test_get_metrics_from_multiple_subprocesses(self):
        """Test getting metrics from multiple subprocesses."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=100,
                labels={"topic": "topic1"},
                metric_type="gauge",
            ),
        ]
        bridge._latest_metrics["http://localhost:8001"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=200,
                labels={"topic": "topic2"},
                metric_type="gauge",
            ),
        ]

        metrics = bridge._get_metrics_by_name("kafka_consumer_lag")
        assert len(metrics) == 2
        values = [m.value for m in metrics]
        assert 100 in values
        assert 200 in values


class TestObservationConversion:
    """Test conversion of scraped metrics to OpenTelemetry observations."""

    @pytest.mark.anyio
    async def test_collect_kafka_lag_observations(self):
        """Test converting kafka lag metrics to observations."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        # Populate with test metrics
        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=1250,
                labels={
                    "topic": "MetadataChangeLog_Versioned_v1",
                    "pipeline_name": "tag-propagation",
                },
                metric_type="gauge",
            ),
        ]

        observations = bridge._collect_kafka_lag(None)

        assert len(observations) == 1
        obs = observations[0]
        assert obs.value == 1250
        assert obs.attributes["topic"] == "MetadataChangeLog_Versioned_v1"
        assert obs.attributes["pipeline_name"] == "tag-propagation"
        assert obs.attributes["service.name"] == "datahub-actions"

    @pytest.mark.anyio
    async def test_collect_multiple_observations(self):
        """Test collecting observations from multiple subprocesses."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        bridge._latest_metrics["http://localhost:8000"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=100,
                labels={"topic": "topic1", "pipeline_name": "pipe1"},
                metric_type="gauge",
            ),
        ]
        bridge._latest_metrics["http://localhost:8001"] = [
            SubprocessMetric(
                name="kafka_consumer_lag",
                value=200,
                labels={"topic": "topic2", "pipeline_name": "pipe2"},
                metric_type="gauge",
            ),
        ]

        observations = bridge._collect_kafka_lag(None)

        assert len(observations) == 2
        values = [obs.value for obs in observations]
        assert 100 in values
        assert 200 in values


class TestBridgeLifecycle:
    """Test bridge start/stop lifecycle."""

    @pytest.mark.anyio
    async def test_start_bridge(self):
        """Test starting the bridge."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        assert not bridge._running
        await bridge.start()
        assert bridge._running
        assert bridge._scrape_task is not None

        await bridge.stop()

    @pytest.mark.anyio
    async def test_stop_bridge(self):
        """Test stopping the bridge."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        await bridge.start()
        await bridge.stop()

        assert not bridge._running
        # Task should be cancelled
        assert bridge._scrape_task.cancelled()

    @pytest.mark.anyio
    async def test_start_already_running(self):
        """Test starting bridge that's already running."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        await bridge.start()
        await bridge.start()  # Should log warning but not error

        assert bridge._running
        await bridge.stop()

    @pytest.mark.anyio
    async def test_stop_not_running(self):
        """Test stopping bridge that's not running."""
        bridge = SubprocessMetricsBridge(subprocess_urls=[])

        # Should not raise error
        await bridge.stop()


class TestBridgeCreation:
    """Test factory function for creating bridge."""

    @pytest.mark.anyio
    async def test_create_bridge_when_otlp_enabled(self):
        """Test creating bridge when OTLP is enabled."""
        with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENABLED": "true"}):
            bridge = await create_subprocess_bridge_if_enabled(
                ["http://localhost:8000"]
            )

            assert bridge is not None
            assert bridge._running
            await bridge.stop()

    @pytest.mark.anyio
    async def test_create_bridge_when_otlp_disabled(self):
        """Test not creating bridge when OTLP is disabled."""
        with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENABLED": "false"}):
            bridge = await create_subprocess_bridge_if_enabled(
                ["http://localhost:8000"]
            )

            assert bridge is None

    @pytest.mark.anyio
    async def test_create_bridge_with_no_subprocesses(self):
        """Test not creating bridge when no subprocess URLs provided."""
        with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENABLED": "true"}):
            bridge = await create_subprocess_bridge_if_enabled([])

            assert bridge is None

    @pytest.mark.anyio
    async def test_create_bridge_with_custom_scrape_interval(self):
        """Test creating bridge with custom scrape interval."""
        with patch.dict(
            "os.environ",
            {
                "OTEL_EXPORTER_OTLP_ENABLED": "true",
                "SUBPROCESS_METRICS_SCRAPE_INTERVAL": "60",
            },
        ):
            bridge = await create_subprocess_bridge_if_enabled(
                ["http://localhost:8000"]
            )

            assert bridge is not None
            assert bridge.scrape_interval == 60
            await bridge.stop()
