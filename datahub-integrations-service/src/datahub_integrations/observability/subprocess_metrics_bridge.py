"""Bridge to relay Prometheus metrics from action subprocesses to OTLP.

This module scrapes Prometheus /metrics endpoints from action subprocesses
and re-exports them via OpenTelemetry OTLP push. This allows subprocess
metrics to reach enterprise observability backends (Datadog, Grafana Cloud,
etc.) without requiring OTLP support in the OSS datahub-actions library.

Architecture:
- Main service scrapes subprocess /metrics (Prometheus text format)
- Parse metrics using prometheus_client parser
- Convert to OpenTelemetry observations
- OTLP exporter pushes to configured collector

Environment Variables:
- OTEL_EXPORTER_OTLP_ENABLED: Enable OTLP push (default: false)
- OTEL_EXPORTER_OTLP_ENDPOINT: Collector endpoint (e.g., http://localhost:4317)
- SUBPROCESS_METRICS_SCRAPE_INTERVAL: Scrape interval in seconds (default: 30)
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from opentelemetry import metrics
from opentelemetry.metrics import Observation
from prometheus_client.parser import text_string_to_metric_families

logger = logging.getLogger(__name__)


@dataclass
class SubprocessMetric:
    """Parsed metric from subprocess."""

    name: str
    value: float
    labels: dict[str, str]
    metric_type: str  # gauge, counter, histogram, summary


class SubprocessMetricsBridge:
    """Bridges Prometheus metrics from subprocesses to OTLP.

    Usage:
        bridge = SubprocessMetricsBridge(
            subprocess_urls=["http://localhost:8000", "http://localhost:8001"]
        )
        await bridge.start()  # Begins periodic scraping
    """

    def __init__(
        self,
        subprocess_urls: list[str],
        scrape_interval_seconds: float = 30.0,
        timeout_seconds: float = 5.0,
        meter: Optional[Any] = None,
    ):
        self.subprocess_urls = subprocess_urls
        self.scrape_interval = scrape_interval_seconds
        self.timeout = timeout_seconds
        self._running = False
        self._scrape_task: Optional[asyncio.Task] = None

        # Create OpenTelemetry meter (or use provided one for testing)
        if meter is None:
            meter = metrics.get_meter(__name__)

        # Track subprocess metrics we care about
        self._metrics_registry: dict[str, Any] = {}

        # Kafka lag gauge
        self.kafka_lag_gauge = meter.create_observable_gauge(
            name="actions.kafka_consumer_lag",
            description="Kafka consumer lag from action subprocesses",
            callbacks=[self._collect_kafka_lag],
        )

        # Kafka offset gauge
        self.kafka_offset_gauge = meter.create_observable_gauge(
            name="actions.kafka_offset",
            description="Kafka offsets from action subprocesses",
            callbacks=[self._collect_kafka_offset],
        )

        # Kafka messages counter
        self.kafka_messages_counter = meter.create_observable_counter(
            name="actions.kafka_messages_total",
            description="Total Kafka messages processed",
            callbacks=[self._collect_kafka_messages],
        )

        # Actions running gauge
        self.actions_running_gauge = meter.create_observable_gauge(
            name="actions.actions_running",
            description="Number of currently running actions",
            callbacks=[self._collect_actions_running],
        )

        # Actions success counter
        self.actions_success_counter = meter.create_observable_counter(
            name="actions.actions_success_total",
            description="Total successful actions",
            callbacks=[self._collect_actions_success],
        )

        # Actions error counter
        self.actions_error_counter = meter.create_observable_counter(
            name="actions.actions_error_total",
            description="Total failed actions",
            callbacks=[self._collect_actions_error],
        )

        # Latest scraped metrics (shared across all callbacks)
        self._latest_metrics: dict[str, list[SubprocessMetric]] = {}

        logger.info(
            f"SubprocessMetricsBridge initialized with {len(subprocess_urls)} "
            f"subprocess(es), scrape_interval={scrape_interval_seconds}s"
        )

    async def start(self) -> None:
        """Start periodic scraping of subprocess metrics."""
        if self._running:
            logger.warning("SubprocessMetricsBridge already running")
            return

        self._running = True
        self._scrape_task = asyncio.create_task(self._scrape_loop())
        logger.info("SubprocessMetricsBridge started")

    async def stop(self) -> None:
        """Stop periodic scraping."""
        if not self._running:
            return

        self._running = False
        if self._scrape_task:
            self._scrape_task.cancel()
            try:
                await self._scrape_task
            except asyncio.CancelledError:
                pass

        logger.info("SubprocessMetricsBridge stopped")

    async def _scrape_loop(self) -> None:
        """Periodically scrape all subprocess metrics."""
        while self._running:
            try:
                await self._scrape_all_subprocesses()
            except Exception as e:
                logger.exception(f"Error in scrape loop: {e}")

            await asyncio.sleep(self.scrape_interval)

    async def _scrape_all_subprocesses(self) -> None:
        """Scrape metrics from all configured subprocesses."""
        for url in self.subprocess_urls:
            try:
                metrics = await self._scrape_subprocess(url)
                self._latest_metrics[url] = metrics
                logger.debug(f"Scraped {len(metrics)} metric samples from {url}")
            except Exception as e:
                logger.warning(f"Failed to scrape {url}: {e}")
                # Clear stale metrics for this subprocess
                self._latest_metrics.pop(url, None)

    async def _scrape_subprocess(self, url: str) -> list[SubprocessMetric]:
        """Scrape Prometheus /metrics from a single subprocess.

        Args:
            url: Base URL of subprocess (e.g., "http://localhost:8000")

        Returns:
            List of parsed metrics

        Raises:
            httpx.HTTPError: If scraping fails
        """
        metrics_url = f"{url}/metrics"

        async with httpx.AsyncClient() as client:
            response = await client.get(metrics_url, timeout=self.timeout)
            response.raise_for_status()
            metrics_text = response.text

        return self._parse_prometheus_metrics(metrics_text)

    def _parse_prometheus_metrics(self, metrics_text: str) -> list[SubprocessMetric]:
        """Parse Prometheus text format into structured metrics.

        Args:
            metrics_text: Prometheus exposition format text

        Returns:
            List of parsed metrics
        """
        parsed_metrics = []

        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                parsed_metrics.append(
                    SubprocessMetric(
                        name=sample.name,
                        value=sample.value,
                        labels=dict(sample.labels),
                        metric_type=family.type,
                    )
                )

        return parsed_metrics

    def _get_metrics_by_name(self, metric_name: str) -> list[SubprocessMetric]:
        """Get all scraped metrics matching a name pattern."""
        results = []
        for url, metric_list in self._latest_metrics.items():
            for metric in metric_list:
                if metric.name == metric_name or metric.name.startswith(metric_name):
                    # Add subprocess URL as attribute
                    enriched_metric = SubprocessMetric(
                        name=metric.name,
                        value=metric.value,
                        labels={**metric.labels, "subprocess_url": url},
                        metric_type=metric.metric_type,
                    )
                    results.append(enriched_metric)
        return results

    def _collect_kafka_lag(self, options: Any) -> list[Observation]:
        """Collect kafka_consumer_lag metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("kafka_consumer_lag"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations

    def _collect_kafka_offset(self, options: Any) -> list[Observation]:
        """Collect kafka_offset metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("kafka_offset"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations

    def _collect_kafka_messages(self, options: Any) -> list[Observation]:
        """Collect kafka_messages metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("kafka_messages"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations

    def _collect_actions_running(self, options: Any) -> list[Observation]:
        """Collect actions_running metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("actions_running"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations

    def _collect_actions_success(self, options: Any) -> list[Observation]:
        """Collect actions_success_total metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("actions_success_total"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations

    def _collect_actions_error(self, options: Any) -> list[Observation]:
        """Collect actions_error_total metrics from subprocesses."""
        observations = []

        for metric in self._get_metrics_by_name("actions_error_total"):
            observations.append(
                Observation(
                    value=metric.value,
                    attributes={
                        **metric.labels,
                        "service.name": "datahub-actions",
                    },
                )
            )

        return observations


async def create_subprocess_bridge_if_enabled(
    subprocess_urls: list[str],
) -> Optional[SubprocessMetricsBridge]:
    """Create and start subprocess metrics bridge if OTLP is enabled.

    Args:
        subprocess_urls: List of subprocess /metrics URLs

    Returns:
        Bridge instance if OTLP enabled, None otherwise
    """
    import os

    otlp_enabled = os.environ.get("OTEL_EXPORTER_OTLP_ENABLED", "false").lower()
    if otlp_enabled not in ("true", "1", "yes"):
        logger.info("OTLP exporter disabled, subprocess bridge not created")
        return None

    if not subprocess_urls:
        logger.warning("No subprocess URLs provided, bridge not created")
        return None

    scrape_interval = float(os.environ.get("SUBPROCESS_METRICS_SCRAPE_INTERVAL", "30"))

    bridge = SubprocessMetricsBridge(
        subprocess_urls=subprocess_urls,
        scrape_interval_seconds=scrape_interval,
    )

    await bridge.start()
    logger.info(
        f"Subprocess metrics bridge started for {len(subprocess_urls)} subprocess(es)"
    )

    return bridge
