# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Background thread for monitoring Kafka consumer lag.

This module provides a KafkaLagMonitor class that periodically calculates
and reports Kafka consumer lag metrics to Prometheus.
"""

import logging
import threading
from dataclasses import dataclass
from typing import Dict, Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

# Prometheus metrics
KAFKA_LAG_GAUGE = Gauge(
    name="kafka_consumer_lag",
    documentation="Kafka consumer lag aggregated per topic",
    labelnames=["topic", "pipeline_name"],
)


@dataclass
class LagStats:
    """Statistics for a topic's consumer lag."""

    topic: str
    total_lag: int
    partition_lags: Dict[int, int]  # partition_id -> lag


class KafkaLagMonitor:
    """Background thread that periodically reports Kafka consumer lag.

    This monitor:
    1. Queries assigned partitions from the Kafka consumer
    2. Gets high water marks for each partition
    3. Gets committed offsets for each partition
    4. Calculates lag = high_water_mark - committed_offset
    5. Aggregates per-topic lag (sum across partitions)
    6. Updates Prometheus Gauge metrics
    7. Optionally updates OpenTelemetry metrics if available
    """

    def __init__(
        self,
        consumer: Consumer,
        pipeline_name: str,
        interval_seconds: float = 30.0,
        timeout_seconds: float = 5.0,
    ):
        """Initialize lag monitor.

        Args:
            consumer: confluent_kafka.Consumer instance to monitor
            pipeline_name: Name of the action pipeline (for metric labels)
            interval_seconds: How often to report lag (default: 30s)
            timeout_seconds: Timeout for Kafka API calls (default: 5s)
        """
        self.consumer = consumer
        self.pipeline_name = pipeline_name
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background monitoring thread."""
        if self._thread is not None:
            logger.warning("Lag monitor already started")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._monitor_loop,
            name=f"kafka-lag-monitor-{self.pipeline_name}",
            daemon=True,  # Daemon thread exits when main thread exits
        )
        self._thread.start()
        logger.info(
            f"Kafka lag monitor started for pipeline '{self.pipeline_name}' "
            f"(interval={self.interval_seconds}s)"
        )

    def stop(self) -> None:
        """Stop the background monitoring thread."""
        if self._thread is None:
            return

        logger.info(f"Stopping Kafka lag monitor for pipeline '{self.pipeline_name}'")
        self._stop_event.set()
        self._thread.join(timeout=10.0)
        self._thread = None

    def _monitor_loop(self) -> None:
        """Main monitoring loop that runs in background thread."""
        while not self._stop_event.is_set():
            try:
                self._collect_and_report_lag()
            except Exception as e:
                # Log error but don't crash - monitoring should be resilient
                logger.error(
                    f"Error collecting lag for pipeline '{self.pipeline_name}': {e}",
                    exc_info=True,
                )

            # Sleep with interrupt support
            self._stop_event.wait(timeout=self.interval_seconds)

    def _collect_and_report_lag(self) -> None:
        """Collect lag statistics and update metrics."""
        # Get assigned partitions
        assignment = self.consumer.assignment()
        if not assignment:
            logger.debug(f"No partitions assigned to pipeline '{self.pipeline_name}'")
            return

        # Group partitions by topic
        topic_partitions: Dict[str, list[TopicPartition]] = {}
        for tp in assignment:
            if tp.topic not in topic_partitions:
                topic_partitions[tp.topic] = []
            topic_partitions[tp.topic].append(tp)

        # Calculate lag per topic
        for topic, partitions in topic_partitions.items():
            lag_stats = self._calculate_topic_lag(topic, partitions)
            if lag_stats:
                self._report_lag(lag_stats)

    def _calculate_topic_lag(
        self, topic: str, partitions: list[TopicPartition]
    ) -> Optional[LagStats]:
        """Calculate lag for all partitions of a topic.

        Args:
            topic: Topic name
            partitions: List of TopicPartition objects for this topic

        Returns:
            LagStats with aggregated lag, or None if calculation failed
        """
        partition_lags: Dict[int, int] = {}

        # Get committed offsets for all partitions at once
        try:
            committed_partitions = self.consumer.committed(
                partitions, timeout=self.timeout_seconds
            )
        except KafkaException as e:
            logger.warning(f"Failed to get committed offsets for topic '{topic}': {e}")
            return None

        # Calculate lag for each partition
        for tp in committed_partitions:
            try:
                # Get high water mark
                watermarks = self.consumer.get_watermark_offsets(
                    tp, timeout=self.timeout_seconds, cached=False
                )
                if watermarks is None:
                    logger.warning(
                        f"Failed to get watermarks for {topic}[{tp.partition}]"
                    )
                    continue

                low, high = watermarks

                # Calculate lag
                if tp.offset < 0:
                    # No committed offset yet - show total available messages as lag
                    lag = high - low
                else:
                    # Normal case: lag = high water mark - committed offset
                    lag = high - tp.offset

                # Ensure non-negative lag
                lag = max(0, lag)
                partition_lags[tp.partition] = lag

            except KafkaException as e:
                logger.warning(
                    f"Error calculating lag for {topic}[{tp.partition}]: {e}"
                )
                continue

        if not partition_lags:
            return None

        total_lag = sum(partition_lags.values())
        return LagStats(
            topic=topic,
            total_lag=total_lag,
            partition_lags=partition_lags,
        )

    def _report_lag(self, lag_stats: LagStats) -> None:
        """Report lag statistics to metrics backends.

        Args:
            lag_stats: Lag statistics to report
        """
        # Always update Prometheus (base requirement)
        KAFKA_LAG_GAUGE.labels(
            topic=lag_stats.topic,
            pipeline_name=self.pipeline_name,
        ).set(lag_stats.total_lag)

        logger.debug(
            f"Pipeline '{self.pipeline_name}' topic '{lag_stats.topic}': "
            f"lag={lag_stats.total_lag} "
            f"(partitions: {lag_stats.partition_lags})"
        )
