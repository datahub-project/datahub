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

"""Unit tests for KafkaLagMonitor."""

import time
from unittest.mock import Mock, patch

from confluent_kafka import KafkaException, TopicPartition

from datahub_actions.observability.kafka_lag_monitor import (
    KAFKA_LAG_GAUGE,
    KafkaLagMonitor,
    LagStats,
)


class TestLagCalculation:
    """Test lag calculation logic."""

    def test_calculate_topic_lag_normal_case(self):
        """Test lag = high_water_mark - committed_offset."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=100),
            TopicPartition("test-topic", 1, offset=200),
        ]
        mock_consumer.get_watermark_offsets.side_effect = [
            (0, 150),  # Partition 0: low=0, high=150
            (0, 250),  # Partition 1: low=0, high=250
        ]

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [
            TopicPartition("test-topic", 0),
            TopicPartition("test-topic", 1),
        ]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is not None
        assert lag_stats.topic == "test-topic"
        assert lag_stats.total_lag == 100  # (150-100) + (250-200)
        assert lag_stats.partition_lags == {0: 50, 1: 50}

    def test_calculate_topic_lag_no_committed_offset(self):
        """Test lag when no offset has been committed yet (offset=-1)."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=-1),  # No commit yet
        ]
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [TopicPartition("test-topic", 0)]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is not None
        assert lag_stats.total_lag == 100  # high - low when offset=-1
        assert lag_stats.partition_lags == {0: 100}

    def test_calculate_topic_lag_multiple_partitions_aggregated(self):
        """Test lag aggregation across multiple partitions."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=50),
            TopicPartition("test-topic", 1, offset=100),
            TopicPartition("test-topic", 2, offset=75),
        ]
        mock_consumer.get_watermark_offsets.side_effect = [
            (0, 100),  # Partition 0: lag = 50
            (0, 200),  # Partition 1: lag = 100
            (0, 150),  # Partition 2: lag = 75
        ]

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [
            TopicPartition("test-topic", 0),
            TopicPartition("test-topic", 1),
            TopicPartition("test-topic", 2),
        ]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is not None
        assert lag_stats.total_lag == 225  # 50 + 100 + 75
        assert len(lag_stats.partition_lags) == 3

    def test_calculate_topic_lag_zero_lag(self):
        """Test case where consumer is caught up (lag = 0)."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=100),
        ]
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [TopicPartition("test-topic", 0)]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is not None
        assert lag_stats.total_lag == 0
        assert lag_stats.partition_lags == {0: 0}

    def test_calculate_topic_lag_negative_handled(self):
        """Test that negative lag is clamped to zero."""
        mock_consumer = Mock()
        # Simulate edge case where committed > high watermark
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=150),
        ]
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [TopicPartition("test-topic", 0)]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is not None
        assert lag_stats.total_lag == 0  # Negative clamped to 0
        assert lag_stats.partition_lags == {0: 0}


class TestErrorHandling:
    """Test error handling in lag calculation."""

    def test_kafka_exception_on_committed(self):
        """Test graceful handling when committed() raises KafkaException."""
        mock_consumer = Mock()
        mock_consumer.committed.side_effect = KafkaException(
            "Failed to fetch committed offsets"
        )

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [TopicPartition("test-topic", 0)]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        assert lag_stats is None  # Returns None on error

    def test_none_watermarks(self):
        """Test handling when get_watermark_offsets returns None."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=100),
            TopicPartition("test-topic", 1, offset=200),
        ]
        mock_consumer.get_watermark_offsets.side_effect = [
            None,  # Partition 0 fails
            (0, 250),  # Partition 1 succeeds
        ]

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [
            TopicPartition("test-topic", 0),
            TopicPartition("test-topic", 1),
        ]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        # Should still return stats for successful partition
        assert lag_stats is not None
        assert lag_stats.total_lag == 50  # Only partition 1
        assert lag_stats.partition_lags == {1: 50}

    def test_kafka_exception_on_watermarks(self):
        """Test handling when get_watermark_offsets raises KafkaException."""
        mock_consumer = Mock()
        mock_consumer.committed.return_value = [
            TopicPartition("test-topic", 0, offset=100),
        ]
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            "Timeout fetching watermarks"
        )

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        partitions = [TopicPartition("test-topic", 0)]
        lag_stats = monitor._calculate_topic_lag("test-topic", partitions)

        # Returns None when no partitions succeed
        assert lag_stats is None


class TestThreadLifecycle:
    """Test monitor thread lifecycle."""

    def test_start_creates_daemon_thread(self):
        """Test that start() creates a daemon background thread."""
        mock_consumer = Mock()
        mock_consumer.assignment.return_value = []

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline", interval_seconds=0.1)
        monitor.start()

        assert monitor._thread is not None
        assert monitor._thread.is_alive()
        assert monitor._thread.daemon  # Daemon thread

        monitor.stop()

    def test_stop_terminates_thread(self):
        """Test that stop() terminates the monitoring thread."""
        mock_consumer = Mock()
        mock_consumer.assignment.return_value = []

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline", interval_seconds=0.1)
        monitor.start()
        time.sleep(0.05)  # Let thread start

        monitor.stop()

        assert monitor._thread is None  # Thread reference cleared

    def test_start_idempotent(self):
        """Test that calling start() multiple times doesn't create multiple threads."""
        mock_consumer = Mock()
        mock_consumer.assignment.return_value = []

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline", interval_seconds=0.1)
        monitor.start()
        first_thread = monitor._thread

        monitor.start()  # Call again

        assert monitor._thread is first_thread  # Same thread

        monitor.stop()

    def test_stop_when_not_started(self):
        """Test that stop() is safe to call when monitor not started."""
        mock_consumer = Mock()
        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")

        # Should not raise
        monitor.stop()


class TestMetricsReporting:
    """Test Prometheus metrics reporting."""

    def test_report_lag_updates_prometheus_gauge(self):
        """Test that lag stats are reported to Prometheus Gauge."""
        mock_consumer = Mock()
        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")

        lag_stats = LagStats(
            topic="test-topic",
            total_lag=1250,
            partition_lags={0: 500, 1: 750},
        )

        with patch.object(KAFKA_LAG_GAUGE, "labels") as mock_labels:
            mock_gauge = Mock()
            mock_labels.return_value = mock_gauge

            monitor._report_lag(lag_stats)

            mock_labels.assert_called_once_with(
                topic="test-topic",
                pipeline_name="test-pipeline",
            )
            mock_gauge.set.assert_called_once_with(1250)


class TestConfiguration:
    """Test configuration and environment variable handling."""

    def test_default_interval_and_timeout(self):
        """Test default values for interval and timeout."""
        mock_consumer = Mock()
        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")

        assert monitor.interval_seconds == 30.0
        assert monitor.timeout_seconds == 5.0

    def test_custom_interval_and_timeout(self):
        """Test custom values for interval and timeout."""
        mock_consumer = Mock()
        monitor = KafkaLagMonitor(
            mock_consumer,
            "test-pipeline",
            interval_seconds=60.0,
            timeout_seconds=10.0,
        )

        assert monitor.interval_seconds == 60.0
        assert monitor.timeout_seconds == 10.0


class TestCollectionAndReporting:
    """Test end-to-end collection and reporting."""

    def test_collect_and_report_no_assignment(self):
        """Test handling when consumer has no partition assignment."""
        mock_consumer = Mock()
        mock_consumer.assignment.return_value = []

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")
        monitor._collect_and_report_lag()

        # Should not raise, just log debug message

    def test_collect_and_report_groups_by_topic(self):
        """Test that partitions are grouped by topic."""
        mock_consumer = Mock()
        mock_consumer.assignment.return_value = [
            TopicPartition("topic-a", 0),
            TopicPartition("topic-a", 1),
            TopicPartition("topic-b", 0),
        ]

        # Mock committed to return only the requested partitions
        def committed_side_effect(partitions, timeout=None):
            """Return committed offsets for requested partitions only."""
            result = []
            for tp in partitions:
                if tp.topic == "topic-a" and tp.partition == 0:
                    result.append(TopicPartition("topic-a", 0, offset=100))
                elif tp.topic == "topic-a" and tp.partition == 1:
                    result.append(TopicPartition("topic-a", 1, offset=200))
                elif tp.topic == "topic-b" and tp.partition == 0:
                    result.append(TopicPartition("topic-b", 0, offset=50))
            return result

        mock_consumer.committed.side_effect = committed_side_effect
        mock_consumer.get_watermark_offsets.side_effect = [
            (0, 150),  # topic-a partition 0
            (0, 250),  # topic-a partition 1
            (0, 100),  # topic-b partition 0
        ]

        monitor = KafkaLagMonitor(mock_consumer, "test-pipeline")

        with patch.object(monitor, "_report_lag") as mock_report:
            monitor._collect_and_report_lag()

            # Should be called once per topic (2 topics)
            assert mock_report.call_count == 2
