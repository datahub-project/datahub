"""Tests for DataHubKafkaReader, particularly confluent-kafka >= 2.13.0 compatibility."""

from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.datahub_kafka_reader import DataHubKafkaReader
from datahub.ingestion.source.datahub.report import DataHubSourceReport


class MockMessage:
    """Mock Kafka message with configurable Optional return values (confluent-kafka >= 2.13.0)."""

    def __init__(
        self,
        value_val: Optional[Any] = None,
        partition_val: Optional[int] = None,
        offset_val: Optional[int] = None,
    ):
        self._value = value_val
        self._partition = partition_val
        self._offset = offset_val

    def value(self) -> Optional[Any]:
        return self._value

    def partition(self) -> Optional[int]:
        return self._partition

    def offset(self) -> Optional[int]:
        return self._offset


@pytest.fixture
def mock_config():
    return MagicMock(spec=DataHubSourceConfig)


@pytest.fixture
def mock_report():
    return DataHubSourceReport()


@pytest.fixture
def mock_ctx():
    return MagicMock(spec=PipelineContext, pipeline_name="test-pipeline")


@pytest.fixture
def mock_connection_config():
    connection_config = MagicMock()
    connection_config.bootstrap = "localhost:9092"
    connection_config.consumer_config = {}
    connection_config.schema_registry_url = "http://localhost:8081"
    return connection_config


class TestDataHubKafkaReaderNoneHandling:
    """Test cases for handling Optional types from confluent-kafka >= 2.13.0."""

    def test_message_with_none_value_increments_counter(
        self, mock_config, mock_report, mock_ctx, mock_connection_config
    ):
        """Test that messages with None value() are counted in the report."""
        mock_config.kafka_topic_name = "test-topic"
        mock_config.exclude_aspects = []

        reader = DataHubKafkaReader(
            config=mock_config,
            connection_config=mock_connection_config,
            report=mock_report,
            ctx=mock_ctx,
        )

        # Create a mock consumer
        mock_consumer = MagicMock()
        reader.consumer = mock_consumer

        # Return message with None value, then None to signal end
        msg_with_none_value = MockMessage(value_val=None, partition_val=0, offset_val=1)
        mock_consumer.poll.side_effect = [msg_with_none_value, None]

        # Execute
        stop_time = datetime.now(tz=timezone.utc)
        results = list(reader._poll_partition(stop_time))

        # Assert
        assert len(results) == 0
        assert mock_report.num_kafka_messages_with_none_value == 1

    @patch(
        "datahub.ingestion.source.datahub.datahub_kafka_reader.MetadataChangeLogClass"
    )
    def test_message_with_none_partition_increments_counter(
        self, mock_mcl_class, mock_config, mock_report, mock_ctx, mock_connection_config
    ):
        """Test that messages with None partition() are counted in the report."""
        mock_config.kafka_topic_name = "test-topic"
        mock_config.exclude_aspects = []

        reader = DataHubKafkaReader(
            config=mock_config,
            connection_config=mock_connection_config,
            report=mock_report,
            ctx=mock_ctx,
        )

        # Create a mock consumer
        mock_consumer = MagicMock()
        reader.consumer = mock_consumer

        # Mock the MCL parsing to return a valid MCL object
        mock_mcl = MagicMock()
        mock_mcl.created = None  # No timestamp means it won't trigger stop
        mock_mcl.aspectName = "datasetProperties"
        mock_mcl_class.from_obj.return_value = mock_mcl

        # Return message with valid value but None partition, then None to signal end
        msg_with_none_partition = MockMessage(
            value_val={"some": "data"}, partition_val=None, offset_val=1
        )
        mock_consumer.poll.side_effect = [msg_with_none_partition, None]

        # Execute
        stop_time = datetime.now(tz=timezone.utc)
        results = list(reader._poll_partition(stop_time))

        # Assert
        assert len(results) == 0
        assert mock_report.num_kafka_messages_with_none_metadata == 1

    @patch(
        "datahub.ingestion.source.datahub.datahub_kafka_reader.MetadataChangeLogClass"
    )
    def test_message_with_none_offset_increments_counter(
        self, mock_mcl_class, mock_config, mock_report, mock_ctx, mock_connection_config
    ):
        """Test that messages with None offset() are counted in the report."""
        mock_config.kafka_topic_name = "test-topic"
        mock_config.exclude_aspects = []

        reader = DataHubKafkaReader(
            config=mock_config,
            connection_config=mock_connection_config,
            report=mock_report,
            ctx=mock_ctx,
        )

        # Create a mock consumer
        mock_consumer = MagicMock()
        reader.consumer = mock_consumer

        # Mock the MCL parsing to return a valid MCL object
        mock_mcl = MagicMock()
        mock_mcl.created = None  # No timestamp means it won't trigger stop
        mock_mcl.aspectName = "datasetProperties"
        mock_mcl_class.from_obj.return_value = mock_mcl

        # Return message with valid value but None offset, then None to signal end
        msg_with_none_offset = MockMessage(
            value_val={"some": "data"}, partition_val=0, offset_val=None
        )
        mock_consumer.poll.side_effect = [msg_with_none_offset, None]

        # Execute
        stop_time = datetime.now(tz=timezone.utc)
        results = list(reader._poll_partition(stop_time))

        # Assert
        assert len(results) == 0
        assert mock_report.num_kafka_messages_with_none_metadata == 1

    @patch(
        "datahub.ingestion.source.datahub.datahub_kafka_reader.MetadataChangeLogClass"
    )
    def test_valid_message_processed_correctly(
        self, mock_mcl_class, mock_config, mock_report, mock_ctx, mock_connection_config
    ):
        """Test that valid messages are processed and counters remain zero."""
        mock_config.kafka_topic_name = "test-topic"
        mock_config.exclude_aspects = []

        reader = DataHubKafkaReader(
            config=mock_config,
            connection_config=mock_connection_config,
            report=mock_report,
            ctx=mock_ctx,
        )

        # Create a mock consumer
        mock_consumer = MagicMock()
        reader.consumer = mock_consumer

        # Mock the MCL parsing to return a valid MCL object
        mock_mcl = MagicMock()
        mock_mcl.created = None  # No timestamp means it won't trigger stop
        mock_mcl.aspectName = "datasetProperties"
        mock_mcl_class.from_obj.return_value = mock_mcl

        # Return valid message with all attributes set, then None to signal end
        valid_msg = MockMessage(
            value_val={"some": "data"}, partition_val=0, offset_val=100
        )
        mock_consumer.poll.side_effect = [valid_msg, None]

        # Execute
        stop_time = datetime.now(tz=timezone.utc)
        results = list(reader._poll_partition(stop_time))

        # Assert - message should be processed
        assert len(results) == 1
        assert mock_report.num_kafka_messages_with_none_value == 0
        assert mock_report.num_kafka_messages_with_none_metadata == 0

    def test_multiple_none_messages_accumulate_counters(
        self, mock_config, mock_report, mock_ctx, mock_connection_config
    ):
        """Test that multiple messages with None values accumulate counters correctly."""
        mock_config.kafka_topic_name = "test-topic"
        mock_config.exclude_aspects = []

        reader = DataHubKafkaReader(
            config=mock_config,
            connection_config=mock_connection_config,
            report=mock_report,
            ctx=mock_ctx,
        )

        # Create a mock consumer
        mock_consumer = MagicMock()
        reader.consumer = mock_consumer

        # Multiple messages with None values
        msg1 = MockMessage(value_val=None, partition_val=0, offset_val=1)
        msg2 = MockMessage(value_val=None, partition_val=0, offset_val=2)
        msg3 = MockMessage(value_val=None, partition_val=0, offset_val=3)
        mock_consumer.poll.side_effect = [msg1, msg2, msg3, None]

        # Execute
        stop_time = datetime.now(tz=timezone.utc)
        results = list(reader._poll_partition(stop_time))

        # Assert
        assert len(results) == 0
        assert mock_report.num_kafka_messages_with_none_value == 3
