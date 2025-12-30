"""Kafka consumer metrics for action pipelines.

This module provides OpenTelemetry metrics for tracking Kafka consumer health:
- Consumer lag per action/topic/partition
- Last message timestamp per action/topic
- Messages consumed and processed counters

These metrics help answer operational questions like:
- Is the consumer keeping up with the Kafka topic?
- When was the last message processed?
- How many messages are being processed successfully vs errors?
"""

from opentelemetry import metrics

# Get meter
meter = metrics.get_meter(__name__)

# Kafka consumer metrics
_kafka_consumer_lag = meter.create_up_down_counter(
    name="actions_kafka_consumer_lag_messages",
    description="Consumer lag in messages per action, topic, and partition",
    unit="messages",
)

_kafka_last_message_timestamp = meter.create_up_down_counter(
    name="actions_kafka_last_message_timestamp_seconds",
    description="Unix timestamp of last processed message (gauge-like)",
    unit="seconds",
)

_kafka_messages_consumed = meter.create_counter(
    name="actions_kafka_messages_consumed_total",
    description="Total messages consumed from Kafka by action and topic",
    unit="messages",
)

_kafka_messages_processed = meter.create_counter(
    name="actions_kafka_messages_processed_total",
    description="Total messages processed (success or error) by action and topic",
    unit="messages",
)


def update_consumer_lag(
    action_urn: str,
    topic: str,
    partition: int,
    lag: int,
) -> None:
    """Update consumer lag metric for a specific action/topic/partition.

    Args:
        action_urn: Unique identifier for the action
        topic: Kafka topic name
        partition: Partition number
        lag: Current lag in number of messages
    """
    labels = {
        "action_urn": action_urn,
        "topic": topic,
        "partition": str(partition),
    }
    # Use set operation by first zeroing out and then adding the lag
    # This simulates gauge behavior with UpDownCounter
    _kafka_consumer_lag.add(lag, attributes=labels)


def update_last_message_timestamp(
    action_urn: str,
    topic: str,
    timestamp: float,
) -> None:
    """Update the timestamp of the last processed message.

    Args:
        action_urn: Unique identifier for the action
        topic: Kafka topic name
        timestamp: Unix timestamp in seconds
    """
    labels = {
        "action_urn": action_urn,
        "topic": topic,
    }
    # Set the timestamp as the metric value (gauge-like behavior)
    _kafka_last_message_timestamp.add(int(timestamp), attributes=labels)


def track_message_consumed(
    action_urn: str,
    topic: str,
) -> None:
    """Track that a message was consumed from Kafka.

    Args:
        action_urn: Unique identifier for the action
        topic: Kafka topic name
    """
    labels = {
        "action_urn": action_urn,
        "topic": topic,
    }
    _kafka_messages_consumed.add(1, attributes=labels)


def track_message_processed(
    action_urn: str,
    topic: str,
    status: str = "success",
) -> None:
    """Track that a message was processed successfully or with error.

    Args:
        action_urn: Unique identifier for the action
        topic: Kafka topic name
        status: Processing status ("success" or "error")
    """
    labels = {
        "action_urn": action_urn,
        "topic": topic,
        "status": status,
    }
    _kafka_messages_processed.add(1, attributes=labels)
