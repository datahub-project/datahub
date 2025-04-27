"""
This module contains smoke tests for the DataHub Integrations Service's internal telemetry API.

The tests verify that:
1. The integrations service can properly send telemetry events to the DataHub tracking API
2. The events are correctly forwarded to Kafka and can be consumed
3. The authentication and graph client configuration works correctly

The test flow:
1. Creates a test telemetry event (ChatbotInteractionEvent)
2. Sends the event using the integrations service's telemetry API
3. Verifies the event is received in Kafka
4. Validates the event contents match what was sent

This is a critical smoke test as it verifies the end-to-end telemetry pipeline
for the integrations service, ensuring that user interactions are properly tracked
and can be analyzed.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer, KafkaError

# Import the graph module after telemetry to ensure proper initialization
# Import the telemetry module first to ensure integrations_graph is initialized
from datahub_integrations.chat.telemetry import track_saas_event
from datahub_integrations.chat.telemetry_models import ChatbotInteractionEvent

from tests.utils import get_kafka_broker_url

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patch_integrations_graph(graph_client):
    """Monkey patch the integrations service graph client with our test graph client"""
    # Store the original graph client
    from datahub_integrations.chat.telemetry import graph as original_graph

    logger.info(f"Original graph client: {original_graph}")

    # Replace with our test graph client
    # Patch the graph variable directly in the telemetry module
    import datahub_integrations.chat.telemetry

    datahub_integrations.chat.telemetry.graph = graph_client
    logger.info(f"Patched graph client: {datahub_integrations.chat.telemetry.graph}")

    yield

    # Restore the original graph client
    datahub_integrations.chat.telemetry.graph = original_graph
    logger.info(f"Restored graph client: {datahub_integrations.chat.telemetry.graph}")


def _send_test_event(test_event, graph_client):
    """Helper function to send a test event and wait for it to complete."""
    # Log the graph client configuration
    logger.info(f"Graph client server URL: {graph_client.config.server}")
    logger.info(f"Graph client token: {graph_client.config.token}")

    # Send the event
    logger.info("Sending telemetry event...")
    track_saas_event(test_event)
    logger.info("Telemetry event sent")


def test_telemetry_api_via_tracking(graph_client, auth_session):
    logger.info("Starting telemetry API test")
    logger.info(f"Graph client server URL: {graph_client.config.server}")

    test_group_id = f"test-group-{str(uuid.uuid4())}"
    # Create a Kafka consumer
    logger.info("Creating Kafka consumer...")

    # Use the utility function to get the Kafka URL
    kafka_url = get_kafka_broker_url()
    logger.info(f"Using Kafka URL: {kafka_url}")

    # Configure the consumer
    consumer_config = {
        "bootstrap.servers": kafka_url,
        "group.id": test_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(["DataHubUsageEvent_v1"])

    # Generate a random message ID
    test_message_id = f"test-message-id-{str(uuid.uuid4())}"

    try:
        # Create a test event
        logger.info("Creating test event...")
        test_event = ChatbotInteractionEvent(
            chat_id="test-chat-id-2",
            message_id=test_message_id,
            slack_thread_id="test-thread-id-2",
            slack_message_id="test-message-id-2",
            slack_user_id="test-user-id-2",
            slack_user_name="Test User",
            slack_email="test@example.com",
            message_contents="Test message",
            response_contents="Test response",
            timestamp=datetime.now(timezone.utc),
        )
        logger.info(f"Test event created: {test_event}")

        # Send the event and wait for it to complete
        logger.info("Sending test event...")
        _send_test_event(test_event, graph_client)
        logger.info("Test event sent")

        # Read messages from Kafka with timeout
        logger.info("Reading messages from Kafka...")
        messages = []
        start_time = time.time()
        timeout = 10

        while time.time() - start_time < timeout:
            # Poll for messages with a short timeout
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.1)  # Small delay between polls
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break

            try:
                message = json.loads(msg.value().decode("utf-8"))
                logger.info(f"Added message: {message}")
                # Check for message_id in the flat structure
                if message.get("message_id") == test_message_id:
                    messages.append(message)
                    break
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

        # Verify we got messages within the timeout
        logger.info(f"Found {len(messages)} messages")
        assert len(messages) > 0, (
            f"No messages received from Kafka within {timeout} seconds"
        )

        # Verify the event was sent
        message = messages[0]  # Get the event data
        logger.info(f"Verifying event: {message}")
        assert message["chat_id"] == "test-chat-id-2"
        assert message["message_id"] == test_message_id
        assert message["slack_user_id"] == "test-user-id-2"
        assert message["slack_user_name"] == "Test User"
        assert message["slack_email"] == "test@example.com"
        assert message["message_contents"] == "Test message"
        assert message["response_contents"] == "Test response"
        assert "timestamp" in message
        logger.info("Event verification successful")

    finally:
        logger.info("Closing Kafka consumer")
        consumer.close()
