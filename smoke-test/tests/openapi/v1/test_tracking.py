"""
Tests for the DataHub tracking API (v1) that verify events are properly tracked and delivered to different destinations.

This test suite validates that events sent to the /openapi/v1/tracking/track endpoint are:
1. Successfully processed by the server
2. Correctly delivered to configured tracking destinations:
   - Mixpanel: Verifies events are sent to Mixpanel's JQL API
   - Kafka: Verifies events are published to the DataHubUsageEvent_v1 topic
   - pgQueue: Verifies events are indexed after pgQueue publication (postgres profiles)
   - Elasticsearch: Verifies events are indexed and searchable

Each test creates a unique event with a custom identifier to ensure test isolation and uses appropriate
wait times and retries to account for asynchronous processing in the different systems.

Note: Some tests may be skipped if required configuration (e.g., Mixpanel API secret) is not available.
"""

import json
import logging
import time
from datetime import datetime, timezone

import pytest
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from tests.utilities import env_vars
from tests.utilities.domains import Domain
from tests.utilities.messaging_transport import is_pgqueue_transport
from tests.utilities.usage_events_sot import (
    assert_tracking_event_indexed,
    resolve_usage_events_implementation,
)
from tests.utils import get_kafka_broker_url

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.PLATFORM)

# Load environment variables from .env file if it exists
load_dotenv()

# Event name constant
EVENT_NAME = "PageViewEvent"


def test_tracking_api_mixpanel(auth_session, graph_client):
    """Test that we can post events to the tracking endpoint and verify they are sent to Mixpanel."""

    # Check if Mixpanel API secret is available in environment variables
    api_secret = env_vars.get_mixpanel_api_secret()
    if not api_secret:
        pytest.skip("MIXPANEL_API_SECRET environment variable not set, skipping test")

    # Check if Mixpanel is enabled in server configuration
    graph_client.test_connection()  # Hack: Needed to ensure that the server config is loaded
    if not graph_client.get_config().get("telemetry", {}).get("enabledMixpanel", False):
        pytest.skip("Mixpanel is disabled in server configuration, skipping test")

    # Test configuration
    base_url = auth_session.gms_url()  # Use the authenticated GMS URL

    # Create a test event with a unique identifier
    unique_id = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"\nLooking for test event with customField: {unique_id}")
    now = datetime.now(timezone.utc)
    test_event = {
        "type": EVENT_NAME,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)",
        "actorUrn": "urn:li:corpuser:test_user",
        "timestamp": int(now.timestamp() * 1000),  # Unix epoch milliseconds
        "customField": unique_id,  # Use unique ID to identify our test event
    }

    # Post the event to the tracking endpoint
    response = auth_session.post(
        f"{base_url}/openapi/v1/tracking/track", json=test_event
    )

    # Print the response for debugging
    logger.info(f"\nResponse status: {response.status_code}")
    logger.info(f"Response body: {response.text}")

    # Verify the response
    assert response.status_code == 200, f"Failed to post event: {response.text}"

    # Wait a moment for the event to be processed by Mixpanel
    logger.info("\nWaiting for event to be processed by Mixpanel...")
    time.sleep(10)

    # Query Mixpanel's JQL API to retrieve our test event
    # Note: This requires a service account with access to JQL
    project_id = env_vars.get_mixpanel_project_id()

    # log the unique_id
    logger.info(f"\nLooking for test event with customField: {unique_id}")

    # JQL query
    jql_query = f"""
    function main() {{
        return Events({{
            from_date: '2024-01-01',
            to_date: '2025-12-31',
            event: '{EVENT_NAME}',
            where: "properties.customField == '{unique_id}'"
        }});
    }}
    """
    jql_query = f"""
    function main() {{
        return Events({{
            from_date: '2024-01-01',
            to_date: '2025-12-31',
            event_selectors: [{{event: '{EVENT_NAME}'}}]
        }}).filter(function(event) {{
            return event.properties.customField == '{unique_id}';
        }});
    }}
    """

    # URL with project ID
    url = f"https://mixpanel.com/api/query/jql?project_id={project_id}"

    # Headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Payload
    payload = {
        "script": jql_query,
    }

    # attempt to query mixpanel 3 times before giving up

    for i in range(20):
        # Send request with Basic Auth
        response = requests.post(
            url, data=payload, headers=headers, auth=(api_secret, "")
        )

        # Print response
        logger.info(f"\nMixpanel JQL response status: {response.status_code}")
        logger.info(response.text)

        if response.status_code == 200:
            events = response.json()
            logger.info(f"\nFound {len(events)} matching events in Mixpanel")

            if len(events) > 0:
                # Verify the event properties
                event = events[0]
                properties = event.get("properties", {})

                assert properties.get("type") == EVENT_NAME, (
                    "Mixpanel event type doesn't match"
                )
                assert properties.get("entityType") == "dataset", (
                    "Mixpanel entity type doesn't match"
                )
                assert (
                    properties.get("entityUrn")
                    == "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)"
                ), "Mixpanel entity URN doesn't match"
                assert properties.get("actorUrn") == "urn:li:corpuser:test_user", (
                    "Mixpanel actor URN doesn't match"
                )
                assert properties.get("customField") == unique_id, (
                    "Mixpanel custom field doesn't match"
                )
                logger.info("\nSuccessfully verified event in Mixpanel")
                return
            else:
                logger.info(
                    "\nNo matching events found in Mixpanel... waiting 1 second and trying again"
                )
        else:
            logger.info(f"\nFailed to query Mixpanel: {response.text}")
            if i == 2:
                raise Exception(f"\nFailed to query Mixpanel: {response.text}")

        time.sleep(1)
        # Don't fail the test if Mixpanel query fails, as this might be due to API limitations
        # in the test environment
    raise Exception("Failed to verify event in Mixpanel")


def _build_tracking_test_event(unique_id: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "type": EVENT_NAME,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)",
        "actorUrn": "urn:li:corpuser:test_user",
        "timestamp": int(now.timestamp() * 1000),
        "customField": unique_id,
    }


def _assert_tracking_event_indexed(unique_id: str, auth_session=None) -> None:
    assert_tracking_event_indexed(
        unique_id, event_type=EVENT_NAME, auth_session=auth_session
    )


def test_tracking_api_kafka(auth_session):
    """Test that we can post events to the tracking endpoint and verify they are sent to Kafka (DUE)."""
    if is_pgqueue_transport(auth_session):
        pytest.skip("Kafka is not the active messaging transport (pgQueue is enabled)")

    # Test configuration
    base_url = auth_session.gms_url()  # Use the authenticated GMS URL
    kafka_bootstrap_servers = get_kafka_broker_url().replace(
        ":29092", ":9092"
    )  # Use port 9092
    due_topic = "DataHubUsageEvent_v1"  # The DUE topic we'll read from

    unique_id = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"\nLooking for test event with customField: {unique_id}")
    test_event = _build_tracking_test_event(unique_id)

    # Create a Kafka consumer for DUE events BEFORE sending the event
    group_id = f"test_tracking_consumer_{unique_id}"
    logger.info(
        f"\nCreating Kafka consumer for topic {due_topic} with group_id {group_id}..."
    )

    # Configure the consumer
    consumer_config = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    due_consumer = Consumer(consumer_config)
    due_consumer.subscribe([due_topic])
    logger.info(f"Subscribed to topic: {due_topic}")

    # Wait a moment to ensure the consumer is ready
    logger.info("Waiting for consumer to be ready...")
    time.sleep(2)

    # Get the current timestamp before sending the event
    start_time = int(time.time() * 1000)  # Convert to milliseconds
    logger.info(f"Starting timestamp: {start_time}")

    # Post the event to the tracking endpoint
    response = auth_session.post(
        f"{base_url}/openapi/v1/tracking/track", json=test_event
    )

    # Print the response for debugging
    logger.info(f"\nResponse status: {response.status_code}")
    logger.info(f"Response body: {response.text}")

    # Verify the response
    assert response.status_code == 200, f"Failed to post event: {response.text}"

    logger.info(f"\nWaiting for messages on topic {due_topic}...")

    # Read messages from Kafka
    messages = []
    poll_start_time = time.time()
    timeout = 10  # seconds

    while time.time() - poll_start_time < timeout:
        msg = due_consumer.poll(1.0)  # Poll with 1 second timeout
        if msg is None:
            time.sleep(0.1)  # Small delay between polls
            continue

        error = msg.error()
        if error:
            if error.code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                continue
            else:
                logger.info(f"Consumer error: {error}")
                break

        msg_value = msg.value()
        if msg_value is None:
            continue

        try:
            message = json.loads(msg_value.decode("utf-8"))
            logger.info(f"Found message: {message}")
            messages.append(message)

            # Check if this is our test event
            if (
                message.get("type") == EVENT_NAME
                and message.get("customField") == unique_id
            ):
                logger.info(f"Found our test event: {message}")
                break
        except Exception as e:
            logger.info(f"Error processing message: {e}")
            continue

    # Close the consumer
    due_consumer.close()

    # Verify we found our test event in DUE
    assert len(messages) > 0, "No messages found in Kafka"

    # Find our test event by matching the event type and custom field
    found_event = False
    for message in messages:
        if isinstance(message, dict):
            logger.info(f"Found message with customField: {message.get('customField')}")
            if (
                message.get("type") == EVENT_NAME
                and message.get("customField") == unique_id
            ):
                found_event = True
                # Verify other fields match
                assert message.get("entityType") == "dataset", (
                    "Entity type doesn't match"
                )
                assert (
                    message.get("entityUrn")
                    == "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)"
                ), "Entity URN doesn't match"
                assert message.get("actorUrn") == "urn:li:corpuser:test_user", (
                    "Actor URN doesn't match"
                )
                break

    assert found_event, "Test event not found in Kafka messages"


def test_tracking_api_pgqueue(auth_session):
    """Verify tracking events published via pgQueue are indexed in the usage SoT."""
    if not is_pgqueue_transport(auth_session):
        pytest.skip("pgQueue is not the active messaging transport")

    unique_id = f"test_pgqueue_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    logger.info("Posting tracking event for pgQueue verification: %s", unique_id)
    test_event = _build_tracking_test_event(unique_id)

    response = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/tracking/track", json=test_event
    )
    assert response.status_code == 200, f"Failed to post event: {response.text}"

    sot = resolve_usage_events_implementation(auth_session)
    logger.info("Waiting for pgQueue publication and %s indexing...", sot)
    time.sleep(10)

    _assert_tracking_event_indexed(unique_id, auth_session)


def test_tracking_api_usage_sot(auth_session):
    """Verify tracking events land in the configured usage SoT (ES or Postgres)."""

    base_url = auth_session.gms_url()

    unique_id = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"\nLooking for test event with customField: {unique_id}")
    now = datetime.now(timezone.utc)

    test_event = {
        "title": "Acryl DataHub",
        "url": "http://localhost:9002/",
        "path": "/",
        "hash": "",
        "search": "",
        "width": 2827,
        "height": 1272,
        "prevPathname": "",
        "type": "PageViewEvent",
        "actorUrn": "urn:li:corpuser:test_user",
        "timestamp": int(now.timestamp() * 1000),
        "date": now.strftime("%a %b %d %Y %H:%M:%S GMT%z"),
        "userAgent": "Mozilla/5.0 (Test Browser)",
        "browserId": unique_id,
        "origin": "http://localhost:9002",
        "isThemeV2Enabled": True,
        "userPersona": "urn:li:dataHubPersona:businessUser",
        "corp_user_name": "Test User",
        "corp_user_username": "test_user",
        "customField": unique_id,
    }

    response = auth_session.post(
        f"{base_url}/openapi/v1/tracking/track", json=test_event
    )

    logger.info(f"\nResponse status: {response.status_code}")
    logger.info(f"Response body: {response.text}")

    assert response.status_code == 200, f"Failed to post event: {response.text}"

    sot = resolve_usage_events_implementation(auth_session)
    logger.info("\nWaiting for event to be indexed in usage SoT (%s)...", sot)
    time.sleep(10)

    _assert_tracking_event_indexed(unique_id, auth_session)
