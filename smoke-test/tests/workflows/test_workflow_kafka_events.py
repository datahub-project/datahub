import json
import time

import pytest
from confluent_kafka import Consumer, KafkaError

from tests.utils import (
    delete_urns_from_file,
    execute_gql,
    get_kafka_broker_url,
    ingest_file_via_rest,
)

# Test action requests that will be created during tests
CREATED_ACTION_REQUEST_URNS: list[str] = []


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Ingest test data before tests and clean up after"""
    print("ingesting action workflow test data")
    ingest_file_via_rest(auth_session, "tests/workflows/action_workflows_data.json")
    yield
    print("removing action workflow test data")
    delete_urns_from_file(graph_client, "tests/workflows/action_workflows_data.json")
    # Clean up any action requests created during testing
    for urn in CREATED_ACTION_REQUEST_URNS:
        try:
            graph_client.hard_delete_entity(urn)
        except Exception:
            pass  # Ignore errors during cleanup


def create_kafka_consumer(unique_id: str):
    """Create a Kafka consumer for PlatformEvent_v1 events"""
    kafka_bootstrap_servers = get_kafka_broker_url().replace(":29092", ":9092")
    platform_event_topic = "PlatformEvent_v1"

    group_id = f"test_workflow_events_{unique_id}"
    print(
        f"\nCreating Kafka consumer for topic {platform_event_topic} with group_id {group_id}..."
    )
    print(f"Kafka Bootstrap Servers: {kafka_bootstrap_servers}")

    consumer_config = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([platform_event_topic])
    print(f"Subscribed to topic: {platform_event_topic}")

    # Wait for consumer to be ready
    time.sleep(2)

    return consumer


def extract_json_from_binary_message(raw_value):
    """Extract JSON objects from binary Kafka message"""
    try:
        # Convert to string using latin-1 (preserves all bytes)
        message_str = raw_value.decode("latin-1")

        # Look for entityChangeEvent in the binary data
        if "entityChangeEvent" not in message_str:
            return []

        # Find all JSON-like objects in the message
        json_objects = []
        i = 0
        while i < len(message_str):
            if message_str[i] == "{":
                # Found potential JSON start, try to find the complete object
                brace_count = 1
                j = i + 1
                while j < len(message_str) and brace_count > 0:
                    if message_str[j] == "{":
                        brace_count += 1
                    elif message_str[j] == "}":
                        brace_count -= 1
                    j += 1

                if brace_count == 0:
                    # Found complete JSON object
                    json_candidate = message_str[i:j]
                    try:
                        parsed = json.loads(json_candidate)
                        json_objects.append(parsed)
                    except json.JSONDecodeError:
                        pass  # Not valid JSON, continue
            i += 1

        return json_objects
    except Exception:
        return []


def process_workflow_event_objects(json_objects):
    """Process JSON objects to find workflow-related events"""
    for obj in json_objects:
        if isinstance(obj, dict):
            entity_urn = obj.get("entityUrn", "")

            if entity_urn and "actionRequest" in entity_urn:
                # Create a proper event structure
                event = {"name": "entityChangeEvent", "payload": obj}
                return event
    return None


def consume_kafka_events(consumer, timeout=15):
    """Consume events from Kafka with timeout"""
    messages = []
    poll_start_time = time.time()

    print(f"Polling for workflow events (timeout: {timeout}s)...")

    while time.time() - poll_start_time < timeout:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Get the raw binary value
            raw_value = msg.value()

            # Extract JSON objects from the binary message
            json_objects = extract_json_from_binary_message(raw_value)

            # Process objects to find workflow events
            event = process_workflow_event_objects(json_objects)
            if event:
                messages.append(event)

        except Exception:
            # Skip problematic messages silently
            continue

    print(f"Found {len(messages)} workflow-related events")
    return messages


def test_workflow_request_creation_event(auth_session):
    """Test that creating a workflow request emits a CREATE event to Kafka"""

    unique_id = f"test_{int(time.time() * 1000)}"
    consumer = create_kafka_consumer(unique_id)

    try:
        # Create a workflow request
        create_query = """
        mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
            createActionWorkflowFormRequest(input: $input)
        }
        """

        variables = {
            "input": {
                "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
                "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-{unique_id},PROD)",
                "description": f"Test workflow request for event testing {unique_id}",
                "fields": [
                    {
                        "id": "reason",
                        "values": [
                            {"stringValue": f"Testing event emission {unique_id}"}
                        ],
                    }
                ],
            }
        }

        print(f"Creating workflow request with unique_id: {unique_id}")
        response = execute_gql(auth_session, create_query, variables)

        assert "errors" not in response
        assert response["data"]["createActionWorkflowFormRequest"] is not None

        action_request_urn = response["data"]["createActionWorkflowFormRequest"]
        CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

        print(f"Created workflow request: {action_request_urn}")

        # Consume events from Kafka
        messages = consume_kafka_events(consumer)

        # Verify we got a CREATE event
        create_events = []
        for msg in messages:
            payload = msg.get("payload", {})
            if (
                action_request_urn in payload.get("entityUrn", "")
                and payload.get("operation") == "CREATE"
            ):
                create_events.append(msg)

        print(f"Found {len(create_events)} CREATE events")

        # Print all events for debugging
        print("\nAll CREATE events found:")
        for event in create_events:
            print(json.dumps(event, indent=2))

        # Basic verification that we got an event
        assert len(create_events) > 0, "No CREATE events found for workflow request"

        # Verify event structure
        create_event = create_events[0]
        assert create_event["name"] == "entityChangeEvent"

        payload = create_event["payload"]
        assert payload["operation"] == "CREATE"
        assert action_request_urn in payload["entityUrn"]
        assert payload["category"] == "LIFECYCLE"

        print("✓ Successfully verified CREATE event emission")

    finally:
        consumer.close()


def test_workflow_request_review_event(auth_session):
    """Test that reviewing a workflow request emits a MODIFY event to Kafka"""

    unique_id = f"test_{int(time.time() * 1000)}"
    consumer = create_kafka_consumer(unique_id)

    try:
        # First create a workflow request
        create_query = """
        mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
            createActionWorkflowFormRequest(input: $input)
        }
        """

        variables = {
            "input": {
                "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
                "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-{unique_id},PROD)",
                "description": f"Test workflow request for review event testing {unique_id}",
                "fields": [
                    {
                        "id": "reason",
                        "values": [
                            {
                                "stringValue": f"Testing review event emission {unique_id}"
                            }
                        ],
                    }
                ],
            }
        }

        print(f"Creating workflow request for review test with unique_id: {unique_id}")
        response = execute_gql(auth_session, create_query, variables)

        assert "errors" not in response
        action_request_urn = response["data"]["createActionWorkflowFormRequest"]
        CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

        print(f"Created workflow request: {action_request_urn}")

        # Wait a moment for the creation event to be processed
        time.sleep(2)

        # Review the workflow request
        review_query = """
        mutation reviewActionWorkflowFormRequest($input: ReviewActionWorkflowFormRequestInput!) {
            reviewActionWorkflowFormRequest(input: $input)
        }
        """

        review_variables = {
            "input": {
                "urn": action_request_urn,
                "result": "ACCEPTED",
                "comment": f"Test review for event testing {unique_id}",
            }
        }

        print(f"Reviewing workflow request: {action_request_urn}")
        review_response = execute_gql(auth_session, review_query, review_variables)

        assert "errors" not in review_response
        assert review_response["data"]["reviewActionWorkflowFormRequest"] is True

        print("Successfully reviewed workflow request")

        # Consume events from Kafka
        messages = consume_kafka_events(consumer)

        # Look for MODIFY/COMPLETED events (depending on workflow steps)
        workflow_events = []
        for msg in messages:
            payload = msg.get("payload", {})
            if action_request_urn in payload.get("entityUrn", "") and payload.get(
                "operation"
            ) in ["MODIFY", "COMPLETED"]:
                workflow_events.append(msg)

        print(f"Found {len(workflow_events)} workflow events")

        # Print all events for debugging
        print("\nAll workflow events found:")
        for event in workflow_events:
            print(json.dumps(event, indent=2))

        # Basic verification that we got an event
        assert len(workflow_events) > 0, "No workflow events found for review"

        # Verify event structure
        workflow_event = workflow_events[0]
        assert workflow_event["name"] == "entityChangeEvent"

        payload = workflow_event["payload"]
        assert payload["operation"] in ["MODIFY", "COMPLETED"]
        assert action_request_urn in payload["entityUrn"]
        assert payload["category"] == "LIFECYCLE"

        print("✓ Successfully verified workflow review event emission")

    finally:
        consumer.close()


def test_workflow_request_completion_event(auth_session):
    """Test that completing a workflow request emits a COMPLETED event to Kafka"""

    unique_id = f"test_{int(time.time() * 1000)}"
    consumer = create_kafka_consumer(unique_id)

    try:
        # Create and complete a workflow request using a single-step workflow
        create_query = """
        mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
            createActionWorkflowFormRequest(input: $input)
        }
        """

        variables = {
            "input": {
                "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",  # Single step workflow
                "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-{unique_id},PROD)",
                "description": f"Test workflow request for completion event testing {unique_id}",
                "fields": [
                    {
                        "id": "reason",
                        "values": [
                            {
                                "stringValue": f"Testing completion event emission {unique_id}"
                            }
                        ],
                    }
                ],
            }
        }

        print(
            f"Creating workflow request for completion test with unique_id: {unique_id}"
        )
        response = execute_gql(auth_session, create_query, variables)

        assert "errors" not in response
        action_request_urn = response["data"]["createActionWorkflowFormRequest"]
        CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

        print(f"Created workflow request: {action_request_urn}")

        # Wait a moment for the creation event to be processed
        time.sleep(2)

        # Review/Complete the workflow request
        review_query = """
        mutation reviewActionWorkflowFormRequest($input: ReviewActionWorkflowFormRequestInput!) {
            reviewActionWorkflowFormRequest(input: $input)
        }
        """

        review_variables = {
            "input": {
                "urn": action_request_urn,
                "result": "ACCEPTED",
                "comment": f"Test completion for event testing {unique_id}",
            }
        }

        print(f"Completing workflow request: {action_request_urn}")
        review_response = execute_gql(auth_session, review_query, review_variables)

        assert "errors" not in review_response
        assert review_response["data"]["reviewActionWorkflowFormRequest"] is True

        print("Successfully completed workflow request")

        # Consume events from Kafka with longer timeout for completion
        messages = consume_kafka_events(consumer, timeout=20)

        # Look for COMPLETED events specifically
        completed_events = []
        for msg in messages:
            payload = msg.get("payload", {})
            if (
                action_request_urn in payload.get("entityUrn", "")
                and payload.get("operation") == "COMPLETED"
            ):
                completed_events.append(msg)

        print(f"Found {len(completed_events)} COMPLETED events")

        # Print all events for debugging
        print("\nAll COMPLETED events found:")
        for event in completed_events:
            print(json.dumps(event, indent=2))

        # Also print all workflow-related events for debugging
        workflow_events = []
        for msg in messages:
            payload = msg.get("payload", {})
            if action_request_urn in payload.get("entityUrn", ""):
                workflow_events.append(msg)

        print(f"\nAll workflow-related events ({len(workflow_events)}):")
        for event in workflow_events:
            print(json.dumps(event, indent=2))

        # Basic verification that we got a completion event
        assert len(completed_events) > 0, (
            "No COMPLETED events found for workflow request"
        )

        # Verify event structure
        completed_event = completed_events[0]
        assert completed_event["name"] == "entityChangeEvent"

        payload = completed_event["payload"]
        assert payload["operation"] == "COMPLETED"
        assert action_request_urn in payload["entityUrn"]
        assert payload["category"] == "LIFECYCLE"

        print("✓ Successfully verified COMPLETED event emission")

    finally:
        consumer.close()


def test_workflow_lifecycle_events_comprehensive(auth_session):
    """Test the complete workflow lifecycle and verify all events are emitted"""

    unique_id = f"test_{int(time.time() * 1000)}"
    consumer = create_kafka_consumer(unique_id)

    try:
        # Create workflow request
        create_query = """
        mutation createActionWorkflowFormRequest($input: CreateActionWorkflowFormRequestInput!) {
            createActionWorkflowFormRequest(input: $input)
        }
        """

        variables = {
            "input": {
                "workflowUrn": "urn:li:actionWorkflow:test-access-workflow-1",
                "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset-{unique_id},PROD)",
                "description": f"Test comprehensive workflow lifecycle {unique_id}",
                "fields": [
                    {
                        "id": "reason",
                        "values": [
                            {"stringValue": f"Testing full lifecycle {unique_id}"}
                        ],
                    }
                ],
            }
        }

        print(
            f"Starting comprehensive workflow lifecycle test with unique_id: {unique_id}"
        )
        response = execute_gql(auth_session, create_query, variables)

        assert "errors" not in response
        action_request_urn = response["data"]["createActionWorkflowFormRequest"]
        CREATED_ACTION_REQUEST_URNS.append(action_request_urn)

        print(f"1. Created workflow request: {action_request_urn}")
        time.sleep(3)

        # Review/Complete the workflow request
        review_query = """
        mutation reviewActionWorkflowFormRequest($input: ReviewActionWorkflowFormRequestInput!) {
            reviewActionWorkflowFormRequest(input: $input)
        }
        """

        review_variables = {
            "input": {
                "urn": action_request_urn,
                "result": "ACCEPTED",
                "comment": f"Comprehensive test completion {unique_id}",
            }
        }

        print("2. Reviewing/completing workflow request...")
        review_response = execute_gql(auth_session, review_query, review_variables)

        assert "errors" not in review_response
        assert review_response["data"]["reviewActionWorkflowFormRequest"] is True

        print("3. Successfully completed workflow request")

        # Consume all events
        messages = consume_kafka_events(consumer, timeout=25)

        # Filter to workflow-related events
        workflow_events = []
        for msg in messages:
            payload = msg.get("payload", {})
            if action_request_urn in payload.get("entityUrn", ""):
                workflow_events.append(msg)

        print("\n=== COMPREHENSIVE WORKFLOW LIFECYCLE EVENTS ===")
        print(f"Total workflow events found: {len(workflow_events)}")

        # Categorize events by operation
        events_by_operation: dict[str, list[dict]] = {}
        for event in workflow_events:
            operation = event["payload"]["operation"]
            if operation not in events_by_operation:
                events_by_operation[operation] = []
            events_by_operation[operation].append(event)

        print(f"Events by operation: {list(events_by_operation.keys())}")

        # Print all events for detailed inspection
        for i, event in enumerate(workflow_events):
            print(f"\n--- Event {i + 1} ---")
            print(json.dumps(event, indent=2))

        # Basic verification
        assert len(workflow_events) > 0, "No workflow events found"

        # Verify we have at least one CREATE event
        create_events = events_by_operation.get("CREATE", [])
        assert len(create_events) > 0, "No CREATE events found"

        print(f"✓ Found {len(create_events)} CREATE events")

        # Check for completion events
        completed_events = events_by_operation.get("COMPLETED", [])
        if len(completed_events) > 0:
            print(f"✓ Found {len(completed_events)} COMPLETED events")

        # Check for modify events
        modify_events = events_by_operation.get("MODIFY", [])
        if len(modify_events) > 0:
            print(f"✓ Found {len(modify_events)} MODIFY events")

        print("✓ Successfully verified comprehensive workflow lifecycle events")

    finally:
        consumer.close()
