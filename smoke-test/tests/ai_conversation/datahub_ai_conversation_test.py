"""
Smoke tests for DataHub AI Conversation APIs.

These tests validate the end-to-end functionality of:
- createDataHubAiConversation mutation
- getDataHubAiConversation query
- deleteDataHubAiConversation mutation
- listDataHubAiConversations query
- SSE streaming endpoint for chat
"""

import json

import pytest
import requests

from tests.consistency_utils import wait_for_writes_to_sync


def execute_graphql(auth_session, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against the frontend API."""
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    result = response.json()
    return result


@pytest.mark.dependency()
def test_create_agent_conversation(auth_session):
    """
    Test creating a new agent conversation.
    1. Create a conversation with a title
    2. Verify the response contains a URN
    3. Verify the conversation has the expected fields
    """
    # Create conversation mutation
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
                created {
                    time
                    actor
                }
                messages {
                    type
                    time
                    actor {
                        type
                        actor
                    }
                    content {
                        text
                    }
                }
            }
        }
    """

    variables = {"input": {"title": "Smoke Test Conversation"}}

    result = execute_graphql(auth_session, create_mutation, variables)

    # Verify no errors
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

    # Verify conversation was created
    conversation = result["data"]["createDataHubAiConversation"]
    assert conversation is not None, "Expected conversation to be created"
    assert conversation["urn"] is not None, "Expected conversation URN"
    assert "dataHubAiConversation" in conversation["urn"], (
        "Expected URN to contain 'dataHubAiConversation'"
    )

    # Verify created timestamp
    assert conversation["created"] is not None, "Expected created timestamp"
    assert conversation["created"]["time"] > 0, "Expected valid timestamp"
    assert conversation["created"]["actor"] is not None, "Expected actor"

    # Verify messages (should be empty for new conversation)
    assert conversation["messages"] is not None, "Expected messages list"
    assert len(conversation["messages"]) == 0, (
        "Expected empty messages for new conversation"
    )


@pytest.mark.dependency(depends=["test_create_agent_conversation"])
def test_get_agent_conversation(auth_session):
    """
    Test retrieving an agent conversation.
    1. Create a conversation
    2. Query it by URN
    3. Verify all fields are returned correctly
    """
    # First create a conversation
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
            }
        }
    """

    create_result = execute_graphql(
        auth_session, create_mutation, {"input": {"title": "Get Test Conversation"}}
    )
    assert "errors" not in create_result, (
        f"Create errors: {create_result.get('errors')}"
    )
    conversation_urn = create_result["data"]["createDataHubAiConversation"]["urn"]

    wait_for_writes_to_sync()

    # Now query the conversation
    get_query = """
        query GetDataHubAiConversation($urn: String!) {
            getDataHubAiConversation(urn: $urn) {
                urn
                created {
                    time
                    actor
                }
                lastUpdated {
                    time
                    actor
                }
                messages {
                    type
                    time
                    actor {
                        type
                        actor
                    }
                    content {
                        text
                    }
                }
            }
        }
    """

    get_result = execute_graphql(auth_session, get_query, {"urn": conversation_urn})

    # Verify no errors
    assert "errors" not in get_result, f"GraphQL errors: {get_result.get('errors')}"

    # Verify conversation was retrieved
    conversation = get_result["data"]["getDataHubAiConversation"]
    assert conversation is not None, "Expected conversation to be retrieved"
    assert conversation["urn"] == conversation_urn, "Expected matching URN"
    assert conversation["created"] is not None, "Expected created timestamp"
    assert conversation["lastUpdated"] is not None, "Expected lastUpdated timestamp"
    assert len(conversation["messages"]) == 0, "Expected no messages"

    # Clean up
    delete_mutation = """
        mutation DeleteDataHubAiConversation($urn: String!) {
            deleteDataHubAiConversation(urn: $urn)
        }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": conversation_urn})


@pytest.mark.dependency(depends=["test_create_agent_conversation"])
def test_list_agent_conversations(auth_session):
    """
    Test listing agent conversations.
    1. Create a conversation
    2. List them with high limit to handle old conversations
    3. Verify it appears in the list
    4. Clean up
    """
    import time

    # Create a single conversation
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
            }
        }
    """

    result = execute_graphql(
        auth_session, create_mutation, {"input": {"title": "List Test Conversation"}}
    )
    assert "errors" not in result, f"Create errors: {result.get('errors')}"
    created_urn = result["data"]["createDataHubAiConversation"]["urn"]

    # Wait for writes to sync through Kafka
    wait_for_writes_to_sync()

    # Additional sleep to ensure Elasticsearch indexing completes
    time.sleep(5)

    # List conversations with high count to handle any old conversations
    list_query = """
        query ListDataHubAiConversations($start: Int, $count: Int) {
            listDataHubAiConversations(start: $start, count: $count) {
                conversations {
                    urn
                    created {
                        time
                        actor
                    }
                }
                total
            }
        }
    """

    list_result = execute_graphql(auth_session, list_query, {"count": 100, "start": 0})

    # Verify no errors
    assert "errors" not in list_result, f"GraphQL errors: {list_result.get('errors')}"

    # Verify conversations are in the list
    list_response = list_result["data"]["listDataHubAiConversations"]
    assert list_response is not None, "Expected list response"
    assert list_response["conversations"] is not None, "Expected conversations list"
    assert list_response["total"] >= 1, "Expected at least 1 conversation"

    # Verify our created conversation is in the list
    returned_urns = [conv["urn"] for conv in list_response["conversations"]]
    assert created_urn in returned_urns, (
        f"Expected conversation {created_urn} to be in list. "
        f"Found {len(returned_urns)} conversations total."
    )

    # Verify conversation structure
    for conversation in list_response["conversations"]:
        assert conversation["urn"] is not None, "Expected URN"
        assert conversation["created"] is not None, "Expected created timestamp"

    # Clean up
    delete_mutation = """
        mutation DeleteDataHubAiConversation($urn: String!) {
            deleteDataHubAiConversation(urn: $urn)
        }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": created_urn})


@pytest.mark.dependency(depends=["test_create_agent_conversation"])
def test_delete_agent_conversation(auth_session):
    """
    Test deleting an agent conversation.
    1. Create a conversation
    2. Delete it
    3. Verify it returns true
    4. Verify it no longer appears in getDataHubAiConversation
    """
    # Create conversation
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
            }
        }
    """

    create_result = execute_graphql(
        auth_session, create_mutation, {"input": {"title": "Delete Test Conversation"}}
    )
    assert "errors" not in create_result, (
        f"Create errors: {create_result.get('errors')}"
    )
    conversation_urn = create_result["data"]["createDataHubAiConversation"]["urn"]

    wait_for_writes_to_sync()

    # Delete conversation
    delete_mutation = """
        mutation DeleteDataHubAiConversation($urn: String!) {
            deleteDataHubAiConversation(urn: $urn)
        }
    """

    delete_result = execute_graphql(
        auth_session, delete_mutation, {"urn": conversation_urn}
    )

    # Verify no errors
    assert "errors" not in delete_result, (
        f"GraphQL errors: {delete_result.get('errors')}"
    )
    assert delete_result["data"]["deleteDataHubAiConversation"] is True, (
        "Expected deleteDataHubAiConversation to return true"
    )

    wait_for_writes_to_sync()

    # Verify conversation is deleted (should return null or throw error)
    get_query = """
        query GetDataHubAiConversation($urn: String!) {
            getDataHubAiConversation(urn: $urn) {
                urn
            }
        }
    """

    get_result = execute_graphql(auth_session, get_query, {"urn": conversation_urn})

    # Either the result is null or we get an error
    if "errors" not in get_result:
        assert get_result["data"]["getDataHubAiConversation"] is None, (
            "Expected conversation to be deleted"
        )


@pytest.mark.dependency(depends=["test_create_agent_conversation"])
def test_sse_chat_stream_endpoint(auth_session):
    """
    Test the SSE streaming chat endpoint.
    1. Create a conversation
    2. Send a message via the SSE endpoint
    3. Verify we receive some SSE events
    4. Clean up
    """
    # Create conversation first
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
            }
        }
    """

    create_result = execute_graphql(
        auth_session, create_mutation, {"input": {"title": "SSE Test Conversation"}}
    )
    assert "errors" not in create_result, (
        f"Create errors: {create_result.get('errors')}"
    )
    conversation_urn = create_result["data"]["createDataHubAiConversation"]["urn"]

    wait_for_writes_to_sync()

    # Prepare SSE request
    stream_url = f"{auth_session.frontend_url()}/openapi/v1/ai-chat/message"
    payload = {
        "conversationUrn": conversation_urn,
        "text": "Hello, can you help me understand our datasets?",
    }

    # Send request and stream response
    # Note: Use auth_session.post() to get automatic authentication headers
    try:
        response = auth_session.post(
            stream_url,
            json=payload,
            stream=True,
            timeout=30,
        )

        # Verify response started successfully
        assert response.status_code == 200, (
            f"Expected 200 status code, got {response.status_code}: {response.text}"
        )

        # Verify we're getting SSE content
        assert "text/event-stream" in response.headers.get("Content-Type", ""), (
            f"Expected SSE content type, got {response.headers.get('Content-Type')}"
        )

        # Read some events to verify streaming works
        events_received = 0
        message_events = 0
        complete_event_received = False

        # Read the stream line by line
        for line in response.iter_lines(decode_unicode=True):
            if line:
                # SSE format: "event: <type>" or "data: <json>"
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                    if event_type == "complete":
                        complete_event_received = True
                        break
                    elif event_type == "message":
                        message_events += 1
                    events_received += 1
                elif line.startswith("data:"):
                    # Parse the data to ensure it's valid JSON
                    data_str = line.split(":", 1)[1].strip()
                    if data_str:
                        try:
                            json.loads(data_str)
                        except json.JSONDecodeError:
                            pass  # Some data may not be JSON

            # Don't wait forever - we just want to verify the endpoint works
            if events_received > 5 or message_events > 0:
                break

        # Verify we received some events
        assert events_received > 0, "Expected to receive at least one SSE event"

        print(
            f"✅ SSE endpoint working: received {events_received} events, "
            f"{message_events} message events, "
            f"complete={complete_event_received}"
        )

    except requests.exceptions.Timeout:
        pytest.fail("SSE endpoint timed out")
    except requests.exceptions.RequestException as e:
        pytest.fail(f"SSE endpoint request failed: {e}")

    finally:
        # Clean up
        delete_mutation = """
            mutation DeleteDataHubAiConversation($urn: String!) {
                deleteDataHubAiConversation(urn: $urn)
            }
        """
        try:
            execute_graphql(auth_session, delete_mutation, {"urn": conversation_urn})
        except Exception as e:
            print(f"Warning: Failed to clean up conversation {conversation_urn}: {e}")


@pytest.mark.dependency(
    depends=[
        "test_create_agent_conversation",
        "test_get_agent_conversation",
        "test_list_agent_conversations",
        "test_delete_agent_conversation",
        "test_sse_chat_stream_endpoint",
    ]
)
def test_agent_conversation_workflow(auth_session):
    """
    Test complete agent conversation workflow.
    1. Create conversation
    2. Verify it exists in list
    3. Get conversation details
    4. Delete conversation
    5. Verify it's removed from list
    """
    # Step 1: Create conversation
    create_mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
            }
        }
    """

    create_result = execute_graphql(
        auth_session,
        create_mutation,
        {"input": {"title": "Workflow Test Conversation"}},
    )
    assert "errors" not in create_result
    conversation_urn = create_result["data"]["createDataHubAiConversation"]["urn"]

    wait_for_writes_to_sync()

    # Additional sleep to ensure Elasticsearch indexing completes
    import time

    time.sleep(5)

    # Step 2: Verify it exists in list
    list_query = """
        query ListDataHubAiConversations($start: Int, $count: Int) {
            listDataHubAiConversations(start: $start, count: $count) {
                conversations {
                    urn
                }
            }
        }
    """

    list_result = execute_graphql(auth_session, list_query, {"count": 200, "start": 0})
    assert "errors" not in list_result
    urns = [
        c["urn"]
        for c in list_result["data"]["listDataHubAiConversations"]["conversations"]
    ]
    assert conversation_urn in urns, "Expected conversation to be in list"

    # Step 3: Get conversation details
    get_query = """
        query GetDataHubAiConversation($urn: String!) {
            getDataHubAiConversation(urn: $urn) {
                urn
            }
        }
    """

    get_result = execute_graphql(auth_session, get_query, {"urn": conversation_urn})
    assert "errors" not in get_result
    assert get_result["data"]["getDataHubAiConversation"]["urn"] == conversation_urn

    # Step 4: Delete conversation
    delete_mutation = """
        mutation DeleteDataHubAiConversation($urn: String!) {
            deleteDataHubAiConversation(urn: $urn)
        }
    """

    delete_result = execute_graphql(
        auth_session, delete_mutation, {"urn": conversation_urn}
    )
    assert "errors" not in delete_result
    assert delete_result["data"]["deleteDataHubAiConversation"] is True

    wait_for_writes_to_sync()

    # Step 5: Verify it's removed (get should return null or error)
    get_result_after_delete = execute_graphql(
        auth_session, get_query, {"urn": conversation_urn}
    )
    if "errors" not in get_result_after_delete:
        assert get_result_after_delete["data"]["getDataHubAiConversation"] is None
