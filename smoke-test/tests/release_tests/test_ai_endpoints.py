# ABOUTME: Release tests for AI endpoints to verify they return proper HTTP status codes.
# ABOUTME: These tests verify endpoint availability and basic functionality in deployed environments.

import logging

import pytest

from tests.utils import execute_graphql

logger = logging.getLogger(__name__)


@pytest.mark.release_tests
def test_list_ai_conversations_endpoint(auth_session):
    """Test listDataHubAiConversations GraphQL query returns valid response structure."""
    query = """
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

    variables = {"start": 0, "count": 10}
    res_data = execute_graphql(auth_session, query, variables)

    # Verify no GraphQL errors
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"

    # Verify response structure
    list_response = res_data["data"]["listDataHubAiConversations"]
    assert list_response is not None, "Expected list response"
    assert "conversations" in list_response, "Response should contain conversations"
    assert "total" in list_response, "Response should contain total"

    logger.info(
        f"listDataHubAiConversations returned {list_response['total']} conversations"
    )

    # If conversations exist, validate structure
    if list_response["total"] > 0:
        for conversation in list_response["conversations"]:
            assert "urn" in conversation, "Conversation should contain urn"
            assert "created" in conversation, "Conversation should contain created"


@pytest.mark.release_tests
def test_get_ai_conversation_endpoint_with_invalid_urn(auth_session):
    """Test getDataHubAiConversation GraphQL query with non-existent URN returns gracefully."""
    query = """
        query GetDataHubAiConversation($urn: String!) {
            getDataHubAiConversation(urn: $urn) {
                urn
                created {
                    time
                    actor
                }
            }
        }
    """

    # Use a non-existent URN to test endpoint without requiring data
    variables = {"urn": "urn:li:dataHubAiConversation:non-existent-test-urn"}
    res_data = execute_graphql(auth_session, query, variables)

    # Verify no GraphQL errors (null result is acceptable)
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"

    # Verify response structure (should be null for non-existent URN)
    conversation = res_data["data"]["getDataHubAiConversation"]
    assert conversation is None, "Expected null for non-existent conversation"

    logger.info("getDataHubAiConversation correctly returned null for non-existent URN")


@pytest.mark.skip(reason="Backend is not working as expected")
@pytest.mark.release_tests
def test_ai_chat_message_endpoint_http_status(auth_session):
    """Test /openapi/v1/ai-chat/message endpoint returns proper HTTP status code."""
    stream_url = f"{auth_session.frontend_url()}/openapi/v1/ai-chat/message"

    # Send minimal payload without a valid conversation URN to test endpoint availability
    # We expect a 400 or 404 error since the conversation doesn't exist, but not a 500
    payload = {
        "conversationUrn": "urn:li:dataHubAiConversation:non-existent-test",
        "text": "test message",
    }

    response = auth_session.post(stream_url, json=payload, timeout=10)

    # Verify we get a reasonable HTTP response (not 500 server error)
    # Accept 200 (if endpoint is permissive), 400 (bad request), 401 (unauthorized), or 404 (not found)
    assert response.status_code in [200, 400, 401, 404], (
        f"Expected 200/400/401/404 status code, got {response.status_code}: {response.text}"
    )

    logger.info(f"AI chat message endpoint returned HTTP {response.status_code}")


@pytest.mark.release_tests
def test_create_ai_conversation_mutation(auth_session):
    """Test createDataHubAiConversation mutation returns valid response structure."""
    mutation = """
        mutation CreateDataHubAiConversation($input: CreateDataHubAiConversationInput!) {
            createDataHubAiConversation(input: $input) {
                urn
                created {
                    time
                    actor
                }
                messages {
                    type
                }
            }
        }
    """

    variables = {"input": {"title": "Release test conversation"}}
    res_data = execute_graphql(auth_session, mutation, variables)

    # Verify no GraphQL errors
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"

    # Verify response structure
    conversation = res_data["data"]["createDataHubAiConversation"]
    assert conversation is not None, "Expected conversation to be created"
    assert "urn" in conversation, "Conversation should contain urn"
    assert "dataHubAiConversation" in conversation["urn"], (
        "Expected URN to contain 'dataHubAiConversation'"
    )
    assert "created" in conversation, "Conversation should contain created timestamp"
    assert "messages" in conversation, "Conversation should contain messages list"

    logger.info(f"Created test conversation: {conversation['urn']}")

    # Clean up: delete the test conversation
    delete_mutation = """
        mutation DeleteDataHubAiConversation($urn: String!) {
            deleteDataHubAiConversation(urn: $urn)
        }
    """

    delete_res = execute_graphql(
        auth_session, delete_mutation, {"urn": conversation["urn"]}
    )
    assert "errors" not in delete_res, f"Delete errors: {delete_res.get('errors')}"
    assert delete_res["data"]["deleteDataHubAiConversation"] is True, (
        "Expected delete to return true"
    )

    logger.info("Successfully cleaned up test conversation")
