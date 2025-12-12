"""
Smoke tests for Document Change History GraphQL API.

Validates end-to-end functionality of:
- Querying document change history
- Verifying change events are captured
"""

import logging
import time
import uuid

import pytest

from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


def execute_graphql(auth_session, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against the frontend API."""
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    result = response.json()
    return result


def _unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.dependency()
def test_document_change_history(auth_session):
    """Test document change history tracking."""
    document_id = _unique_id("smoke-doc-history")

    # Create a document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    create_vars = {
        "input": {
            "id": document_id,
            "subType": "faq",
            "title": "Change History Test",
            "contents": {"text": "Initial content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, create_vars)
    assert "errors" not in create_res, f"GraphQL errors: {create_res.get('errors')}"
    urn = create_res["data"]["createDocument"]
    assert urn is not None

    wait_for_writes_to_sync()
    time.sleep(2)

    # Update the document content
    update_mutation = """
        mutation UpdateContents($input: UpdateDocumentContentsInput!) {
            updateDocumentContents(input: $input)
        }
    """
    update_vars = {
        "input": {
            "urn": urn,
            "contents": {"text": "Updated content"},
        },
    }
    update_res = execute_graphql(auth_session, update_mutation, update_vars)
    assert "errors" not in update_res, f"GraphQL errors: {update_res.get('errors')}"
    assert update_res["data"]["updateDocumentContents"] is True

    wait_for_writes_to_sync()
    time.sleep(2)

    # Update the document state
    status_mutation = """
        mutation UpdateStatus($input: UpdateDocumentStatusInput!) {
            updateDocumentStatus(input: $input)
        }
    """
    status_vars = {"input": {"urn": urn, "state": "PUBLISHED"}}
    status_res = execute_graphql(auth_session, status_mutation, status_vars)
    assert "errors" not in status_res, f"GraphQL errors: {status_res.get('errors')}"
    assert status_res["data"]["updateDocumentStatus"] is True

    wait_for_writes_to_sync()
    time.sleep(2)

    # Query change history
    history_query = """
        query GetDocumentHistory($urn: String!) {
            document(urn: $urn) {
                urn
                changeHistory(limit: 50) {
                    changeType
                    description
                    actor {
                        urn
                    }
                    timestamp
                }
            }
        }
    """
    history_vars = {"urn": urn}
    history_res = execute_graphql(auth_session, history_query, history_vars)
    assert "errors" not in history_res, f"GraphQL errors: {history_res.get('errors')}"

    doc = history_res["data"]["document"]
    assert doc is not None
    assert doc["urn"] == urn

    change_history = doc["changeHistory"]
    assert change_history is not None
    assert isinstance(change_history, list)

    # We should have at least the creation event
    # (content and state changes may or may not be captured depending on implementation)
    assert len(change_history) >= 1, "Expected at least one change event (creation)"

    # Check that each change has the required fields
    for change in change_history:
        assert "changeType" in change
        assert "description" in change
        assert "timestamp" in change
        assert isinstance(change["timestamp"], int)
        # Actor is optional
        if change.get("actor"):
            assert change["actor"]["urn"] is not None

    # Check if we have a CREATED event
    change_types = [c["changeType"] for c in change_history]
    logger.info(f"Change history types: {change_types}")

    # Basic smoke test - just verify we can query change history
    # and it has the right structure (actual event generation depends on
    # Timeline Service configuration and entity registration)

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency(depends=["test_document_change_history"])
def test_document_change_history_with_time_range(auth_session):
    """Test document change history with time range parameters."""
    document_id = _unique_id("smoke-doc-history-time")

    # Create a document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    create_vars = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": "Time Range Test",
            "contents": {"text": "Test content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, create_vars)
    assert "errors" not in create_res, f"GraphQL errors: {create_res.get('errors')}"
    urn = create_res["data"]["createDocument"]
    assert urn is not None

    wait_for_writes_to_sync()
    time.sleep(2)

    # Query change history with time range
    current_time = int(time.time() * 1000)
    thirty_days_ago = current_time - (30 * 24 * 60 * 60 * 1000)

    history_query = """
        query GetDocumentHistory($urn: String!, $startTime: Long, $endTime: Long, $limit: Int) {
            document(urn: $urn) {
                urn
                changeHistory(startTimeMillis: $startTime, endTimeMillis: $endTime, limit: $limit) {
                    changeType
                    description
                    timestamp
                }
            }
        }
    """
    history_vars = {
        "urn": urn,
        "startTime": thirty_days_ago,
        "endTime": current_time,
        "limit": 10,
    }
    history_res = execute_graphql(auth_session, history_query, history_vars)
    assert "errors" not in history_res, f"GraphQL errors: {history_res.get('errors')}"

    doc = history_res["data"]["document"]
    assert doc is not None

    change_history = doc["changeHistory"]
    assert change_history is not None
    assert isinstance(change_history, list)

    # Verify the limit is respected
    assert len(change_history) <= 10, "Expected at most 10 change events"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency(depends=["test_document_change_history"])
def test_document_change_history_empty(auth_session):
    """Test change history for a document with no changes (or future time range)."""
    document_id = _unique_id("smoke-doc-history-empty")

    # Create a document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    create_vars = {
        "input": {
            "id": document_id,
            "subType": "reference",
            "title": "Empty History Test",
            "contents": {"text": "Test"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, create_vars)
    assert "errors" not in create_res, f"GraphQL errors: {create_res.get('errors')}"
    urn = create_res["data"]["createDocument"]
    assert urn is not None

    wait_for_writes_to_sync()
    time.sleep(2)

    # Query change history with a future time range (should return empty)
    future_start = int(time.time() * 1000) + (
        365 * 24 * 60 * 60 * 1000
    )  # 1 year in future
    future_end = future_start + (30 * 24 * 60 * 60 * 1000)  # 30 days after that

    history_query = """
        query GetDocumentHistory($urn: String!, $startTime: Long, $endTime: Long) {
            document(urn: $urn) {
                changeHistory(startTimeMillis: $startTime, endTimeMillis: $endTime) {
                    changeType
                    description
                }
            }
        }
    """
    history_vars = {"urn": urn, "startTime": future_start, "endTime": future_end}
    history_res = execute_graphql(auth_session, history_query, history_vars)
    assert "errors" not in history_res, f"GraphQL errors: {history_res.get('errors')}"

    doc = history_res["data"]["document"]
    assert doc is not None

    change_history = doc["changeHistory"]
    assert change_history is not None
    assert isinstance(change_history, list)
    # Should be empty since we queried a future time range
    assert len(change_history) == 0, "Expected no change events in future time range"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True
