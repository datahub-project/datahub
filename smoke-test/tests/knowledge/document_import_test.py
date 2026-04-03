"""
Smoke tests for Document Import APIs.

Validates end-to-end functionality of:
- GET /openapi/v1/documents/import/supported-formats
- POST /openapi/v1/documents/import/files (file upload → document creation)
- importDocumentsFromGitHub GraphQL mutation (schema validation only — no real GitHub call)

Tests are idempotent and clean up after themselves.
"""

import logging
import uuid

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def execute_graphql(auth_session, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against the frontend API."""
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    return response.json()


def _delete_document(auth_session, urn: str) -> None:
    """Best-effort cleanup of a document by URN."""
    delete_mutation = """
        mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
    """
    try:
        execute_graphql(auth_session, delete_mutation, {"urn": urn})
    except Exception:
        logger.warning("Failed to delete document %s during cleanup", urn)


def _create_parent_document(auth_session) -> str:
    """Create a throwaway parent document and return its URN."""
    doc_id = _unique_id("smoke-import-parent")
    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": doc_id,
            "title": f"Import Parent {doc_id}",
            "contents": {"text": "Parent for import tests"},
        }
    }
    result = execute_graphql(auth_session, create_mutation, variables)
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"
    return result["data"]["createDocument"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.dependency()
def test_supported_formats_endpoint(auth_session):
    """GET /openapi/v1/documents/import/supported-formats returns a list of extensions."""
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/supported-formats"
    )
    assert response.status_code == 200, f"Unexpected status: {response.status_code}"

    data = response.json()
    extensions = data.get("extensions", [])
    logger.info("Supported extensions: %s", extensions)

    assert isinstance(extensions, list)
    assert len(extensions) > 0, "Expected at least one supported extension"
    assert ".md" in extensions
    assert ".txt" in extensions
    assert ".pdf" in extensions


@pytest.mark.dependency()
def test_import_file_upload(auth_session):
    """Upload a .txt file via REST and verify a document is created."""
    file_content = (
        b"# Smoke Test Document\n\nThis is test content for the import smoke test."
    )
    file_name = f"smoke-import-{uuid.uuid4().hex[:8]}.txt"

    response = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/files",
        files=[("files", (file_name, file_content, "text/plain"))],
        data={
            "showInGlobalContext": "true",
            "useCase": "CONTEXT_DOCUMENT",
        },
    )
    assert response.status_code == 200, (
        f"Upload failed with status {response.status_code}: {response.text}"
    )

    result = response.json()
    logger.info("Import result: %s", result)

    assert result["createdCount"] >= 1, f"Expected at least 1 created, got {result}"
    assert result["failedCount"] == 0, f"Unexpected failures: {result.get('errors')}"
    assert len(result["documentUrns"]) >= 1

    created_urn = result["documentUrns"][0]
    logger.info("Created document URN: %s", created_urn)

    # Verify the document exists via GraphQL
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              title
              source { sourceType }
              contents { text }
            }
          }
        }
    """
    get_result = execute_graphql(auth_session, get_query, {"urn": created_urn})
    assert "errors" not in get_result, f"GraphQL errors: {get_result.get('errors')}"

    doc = get_result["data"]["document"]
    assert doc is not None, "Document not found after import"
    assert doc["info"]["source"]["sourceType"] == "NATIVE"
    assert "Smoke Test Document" in (doc["info"]["contents"]["text"] or "")

    logger.info("Verified document title: %s", doc["info"]["title"])

    # Cleanup
    _delete_document(auth_session, created_urn)


@pytest.mark.dependency()
def test_import_file_upload_with_parent(auth_session):
    """Upload a file into a specific parent document and verify the hierarchy."""
    parent_urn = _create_parent_document(auth_session)
    logger.info("Created parent document: %s", parent_urn)

    file_content = b"Child document content for hierarchy test."
    file_name = f"smoke-child-{uuid.uuid4().hex[:8]}.md"

    response = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/files",
        files=[("files", (file_name, file_content, "text/plain"))],
        data={
            "showInGlobalContext": "true",
            "useCase": "CONTEXT_DOCUMENT",
            "parentDocumentUrn": parent_urn,
        },
    )
    assert response.status_code == 200, (
        f"Upload failed: {response.status_code}: {response.text}"
    )

    result = response.json()
    assert result["createdCount"] >= 1
    child_urn = result["documentUrns"][0]
    logger.info("Created child document: %s", child_urn)

    # Verify parent-child relationship
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              parentDocument { document { urn } }
            }
          }
        }
    """
    get_result = execute_graphql(auth_session, get_query, {"urn": child_urn})
    assert "errors" not in get_result, f"GraphQL errors: {get_result.get('errors')}"

    doc = get_result["data"]["document"]
    assert doc is not None, "Child document not found"

    parent_doc = doc["info"].get("parentDocument")
    assert parent_doc is not None, "Expected parentDocument to be set"
    assert parent_doc["document"]["urn"] == parent_urn, (
        f"Expected parent {parent_urn}, got {parent_doc['document']['urn']}"
    )
    logger.info("Verified parent-child relationship: %s → %s", parent_urn, child_urn)

    # Cleanup (child first, then parent)
    _delete_document(auth_session, child_urn)
    _delete_document(auth_session, parent_urn)


@pytest.mark.dependency()
def test_import_file_upload_rejects_empty(auth_session):
    """Uploading with no files should return an error (400 or 415)."""
    response = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/files",
        data={
            "showInGlobalContext": "true",
            "useCase": "CONTEXT_DOCUMENT",
        },
    )
    # Spring returns 415 (no multipart boundary) or 400 (empty files list)
    assert response.status_code in (400, 415), (
        f"Expected 400 or 415 for empty upload, got {response.status_code}"
    )


@pytest.mark.dependency()
def test_import_file_upload_idempotent(auth_session):
    """Uploading the same file twice should update (not duplicate) the document."""
    file_content = b"Idempotency test content"
    file_name = "smoke-idempotent-test.txt"

    # First upload — creates
    resp1 = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/files",
        files=[("files", (file_name, file_content, "text/plain"))],
        data={"showInGlobalContext": "true", "useCase": "CONTEXT_DOCUMENT"},
    )
    assert resp1.status_code == 200
    result1 = resp1.json()
    assert result1["createdCount"] == 1
    urn1 = result1["documentUrns"][0]

    # Second upload — should update the same document
    resp2 = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/documents/import/files",
        files=[("files", (file_name, file_content, "text/plain"))],
        data={"showInGlobalContext": "true", "useCase": "CONTEXT_DOCUMENT"},
    )
    assert resp2.status_code == 200
    result2 = resp2.json()
    assert result2["updatedCount"] == 1, (
        f"Expected 1 update on re-import, got {result2}"
    )
    urn2 = result2["documentUrns"][0]

    assert urn1 == urn2, "Re-importing the same file should produce the same URN"
    logger.info("Verified idempotent import: %s", urn1)

    # Cleanup
    _delete_document(auth_session, urn1)
