"""
Smoke tests for Semantic Search functionality.

These tests validate end-to-end semantic search:
1. Ingest sample documents via GraphQL
2. Generate embeddings and emit via MCP (Metadata Change Proposal)
3. Wait for indexing (20 seconds)
4. Execute semantic search via GraphQL
5. Verify our created documents appear in results

These tests are DISABLED by default. To enable, set:
    ENABLE_SEMANTIC_SEARCH_TESTS=true

Prerequisites:
- DataHub running with semantic search enabled
- ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
- SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
- AWS credentials configured for embedding generation (AWS_PROFILE or AWS_ACCESS_KEY_ID)

Usage:
    # Run with semantic search tests enabled
    AWS_PROFILE=your-profile ENABLE_SEMANTIC_SEARCH_TESTS=true pytest tests/semantic/test_semantic_search.py -v

For implementation details, see docs/dev-guides/semantic-search/.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime

import pytest
import requests

from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)

# Test gating - disabled by default
SEMANTIC_SEARCH_ENABLED = (
    os.environ.get("ENABLE_SEMANTIC_SEARCH_TESTS", "false").lower() == "true"
)

# Hardcoded wait time for indexing
INDEXING_WAIT_SECONDS = 20

# Sample documents for testing semantic search
SAMPLE_DOCUMENTS: list[dict[str, str]] = [
    {
        "title": "Getting Started with DataHub",
        "text": """Getting Started with DataHub

Introduction
DataHub is a modern data catalog designed to help you discover, understand, and trust your data.

Quick Start
1. Navigate to the DataHub UI
2. Use the search bar to find datasets
3. Explore metadata, lineage, and documentation

Key Features
- Search: Find data assets across your organization
- Lineage: Understand data dependencies and flow
- Documentation: Add and view rich documentation
- Governance: Manage access and policies
""",
        "sub_type": "guide",
    },
    {
        "title": "Data Access Request Process",
        "text": """Data Access Request Process

Overview
This document describes how to request access to data assets in our data platform.

Access Levels

Read Access
- View data in dashboards and reports
- Run queries on approved datasets

Write Access
- Modify existing records
- Requires additional approval

Request Steps
1. Identify the dataset you need access to
2. Submit a request through the data portal
3. Wait for owner approval (typically 24-48 hours)
4. Access will be granted automatically upon approval
""",
        "sub_type": "process",
    },
    {
        "title": "Machine Learning Model: Churn Prediction",
        "text": """Churn Prediction Model

Model Overview
This model predicts the probability of customer churn within the next 30 days.

Features
- Days since last login
- Purchase frequency
- Support ticket count

Performance Metrics
- AUC-ROC: 0.87
- Precision: 0.82
- F1 Score: 0.80
""",
        "sub_type": "model",
    },
    {
        "title": "ETL Pipeline Documentation: Sales Data",
        "text": """Sales Data ETL Pipeline

Pipeline Overview
This pipeline extracts sales data from our CRM, transforms it, and loads into the data warehouse.

Schedule
- Runs daily at 2:00 AM UTC
- Full refresh on Sundays

Data Sources
1. Salesforce CRM (primary)
2. Payment gateway logs
3. Product catalog database

Output Tables
- sales.transactions - Individual sale records
- sales.daily_summary - Aggregated daily metrics
""",
        "sub_type": "pipeline",
    },
    {
        "title": "Customer Data Platform Overview",
        "text": """Customer Data Platform (CDP)

What is a CDP?
A Customer Data Platform is a unified database that collects customer data from multiple sources.

Our CDP Architecture
- Website tracking
- Mobile app events
- CRM integrations

Key Metrics
- 50M+ unified customer profiles
- 200+ data sources integrated
""",
        "sub_type": "overview",
    },
]


def _unique_id(prefix: str) -> str:
    """Generate a unique ID for test documents."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def execute_graphql(auth_session, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against the frontend API."""
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    result = response.json()
    return result


def create_document(
    auth_session, doc_id: str, title: str, text: str, sub_type: str
) -> str:
    """Create a document via GraphQL and return the URN."""
    mutation = """
        mutation CreateDocument($input: CreateDocumentInput!) {
            createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": doc_id,
            "subType": sub_type,
            "title": title,
            "contents": {"text": text},
            "state": "PUBLISHED",
        }
    }
    result = execute_graphql(auth_session, mutation, variables)
    if "errors" in result:
        raise Exception(f"Failed to create document: {result['errors']}")
    return result["data"]["createDocument"]


def delete_document(auth_session, urn: str) -> bool:
    """Delete a document via GraphQL."""
    mutation = """
        mutation DeleteDocument($urn: String!) {
            deleteDocument(urn: $urn)
        }
    """
    result = execute_graphql(auth_session, mutation, {"urn": urn})
    return result.get("data", {}).get("deleteDocument", False)


def search_documents_keyword(auth_session, query: str, count: int = 10) -> dict:
    """Search documents using keyword search."""
    graphql_query = """
        query SearchDocuments($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Document {
                            info {
                                title
                            }
                        }
                    }
                }
            }
        }
    """
    variables = {
        "input": {
            "query": query,
            "types": ["DOCUMENT"],
            "start": 0,
            "count": count,
        }
    }
    return execute_graphql(auth_session, graphql_query, variables)


def search_documents_semantic(auth_session, query: str, count: int = 10) -> dict:
    """Search documents using semantic search."""
    graphql_query = """
        query SemanticSearchDocuments($input: SearchAcrossEntitiesInput!) {
            semanticSearchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Document {
                            info {
                                title
                            }
                        }
                    }
                }
            }
        }
    """
    variables = {
        "input": {
            "query": query,
            "types": ["DOCUMENT"],
            "start": 0,
            "count": count,
        }
    }
    return execute_graphql(auth_session, graphql_query, variables)


def chunk_text(
    text: str, max_chunk_tokens: int = 400, chars_per_token: int = 4
) -> list[str]:
    """Chunk text into segments of approximately max_chunk_tokens."""
    if not text or not text.strip():
        return []

    max_chars = max_chunk_tokens * chars_per_token
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current_chunk = ""
    sentences = text.replace(". ", ".|").replace(".\n", ".|").split("|")

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        if current_chunk and len(current_chunk) + len(sentence) + 1 > max_chars:
            chunks.append(current_chunk)
            current_chunk = sentence
        else:
            current_chunk = (
                f"{current_chunk} {sentence}".strip() if current_chunk else sentence
            )

        while len(current_chunk) > max_chars:
            chunks.append(current_chunk[:max_chars])
            current_chunk = current_chunk[max_chars:]

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def create_embeddings():
    """Create an embeddings instance using litellm with AWS Bedrock."""
    try:
        from litellm import embedding

        class LiteLLMEmbeddings:
            def __init__(self, model: str = "bedrock/cohere.embed-english-v3"):
                self.model = model
                self.model_name = model

            def embed_documents(self, texts: list[str]) -> list[list[float]]:
                """Generate embeddings for a list of texts."""
                response = embedding(
                    model=self.model,
                    input=texts,
                    input_type="search_document",
                )
                return [item["embedding"] for item in response.data]

            def embed_query(self, text: str) -> list[float]:
                """Generate embedding for a single query."""
                response = embedding(
                    model=self.model,
                    input=[text],
                    input_type="search_query",
                )
                return response.data[0]["embedding"]

        return LiteLLMEmbeddings()
    except ImportError:
        raise ImportError(
            "litellm is required for embedding generation. Install with: pip install litellm"
        )


def emit_semantic_content_mcp(
    auth_session,
    doc_id: str,
    text: str,
    embedding_field: str = "cohere_embed_v3",
) -> None:
    """
    Generate embeddings and emit them via MCP (Metadata Change Proposal).

    This tests the full MCP flow for semantic content ingestion.
    Raises exception on failure.
    """
    urn = f"urn:li:document:{doc_id}"

    embeddings = create_embeddings()

    # Chunk the text
    chunks = chunk_text(text, max_chunk_tokens=400)
    assert chunks, f"No text to embed for {urn}"

    logger.info(f"Generating embeddings for {doc_id}: {len(chunks)} chunks...")
    vectors = embeddings.embed_documents(chunks)

    # Build chunk data matching the PDL schema
    chunk_data = []
    current_offset = 0
    for i, (chunk_text_item, vector) in enumerate(zip(chunks, vectors, strict=False)):
        chunk_data.append(
            {
                "position": i,
                "vector": vector,
                "characterOffset": current_offset,
                "characterLength": len(chunk_text_item),
                "tokenCount": len(chunk_text_item.split()),
                "text": chunk_text_item,
            }
        )
        current_offset += len(chunk_text_item)

    # Build the SemanticContent aspect payload
    aspect_value = {
        "embeddings": {
            embedding_field: {
                "modelVersion": embeddings.model_name,
                "generatedAt": int(datetime.utcnow().timestamp() * 1000),
                "chunkingStrategy": "sentence_boundary_400t",
                "totalChunks": len(chunks),
                "totalTokens": sum(c["tokenCount"] for c in chunk_data),
                "chunks": chunk_data,
            }
        }
    }

    # Build the MCP payload
    mcp_payload = {
        "entityType": "document",
        "entityUrn": urn,
        "changeType": "UPSERT",
        "aspectName": "semanticContent",
        "aspect": {
            "value": json.dumps(aspect_value),
            "contentType": "application/json",
        },
    }

    # Emit MCP via the GMS ingest endpoint
    gms_url = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
    url = f"{gms_url}/aspects?action=ingestProposal"

    # Use a new session with the auth token
    session = requests.Session()
    token = auth_session.gms_token()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    response = session.post(
        url,
        json={"proposal": mcp_payload},
        timeout=60,
    )
    response.raise_for_status()

    logger.info(
        f"Emitted SemanticContent MCP for {doc_id}: {len(chunks)} chunks, {len(vectors[0])} dims"
    )


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestSemanticSearch:
    """
    Semantic search smoke tests.

    These tests ingest sample documents, wait for indexing, and verify
    that semantic search returns relevant results.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        """Set up test documents and clean up after tests."""
        self.created_urns: list[str] = []
        self.auth_session = auth_session

        yield

        # Cleanup: delete all created documents
        for urn in self.created_urns:
            try:
                delete_document(auth_session, urn)
                logger.info(f"Cleaned up document: {urn}")
            except Exception as e:
                logger.warning(f"Failed to delete {urn}: {e}")

    def test_ingest_and_semantic_search(self, auth_session):
        """
        End-to-end test: ingest documents with embeddings and verify semantic search works.

        Steps:
        1. Create sample documents via GraphQL
        2. Generate embeddings and emit via MCP
        3. Wait for indexing (20 seconds)
        4. Execute semantic search
        5. Verify our created documents appear in results
        """
        logger.info("Starting semantic search smoke test")

        # Track doc_ids for embedding generation
        created_docs: list[dict[str, str]] = []

        # Step 1: Ingest sample documents
        logger.info(f"Ingesting {len(SAMPLE_DOCUMENTS)} sample documents...")
        for doc in SAMPLE_DOCUMENTS:
            doc_id = _unique_id("semantic-test")
            try:
                urn = create_document(
                    auth_session,
                    doc_id=doc_id,
                    title=doc["title"],
                    text=doc["text"],
                    sub_type=doc["sub_type"],
                )
                self.created_urns.append(urn)
                created_docs.append(
                    {"id": doc_id, "text": doc["text"], "title": doc["title"]}
                )
                logger.info(f"Created document: {urn}")
            except Exception as e:
                pytest.fail(f"Failed to create document '{doc['title']}': {e}")

        assert len(self.created_urns) == len(SAMPLE_DOCUMENTS), (
            f"Expected to create {len(SAMPLE_DOCUMENTS)} documents, "
            f"but created {len(self.created_urns)}"
        )

        # Step 2: Generate embeddings and emit via MCP
        logger.info("Generating embeddings and emitting via MCP...")
        for doc in created_docs:
            emit_semantic_content_mcp(auth_session, doc["id"], doc["text"])

        logger.info(f"Emitted embeddings for {len(created_docs)} documents")

        # Step 3: Wait for indexing
        logger.info(f"Waiting {INDEXING_WAIT_SECONDS} seconds for indexing...")
        time.sleep(INDEXING_WAIT_SECONDS)

        # Also wait for writes to sync
        wait_for_writes_to_sync()

        # Step 4: Execute semantic search with a query that should match our documents
        test_query = "how to request data access permissions"
        logger.info(f"Executing semantic search: '{test_query}'")

        result = search_documents_semantic(auth_session, test_query)

        # Check for GraphQL errors
        if "errors" in result:
            pytest.fail(f"GraphQL errors: {result['errors']}")

        # Step 5: Verify results
        search_data = result.get("data", {}).get("semanticSearchAcrossEntities", {})
        total = search_data.get("total", 0)
        search_results = search_data.get("searchResults", [])

        logger.info(f"Semantic search returned {total} total results")

        # We should get at least some results
        assert total > 0, "Semantic search returned no results"
        assert len(search_results) > 0, "No search results in response"

        # Log the results for debugging
        for i, item in enumerate(search_results[:5], 1):
            entity = item.get("entity", {})
            info = entity.get("info") or {}
            title = info.get("title", "Unknown")
            urn = entity.get("urn", "no-urn")
            is_ours = "✓" if urn in self.created_urns else " "
            logger.info(f"  {i}. [{is_ours}] {title} ({urn})")

        # Verify at least one of our test documents appears in results
        result_urns = {item.get("entity", {}).get("urn") for item in search_results}

        matching_urns = result_urns.intersection(set(self.created_urns))
        logger.info(f"Found {len(matching_urns)} of our test documents in results")

        # Assert that at least one of our documents with embeddings appears in results
        assert len(matching_urns) > 0, (
            f"None of our test documents appeared in semantic search results. "
            f"Created URNs: {self.created_urns}, "
            f"Result URNs: {result_urns}"
        )

    def test_keyword_search_comparison(self, auth_session):
        """
        Compare keyword search to ensure basic search works.

        This test verifies that keyword search works, which is a prerequisite
        for semantic search.
        """
        logger.info("Testing keyword search as baseline")

        # Create a single document for this test
        doc_id = _unique_id("keyword-test")
        doc_title = "Unique Keyword Search Test Document"
        doc_text = "This is a unique test document for keyword search validation."

        urn = create_document(
            auth_session,
            doc_id=doc_id,
            title=doc_title,
            text=doc_text,
            sub_type="test",
        )
        self.created_urns.append(urn)

        # Wait for indexing
        logger.info(f"Waiting {INDEXING_WAIT_SECONDS} seconds for indexing...")
        time.sleep(INDEXING_WAIT_SECONDS)
        wait_for_writes_to_sync()

        # Search for the unique title
        result = search_documents_keyword(auth_session, "Unique Keyword Search Test")

        assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

        search_data = result.get("data", {}).get("searchAcrossEntities", {})
        total = search_data.get("total", 0)

        logger.info(f"Keyword search returned {total} results")
        assert total > 0, "Keyword search returned no results"
