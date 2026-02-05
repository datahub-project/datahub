"""
Smoke tests for Semantic Search functionality.

These tests validate end-to-end semantic search using the complete ingestion pipeline:
1. Create documents using the Document SDK
2. Run the datahub-documents ingestion source with chunking and embedding
3. Verify semanticContent aspect was created with chunks and embeddings
4. Execute semantic search via GraphQL
5. Verify documents appear in semantic search results

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
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import run_datahub_cmd

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


def create_documents_with_sdk(
    auth_session, docs: list[dict[str, str]]
) -> list[tuple[str, str]]:
    """
    Create documents using the Document SDK.
    Returns list of (doc_id, urn) tuples.
    """
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.sdk.document import Document

    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )

    created_docs: list[tuple[str, str]] = []

    for doc_data in docs:
        doc_id = _unique_id("semantic-sdk-test")
        doc = Document.create_document(
            id=doc_id,
            title=doc_data["title"],
            text=doc_data["text"],
            subtype=doc_data["sub_type"],
        )

        # Emit the document
        for mcp in doc.as_mcps():
            emitter.emit(mcp)

        urn = f"urn:li:document:{doc_id}"
        created_docs.append((doc_id, urn))
        logger.info(f"Created document via SDK: {urn}")

    emitter.close()
    wait_for_writes_to_sync(mcp_only=True)

    return created_docs


def create_ingestion_recipe(auth_session, doc_ids: list[str], tmp_dir: Path) -> Path:
    """
    Create a recipe file for the datahub-documents ingestion source.
    Returns path to the recipe file.
    """
    recipe = {
        "source": {
            "type": "datahub-documents",
            "config": {
                "datahub": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
                "document_urns": [f"urn:li:document:{doc_id}" for doc_id in doc_ids],
                "chunking": {
                    "strategy": "by_title",
                    "max_characters": 500,
                },
                # No embedding config - should auto-load from server via AppConfig API
                # Disable incremental mode for testing to force reprocessing
                "incremental": {
                    "enabled": False,
                },
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
            },
        },
    }

    recipe_path = tmp_dir / "datahub_documents_recipe.yml"
    with open(recipe_path, "w") as f:
        import yaml

        yaml.dump(recipe, f)

    logger.info(f"Created recipe file: {recipe_path}")
    return recipe_path


def run_ingestion(auth_session, recipe_path: Path) -> None:
    """Run the datahub-documents ingestion source."""
    logger.info(f"Running ingestion with recipe: {recipe_path}")

    try:
        # Load recipe file
        import yaml

        with open(recipe_path) as f:
            recipe_config = yaml.safe_load(f)

        # Create and run pipeline
        from datahub.ingestion.run.pipeline import Pipeline

        pipeline = Pipeline.create(recipe_config)
        pipeline.run()
        pipeline.raise_from_status()

        logger.info("Ingestion completed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def verify_semantic_content(auth_session, urn: str) -> dict[str, Any]:
    """
    Verify that the semanticContent aspect exists with chunks and embeddings.
    Returns the semantic content aspect data.
    """
    logger.info(f"Verifying semanticContent for {urn}")

    result = run_datahub_cmd(
        ["get", "--urn", urn, "-a", "semanticContent"],
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    if result.exit_code != 0:
        raise Exception(f"Failed to get semanticContent aspect: {result.stderr}")

    aspect_data = json.loads(result.stdout)

    if not aspect_data:
        raise Exception(f"No semanticContent aspect found for {urn}")

    semantic_content = aspect_data.get("semanticContent", {})
    embeddings = semantic_content.get("embeddings", {})

    if not embeddings:
        raise Exception(f"No embeddings found in semanticContent for {urn}")

    # Check for the expected embedding field
    cohere_embed = embeddings.get("cohere_embed_v3")
    if not cohere_embed:
        raise Exception(
            f"No cohere_embed_v3 embeddings found. Available fields: {list(embeddings.keys())}"
        )

    chunks = cohere_embed.get("chunks", [])
    total_chunks = cohere_embed.get("totalChunks", 0)

    logger.info(
        f"  ✓ Found semanticContent with {len(chunks)} chunks (totalChunks: {total_chunks})"
    )

    if len(chunks) == 0:
        raise Exception(f"No chunks found in embeddings for {urn}")

    # Verify first chunk structure
    first_chunk = chunks[0]
    required_fields = ["position", "vector", "text", "characterOffset"]
    for field in required_fields:
        if field not in first_chunk:
            raise Exception(f"Chunk missing required field: {field}")

    vector = first_chunk["vector"]
    if not isinstance(vector, list) or len(vector) == 0:
        raise Exception(f"Invalid vector in chunk: {vector}")

    logger.info(
        f"  ✓ Chunks have valid structure with {len(vector)}-dimensional embeddings"
    )

    return semantic_content


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


def delete_document(auth_session, urn: str) -> bool:
    """Delete a document via GraphQL."""
    mutation = """
        mutation DeleteDocument($urn: String!) {
            deleteDocument(urn: $urn)
        }
    """
    result = execute_graphql(auth_session, mutation, {"urn": urn})
    return result.get("data", {}).get("deleteDocument", False)


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestSemanticSearchWithIngestion:
    """
    Semantic search smoke tests using the full ingestion pipeline.

    These tests create documents with the SDK, run the datahub-documents
    ingestion source to generate chunks and embeddings, and verify
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

    def test_end_to_end_ingestion_and_semantic_search(self, auth_session):
        """
        End-to-end test: Create documents with SDK, run ingestion, verify semantic search.

        Steps:
        1. Create documents using Document SDK
        2. Create recipe for datahub-documents source
        3. Run ingestion to generate chunks and embeddings
        4. Verify semanticContent aspect was created
        5. Execute semantic search
        6. Verify documents appear in results
        """
        logger.info("Starting end-to-end semantic search test with ingestion pipeline")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents using SDK
            logger.info(f"Creating {len(SAMPLE_DOCUMENTS)} documents with SDK...")
            created_docs = create_documents_with_sdk(auth_session, SAMPLE_DOCUMENTS)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            assert len(created_docs) == len(SAMPLE_DOCUMENTS), (
                f"Expected to create {len(SAMPLE_DOCUMENTS)} documents, "
                f"but created {len(created_docs)}"
            )

            # Step 2: Create ingestion recipe
            doc_ids = [doc_id for doc_id, _ in created_docs]
            recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)

            # Step 3: Run ingestion
            logger.info("Running datahub-documents ingestion source...")
            run_ingestion(auth_session, recipe_path)

            # Wait for writes to sync
            wait_for_writes_to_sync(mcp_only=True)

            # Step 4: Verify semanticContent aspect was created for each document
            logger.info("Verifying semanticContent aspects were created...")
            for _doc_id, urn in created_docs:
                try:
                    verify_semantic_content(auth_session, urn)
                    logger.info(f"  ✓ {urn}: semanticContent verified")
                except Exception as e:
                    pytest.fail(f"Failed to verify semanticContent for {urn}: {e}")

            # Step 5: Wait for indexing
            logger.info(f"Waiting {INDEXING_WAIT_SECONDS} seconds for indexing...")
            time.sleep(INDEXING_WAIT_SECONDS)
            wait_for_writes_to_sync(mcp_only=True)

            # Step 6: Execute semantic search
            test_query = "how to request data access permissions"
            logger.info(f"Executing semantic search: '{test_query}'")

            result = search_documents_semantic(auth_session, test_query)

            # Check for GraphQL errors
            if "errors" in result:
                pytest.fail(f"GraphQL errors: {result['errors']}")

            # Step 7: Verify results
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

            # Assert that at least one of our documents appears in results
            assert len(matching_urns) > 0, (
                f"None of our test documents appeared in semantic search results. "
                f"Created URNs: {self.created_urns}, "
                f"Result URNs: {result_urns}"
            )

            logger.info("✓ End-to-end semantic search test passed!")
