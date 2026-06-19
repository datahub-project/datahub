"""
Smoke tests for Semantic Search functionality.

These tests validate end-to-end semantic search in two embedding modes:

**Client-side embedding (default)**
  The test runner executes the ``datahub-documents`` ingestion source in-process,
  generating embeddings via Bedrock and writing ``semanticContent`` aspects to the
  target DataHub instance.  Requires AWS credentials on the machine running the tests.

**Server-side embedding (pre-provisioned)**
  Set ``DOCS_INGESTION_SERVER_SIDE=true`` when the target DataHub instance already has
  a ``datahub-documents`` ingestion source pre-provisioned (as in production DataHub
  Cloud).  The test skips the in-process ingestion and instead polls until
  ``semanticContent`` appears on each document — emitted automatically by the server.
  No AWS credentials needed on the test runner in this mode.

Gate: ENABLE_SEMANTIC_SEARCH_TESTS=true

Usage — local instance (client-side embedding):
    AWS_PROFILE=acryl-read-write ENABLE_SEMANTIC_SEARCH_TESTS=true \\
        pytest tests/semantic/test_semantic_search.py -v

Usage — remote pre-provisioned instance (server-side embedding):
    ENABLE_SEMANTIC_SEARCH_TESTS=true DOCS_INGESTION_SERVER_SIDE=true \\
        DATAHUB_GMS_URL=https://your-instance.acryl.io/gms \\
        DATAHUB_FRONTEND_URL=https://your-instance.acryl.io \\
        ADMIN_USERNAME=you@company.com ADMIN_PASSWORD=<pat-token> \\
        pytest tests/semantic/test_semantic_search.py -v
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

# When True, skip the in-process ingestion and poll for server-generated semanticContent.
# Use when the target instance has datahub-documents pre-provisioned (e.g. DataHub Cloud).
DOCS_INGESTION_SERVER_SIDE = (
    os.environ.get("DOCS_INGESTION_SERVER_SIDE", "false").lower() == "true"
)

# How long to poll for server-side embedding generation before giving up (seconds).
SERVER_SIDE_EMBEDDING_TIMEOUT = int(os.environ.get("EMBEDDING_WAIT_TIMEOUT", "300"))

# How long to wait for ES to index after embedding is confirmed
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
        "pipeline_name": "smoke-test-semantic-search",
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


DOCS_INGESTION_SOURCE_URN = "urn:li:dataHubIngestionSource:datahub-documents"

TRIGGER_INGESTION_MUTATION = """
    mutation TriggerIngestion($urn: String!) {
        createIngestionExecutionRequest(input: { ingestionSourceUrn: $urn })
    }
"""

GET_EXECUTION_REQUEST_QUERY = """
    query GetExecutionRequest($urn: String!) {
        executionRequest(urn: $urn) {
            result { status durationMs }
        }
    }
"""


def trigger_and_wait_for_ingestion(
    auth_session, timeout: int = SERVER_SIDE_EMBEDDING_TIMEOUT
) -> None:
    """Trigger the datahub-documents ingestion source and wait for it to complete.

    This is faster and more deterministic than passively polling for semanticContent —
    we fire the run immediately and monitor the execution request until SUCCESS/FAILURE.
    """
    import time as _time

    # 1. Trigger the ingestion source
    trigger_result = execute_graphql(
        auth_session,
        TRIGGER_INGESTION_MUTATION,
        {"urn": DOCS_INGESTION_SOURCE_URN},
    )
    if "errors" in trigger_result:
        raise RuntimeError(f"Failed to trigger ingestion: {trigger_result['errors']}")
    execution_urn = trigger_result["data"]["createIngestionExecutionRequest"]
    logger.info(f"Triggered ingestion — execution request: {execution_urn}")

    # 2. Poll execution request until terminal state
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        status_result = execute_graphql(
            auth_session, GET_EXECUTION_REQUEST_QUERY, {"urn": execution_urn}
        )
        result = (
            status_result.get("data", {}).get("executionRequest", {}).get("result")
            or {}
        )
        status = result.get("status", "PENDING")
        if status == "SUCCESS":
            duration_ms = result.get("durationMs", 0)
            logger.info(f"  ✓ Ingestion completed in {duration_ms}ms")
            return
        if status in ("FAILURE", "FAILED", "CANCELLED"):
            raise RuntimeError(f"Ingestion execution ended with status: {status}")
        logger.debug(f"  … ingestion status={status}, waiting…")
        _time.sleep(5)

    raise TimeoutError(
        f"Timed out after {timeout}s waiting for ingestion execution {execution_urn}"
    )


def wait_for_server_side_embeddings(
    auth_session, urns: list[str], timeout: int = SERVER_SIDE_EMBEDDING_TIMEOUT
) -> None:
    """Trigger the datahub-documents ingestion source and wait for semanticContent.

    In server-side mode we:
    1. Fire the ingestion source immediately (no waiting for a schedule)
    2. Poll the execution request until it completes
    3. Then verify semanticContent appeared on each document
    """
    import time as _time

    # Trigger the ingestion and wait for the run to finish
    trigger_and_wait_for_ingestion(auth_session, timeout=timeout)

    # Verify semanticContent is now present on each document
    for urn in urns:
        deadline = _time.time() + 60  # short timeout after ingestion just ran
        while _time.time() < deadline:
            try:
                verify_semantic_content(auth_session, urn)
                logger.info(f"  ✓ {urn}: semanticContent verified")
                break
            except Exception:
                _time.sleep(5)
        else:
            raise TimeoutError(
                f"semanticContent not found on {urn} after ingestion completed. "
                "The document may not have been processed by this run."
            )


def ensure_embeddings(
    auth_session, doc_ids: list[str], tmp_path: Path | None = None
) -> None:
    """Generate (or wait for) embeddings depending on the current mode.

    - Client-side mode: runs the datahub-documents ingestion source in-process.
    - Server-side mode: triggers the pre-provisioned source, waits for completion.
    """
    urns = [f"urn:li:document:{doc_id}" for doc_id in doc_ids]
    if DOCS_INGESTION_SERVER_SIDE:
        logger.info("Server-side embedding mode: triggering datahub-documents source…")
        wait_for_server_side_embeddings(auth_session, urns)
    else:
        if tmp_path is None:
            raise ValueError("tmp_path required for client-side embedding mode")
        logger.info("Client-side embedding mode: running datahub-documents ingestion…")
        recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
        run_ingestion(auth_session, recipe_path)
        wait_for_writes_to_sync(mcp_only=True)

    # Wait for Kafka consumers to catch up, then sleep for ES semantic index refresh.
    wait_for_writes_to_sync(mcp_only=True)
    logger.info(f"Waiting {INDEXING_WAIT_SECONDS}s for ES semantic index refresh…")
    time.sleep(INDEXING_WAIT_SECONDS)


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

    # Find the first embedding field dynamically - supports any model key
    # (cohere_embed_v3, text_embedding_3_small, etc.)
    embedding_keys = list(embeddings.keys())
    if not embedding_keys:
        raise Exception(f"No embedding keys found in semanticContent for {urn}")

    model_embedding_key = embedding_keys[0]
    model_embed = embeddings.get(model_embedding_key)
    if not model_embed:
        raise Exception(
            f"No embeddings found for key '{model_embedding_key}'. Available fields: {embedding_keys}"
        )

    chunks = model_embed.get("chunks", [])
    total_chunks = model_embed.get("totalChunks", 0)

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


def search_documents_semantic(auth_session, query: str, count: int = 50) -> dict:
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
        End-to-end test: create documents, ensure embeddings exist, verify semantic search.

        Embedding mode is selected by DOCS_INGESTION_SERVER_SIDE env var:
          - false (default): runs datahub-documents ingestion in-process (requires AWS creds)
          - true: polls until the server generates semanticContent automatically
        """
        mode = "server-side" if DOCS_INGESTION_SERVER_SIDE else "client-side"
        logger.info(f"Starting semantic search test [{mode} embedding mode]")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents
            logger.info(f"Creating {len(SAMPLE_DOCUMENTS)} documents with SDK...")
            created_docs = create_documents_with_sdk(auth_session, SAMPLE_DOCUMENTS)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            assert len(created_docs) == len(SAMPLE_DOCUMENTS), (
                f"Expected to create {len(SAMPLE_DOCUMENTS)} documents, "
                f"but created {len(created_docs)}"
            )

            # Step 2: Ensure embeddings are generated (client-side or server-side)
            doc_ids = [doc_id for doc_id, _ in created_docs]
            ensure_embeddings(auth_session, doc_ids, tmp_path)

            # Step 3: Verify semanticContent aspect is present for each document
            logger.info("Verifying semanticContent aspects...")
            for _doc_id, urn in created_docs:
                try:
                    verify_semantic_content(auth_session, urn)
                    logger.info(f"  ✓ {urn}: semanticContent verified")
                except Exception as e:
                    pytest.fail(f"Failed to verify semanticContent for {urn}: {e}")

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

            result_urns = [item.get("entity", {}).get("urn") for item in search_results]

            # Map each of our 3 documents to its rank in the result list.
            # inf = not in results at all.
            our_doc_urns = [f"urn:li:document:{doc_id}" for doc_id in doc_ids]
            access_doc_urn = our_doc_urns[1]  # "Data Access Request Process"
            getting_started_urn = our_doc_urns[0]  # "Getting Started with DataHub"
            churn_doc_urn = our_doc_urns[
                2
            ]  # "Machine Learning Model: Churn Prediction"

            ranks = {
                urn: result_urns.index(urn) if urn in result_urns else float("inf")
                for urn in our_doc_urns
            }

            for urn, rank in ranks.items():
                title = next(
                    d["title"]
                    for d, uid in zip(SAMPLE_DOCUMENTS, doc_ids, strict=False)
                    if f"urn:li:document:{uid}" == urn
                )
                logger.info(
                    f"  Rank {rank + 1 if rank != float('inf') else 'N/A'}: {title}"
                )

            # All 3 docs must appear somewhere in results (count=50 to leave headroom)
            for urn in our_doc_urns:
                assert urn in result_urns, (
                    f"Document {urn} not found in top-{len(result_urns)} semantic search "
                    f"results — embeddings may not have been indexed yet or count is too low"
                )

            # Relative ranking: "Data Access Request Process" must rank higher than
            # both other docs for the query "how to request data access permissions".
            # This validates semantic relevance ordering, not absolute position —
            # it is robust to stale documents from prior test runs ranking above ours.
            assert ranks[access_doc_urn] < ranks[getting_started_urn], (
                f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should "
                f"rank above 'Getting Started' (rank {ranks[getting_started_urn] + 1})"
            )
            assert ranks[access_doc_urn] < ranks[churn_doc_urn], (
                f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should "
                f"rank above 'Churn Prediction' (rank {ranks[churn_doc_urn] + 1})"
            )

            logger.info(
                f"✓ Relative ranking correct — 'Data Access Request Process' ranked "
                f"#{ranks[access_doc_urn] + 1} (above Getting Started #{ranks[getting_started_urn] + 1} "
                f"and Churn Prediction #{ranks[churn_doc_urn] + 1})"
            )
            logger.info("✓ End-to-end semantic search test passed!")
