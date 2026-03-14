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

import contextlib
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


@contextlib.contextmanager
def _timed(label: str):
    """Log elapsed wall-clock time for a block of code."""
    t0 = time.monotonic()
    logger.info(f"[TIMER] {label} — start")
    try:
        yield
    finally:
        elapsed = time.monotonic() - t0
        logger.info(f"[TIMER] {label} — done in {elapsed:.1f}s")


# Test gating - disabled by default
SEMANTIC_SEARCH_ENABLED = (
    os.environ.get("ENABLE_SEMANTIC_SEARCH_TESTS", "false").lower() == "true"
)

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
    # Wait for the MAE consumer (MetadataChangeLog → Elasticsearch). In quickstartDebug,
    # GMS processes MCPs synchronously so generic-mcp-consumer-job-client is not deployed;
    # it is the MAE consumer that drives ES indexing.
    wait_for_writes_to_sync(mae_only=True)

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


def search_documents_semantic_with_chunks(
    auth_session, query: str, count: int = 10
) -> dict:
    """Search documents using semantic search, requesting matchedChunks highlighting."""
    graphql_query = """
        query SemanticSearchWithChunks($input: SearchAcrossEntitiesInput!) {
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
                    matchedChunks {
                        position
                        text
                        characterOffset
                        characterLength
                        score
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
            with _timed("create_documents_with_sdk"):
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
            with _timed("run_ingestion"):
                run_ingestion(auth_session, recipe_path)

            # Wait for writes to sync
            with _timed("wait_for_writes_to_sync (post-ingestion)"):
                wait_for_writes_to_sync(mae_only=True)

            # Step 4: Verify semanticContent aspect was created for each document
            logger.info("Verifying semanticContent aspects were created...")
            with _timed("verify_semantic_content (all docs)"):
                for _doc_id, urn in created_docs:
                    try:
                        verify_semantic_content(auth_session, urn)
                        logger.info(f"  ✓ {urn}: semanticContent verified")
                    except Exception as e:
                        pytest.fail(f"Failed to verify semanticContent for {urn}: {e}")

            # Step 6: Execute semantic search
            test_query = "how to request data access permissions"
            logger.info(f"Executing semantic search: '{test_query}'")

            with _timed("semantic search GraphQL"):
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

    def test_matched_chunks_in_semantic_search_results(self, auth_session):
        """
        Verify that matchedChunks is populated for semantic search results.

        This proves the chunk-level highlighting feature end-to-end:
        - The nested kNN query returns inner_hits (the chunk that drove ranking)
        - SemanticEntitySearchService extracts them into SearchEntity.semanticMatchedChunks
        - MapperUtils maps them to GraphQL SearchResult.matchedChunks
        - The API returns position, text/characterOffset, and score per chunk

        Steps:
        1. Create a document with clearly separable sections via SDK
        2. Run datahub-documents ingestion to generate chunks + embeddings
        3. Execute semanticSearchAcrossEntities with matchedChunks in the selection set
        4. Assert every result has at least one matchedChunk
        5. Assert each chunk has valid position, score, and text or offset fields
        6. Assert chunks are ordered by descending score (best match first)
        """
        logger.info("Starting matched chunks smoke test")

        # Use a single focused document so we can predict what should match.
        # The "Data Access Request Process" doc has a distinct section about
        # "request steps" — querying for "how to request data access" should
        # land in that chunk rather than the intro.
        focused_docs = [SAMPLE_DOCUMENTS[1]]  # "Data Access Request Process"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            logger.info("Creating test document via SDK...")
            with _timed("create_documents_with_sdk"):
                created_docs = create_documents_with_sdk(auth_session, focused_docs)
            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            # Run ingestion to produce chunks and embeddings
            doc_ids = [doc_id for doc_id, _ in created_docs]
            recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
            logger.info("Running ingestion to generate embeddings...")
            with _timed("run_ingestion"):
                run_ingestion(auth_session, recipe_path)
            with _timed("wait_for_writes_to_sync (post-ingestion)"):
                wait_for_writes_to_sync(mae_only=True)

            # Verify semanticContent was written before searching
            with _timed("verify_semantic_content"):
                for _doc_id, urn in created_docs:
                    verify_semantic_content(auth_session, urn)

            # Search with matchedChunks requested
            query = "steps to request access to a dataset"
            logger.info(f"Searching with matchedChunks: '{query}'")
            with _timed("semantic search with matchedChunks GraphQL"):
                result = search_documents_semantic_with_chunks(auth_session, query)

            if "errors" in result:
                pytest.fail(
                    f"GraphQL errors in matchedChunks query: {result['errors']}"
                )

            search_data = result.get("data", {}).get("semanticSearchAcrossEntities", {})
            search_results = search_data.get("searchResults", [])

            assert len(search_results) > 0, "Semantic search returned no results"

            # Find our test document in the results
            our_urns = {urn for _doc_id, urn in created_docs}
            our_results = [
                r for r in search_results if r.get("entity", {}).get("urn") in our_urns
            ]
            assert len(our_results) > 0, (
                f"Our test document did not appear in results. "
                f"Created: {our_urns}, returned: "
                f"{[r.get('entity', {}).get('urn') for r in search_results]}"
            )

            # Validate matchedChunks on our test documents only. Other results
            # in the index may be stale (deleted entities whose kNN vectors linger
            # briefly) and their _source embeddings will already be cleared.
            for item in our_results:
                entity_urn = item.get("entity", {}).get("urn")
                chunks = item.get("matchedChunks", [])

                assert chunks is not None, (
                    f"matchedChunks field is null for {entity_urn} — "
                    "expected non-null list (even if empty for keyword results)"
                )
                assert len(chunks) > 0, (
                    f"matchedChunks is empty for semantic result {entity_urn} — "
                    "inner_hits should have returned at least one chunk"
                )

                for i, chunk in enumerate(chunks):
                    # position must always be present and non-negative
                    assert "position" in chunk, (
                        f"chunk[{i}] missing 'position' for {entity_urn}"
                    )
                    assert (
                        isinstance(chunk["position"], int) and chunk["position"] >= 0
                    ), (
                        f"chunk[{i}].position={chunk['position']} is invalid for {entity_urn}"
                    )

                    # score must be present and a positive float
                    assert "score" in chunk and chunk["score"] is not None, (
                        f"chunk[{i}] missing 'score' for {entity_urn}"
                    )
                    assert chunk["score"] > 0, (
                        f"chunk[{i}].score={chunk['score']} should be > 0 for {entity_urn}"
                    )

                    # At least one of text or characterOffset must be present so
                    # the UI can display or slice the matching passage
                    has_text = chunk.get("text") is not None
                    has_offset = chunk.get("characterOffset") is not None
                    assert has_text or has_offset, (
                        f"chunk[{i}] for {entity_urn} has neither 'text' nor "
                        "'characterOffset' — UI cannot highlight this result"
                    )

                    if has_text:
                        assert len(chunk["text"]) > 0, (
                            f"chunk[{i}].text is empty for {entity_urn}"
                        )
                        # Verify the chunk text contains words from the document we
                        # ingested. We don't do an exact substring match because the
                        # chunking library normalizes whitespace (e.g. list-item newlines
                        # become spaces). Instead we check for a few distinctive phrases
                        # that must survive any reasonable normalization.
                        doc_title = focused_docs[0]["title"]
                        doc_text = focused_docs[0]["text"]
                        distinctive_phrases = [
                            w
                            for w in doc_text.split()
                            if len(w) > 6  # skip short/common words
                        ]
                        chunk_words = set(chunk["text"].split())
                        overlap = [p for p in distinctive_phrases if p in chunk_words]
                        assert len(overlap) >= 3, (
                            f"chunk[{i}].text for {entity_urn} shares fewer than 3 words "
                            f"with the ingested document '{doc_title}' — likely wrong content.\n"
                            f"  chunk text (first 300): {chunk['text'][:300]!r}"
                        )

                    if has_offset:
                        assert chunk.get("characterLength") is not None, (
                            f"chunk[{i}] has characterOffset but no characterLength "
                            f"for {entity_urn}"
                        )

                # Chunks must be ordered by descending score (best match first)
                scores = [c["score"] for c in chunks]
                assert scores == sorted(scores, reverse=True), (
                    f"matchedChunks for {entity_urn} are not sorted by descending score: "
                    f"{scores}"
                )
                logger.info(
                    f"  ✓ {entity_urn}: {len(chunks)} chunk(s), "
                    f"top score={scores[0]:.4f}, "
                    f"text={'present' if chunks[0].get('text') else 'absent (offset fallback)'}"
                )

            logger.info("✓ matchedChunks chunk-level highlighting test passed!")
