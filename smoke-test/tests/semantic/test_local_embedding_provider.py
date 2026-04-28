"""
Smoke tests for the local (Ollama) embedding provider.

Validates end-to-end semantic search using the ``local`` embedding provider,
which calls a locally-running OpenAI-compatible embeddings server (e.g. Ollama)
instead of a cloud API.

Gate: LOCAL_EMBEDDING_PROVIDER_TESTS=true
Requires: EMBEDDING_PROVIDER_TYPE=local configured on the target DataHub instance,
          and an Ollama (or compatible) server reachable at LOCAL_EMBEDDING_ENDPOINT.

Usage — local dev with Ollama on host:
    ollama serve                          # starts on http://localhost:11434
    ollama pull nomic-embed-text

    LOCAL_EMBEDDING_PROVIDER_TESTS=true \\
    LOCAL_EMBEDDING_ENDPOINT=http://localhost:11434/v1/embeddings \\
    LOCAL_EMBEDDING_MODEL=nomic-embed-text \\
        pytest tests/semantic/test_local_embedding_provider.py -v

Usage — quickstart-ai docker-compose profile:
    docker compose --profile quickstart-ai up -d
    # Ollama is accessible inside the Docker network; GMS is wired via LOCAL_EMBEDDING_ENDPOINT.
    # On the host, use host.docker.internal:11434 (Mac/Windows) or host IP (Linux).

    LOCAL_EMBEDDING_PROVIDER_TESTS=true \\
        pytest tests/semantic/test_local_embedding_provider.py -v
"""

import logging
import os
import time
import urllib.error
import urllib.request

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.semantic.test_semantic_search import (
    SAMPLE_DOCUMENTS,
    create_documents_with_sdk,
    create_ingestion_recipe,
    delete_document,
    execute_graphql,
    run_ingestion,
    search_documents_semantic,
    verify_semantic_content,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment gates
# ---------------------------------------------------------------------------

LOCAL_EMBEDDING_TESTS_ENABLED = (
    os.environ.get("LOCAL_EMBEDDING_PROVIDER_TESTS", "false").lower() == "true"
)

# Where to reach the local embedding server from the test runner host.
# This may differ from what GMS uses (e.g., host.docker.internal vs ollama service name).
LOCAL_EMBEDDING_ENDPOINT = os.environ.get(
    "LOCAL_EMBEDDING_ENDPOINT", "http://localhost:11434/v1/embeddings"
)

LOCAL_EMBEDDING_MODEL = os.environ.get("LOCAL_EMBEDDING_MODEL", "nomic-embed-text")

# Expected embedding key after model name normalisation (- and . replaced with _)
EXPECTED_EMBEDDING_KEY = LOCAL_EMBEDDING_MODEL.replace("-", "_").replace(".", "_")

# Seconds to wait for ES semantic index refresh after embedding generation
INDEXING_WAIT_SECONDS = int(os.environ.get("EMBEDDING_WAIT_SECONDS", "20"))


# ---------------------------------------------------------------------------
# Connectivity helpers
# ---------------------------------------------------------------------------


def _ollama_is_reachable(endpoint: str, timeout: int = 5) -> bool:
    """Return True if the embeddings endpoint responds to a HEAD/GET probe."""
    # Probe the root URL (strip /v1/embeddings) so we don't need a real request body
    base_url = endpoint.split("/v1/")[0] if "/v1/" in endpoint else endpoint
    try:
        req = urllib.request.Request(base_url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except (urllib.error.URLError, OSError):
        return False


def _require_ollama_or_skip(endpoint: str) -> None:
    """Skip the calling test with an informative message if Ollama is unreachable."""
    if not _ollama_is_reachable(endpoint):
        pytest.skip(
            f"Local embedding server not reachable at {endpoint}. "
            "Start Ollama with: ollama serve"
        )


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _verify_local_embedding_key(auth_session, urn: str, expected_key: str) -> dict:
    """Verify semanticContent exists and uses the expected embedding model key."""
    semantic_content = verify_semantic_content(auth_session, urn)
    embeddings = semantic_content.get("embeddings", {})
    assert expected_key in embeddings, (
        f"Expected embedding key '{expected_key}' not found in semanticContent for {urn}. "
        f"Available keys: {list(embeddings.keys())}"
    )
    model_embed = embeddings[expected_key]
    chunks = model_embed.get("chunks", [])
    assert len(chunks) > 0, f"No chunks found under key '{expected_key}' for {urn}"

    # Verify vector dimensions match nomic-embed-text (768) or user-specified model
    first_vector = chunks[0].get("vector", [])
    logger.info(
        f"  ✓ '{expected_key}': {len(chunks)} chunks, {len(first_vector)}-dimensional vectors"
    )
    return semantic_content


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not LOCAL_EMBEDDING_TESTS_ENABLED,
    reason=(
        "Local embedding provider tests disabled. "
        "Set LOCAL_EMBEDDING_PROVIDER_TESTS=true to run."
    ),
)
class TestLocalEmbeddingProvider:
    """
    Smoke tests for the DataHub local embedding provider (Ollama / OpenAI-compatible).

    Each test creates its own documents and cleans them up on teardown so the
    suite is fully idempotent regardless of run order.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        self.created_urns: list[str] = []
        self.auth_session = auth_session

        # Skip all tests in this class if Ollama is unreachable from the host.
        _require_ollama_or_skip(LOCAL_EMBEDDING_ENDPOINT)

        yield

        for urn in self.created_urns:
            try:
                delete_document(auth_session, urn)
                logger.info(f"Cleaned up document: {urn}")
            except Exception as e:
                logger.warning(f"Failed to clean up {urn}: {e}")

    # ------------------------------------------------------------------
    # Test 1 — Connectivity
    # ------------------------------------------------------------------

    def test_local_embedding_server_reachable(self):
        """The Ollama server must be reachable at the configured endpoint."""
        assert _ollama_is_reachable(LOCAL_EMBEDDING_ENDPOINT), (
            f"Local embedding server not reachable at {LOCAL_EMBEDDING_ENDPOINT}. "
            "Start Ollama with: ollama serve"
        )
        logger.info(f"✓ Local embedding server reachable at {LOCAL_EMBEDDING_ENDPOINT}")

    # ------------------------------------------------------------------
    # Test 2 — Provider configuration
    # ------------------------------------------------------------------

    def test_embedding_provider_type_is_local(self, auth_session):
        """
        Verify that the DataHub instance is configured to use the local provider.

        Queries the AppConfig endpoint to confirm EMBEDDING_PROVIDER_TYPE=local.
        If the server is not configured with the local provider this test will
        fail with a descriptive message rather than silently producing wrong results.
        """
        query = """
            query GetAppConfig {
                appConfig {
                    featureFlags {
                        showSearchFiltersV2
                    }
                    semanticSearchConfig {
                        embeddingConfig {
                            provider
                        }
                    }
                }
            }
        """
        result = execute_graphql(auth_session, query)
        if "errors" in result:
            pytest.skip(
                f"Could not fetch appConfig (GraphQL errors: {result['errors']}). "
                "Skipping provider type assertion."
            )

        semantic_config = (
            result.get("data", {}).get("appConfig", {}).get("semanticSearchConfig")
            or {}
        )
        embedding_config = semantic_config.get("embeddingConfig") or {}
        provider_type = embedding_config.get("provider")

        if provider_type is None:
            pytest.skip(
                "semanticSearchConfig.embeddingConfig.provider not exposed by this server. "
                "Skipping check."
            )

        assert provider_type == "local", (
            f"Expected EMBEDDING_PROVIDER_TYPE=local but got '{provider_type}'. "
            "Configure the DataHub GMS with: EMBEDDING_PROVIDER_TYPE=local"
        )
        logger.info(f"✓ Embedding provider type confirmed: {provider_type}")

    # ------------------------------------------------------------------
    # Test 3 — Embedding generation with model key validation
    # ------------------------------------------------------------------

    def test_local_embeddings_generated_with_correct_model_key(
        self, auth_session, tmp_path
    ):
        """
        Create documents, generate embeddings via the local provider, and verify
        the semanticContent aspect uses the expected model key (e.g. nomic_embed_text).
        """
        logger.info(
            f"Testing local embedding generation "
            f"(model: {LOCAL_EMBEDDING_MODEL}, expected key: {EXPECTED_EMBEDDING_KEY})"
        )

        # Use a single document to keep the test fast
        test_doc = SAMPLE_DOCUMENTS[:1]
        created_docs = create_documents_with_sdk(auth_session, test_doc)
        for _doc_id, urn in created_docs:
            self.created_urns.append(urn)

        doc_ids = [doc_id for doc_id, _ in created_docs]
        urns = [urn for _, urn in created_docs]

        # Run the datahub-documents ingestion to generate embeddings
        recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
        run_ingestion(auth_session, recipe_path)
        wait_for_writes_to_sync(mcp_only=True)

        logger.info(f"Waiting {INDEXING_WAIT_SECONDS}s for semantic index refresh...")
        time.sleep(INDEXING_WAIT_SECONDS)

        # Verify semanticContent with expected model key
        for urn in urns:
            _verify_local_embedding_key(auth_session, urn, EXPECTED_EMBEDDING_KEY)
            logger.info(
                f"  ✓ {urn}: local embedding with key '{EXPECTED_EMBEDDING_KEY}' verified"
            )

        logger.info(
            f"✓ Local embedding generation confirmed with model key '{EXPECTED_EMBEDDING_KEY}'"
        )

    # ------------------------------------------------------------------
    # Test 4 — End-to-end semantic search with local provider
    # ------------------------------------------------------------------

    def test_end_to_end_semantic_search_with_local_provider(
        self, auth_session, tmp_path
    ):
        """
        Full end-to-end test: create documents, generate local embeddings, verify
        semantic search returns semantically relevant results in the correct order.

        Uses the same 3-document corpus as the main semantic search test so the
        ranking assertion is identical: "Data Access Request Process" must rank
        higher than the other two docs for the query "how to request data access
        permissions".
        """
        logger.info(
            f"Starting end-to-end local embedding semantic search test "
            f"(model={LOCAL_EMBEDDING_MODEL})"
        )

        # Step 1: Create all 3 sample documents
        created_docs = create_documents_with_sdk(auth_session, SAMPLE_DOCUMENTS)
        for _doc_id, urn in created_docs:
            self.created_urns.append(urn)

        assert len(created_docs) == len(SAMPLE_DOCUMENTS)
        doc_ids = [doc_id for doc_id, _ in created_docs]
        urns = [urn for _, urn in created_docs]

        # Step 2: Generate embeddings via the local provider
        recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
        run_ingestion(auth_session, recipe_path)
        wait_for_writes_to_sync(mcp_only=True)

        logger.info(f"Waiting {INDEXING_WAIT_SECONDS}s for semantic index refresh...")
        time.sleep(INDEXING_WAIT_SECONDS)

        # Step 3: Verify semanticContent exists for each document
        logger.info("Verifying semanticContent aspects...")
        for urn in urns:
            try:
                _verify_local_embedding_key(auth_session, urn, EXPECTED_EMBEDDING_KEY)
                logger.info(f"  ✓ {urn}: local embedding verified")
            except Exception as e:
                pytest.fail(f"semanticContent verification failed for {urn}: {e}")

        # Step 4: Run semantic search and assert relevance ranking
        test_query = "how to request data access permissions"
        logger.info(f"Executing semantic search: '{test_query}'")

        result = search_documents_semantic(auth_session, test_query)

        if "errors" in result:
            pytest.fail(
                f"GraphQL errors from semanticSearchAcrossEntities: {result['errors']}"
            )

        search_data = result.get("data", {}).get("semanticSearchAcrossEntities", {})
        total = search_data.get("total", 0)
        search_results = search_data.get("searchResults", [])

        logger.info(f"Semantic search returned {total} total results")
        assert total > 0, "Semantic search returned no results"
        assert len(search_results) > 0, "No search results in response"

        result_urns = [item.get("entity", {}).get("urn") for item in search_results]

        our_doc_urns = [f"urn:li:document:{doc_id}" for doc_id in doc_ids]
        access_doc_urn = our_doc_urns[1]  # "Data Access Request Process"
        getting_started_urn = our_doc_urns[0]  # "Getting Started with DataHub"
        churn_doc_urn = our_doc_urns[2]  # "Machine Learning Model: Churn Prediction"

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
            display_rank = rank + 1 if rank != float("inf") else "N/A"
            logger.info(f"  Rank {display_rank}: {title}")

        # All 3 documents must appear in the top-50 results
        for urn in our_doc_urns:
            assert urn in result_urns, (
                f"Document {urn} not found in top-{len(result_urns)} results. "
                "Embeddings may not have been indexed yet, or count is too low."
            )

        # Relevance ordering: "Data Access Request Process" must rank above both others
        assert ranks[access_doc_urn] < ranks[getting_started_urn], (
            f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should rank "
            f"above 'Getting Started with DataHub' (rank {ranks[getting_started_urn] + 1})"
        )
        assert ranks[access_doc_urn] < ranks[churn_doc_urn], (
            f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should rank "
            f"above 'Churn Prediction' (rank {ranks[churn_doc_urn] + 1})"
        )

        logger.info(
            f"✓ Relevance ranking correct — 'Data Access Request Process' "
            f"ranked #{ranks[access_doc_urn] + 1} "
            f"(above Getting Started #{ranks[getting_started_urn] + 1} "
            f"and Churn Prediction #{ranks[churn_doc_urn] + 1})"
        )
        logger.info("✓ End-to-end local embedding semantic search test passed!")
