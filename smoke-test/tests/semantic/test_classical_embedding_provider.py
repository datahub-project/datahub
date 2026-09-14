"""
Smoke tests for the classical (deterministic hashing) embedding provider.

Validates end-to-end semantic search using the ``classical`` embedding provider:
a stateless hashed-feature embedding that GMS (queries) and the
``datahub-documents`` ingestion source (documents) compute identically. It needs
no API key, endpoint, or model download, so it is the lightest way to exercise
the semantic search path on a quickstart.

Gate: CLASSICAL_EMBEDDING_PROVIDER_TESTS=true
Requires: a DataHub instance whose GMS runs with
          ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true and
          EMBEDDING_PROVIDER_TYPE=classical.

Usage — quickstart:
    # GMS (and system-update, which builds the semantic index) must run with
    #   EMBEDDING_PROVIDER_TYPE=classical
    #   ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
    # set on the service environment, e.g. through a compose override like
    # docker/profiles/docker-compose.onnx-override.yml with the ONNX settings
    # replaced by the two variables above (classical needs no model files).

    CLASSICAL_EMBEDDING_PROVIDER_TESTS=true \\
        pytest tests/semantic/test_classical_embedding_provider.py -v

The ingestion recipe carries no embedding config: provider and model are loaded
from the server's AppConfig, which is what these tests verify. If GMS runs a
non-default model name (CLASSICAL_EMBEDDING_MODEL), export the same value here so
the expected storage key and vector width match.
"""

import logging
import os
import time

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
from tests.utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.AI)

# ---------------------------------------------------------------------------
# Environment gates
# ---------------------------------------------------------------------------

CLASSICAL_EMBEDDING_TESTS_ENABLED = (
    os.environ.get("CLASSICAL_EMBEDDING_PROVIDER_TESTS", "false").lower() == "true"
)

# Model name GMS runs. The storage key and vector width follow from it:
# hash-v1-2048 -> key "hash_v1_2048", 2048 components.
CLASSICAL_EMBEDDING_MODEL = os.environ.get("CLASSICAL_EMBEDDING_MODEL", "hash-v1-2048")

EXPECTED_EMBEDDING_KEY = CLASSICAL_EMBEDDING_MODEL.replace("-", "_").replace(".", "_")
EXPECTED_DIMENSIONS = int(CLASSICAL_EMBEDDING_MODEL.rsplit("-", 1)[1])

# Deadline for the ES semantic index to surface freshly embedded documents
INDEXING_WAIT_SECONDS = int(os.environ.get("EMBEDDING_WAIT_SECONDS", "20"))
INDEXING_POLL_INTERVAL_SECONDS = 2


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _wait_for_semantic_index(
    auth_session, query: str, expected_urns: list[str]
) -> dict:
    """Poll semantic search until every expected URN is returned, or the deadline passes.

    The semantic index refreshes asynchronously after the semanticContent write, so
    a single query after a fixed sleep races it. Pass only the URNs the caller goes
    on to assert on: the classical provider is lexical, so unrelated documents may
    legitimately never rank for the query. Returns the last response either way.
    """
    deadline = time.monotonic() + INDEXING_WAIT_SECONDS
    while True:
        result = search_documents_semantic(auth_session, query)
        search_data = (result.get("data") or {}).get(
            "semanticSearchAcrossEntities"
        ) or {}
        found = {
            (item.get("entity") or {}).get("urn")
            for item in search_data.get("searchResults", [])
        }
        if "errors" not in result and set(expected_urns) <= found:
            return result
        if time.monotonic() >= deadline:
            logger.warning(
                f"Semantic index did not surface {set(expected_urns) - found} for "
                f"'{query}' within {INDEXING_WAIT_SECONDS}s"
            )
            return result
        time.sleep(INDEXING_POLL_INTERVAL_SECONDS)


def _verify_classical_embedding(auth_session, urn: str) -> None:
    """Verify semanticContent uses the classical model key with integer-valued vectors."""
    semantic_content = verify_semantic_content(auth_session, urn)
    embeddings = semantic_content.get("embeddings", {})
    assert EXPECTED_EMBEDDING_KEY in embeddings, (
        f"Expected embedding key '{EXPECTED_EMBEDDING_KEY}' not found in semanticContent "
        f"for {urn}. Available keys: {list(embeddings.keys())}"
    )
    chunks = embeddings[EXPECTED_EMBEDDING_KEY].get("chunks", [])
    assert len(chunks) > 0, f"No chunks under key '{EXPECTED_EMBEDDING_KEY}' for {urn}"

    for chunk in chunks:
        vector = chunk.get("vector", [])
        assert len(vector) == EXPECTED_DIMENSIONS, (
            f"Chunk {chunk.get('position')} of {urn} has {len(vector)} components, "
            f"expected {EXPECTED_DIMENSIONS} from model '{CLASSICAL_EMBEDDING_MODEL}'"
        )
        # Classical vectors are raw integer feature counts (no L2 normalization).
        assert all(v == int(v) for v in vector), (
            f"Chunk {chunk.get('position')} of {urn} has non-integer components; "
            "was it embedded by a different provider?"
        )
    logger.info(
        f"  ✓ '{EXPECTED_EMBEDDING_KEY}': {len(chunks)} chunks, "
        f"{EXPECTED_DIMENSIONS}-dimensional integer vectors"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not CLASSICAL_EMBEDDING_TESTS_ENABLED,
    reason=(
        "Classical embedding provider tests disabled. "
        "Set CLASSICAL_EMBEDDING_PROVIDER_TESTS=true to run."
    ),
)
class TestClassicalEmbeddingProvider:
    """
    Smoke tests for the DataHub classical (deterministic hashing) embedding provider.

    Each test creates its own documents and cleans them up on teardown so the
    suite is fully idempotent regardless of run order.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        self.created_urns: list[str] = []
        self.auth_session = auth_session

        yield

        for urn in self.created_urns:
            try:
                delete_document(auth_session, urn)
                logger.info(f"Cleaned up document: {urn}")
            except Exception as e:
                logger.warning(f"Failed to clean up {urn}: {e}")

    # ------------------------------------------------------------------
    # Test 1 — Provider configuration
    # ------------------------------------------------------------------

    def test_embedding_provider_type_is_classical(self, auth_session):
        """
        Verify that the DataHub instance is configured to use the classical provider.

        Queries the AppConfig endpoint to confirm EMBEDDING_PROVIDER_TYPE=classical.
        If the server is not configured with the classical provider this test will
        fail with a descriptive message rather than silently producing wrong results.
        """
        query = """
            query GetAppConfig {
                appConfig {
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

        assert provider_type == "classical", (
            f"Expected EMBEDDING_PROVIDER_TYPE=classical but got '{provider_type}'. "
            "Configure the DataHub GMS with: EMBEDDING_PROVIDER_TYPE=classical"
        )
        logger.info(f"✓ Embedding provider type confirmed: {provider_type}")

    # ------------------------------------------------------------------
    # Test 2 — Embedding generation with server-loaded config
    # ------------------------------------------------------------------

    def test_classical_embeddings_generated_with_expected_model_key(
        self, auth_session, tmp_path
    ):
        """
        Create a document, run the datahub-documents ingestion with no embedding
        config (provider and model come from the server), and verify semanticContent
        is written under the classical model key with the expected vector width.
        """
        logger.info(
            f"Testing classical embedding generation "
            f"(model: {CLASSICAL_EMBEDDING_MODEL}, expected key: {EXPECTED_EMBEDDING_KEY})"
        )

        created_docs = create_documents_with_sdk(auth_session, SAMPLE_DOCUMENTS[:1])
        for _doc_id, urn in created_docs:
            self.created_urns.append(urn)

        doc_ids = [doc_id for doc_id, _ in created_docs]
        urns = [urn for _, urn in created_docs]

        recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
        run_ingestion(auth_session, recipe_path)
        wait_for_writes_to_sync(mcp_only=True)

        # The title is a lexical query the classical provider must match.
        _wait_for_semantic_index(auth_session, SAMPLE_DOCUMENTS[0]["title"], urns)

        for urn in urns:
            _verify_classical_embedding(auth_session, urn)

        logger.info(
            f"✓ Classical embedding generation confirmed with model key '{EXPECTED_EMBEDDING_KEY}'"
        )

    # ------------------------------------------------------------------
    # Test 3 — End-to-end semantic search with the classical provider
    # ------------------------------------------------------------------

    def test_end_to_end_semantic_search_with_classical_provider(
        self, auth_session, tmp_path
    ):
        """
        Full end-to-end test: create documents, embed them with the classical
        provider, and verify semantic search finds them for a lexical query.

        The classical provider ranks by shared words and character fragments, not
        meaning, so the query reuses the title words of "Data Access Request
        Process": that document must be returned and rank above the other two.
        """
        logger.info(
            f"Starting end-to-end classical embedding semantic search test "
            f"(model={CLASSICAL_EMBEDDING_MODEL})"
        )

        # Step 1: Create all 3 sample documents
        created_docs = create_documents_with_sdk(auth_session, SAMPLE_DOCUMENTS)
        for _doc_id, urn in created_docs:
            self.created_urns.append(urn)

        assert len(created_docs) == len(SAMPLE_DOCUMENTS)
        doc_ids = [doc_id for doc_id, _ in created_docs]
        urns = [urn for _, urn in created_docs]

        # Step 2: Generate embeddings via the classical provider (config from server)
        recipe_path = create_ingestion_recipe(auth_session, doc_ids, tmp_path)
        run_ingestion(auth_session, recipe_path)
        wait_for_writes_to_sync(mcp_only=True)

        # Step 3: Poll the semantic index with the lexical query used below until the
        # document it must match is visible (or the deadline passes), then verify
        # aspects. The other two documents share no words with the query and are only
        # asserted to rank below it, which "not returned" satisfies.
        test_query = "data access request process"
        result = _wait_for_semantic_index(
            auth_session, test_query, [f"urn:li:document:{doc_ids[1]}"]
        )

        # Step 3b: Verify semanticContent exists for each document
        logger.info("Verifying semanticContent aspects...")
        for urn in urns:
            try:
                _verify_classical_embedding(auth_session, urn)
                logger.info(f"  ✓ {urn}: classical embedding verified")
            except Exception as e:
                pytest.fail(f"semanticContent verification failed for {urn}: {e}")

        # Step 4: Assert on the lexical query built from the target document's title words
        logger.info(f"Checking semantic search results for: '{test_query}'")

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
        getting_started_urn = our_doc_urns[0]  # "Getting Started with DataHub"
        access_doc_urn = our_doc_urns[1]  # "Data Access Request Process"
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

        assert access_doc_urn in result_urns, (
            f"Document {access_doc_urn} not found in top-{len(result_urns)} results for "
            f"'{test_query}'. Embeddings may not have been indexed yet."
        )
        assert ranks[access_doc_urn] < ranks[getting_started_urn], (
            f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should rank "
            f"above 'Getting Started with DataHub' (rank {ranks[getting_started_urn]})"
        )
        assert ranks[access_doc_urn] < ranks[churn_doc_urn], (
            f"'Data Access Request Process' (rank {ranks[access_doc_urn] + 1}) should rank "
            f"above 'Churn Prediction' (rank {ranks[churn_doc_urn]})"
        )

        logger.info(
            f"✓ 'Data Access Request Process' ranked #{ranks[access_doc_urn] + 1} "
            f"for the lexical query '{test_query}'"
        )
        logger.info("✓ End-to-end classical embedding semantic search test passed!")
