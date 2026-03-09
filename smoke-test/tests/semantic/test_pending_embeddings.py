"""
Smoke tests for the two-phase pending-embeddings pipeline.

Phase 1 — simulates what Notion/Confluence does when no embedding provider is
           configured: emits SemanticContent with {"__pending__": ...} and
           empty (zero-length) vectors.

Phase 2 — DataHubDocumentsSource (with embedding model) detects the
           semanticContent MCL event, reads the pending chunks, generates
           real embeddings, and writes back SemanticContent with the actual
           model key (no __pending__ key).

These tests are DISABLED by default. To enable, set:
    ENABLE_SEMANTIC_SEARCH_TESTS=true

Prerequisites:
- DataHub running with semantic search enabled
- Kafka accessible (MCL events published)
- AWS credentials configured for embedding generation
"""

import logging
import os
import time
import uuid
from typing import Any

import pytest

from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)

SEMANTIC_SEARCH_ENABLED = (
    os.environ.get("ENABLE_SEMANTIC_SEARCH_TESTS", "false").lower() == "true"
)

PENDING_CHUNKS_KEY = "__pending__"

# How long to wait for the DataHubDocumentsSource (event mode) to process the MCL
EVENT_PROCESSING_WAIT_SECONDS = int(
    os.environ.get("PENDING_EMBEDDINGS_WAIT_SECONDS", "30")
)


def _unique_id(prefix: str = "pending-test") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _emit_pending_semantic_content(
    auth_session, doc_urn: str, chunks: list[dict[str, Any]]
) -> None:
    """Phase 1 helper: emit SemanticContent with __pending__ sentinel via REST emitter.

    This simulates what a source (Notion, Confluence) emits when it has no
    embedding provider configured — chunks stored, vectors empty.
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        EmbeddingChunkClass,
        EmbeddingModelDataClass,
        SemanticContentClass,
    )

    embedding_chunks = [
        EmbeddingChunkClass(
            position=i,
            vector=[],  # empty = pending sentinel
            text=c["text"],
            characterOffset=c.get("characterOffset", 0),
            characterLength=c.get("characterLength", len(c["text"])),
        )
        for i, c in enumerate(chunks)
    ]

    pending_model_data = EmbeddingModelDataClass(
        modelVersion=PENDING_CHUNKS_KEY,
        generatedAt=int(time.time() * 1000),
        chunkingStrategy="basic",
        totalChunks=len(chunks),
        totalTokens=0,
        chunks=embedding_chunks,
    )

    semantic_content = SemanticContentClass(
        embeddings={PENDING_CHUNKS_KEY: pending_model_data}
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=doc_urn,
        aspect=semantic_content,
    )

    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )
    emitter.emit(mcp)
    emitter.close()
    logger.info(f"Emitted pending SemanticContent for {doc_urn}")


def _get_semantic_content_aspect(auth_session, doc_urn: str) -> dict[str, Any]:
    """Fetch the raw semanticContent aspect dict for a document."""
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.graph.config import DatahubClientConfig
    from datahub.metadata.schema_classes import SemanticContentClass

    graph = DataHubGraph(
        DatahubClientConfig(
            server=auth_session.gms_url(),
            token=auth_session.gms_token(),
        )
    )
    aspect = graph.get_aspect(doc_urn, SemanticContentClass)
    graph.close()

    if aspect is None:
        return {}
    return {"embeddings": {k: v.to_obj() for k, v in (aspect.embeddings or {}).items()}}


def _run_datahub_documents_event_mode(
    auth_session, doc_urn: str, lookback_days: int = 1
) -> None:
    """Run DataHubDocumentsSource in event mode to process pending embeddings.

    Uses a short lookback window (default 1 day) so it picks up the recent
    semanticContent MCL we just emitted.
    """

    from datahub.ingestion.run.pipeline import Pipeline

    recipe: dict[str, Any] = {
        "pipeline_name": "smoke-test-pending-embeddings",
        "source": {
            "type": "datahub-documents",
            "config": {
                "datahub": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
                "document_urns": [doc_urn],
                "event_mode": {
                    "enabled": True,
                    "lookback_days": lookback_days,
                    "idle_timeout_seconds": EVENT_PROCESSING_WAIT_SECONDS,
                    "poll_timeout_seconds": 5,
                },
                "incremental": {"enabled": False},
                # No explicit embedding config — auto-loaded from server
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

    logger.info(
        f"Running DataHubDocumentsSource in event mode (lookback={lookback_days}d) ..."
    )
    pipeline = Pipeline.create(recipe)
    pipeline.run()
    pipeline.raise_from_status()
    logger.info("DataHubDocumentsSource event-mode run complete")


def _create_test_document(auth_session, doc_id: str, text: str) -> str:
    """Create a minimal Document entity and return its URN."""
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.sdk.document import Document

    doc = Document.create_document(
        id=doc_id,
        title=f"Pending Embeddings Test — {doc_id}",
        text=text,
        subtype="guide",
    )

    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )
    for mcp in doc.as_mcps():
        emitter.emit(mcp)
    emitter.close()

    urn = f"urn:li:document:{doc_id}"
    wait_for_writes_to_sync(mae_only=True)
    logger.info(f"Created test document: {urn}")
    return urn


def _create_external_test_document(
    auth_session, doc_id: str, platform: str = "notion"
) -> str:
    """Create an EXTERNAL Document entity (sourceType=EXTERNAL) and return its URN.

    EXTERNAL documents are those ingested by third-party sources (Notion, Confluence,
    etc.).  They are skipped by DataHubDocumentsSource's Pass 1 (NATIVE re-chunking)
    but are picked up by Pass 2 (__pending__ chunk resolution).
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import DataPlatformInstanceClass
    from datahub.sdk.document import Document

    doc = Document.create_external_document(
        id=doc_id,
        title=f"Pending Embeddings Batch Test — {doc_id}",
        platform=platform,
        external_url=f"https://{platform}.so/pages/{doc_id}",
        external_id=doc_id,
        subtype="guide",
    )

    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )
    for mcp in doc.as_mcps():
        emitter.emit(mcp)

    # Emit dataPlatformInstance so OpenSearch indexes the platform field,
    # allowing platform_filter to match this document by name.
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=f"urn:li:document:{doc_id}",
            aspect=DataPlatformInstanceClass(
                platform=f"urn:li:dataPlatform:{platform}",
            ),
        )
    )
    emitter.close()

    urn = f"urn:li:document:{doc_id}"
    wait_for_writes_to_sync(mae_only=True)
    logger.info(f"Created external test document: {urn} (platform={platform})")
    return urn


def _run_datahub_documents_batch_mode(
    auth_session, doc_urn: str, platform_filter: list[str] | None = None
) -> None:
    """Run DataHubDocumentsSource in batch mode (default) to resolve pending chunks.

    Pass 2 of batch mode calls _fetch_external_document_urns() and then
    _process_document_for_pending_chunks() for each EXTERNAL URN found.
    """
    from datahub.ingestion.run.pipeline import Pipeline

    recipe: dict = {
        "pipeline_name": "smoke-test-pending-embeddings-batch",
        "source": {
            "type": "datahub-documents",
            "config": {
                "datahub": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
                "document_urns": [doc_urn],
                "platform_filter": platform_filter,
                "incremental": {"enabled": False},
                # No explicit embedding config — auto-loaded from server
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

    logger.info("Running DataHubDocumentsSource in batch mode ...")
    pipeline = Pipeline.create(recipe)
    pipeline.run()
    pipeline.raise_from_status()
    logger.info("DataHubDocumentsSource batch-mode run complete")


def _delete_document(auth_session, urn: str) -> None:
    """Best-effort document deletion for cleanup."""
    try:
        from datahub.ingestion.graph.client import DataHubGraph
        from datahub.ingestion.graph.config import DatahubClientConfig

        graph = DataHubGraph(
            DatahubClientConfig(
                server=auth_session.gms_url(),
                token=auth_session.gms_token(),
            )
        )
        graph.hard_delete_entity(urn=urn)
        graph.close()
        logger.info(f"Deleted document: {urn}")
    except Exception as e:
        logger.warning(f"Failed to delete {urn}: {e}")


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestPendingEmbeddingsTwoPhase:
    """
    Smoke tests for the two-phase __pending__ embedding flow.

    Phase 1: emit SemanticContent with __pending__ sentinel (no real vectors).
    Phase 2: DataHubDocumentsSource (event mode) detects the MCL event and
             generates real embeddings, replacing __pending__ with the model key.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        self.auth_session = auth_session
        self.created_urns: list[str] = []
        yield
        for urn in self.created_urns:
            _delete_document(auth_session, urn)

    def test_phase1_pending_sentinel_stored_correctly(self, auth_session):
        """Phase 1: emitting __pending__ SemanticContent is stored as-is."""
        doc_id = _unique_id("phase1")
        doc_urn = _create_test_document(
            auth_session,
            doc_id,
            "DataHub is a modern metadata platform for data discovery.",
        )
        self.created_urns.append(doc_urn)

        chunks = [
            {"text": "DataHub is a modern metadata platform.", "characterOffset": 0},
            {"text": "It helps teams discover and govern data.", "characterOffset": 40},
        ]
        _emit_pending_semantic_content(auth_session, doc_urn, chunks)
        wait_for_writes_to_sync(mae_only=True)

        aspect = _get_semantic_content_aspect(auth_session, doc_urn)
        embeddings = aspect.get("embeddings", {})

        assert PENDING_CHUNKS_KEY in embeddings, (
            f"Expected __pending__ key in stored semanticContent, got keys: {list(embeddings.keys())}"
        )

        pending = embeddings[PENDING_CHUNKS_KEY]
        stored_chunks = pending.get("chunks", [])
        assert len(stored_chunks) == 2, (
            f"Expected 2 stored chunks, got {len(stored_chunks)}"
        )

        for chunk in stored_chunks:
            assert chunk.get("vector") == [], "Pending chunk should have empty vector"
            assert chunk.get("text"), "Pending chunk should carry text"

        logger.info(
            f"✓ Phase 1 verified: __pending__ stored with {len(stored_chunks)} chunks"
        )

    def test_phase2_event_mode_resolves_pending_to_real_embeddings(self, auth_session):
        """Full two-phase round-trip: pending → real embeddings via event mode."""
        doc_id = _unique_id("phase2")
        doc_urn = _create_test_document(
            auth_session,
            doc_id,
            "Data governance requires tracking data lineage and ownership.",
        )
        self.created_urns.append(doc_urn)

        # --- Phase 1: emit pending chunks ---
        chunks = [
            {
                "text": "Data governance requires tracking data lineage.",
                "characterOffset": 0,
            },
            {
                "text": "Ownership metadata helps teams understand accountability.",
                "characterOffset": 47,
            },
        ]
        _emit_pending_semantic_content(auth_session, doc_urn, chunks)
        wait_for_writes_to_sync(mae_only=True)

        # Confirm __pending__ is present before Phase 2
        pre_aspect = _get_semantic_content_aspect(auth_session, doc_urn)
        assert PENDING_CHUNKS_KEY in pre_aspect.get("embeddings", {}), (
            "Expected __pending__ key before Phase 2 run"
        )
        logger.info("✓ Phase 1 complete: __pending__ confirmed in aspect")

        # --- Phase 2: run DataHubDocumentsSource in event mode ---
        _run_datahub_documents_event_mode(auth_session, doc_urn, lookback_days=1)
        wait_for_writes_to_sync(mae_only=True)

        # Verify __pending__ is gone and replaced with real embeddings
        post_aspect = _get_semantic_content_aspect(auth_session, doc_urn)
        embeddings = post_aspect.get("embeddings", {})

        assert PENDING_CHUNKS_KEY not in embeddings, (
            f"__pending__ key should have been replaced after Phase 2. "
            f"Current keys: {list(embeddings.keys())}"
        )
        assert len(embeddings) > 0, (
            "Expected at least one real embedding key after Phase 2"
        )

        model_key = next(iter(embeddings))
        model_data = embeddings[model_key]
        real_chunks = model_data.get("chunks", [])

        assert len(real_chunks) > 0, "Expected chunks in real embedding"
        for chunk in real_chunks:
            vector = chunk.get("vector", [])
            assert isinstance(vector, list) and len(vector) > 0, (
                f"Expected non-empty vector in chunk after Phase 2, got: {vector}"
            )

        logger.info(
            f"✓ Phase 2 complete: __pending__ replaced by '{model_key}' "
            f"with {len(real_chunks)} chunks of dimension {len(real_chunks[0]['vector'])}"
        )

    def test_phase2_batch_mode_resolves_pending_chunks(self, auth_session):
        """Batch mode Pass 2 resolves __pending__ chunks for EXTERNAL documents.

        This exercises the second pass added to _process_batch_mode():
        _fetch_external_document_urns() finds the EXTERNAL doc, then
        _process_document_for_pending_chunks() generates real embeddings.
        """
        doc_id = _unique_id("phase2-batch")
        doc_urn = _create_external_test_document(
            auth_session, doc_id, platform="notion"
        )
        self.created_urns.append(doc_urn)

        # --- Phase 1: emit pending chunks (simulates what Notion source does) ---
        chunks = [
            {
                "text": "DataHub enables data discovery at scale.",
                "characterOffset": 0,
            },
            {
                "text": "Teams use it to find and govern data assets.",
                "characterOffset": 40,
            },
        ]
        _emit_pending_semantic_content(auth_session, doc_urn, chunks)
        wait_for_writes_to_sync(mae_only=True)

        pre_aspect = _get_semantic_content_aspect(auth_session, doc_urn)
        assert PENDING_CHUNKS_KEY in pre_aspect.get("embeddings", {}), (
            "Expected __pending__ key before batch mode run"
        )
        logger.info("✓ Phase 1 complete: __pending__ confirmed in aspect")

        # --- Phase 2: run DataHubDocumentsSource in batch mode ---
        # platform_filter=["notion"] exercises client-side platform filtering:
        # _fetch_external_document_urns uses sourceType=EXTERNAL as the server-side
        # filter, then _should_process_by_source_type checks dataPlatformInstance.
        _run_datahub_documents_batch_mode(
            auth_session, doc_urn, platform_filter=["notion"]
        )
        wait_for_writes_to_sync(mae_only=True)

        post_aspect = _get_semantic_content_aspect(auth_session, doc_urn)
        embeddings = post_aspect.get("embeddings", {})

        assert PENDING_CHUNKS_KEY not in embeddings, (
            f"__pending__ key should have been replaced after batch mode Pass 2. "
            f"Current keys: {list(embeddings.keys())}"
        )
        assert len(embeddings) > 0, (
            "Expected at least one real embedding key after batch mode Pass 2"
        )

        model_key = next(iter(embeddings))
        real_chunks = embeddings[model_key].get("chunks", [])
        assert len(real_chunks) > 0, "Expected chunks in real embedding"
        for chunk in real_chunks:
            vector = chunk.get("vector", [])
            assert isinstance(vector, list) and len(vector) > 0, (
                f"Expected non-empty vector after batch mode Pass 2, got: {vector}"
            )

        logger.info(
            f"✓ Batch mode Phase 2 complete: __pending__ replaced by '{model_key}' "
            f"with {len(real_chunks)} chunks of dimension {len(real_chunks[0]['vector'])}"
        )
