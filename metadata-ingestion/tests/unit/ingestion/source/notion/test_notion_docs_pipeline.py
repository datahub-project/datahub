"""Tests for the two-phase embedding pipeline via __pending__ sentinel.

Phase 1: Notion (or any source) processes a document without an embedding provider,
         emitting SemanticContent with embeddings={"__pending__": ...} and empty vectors.
Phase 2: DataHubDocumentsSource detects the __pending__ key, generates real embeddings,
         and writes back SemanticContent with the actual model key and real vectors.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    PENDING_CHUNKS_KEY,
    DocumentChunkingSource,
)
from datahub.metadata.schema_classes import SemanticContentClass


@pytest.fixture
def pipeline_context():
    ctx = MagicMock(spec=PipelineContext)
    ctx.pipeline_name = "test_pipeline"
    return ctx


@pytest.fixture
def no_embedding_config():
    return DocumentChunkingSourceConfig(
        chunking=ChunkingConfig(strategy="basic"),
    )


@pytest.fixture
def with_embedding_config():
    return DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )


def _make_source_no_embedding(ctx, config):
    """Create DocumentChunkingSource with no embedding model, bypassing auto-resolution."""
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source.DocumentChunkingSource.resolve_embedding_config",
        return_value=config.embedding,
    ):
        source = DocumentChunkingSource(
            ctx=ctx,
            config=config,
            standalone=False,
            graph=None,
        )
    # Ensure no embedding model is set (auto-resolve may have set one from defaults)
    source.embedding_model = None
    return source


DOC_URN = "urn:li:document:(urn:li:dataPlatform:notion,page1,PROD)"

ELEMENTS = [
    {"type": "Title", "text": "Test Page Title"},
    {"type": "NarrativeText", "text": "Page content for semantic search testing."},
]


def test_phase1_no_embedding_emits_pending_sentinel(
    pipeline_context, no_embedding_config
):
    """Phase 1: without embedding model, process_elements_inline emits pending sentinel."""
    source = _make_source_no_embedding(pipeline_context, no_embedding_config)

    workunits = list(source.process_elements_inline(DOC_URN, ELEMENTS))

    assert len(workunits) == 1, "Expected exactly one workunit for pending chunks"
    aspect = workunits[0].get_aspect_of_type(SemanticContentClass)
    assert aspect is not None

    assert PENDING_CHUNKS_KEY in aspect.embeddings
    pending = aspect.embeddings[PENDING_CHUNKS_KEY]

    assert len(pending.chunks) > 0
    for chunk in pending.chunks:
        assert chunk.vector == [], "Pending chunk must have empty vector"
        assert chunk.text, "Pending chunk must carry text"

    assert pending.modelVersion == PENDING_CHUNKS_KEY
    assert source.report.num_documents_processed == 1


def test_phase1_pending_chunk_text_preserved(pipeline_context, no_embedding_config):
    """Chunk text must survive round-trip through pending storage."""
    source = _make_source_no_embedding(pipeline_context, no_embedding_config)

    single_element = [{"type": "NarrativeText", "text": "Hello, semantic search!"}]
    workunits = list(source.process_elements_inline(DOC_URN, single_element))

    aspect = workunits[0].get_aspect_of_type(SemanticContentClass)
    assert aspect is not None
    pending = aspect.embeddings[PENDING_CHUNKS_KEY]
    combined_text = " ".join(c.text for c in pending.chunks if c.text)
    assert "Hello, semantic search!" in combined_text


def test_phase2_with_embedding_does_not_emit_pending(
    pipeline_context, with_embedding_config
):
    """Phase 2: when embedding model is configured, no __pending__ key is emitted."""
    mock_embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

    # The startup probe fires first (1-chunk call), then the per-document call follows.
    # Both must be mocked so construction and processing succeed.
    with patch.object(
        DocumentChunkingSource,
        "_generate_embeddings",
        side_effect=[[[0.0]], mock_embeddings],
    ):
        source = DocumentChunkingSource(
            ctx=pipeline_context,
            config=with_embedding_config,
            standalone=False,
            graph=None,
        )
        workunits = list(source.process_elements_inline(DOC_URN, ELEMENTS))

    assert len(workunits) == 1
    aspect = workunits[0].get_aspect_of_type(SemanticContentClass)
    assert aspect is not None

    assert PENDING_CHUNKS_KEY not in aspect.embeddings
    for model_data in aspect.embeddings.values():
        for chunk in model_data.chunks:
            assert chunk.vector != [], (
                "Real embedding chunks must have non-empty vectors"
            )


def test_embedding_failure_falls_back_to_pending(
    pipeline_context, with_embedding_config
):
    """Startup probe failure disables embedding for the entire run.

    When the probe fires at __init__ and the embedding API is unreachable
    (no Bedrock access, expired credentials, network restrictions, etc.),
    embedding_model is set to None so every document goes straight to
    __pending__ without a per-document retry.
    """
    with patch.object(
        DocumentChunkingSource,
        "_generate_embeddings",
        side_effect=RuntimeError("Connection refused"),
    ):
        source = DocumentChunkingSource(
            ctx=pipeline_context,
            config=with_embedding_config,
            standalone=False,
            graph=None,
        )
        workunits = list(source.process_elements_inline(DOC_URN, ELEMENTS))

    assert len(workunits) == 1, "Expected one pending workunit after probe failure"
    aspect = workunits[0].get_aspect_of_type(SemanticContentClass)
    assert aspect is not None

    assert PENDING_CHUNKS_KEY in aspect.embeddings, (
        "Probe failure should fall back to __pending__ sentinel"
    )
    for chunk in aspect.embeddings[PENDING_CHUNKS_KEY].chunks:
        assert chunk.vector == [], "Pending chunks must have empty vectors"
        assert chunk.text, "Pending chunks must carry text for later embedding"

    # Probe failure is recorded on the report; no per-document failures occur.
    assert source.report.embedding_probe_failed
    assert source.report.num_embedding_failures == 0


def test_phase1_document_limit_respected(pipeline_context):
    """Document limit is still enforced when emitting pending chunks.

    The limit check fires after each document is processed. With max_documents=1,
    processing the first document increments the counter to 1 and raises RuntimeError
    (matching the same behaviour as the embedding path).
    """
    config = DocumentChunkingSourceConfig(
        chunking=ChunkingConfig(strategy="basic"),
        max_documents=1,
    )
    source = _make_source_no_embedding(pipeline_context, config)

    # Processing the 1st document succeeds, then the limit check raises
    with pytest.raises(RuntimeError, match="Document limit"):
        list(source.process_elements_inline(DOC_URN, ELEMENTS))

    assert source.report.num_documents_processed == 1
    assert source.report.num_documents_limit_reached
