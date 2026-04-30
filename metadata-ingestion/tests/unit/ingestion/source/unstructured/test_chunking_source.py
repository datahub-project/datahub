"""Unit tests for DocumentChunkingSource embedding failure reporting."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)


@pytest.fixture
def pipeline_context():
    """Create a mock pipeline context."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.pipeline_name = "test_pipeline"
    return ctx


@pytest.fixture
def chunking_config():
    """Create a basic chunking config with embedding enabled."""
    return DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )


def test_embedding_failure_reporting_inline_mode(pipeline_context, chunking_config):
    """Test that embedding failures propagate as exceptions in inline mode."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [
        {"type": "Title", "text": "Test Title"},
        {"type": "NarrativeText", "text": "Test content"},
    ]

    # In inline mode, embedding failures raise — the caller decides how to handle them
    with (
        patch.object(
            source,
            "_generate_embeddings",
            side_effect=Exception("AWS credentials expired"),
        ),
        pytest.raises(Exception, match="AWS credentials expired"),
    ):
        list(source.process_elements_inline(document_urn, elements))


def test_embedding_success_reporting_inline_mode(pipeline_context, chunking_config):
    """Test that successful embedding generation is tracked."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [
        {"type": "Title", "text": "Test Title"},
        {"type": "NarrativeText", "text": "Test content"},
    ]

    # Mock successful embedding generation
    mock_embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    with patch.object(source, "_generate_embeddings", return_value=mock_embeddings):
        list(source.process_elements_inline(document_urn, elements))

    # Verify document was processed and embeddings counted
    assert source.report.num_documents_processed == 1
    assert source.report.num_embeddings_generated == 2


def test_embedding_failure_batch_mode(pipeline_context, chunking_config):
    """Test that embedding failures are reported as warnings in batch mode."""
    # Initialize source in standalone batch mode
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )

    # Mock a document
    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }

    # Mock _generate_embeddings to raise an exception
    with patch.object(
        source, "_generate_embeddings", side_effect=Exception("AWS credentials expired")
    ):
        # Process single document
        list(source._process_single_document(doc))

    # Verify embedding failure was tracked
    assert source.report.num_embedding_failures == 1
    assert len(source.report.embedding_failures) == 1
    assert "AWS credentials expired" in source.report.embedding_failures[0]

    # Verify warning was reported
    assert len(source.report.warnings) > 0

    # Verify document was still processed
    assert source.report.num_documents_processed == 1


def test_document_processed_without_embeddings_on_failure(
    pipeline_context, chunking_config
):
    """Test that embedding failures propagate as exceptions in inline mode."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [
        {"type": "Title", "text": "Test Title"},
        {"type": "NarrativeText", "text": "Test content"},
    ]

    # In inline mode, failures propagate so the caller can decide how to handle them
    with (
        patch.object(
            source, "_generate_embeddings", side_effect=Exception("Service unavailable")
        ),
        pytest.raises(Exception, match="Service unavailable"),
    ):
        list(source.process_elements_inline(document_urn, elements))


def test_multiple_embedding_failures(pipeline_context, chunking_config):
    """Test that embedding failures propagate in inline mode."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [{"type": "Title", "text": "Doc 1"}]

    # Each call raises — verified per-call
    with (
        patch.object(
            source, "_generate_embeddings", side_effect=Exception("Connection timeout")
        ),
        pytest.raises(Exception, match="Connection timeout"),
    ):
        list(source.process_elements_inline(document_urn, elements))


def test_mixed_success_and_failure(pipeline_context, chunking_config):
    """Test that successful calls process correctly and failures propagate."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    # Successful document
    doc1_urn = "urn:li:document:(test,doc1,PROD)"
    doc1_elements = [{"type": "Title", "text": "Doc 1"}]
    mock_embeddings = [[0.1, 0.2, 0.3]]

    with patch.object(source, "_generate_embeddings", return_value=mock_embeddings):
        list(source.process_elements_inline(doc1_urn, doc1_elements))

    assert source.report.num_documents_processed == 1

    # Failing document raises
    doc2_urn = "urn:li:document:(test,doc2,PROD)"
    doc2_elements = [{"type": "Title", "text": "Doc 2"}]

    with (
        patch.object(
            source, "_generate_embeddings", side_effect=Exception("Temporary failure")
        ),
        pytest.raises(Exception, match="Temporary failure"),
    ):
        list(source.process_elements_inline(doc2_urn, doc2_elements))


def test_embedding_success_batch_mode(pipeline_context, chunking_config):
    """Test that successful embedding generation is tracked in batch mode."""
    # Initialize source in standalone batch mode
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )

    # Mock a document
    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }

    # Mock successful embedding generation
    mock_embeddings = [[0.1, 0.2, 0.3]]
    with patch.object(source, "_generate_embeddings", return_value=mock_embeddings):
        # Process single document
        workunits = list(source._process_single_document(doc))

    # Verify work units were emitted
    assert len(workunits) > 0

    # Verify embedding success was tracked
    assert source.report.num_documents_with_embeddings == 1
    assert source.report.num_embedding_failures == 0
    assert len(source.report.embedding_failures) == 0

    # Verify no warnings
    assert len(source.report.warnings) == 0

    # Verify document was processed
    assert source.report.num_documents_processed == 1


def test_batch_mode_no_embedding_model(pipeline_context, chunking_config):
    """Test batch mode when embedding_model is None (edge case for coverage)."""
    # Initialize source in standalone batch mode
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )

    # Manually set embedding_model to None to simulate no embedding provider
    source.embedding_model = None

    # Mock a document
    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }

    # Process single document - should skip embedding generation
    workunits = list(source._process_single_document(doc))

    # Verify no work units were emitted (no embeddings = no SemanticContent aspect)
    assert len(workunits) == 0

    # Verify no embeddings were generated
    assert source.report.num_documents_with_embeddings == 0
    assert source.report.num_embedding_failures == 0
    assert source.report.num_embeddings_generated == 0

    # Verify document was still processed
    assert source.report.num_documents_processed == 1


def test_inline_mode_no_embedding_model(pipeline_context, chunking_config):
    """Test inline mode when embedding_model is None (edge case for coverage)."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    # Manually set embedding_model to None to simulate no embedding provider
    source.embedding_model = None

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [
        {"type": "Title", "text": "Test Title"},
        {"type": "NarrativeText", "text": "Test content"},
    ]

    # Process elements inline - should skip embedding generation
    workunits = list(source.process_elements_inline(document_urn, elements))

    # Verify no work units were emitted (no embeddings = no SemanticContent aspect)
    assert len(workunits) == 0

    # Verify no embeddings were generated
    assert source.report.num_documents_with_embeddings == 0
    assert source.report.num_embedding_failures == 0
    assert source.report.num_embeddings_generated == 0

    # Verify document was still processed
    assert source.report.num_documents_processed == 1


# --- Env var tests ---


def test_cohere_api_key_from_env_var(pipeline_context):
    """COHERE_API_KEY env var satisfies the API key requirement."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="cohere",
            model="embed-english-v3.0",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with patch.dict("os.environ", {"COHERE_API_KEY": "test-cohere-key"}):
        # Should not raise even though api_key is not set in config
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )
    assert source.embedding_model == "cohere/embed-english-v3.0"


def test_cohere_missing_api_key_raises(pipeline_context):
    """Missing COHERE_API_KEY env var and no config api_key raises ValueError."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="cohere",
            model="embed-english-v3.0",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="COHERE_API_KEY"),
    ):
        DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )


def test_openai_api_key_from_env_var(pipeline_context):
    """OPENAI_API_KEY env var satisfies the API key requirement."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-test-key"}):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )
    assert source.embedding_model == "openai/text-embedding-3-small"


def test_openai_missing_api_key_raises(pipeline_context):
    """Missing OPENAI_API_KEY env var and no config api_key raises ValueError."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="OPENAI_API_KEY"),
    ):
        DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )


def test_bedrock_requires_no_api_key(pipeline_context):
    """Bedrock provider initialises without any API key (uses AWS credential chain)."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-east-1",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    # No env vars needed — should not raise
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    assert source.embedding_model == "bedrock/cohere.embed-english-v3"


# --- max_documents limit tests ---


def test_max_documents_limit_raises_after_nth_document(
    pipeline_context, chunking_config
):
    """RuntimeError is raised after processing max_documents documents."""
    chunking_config.max_documents = 2
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )
    # Disable embedding to keep the test focused on limit logic only
    source.embedding_model = None

    elements = [{"type": "NarrativeText", "text": "Some content"}]
    dummy_chunk = [{"text": "Some content", "type": "NarrativeText"}]

    with patch.object(source, "_chunk_elements", return_value=dummy_chunk):
        # First document — should succeed
        list(source.process_elements_inline("urn:li:document:doc1", elements))
        assert source.report.num_documents_processed == 1
        assert source.report.num_documents_limit_reached is False

        # Second document — hits the limit
        with pytest.raises(RuntimeError, match="Document limit of 2 reached"):
            list(source.process_elements_inline("urn:li:document:doc2", elements))

    assert source.report.num_documents_processed == 2
    assert source.report.num_documents_limit_reached is True


def test_max_documents_minus_one_disables_limit(pipeline_context, chunking_config):
    """Setting max_documents=-1 disables the limit entirely."""
    chunking_config.max_documents = -1
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )
    source.embedding_model = None

    elements = [{"type": "NarrativeText", "text": "Some content"}]
    dummy_chunk = [{"text": "Some content", "type": "NarrativeText"}]

    with patch.object(source, "_chunk_elements", return_value=dummy_chunk):
        for i in range(5):
            list(source.process_elements_inline(f"urn:li:document:doc{i}", elements))

    assert source.report.num_documents_processed == 5
    assert source.report.num_documents_limit_reached is False


# --- Raw chunks (emit_chunks_without_embeddings) tests ---


def test_emit_raw_chunks_called_when_no_embedding_and_flag_true(
    pipeline_context, chunking_config
):
    """process_elements_inline calls _emit_raw_chunks when emit_chunks_without_embeddings=True."""
    chunking_config.emit_chunks_without_embeddings = True
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )
    # Simulate no embedding provider
    source.embedding_model = None

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [{"type": "NarrativeText", "text": "Hello world"}]
    dummy_chunk = [{"text": "Hello world"}]

    with (
        patch.object(source, "_chunk_elements", return_value=dummy_chunk),
        patch.object(source, "_emit_raw_chunks", return_value=iter([])) as mock_emit,
    ):
        list(source.process_elements_inline(document_urn, elements))

    mock_emit.assert_called_once_with(document_urn, dummy_chunk)
    assert source.report.num_documents_processed == 1


def test_no_raw_chunks_when_flag_false(pipeline_context, chunking_config):
    """process_elements_inline emits nothing when emit_chunks_without_embeddings=False (default)."""
    assert not chunking_config.emit_chunks_without_embeddings

    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )
    # Simulate no embedding provider
    source.embedding_model = None

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [{"type": "NarrativeText", "text": "Hello world"}]
    dummy_chunk = [{"text": "Hello world"}]

    with (
        patch.object(source, "_chunk_elements", return_value=dummy_chunk),
        patch.object(source, "_emit_raw_chunks") as mock_emit,
    ):
        workunits = list(source.process_elements_inline(document_urn, elements))

    mock_emit.assert_not_called()
    assert len(workunits) == 0
    assert source.report.num_documents_processed == 1


def test_real_embeddings_take_precedence_over_raw_chunks(
    pipeline_context, chunking_config
):
    """When embedding model is set, real embeddings are emitted (not raw chunks)."""
    chunking_config.emit_chunks_without_embeddings = (
        True  # set True but should be ignored
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )
    assert source.embedding_model is not None

    document_urn = "urn:li:document:(test,doc1,PROD)"
    elements = [{"type": "NarrativeText", "text": "Hello world"}]
    mock_embeddings = [[0.1, 0.2, 0.3]]

    with (
        patch.object(source, "_generate_embeddings", return_value=mock_embeddings),
        patch.object(source, "_emit_raw_chunks") as mock_raw,
        patch.object(
            source, "_emit_semantic_content", return_value=iter([])
        ) as mock_embed,
    ):
        list(source.process_elements_inline(document_urn, elements))

    mock_embed.assert_called_once()
    mock_raw.assert_not_called()


# --- DataHubDocumentsSource helper method tests ---


def test_aspect_has_raw_chunks_detects_presence():
    """_aspect_has_raw_chunks returns True when rawChunks key is present and non-null."""
    from unittest.mock import MagicMock

    from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
        DataHubDocumentsSource,
    )

    # Create a minimal instance with mocked dependencies
    source = MagicMock(spec=DataHubDocumentsSource)
    # Call the actual method (unbound)
    result = DataHubDocumentsSource._aspect_has_raw_chunks(
        source, {"rawChunks": {"totalChunks": 2, "chunks": []}}
    )
    assert result is True


def test_aspect_has_raw_chunks_returns_false_for_null():
    """_aspect_has_raw_chunks returns False when rawChunks is null."""
    from unittest.mock import MagicMock

    from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
        DataHubDocumentsSource,
    )

    source = MagicMock(spec=DataHubDocumentsSource)
    result = DataHubDocumentsSource._aspect_has_raw_chunks(source, {"rawChunks": None})
    assert result is False


def test_aspect_has_raw_chunks_returns_false_when_absent():
    """_aspect_has_raw_chunks returns False when rawChunks key is missing."""
    from unittest.mock import MagicMock

    from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
        DataHubDocumentsSource,
    )

    source = MagicMock(spec=DataHubDocumentsSource)
    result = DataHubDocumentsSource._aspect_has_raw_chunks(source, {"embeddings": {}})
    assert result is False
