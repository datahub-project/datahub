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
    """Test that embedding failures are reported as warnings in inline mode."""
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

    # Mock _generate_embeddings to raise an exception
    with patch.object(
        source, "_generate_embeddings", side_effect=Exception("AWS credentials expired")
    ):
        # Process elements inline
        list(source.process_elements_inline(document_urn, elements))

    # Verify embedding failure was tracked
    assert source.report.num_embedding_failures == 1
    assert len(source.report.embedding_failures) == 1
    assert "AWS credentials expired" in source.report.embedding_failures[0]
    assert document_urn in source.report.embedding_failures[0]

    # Verify warning was reported
    assert len(source.report.warnings) > 0

    # Verify document was still processed (not failed)
    assert source.report.num_documents_processed == 1
    assert source.report.num_documents_with_embeddings == 0


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
        # Process elements inline
        list(source.process_elements_inline(document_urn, elements))

    # Verify embedding success was tracked
    assert source.report.num_documents_with_embeddings == 1
    assert source.report.num_embedding_failures == 0
    assert len(source.report.embedding_failures) == 0

    # Verify no warnings
    assert len(source.report.warnings) == 0

    # Verify document was processed
    assert source.report.num_documents_processed == 1


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
    """Test that documents are still marked as processed when embeddings fail."""
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

    # Mock _generate_embeddings to raise an exception
    with patch.object(
        source, "_generate_embeddings", side_effect=Exception("Service unavailable")
    ):
        # Process elements inline
        workunits = list(source.process_elements_inline(document_urn, elements))

    # Verify no work units were emitted (no embeddings = no SemanticContent aspect)
    assert len(workunits) == 0

    # Verify embedding failure was tracked
    assert source.report.num_embedding_failures == 1
    assert source.report.num_documents_with_embeddings == 0

    # Verify warning was reported
    assert len(source.report.warnings) >= 1

    # Verify document was still processed (not failed)
    assert source.report.num_documents_processed == 1
    assert source.report.num_embeddings_generated == 0


def test_multiple_embedding_failures(pipeline_context, chunking_config):
    """Test that multiple embedding failures are tracked correctly."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    documents = [
        ("urn:li:document:(test,doc1,PROD)", [{"type": "Title", "text": "Doc 1"}]),
        ("urn:li:document:(test,doc2,PROD)", [{"type": "Title", "text": "Doc 2"}]),
        ("urn:li:document:(test,doc3,PROD)", [{"type": "Title", "text": "Doc 3"}]),
    ]

    # Mock _generate_embeddings to raise an exception for all documents
    with patch.object(
        source, "_generate_embeddings", side_effect=Exception("Connection timeout")
    ):
        for doc_urn, elements in documents:
            list(source.process_elements_inline(doc_urn, elements))

    # Verify all failures were tracked
    assert source.report.num_embedding_failures == 3
    assert len(source.report.embedding_failures) == 3
    assert source.report.num_documents_with_embeddings == 0

    # Verify warnings were reported (note: structured logs may deduplicate by title+message)
    # At minimum, we should have at least 1 warning entry
    assert len(source.report.warnings) >= 1

    # Verify all documents were still processed
    assert source.report.num_documents_processed == 3


def test_mixed_success_and_failure(pipeline_context, chunking_config):
    """Test scenario with both successful and failed embedding generations."""
    # Initialize source in inline mode
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=chunking_config,
        standalone=False,
        graph=None,
    )

    documents = [
        ("urn:li:document:(test,doc1,PROD)", [{"type": "Title", "text": "Doc 1"}]),
        ("urn:li:document:(test,doc2,PROD)", [{"type": "Title", "text": "Doc 2"}]),
        ("urn:li:document:(test,doc3,PROD)", [{"type": "Title", "text": "Doc 3"}]),
    ]

    call_count = 0

    def mock_generate_embeddings(chunks):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            # Second call fails
            raise Exception("Temporary failure")
        return [[0.1, 0.2, 0.3]]

    with patch.object(
        source, "_generate_embeddings", side_effect=mock_generate_embeddings
    ):
        for doc_urn, elements in documents:
            list(source.process_elements_inline(doc_urn, elements))

    # Verify statistics
    assert source.report.num_documents_with_embeddings == 2
    assert source.report.num_embedding_failures == 1
    assert len(source.report.embedding_failures) == 1
    assert len(source.report.warnings) == 1

    # Verify all documents were processed
    assert source.report.num_documents_processed == 3


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
