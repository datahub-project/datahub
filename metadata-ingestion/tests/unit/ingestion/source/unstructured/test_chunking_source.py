"""Unit tests for DocumentChunkingSource embedding failure reporting."""

import json
import math
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext

if TYPE_CHECKING:
    from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    MAX_SERIALIZED_ASPECT_BYTES,
    DocumentChunkingSource,
    SkipMarkerReadError,
    compute_source_text_sha256,
)
from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingResult,
)
from datahub.metadata.schema_classes import (
    EmbeddingChunkClass,
    EmbeddingModelDataClass,
    SemanticContentClass,
)


def _semantic_embeddings(workunit: "MetadataWorkUnit") -> dict:
    """Extract the embeddings map from a SemanticContent workunit."""
    aspect = workunit.metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, SemanticContentClass)
    return aspect.embeddings


def _mock_provider(embeddings_per_call: list[list[float]]) -> MagicMock:
    """Build a MagicMock provider whose ``embed`` returns the given embeddings."""
    provider = MagicMock()
    provider.embed.return_value = EmbeddingResult(embeddings=embeddings_per_call)
    return provider


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

    # Mock successful embedding generation: the basic strategy folds both elements
    # into one chunk, so the provider returns one vector.
    mock_embeddings = [[0.1, 0.2, 0.3]]
    with patch.object(source, "_generate_embeddings", return_value=mock_embeddings):
        list(source.process_elements_inline(document_urn, elements))

    # Verify document was processed and embeddings counted
    assert source.report.num_documents_processed == 1
    assert source.report.num_embeddings_generated == 1


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


def test_vertex_ai_provider_initialization(pipeline_context):
    """vertex_ai provider should set embedding_model with vertex_ai/ prefix."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="vertex_ai",
            model="gemini-embedding-001",
            model_embedding_key="gemini_embedding_001",
            vertex_project_id="my-project",
            vertex_location="us-east1",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    assert source.embedding_model == "vertex_ai/gemini-embedding-001"


def test_vertex_ai_missing_project_id_raises(pipeline_context):
    """Missing vertex_project_id with local config should raise ValueError."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="vertex_ai",
            model="gemini-embedding-001",
            model_embedding_key="gemini_embedding_001",
            vertex_location="us-east1",
            # vertex_project_id intentionally omitted
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="vertex_project_id"),
    ):
        DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )


def test_vertex_ai_project_id_resolved_from_env_var(pipeline_context):
    """When vertex_project_id is omitted but VERTEX_AI_PROJECT_ID is set in env,
    source construction must succeed (validator passes) and the factory must
    resolve the env value lazily when building the provider."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="vertex_ai",
            model="gemini-embedding-001",
            model_embedding_key="gemini_embedding_001",
            vertex_location="us-east1",
            # vertex_project_id intentionally omitted — should be picked up from env
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    with patch.dict("os.environ", {"VERTEX_AI_PROJECT_ID": "env-project"}, clear=True):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )
        assert source.embedding_model == "vertex_ai/gemini-embedding-001"

        # Factory resolves the env var when actually instantiating the provider —
        # config itself stays unmodified.
        with patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai."
            "VertexAIEmbeddingProvider.__init__",
            return_value=None,
        ) as mock_init:
            create_embedding_provider(source.config.embedding)

    assert mock_init.call_args.kwargs["project_id"] == "env-project"
    # The validator must NOT mutate the config.
    assert source.config.embedding.vertex_project_id is None


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


def test_vertex_ai_provider_literal_accepted():
    """vertex_ai should be a valid provider literal in EmbeddingConfig."""
    config = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        model_embedding_key="gemini_embedding_001",
        vertex_project_id="my-gcp-project",
        vertex_location="us-east1",
        allow_local_embedding_config=True,
    )
    assert config.provider == "vertex_ai"
    assert config.vertex_project_id == "my-gcp-project"
    assert config.vertex_location == "us-east1"


def test_validate_provider_config_vertex_ai_valid():
    """_validate_provider_config returns vertex_ai/<model> when valid."""
    config = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        model_embedding_key="gemini_embedding_001",
        vertex_project_id="my-project",
        allow_local_embedding_config=True,
    )
    model_str, report = DocumentChunkingSource._validate_provider_config(config)
    assert model_str == "vertex_ai/gemini-embedding-001"
    assert report is None


def test_validate_provider_config_vertex_ai_missing_project():
    """_validate_provider_config returns CapabilityReport when project_id missing."""
    config = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        model_embedding_key="gemini_embedding_001",
        # vertex_project_id intentionally omitted
        allow_local_embedding_config=True,
    )
    with patch.dict("os.environ", {}, clear=True):
        model_str, report = DocumentChunkingSource._validate_provider_config(config)
    assert model_str is None
    assert report is not None
    assert not report.capable
    assert "vertex_project_id" in (report.failure_reason or "").lower()


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


def test_generate_embeddings_invokes_provider_with_text(pipeline_context):
    """_generate_embeddings forwards chunk text to the provider and returns its embeddings."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="vertex_ai",
            model="gemini-embedding-001",
            model_embedding_key="gemini_embedding_001",
            vertex_project_id="my-project",
            vertex_location="us-east1",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    provider = _mock_provider([[0.1, 0.2, 0.3]])
    source._provider = provider

    embeddings = source._generate_embeddings([{"text": "hello world"}])
    assert embeddings == [[0.1, 0.2, 0.3]]
    provider.embed.assert_called_once_with(["hello world"])


def test_generate_embeddings_creates_provider_from_config(pipeline_context):
    """First call to _generate_embeddings should build the provider from the config."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="cohere",
            model="embed-english-v3.0",
            api_key="test-cohere-key",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )

    fake_provider = _mock_provider([[0.1, 0.2, 0.3]])
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source.create_embedding_provider",
        return_value=fake_provider,
    ) as mock_factory:
        embeddings = source._generate_embeddings([{"text": "hello world"}])

    assert embeddings == [[0.1, 0.2, 0.3]]
    mock_factory.assert_called_once_with(source.config.embedding)
    fake_provider.embed.assert_called_once_with(["hello world"])


def test_test_embedding_capability_uses_factory_and_returns_dimension():
    """test_embedding_capability builds a provider and reports embedding dimension."""
    config = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        model_embedding_key="gemini_embedding_001",
        vertex_project_id="my-project",
        vertex_location="us-east1",
        allow_local_embedding_config=True,
    )
    fake_provider = _mock_provider([[0.1, 0.2, 0.3]])
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source.create_embedding_provider",
        return_value=fake_provider,
    ) as mock_factory:
        report = DocumentChunkingSource.test_embedding_capability(config)

    assert report.capable
    assert "dimension: 3" in (report.mitigation_message or "")
    mock_factory.assert_called_once_with(config)


# ---------------------------------------------------------------------------
# Local embedding provider tests
# ---------------------------------------------------------------------------


def _local_config(
    model: str = "nomic-embed-text", endpoint: str = ""
) -> DocumentChunkingSourceConfig:
    return DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="local",
            model=model,
            endpoint=endpoint or None,
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )


def test_local_provider_sets_embedding_model(pipeline_context):
    """Local provider sets embedding_model to 'openai/<model>' (mirrors prior behaviour)."""
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=_local_config("nomic-embed-text"),
        standalone=False,
        graph=None,
    )
    assert source.embedding_model == "openai/nomic-embed-text"


def test_local_provider_api_base_strips_embeddings_suffix():
    """_resolve_local_base_url strips a /embeddings suffix."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url as _resolve_local_base_url,
    )

    assert (
        _resolve_local_base_url("http://localhost:11434/v1/embeddings")
        == "http://localhost:11434/v1"
    )


def test_local_provider_api_base_no_suffix():
    """An endpoint without /embeddings is passed through unchanged."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url as _resolve_local_base_url,
    )

    assert (
        _resolve_local_base_url("http://myserver:8080/v1") == "http://myserver:8080/v1"
    )


def test_local_provider_api_base_from_env_var():
    """Falls back to LOCAL_EMBEDDING_ENDPOINT env var when no endpoint configured."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url as _resolve_local_base_url,
    )

    with patch.dict(
        "os.environ",
        {"LOCAL_EMBEDDING_ENDPOINT": "http://envhost:11434/v1/embeddings"},
        clear=True,
    ):
        assert _resolve_local_base_url(None) == "http://envhost:11434/v1"


def test_local_provider_api_base_default_fallback():
    """Falls back to localhost:11434 when neither config nor env var is set."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url as _resolve_local_base_url,
    )

    with patch.dict("os.environ", {}, clear=True):
        assert _resolve_local_base_url(None) == "http://localhost:11434/v1"


# --- model_embedding_key derivation ---


def test_model_key_uses_explicit_model_embedding_key(pipeline_context):
    """Explicit model_embedding_key takes precedence over derivation."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-east-1",
            model_embedding_key="my_custom_key",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    source._provider = _mock_provider([[0.0] * 1024])

    with patch.object(
        source,
        "_chunk_elements",
        return_value=[{"text": "hi", "type": "NarrativeText"}],
    ):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:doc1", [{"type": "NarrativeText", "text": "hi"}]
            )
        )

    assert any(
        "my_custom_key"
        in getattr(getattr(wu.metadata, "aspect", None), "embeddings", {})
        for wu in workunits
    )


def test_model_key_normalizes_hyphens_for_local(pipeline_context):
    """Local model names have hyphens/dots replaced with underscores for the ES key."""
    config = _local_config("nomic-embed-text")
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    source._provider = _mock_provider([[0.0] * 768])

    with patch.object(
        source,
        "_chunk_elements",
        return_value=[{"text": "hi", "type": "NarrativeText"}],
    ):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:doc1", [{"type": "NarrativeText", "text": "hi"}]
            )
        )

    assert any(
        "nomic_embed_text"
        in getattr(getattr(wu.metadata, "aspect", None), "embeddings", {})
        for wu in workunits
    )


# --- _validate_provider_config for local ---


def test_validate_provider_config_local_success():
    """Local provider with a model returns the prefixed model string."""
    config = EmbeddingConfig(
        provider="local",
        model="nomic-embed-text",
        allow_local_embedding_config=True,
    )
    model, report = DocumentChunkingSource._validate_provider_config(config)
    assert model == "openai/nomic-embed-text"
    assert report is None


def test_validate_provider_config_local_no_model_fails():
    """Local provider without a model returns a CapabilityReport failure."""
    config = EmbeddingConfig(
        provider="local",
        model=None,
        allow_local_embedding_config=True,
    )
    model, report = DocumentChunkingSource._validate_provider_config(config)
    assert model is None
    assert report is not None
    assert not report.capable


# --- _validate_provider_config for classical ---


def test_validate_provider_config_classical_success():
    """Classical provider needs nothing beyond the model name."""
    config = EmbeddingConfig(
        provider="classical",
        model="hash-v1-2048",
        allow_local_embedding_config=True,
    )
    model, report = DocumentChunkingSource._validate_provider_config(config)
    assert model == "classical/hash-v1-2048"
    assert report is None


def test_validate_provider_config_classical_no_model_fails():
    config = EmbeddingConfig(
        provider="classical",
        model=None,
        allow_local_embedding_config=True,
    )
    model, report = DocumentChunkingSource._validate_provider_config(config)
    assert model is None
    assert report is not None
    assert not report.capable


# ---------------------------------------------------------------------------
# _validate_provider_init_requirements — fail-fast presence checks
# ---------------------------------------------------------------------------


def test_validate_init_requirements_cohere_requires_key():
    cfg = EmbeddingConfig(
        provider="cohere",
        model="embed-english-v3.0",
        api_key=None,
        allow_local_embedding_config=True,
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="Cohere API key is required"),
    ):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)


def test_validate_init_requirements_cohere_accepts_env_var():
    cfg = EmbeddingConfig(
        provider="cohere",
        model="embed-english-v3.0",
        api_key=None,
        allow_local_embedding_config=True,
    )
    with patch.dict("os.environ", {"COHERE_API_KEY": "env-k"}, clear=True):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)  # no raise


def test_validate_init_requirements_openai_requires_key():
    cfg = EmbeddingConfig(
        provider="openai",
        model="text-embedding-3-small",
        api_key=None,
        allow_local_embedding_config=True,
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="OpenAI API key is required"),
    ):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)


def test_validate_init_requirements_vertex_ai_requires_project():
    cfg = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        vertex_project_id=None,
        allow_local_embedding_config=True,
    )
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="vertex_project_id is required"),
    ):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)


def test_validate_init_requirements_vertex_ai_accepts_env_var():
    cfg = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        vertex_project_id=None,
        allow_local_embedding_config=True,
    )
    with patch.dict("os.environ", {"VERTEX_AI_PROJECT_ID": "env-proj"}, clear=True):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)  # no raise


def test_validate_init_requirements_bedrock_no_key_check():
    """Bedrock auth comes from the AWS credential chain — no init-time key check."""
    cfg = EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-english-v3",
        api_key=None,
        aws_region="us-east-1",
        allow_local_embedding_config=True,
    )
    DocumentChunkingSource._validate_provider_init_requirements(cfg)  # no raise


def test_validate_init_requirements_rejects_provider_without_model():
    """Without a model, derive_model_id returns None and embedding generation
    silently no-ops. Catch this at init time instead."""
    cfg = EmbeddingConfig(
        provider="bedrock",
        model=None,
        aws_region="us-east-1",
        allow_local_embedding_config=True,
    )
    with pytest.raises(ValueError, match="embedding.model is required"):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)


def test_validate_init_requirements_rejects_malformed_classical_model():
    """A malformed classical model name must fail at init, not as a per-document
    embedding failure inside the first embed call."""
    cfg = EmbeddingConfig(
        provider="classical",
        model="hash-v1-lots",
        allow_local_embedding_config=True,
    )
    with pytest.raises(ValueError, match="hash-v1-<dimensions>"):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)

    DocumentChunkingSource._validate_provider_init_requirements(
        EmbeddingConfig(
            provider="classical",
            model="hash-v1-2048",
            allow_local_embedding_config=True,
        )
    )  # no raise


def test_classical_provider_is_not_rate_limited(pipeline_context, chunking_config):
    """The documents-per-minute limiter protects external APIs; the in-process
    classical provider must not be throttled by it."""
    classical = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="classical",
            model="hash-v1-2048",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    assert (
        DocumentChunkingSource(
            ctx=pipeline_context, config=classical, standalone=False, graph=None
        ).rate_limiter
        is None
    )
    onnx = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="onnx",
            model="bge-small-en-v1.5",
            onnx_model_dir="/tmp/onnx-model",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    assert (
        DocumentChunkingSource(
            ctx=pipeline_context, config=onnx, standalone=False, graph=None
        ).rate_limiter
        is None
    )
    # An API-backed provider keeps the default limiter.
    assert (
        DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
        ).rate_limiter
        is not None
    )


def test_classical_rejects_chunk_size_above_code_point_limit(pipeline_context):
    """The classical provider rejects oversized inputs instead of truncating, so a
    chunk size above its cap would fail every document; refuse it up front."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="classical",
            model="hash-v1-2048",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic", max_characters=20000),
    )
    with pytest.raises(ValueError, match="16384 code point limit"):
        DocumentChunkingSource(
            ctx=pipeline_context, config=config, standalone=False, graph=None
        )


# ---------------------------------------------------------------------------
# _get_provider — caching behavior
# ---------------------------------------------------------------------------


def test_get_provider_caches_instance(pipeline_context):
    """_get_provider should call the factory once and reuse the instance."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="cohere",
            model="embed-english-v3.0",
            api_key="k",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    fake_provider = _mock_provider([[0.1]])
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source.create_embedding_provider",
        return_value=fake_provider,
    ) as mock_factory:
        first = source._get_provider()
        second = source._get_provider()

    assert first is second is fake_provider
    mock_factory.assert_called_once()


# ---------------------------------------------------------------------------
# model_key derivation in semantic content workunit
# ---------------------------------------------------------------------------


def test_model_key_prefers_server_sourced_embedding_key(pipeline_context):
    """When model_embedding_key is set on the config, use it verbatim."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="vertex_ai",
            model="gemini-embedding-001",
            model_embedding_key="server_provided_key",
            vertex_project_id="my-project",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    fake_provider = _mock_provider([[0.1, 0.2]])
    source._provider = fake_provider

    workunits = list(
        source.process_elements_inline(
            "urn:li:document:(test,doc1,PROD)",
            [{"type": "NarrativeText", "text": "hello"}],
        )
    )
    semantic_wu = next(wu for wu in workunits if "semanticContent" in wu.id)
    assert "server_provided_key" in _semantic_embeddings(semantic_wu)


def test_model_key_falls_back_to_cohere_v3_alias(pipeline_context):
    """Without model_embedding_key, the legacy cohere-v3 substring rule still applies."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="cohere",
            model="embed-english-v3.0",
            api_key="k",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    fake_provider = _mock_provider([[0.1, 0.2]])
    source._provider = fake_provider

    workunits = list(
        source.process_elements_inline(
            "urn:li:document:(test,doc1,PROD)",
            [{"type": "NarrativeText", "text": "hello"}],
        )
    )
    semantic_wu = next(wu for wu in workunits if "semanticContent" in wu.id)
    assert "cohere_embed_v3" in _semantic_embeddings(semantic_wu)


def test_model_key_default_normalizes_dashes_and_dots(pipeline_context):
    """Generic model id without server key or v3 alias gets `-`/`.` → `_` normalization."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="openai",
            model="text-embedding-3-small",
            api_key="sk-x",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    fake_provider = _mock_provider([[0.1, 0.2]])
    source._provider = fake_provider

    workunits = list(
        source.process_elements_inline(
            "urn:li:document:(test,doc1,PROD)",
            [{"type": "NarrativeText", "text": "hello"}],
        )
    )
    semantic_wu = next(wu for wu in workunits if "semanticContent" in wu.id)
    assert "text_embedding_3_small" in _semantic_embeddings(semantic_wu)


def test_model_key_default_sanitizes_colon_in_titan_model_id(pipeline_context):
    """Bedrock Titan IDs like ``amazon.titan-embed-text-v2:0`` contain ':',
    which Elasticsearch rejects in field names. Auto-derived keys must replace it."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="amazon.titan-embed-text-v2:0",
            aws_region="us-east-1",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    fake_provider = _mock_provider([[0.1, 0.2]])
    source._provider = fake_provider

    workunits = list(
        source.process_elements_inline(
            "urn:li:document:(test,doc1,PROD)",
            [{"type": "NarrativeText", "text": "hello"}],
        )
    )
    semantic_wu = next(wu for wu in workunits if "semanticContent" in wu.id)
    keys = list(_semantic_embeddings(semantic_wu).keys())
    assert all(":" not in k for k in keys)
    assert "amazon_titan_embed_text_v2_0" in _semantic_embeddings(semantic_wu)


def test_semantic_content_workunit_is_not_primary_source(pipeline_context):
    """Workunits from _emit_semantic_content must have is_primary_source=False
    so AutoStatusAspectProcessor does not inject a Status UPSERT that would
    overwrite lifecycleStage set by other sources or users."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-east-1",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    source._provider = _mock_provider([[0.1, 0.2, 0.3]])

    workunits = list(
        source.process_elements_inline(
            "urn:li:document:(test,doc1,PROD)",
            [{"type": "NarrativeText", "text": "hello"}],
        )
    )
    semantic_wu = next(wu for wu in workunits if "semanticContent" in wu.id)
    assert semantic_wu.is_primary_source is False


def test_batch_failed_document_not_recorded_in_state(pipeline_context, chunking_config):
    """A document whose processing fails (chunker crash) must not get its hash
    recorded in incremental state, or every later run skips it as unchanged."""
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )
    source.config.incremental_mode = True

    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }
    with (
        patch.object(source, "_fetch_documents", return_value=[doc]),
        patch.object(source, "_save_state"),
        patch.object(
            source, "_chunk_elements", side_effect=RuntimeError("chunker crashed")
        ),
    ):
        list(source._process_batch())

    assert doc["urn"] not in source.document_state


def test_batch_embed_failure_not_recorded_in_state(pipeline_context, chunking_config):
    """An embedding failure means no semanticContent was written — the document
    must be retried next run, not recorded as done."""
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )
    source.config.incremental_mode = True

    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }
    with (
        patch.object(source, "_fetch_documents", return_value=[doc]),
        patch.object(source, "_save_state"),
        patch.object(
            source, "_generate_embeddings", side_effect=Exception("provider down")
        ),
    ):
        list(source._process_batch())

    assert doc["urn"] not in source.document_state


def test_batch_success_recorded_in_state(pipeline_context, chunking_config):
    """Successful processing still records incremental state."""
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )
    source.config.incremental_mode = True

    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {
            "unstructured_elements": '[{"type": "Title", "text": "Test Title"}]'
        },
    }
    with (
        patch.object(source, "_fetch_documents", return_value=[doc]),
        patch.object(source, "_save_state"),
        patch.object(source, "_generate_embeddings", return_value=[[0.1] * 4]),
    ):
        list(source._process_batch())

    assert doc["urn"] in source.document_state


def test_malformed_elements_json_not_recorded_in_state(
    pipeline_context, chunking_config
):
    """Malformed unstructured_elements JSON is a failure, not an empty document —
    the hash must not be recorded."""
    with patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph"):
        source = DocumentChunkingSource(
            ctx=pipeline_context, config=chunking_config, standalone=True, graph=None
        )
    source.config.incremental_mode = True

    doc = {
        "urn": "urn:li:document:(test,doc1,PROD)",
        "custom_properties": {"unstructured_elements": "{not valid json"},
    }
    with (
        patch.object(source, "_fetch_documents", return_value=[doc]),
        patch.object(source, "_save_state"),
    ):
        list(source._process_batch())

    assert doc["urn"] not in source.document_state


class TestSkipMarkersAndEmbedAccounting:
    """Skip-marker paths and success/failure accounting in process_elements_inline."""

    def _source(self, pipeline_context, chunking_config):
        return DocumentChunkingSource(
            ctx=pipeline_context,
            config=chunking_config,
            standalone=False,
            graph=None,
        )

    def test_no_elements_emits_no_indexable_content_marker(
        self, pipeline_context, chunking_config
    ):
        source = self._source(pipeline_context, chunking_config)
        wus = list(source.process_elements_inline("urn:li:document:no-elements", []))

        assert len(wus) == 1
        aspect = wus[0].metadata.aspect
        assert isinstance(aspect, SemanticContentClass)
        assert aspect.embeddings == {}
        assert aspect.skipReason == "NO_INDEXABLE_CONTENT"
        assert isinstance(aspect.skippedAt, int)

    def test_blank_chunk_text_emits_skip_marker_not_failure(
        self, pipeline_context, chunking_config
    ):
        """All-blank chunk text is deterministic: it must become a deliberate skip,
        not an embedding failure that retries forever (the provider is never called
        because _generate_embeddings filters blank texts)."""
        source = self._source(pipeline_context, chunking_config)
        elements = [{"type": "NarrativeText", "text": "   "}]

        with patch.object(
            source, "_chunk_elements", return_value=[{"text": "   "}, {"text": "\n"}]
        ):
            wus = list(
                source.process_elements_inline("urn:li:document:blank", elements)
            )

        assert len(wus) == 1
        aspect = wus[0].metadata.aspect
        assert isinstance(aspect, SemanticContentClass)
        assert aspect.skipReason == "NO_INDEXABLE_CONTENT"
        assert source.report.num_embedding_failures == 0

    def test_blank_chunk_in_middle_keeps_embedding_alignment(
        self, pipeline_context, chunking_config
    ):
        """Blank chunks are never sent to the provider; every emitted chunk must
        still carry the vector computed from its own text, not its neighbour's."""
        source = self._source(pipeline_context, chunking_config)
        vectors = {"alpha": [1.0, 0.0], "gamma": [0.0, 1.0]}
        provider = MagicMock()
        provider.embed.side_effect = lambda texts: EmbeddingResult(
            embeddings=[vectors[t] for t in texts]
        )
        source._provider = provider
        chunks = [{"text": "alpha"}, {"text": "   "}, {"text": "gamma"}]

        with patch.object(source, "_chunk_elements", return_value=chunks):
            wus = list(
                source.process_elements_inline(
                    "urn:li:document:aligned",
                    [{"type": "NarrativeText", "text": "alpha gamma"}],
                )
            )

        semantic_wu = next(wu for wu in wus if "semanticContent" in wu.id)
        (model_data,) = _semantic_embeddings(semantic_wu).values()
        assert [(c.text, c.vector) for c in model_data.chunks] == [
            ("alpha", [1.0, 0.0]),
            ("gamma", [0.0, 1.0]),
        ]
        assert [c.position for c in model_data.chunks] == [0, 1]
        assert model_data.totalChunks == 2
        # Offsets still count the skipped blank chunk ("   ", 3 chars) so they map
        # onto the original document text.
        assert [(c.characterOffset, c.characterLength) for c in model_data.chunks] == [
            (0, 5),
            (8, 5),
        ]

    def test_null_chunk_text_is_skipped_like_blank(
        self, pipeline_context, chunking_config
    ):
        source = self._source(pipeline_context, chunking_config)
        provider = MagicMock()
        provider.embed.return_value = EmbeddingResult(embeddings=[[1.0, 0.0]])
        source._provider = provider
        chunks = [{"text": None}, {"text": "alpha"}]

        with patch.object(source, "_chunk_elements", return_value=chunks):
            wus = list(
                source.process_elements_inline(
                    "urn:li:document:nulltext",
                    [{"type": "NarrativeText", "text": "alpha"}],
                )
            )

        semantic_wu = next(wu for wu in wus if "semanticContent" in wu.id)
        (model_data,) = _semantic_embeddings(semantic_wu).values()
        assert [(c.text, c.characterOffset) for c in model_data.chunks] == [
            ("alpha", 0)
        ]
        assert model_data.totalChunks == 1

    def test_vector_count_mismatch_fails_document_instead_of_emitting(
        self, pipeline_context, chunking_config
    ):
        """A provider returning fewer vectors than chunks must not produce a
        semanticContent whose totalChunks claims chunks that carry no vector."""
        source = self._source(pipeline_context, chunking_config)
        provider = MagicMock()
        provider.embed.return_value = EmbeddingResult(embeddings=[[1.0, 0.0]])
        source._provider = provider
        chunks = [{"text": "alpha"}, {"text": "gamma"}]

        with (
            patch.object(source, "_chunk_elements", return_value=chunks),
            pytest.raises(RuntimeError, match="1 vectors for 2 chunks"),
        ):
            list(
                source.process_elements_inline(
                    "urn:li:document:mismatch",
                    [{"type": "NarrativeText", "text": "alpha gamma"}],
                )
            )
        # Recorded as an embedding failure, never as a success.
        assert source.report.num_embedding_failures == 1
        assert "1 vectors for 2 chunks" in source.report.embedding_failures[0]

    def test_vector_count_is_checked_per_batch(self, pipeline_context):
        """Miscounts that net out across batches must still fail, and a short batch
        must fail before the remaining provider calls are spent."""
        config = DocumentChunkingSourceConfig(
            embedding=EmbeddingConfig(
                provider="bedrock",
                model="cohere.embed-english-v3",
                aws_region="us-west-2",
                allow_local_embedding_config=True,
                batch_size=1,
            ),
            chunking=ChunkingConfig(strategy="basic"),
        )
        source = self._source(pipeline_context, config)
        provider = MagicMock()
        # Two vectors for the first one-chunk batch, none for the second: the
        # aggregate count would match.
        provider.embed.side_effect = [
            EmbeddingResult(embeddings=[[1.0, 0.0], [0.0, 1.0]]),
            EmbeddingResult(embeddings=[]),
        ]
        source._provider = provider
        chunks = [{"text": "alpha"}, {"text": "gamma"}]

        with (
            patch.object(source, "_chunk_elements", return_value=chunks),
            pytest.raises(RuntimeError, match="2 vectors for 1 chunks"),
        ):
            list(
                source.process_elements_inline(
                    "urn:li:document:per-batch",
                    [{"type": "NarrativeText", "text": "alpha gamma"}],
                )
            )
        # Failed on the first short batch; the second provider call was never made.
        assert provider.embed.call_count == 1
        assert source.report.num_embedding_failures == 1

    def test_provider_returning_no_vectors_is_failure_not_success(
        self, pipeline_context, chunking_config
    ):
        """A configured provider returning zero vectors for non-blank chunks must be
        accounted as a failure (and raise), never as a successful embed."""
        source = self._source(pipeline_context, chunking_config)
        elements = [{"type": "NarrativeText", "text": "real content here"}]

        with (
            patch.object(source, "_generate_embeddings", return_value=[]),
            pytest.raises(RuntimeError, match="no vectors"),
        ):
            list(source.process_elements_inline("urn:li:document:anomaly", elements))

        assert source.report.num_embedding_failures == 1
        assert source.report.num_documents_with_embeddings == 0

    def test_embed_emission_carries_no_skip_marker(
        self, pipeline_context, chunking_config
    ):
        """A real embed must emit an aspect without skipReason/skippedAt, so it
        replaces (clears) any previous skip marker for the document."""
        source = self._source(pipeline_context, chunking_config)
        elements = [
            {"type": "Title", "text": "Test Title"},
            {"type": "NarrativeText", "text": "Test content"},
        ]

        # Both elements fold into one chunk under the basic strategy: one vector.
        with patch.object(source, "_generate_embeddings", return_value=[[0.1, 0.2]]):
            wus = list(
                source.process_elements_inline("urn:li:document:embedded", elements)
            )

        semantic = [
            wu
            for wu in wus
            if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
        ]
        assert len(semantic) == 1
        aspect = semantic[0].metadata.aspect
        assert isinstance(aspect, SemanticContentClass)
        assert aspect.embeddings
        assert aspect.skipReason is None
        assert aspect.skippedAt is None

    def test_compute_source_text_sha256_cross_language_vector(self):
        """Pinned vector shared with the Java projection test
        (UpdateIndicesV2StrategyTest): the production helper must produce a digest
        byte-identical to the server-side resolvedTextSha256 stamp."""
        assert (
            compute_source_text_sha256("héllo \U0001f680\r\nworld")
            == "f319ae6318b99bf8c83d79fe08bdcbc42928dc83c0d9e23145440c83141321a9"
        )

    def test_skip_marker_preserves_other_models_embeddings(
        self, pipeline_context, chunking_config
    ):
        """SemanticContent.embeddings is a multi-model map written as a full-aspect
        UPSERT: a skip marker must carry forward other models' existing entries
        (dropping only this pipeline's own), otherwise one pipeline's skip erases
        another model's embeddings and the index projection clears its vectors."""
        source = self._source(pipeline_context, chunking_config)
        own_key = source.get_model_embedding_key()
        assert own_key is not None
        graph = MagicMock()
        graph.get_aspect.return_value = SemanticContentClass(
            embeddings={
                own_key: EmbeddingModelDataClass(
                    modelVersion="own/model-v1",
                    generatedAt=456,
                    totalChunks=0,
                    chunks=[],
                ),
                "other_model": EmbeddingModelDataClass(
                    modelVersion="other/model-v1",
                    generatedAt=123,
                    totalChunks=0,
                    chunks=[],
                ),
            }
        )
        source.graph = graph

        wu = source.build_skip_marker_workunit("urn:li:document:multi", "EMPTY_TEXT")

        aspect = wu.metadata.aspect
        assert isinstance(aspect, SemanticContentClass)
        assert aspect.skipReason == "EMPTY_TEXT"
        assert "other_model" in aspect.embeddings
        assert aspect.embeddings["other_model"].modelVersion == "other/model-v1"
        assert own_key not in aspect.embeddings

    def test_skip_marker_read_failure_raises_instead_of_erasing(
        self, pipeline_context, chunking_config
    ):
        """A transient read failure must NOT produce a marker with an empty map (a
        full-aspect UPSERT that would erase other models' entries); it fails the
        operation so the document is retried next run."""
        source = self._source(pipeline_context, chunking_config)
        graph = MagicMock()
        graph.get_aspect.side_effect = RuntimeError("boom")
        source.graph = graph

        with pytest.raises(SkipMarkerReadError, match="not emitting a skip marker"):
            source.build_skip_marker_workunit(
                "urn:li:document:unreadable", "EMPTY_TEXT"
            )


def _per_input_provider(dim: int = 4) -> MagicMock:
    """Provider returning one deterministic vector per input text.

    Unlike ``_mock_provider`` (fixed return), this yields one vector per element of
    each batch, so it passes ``_generate_embeddings``'s per-batch length check across
    multiple batches.
    """
    provider = MagicMock()
    provider.embed.side_effect = lambda batch: EmbeddingResult(
        embeddings=[[0.1] * dim for _ in batch]
    )
    return provider


def test_oversized_document_truncated_to_cap(pipeline_context, chunking_config):
    """A document over the chunk cap keeps the first N chunks, embeds N vectors, emits
    one aspect, increments the truncation counter, and does not raise."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    cap = source.config.chunking.max_chunks_per_document
    chunks = [{"text": f"chunk {i}", "type": "NarrativeText"} for i in range(cap + 50)]
    with patch.object(source, "_chunk_elements", return_value=chunks):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:oversized",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    model_data = _semantic_embeddings(semantic_wus[0])["cohere_embed_v3"]
    # Kept exactly the first N chunks, in order, one vector each.
    assert len(model_data.chunks) == cap
    assert model_data.totalChunks == cap
    assert model_data.chunks[0].text == "chunk 0"
    assert model_data.chunks[cap - 1].text == f"chunk {cap - 1}"
    assert source.report.num_documents_truncated_oversized == 1


def test_document_under_cap_not_truncated(pipeline_context, chunking_config):
    """A document below the cap is emitted whole with no truncation or drop."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    chunks = [{"text": f"chunk {i}", "type": "NarrativeText"} for i in range(3)]
    with patch.object(source, "_chunk_elements", return_value=chunks):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:small",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    assert len(_semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks) == 3
    assert source.report.num_documents_truncated_oversized == 0
    assert source.report.num_documents_dropped_oversized == 0


def test_serialized_aspect_at_default_cap_under_ceiling():
    """A full default-cap aspect (N chunks x 1024-dim vectors + chunk text) serializes
    to well under the byte ceiling. This is what justifies the default cap value."""
    cap = ChunkingConfig().max_chunks_per_document
    dim = 1024  # cohere embed-english-v3 dimension
    text = "x" * ChunkingConfig().max_characters
    chunks = [
        EmbeddingChunkClass(
            position=i,
            # Full-precision floats give a realistic serialized length; all-zero
            # vectors would serialize to "0.0" and badly understate the real size.
            vector=[math.sin(i * dim + j) for j in range(dim)],
            characterOffset=i * len(text),
            characterLength=len(text),
            tokenCount=None,
            text=text,
        )
        for i in range(cap)
    ]
    aspect = SemanticContentClass(
        embeddings={
            "cohere_embed_v3": EmbeddingModelDataClass(
                modelVersion="bedrock/cohere.embed-english-v3",
                generatedAt=0,
                sourceTextSha256=None,
                chunkingStrategy="basic",
                totalChunks=cap,
                chunks=chunks,
            )
        }
    )
    size = len(json.dumps(aspect.to_obj()))
    assert size < MAX_SERIALIZED_ASPECT_BYTES, (
        f"aspect for {cap} chunks serialized to {size} bytes, not under the "
        f"{MAX_SERIALIZED_ASPECT_BYTES}-byte ceiling"
    )


def test_oversized_assembled_aspect_dropped(pipeline_context, chunking_config):
    """An assembled aspect over the byte ceiling is skipped (not emitted) with a
    counter, without raising — no poison MCP reaches the sink."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    chunks = [{"text": f"chunk {i}", "type": "NarrativeText"} for i in range(3)]
    with (
        patch.object(source, "_chunk_elements", return_value=chunks),
        patch(
            "datahub.ingestion.source.unstructured.chunking_source.MAX_SERIALIZED_ASPECT_BYTES",
            100,
        ),
    ):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:huge",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert semantic_wus == []
    assert source.report.num_documents_dropped_oversized == 1


def test_document_at_exact_cap_not_truncated(pipeline_context, chunking_config):
    """A document with exactly max_chunks_per_document chunks is not truncated (the cap
    is a <= boundary): all N chunks are embedded and the truncation counter stays zero."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    cap = source.config.chunking.max_chunks_per_document
    chunks = [{"text": f"chunk {i}", "type": "NarrativeText"} for i in range(cap)]
    with patch.object(source, "_chunk_elements", return_value=chunks):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:exact",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    assert len(_semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks) == cap
    assert source.report.num_documents_truncated_oversized == 0


def test_standalone_oversized_aspect_dropped_records_processed(
    pipeline_context, chunking_config
):
    """Standalone path: an aspect over the byte ceiling is dropped (nothing emitted,
    counter incremented) yet _process_single_document returns True, so an unchanged
    oversized document is a deliberate skip recorded as done, not retried every run."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    chunks = [{"text": f"chunk {i}", "type": "NarrativeText"} for i in range(3)]
    with (
        patch.object(
            source,
            "_extract_elements",
            return_value=[{"type": "NarrativeText", "text": "x"}],
        ),
        patch.object(source, "_chunk_elements", return_value=chunks),
        patch(
            "datahub.ingestion.source.unstructured.chunking_source.MAX_SERIALIZED_ASPECT_BYTES",
            100,
        ),
    ):
        gen = source._process_single_document({"urn": "urn:li:document:huge"})
        workunits = []
        try:
            while True:
                workunits.append(next(gen))
        except StopIteration as stop:
            returned = stop.value

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert semantic_wus == []
    assert source.report.num_documents_dropped_oversized == 1
    # Drop is a deliberate skip: the document is recorded as processed, not retried.
    assert returned is True


def test_oversized_aspect_truncated_to_fit_not_dropped(
    pipeline_context, chunking_config
):
    """An assembled aspect over the byte ceiling is truncated to the largest chunk prefix
    that fits and still emitted (not dropped whole) — this is what saves high-dimensional
    models, whose vectors inflate the aspect past the ceiling under the default count cap."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()

    # 8 chunks < the default count cap, so only the byte backstop can truncate here.
    # Large per-chunk text makes the assembled aspect exceed a small patched ceiling.
    chunks = [{"text": "x" * 1000, "type": "NarrativeText"} for _ in range(8)]
    with (
        patch.object(source, "_chunk_elements", return_value=chunks),
        patch(
            "datahub.ingestion.source.unstructured.chunking_source.MAX_SERIALIZED_ASPECT_BYTES",
            4000,
        ),
    ):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:fit",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    kept = _semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks
    # Kept a nonempty prefix strictly smaller than the input, and it fits the ceiling.
    assert 1 <= len(kept) < 8
    assert len(json.dumps(semantic_wus[0].metadata.aspect.to_obj())) <= 4000  # type: ignore[union-attr]
    assert source.report.num_documents_truncated_oversized == 1
    assert source.report.num_documents_dropped_oversized == 0


def test_fingerprint_includes_max_chunks_only_when_non_default():
    """The chunk cap is absent from the staleness fingerprint at its default (so upgrading
    to a build that adds the knob does not re-embed every document) and present once tuned
    (so a change re-hashes affected documents)."""
    from datahub.ingestion.source.unstructured.chunking_config import (
        DEFAULT_MAX_CHUNKS_PER_DOCUMENT,
        ChunkingConfig,
        EmbeddingConfig,
        get_processing_config_fingerprint,
    )

    embedding = EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-english-v3",
        model_embedding_key="cohere_embed_v3",
        allow_local_embedding_config=True,
    )
    default_fp = get_processing_config_fingerprint(ChunkingConfig(), embedding)
    assert "chunking_max_chunks_per_document" not in default_fp

    tuned_fp = get_processing_config_fingerprint(
        ChunkingConfig(max_chunks_per_document=DEFAULT_MAX_CHUNKS_PER_DOCUMENT - 50),
        embedding,
    )
    assert (
        tuned_fp["chunking_max_chunks_per_document"]
        == DEFAULT_MAX_CHUNKS_PER_DOCUMENT - 50
    )


def test_leading_blank_chunks_capped_by_embeddable_count(
    pipeline_context, chunking_config
):
    """The cap counts embeddable chunks, so a document whose text begins after a run of
    blank chunks longer than the cap is still embedded (its later text is kept), not
    capped to an all-blank prefix and misclassified as non-indexable."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()
    source.config.chunking.max_chunks_per_document = 3

    # 4 leading blank chunks (more than the cap), then 5 chunks with text.
    chunks = [{"text": "", "type": "NarrativeText"} for _ in range(4)] + [
        {"text": f"body {i}", "type": "NarrativeText"} for i in range(5)
    ]
    with patch.object(source, "_chunk_elements", return_value=chunks):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:blanks",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    # Emitted (not skip-markered), keeping the first 3 embeddable chunks in order.
    assert len(semantic_wus) == 1
    kept = _semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks
    assert [chunk.text for chunk in kept] == ["body 0", "body 1", "body 2"]
    assert source.report.num_documents_truncated_oversized == 1


def test_oversized_counted_once_across_both_chokepoints(
    pipeline_context, chunking_config
):
    """A document that trips both the count cap and the byte backstop is reported once,
    not twice."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()
    source.config.chunking.max_chunks_per_document = 3

    chunks = [{"text": "x" * 1000, "type": "NarrativeText"} for _ in range(5)]
    with (
        patch.object(source, "_chunk_elements", return_value=chunks),
        patch(
            "datahub.ingestion.source.unstructured.chunking_source.MAX_SERIALIZED_ASPECT_BYTES",
            2500,
        ),
    ):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:both",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    kept = _semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks
    # The count cap kept 3; the byte backstop truncated further, to fewer than 3.
    assert 1 <= len(kept) < 3
    # Counted once (by the count cap), not again by the byte truncation.
    assert source.report.num_documents_truncated_oversized == 1
    assert source.report.num_documents_dropped_oversized == 0


def test_exact_cap_with_trailing_blanks_not_counted_truncated(
    pipeline_context, chunking_config
):
    """Exactly max_chunks embeddable chunks followed by trailing blank chunks is emitted
    whole and NOT counted/warned as truncated — the dropped tail has no embeddable text,
    so nothing that would have been embedded is lost."""
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=chunking_config, standalone=False, graph=None
    )
    source._provider = _per_input_provider()
    source.config.chunking.max_chunks_per_document = 3

    chunks = [{"text": f"body {i}", "type": "NarrativeText"} for i in range(3)] + [
        {"text": "", "type": "NarrativeText"} for _ in range(2)
    ]
    with patch.object(source, "_chunk_elements", return_value=chunks):
        workunits = list(
            source.process_elements_inline(
                "urn:li:document:trailing",
                [{"type": "NarrativeText", "text": "x"}],
            )
        )

    semantic_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, SemanticContentClass)  # type: ignore[union-attr]
    ]
    assert len(semantic_wus) == 1
    assert len(_semantic_embeddings(semantic_wus[0])["cohere_embed_v3"].chunks) == 3
    assert source.report.num_documents_truncated_oversized == 0
