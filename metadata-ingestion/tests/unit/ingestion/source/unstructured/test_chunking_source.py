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
    construction should succeed and the env value should be written back to the config
    so downstream litellm calls have a single source of truth."""
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
    assert source.config.embedding.vertex_project_id == "env-project"
    assert source.embedding_model == "vertex_ai/gemini-embedding-001"


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


def test_generate_embeddings_vertex_ai_passes_task_type(pipeline_context):
    """For vertex_ai provider, _generate_embeddings must pass task_type=RETRIEVAL_DOCUMENT and vertex auth."""
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
    chunks = [{"text": "hello world"}]
    fake_response = MagicMock()
    fake_response.data = [{"embedding": [0.1, 0.2, 0.3]}]
    with patch("litellm.embedding", return_value=fake_response) as mock_embed:
        embeddings = source._generate_embeddings(chunks)
    assert embeddings == [[0.1, 0.2, 0.3]]
    call_kwargs = mock_embed.call_args.kwargs
    assert call_kwargs["model"] == "vertex_ai/gemini-embedding-001"
    assert call_kwargs["task_type"] == "RETRIEVAL_DOCUMENT"
    assert call_kwargs["vertex_project"] == "my-project"
    assert call_kwargs["vertex_location"] == "us-east1"


def test_generate_embeddings_cohere_passes_api_key(pipeline_context):
    """For non-vertex providers, _generate_embeddings must forward api_key from config."""
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
    chunks = [{"text": "hello world"}]
    fake_response = MagicMock()
    fake_response.data = [{"embedding": [0.1, 0.2, 0.3]}]
    with patch("litellm.embedding", return_value=fake_response) as mock_embed:
        embeddings = source._generate_embeddings(chunks)
    assert embeddings == [[0.1, 0.2, 0.3]]
    call_kwargs = mock_embed.call_args.kwargs
    assert call_kwargs["model"] == "cohere/embed-english-v3.0"
    assert call_kwargs["api_key"] == "test-cohere-key"
    # vertex-only kwargs must not leak into non-vertex calls
    assert "task_type" not in call_kwargs
    assert "vertex_project" not in call_kwargs


def test_generate_embeddings_bedrock_passes_aws_region(pipeline_context):
    """For bedrock provider, _generate_embeddings must forward aws_region as aws_region_name."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,
        ),
        chunking=ChunkingConfig(strategy="basic"),
    )
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )
    chunks = [{"text": "hello world"}]
    fake_response = MagicMock()
    fake_response.data = [{"embedding": [0.1, 0.2, 0.3]}]
    with patch("litellm.embedding", return_value=fake_response) as mock_embed:
        embeddings = source._generate_embeddings(chunks)
    assert embeddings == [[0.1, 0.2, 0.3]]
    call_kwargs = mock_embed.call_args.kwargs
    assert call_kwargs["model"] == "bedrock/cohere.embed-english-v3"
    assert call_kwargs["aws_region_name"] == "us-west-2"
    assert "task_type" not in call_kwargs


def test_embedding_capability_vertex_ai_passes_task_type():
    """test_embedding_capability for vertex_ai must pass task_type=RETRIEVAL_DOCUMENT to litellm."""
    config = EmbeddingConfig(
        provider="vertex_ai",
        model="gemini-embedding-001",
        model_embedding_key="gemini_embedding_001",
        vertex_project_id="my-project",
        vertex_location="us-east1",
        allow_local_embedding_config=True,
    )
    fake_response = MagicMock()
    fake_response.data = [{"embedding": [0.1, 0.2, 0.3]}]
    with patch("litellm.embedding", return_value=fake_response) as mock_embed:
        report = DocumentChunkingSource.test_embedding_capability(config)
    assert report.capable
    call_kwargs = mock_embed.call_args.kwargs
    assert call_kwargs["model"] == "vertex_ai/gemini-embedding-001"
    assert call_kwargs["task_type"] == "RETRIEVAL_DOCUMENT"
    assert call_kwargs["vertex_project"] == "my-project"
    assert call_kwargs["vertex_location"] == "us-east1"


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
    """Local provider prefixes the model name with 'openai/' for litellm routing."""
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=_local_config("nomic-embed-text"),
        standalone=False,
        graph=None,
    )
    assert source.embedding_model == "openai/nomic-embed-text"


def test_local_provider_already_prefixed_model(pipeline_context):
    """If model already starts with 'openai/', it is not double-prefixed."""
    source = DocumentChunkingSource(
        ctx=pipeline_context,
        config=_local_config("openai/nomic-embed-text"),
        standalone=False,
        graph=None,
    )
    assert source.embedding_model == "openai/nomic-embed-text"


def test_local_provider_api_base_strips_embeddings_suffix(pipeline_context):
    """api_base strips /embeddings so litellm can append its own path."""
    config = _local_config(endpoint="http://localhost:11434/v1/embeddings")
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )

    captured: dict = {}

    def fake_embedding(**kwargs):
        captured.update(kwargs)
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]}]
        return resp

    chunk = {"text": "hello", "type": "NarrativeText"}
    with patch("litellm.embedding", side_effect=fake_embedding):
        source._generate_embeddings([chunk])

    assert captured["api_base"] == "http://localhost:11434/v1"
    assert captured["api_key"] == "local"


def test_local_provider_api_base_no_suffix(pipeline_context):
    """An endpoint without /embeddings is passed through unchanged."""
    config = _local_config(endpoint="http://myserver:8080/v1")
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )

    captured: dict = {}

    def fake_embedding(**kwargs):
        captured.update(kwargs)
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]}]
        return resp

    chunk = {"text": "hello", "type": "NarrativeText"}
    with patch("litellm.embedding", side_effect=fake_embedding):
        source._generate_embeddings([chunk])

    assert captured["api_base"] == "http://myserver:8080/v1"


def test_local_provider_api_base_from_env_var(pipeline_context):
    """Falls back to LOCAL_EMBEDDING_ENDPOINT env var when no endpoint configured."""
    config = _local_config()  # no endpoint
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )

    captured: dict = {}

    def fake_embedding(**kwargs):
        captured.update(kwargs)
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]}]
        return resp

    with (
        patch("litellm.embedding", side_effect=fake_embedding),
        patch.dict(
            "os.environ",
            {"LOCAL_EMBEDDING_ENDPOINT": "http://envhost:11434/v1/embeddings"},
        ),
    ):
        chunk = {"text": "hello", "type": "NarrativeText"}
        source._generate_embeddings([chunk])

    assert captured["api_base"] == "http://envhost:11434/v1"


def test_local_provider_api_base_default_fallback(pipeline_context):
    """Falls back to localhost:11434 when neither config nor env var is set."""
    config = _local_config()
    source = DocumentChunkingSource(
        ctx=pipeline_context, config=config, standalone=False, graph=None
    )

    captured: dict = {}

    def fake_embedding(**kwargs):
        captured.update(kwargs)
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]}]
        return resp

    with (
        patch("litellm.embedding", side_effect=fake_embedding),
        patch.dict("os.environ", {}, clear=True),
    ):
        chunk = {"text": "hello", "type": "NarrativeText"}
        source._generate_embeddings([chunk])

    assert captured["api_base"] == "http://localhost:11434/v1"


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

    fake_embedding_result = MagicMock()
    fake_embedding_result.data = [{"embedding": [0.0] * 1024}]
    with (
        patch("litellm.embedding", return_value=fake_embedding_result),
        patch.object(
            source,
            "_chunk_elements",
            return_value=[{"text": "hi", "type": "NarrativeText"}],
        ),
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

    fake_embedding_result = MagicMock()
    fake_embedding_result.data = [{"embedding": [0.0] * 768}]
    with (
        patch("litellm.embedding", return_value=fake_embedding_result),
        patch.object(
            source,
            "_chunk_elements",
            return_value=[{"text": "hi", "type": "NarrativeText"}],
        ),
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


def test_test_embedding_capability_local_passes_api_base():
    """test_embedding_capability routes local provider to api_base, not api.openai.com."""
    config = EmbeddingConfig(
        provider="local",
        model="nomic-embed-text",
        endpoint="http://myollama:11434/v1/embeddings",
        allow_local_embedding_config=True,
    )
    captured: dict = {}

    def fake_embedding(**kwargs):
        captured.update(kwargs)
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]}]
        return resp

    with patch("litellm.embedding", side_effect=fake_embedding):
        report = DocumentChunkingSource.test_embedding_capability(config)

    assert report.capable
    assert captured.get("api_base") == "http://myollama:11434/v1"
    assert captured.get("api_key") == "local"
