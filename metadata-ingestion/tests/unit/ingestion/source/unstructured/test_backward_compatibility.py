"""Test backward compatibility with older DataHub servers that don't have semantic search config API."""

from unittest.mock import Mock, patch

import pytest

# Skip entire module if litellm is not installed (requires Python 3.10+)
pytest.importorskip("litellm")

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import GraphError
from datahub.ingestion.source.unstructured.chunking_config import (
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_old_server_with_local_config_succeeds(mock_graph_class, mock_get_config):
    """Test that source works with old server when local embedding config is provided."""
    # Setup: Old server that doesn't have semanticSearchConfig field
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            model_embedding_key="cohere_embed_v3",
            aws_region="us-west-2",
        )
    )

    ctx = PipelineContext(run_id="test")

    mock_graph = Mock()
    mock_graph_class.return_value = mock_graph

    # Simulate old server - semanticSearchConfig field doesn't exist
    mock_get_config.side_effect = GraphError(
        "Server does not expose semantic search configuration. "
        "Ensure DataHub server version supports semantic search config API (v0.14.0+)."
    )

    # This should succeed with backward compatibility
    source = DocumentChunkingSource(ctx, config, standalone=True)

    # Verify the source was created successfully
    assert source is not None
    assert source.config.embedding.provider == "bedrock"
    assert source.config.embedding.model == "cohere.embed-english-v3"


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_old_server_without_local_config_uses_defaults(
    mock_graph_class, mock_get_config
):
    """Test that source uses defaults when old server and no local config."""
    # Setup: Old server + NO local embedding config
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig()  # No provider/model specified
    )

    ctx = PipelineContext(run_id="test")

    mock_graph = Mock()
    mock_graph_class.return_value = mock_graph

    # Simulate old server
    mock_get_config.side_effect = GraphError(
        "Server does not expose semantic search configuration. "
        "Ensure DataHub server version supports semantic search config API (v0.14.0+)."
    )

    # Should succeed with default config fallback
    source = DocumentChunkingSource(ctx, config, standalone=True)

    # Verify source was created with default config
    assert source is not None
    assert source.config.embedding.provider == "bedrock"
    assert source.config.embedding.model == "cohere.embed-english-v3"
    assert source.config.embedding.model_embedding_key == "cohere_embed_v3"
    assert source.config.embedding.aws_region == "us-west-2"
    assert source.config.embedding.allow_local_embedding_config is True


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_old_server_with_break_glass_flag(mock_graph_class, mock_get_config):
    """Test that break-glass flag skips server validation entirely."""
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,  # Break-glass flag
        )
    )

    ctx = PipelineContext(run_id="test")

    mock_graph = Mock()
    mock_graph_class.return_value = mock_graph

    # With break-glass flag, we shouldn't even call the API
    source = DocumentChunkingSource(ctx, config, standalone=True)

    # Verify we didn't try to call the server API
    mock_get_config.assert_not_called()

    # Verify source was created with local config
    assert source.config.embedding.provider == "bedrock"
    assert source.config.embedding.model == "cohere.embed-english-v3"


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_new_server_with_semantic_search_disabled_fails(
    mock_graph_class, mock_get_config
):
    """Test that validation fails when new server but semantic search is disabled."""
    from datahub.ingestion.source.unstructured.chunking_config import (
        ServerEmbeddingConfig,
        ServerSemanticSearchConfig,
    )

    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            model_embedding_key="cohere_embed_v3",
            aws_region="us-west-2",
        )
    )

    ctx = PipelineContext(run_id="test")

    mock_graph = Mock()
    mock_graph_class.return_value = mock_graph

    # Simulate new server with semantic search disabled
    server_config = ServerSemanticSearchConfig(
        enabled=False,  # Disabled!
        enabled_entities=[],
        embedding_config=ServerEmbeddingConfig(
            provider="aws-bedrock",
            model_id="cohere.embed-english-v3",
            aws_region="us-west-2",
            model_embedding_key="cohere_embed_v3",
        ),
    )
    mock_get_config.return_value = server_config

    # Should fail because semantic search is disabled
    with pytest.raises(
        ValueError, match="Semantic search is not enabled on the DataHub server"
    ):
        DocumentChunkingSource(ctx, config, standalone=True)
