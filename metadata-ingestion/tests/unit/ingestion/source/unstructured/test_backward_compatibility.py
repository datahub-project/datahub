"""Test backward compatibility with older DataHub servers that don't have semantic search config API."""

from typing import Optional
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import GraphError
from datahub.ingestion.graph.embeddings import ServerEmbeddingsUnavailableError
from datahub.ingestion.source.unstructured.chunking_config import (
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
    ServerEmbeddingConfig,
    ServerSemanticSearchConfig,
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
def test_old_server_without_local_config_skips_embedding(
    mock_graph_class, mock_get_config
):
    """Test that source skips embedding when old server can't provide config and no local config.

    Old behavior fell back to Bedrock defaults, which caused AccessDeniedException on
    deployments without Bedrock access. The safe default is to skip embedding entirely.
    """
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

    # Should succeed — but skip embedding rather than fall back to Bedrock
    source = DocumentChunkingSource(ctx, config, standalone=True)

    # provider must be None — no embedding should be attempted
    assert source is not None
    assert source.config.embedding.provider is None


def _break_glass_config() -> DocumentChunkingSourceConfig:
    return DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,
        )
    )


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_old_server_with_break_glass_flag(mock_graph_class, mock_get_config):
    """A failed patch-capability probe still keeps the local embedding config.

    Break-glass skips validation. Standalone mode still asks the server whether
    semanticContent patches are supported, and a lookup failure must not fail startup.
    """
    mock_graph_class.return_value = Mock()
    mock_get_config.side_effect = GraphError(
        "Server does not expose semantic search configuration."
    )

    source = DocumentChunkingSource(
        PipelineContext(run_id="test"), _break_glass_config(), standalone=True
    )

    mock_get_config.assert_called_once()
    assert source.config.embedding.provider == "bedrock"
    assert source.config.embedding.model == "cohere.embed-english-v3"
    assert not source.config.embedding._semantic_content_patch_supported


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_break_glass_records_patch_support_without_validating(
    mock_graph_class, mock_get_config
):
    """A disabled or mismatched server config does not reject the local config.

    The probe only records patch support. It does not validate the local model.
    """
    from datahub.ingestion.source.unstructured.chunking_config import (
        ServerEmbeddingConfig,
        ServerSemanticSearchConfig,
    )

    mock_graph_class.return_value = Mock()
    mock_get_config.return_value = ServerSemanticSearchConfig(
        enabled=False,
        enabled_entities=[],
        embedding_config=ServerEmbeddingConfig(
            provider="openai",
            model_id="text-embedding-3-small",
            model_embedding_key="openai_3_small",
        ),
        semantic_content_patch_supported=True,
    )

    source = DocumentChunkingSource(
        PipelineContext(run_id="test"), _break_glass_config(), standalone=True
    )

    assert source.config.embedding.provider == "bedrock"
    assert source.config.embedding.model == "cohere.embed-english-v3"
    assert source.config.embedding._semantic_content_patch_supported


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


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
@patch("datahub.ingestion.source.unstructured.chunking_source.DataHubGraph")
def test_no_local_config_server_disabled_skips_embedding(
    mock_graph_class, mock_get_config
):
    """When no local embedding config is set and the server has semantic search
    disabled, the source must skip embedding entirely (provider=None).

    Regression: previously this path fell through to get_default_config() which
    returned provider='bedrock', causing boto3 import failures on minimal installs
    (e.g. Sproutsocial executor with enabled=false but provider='aws-bedrock').
    """
    from datahub.ingestion.source.unstructured.chunking_config import (
        ServerEmbeddingConfig,
        ServerSemanticSearchConfig,
    )

    # No local embedding config — just defaults (provider=None)
    config = DocumentChunkingSourceConfig()

    ctx = PipelineContext(run_id="test")
    mock_graph = Mock()
    mock_graph_class.return_value = mock_graph

    # Server is reachable, has a provider configured, but semantic search is disabled
    server_config = ServerSemanticSearchConfig(
        enabled=False,
        enabled_entities=["document"],
        embedding_config=ServerEmbeddingConfig(
            provider="aws-bedrock",
            model_id="cohere.embed-english-v3",
            aws_region="us-west-2",
            model_embedding_key="cohere_embed_v3",
        ),
    )
    mock_get_config.return_value = server_config

    source = DocumentChunkingSource(ctx, config, standalone=True)

    # provider must be None — no embedding should be attempted
    assert source.config.embedding.provider is None


@patch(
    "datahub.ingestion.source.unstructured.embedding_target.get_semantic_search_config"
)
@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
def test_generation_enabled_skips_server_loaded_and_datahub_provider(
    mock_source_config, mock_target_config
):
    """A server that generates embeddings itself is not embedded again by this source."""
    server_config = _generating_server_config()
    mock_source_config.return_value = server_config
    mock_target_config.return_value = server_config
    graph = Mock()
    graph.server_config.supports_feature.return_value = True

    server_loaded = DocumentChunkingSource.resolve_embedding_config(
        EmbeddingConfig(), graph=graph
    )
    datahub_provider = DocumentChunkingSource.resolve_embedding_config(
        EmbeddingConfig(provider="datahub"), graph=graph
    )

    assert server_loaded.provider is None
    assert datahub_provider.provider is None
    mock_target_config.assert_called()


def _generating_server_config(
    *,
    generation_enabled: bool = True,
    service_profile_key: Optional[str] = "cohere_embed_v3",
) -> ServerSemanticSearchConfig:
    return ServerSemanticSearchConfig(
        enabled=True,
        enabled_entities=["document"],
        generation_enabled=generation_enabled,
        service_profile_key=service_profile_key,
        embedding_config=ServerEmbeddingConfig(
            provider="aws-bedrock",
            model_id="cohere.embed-english-v3",
            aws_region="us-west-2",
            model_embedding_key="cohere_embed_v3",
        ),
    )


def _explicit_bedrock_config(
    *, allow_local: bool = False, model_embedding_key: str = "cohere_embed_v3"
) -> EmbeddingConfig:
    return EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-english-v3",
        model_embedding_key=model_embedding_key,
        aws_region="us-west-2",
        allow_local_embedding_config=allow_local,
    )


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
def test_generation_enabled_skips_explicit_local_provider(mock_get_config):
    """A recipe that names its own provider does not call it when the server generates."""
    mock_get_config.return_value = _generating_server_config()

    validated = DocumentChunkingSource.resolve_embedding_config(
        _explicit_bedrock_config(), graph=Mock()
    )
    break_glass = DocumentChunkingSource.resolve_embedding_config(
        _explicit_bedrock_config(allow_local=True), graph=Mock()
    )

    assert validated.provider is None
    assert validated._server_generates_embeddings
    assert break_glass.provider is None
    assert break_glass._server_generates_embeddings
    report = DocumentChunkingSource.test_embedding_capability(validated)
    assert report.capable
    assert mock_get_config.call_count == 2


@patch(
    "datahub.ingestion.source.unstructured.embedding_target.get_semantic_search_config"
)
@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
def test_generation_enabled_keeps_embedding_a_connector_owned_key(
    mock_source_config, mock_target_config
):
    """Only the service profile key is generated by the server; other keys still embed."""
    mock_source_config.return_value = _generating_server_config()
    mock_target_config.return_value = _generating_server_config(
        service_profile_key="service_profile"
    )
    graph = Mock()
    graph.server_config.supports_feature.return_value = True
    graph.server_config.embedding_service_model = None

    connector = DocumentChunkingSource.resolve_embedding_config(
        _explicit_bedrock_config(allow_local=True, model_embedding_key="connector_key"),
        graph=graph,
    )
    datahub_provider = DocumentChunkingSource.resolve_embedding_config(
        EmbeddingConfig(provider="datahub"), graph=graph
    )

    assert connector.provider == "bedrock"
    assert not connector._server_generates_embeddings
    assert datahub_provider.provider == "datahub"
    assert datahub_provider.model_embedding_key == "cohere_embed_v3"


@patch(
    "datahub.ingestion.source.unstructured.chunking_source.get_semantic_search_config"
)
def test_service_profile_key_without_generation_does_not_skip(mock_get_config):
    mock_get_config.return_value = _generating_server_config(generation_enabled=False)

    server_loaded = DocumentChunkingSource.resolve_embedding_config(
        EmbeddingConfig(), graph=Mock()
    )
    validated = DocumentChunkingSource.resolve_embedding_config(
        _explicit_bedrock_config(), graph=Mock()
    )

    assert server_loaded.provider == "bedrock"
    assert validated.provider == "bedrock"
    assert not validated._server_generates_embeddings


@patch(
    "datahub.ingestion.source.unstructured.embedding_target.get_semantic_search_config"
)
def test_datahub_provider_checks_the_embedding_service_before_semantic_config(
    mock_get_config,
):
    """A missing embedding service fails before the semantic-config query."""
    graph = Mock()
    graph.server_config.supports_feature.return_value = False
    mock_get_config.side_effect = GraphError(
        "Server does not expose semantic search configuration."
    )

    with pytest.raises(ServerEmbeddingsUnavailableError):
        DocumentChunkingSource.resolve_embedding_config(
            EmbeddingConfig(provider="datahub"), graph=graph
        )

    mock_get_config.assert_not_called()
