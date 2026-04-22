"""Unit tests for embedding configuration validation."""

import pytest
from pydantic import SecretStr

from datahub.ingestion.source.unstructured.chunking_config import (
    EmbeddingConfig,
    ServerEmbeddingConfig,
)


def test_has_local_config_with_provider():
    """Test has_local_config returns True when provider is set."""
    config = EmbeddingConfig(provider="bedrock")
    assert config.has_local_config() is True


def test_has_local_config_with_model():
    """Test has_local_config returns True when model is set."""
    config = EmbeddingConfig(model="cohere.embed-english-v3")
    assert config.has_local_config() is True


def test_has_local_config_with_both():
    """Test has_local_config returns True when both provider and model are set."""
    config = EmbeddingConfig(provider="bedrock", model="cohere.embed-english-v3")
    assert config.has_local_config() is True


def test_has_local_config_empty():
    """Test has_local_config returns False when no config is set."""
    config = EmbeddingConfig()
    assert config.has_local_config() is False


def test_from_server_bedrock():
    """Test creating EmbeddingConfig from server config (Bedrock)."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig.from_server(server_config)

    assert config.provider == "bedrock"
    assert config.model == "cohere.embed-english-v3"
    assert config.aws_region == "us-west-2"
    assert config._server_config == server_config


def test_from_server_cohere():
    """Test creating EmbeddingConfig from server config (Cohere)."""
    server_config = ServerEmbeddingConfig(
        provider="cohere",
        model_id="embed-multilingual-v3.0",
        aws_region=None,
        model_embedding_key="cohere_embed_multilingual_v3",
    )

    config = EmbeddingConfig.from_server(server_config, api_key=SecretStr("test-key"))

    assert config.provider == "cohere"
    assert config.model == "embed-multilingual-v3.0"
    assert config.aws_region is None
    assert (
        config.api_key is not None and config.api_key.get_secret_value() == "test-key"
    )
    assert config._server_config == server_config


def test_validate_against_server_success_bedrock():
    """Test successful validation with matching Bedrock config."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-english-v3",
        model_embedding_key="cohere_embed_v3",
        aws_region="us-west-2",
    )

    # Should not raise
    config.validate_against_server(server_config)
    assert config._server_config == server_config


def test_validate_against_server_success_cohere():
    """Test successful validation with matching Cohere config."""
    server_config = ServerEmbeddingConfig(
        provider="cohere",
        model_id="embed-english-v3.0",
        aws_region=None,
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="cohere",
        model="embed-english-v3.0",
        model_embedding_key="cohere_embed_v3",
    )

    # Should not raise
    config.validate_against_server(server_config)
    assert config._server_config == server_config


def test_validate_against_server_provider_mismatch():
    """Test validation fails when providers don't match."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="cohere",
        model="cohere.embed-english-v3",
        model_embedding_key="cohere_embed_v3",
    )

    with pytest.raises(ValueError, match="Provider mismatch"):
        config.validate_against_server(server_config)


def test_validate_against_server_model_mismatch():
    """Test validation fails when models don't match."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-multilingual-v3",
        model_embedding_key="cohere_embed_v3",
        aws_region="us-west-2",
    )

    with pytest.raises(ValueError, match="Model mismatch"):
        config.validate_against_server(server_config)


def test_validate_against_server_region_mismatch():
    """Test validation fails when AWS regions don't match (Bedrock only)."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="bedrock",
        model="cohere.embed-english-v3",
        model_embedding_key="cohere_embed_v3",
        aws_region="us-east-1",
    )

    with pytest.raises(ValueError, match="AWS region mismatch"):
        config.validate_against_server(server_config)


def test_validate_against_server_multiple_errors():
    """Test validation fails with multiple mismatches and shows all errors."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="cohere",
        model="embed-multilingual-v3.0",
        model_embedding_key="cohere_embed_multilingual_v3",
        aws_region="us-east-1",
    )

    with pytest.raises(ValueError) as exc_info:
        config.validate_against_server(server_config)

    error_msg = str(exc_info.value)
    assert "Provider mismatch" in error_msg
    assert "Model mismatch" in error_msg
    assert "Server configuration:" in error_msg
    assert "To fix this issue:" in error_msg


def test_validate_against_server_error_includes_fix_suggestions():
    """Test validation error includes actionable fix suggestions."""
    server_config = ServerEmbeddingConfig(
        provider="aws-bedrock",
        model_id="cohere.embed-english-v3",
        aws_region="us-west-2",
        model_embedding_key="cohere_embed_v3",
    )

    config = EmbeddingConfig(
        provider="cohere",
        model="cohere.embed-english-v3",
        model_embedding_key="cohere_embed_v3",
    )

    with pytest.raises(ValueError) as exc_info:
        config.validate_against_server(server_config)

    error_msg = str(exc_info.value)
    assert "Update your local config to match the server configuration" in error_msg
    assert "Update the server's semantic search configuration" in error_msg
    assert "allow_local_embedding_config: true" in error_msg


def test_normalize_provider_bedrock_variants():
    """Test provider normalization for Bedrock variants."""
    assert EmbeddingConfig._normalize_provider("bedrock") == "bedrock"
    assert EmbeddingConfig._normalize_provider("Bedrock") == "bedrock"
    assert EmbeddingConfig._normalize_provider("BEDROCK") == "bedrock"
    assert EmbeddingConfig._normalize_provider("aws-bedrock") == "bedrock"
    assert EmbeddingConfig._normalize_provider("AWS-Bedrock") == "bedrock"


def test_normalize_provider_cohere_variants():
    """Test provider normalization for Cohere variants."""
    assert EmbeddingConfig._normalize_provider("cohere") == "cohere"
    assert EmbeddingConfig._normalize_provider("Cohere") == "cohere"
    assert EmbeddingConfig._normalize_provider("COHERE") == "cohere"


def test_normalize_provider_unknown():
    """Test provider normalization for unknown providers returns as-is."""
    assert EmbeddingConfig._normalize_provider("openai") == "openai"
    assert EmbeddingConfig._normalize_provider("custom-provider") == "custom-provider"


def test_normalize_provider_from_server_bedrock():
    """Test converting server provider format to local format (Bedrock)."""
    assert EmbeddingConfig._normalize_provider_from_server("aws-bedrock") == "bedrock"
    assert EmbeddingConfig._normalize_provider_from_server("bedrock") == "bedrock"
    assert EmbeddingConfig._normalize_provider_from_server("BEDROCK") == "bedrock"


def test_normalize_provider_from_server_cohere():
    """Test converting server provider format to local format (Cohere)."""
    assert EmbeddingConfig._normalize_provider_from_server("cohere") == "cohere"
    assert EmbeddingConfig._normalize_provider_from_server("Cohere") == "cohere"
    assert EmbeddingConfig._normalize_provider_from_server("COHERE") == "cohere"


def test_normalize_provider_from_server_openai():
    """Test converting server provider format to local format (OpenAI)."""
    assert EmbeddingConfig._normalize_provider_from_server("openai") == "openai"
    assert EmbeddingConfig._normalize_provider_from_server("OpenAI") == "openai"
    assert EmbeddingConfig._normalize_provider_from_server("OPENAI") == "openai"


def test_normalize_provider_from_server_unsupported():
    """Test converting unsupported provider raises error."""
    with pytest.raises(ValueError, match="Unsupported provider from server"):
        EmbeddingConfig._normalize_provider_from_server("azure")


def test_default_values():
    """Test default values for EmbeddingConfig."""
    config = EmbeddingConfig()

    assert config.provider is None
    assert config.model is None
    assert config.aws_region is None
    assert config.api_key is None
    assert config.batch_size == 25
    assert config.input_type == "search_document"
    assert config.allow_local_embedding_config is False
    assert config._server_config is None


def test_allow_local_embedding_config_flag():
    """Test allow_local_embedding_config flag can be set."""
    config = EmbeddingConfig(allow_local_embedding_config=True)
    assert config.allow_local_embedding_config is True

    config2 = EmbeddingConfig(allow_local_embedding_config=False)
    assert config2.allow_local_embedding_config is False


def test_get_default_config():
    """Test get_default_config returns sensible defaults."""
    config = EmbeddingConfig.get_default_config()

    assert config.provider == "bedrock"
    assert config.model == "cohere.embed-english-v3"
    assert config.model_embedding_key == "cohere_embed_v3"
    assert config.aws_region == "us-west-2"
    assert config.allow_local_embedding_config is True
    assert config.batch_size == 25
    assert config.input_type == "search_document"


def test_get_default_config_has_local_config():
    """Test get_default_config returns config that has_local_config."""
    config = EmbeddingConfig.get_default_config()

    # Default config should register as having local config
    assert config.has_local_config() is True
