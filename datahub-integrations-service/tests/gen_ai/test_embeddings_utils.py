"""Unit tests for embeddings utility functions."""

import os
from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.gen_ai.embeddings.litellm_wrapper import LiteLLMEmbeddings
from datahub_integrations.gen_ai.embeddings.utils import (
    batch_embed_texts,
    create_bedrock_embeddings,
    create_embeddings,
    get_model_info,
)


class TestCreateBedrockEmbeddings:
    """Test cases for create_bedrock_embeddings function."""

    def test_create_bedrock_embeddings_default(self) -> None:
        """Test creating Bedrock embeddings with default parameters."""
        embeddings = create_bedrock_embeddings()

        assert isinstance(embeddings, LiteLLMEmbeddings)
        assert embeddings.model_id == "bedrock/cohere.embed-english-v3"
        assert embeddings._aws_region_name == "us-west-2"
        assert embeddings._max_character_length == 2048

    def test_create_bedrock_embeddings_custom_model(self) -> None:
        """Test creating Bedrock embeddings with custom model."""
        embeddings = create_bedrock_embeddings(
            model_id="amazon.titan-embed-text-v2:0", region="us-east-1"
        )

        assert embeddings.model_id == "bedrock/amazon.titan-embed-text-v2:0"
        assert embeddings._aws_region_name == "us-east-1"

    def test_create_bedrock_embeddings_already_prefixed(self) -> None:
        """Test creating embeddings with model ID already prefixed."""
        embeddings = create_bedrock_embeddings(
            model_id="bedrock/cohere.embed-english-v3"
        )

        # Should not double-prefix
        assert embeddings.model_id == "bedrock/cohere.embed-english-v3"

    def test_create_bedrock_embeddings_character_limit(self) -> None:
        """Test that character limit is always set to 2048 for Bedrock Cohere."""
        embeddings = create_bedrock_embeddings()
        assert embeddings._max_character_length == 2048


class TestGetModelInfo:
    """Test cases for get_model_info function."""

    def test_get_model_info_cohere_english(self) -> None:
        """Test model info for Cohere English model."""
        info = get_model_info("cohere.embed-english-v3")

        expected = {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False,
            "allowed_dims": None,
        }
        assert info == expected

    def test_get_model_info_cohere_multilingual(self) -> None:
        """Test model info for Cohere multilingual model."""
        info = get_model_info("cohere.embed-multilingual-v3")

        expected = {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False,
            "allowed_dims": None,
        }
        assert info == expected

    def test_get_model_info_titan_v1(self) -> None:
        """Test model info for Titan v1 model."""
        info = get_model_info("amazon.titan-embed-text-v1")

        expected = {
            "dimensions": 1536,
            "max_tokens": 8192,
            "configurable_dims": False,
            "allowed_dims": None,
        }
        assert info == expected

    def test_get_model_info_titan_v2(self) -> None:
        """Test model info for Titan v2 model."""
        info = get_model_info("amazon.titan-embed-text-v2:0")

        expected = {
            "dimensions": 1024,
            "max_tokens": 8192,
            "configurable_dims": True,
            "allowed_dims": [256, 512, 1024],
        }
        assert info == expected

    def test_get_model_info_with_bedrock_prefix(self) -> None:
        """Test model info lookup with bedrock/ prefix."""
        info = get_model_info("bedrock/cohere.embed-english-v3")

        expected = {
            "dimensions": 1024,
            "max_tokens": 512,
            "configurable_dims": False,
            "allowed_dims": None,
        }
        assert info == expected

    def test_get_model_info_unknown_model(self) -> None:
        """Test model info for unknown model returns default."""
        info = get_model_info("unknown.model")

        expected = {
            "dimensions": -1,
            "max_tokens": -1,
            "configurable_dims": False,
            "allowed_dims": None,
        }
        assert info == expected


class TestBatchEmbedTexts:
    """Test cases for batch_embed_texts function."""

    def test_batch_embed_texts_single_batch(self) -> None:
        """Test batching with all texts fitting in one batch."""
        mock_embeddings = MagicMock(spec=LiteLLMEmbeddings)
        mock_embeddings.embed_documents.return_value = [
            [0.1, 0.2],
            [0.3, 0.4],
            [0.5, 0.6],
        ]

        texts = ["text1", "text2", "text3"]
        result = batch_embed_texts(mock_embeddings, texts, batch_size=5)

        assert result == [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
        mock_embeddings.embed_documents.assert_called_once_with(texts)

    def test_batch_embed_texts_multiple_batches(self) -> None:
        """Test batching with texts split across multiple batches."""
        mock_embeddings = MagicMock(spec=LiteLLMEmbeddings)

        # Mock responses for each batch
        def mock_embed_side_effect(batch_texts: list[str]) -> list[list[float]]:
            if batch_texts == ["text1", "text2"]:
                return [[0.1, 0.2], [0.3, 0.4]]
            elif batch_texts == ["text3", "text4"]:
                return [[0.5, 0.6], [0.7, 0.8]]
            elif batch_texts == ["text5"]:
                return [[0.9, 1.0]]
            return []

        mock_embeddings.embed_documents.side_effect = mock_embed_side_effect

        texts = ["text1", "text2", "text3", "text4", "text5"]
        result = batch_embed_texts(mock_embeddings, texts, batch_size=2)

        expected = [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6], [0.7, 0.8], [0.9, 1.0]]
        assert result == expected
        assert mock_embeddings.embed_documents.call_count == 3

    def test_batch_embed_texts_empty_list(self) -> None:
        """Test batching with empty text list."""
        mock_embeddings = MagicMock(spec=LiteLLMEmbeddings)

        result = batch_embed_texts(mock_embeddings, [], batch_size=5)

        assert result == []
        mock_embeddings.embed_documents.assert_not_called()

    def test_batch_embed_texts_default_batch_size(self) -> None:
        """Test that default batch size of 25 is used."""
        mock_embeddings = MagicMock(spec=LiteLLMEmbeddings)
        mock_embeddings.embed_documents.return_value = []

        # Create exactly 25 texts to test default batch size
        texts = [f"text{i}" for i in range(25)]
        batch_embed_texts(mock_embeddings, texts)

        # Should be called once with all 25 texts
        mock_embeddings.embed_documents.assert_called_once_with(texts)


class TestCreateEmbeddings:
    """Test cases for create_embeddings factory function."""

    def test_create_embeddings_default(self) -> None:
        """Test creating embeddings with default parameters."""
        embeddings = create_embeddings()

        assert isinstance(embeddings, LiteLLMEmbeddings)
        assert embeddings.model_id == "bedrock/cohere.embed-english-v3"

    def test_create_embeddings_explicit_params(self) -> None:
        """Test creating embeddings with explicit parameters."""
        embeddings = create_embeddings(
            provider="bedrock",
            model_id="amazon.titan-embed-text-v2:0",
            aws_region="us-east-1",
        )

        assert embeddings.model_id == "bedrock/amazon.titan-embed-text-v2:0"
        assert embeddings._aws_region_name == "us-east-1"

    def test_create_embeddings_unsupported_provider(self) -> None:
        """Test that unsupported provider raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported embedding provider: openai"):
            create_embeddings(provider="openai")

    @patch.dict(
        os.environ,
        {
            "EMBED_PROVIDER": "bedrock",
            "BEDROCK_MODEL": "cohere.embed-multilingual-v3",
            "BEDROCK_AWS_REGION": "eu-west-1",
        },
    )
    def test_create_embeddings_from_env_vars(self) -> None:
        """Test creating embeddings from environment variables."""
        embeddings = create_embeddings()

        assert embeddings.model_id == "bedrock/cohere.embed-multilingual-v3"
        assert embeddings._aws_region_name == "eu-west-1"

    @patch.dict(os.environ, {"AWS_REGION": "ap-southeast-1", "BEDROCK_AWS_REGION": ""})
    def test_create_embeddings_aws_region_fallback(self) -> None:
        """Test that AWS_REGION is used as fallback for region."""
        embeddings = create_embeddings()

        assert embeddings._aws_region_name == "ap-southeast-1"

    @patch.dict(
        os.environ, {"BEDROCK_AWS_REGION": "us-east-1", "AWS_REGION": "us-west-2"}
    )
    def test_create_embeddings_region_precedence(self) -> None:
        """Test that BEDROCK_AWS_REGION takes precedence over AWS_REGION."""
        embeddings = create_embeddings()

        assert embeddings._aws_region_name == "us-east-1"

    def test_create_embeddings_explicit_overrides_env(self) -> None:
        """Test that explicit parameters override environment variables."""
        with patch.dict(os.environ, {"BEDROCK_MODEL": "cohere.embed-english-v3"}):
            embeddings = create_embeddings(
                model_id="amazon.titan-embed-text-v2:0", aws_region="us-east-1"
            )

        assert embeddings.model_id == "bedrock/amazon.titan-embed-text-v2:0"
        assert embeddings._aws_region_name == "us-east-1"

    def test_create_embeddings_provider_env_var(self) -> None:
        """Test that provider can be set via environment variable."""
        with patch.dict(os.environ, {"EMBED_PROVIDER": "bedrock"}):
            embeddings = create_embeddings()
            # Should succeed and not raise ValueError
            assert isinstance(embeddings, LiteLLMEmbeddings)

        with patch.dict(os.environ, {"EMBED_PROVIDER": "invalid"}):
            with pytest.raises(
                ValueError, match="Unsupported embedding provider: invalid"
            ):
                create_embeddings()

    def test_create_embeddings_none_params(self) -> None:
        """Test that None parameters use defaults correctly."""
        embeddings = create_embeddings(provider=None, model_id=None, aws_region=None)

        assert isinstance(embeddings, LiteLLMEmbeddings)
        # Should use default values, not fail with None
        assert "bedrock/" in embeddings.model_id
