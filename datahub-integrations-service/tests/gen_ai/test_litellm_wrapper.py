"""Unit tests for LiteLLMEmbeddings wrapper class."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.gen_ai.embeddings.litellm_wrapper import LiteLLMEmbeddings


class TestLiteLLMEmbeddings:
    """Test cases for LiteLLMEmbeddings class."""

    def test_init_basic(self) -> None:
        """Test basic initialization."""
        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        assert embeddings._model_id == "bedrock/cohere.embed-english-v3"
        assert embeddings._aws_region_name is None
        assert embeddings._default_kwargs == {}
        assert embeddings._max_character_length is None

    def test_init_with_all_params(self) -> None:
        """Test initialization with all parameters."""
        default_kwargs = {"temperature": 0.5}
        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/amazon.titan-embed-text-v2:0",
            aws_region_name="us-east-1",
            default_kwargs=default_kwargs,
            max_character_length=1024,
        )

        assert embeddings._model_id == "bedrock/amazon.titan-embed-text-v2:0"
        assert embeddings._aws_region_name == "us-east-1"
        assert embeddings._default_kwargs == default_kwargs
        assert embeddings._max_character_length == 1024

    def test_model_id_property(self) -> None:
        """Test model_id property returns correct value."""
        model_id = "bedrock/cohere.embed-english-v3"
        embeddings = LiteLLMEmbeddings(model_id=model_id)

        assert embeddings.model_id == model_id

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_documents_basic(self, mock_litellm: MagicMock) -> None:
        """Test basic document embedding."""
        # Mock response
        mock_response = MagicMock()
        mock_response.data = [
            MagicMock(embedding=[0.1, 0.2, 0.3]),
            MagicMock(embedding=[0.4, 0.5, 0.6]),
        ]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")
        texts = ["doc1", "doc2"]

        result = embeddings.embed_documents(texts)

        assert result == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
        mock_litellm.assert_called_once_with(
            model="bedrock/cohere.embed-english-v3",
            input=texts,
            input_type="search_document",
            truncate="END",
        )

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_documents_empty_list(self, mock_litellm: MagicMock) -> None:
        """Test embed_documents with empty list."""
        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        result = embeddings.embed_documents([])

        assert result == []
        mock_litellm.assert_not_called()

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_documents_with_character_limit(
        self, mock_litellm: MagicMock
    ) -> None:
        """Test embed_documents with character length limit."""
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.1, 0.2])]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/cohere.embed-english-v3", max_character_length=5
        )

        result = embeddings.embed_documents(["this is too long"])

        assert result == [[0.1, 0.2]]
        # Should truncate to first 5 characters
        mock_litellm.assert_called_once_with(
            model="bedrock/cohere.embed-english-v3",
            input=["this "],
            input_type="search_document",
            truncate="END",
        )

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_query_basic(self, mock_litellm: MagicMock) -> None:
        """Test basic query embedding."""
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.1, 0.2, 0.3])]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        result = embeddings.embed_query("test query")

        assert result == [0.1, 0.2, 0.3]
        mock_litellm.assert_called_once_with(
            model="bedrock/cohere.embed-english-v3",
            input="test query",
            input_type="search_query",
            truncate="END",
        )

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_query_with_character_limit(self, mock_litellm: MagicMock) -> None:
        """Test embed_query with character length limit."""
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=[0.1, 0.2])]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/cohere.embed-english-v3", max_character_length=5
        )

        result = embeddings.embed_query("this is too long")

        assert result == [0.1, 0.2]
        mock_litellm.assert_called_once_with(
            model="bedrock/cohere.embed-english-v3",
            input="this ",
            input_type="search_query",
            truncate="END",
        )

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_query_dict_response(self, mock_litellm: MagicMock) -> None:
        """Test embed_query with dict-style response."""
        mock_response = MagicMock()
        mock_response.data = [{"embedding": [0.1, 0.2, 0.3]}]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/amazon.titan-embed-text-v2:0")

        result = embeddings.embed_query("test query")

        assert result == [0.1, 0.2, 0.3]

    def test_build_call_kwargs_cohere_model_query(self) -> None:
        """Test _build_call_kwargs for Cohere model with query."""
        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/cohere.embed-english-v3",
            aws_region_name="us-east-1",
        )

        kwargs = embeddings._build_call_kwargs(is_query=True)

        assert kwargs == {
            "aws_region_name": "us-east-1",
            "input_type": "search_query",
            "truncate": "END",
        }

    def test_build_call_kwargs_cohere_model_document(self) -> None:
        """Test _build_call_kwargs for Cohere model with document."""
        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/cohere.embed-english-v3",
            aws_region_name="us-east-1",
        )

        kwargs = embeddings._build_call_kwargs(is_query=False)

        assert kwargs == {
            "aws_region_name": "us-east-1",
            "input_type": "search_document",
            "truncate": "END",
        }

    def test_build_call_kwargs_titan_model(self) -> None:
        """Test _build_call_kwargs for non-Cohere model."""
        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/amazon.titan-embed-text-v2:0",
            aws_region_name="us-west-2",
        )

        kwargs = embeddings._build_call_kwargs(is_query=True)

        assert kwargs == {
            "aws_region_name": "us-west-2",
        }

    def test_build_call_kwargs_no_region(self) -> None:
        """Test _build_call_kwargs without AWS region."""
        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        kwargs = embeddings._build_call_kwargs(is_query=True)

        assert kwargs == {
            "input_type": "search_query",
            "truncate": "END",
        }

    def test_build_call_kwargs_with_defaults(self) -> None:
        """Test _build_call_kwargs with default kwargs."""
        default_kwargs = {"custom_param": "value", "input_type": "custom"}
        embeddings = LiteLLMEmbeddings(
            model_id="bedrock/cohere.embed-english-v3",
            default_kwargs=default_kwargs,
        )

        kwargs = embeddings._build_call_kwargs(is_query=True)

        # Should merge but not override existing defaults
        assert kwargs == {
            "custom_param": "value",
            "input_type": "custom",  # Default not overridden
            "truncate": "END",
        }

    def test_build_call_kwargs_case_insensitive_cohere_detection(self) -> None:
        """Test that Cohere model detection is case insensitive."""
        embeddings = LiteLLMEmbeddings(model_id="bedrock/COHERE.EMBED-english-v3")

        kwargs = embeddings._build_call_kwargs(is_query=True)

        assert "input_type" in kwargs
        assert kwargs["input_type"] == "search_query"

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_query_invalid_response_format(self, mock_litellm: MagicMock) -> None:
        """Test that invalid response format raises ValueError."""
        mock_response = MagicMock()
        mock_response.data = [{"embedding": "invalid_format"}]  # String instead of list
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        with pytest.raises(ValueError, match="Expected List\\[float\\] from litellm"):
            embeddings.embed_query("test query")

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_documents_invalid_response_format(
        self, mock_litellm: MagicMock
    ) -> None:
        """Test that invalid response format in documents raises ValueError."""
        mock_response = MagicMock()
        mock_response.data = [
            MagicMock(embedding=[0.1, 0.2]),  # Valid
            {"embedding": ["not", "numbers"]},  # Invalid - strings instead of floats
        ]
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/cohere.embed-english-v3")

        with pytest.raises(ValueError, match="Expected List\\[float\\] from litellm"):
            embeddings.embed_documents(["doc1", "doc2"])

    @patch("datahub_integrations.gen_ai.embeddings.litellm_wrapper.litellm_embedding")
    def test_embed_query_non_list_response(self, mock_litellm: MagicMock) -> None:
        """Test that non-list embedding response raises ValueError."""
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=42)]  # Number instead of list
        mock_litellm.return_value = mock_response

        embeddings = LiteLLMEmbeddings(model_id="bedrock/titan-embed-v2")

        with pytest.raises(
            ValueError, match="Expected List\\[float\\] from litellm, got <class 'int'>"
        ):
            embeddings.embed_query("test query")
