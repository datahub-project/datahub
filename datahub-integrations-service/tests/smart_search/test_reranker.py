"""
Unit tests for reranker module.

Tests the CohereBedrockReranker and create_reranker factory function.
Note: LLMReranker is tested separately in test_llm_reranker.py
"""

import json
from unittest.mock import MagicMock, patch

from datahub_integrations.smart_search.reranker import (
    CohereBedrockReranker,
    RerankResult,
    create_reranker,
)


class TestRerankResult:
    """Tests for RerankResult model."""

    def test_rerank_result_creation(self):
        """Test creating a RerankResult."""
        result = RerankResult(index=5, score=0.85)
        assert result.index == 5
        assert result.score == 0.85

    def test_rerank_result_validation(self):
        """Test that RerankResult validates types."""
        # Should work with valid types
        result = RerankResult(index=0, score=1.0)
        assert result.index == 0
        assert result.score == 1.0


class TestCohereBedrockRerankerInit:
    """Tests for CohereBedrockReranker initialization."""

    def test_init_with_default_model(self):
        """Test initialization with default model ID."""
        reranker = CohereBedrockReranker()
        assert reranker.model_id == "cohere.rerank-v3-5:0"

    def test_init_with_custom_model(self):
        """Test initialization with custom model ID."""
        reranker = CohereBedrockReranker(model_id="cohere.rerank-v4:0")
        assert reranker.model_id == "cohere.rerank-v4:0"


class TestCohereBedrockRerankerRerank:
    """Tests for CohereBedrockReranker.rerank method."""

    def test_rerank_empty_entities(self):
        """Test that empty entities list returns empty results."""
        reranker = CohereBedrockReranker()
        results = reranker.rerank(
            semantic_query="test query",
            entities=[],
            keyword_search_query="/q test",
        )
        assert results == []

    @patch(
        "datahub_integrations.smart_search.smart_search._extract_keywords_from_query"
    )
    @patch("datahub_integrations.smart_search.text_generator.EntityTextGenerator")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_client")
    def test_rerank_basic_functionality(
        self, mock_get_bedrock_client, mock_text_generator_class, mock_extract_keywords
    ):
        """Test basic reranking with Cohere."""
        # Setup mocks
        mock_extract_keywords.return_value = ["premium", "plan"]

        mock_generator = MagicMock()
        mock_generator.generate.side_effect = [
            "Dataset 1 with premium features",
            "Dataset 2 with basic plan",
        ]
        mock_text_generator_class.return_value = mock_generator

        mock_client = MagicMock()
        mock_response_body = {
            "results": [
                {"index": 0, "relevance_score": 0.95},
                {"index": 1, "relevance_score": 0.75},
            ]
        }
        mock_response = {
            "body": MagicMock(read=lambda: json.dumps(mock_response_body).encode())
        }
        mock_client.invoke_model.return_value = mock_response
        mock_get_bedrock_client.return_value = mock_client

        # Setup entities
        entities = [
            {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
            {"urn": "urn:li:dataset:2", "name": "Dataset 2"},
        ]

        # Execute
        reranker = CohereBedrockReranker(model_id="cohere.rerank-v3-5:0")
        results = reranker.rerank(
            semantic_query="find premium datasets",
            entities=entities,
            keyword_search_query="/q premium+plan",
        )

        # Verify
        assert len(results) == 2
        assert results[0].index == 0
        assert results[0].score == 0.95
        assert results[1].index == 1
        assert results[1].score == 0.75

        # Verify Bedrock was called correctly
        mock_client.invoke_model.assert_called_once()
        call_args = mock_client.invoke_model.call_args
        assert call_args[1]["modelId"] == "cohere.rerank-v3-5:0"

        # Verify request body
        body = json.loads(call_args[1]["body"])
        assert body["api_version"] == 2
        assert body["query"] == "find premium datasets"
        assert len(body["documents"]) == 2
        assert body["top_n"] == 2

    @patch(
        "datahub_integrations.smart_search.smart_search._extract_keywords_from_query"
    )
    @patch("datahub_integrations.smart_search.text_generator.EntityTextGenerator")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_client")
    def test_rerank_generates_text_for_entities(
        self, mock_get_bedrock_client, mock_text_generator_class, mock_extract_keywords
    ):
        """Test that text is generated for each entity."""
        # Setup mocks
        mock_extract_keywords.return_value = ["premium"]

        mock_generator = MagicMock()
        mock_generator.generate.side_effect = ["Text 1", "Text 2", "Text 3"]
        mock_text_generator_class.return_value = mock_generator

        mock_client = MagicMock()
        mock_response = {
            "body": MagicMock(
                read=lambda: json.dumps(
                    {
                        "results": [
                            {"index": 0, "relevance_score": 0.9},
                            {"index": 1, "relevance_score": 0.8},
                            {"index": 2, "relevance_score": 0.7},
                        ]
                    }
                ).encode()
            )
        }
        mock_client.invoke_model.return_value = mock_response
        mock_get_bedrock_client.return_value = mock_client

        # Setup entities
        entities = [
            {"urn": f"urn:li:dataset:{i}", "name": f"Dataset {i}"} for i in range(3)
        ]

        # Execute
        reranker = CohereBedrockReranker()
        reranker.rerank(
            semantic_query="query",
            entities=entities,
            keyword_search_query="/q premium",
        )

        # Verify text generator was called for each entity
        assert mock_generator.generate.call_count == 3
        for i, call in enumerate(mock_generator.generate.call_args_list):
            assert call[0][0] == entities[i]
            assert call[1]["search_keywords"] == ["premium"]

    @patch(
        "datahub_integrations.smart_search.smart_search._extract_keywords_from_query"
    )
    @patch("datahub_integrations.smart_search.text_generator.EntityTextGenerator")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_client")
    def test_rerank_requests_all_documents(
        self, mock_get_bedrock_client, mock_text_generator_class, mock_extract_keywords
    ):
        """Test that top_n equals number of documents."""
        # Setup mocks
        mock_extract_keywords.return_value = []
        mock_generator = MagicMock()
        mock_generator.generate.return_value = "text"
        mock_text_generator_class.return_value = mock_generator

        mock_client = MagicMock()
        mock_response = {
            "body": MagicMock(
                read=lambda: json.dumps(
                    {
                        "results": [
                            {"index": 0, "relevance_score": 0.9},
                            {"index": 1, "relevance_score": 0.8},
                        ]
                    }
                ).encode()
            )
        }
        mock_client.invoke_model.return_value = mock_response
        mock_get_bedrock_client.return_value = mock_client

        # Setup entities
        entities = [
            {"urn": "urn:li:dataset:1"},
            {"urn": "urn:li:dataset:2"},
        ]

        # Execute
        reranker = CohereBedrockReranker()
        reranker.rerank(
            semantic_query="query",
            entities=entities,
            keyword_search_query="/q test",
        )

        # Verify top_n equals number of documents
        call_args = mock_client.invoke_model.call_args
        body = json.loads(call_args[1]["body"])
        assert body["top_n"] == len(entities)

    @patch("datahub_integrations.smart_search.reranker.logger")
    @patch(
        "datahub_integrations.smart_search.smart_search._extract_keywords_from_query"
    )
    @patch("datahub_integrations.smart_search.text_generator.EntityTextGenerator")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_client")
    def test_rerank_logs_appropriately(
        self,
        mock_get_bedrock_client,
        mock_text_generator_class,
        mock_extract_keywords,
        mock_logger,
    ):
        """Test that reranker logs key information."""
        # Setup mocks
        mock_extract_keywords.return_value = []
        mock_generator = MagicMock()
        mock_generator.generate.return_value = "text"
        mock_text_generator_class.return_value = mock_generator

        mock_client = MagicMock()
        mock_response = {
            "body": MagicMock(
                read=lambda: json.dumps(
                    {"results": [{"index": 0, "relevance_score": 0.9}]}
                ).encode()
            )
        }
        mock_client.invoke_model.return_value = mock_response
        mock_get_bedrock_client.return_value = mock_client

        entities = [{"urn": "urn:li:dataset:1"}]

        # Execute
        reranker = CohereBedrockReranker()
        reranker.rerank(
            semantic_query="query",
            entities=entities,
            keyword_search_query="/q test",
        )

        # Verify logging
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)
        assert "Reranking" in log_messages
        assert "Cohere" in log_messages


class TestCreateReranker:
    """Tests for create_reranker factory function."""

    @patch.dict("os.environ", {}, clear=True)
    @patch("datahub_integrations.smart_search.reranker.logger")
    def test_create_reranker_default_cohere(self, mock_logger):
        """Test that default creates Cohere reranker."""
        reranker = create_reranker()

        assert isinstance(reranker, CohereBedrockReranker)
        assert reranker.model_id == "cohere.rerank-v3-5:0"

        # Verify logging
        mock_logger.info.assert_called_once()
        log_msg = mock_logger.info.call_args[0][0]
        assert "Cohere Rerank" in log_msg

    @patch.dict("os.environ", {"SMART_SEARCH_RERANK_MODEL": "cohere.rerank-v4:0"})
    @patch("datahub_integrations.smart_search.reranker.logger")
    def test_create_reranker_custom_cohere(self, mock_logger):
        """Test creating Cohere reranker with custom model."""
        reranker = create_reranker()

        assert isinstance(reranker, CohereBedrockReranker)
        assert reranker.model_id == "cohere.rerank-v4:0"

    @patch.dict("os.environ", {"SMART_SEARCH_RERANK_MODEL": "CLAUDE_37_SONNET"})
    @patch("datahub_integrations.smart_search.reranker.logger")
    @patch("datahub_integrations.smart_search.llm_reranker.LLMReranker")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_model_env_variable")
    def test_create_reranker_llm_with_enum(
        self, mock_get_model_env_var, mock_llm_reranker_class, mock_logger
    ):
        """Test creating LLM reranker with BedrockModel enum name."""
        from datahub_integrations.gen_ai.model_config import BedrockModel

        # Setup mock
        mock_get_model_env_var.return_value = BedrockModel.CLAUDE_37_SONNET
        mock_llm_instance = MagicMock()
        mock_llm_reranker_class.return_value = mock_llm_instance

        # Execute
        reranker = create_reranker()

        # Verify LLMReranker was created
        mock_llm_reranker_class.assert_called_once_with(
            model=BedrockModel.CLAUDE_37_SONNET
        )
        assert reranker == mock_llm_instance

        # Verify logging
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)
        assert "LLM reranker" in log_messages

    @patch.dict(
        "os.environ",
        {"SMART_SEARCH_RERANK_MODEL": "us.anthropic.claude-3-7-sonnet-20250219-v1:0"},
    )
    @patch("datahub_integrations.smart_search.llm_reranker.LLMReranker")
    @patch("datahub_integrations.smart_search.reranker.get_bedrock_model_env_variable")
    def test_create_reranker_llm_with_explicit_model_id(
        self, mock_get_model_env_var, mock_llm_reranker_class
    ):
        """Test creating LLM reranker with explicit model ID."""
        # Setup mock
        mock_get_model_env_var.return_value = (
            "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        mock_llm_instance = MagicMock()
        mock_llm_reranker_class.return_value = mock_llm_instance

        # Execute
        reranker = create_reranker()

        # Verify LLMReranker was created with explicit model
        mock_llm_reranker_class.assert_called_once()
        assert reranker == mock_llm_instance

    @patch.dict("os.environ", {"SMART_SEARCH_RERANK_MODEL": "cohere.rerank-v3-5:0"})
    def test_create_reranker_identifies_cohere_by_prefix(self):
        """Test that models starting with 'cohere.rerank' use Cohere."""
        reranker = create_reranker()

        assert isinstance(reranker, CohereBedrockReranker)
        assert reranker.model_id == "cohere.rerank-v3-5:0"
