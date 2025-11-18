"""
Unit tests for LLMReranker.

Tests the LLM-based reranker that uses Claude via Bedrock to score entities
by semantic relevance without text generation.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.smart_search.llm_reranker import LLMReranker


class TestLLMRerankerInit:
    """Tests for LLMReranker initialization."""

    def test_init_with_string_model(self):
        """Test initialization with string model ID."""
        reranker = LLMReranker(model="us.anthropic.claude-3-7-sonnet-20250219-v1:0")
        assert reranker.model_id == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

    def test_init_with_enum_model(self):
        """Test initialization with BedrockModel enum."""
        from datahub_integrations.gen_ai.model_config import BedrockModel

        reranker = LLMReranker(model=BedrockModel.CLAUDE_37_SONNET)
        # Enum should be converted to string via str()
        assert isinstance(reranker.model_id, str)
        assert "claude" in reranker.model_id.lower()


class TestLLMRerankerRerank:
    """Tests for LLMReranker.rerank method."""

    def test_rerank_empty_entities(self):
        """Test that empty entities list returns empty results."""
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="test query",
            entities=[],
            keyword_search_query="/q test",
        )
        assert results == []

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_basic_functionality(self, mock_get_llm_client):
        """Test basic reranking with valid response."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response from Bedrock
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.95,
                                        },
                                        {
                                            "urn": "urn:li:dataset:2",
                                            "score": 0.75,
                                        },
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Setup entities
        entities = [
            {
                "urn": "urn:li:dataset:1",
                "name": "Dataset 1",
                "description": "First dataset",
            },
            {
                "urn": "urn:li:dataset:2",
                "name": "Dataset 2",
                "description": "Second dataset",
            },
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="find datasets",
            entities=entities,
            keyword_search_query="/q datasets",
        )

        # Verify
        assert len(results) == 2
        assert results[0].index == 0
        assert results[0].score == 0.95
        assert results[1].index == 1
        assert results[1].score == 0.75

        # Verify bedrock was called
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        assert call_args[1]["modelId"] == "test-model"
        assert call_args[1]["inferenceConfig"]["temperature"] == 0.0

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_results_sorted_by_score(self, mock_get_llm_client):
        """Test that results are sorted by score in descending order."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response with unsorted scores
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.5,
                                        },
                                        {
                                            "urn": "urn:li:dataset:2",
                                            "score": 0.9,
                                        },
                                        {
                                            "urn": "urn:li:dataset:3",
                                            "score": 0.7,
                                        },
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Setup entities
        entities = [
            {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
            {"urn": "urn:li:dataset:2", "name": "Dataset 2"},
            {"urn": "urn:li:dataset:3", "name": "Dataset 3"},
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify sorted by score descending
        assert len(results) == 3
        assert results[0].score == 0.9
        assert results[0].index == 1
        assert results[1].score == 0.7
        assert results[1].index == 2
        assert results[2].score == 0.5
        assert results[2].index == 0

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    @patch("datahub_integrations.smart_search.llm_reranker.logger")
    def test_rerank_caps_at_100_entities(self, mock_logger, mock_get_llm_client):
        """Test that reranker caps entities at 100 and logs warning."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response with first 100 entities
        ranked_entities = [
            {"urn": f"urn:li:dataset:{i}", "score": 1.0 - (i * 0.01)}
            for i in range(100)
        ]
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {"toolUse": {"input": {"ranked_entities": ranked_entities}}}
                    ]
                }
            }
        }

        # Create 150 entities
        entities = [
            {"urn": f"urn:li:dataset:{i}", "name": f"Dataset {i}"} for i in range(150)
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify capped at 100
        assert len(results) == 100

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "100 entities" in warning_msg
        assert "received 150" in warning_msg

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_with_schema_metadata(self, mock_get_llm_client):
        """Test that schema fields are included in entity summaries."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Entity with schema fields
        entities = [
            {
                "urn": "urn:li:dataset:1",
                "name": "Dataset 1",
                "description": "Test dataset",
                "schemaMetadata": {
                    "fields": [
                        {"fieldPath": "field1"},
                        {"fieldPath": "field2"},
                        {"fieldPath": "field3"},
                    ]
                },
            }
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify call was made and prompt includes fields
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        prompt = call_args[1]["messages"][0]["content"][0]["text"]
        assert "field1" in prompt
        assert "field2" in prompt
        assert "field3" in prompt

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_caps_field_count_at_10(self, mock_get_llm_client):
        """Test that only first 10 fields are included in summaries."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Entity with 15 fields
        fields = [{"fieldPath": f"field{i}"} for i in range(15)]
        entities = [
            {
                "urn": "urn:li:dataset:1",
                "name": "Dataset 1",
                "schemaMetadata": {"fields": fields},
            }
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify call was made and prompt includes only first 10 fields
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        prompt = call_args[1]["messages"][0]["content"][0]["text"]
        assert "field9" in prompt  # 10th field
        assert "field10" not in prompt  # 11th field should not be included

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_truncates_long_descriptions(self, mock_get_llm_client):
        """Test that descriptions are truncated at 4000 chars."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Entity with very long description
        long_description = "x" * 5000
        entities = [
            {
                "urn": "urn:li:dataset:1",
                "name": "Dataset 1",
                "description": long_description,
            }
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify call was made and description was truncated
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        prompt = call_args[1]["messages"][0]["content"][0]["text"]
        # Should have truncated description
        # Extract the entity summaries from prompt
        assert "xxxx" in prompt  # Should have some x's
        # Count x's in prompt - should be around 4000, not 5000
        x_count = prompt.count("x")
        assert x_count < 4500  # Less than 4500 to account for truncation

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_handles_missing_description(self, mock_get_llm_client):
        """Test that entities without descriptions get default text."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Entity without description
        entities = [
            {
                "urn": "urn:li:dataset:1",
                "name": "Dataset 1",
            }
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify call was made and includes "No description"
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        prompt = call_args[1]["messages"][0]["content"][0]["text"]
        assert "No description" in prompt

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    @patch("datahub_integrations.smart_search.llm_reranker.logger")
    def test_rerank_handles_unknown_urn_from_llm(
        self, mock_logger, mock_get_llm_client
    ):
        """Test that unknown URNs returned by LLM are logged and ignored."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response includes unknown URN
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        },
                                        {
                                            "urn": "urn:li:dataset:999",  # Unknown URN
                                            "score": 0.8,
                                        },
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Setup entities (without dataset:999)
        entities = [
            {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify only valid URN is in results
        assert len(results) == 1
        assert results[0].index == 0

        # Verify warning was logged
        mock_logger.warning.assert_any_call(
            "LLM returned unknown URN: urn:li:dataset:999"
        )

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    @patch("datahub_integrations.smart_search.llm_reranker.logger")
    def test_rerank_handles_missing_entities_in_llm_response(
        self, mock_logger, mock_get_llm_client
    ):
        """Test that entities not scored by LLM get score of 0.0."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response only scores first entity
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                        # dataset:2 not scored
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Setup entities
        entities = [
            {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
            {"urn": "urn:li:dataset:2", "name": "Dataset 2"},
        ]

        # Execute
        reranker = LLMReranker(model="test-model")
        results = reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify both entities are in results
        assert len(results) == 2

        # First should be scored entity
        assert results[0].index == 0
        assert results[0].score == 0.9

        # Second should be missing entity with score 0.0
        assert results[1].index == 1
        assert results[1].score == 0.0

        # Verify warning was logged
        mock_logger.warning.assert_any_call(
            "LLM did not score entity: urn:li:dataset:2"
        )

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_raises_error_when_no_tool_use(self, mock_get_llm_client):
        """Test that error is raised when LLM doesn't use the tool."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        # Mock response without tool use
        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "text": "I cannot help with that."  # Text response instead of tool use
                        }
                    ]
                }
            }
        }

        # Setup entities
        entities = [
            {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
        ]

        # Execute and expect error
        reranker = LLMReranker(model="test-model")
        with pytest.raises(ValueError, match="LLM did not use rank_entities tool"):
            reranker.rerank(
                semantic_query="query",
                entities=entities,
                keyword_search_query="/q query",
            )

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_tool_config_structure(self, mock_get_llm_client):
        """Test that the tool configuration has the correct structure."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        entities = [{"urn": "urn:li:dataset:1", "name": "Dataset 1"}]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify tool config structure
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        tool_config = call_args[1]["toolConfig"]

        assert "tools" in tool_config
        assert len(tool_config["tools"]) == 1

        tool = tool_config["tools"][0]["toolSpec"]
        assert tool["name"] == "rank_entities"
        assert "description" in tool
        assert "inputSchema" in tool

        # Verify schema structure
        schema = tool["inputSchema"]["json"]
        assert schema["type"] == "object"
        assert "ranked_entities" in schema["properties"]
        assert schema["properties"]["ranked_entities"]["type"] == "array"

        # Verify item schema
        items = schema["properties"]["ranked_entities"]["items"]
        assert items["type"] == "object"
        assert "urn" in items["properties"]
        assert "score" in items["properties"]
        assert items["properties"]["score"]["minimum"] == 0.0
        assert items["properties"]["score"]["maximum"] == 1.0

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    def test_rerank_inference_config(self, mock_get_llm_client):
        """Test that inference config has correct temperature and token settings."""
        # Setup mock
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        entities = [{"urn": "urn:li:dataset:1", "name": "Dataset 1"}]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify inference config
        mock_client.converse.assert_called_once()
        call_args = mock_client.converse.call_args
        inference_config = call_args[1]["inferenceConfig"]

        assert inference_config["temperature"] == 0.0
        assert inference_config["maxTokens"] == 8192

    @patch("datahub_integrations.smart_search.llm_reranker.get_llm_client")
    @patch("datahub_integrations.smart_search.llm_reranker.EntityNormalizer")
    def test_rerank_uses_entity_normalizer(self, mock_normalizer, mock_get_llm_client):
        """Test that EntityNormalizer is used to extract name and description."""
        # Setup mocks
        mock_client = MagicMock()
        mock_get_llm_client.return_value = mock_client

        mock_normalizer.get_name.return_value = "Normalized Name"
        mock_normalizer.get_description.return_value = "Normalized Description"

        mock_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "ranked_entities": [
                                        {
                                            "urn": "urn:li:dataset:1",
                                            "score": 0.9,
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }

        entities = [{"urn": "urn:li:dataset:1"}]

        # Execute
        reranker = LLMReranker(model="test-model")
        reranker.rerank(
            semantic_query="query", entities=entities, keyword_search_query="/q query"
        )

        # Verify EntityNormalizer was used
        mock_normalizer.get_name.assert_called_once_with(entities[0])
        mock_normalizer.get_description.assert_called_once_with(entities[0])

        # Verify normalized values appear in prompt
        call_args = mock_client.converse.call_args
        prompt = call_args[1]["messages"][0]["content"][0]["text"]
        assert "Normalized Name" in prompt
        assert "Normalized Description" in prompt
