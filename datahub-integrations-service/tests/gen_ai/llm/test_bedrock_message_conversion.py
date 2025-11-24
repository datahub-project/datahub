"""
Comprehensive tests for _convert_bedrock_messages_to_langchain.

Tests cover:
- System message conversion
- User message conversion (with multiple text blocks)
- Assistant message conversion (with tool calls)
- Tool result conversion
- CachePoint marker filtering
"""

from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from datahub_integrations.gen_ai.llm.base import LLMWrapper


class MockLLMWrapper(LLMWrapper):
    """Mock LLM wrapper for testing base class methods."""

    def _initialize_client(self):
        return None

    @property
    def exceptions(self):
        return Exception

    def converse(self, system, messages, toolConfig=None, inferenceConfig=None):
        raise NotImplementedError


class TestConvertBedrockMessagesToLangchain:
    """Tests for _convert_bedrock_messages_to_langchain helper method."""

    def test_system_messages_conversion(self) -> None:
        """Test that Bedrock system messages are converted to SystemMessage objects."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = [
            {"text": "You are a helpful assistant"},
            {"text": "Always be concise"},
        ]
        messages: list[Any] = []

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        # Should have 2 SystemMessage objects
        system_messages = [msg for msg in result if isinstance(msg, SystemMessage)]
        assert len(system_messages) == 2
        assert system_messages[0].content == "You are a helpful assistant"
        assert system_messages[1].content == "Always be concise"

    def test_user_message_with_single_text_block(self) -> None:
        """Test user message with single text block."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {"role": "user", "content": [{"text": "Hello, how are you?"}]}
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)
        assert result[0].content == "Hello, how are you?"

    def test_user_message_with_multiple_text_blocks(self) -> None:
        """Test user message with multiple text blocks combined with newlines."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {
                "role": "user",
                "content": [
                    {"text": "First question"},
                    {"text": "Second question"},
                    {"text": "Third question"},
                ],
            }
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)
        assert result[0].content == "First question\nSecond question\nThird question"

    def test_user_message_with_cache_point_filtered(self) -> None:
        """Test that Bedrock cachePoint markers are filtered out."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {
                "role": "user",
                "content": [
                    {"text": "User question"},
                    {"cachePoint": {"type": "default"}},  # Should be filtered
                ],
            }
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)
        assert result[0].content == "User question"

    def test_assistant_message_without_tool_calls(self) -> None:
        """Test assistant message with only text content."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {"role": "assistant", "content": [{"text": "I can help with that!"}]}
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], AIMessage)
        assert result[0].content == "I can help with that!"
        assert not hasattr(result[0], "tool_calls") or not result[0].tool_calls

    def test_assistant_message_with_tool_calls(self) -> None:
        """Test assistant message with both text and tool calls."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {
                "role": "assistant",
                "content": [
                    {"text": "I'll search for that"},
                    {
                        "toolUse": {
                            "toolUseId": "call_123",
                            "name": "search_entities",
                            "input": {"query": "datasets", "limit": 10},
                        }
                    },
                ],
            }
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], AIMessage)
        assert result[0].content == "I'll search for that"
        assert hasattr(result[0], "tool_calls")
        assert len(result[0].tool_calls) == 1
        tool_call = result[0].tool_calls[0]
        assert tool_call["id"] == "call_123"
        assert tool_call["name"] == "search_entities"
        assert tool_call["args"] == {"query": "datasets", "limit": 10}

    def test_assistant_message_with_multiple_tool_calls(self) -> None:
        """Test assistant message with multiple tool calls."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "call_1",
                            "name": "search",
                            "input": {"query": "test1"},
                        }
                    },
                    {
                        "toolUse": {
                            "toolUseId": "call_2",
                            "name": "get_entities",
                            "input": {"urns": ["urn:1", "urn:2"]},
                        }
                    },
                ],
            }
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], AIMessage)
        assert len(result[0].tool_calls) == 2
        assert result[0].tool_calls[0]["name"] == "search"
        assert result[0].tool_calls[1]["name"] == "get_entities"

    def test_tool_result_conversion(self) -> None:
        """Test that Bedrock toolResult blocks are converted to ToolMessage."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = []
        messages: list[Any] = [
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "call_123",
                            "content": [
                                {"json": {"count": 5, "results": ["a", "b"]}},
                                {"text": "Found 5 results"},
                            ],
                        }
                    }
                ],
            }
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        assert len(result) == 1
        assert isinstance(result[0], ToolMessage)
        assert result[0].tool_call_id == "call_123"
        # Should combine JSON (serialized) and text with newlines
        assert '{"count": 5, "results": ["a", "b"]}' in result[0].content
        assert "Found 5 results" in result[0].content

    def test_complex_conversation_history(self) -> None:
        """Test realistic multi-turn conversation with tool calling."""
        wrapper = MockLLMWrapper(model_name="test")

        system: list[Any] = [{"text": "You are helpful"}]
        messages: list[Any] = [
            # Turn 1: User asks question
            {"role": "user", "content": [{"text": "Find datasets about users"}]},
            # Turn 2: Assistant decides to use tool
            {
                "role": "assistant",
                "content": [
                    {"text": "I'll search for that"},
                    {
                        "toolUse": {
                            "toolUseId": "call_1",
                            "name": "search",
                            "input": {"query": "/q user"},
                        }
                    },
                ],
            },
            # Turn 3: User provides tool result
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "call_1",
                            "content": [{"json": {"count": 3}}],
                        }
                    }
                ],
            },
            # Turn 4: Assistant responds with answer
            {"role": "assistant", "content": [{"text": "I found 3 datasets"}]},
        ]

        result = wrapper._convert_bedrock_messages_to_langchain(system, messages)

        # Should have: 1 system + 1 user + 1 assistant (with tool) + 1 tool result + 1 assistant
        assert len(result) == 5
        assert isinstance(result[0], SystemMessage)
        assert isinstance(result[1], HumanMessage)
        assert isinstance(result[2], AIMessage) and result[2].tool_calls
        assert isinstance(result[3], ToolMessage)
        assert isinstance(result[4], AIMessage) and not result[4].tool_calls


class TestBedrockCacheTokens:
    """Tests for Bedrock cache token tracking from streaming responses."""

    def test_cache_read_tokens_captured(self) -> None:
        """Test that cacheReadInputTokens are captured from streaming metadata."""
        from unittest.mock import MagicMock, patch

        from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper

        with patch(
            "datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client"
        ) as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            # Mock streaming response with cache tokens
            mock_client.converse_stream.return_value = {
                "stream": [
                    {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
                    {
                        "contentBlockDelta": {
                            "delta": {"text": "Cached response"},
                            "contentBlockIndex": 0,
                        }
                    },
                    {"contentBlockStop": {"contentBlockIndex": 0}},
                    {"messageStop": {"stopReason": "end_turn"}},
                    {
                        "metadata": {
                            "usage": {
                                "inputTokens": 1000,
                                "outputTokens": 50,
                                "totalTokens": 1050,
                                "cacheReadInputTokens": 800,  # Cache hit!
                            }
                        }
                    },
                ]
            }

            wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

            response = wrapper.converse(
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

            # Verify cache read tokens are captured
            assert response["usage"]["inputTokens"] == 1000
            assert response["usage"]["outputTokens"] == 50
            assert response["usage"]["cacheReadInputTokens"] == 800

    def test_cache_write_tokens_captured(self) -> None:
        """Test that cacheWriteInputTokens are captured from streaming metadata."""
        from unittest.mock import MagicMock, patch

        from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper

        with patch(
            "datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client"
        ) as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            # Mock streaming response with cache write
            mock_client.converse_stream.return_value = {
                "stream": [
                    {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
                    {
                        "contentBlockDelta": {
                            "delta": {"text": "Building cache"},
                            "contentBlockIndex": 0,
                        }
                    },
                    {"contentBlockStop": {"contentBlockIndex": 0}},
                    {"messageStop": {"stopReason": "end_turn"}},
                    {
                        "metadata": {
                            "usage": {
                                "inputTokens": 1000,
                                "outputTokens": 50,
                                "totalTokens": 1050,
                                "cacheWriteInputTokens": 1000,  # Cache write!
                            }
                        }
                    },
                ]
            }

            wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

            response = wrapper.converse(
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

            # Verify cache write tokens are captured
            assert response["usage"]["inputTokens"] == 1000
            assert response["usage"]["outputTokens"] == 50
            assert response["usage"]["cacheWriteInputTokens"] == 1000

    def test_both_cache_tokens_captured(self) -> None:
        """Test that both cacheReadInputTokens and cacheWriteInputTokens are captured."""
        from unittest.mock import MagicMock, patch

        from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper

        with patch(
            "datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client"
        ) as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            # Mock streaming response with both cache read and write
            mock_client.converse_stream.return_value = {
                "stream": [
                    {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
                    {
                        "contentBlockDelta": {
                            "delta": {"text": "Partial cache hit"},
                            "contentBlockIndex": 0,
                        }
                    },
                    {"contentBlockStop": {"contentBlockIndex": 0}},
                    {"messageStop": {"stopReason": "end_turn"}},
                    {
                        "metadata": {
                            "usage": {
                                "inputTokens": 1200,
                                "outputTokens": 60,
                                "totalTokens": 1260,
                                "cacheReadInputTokens": 800,  # Partial cache hit
                                "cacheWriteInputTokens": 400,  # Write new portion
                            }
                        }
                    },
                ]
            }

            wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

            response = wrapper.converse(
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

            # Verify both cache tokens are captured
            assert response["usage"]["inputTokens"] == 1200
            assert response["usage"]["outputTokens"] == 60
            assert response["usage"]["cacheReadInputTokens"] == 800
            assert response["usage"]["cacheWriteInputTokens"] == 400

    def test_no_cache_tokens_when_absent(self) -> None:
        """Test that cache token fields are only present when provided by Bedrock."""
        from unittest.mock import MagicMock, patch

        from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper

        with patch(
            "datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client"
        ) as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            # Mock streaming response WITHOUT cache tokens
            mock_client.converse_stream.return_value = {
                "stream": [
                    {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
                    {
                        "contentBlockDelta": {
                            "delta": {"text": "No cache"},
                            "contentBlockIndex": 0,
                        }
                    },
                    {"contentBlockStop": {"contentBlockIndex": 0}},
                    {"messageStop": {"stopReason": "end_turn"}},
                    {
                        "metadata": {
                            "usage": {
                                "inputTokens": 100,
                                "outputTokens": 20,
                                "totalTokens": 120,
                                # No cache tokens
                            }
                        }
                    },
                ]
            }

            wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

            response = wrapper.converse(
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

            # Verify cache token fields are NOT present when not provided
            assert "cacheReadInputTokens" not in response["usage"]
            assert "cacheWriteInputTokens" not in response["usage"]
            assert response["usage"]["inputTokens"] == 100
            assert response["usage"]["outputTokens"] == 20
