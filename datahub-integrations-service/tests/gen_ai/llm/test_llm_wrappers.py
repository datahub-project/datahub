"""
Unit tests for LLM wrapper implementations.

These tests verify the correct behavior of each LLM provider wrapper:
- BedrockLLMWrapper: Native Bedrock API with exception translation
- OpenAILLMWrapper: Langchain-based wrapper with format transformations
- GeminiLLMWrapper: Langchain-based wrapper with format transformations

Key testing areas:
1. Client initialization and configuration
2. Exception handling and translation to standardized exceptions
3. Format transformations (for OpenAI/Gemini)
4. Tool calling support
5. Token usage tracking

Note: Bedrock tests require AWS credentials and are skipped in environments without them.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper
from datahub_integrations.gen_ai.llm.exceptions import (
    LlmAuthenticationException,
    LlmInputTooLongException,
    LlmRateLimitException,
    LlmValidationException,
)
from datahub_integrations.gen_ai.llm.openai import OpenAILLMWrapper


class TestBedrockLLMWrapper:
    """Tests for BedrockLLMWrapper - native Bedrock API pass-through."""

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_initialization(self, mock_get_client: Mock) -> None:
        """Test that Bedrock wrapper stores configuration correctly."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        wrapper = BedrockLLMWrapper(
            model_name="claude-3-5-sonnet",
            read_timeout=120,
            connect_timeout=30,
            max_attempts=5,
        )

        # Verify wrapper properties are set correctly
        assert wrapper.model_name == "claude-3-5-sonnet"
        assert wrapper.read_timeout == 120
        assert wrapper.connect_timeout == 30
        assert wrapper.max_attempts == 5
        mock_get_client.assert_called_once_with(read_timeout=120, connect_timeout=30)

    @patch("datahub_integrations.gen_ai.llm.bedrock.aggregate_converse_stream")
    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_successful_converse_call(
        self, mock_get_client: Mock, mock_aggregate: Mock
    ) -> None:
        """Test successful Bedrock converse API call (uses streaming internally)."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock the streaming response (just the structure, not the events)
        mock_stream = MagicMock()
        mock_client.converse_stream.return_value = {"stream": mock_stream}

        # Mock the aggregated result from aggregate_converse_stream
        mock_aggregate.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "Hello! How can I help you?"}],
                }
            },
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 20,
                "outputTokens": 10,
                "totalTokens": 30,
            },
            "metrics": {},
        }

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        response = wrapper.converse(
            modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"temperature": 0.5, "maxTokens": 4096},
        )

        # Verify converse_stream was called with correct parameters
        mock_client.converse_stream.assert_called_once()
        call_kwargs = mock_client.converse_stream.call_args.kwargs
        assert call_kwargs["modelId"] == "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
        assert call_kwargs["system"] == [{"text": "You are helpful"}]
        assert call_kwargs["messages"] == [
            {"role": "user", "content": [{"text": "Hello"}]}
        ]
        assert call_kwargs["inferenceConfig"]["temperature"] == 0.5

        # Verify aggregate_converse_stream was called with the stream
        mock_aggregate.assert_called_once_with(mock_stream)

        # Verify response has the expected format
        assert response["stopReason"] == "end_turn"
        assert response["usage"]["inputTokens"] == 20
        assert (
            response["output"]["message"]["content"][0]["text"]
            == "Hello! How can I help you?"
        )

    @patch("datahub_integrations.gen_ai.llm.bedrock.aggregate_converse_stream")
    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_converse_with_tools(
        self, mock_get_client: Mock, mock_aggregate: Mock
    ) -> None:
        """Test Bedrock converse call with tool configuration."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock the streaming response
        mock_stream = MagicMock()
        mock_client.converse_stream.return_value = {"stream": mock_stream}

        # Mock the aggregated result with tool use
        mock_aggregate.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"text": "I'll search for that"},
                        {
                            "toolUse": {
                                "toolUseId": "call_123",
                                "name": "search",
                                "input": {"query": "test"},
                            }
                        },
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {
                "inputTokens": 50,
                "outputTokens": 20,
                "totalTokens": 70,
            },
            "metrics": {},
        }

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        tool_config = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "search",
                        "description": "Search entities",
                        "inputSchema": {
                            "json": {
                                "type": "object",
                                "properties": {"query": {"type": "string"}},
                                "required": ["query"],
                            }
                        },
                    }
                }
            ]
        }

        response = wrapper.converse(
            modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Search for datasets"}]}],
            toolConfig=tool_config,
        )

        # Verify toolConfig was passed through
        call_kwargs = mock_client.converse_stream.call_args.kwargs
        assert "toolConfig" in call_kwargs
        assert call_kwargs["toolConfig"] == tool_config

        # Verify aggregate_converse_stream was called
        mock_aggregate.assert_called_once_with(mock_stream)

        # Verify tool use response
        assert response["stopReason"] == "tool_use"
        assert "toolUse" in response["output"]["message"]["content"][1]
        assert (
            response["output"]["message"]["content"][1]["toolUse"]["name"] == "search"
        )
        assert response["output"]["message"]["content"][1]["toolUse"]["input"] == {
            "query": "test"
        }

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_validation_exception_input_too_long(self, mock_get_client: Mock) -> None:
        """Test that ValidationException with 'Input is too long' raises LlmInputTooLongException."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create all exception types that will be checked in the except clauses
        ValidationException = type("ValidationException", (Exception,), {})
        ThrottlingException = type("ThrottlingException", (Exception,), {})
        AccessDeniedException = type("AccessDeniedException", (Exception,), {})

        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException
        mock_client.converse_stream.side_effect = ValidationException(
            "Input is too long for model context window"
        )

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        with pytest.raises(LlmInputTooLongException) as exc_info:
            wrapper.converse(
                modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "x" * 100000}]}],
            )

        assert "Input is too long" in str(exc_info.value)

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_validation_exception_generic(self, mock_get_client: Mock) -> None:
        """Test that generic ValidationException raises LlmValidationException."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create all exception types that will be checked in the except clauses
        ValidationException = type("ValidationException", (Exception,), {})
        ThrottlingException = type("ThrottlingException", (Exception,), {})
        AccessDeniedException = type("AccessDeniedException", (Exception,), {})

        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException
        mock_client.converse_stream.side_effect = ValidationException(
            "Invalid model ID"
        )

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        with pytest.raises(LlmValidationException) as exc_info:
            wrapper.converse(
                modelId="invalid-model-id",
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

        assert "Invalid model ID" in str(exc_info.value)

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_throttling_exception(self, mock_get_client: Mock) -> None:
        """Test that ThrottlingException raises LlmRateLimitException."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create all exception types that will be checked in the except clauses
        ValidationException = type("ValidationException", (Exception,), {})
        ThrottlingException = type("ThrottlingException", (Exception,), {})
        AccessDeniedException = type("AccessDeniedException", (Exception,), {})

        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException
        mock_client.converse_stream.side_effect = ThrottlingException(
            "Rate limit exceeded"
        )

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        with pytest.raises(LlmRateLimitException) as exc_info:
            wrapper.converse(
                modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

        assert "Rate limit exceeded" in str(exc_info.value)

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_access_denied_exception(self, mock_get_client: Mock) -> None:
        """Test that AccessDeniedException raises LlmAuthenticationException."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create all exception types that will be checked in the except clauses
        ValidationException = type("ValidationException", (Exception,), {})
        ThrottlingException = type("ThrottlingException", (Exception,), {})
        AccessDeniedException = type("AccessDeniedException", (Exception,), {})

        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException
        mock_client.converse_stream.side_effect = AccessDeniedException(
            "Access denied to model bedrock:us.anthropic.claude-3-5-sonnet"
        )

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        with pytest.raises(LlmAuthenticationException) as exc_info:
            wrapper.converse(
                modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                system=[{"text": "You are helpful"}],
                messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            )

        assert "Access denied" in str(exc_info.value)

    @patch("datahub_integrations.gen_ai.llm.bedrock.aggregate_converse_stream")
    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_max_tokens_not_treated_as_error(
        self, mock_get_client: Mock, mock_aggregate: Mock
    ) -> None:
        """Test that stopReason='max_tokens' is returned as valid response, not error."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock the streaming response
        mock_stream = MagicMock()
        mock_client.converse_stream.return_value = {"stream": mock_stream}

        # Mock aggregated result with max_tokens stop reason
        mock_aggregate.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "This is a truncated respon"}],
                }
            },
            "stopReason": "max_tokens",
            "usage": {
                "inputTokens": 100,
                "outputTokens": 200,
                "totalTokens": 300,
            },
            "metrics": {},
        }

        wrapper = BedrockLLMWrapper(model_name="claude-3-5-sonnet")

        # Should NOT raise an exception
        response = wrapper.converse(
            modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"maxTokens": 200},
        )

        # Verify it's returned as a valid response
        assert response["stopReason"] == "max_tokens"
        assert response["usage"]["outputTokens"] == 200


class TestOpenAILLMWrapper:
    """Tests for OpenAILLMWrapper - Langchain-based with format transformations."""

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_initialization_with_api_key(self, mock_chat_openai: Mock) -> None:
        """Test OpenAI wrapper initialization with API key."""
        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        OpenAILLMWrapper(
            model_name="gpt-4o",
            read_timeout=90,
            connect_timeout=20,
            max_attempts=3,
        )

        # Verify ChatOpenAI was initialized with correct params
        mock_chat_openai.assert_called_once()
        call_kwargs = mock_chat_openai.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o"
        # api_key is wrapped in SecretStr for type safety
        assert call_kwargs["api_key"].get_secret_value() == "test-api-key"
        assert call_kwargs["timeout"] == 90
        assert call_kwargs["max_retries"] == 3

    @patch.dict("os.environ", {}, clear=True)
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_initialization_without_api_key_raises_error(
        self, mock_chat_openai: Mock
    ) -> None:
        """Test that missing OPENAI_API_KEY raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            OpenAILLMWrapper(model_name="gpt-4o")

        assert "OPENAI_API_KEY" in str(exc_info.value)

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_successful_converse_call(self, mock_chat_openai: Mock) -> None:
        """Test successful OpenAI converse call with format transformation (uses streaming)."""
        from langchain_core.messages import AIMessageChunk

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        # Mock streaming response - now using .stream() instead of .invoke()
        def mock_stream(messages, **kwargs):
            chunk = AIMessageChunk(content="Hello! How can I help you today?")
            chunk.usage_metadata = {
                "input_tokens": 20,
                "output_tokens": 10,
                "total_tokens": 30,
            }
            chunk.response_metadata = {"finish_reason": "stop"}
            yield chunk

        mock_client.stream.side_effect = mock_stream

        wrapper = OpenAILLMWrapper(model_name="gpt-4o")

        response = wrapper.converse(
            modelId="gpt-4o",
            system=[{"text": "You are a helpful assistant"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"temperature": 0.7, "maxTokens": 2048},
        )

        # Verify response was transformed to Bedrock format
        assert response["stopReason"] == "end_turn"
        assert response["usage"]["inputTokens"] == 20
        assert response["usage"]["outputTokens"] == 10
        assert response["output"]["message"]["role"] == "assistant"
        assert (
            response["output"]["message"]["content"][0]["text"]
            == "Hello! How can I help you today?"
        )

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_converse_with_multiple_system_messages(
        self, mock_chat_openai: Mock
    ) -> None:
        """Test that multiple Bedrock system messages are converted to langchain format."""
        from langchain_core.messages import AIMessageChunk, SystemMessage

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        # Mock streaming response
        def mock_stream(messages, **kwargs):
            chunk = AIMessageChunk(content="I understand")
            chunk.usage_metadata = {
                "input_tokens": 50,
                "output_tokens": 5,
                "total_tokens": 55,
            }
            chunk.response_metadata = {"finish_reason": "stop"}
            yield chunk

        mock_client.stream.side_effect = mock_stream

        wrapper = OpenAILLMWrapper(model_name="gpt-4o")

        wrapper.converse(
            modelId="gpt-4o",
            system=[
                {"text": "You are a helpful assistant"},
                {"text": "Always be concise"},
            ],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
        )

        # Verify system messages were converted
        call_args = mock_client.stream.call_args[0][0]
        system_messages = [msg for msg in call_args if isinstance(msg, SystemMessage)]
        assert len(system_messages) == 2
        assert system_messages[0].content == "You are a helpful assistant"
        assert system_messages[1].content == "Always be concise"

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_converse_with_tool_calling(self, mock_chat_openai: Mock) -> None:
        """Test OpenAI tool calling with format transformation."""
        from langchain_core.messages import AIMessage

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client
        mock_llm_with_tools = MagicMock()
        mock_client.bind_tools.return_value = mock_llm_with_tools

        # Mock streaming response with tool calls
        def mock_stream(messages, **kwargs):
            chunk = AIMessage(
                content="I'll search for that",
                tool_calls=[
                    {
                        "name": "search_entities",
                        "args": {"query": "datasets", "limit": 10},
                        "id": "call_abc123",
                    }
                ],
            )
            chunk.usage_metadata = {
                "input_tokens": 100,
                "output_tokens": 30,
                "total_tokens": 130,
            }
            chunk.response_metadata = {"finish_reason": "tool_calls"}
            yield chunk

        mock_llm_with_tools.stream.side_effect = mock_stream

        wrapper = OpenAILLMWrapper(model_name="gpt-4o")

        tool_config = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "search_entities",
                        "description": "Search for entities",
                        "inputSchema": {
                            "json": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string"},
                                    "limit": {"type": "integer"},
                                },
                                "required": ["query"],
                            }
                        },
                    }
                }
            ]
        }

        response = wrapper.converse(
            modelId="gpt-4o",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Find datasets"}]}],
            toolConfig=tool_config,
        )

        # Verify tools were bound
        mock_client.bind_tools.assert_called_once()
        bound_tools = mock_client.bind_tools.call_args[0][0]
        assert len(bound_tools) == 1
        assert bound_tools[0]["function"]["name"] == "search_entities"

        # Verify response was transformed to Bedrock format
        assert response["stopReason"] == "tool_use"
        assert len(response["output"]["message"]["content"]) == 2
        assert (
            response["output"]["message"]["content"][0]["text"]
            == "I'll search for that"
        )
        assert "toolUse" in response["output"]["message"]["content"][1]
        tool_use = response["output"]["message"]["content"][1]["toolUse"]
        assert tool_use["name"] == "search_entities"
        assert tool_use["toolUseId"] == "call_abc123"
        assert tool_use["input"]["query"] == "datasets"

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_authentication_error(self, mock_chat_openai: Mock) -> None:
        """Test that OpenAI AuthenticationError raises LlmAuthenticationException."""
        import openai

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        # Create proper OpenAI exception class and instance
        AuthenticationError = type("AuthenticationError", (Exception,), {})
        mock_client.stream.side_effect = AuthenticationError("Invalid API key")

        # Patch the openai module's exception class for isinstance check
        with patch.object(openai, "AuthenticationError", AuthenticationError):
            wrapper = OpenAILLMWrapper(model_name="gpt-4o")

            with pytest.raises(LlmAuthenticationException) as exc_info:
                wrapper.converse(
                    modelId="gpt-4o",
                    system=[{"text": "You are helpful"}],
                    messages=[{"role": "user", "content": [{"text": "Hello"}]}],
                )

            assert "Invalid API key" in str(exc_info.value)

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_rate_limit_error(self, mock_chat_openai: Mock) -> None:
        """Test that OpenAI RateLimitError raises LlmRateLimitException."""
        import openai

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        RateLimitError = type("RateLimitError", (Exception,), {})
        mock_client.stream.side_effect = RateLimitError("Rate limit exceeded")

        with patch.object(openai, "RateLimitError", RateLimitError):
            wrapper = OpenAILLMWrapper(model_name="gpt-4o")

            with pytest.raises(LlmRateLimitException) as exc_info:
                wrapper.converse(
                    modelId="gpt-4o",
                    system=[{"text": "You are helpful"}],
                    messages=[{"role": "user", "content": [{"text": "Hello"}]}],
                )

            assert "Rate limit exceeded" in str(exc_info.value)

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_context_length_exceeded_error(self, mock_chat_openai: Mock) -> None:
        """Test that OpenAI context_length_exceeded error raises LlmInputTooLongException."""
        import openai

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        BadRequestError = type("BadRequestError", (Exception,), {})
        mock_client.stream.side_effect = BadRequestError(
            "This model's maximum context length is 4096 tokens"
        )

        with patch.object(openai, "BadRequestError", BadRequestError):
            wrapper = OpenAILLMWrapper(model_name="gpt-4o")

            with pytest.raises(LlmInputTooLongException) as exc_info:
                wrapper.converse(
                    modelId="gpt-4o",
                    system=[{"text": "You are helpful"}],
                    messages=[{"role": "user", "content": [{"text": "x" * 100000}]}],
                )

            assert "maximum context length" in str(exc_info.value)

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_inference_config_passed_to_invoke(self, mock_chat_openai: Mock) -> None:
        """Test that inferenceConfig is passed as kwargs to stream() (thread-safe)."""
        from langchain_core.messages import AIMessageChunk

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        # Mock streaming response
        def mock_stream(messages, **kwargs):
            chunk = AIMessageChunk(content="Response")
            chunk.usage_metadata = {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
            }
            yield chunk

        mock_client.stream.side_effect = mock_stream

        wrapper = OpenAILLMWrapper(model_name="gpt-4o")

        wrapper.converse(
            modelId="gpt-4o",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"temperature": 0.9, "maxTokens": 1024},
        )

        # Verify stream was called with the config as kwargs (not client mutation)
        mock_client.stream.assert_called_once()
        call_kwargs = mock_client.stream.call_args[1]
        assert call_kwargs["temperature"] == 0.9
        assert call_kwargs["max_tokens"] == 1024

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-api-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_cache_point_markers_filtered_out(self, mock_chat_openai: Mock) -> None:
        """Test that Bedrock cachePoint markers are filtered from tools list."""
        from langchain_core.messages import AIMessage

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client
        mock_llm_with_tools = MagicMock()
        mock_client.bind_tools.return_value = mock_llm_with_tools

        # Mock streaming response
        def mock_stream(messages, **kwargs):
            chunk = AIMessage(content="Response")
            chunk.usage_metadata = {
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            }
            yield chunk

        mock_llm_with_tools.stream.side_effect = mock_stream

        wrapper = OpenAILLMWrapper(model_name="gpt-4o")

        # Tool config with cachePoint markers
        tool_config = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "tool1",
                        "inputSchema": {"json": {"type": "object", "properties": {}}},
                    }
                },
                {"cachePoint": {"type": "default"}},  # Should be filtered
                {
                    "toolSpec": {
                        "name": "tool2",
                        "inputSchema": {"json": {"type": "object", "properties": {}}},
                    }
                },
            ]
        }

        wrapper.converse(
            modelId="gpt-4o",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            toolConfig=tool_config,
        )

        # Verify only actual tools were bound (cachePoint filtered out)
        bound_tools = mock_client.bind_tools.call_args[0][0]
        assert len(bound_tools) == 2
        assert all("function" in tool for tool in bound_tools)


class TestGeminiLLMWrapper:
    """Tests for GeminiLLMWrapper - Langchain-based Vertex AI wrapper."""

    @patch.dict(
        "os.environ",
        {"VERTEXAI_PROJECT": "test-project", "VERTEXAI_LOCATION": "us-central1"},
    )
    @patch("datahub_integrations.gen_ai.llm.gemini.ChatVertexAI")
    def test_initialization_with_env_vars(self, mock_chat_vertex: Mock) -> None:
        """Test that Gemini client initializes with environment variables."""
        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        mock_client = MagicMock()
        mock_chat_vertex.return_value = mock_client

        GeminiLLMWrapper(
            model_name="gemini-1.5-pro",
            read_timeout=90,
            connect_timeout=20,
            max_attempts=3,
        )

        # Verify ChatVertexAI was initialized with correct params
        mock_chat_vertex.assert_called_once()
        call_kwargs = mock_chat_vertex.call_args.kwargs
        assert call_kwargs["model"] == "gemini-1.5-pro"
        assert call_kwargs["project"] == "test-project"
        assert call_kwargs["location"] == "us-central1"
        assert call_kwargs["temperature"] == 0.5
        assert call_kwargs["thinking_budget"] == 0  # Disabled by default
        assert call_kwargs["timeout"] == 90
        assert call_kwargs["max_retries"] == 3

    @patch.dict("os.environ", {}, clear=True)
    def test_initialization_without_project_raises_error(self) -> None:
        """Test that missing VERTEXAI_PROJECT raises ValueError."""
        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        with pytest.raises(ValueError) as exc_info:
            GeminiLLMWrapper(model_name="gemini-1.5-pro")

        assert "VERTEXAI_PROJECT" in str(exc_info.value)

    @patch.dict("os.environ", {"VERTEXAI_PROJECT": "test-project"}, clear=True)
    def test_initialization_without_location_raises_error(self) -> None:
        """Test that missing VERTEXAI_LOCATION raises ValueError."""
        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        with pytest.raises(ValueError) as exc_info:
            GeminiLLMWrapper(model_name="gemini-1.5-pro")

        assert "VERTEXAI_LOCATION" in str(exc_info.value)

    @patch.dict(
        "os.environ",
        {"VERTEXAI_PROJECT": "test-project", "VERTEXAI_LOCATION": "us-central1"},
    )
    @patch("datahub_integrations.gen_ai.llm.gemini.ChatVertexAI")
    def test_successful_converse_call(self, mock_chat_vertex: Mock) -> None:
        """Test successful Gemini converse API call."""
        from langchain_core.messages import AIMessage

        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        mock_client = MagicMock()
        mock_chat_vertex.return_value = mock_client

        mock_response = AIMessage(
            content="Hello! How can I help you?",
            usage_metadata={
                "input_tokens": 20,
                "output_tokens": 10,
                "total_tokens": 30,
            },
            response_metadata={"finish_reason": "STOP"},
        )

        # Mock streaming response
        def mock_stream(messages, **kwargs):
            chunk = mock_response
            yield chunk

        mock_client.stream.side_effect = mock_stream

        wrapper = GeminiLLMWrapper(model_name="gemini-1.5-pro")

        response = wrapper.converse(
            modelId="gemini-1.5-pro",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"temperature": 0.7, "maxTokens": 2048},
        )

        # Verify stream was called with correct parameters
        mock_client.stream.assert_called_once()
        call_kwargs = mock_client.stream.call_args[1]
        assert call_kwargs["temperature"] == 0.7
        assert call_kwargs["max_tokens"] == 2048

        # Verify response is in Bedrock format
        assert response["stopReason"] == "end_turn"  # STOP mapped to end_turn
        assert response["usage"]["inputTokens"] == 20
        assert response["usage"]["outputTokens"] == 10

    @patch.dict(
        "os.environ",
        {"VERTEXAI_PROJECT": "test-project", "VERTEXAI_LOCATION": "us-central1"},
    )
    @patch("datahub_integrations.gen_ai.llm.gemini.ChatVertexAI")
    def test_inference_config_passed_to_invoke(self, mock_chat_vertex: Mock) -> None:
        """Test that inferenceConfig is passed as kwargs to stream() (thread-safe)."""
        from langchain_core.messages import AIMessageChunk

        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        mock_client = MagicMock()
        mock_chat_vertex.return_value = mock_client

        # Mock streaming response
        def mock_stream(messages, **kwargs):
            chunk = AIMessageChunk(content="Response")
            chunk.usage_metadata = {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
            }
            yield chunk

        mock_client.stream.side_effect = mock_stream

        wrapper = GeminiLLMWrapper(model_name="gemini-1.5-pro")

        wrapper.converse(
            modelId="gemini-1.5-pro",
            system=[{"text": "You are helpful"}],
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
            inferenceConfig={"temperature": 0.9, "maxTokens": 1024},
        )

        # Verify stream was called with the config as kwargs (not client mutation)
        mock_client.stream.assert_called_once()
        call_kwargs = mock_client.stream.call_args[1]
        assert call_kwargs["temperature"] == 0.9
        assert call_kwargs["max_tokens"] == 1024


class TestLLMFactory:
    """Tests for LLM factory functions."""

    def test_parse_model_id_with_bedrock_prefix(self) -> None:
        """Test parsing model ID with bedrock/ prefix."""
        from datahub_integrations.gen_ai.llm.factory import _parse_model_id

        provider, model_name = _parse_model_id(
            "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert provider == "bedrock"
        assert model_name == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

    def test_parse_model_id_without_prefix(self) -> None:
        """Test parsing model ID without prefix defaults to bedrock."""
        from datahub_integrations.gen_ai.llm.factory import _parse_model_id

        provider, model_name = _parse_model_id(
            "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert provider == "bedrock"
        assert model_name == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

    def test_parse_model_id_with_openai_prefix(self) -> None:
        """Test parsing model ID with openai/ prefix."""
        from datahub_integrations.gen_ai.llm.factory import _parse_model_id

        provider, model_name = _parse_model_id("openai/gpt-4o")
        assert provider == "openai"
        assert model_name == "gpt-4o"

    def test_parse_model_id_with_gemini_prefix(self) -> None:
        """Test parsing model ID with gemini/ prefix."""
        from datahub_integrations.gen_ai.llm.factory import _parse_model_id

        provider, model_name = _parse_model_id("gemini/gemini-1.5-pro")
        assert provider == "gemini"
        assert model_name == "gemini-1.5-pro"

    def test_parse_model_id_with_vertex_ai_prefix(self) -> None:
        """Test parsing model ID with vertex_ai/ prefix."""
        from datahub_integrations.gen_ai.llm.factory import _parse_model_id

        provider, model_name = _parse_model_id("vertex_ai/gemini-1.5-pro")
        assert provider == "vertex_ai"
        assert model_name == "gemini-1.5-pro"

    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_get_llm_client_bedrock(self, mock_get_bedrock_client: Mock) -> None:
        """Test that get_llm_client returns BedrockLLMWrapper for bedrock models."""
        from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper
        from datahub_integrations.gen_ai.llm.factory import get_llm_client

        mock_client = MagicMock()
        mock_get_bedrock_client.return_value = mock_client

        # Clear cache to ensure fresh instance
        get_llm_client.cache_clear()

        client = get_llm_client("bedrock/claude-3-5-sonnet")

        assert isinstance(client, BedrockLLMWrapper)
        assert client.model_name == "claude-3-5-sonnet"

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    def test_get_llm_client_openai(self, mock_chat_openai: Mock) -> None:
        """Test that get_llm_client returns OpenAILLMWrapper for openai models."""
        from datahub_integrations.gen_ai.llm.factory import get_llm_client
        from datahub_integrations.gen_ai.llm.openai import OpenAILLMWrapper

        mock_client = MagicMock()
        mock_chat_openai.return_value = mock_client

        # Clear cache to ensure fresh instance
        get_llm_client.cache_clear()

        client = get_llm_client("openai/gpt-4o")

        assert isinstance(client, OpenAILLMWrapper)
        assert client.model_name == "gpt-4o"

    @patch.dict(
        "os.environ",
        {"VERTEXAI_PROJECT": "test-project", "VERTEXAI_LOCATION": "us-central1"},
    )
    @patch("datahub_integrations.gen_ai.llm.gemini.ChatVertexAI")
    def test_get_llm_client_gemini(self, mock_chat_vertex: Mock) -> None:
        """Test that get_llm_client returns GeminiLLMWrapper for gemini models."""
        from datahub_integrations.gen_ai.llm.factory import get_llm_client
        from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper

        mock_client = MagicMock()
        mock_chat_vertex.return_value = mock_client

        # Clear cache to ensure fresh instance
        get_llm_client.cache_clear()

        client = get_llm_client("gemini/gemini-1.5-pro")

        assert isinstance(client, GeminiLLMWrapper)
        assert client.model_name == "gemini-1.5-pro"

    def test_get_llm_client_unsupported_provider(self) -> None:
        """Test that unsupported provider raises ValueError."""
        from datahub_integrations.gen_ai.llm.factory import get_llm_client

        # Clear cache to ensure fresh instance
        get_llm_client.cache_clear()

        with pytest.raises(ValueError) as exc_info:
            get_llm_client("unsupported/model-name")

        assert "Unsupported LLM provider" in str(exc_info.value)
        assert "unsupported" in str(exc_info.value)

    @patch(
        "datahub_integrations.gen_ai.llm.bedrock.BedrockLLMWrapper._initialize_client"
    )
    def test_get_llm_client_caches_instances(self, mock_init_client: Mock) -> None:
        """Test that get_llm_client caches instances by model_id."""
        from datahub_integrations.gen_ai.llm.factory import get_llm_client

        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        # Clear cache to start fresh
        get_llm_client.cache_clear()

        # Call twice with same model_id
        client1 = get_llm_client("claude-3-5-sonnet")
        client2 = get_llm_client("claude-3-5-sonnet")

        # Should return the exact same instance (cached)
        assert client1 is client2

        # Should only initialize client once (due to caching)
        assert mock_init_client.call_count == 1

    @patch.dict("os.environ", {"OPENAI_API_KEY": "key1"})
    @patch("datahub_integrations.gen_ai.llm.openai.ChatOpenAI")
    @patch("datahub_integrations.gen_ai.llm.bedrock.get_bedrock_client")
    def test_get_llm_client_different_providers_not_cached_together(
        self, mock_get_bedrock_client: Mock, mock_chat_openai: Mock
    ) -> None:
        """Test that different providers create separate cached instances."""
        from datahub_integrations.gen_ai.llm.factory import get_llm_client

        mock_bedrock = MagicMock()
        mock_get_bedrock_client.return_value = mock_bedrock
        mock_openai = MagicMock()
        mock_chat_openai.return_value = mock_openai

        # Clear cache to start fresh
        get_llm_client.cache_clear()

        # Different providers, same model name
        client1 = get_llm_client("bedrock/gpt-4o")
        client2 = get_llm_client("openai/gpt-4o")

        # Should be different instances (different providers)
        assert client1 is not client2
        assert type(client1).__name__ == "BedrockLLMWrapper"
        assert type(client2).__name__ == "OpenAILLMWrapper"
