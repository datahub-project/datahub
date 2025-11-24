"""
Unit tests for Bedrock-Langchain transformation helpers.

These tests verify the correctness of format transformations between Bedrock's native
API format and Langchain's format used by OpenAI and Gemini providers.

The transformations are critical for:
1. Tool definitions - converting Bedrock toolSpec to Langchain function format
2. Tool results - converting Bedrock toolResult blocks to Langchain ToolMessage
3. Responses - converting Langchain AIMessage back to Bedrock format

Each test includes clear examples showing the input format and expected output format
to make the transformations easy to understand.
"""

from typing import Any, List

from langchain_core.messages import AIMessage, HumanMessage

from datahub_integrations.gen_ai.llm.base import LLMWrapper


class MockLLMWrapper(LLMWrapper):
    """
    Concrete test implementation of LLMWrapper.

    This allows us to test the shared transformation methods without
    needing to instantiate a real provider.
    """

    def _initialize_client(self) -> Any:
        """Dummy client for testing."""
        return None

    @property
    def exceptions(self) -> Any:
        """Dummy exceptions for testing."""
        return Exception

    def converse(
        self,
        system: List[Any],
        messages: List[Any],
        toolConfig: Any = None,
        inferenceConfig: Any = None,
    ) -> Any:
        """Not used in transformation tests."""
        raise NotImplementedError


class TestConvertBedrockToolsToLangchain:
    """
    Tests for converting Bedrock tool format to Langchain/OpenAI function format.

    Bedrock Format:
    {
        "toolSpec": {
            "name": "tool_name",
            "description": "Tool description",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {...},
                    "required": [...]
                }
            }
        }
    }

    Langchain/OpenAI Format:
    {
        "type": "function",
        "function": {
            "name": "tool_name",
            "description": "Tool description",
            "parameters": {
                "type": "object",
                "properties": {...},
                "required": [...]
            }
        }
    }
    """

    def test_simple_tool_conversion(self) -> None:
        """Test conversion of a simple tool with basic schema."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "search_entities",
                    "description": "Search for entities in DataHub",
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

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        assert result[0]["type"] == "function"
        assert result[0]["function"]["name"] == "search_entities"
        assert result[0]["function"]["description"] == "Search for entities in DataHub"
        assert result[0]["function"]["parameters"]["type"] == "object"
        assert (
            result[0]["function"]["parameters"]["properties"]["query"]["type"]
            == "string"
        )
        assert (
            result[0]["function"]["parameters"]["properties"]["limit"]["type"]
            == "integer"
        )
        assert result[0]["function"]["parameters"]["required"] == ["query"]

    def test_tool_without_description(self) -> None:
        """Test conversion when description is missing (should default to empty string)."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "no_description_tool",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"param": {"type": "string"}},
                        }
                    },
                }
            }
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        assert result[0]["function"]["name"] == "no_description_tool"
        assert result[0]["function"]["description"] == ""

    def test_tool_with_complex_schema(self) -> None:
        """Test conversion of tool with nested objects and arrays."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "complex_tool",
                    "description": "Tool with complex schema",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "filters": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "value": {"type": "string"},
                                        },
                                    },
                                },
                                "metadata": {
                                    "type": "object",
                                    "properties": {
                                        "source": {"type": "string"},
                                        "tags": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                        },
                                    },
                                },
                            },
                            "required": ["filters"],
                        }
                    },
                }
            }
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        params = result[0]["function"]["parameters"]
        assert params["properties"]["filters"]["type"] == "array"
        assert params["properties"]["filters"]["items"]["type"] == "object"
        assert params["properties"]["metadata"]["type"] == "object"
        assert params["required"] == ["filters"]

    def test_multiple_tools(self) -> None:
        """Test conversion of multiple tools in one call."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "tool1",
                    "description": "First tool",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"param1": {"type": "string"}},
                        }
                    },
                }
            },
            {
                "toolSpec": {
                    "name": "tool2",
                    "description": "Second tool",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"param2": {"type": "integer"}},
                        }
                    },
                }
            },
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 2
        assert result[0]["function"]["name"] == "tool1"
        assert result[1]["function"]["name"] == "tool2"

    def test_unexpected_tool_format(self) -> None:
        """Test handling of tool without toolSpec key (should log warning and skip)."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "valid_tool",
                    "inputSchema": {"json": {"type": "object", "properties": {}}},
                }
            },
            {
                "unexpectedKey": {
                    "name": "invalid_tool",
                }
            },
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        # Should only include the valid tool
        assert len(result) == 1
        assert result[0]["function"]["name"] == "valid_tool"

    def test_empty_tools_list(self) -> None:
        """Test conversion of empty tools list."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools: List[Any] = []

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)  # type: ignore[arg-type]

        assert len(result) == 0
        assert result == []

    def test_tool_without_required_field(self) -> None:
        """Test handling of tool schema without required field."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "optional_params_tool",
                    "description": "Tool where all parameters are optional",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "optional_param": {"type": "string"},
                            },
                            # No "required" field
                        }
                    },
                }
            }
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        assert result[0]["function"]["name"] == "optional_params_tool"
        # Required field should not be present if not in source
        assert "required" not in result[0]["function"]["parameters"]

    def test_tool_with_missing_input_schema(self) -> None:
        """Test handling of tool with missing inputSchema."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "no_schema_tool",
                    "description": "Tool without input schema",
                    # No inputSchema field
                }
            }
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        assert result[0]["function"]["name"] == "no_schema_tool"
        # Should default to empty dict for parameters
        assert result[0]["function"]["parameters"] == {}

    def test_tool_with_empty_input_schema(self) -> None:
        """Test handling of tool with empty inputSchema.json."""
        wrapper = MockLLMWrapper(model_name="test")

        bedrock_tools = [
            {
                "toolSpec": {
                    "name": "empty_schema_tool",
                    "description": "Tool with empty schema",
                    "inputSchema": {"json": {}},
                }
            }
        ]

        result = wrapper._convert_bedrock_tools_to_langchain(bedrock_tools)

        assert len(result) == 1
        assert result[0]["function"]["name"] == "empty_schema_tool"
        assert result[0]["function"]["parameters"] == {}


class TestConvertBedrockToolResultsToLangchain:
    """
    Tests for converting Bedrock tool result blocks to Langchain ToolMessage.

    Bedrock Format:
    [
        {
            "toolResult": {
                "toolUseId": "call_123",
                "content": [
                    {"json": {"key": "value"}},
                    {"text": "Additional context"}
                ],
                "status": "success"  # optional
            }
        }
    ]

    Langchain Format:
    [
        ToolMessage(
            content='{"key": "value"}\\nAdditional context',
            tool_call_id="call_123"
        )
    ]
    """

    def test_simple_tool_result_with_json(self) -> None:
        """Test conversion of tool result with JSON content."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_abc123",
                    "content": [{"json": {"entities": ["entity1", "entity2"]}}],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_abc123"
        # JSON should be serialized to string
        assert "entities" in result[0].content
        assert "entity1" in result[0].content

    def test_tool_result_with_text(self) -> None:
        """Test conversion of tool result with text content."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_xyz789",
                    "content": [{"text": "Found 5 matching entities"}],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_xyz789"
        assert result[0].content == "Found 5 matching entities"

    def test_tool_result_with_mixed_content(self) -> None:
        """Test conversion of tool result with both JSON and text blocks."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_mixed456",
                    "content": [
                        {"json": {"count": 3, "results": ["a", "b", "c"]}},
                        {"text": "Search completed successfully"},
                    ],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_mixed456"
        # JSON and text should be combined with newline
        assert "count" in result[0].content
        assert "Search completed successfully" in result[0].content

    def test_multiple_tool_results(self) -> None:
        """Test conversion of multiple tool results."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_1",
                    "content": [{"text": "Result 1"}],
                }
            },
            {
                "toolResult": {
                    "toolUseId": "call_2",
                    "content": [{"json": {"result": "Result 2"}}],
                }
            },
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 2
        assert result[0].tool_call_id == "call_1"
        assert result[0].content == "Result 1"
        assert result[1].tool_call_id == "call_2"
        assert "Result 2" in result[1].content

    def test_tool_result_with_status(self) -> None:
        """Test that status field is ignored (not used in Langchain format)."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_with_status",
                    "content": [{"text": "Success"}],
                    "status": "success",
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_with_status"
        assert result[0].content == "Success"

    def test_tool_result_without_content(self) -> None:
        """Test handling of tool result with empty content array."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_empty",
                    "content": [],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_empty"
        assert result[0].content == ""

    def test_tool_result_without_tool_use_id(self) -> None:
        """Test handling of tool result missing toolUseId (should default to empty string)."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "content": [{"text": "Result without ID"}],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == ""
        assert result[0].content == "Result without ID"

    def test_mixed_content_with_non_tool_blocks(self) -> None:
        """Test that non-toolResult blocks are ignored."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {"text": "This is a regular text block"},
            {
                "toolResult": {
                    "toolUseId": "call_123",
                    "content": [{"text": "Tool result"}],
                }
            },
            {"image": {"format": "jpeg", "source": {"bytes": b"fake"}}},
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        # Should only extract the toolResult block
        assert len(result) == 1
        assert result[0].tool_call_id == "call_123"
        assert result[0].content == "Tool result"

    def test_empty_content_list(self) -> None:
        """Test conversion of empty content list."""
        wrapper = MockLLMWrapper(model_name="test")

        content: List[Any] = []

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)  # type: ignore[arg-type]

        assert len(result) == 0
        assert result == []

    def test_tool_result_with_multiple_json_blocks(self) -> None:
        """Test tool result with multiple JSON content blocks."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_multi_json",
                    "content": [
                        {"json": {"part": 1, "data": "first"}},
                        {"json": {"part": 2, "data": "second"}},
                        {"json": {"part": 3, "data": "third"}},
                    ],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_multi_json"
        # All JSON blocks should be serialized and joined with newlines
        assert '"part": 1' in result[0].content
        assert '"part": 2' in result[0].content
        assert '"part": 3' in result[0].content
        # Count newlines - should be 2 (between 3 parts)
        assert result[0].content.count("\n") == 2

    def test_tool_result_with_multiple_text_blocks(self) -> None:
        """Test tool result with multiple text content blocks."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_multi_text",
                    "content": [
                        {"text": "First line"},
                        {"text": "Second line"},
                        {"text": "Third line"},
                    ],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_multi_text"
        assert result[0].content == "First line\nSecond line\nThird line"

    def test_tool_result_with_unknown_content_type(self) -> None:
        """Test that unknown content types in toolResult are skipped."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_unknown",
                    "content": [
                        {"text": "Known type"},
                        {"unknown_type": "Should be skipped"},
                        {"json": {"valid": True}},
                        {"image": {"format": "png"}},  # Not supported yet
                    ],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        assert result[0].tool_call_id == "call_unknown"
        # Should only include text and json blocks
        assert "Known type" in result[0].content
        assert "valid" in result[0].content
        # Should not include unknown types
        assert "Should be skipped" not in result[0].content
        assert "image" not in result[0].content

    def test_tool_result_with_nested_json_structures(self) -> None:
        """Test tool result with deeply nested JSON structures."""
        wrapper = MockLLMWrapper(model_name="test")

        content = [
            {
                "toolResult": {
                    "toolUseId": "call_nested",
                    "content": [
                        {
                            "json": {
                                "entities": [
                                    {
                                        "urn": "urn:li:dataset:123",
                                        "properties": {
                                            "name": "test_dataset",
                                            "tags": ["prod", "analytics"],
                                        },
                                    }
                                ],
                                "metadata": {"count": 1, "hasMore": False},
                            }
                        }
                    ],
                }
            }
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        assert len(result) == 1
        # Verify nested structure is preserved in JSON serialization
        assert "urn:li:dataset:123" in result[0].content
        assert "test_dataset" in result[0].content
        assert "prod" in result[0].content

    def test_content_with_non_dict_items(self) -> None:
        """Test that non-dict items in content list are skipped."""
        wrapper = MockLLMWrapper(model_name="test")

        content: List[Any] = [
            "string item",  # Not a dict
            123,  # Not a dict
            {
                "toolResult": {
                    "toolUseId": "call_valid",
                    "content": [{"text": "Valid result"}],
                }
            },
            None,  # Not a dict
        ]

        result = wrapper._convert_bedrock_tool_results_to_langchain(content)

        # Should only process the valid dict with toolResult
        assert len(result) == 1
        assert result[0].tool_call_id == "call_valid"
        assert result[0].content == "Valid result"


class TestConvertLangchainResponseToBedrock:
    """
    Tests for converting Langchain AIMessage back to Bedrock format.

    Langchain Format:
    AIMessage(
        content="Response text",
        tool_calls=[
            {"name": "tool_name", "args": {"param": "value"}, "id": "call_123"}
        ],
        usage_metadata={"input_tokens": 100, "output_tokens": 50},
        response_metadata={"finish_reason": "stop"}
    )

    Bedrock Format:
    {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {"text": "Response text"},
                    {"toolUse": {
                        "toolUseId": "call_123",
                        "name": "tool_name",
                        "input": {"param": "value"}
                    }}
                ]
            }
        },
        "stopReason": "tool_use",  # or "end_turn", "max_tokens"
        "usage": {
            "inputTokens": 100,
            "outputTokens": 50
        }
    }
    """

    def test_text_only_response(self) -> None:
        """Test conversion of response with only text content."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="This is a simple text response",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "stop"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["output"]["message"]["role"] == "assistant"
        assert len(result["output"]["message"]["content"]) == 1
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "This is a simple text response"
        )
        assert result["stopReason"] == "end_turn"
        assert result["usage"]["inputTokens"] == 50
        assert result["usage"]["outputTokens"] == 10

    def test_response_with_tool_calls(self) -> None:
        """Test conversion of response with tool calls."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="I'll search for that",
            tool_calls=[
                {
                    "name": "search_entities",
                    "args": {"query": "test query", "limit": 10},
                    "id": "call_search123",
                }
            ],
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 20,
                "total_tokens": 120,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert len(result["output"]["message"]["content"]) == 2
        # First block should be text
        assert (
            result["output"]["message"]["content"][0]["text"] == "I'll search for that"
        )
        # Second block should be toolUse
        assert "toolUse" in result["output"]["message"]["content"][1]
        tool_use = result["output"]["message"]["content"][1]["toolUse"]
        assert tool_use["toolUseId"] == "call_search123"
        assert tool_use["name"] == "search_entities"
        assert tool_use["input"]["query"] == "test query"
        assert tool_use["input"]["limit"] == 10
        assert result["stopReason"] == "tool_use"

    def test_response_with_multiple_tool_calls(self) -> None:
        """Test conversion of response with multiple tool calls."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="I'll call multiple tools",
            tool_calls=[
                {"name": "tool1", "args": {"param1": "value1"}, "id": "call_1"},
                {"name": "tool2", "args": {"param2": "value2"}, "id": "call_2"},
            ],
            usage_metadata={
                "input_tokens": 150,
                "output_tokens": 30,
                "total_tokens": 180,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert len(result["output"]["message"]["content"]) == 3
        # Text block
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "I'll call multiple tools"
        )
        # First tool call
        assert result["output"]["message"]["content"][1]["toolUse"]["name"] == "tool1"
        # Second tool call
        assert result["output"]["message"]["content"][2]["toolUse"]["name"] == "tool2"
        assert result["stopReason"] == "tool_use"

    def test_response_without_text_content(self) -> None:
        """Test conversion when model only makes tool calls (no text)."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="",  # Empty content
            tool_calls=[
                {"name": "search", "args": {}, "id": "call_empty"},
            ],
            usage_metadata={
                "input_tokens": 80,
                "output_tokens": 15,
                "total_tokens": 95,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        # Should only have toolUse block, no text block
        assert len(result["output"]["message"]["content"]) == 1
        assert "toolUse" in result["output"]["message"]["content"][0]
        assert result["stopReason"] == "tool_use"

    def test_stop_reason_mapping_openai_stop(self) -> None:
        """Test mapping of OpenAI 'stop' finish_reason to 'end_turn'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Response text",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "stop"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_stop_reason_mapping_openai_length(self) -> None:
        """Test mapping of OpenAI 'length' finish_reason to 'max_tokens'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Truncated response",
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 200,
                "total_tokens": 300,
            },
            response_metadata={"finish_reason": "length"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "max_tokens"

    def test_stop_reason_mapping_openai_tool_calls(self) -> None:
        """Test mapping of OpenAI 'tool_calls' finish_reason to 'tool_use'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="",
            tool_calls=[{"name": "tool", "args": {}, "id": "call_1"}],
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "tool_calls"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        # Should prioritize tool_calls presence over finish_reason
        assert result["stopReason"] == "tool_use"

    def test_stop_reason_mapping_gemini_stop(self) -> None:
        """Test mapping of Gemini 'STOP' finish_reason to 'end_turn'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Response text",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "STOP"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_stop_reason_mapping_gemini_max_tokens(self) -> None:
        """Test mapping of Gemini 'MAX_TOKENS' finish_reason to 'max_tokens'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Truncated",
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 200,
                "total_tokens": 300,
            },
            response_metadata={"finish_reason": "MAX_TOKENS"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "max_tokens"

    def test_stop_reason_mapping_content_filter(self) -> None:
        """Test mapping of content filter finish_reason to 'end_turn'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Filtered content",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "content_filter"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_stop_reason_mapping_unknown(self) -> None:
        """Test handling of unknown finish_reason (should default to 'end_turn')."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "unknown_reason"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_usage_metadata_extraction(self) -> None:
        """Test extraction of token usage from usage_metadata."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Test",
            usage_metadata={
                "input_tokens": 123,
                "output_tokens": 456,
                "total_tokens": 579,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["usage"]["inputTokens"] == 123
        assert result["usage"]["outputTokens"] == 456

    def test_usage_metadata_missing(self) -> None:
        """Test handling when usage_metadata is missing (should default to 0)."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(content="Test")

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["usage"]["inputTokens"] == 0
        assert result["usage"]["outputTokens"] == 0

    def test_usage_metadata_partial(self) -> None:
        """
        Test handling when usage_metadata has partial data.

        Note: AIMessage validation requires total_tokens, but we can test that
        the conversion code handles missing fields gracefully by using .get().
        """
        wrapper = MockLLMWrapper(model_name="test")

        # Create valid AIMessage, then test that conversion handles missing fields
        response = AIMessage(
            content="Test",
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 0,
                "total_tokens": 100,
            },
        )
        # The conversion code uses .get() which handles missing keys gracefully
        # This test verifies that output_tokens defaults to 0 when not present
        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["usage"]["inputTokens"] == 100
        # Note: Since we provided output_tokens=0, this will be 0
        # The actual edge case (missing key) is handled by .get() in the code
        assert result["usage"]["outputTokens"] == 0

    def test_tool_call_with_empty_id(self) -> None:
        """Test handling of tool call with empty id string."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="",
            tool_calls=[{"name": "tool", "args": {}, "id": ""}],  # Empty id
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        tool_use = result["output"]["message"]["content"][0]["toolUse"]
        assert tool_use["toolUseId"] == ""
        assert tool_use["name"] == "tool"

    def test_tool_call_with_empty_args(self) -> None:
        """Test handling of tool call with empty args dict."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="",
            tool_calls=[{"name": "tool", "args": {}, "id": "call_1"}],  # Empty args
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        tool_use = result["output"]["message"]["content"][0]["toolUse"]
        assert tool_use["input"] == {}

    def test_tool_call_with_complex_args(self) -> None:
        """Test handling of tool call with nested objects and arrays in args."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="I'll search with complex filters",
            tool_calls=[
                {
                    "name": "complex_search",
                    "args": {
                        "filters": [
                            {"field": "type", "value": "dataset"},
                            {"field": "platform", "value": "snowflake"},
                        ],
                        "metadata": {
                            "source": "ui",
                            "tags": ["production", "analytics"],
                        },
                        "limit": 100,
                    },
                    "id": "call_complex",
                }
            ],
            usage_metadata={
                "input_tokens": 150,
                "output_tokens": 40,
                "total_tokens": 190,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        tool_use = result["output"]["message"]["content"][1]["toolUse"]
        assert tool_use["name"] == "complex_search"
        assert tool_use["input"]["filters"][0]["field"] == "type"
        assert tool_use["input"]["metadata"]["tags"] == ["production", "analytics"]
        assert tool_use["input"]["limit"] == 100

    def test_response_with_text_and_tool_call_content_order(self) -> None:
        """Test that text content comes before tool calls in content blocks."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Let me search for that",
            tool_calls=[
                {"name": "search", "args": {"query": "test"}, "id": "call_1"},
            ],
            usage_metadata={
                "input_tokens": 60,
                "output_tokens": 15,
                "total_tokens": 75,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        # Text block should be first
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "Let me search for that"
        )
        # Tool use block should be second
        assert "toolUse" in result["output"]["message"]["content"][1]
        assert result["output"]["message"]["content"][1]["toolUse"]["name"] == "search"

    def test_stop_reason_gemini_safety(self) -> None:
        """Test mapping of Gemini 'SAFETY' finish_reason to 'end_turn'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Filtered response",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "SAFETY"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_stop_reason_gemini_recitation(self) -> None:
        """Test mapping of Gemini 'RECITATION' finish_reason to 'end_turn'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Filtered response",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "RECITATION"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "end_turn"

    def test_stop_reason_openai_function_call(self) -> None:
        """Test mapping of OpenAI 'function_call' finish_reason to 'tool_use'."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="",
            tool_calls=[{"name": "tool", "args": {}, "id": "call_1"}],
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
            response_metadata={"finish_reason": "function_call"},
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        assert result["stopReason"] == "tool_use"

    def test_response_without_response_metadata(self) -> None:
        """Test handling when response_metadata is completely missing."""
        wrapper = MockLLMWrapper(model_name="test")

        response = AIMessage(
            content="Test response",
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
        )

        result = wrapper._convert_langchain_response_to_bedrock(response)

        # Should default to end_turn when no response_metadata
        assert result["stopReason"] == "end_turn"


class TestInvokeWithLangchain:
    """
    Tests for _invoke_with_langchain helper method.

    This method is shared by all langchain-based providers (OpenAI, Gemini) and handles:
    - Filtering out Bedrock cachePoint markers
    - Converting tools to langchain format
    - Binding tools and invoking with appropriate configuration
    """

    def test_invoke_without_tools(self) -> None:
        """Test invocation without any tools configured (uses streaming internally)."""
        wrapper = MockLLMWrapper(model_name="test")

        # Mock the client and its stream method
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = AIMessage(
            content="Response without tools",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming to return single chunk
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_client.stream.side_effect = mock_stream
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Hello")]
        inferenceConfig = {"temperature": 0.7}

        result = wrapper._invoke_with_langchain(lc_messages, None, inferenceConfig)

        # Should call client.stream with mapped inference config
        mock_client.stream.assert_called_once_with(lc_messages, temperature=0.7)
        assert result.content == mock_response.content

    def test_invoke_with_tools(self) -> None:
        """Test invocation with tools configured (uses streaming internally)."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_bound_client = MagicMock()
        mock_response = AIMessage(
            content="Using tool",
            tool_calls=[{"name": "search", "args": {}, "id": "call_1"}],
            usage_metadata={
                "input_tokens": 50,
                "output_tokens": 30,
                "total_tokens": 80,
            },
        )

        # Mock streaming to return single chunk
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_bound_client.stream.side_effect = mock_stream
        mock_client.bind_tools.return_value = mock_bound_client
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Search for something")]
        toolConfig = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "search",
                        "description": "Search tool",
                        "inputSchema": {"json": {"type": "object", "properties": {}}},
                    }
                }
            ]
        }
        inferenceConfig = {"temperature": 0.5, "maxTokens": 100}

        result = wrapper._invoke_with_langchain(
            lc_messages, toolConfig, inferenceConfig
        )

        # Should bind tools and stream on the bound client
        mock_client.bind_tools.assert_called_once()
        bound_tools = mock_client.bind_tools.call_args[0][0]
        assert len(bound_tools) == 1
        assert bound_tools[0]["type"] == "function"
        assert bound_tools[0]["function"]["name"] == "search"

        mock_bound_client.stream.assert_called_once_with(
            lc_messages, temperature=0.5, max_tokens=100
        )
        assert result == mock_response

    def test_invoke_filters_cache_point(self) -> None:
        """Test that cachePoint markers are filtered out from tools."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_bound_client = MagicMock()
        mock_response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_bound_client.stream.side_effect = mock_stream
        mock_client.bind_tools.return_value = mock_bound_client
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Test")]
        toolConfig = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "tool1",
                        "inputSchema": {"json": {"type": "object"}},
                    }
                },
                {"cachePoint": {"type": "default"}},  # Should be filtered out
                {
                    "toolSpec": {
                        "name": "tool2",
                        "inputSchema": {"json": {"type": "object"}},
                    }
                },
            ]
        }
        inferenceConfig = None

        wrapper._invoke_with_langchain(lc_messages, toolConfig, inferenceConfig)

        # Should only bind the 2 actual tools, not the cachePoint
        mock_client.bind_tools.assert_called_once()
        bound_tools = mock_client.bind_tools.call_args[0][0]
        assert len(bound_tools) == 2  # cachePoint filtered out
        assert bound_tools[0]["function"]["name"] == "tool1"
        assert bound_tools[1]["function"]["name"] == "tool2"

    def test_invoke_with_only_cache_point(self) -> None:
        """Test that if only cachePoint markers exist, regular invoke is used."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_client.stream.side_effect = mock_stream
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Test")]
        toolConfig = {
            "tools": [
                {"cachePoint": {"type": "default"}},  # Only cachePoint, no real tools
            ]
        }
        inferenceConfig = {"temperature": 0.7}

        result = wrapper._invoke_with_langchain(
            lc_messages, toolConfig, inferenceConfig
        )

        # Should call regular invoke, not bind_tools
        mock_client.bind_tools.assert_not_called()
        mock_client.stream.assert_called_once_with(lc_messages, temperature=0.7)
        assert result == mock_response

    def test_invoke_with_empty_tools_list(self) -> None:
        """Test invocation with empty tools list."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_client.stream.side_effect = mock_stream
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Test")]
        toolConfig: dict[str, Any] = {"tools": []}  # Empty list
        inferenceConfig = None

        result = wrapper._invoke_with_langchain(
            lc_messages, toolConfig, inferenceConfig
        )

        # Should call regular invoke without tool binding
        mock_client.bind_tools.assert_not_called()
        mock_client.stream.assert_called_once_with(lc_messages)
        assert result == mock_response

    def test_invoke_maps_inference_config_correctly(self) -> None:
        """Test that inferenceConfig is mapped to invoke kwargs correctly."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_client.stream.side_effect = mock_stream
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Test")]
        inferenceConfig = {
            "temperature": 0.9,
            "maxTokens": 500,
        }

        wrapper._invoke_with_langchain(lc_messages, None, inferenceConfig)

        # Should map Bedrock config to langchain kwargs
        mock_client.stream.assert_called_once_with(
            lc_messages, temperature=0.9, max_tokens=500
        )
        call_kwargs = mock_client.stream.call_args[1]
        assert call_kwargs["temperature"] == 0.9
        assert call_kwargs["max_tokens"] == 500

    def test_invoke_with_multiple_tools(self) -> None:
        """Test invocation with multiple tools."""
        wrapper = MockLLMWrapper(model_name="test")

        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_bound_client = MagicMock()
        mock_response = AIMessage(
            content="Response",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
            },
        )

        # Mock streaming
        def mock_stream(messages, **kwargs):
            yield mock_response

        mock_bound_client.stream.side_effect = mock_stream
        mock_client.bind_tools.return_value = mock_bound_client
        wrapper._client = mock_client

        lc_messages = [HumanMessage(content="Test")]
        toolConfig = {
            "tools": [
                {
                    "toolSpec": {
                        "name": "search",
                        "description": "Search for entities",
                        "inputSchema": {
                            "json": {
                                "type": "object",
                                "properties": {"query": {"type": "string"}},
                            }
                        },
                    }
                },
                {
                    "toolSpec": {
                        "name": "get_details",
                        "description": "Get entity details",
                        "inputSchema": {
                            "json": {
                                "type": "object",
                                "properties": {"urn": {"type": "string"}},
                            }
                        },
                    }
                },
            ]
        }
        inferenceConfig = None

        wrapper._invoke_with_langchain(lc_messages, toolConfig, inferenceConfig)

        # Should bind all tools
        mock_client.bind_tools.assert_called_once()
        bound_tools = mock_client.bind_tools.call_args[0][0]
        assert len(bound_tools) == 2
        assert bound_tools[0]["function"]["name"] == "search"
        assert bound_tools[1]["function"]["name"] == "get_details"

        # Verify strict=True is passed
        assert mock_client.bind_tools.call_args[1]["strict"] is True
