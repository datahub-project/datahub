"""
Abstract base class for LLM provider wrappers.

This module defines the interface that all LLM providers must implement to work with
the DataHub chatbot. The key design principle is to mimic Bedrock's converse() API
so that we can swap providers with minimal code changes.

Architecture Overview:
=====================

The LLMWrapper abstract class defines three key responsibilities:
1. Initialize provider-specific client
2. Expose provider-specific exceptions
3. Implement converse() method that translates between Bedrock and provider formats

The translation workflow for non-Bedrock providers (OpenAI, Gemini):

  Bedrock Format (Input)
    ↓
  _convert_bedrock_messages_to_langchain()
    ↓
  Langchain Messages
    ↓
  Provider API Call (via langchain)
    ↓
  Langchain AIMessage (Output)
    ↓
  _convert_langchain_response_to_bedrock()
    ↓
  Bedrock Format (Output)

For Bedrock, we pass through directly to the native API with zero overhead.

Shared Utilities:
================

This base class provides three shared utility methods:
1. _convert_bedrock_tools_to_langchain() - Tool format conversion
2. _convert_bedrock_tool_results_to_langchain() - Tool result format conversion
3. _convert_langchain_response_to_bedrock() - Response format conversion

These are used by OpenAI and Gemini implementations but not by Bedrock.
"""

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from langchain_core.messages import AIMessage, ToolMessage
from loguru import logger

from datahub_integrations.gen_ai.llm.types import ConverseResponse, TokenUsage

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


class LLMWrapper(ABC):
    """
    Abstract base class for LLM provider wrappers.

    Each provider (Bedrock, OpenAI, Gemini) has its own concrete implementation
    that handles provider-specific details while exposing a unified interface.
    """

    def __init__(
        self,
        model_name: str,
        read_timeout: int = 60,
        connect_timeout: int = 60,
        max_attempts: int = 10,
    ):
        """
        Initialize LLM wrapper.

        Args:
            model_name: Model name (without provider prefix)
            read_timeout: Read timeout in seconds
            connect_timeout: Connection timeout in seconds
            max_attempts: Maximum retry attempts
        """
        self.model_name = model_name
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_attempts = max_attempts
        self._client = self._initialize_client()

    @abstractmethod
    def _initialize_client(self) -> Any:
        """
        Initialize the provider-specific client.

        Returns:
            Provider-specific client instance
        """
        pass

    @property
    @abstractmethod
    def exceptions(self) -> Any:
        """
        Get provider-specific exception classes for error handling.

        Returns:
            Exception class or module with exception attributes
        """
        pass

    @abstractmethod
    def converse(
        self,
        modelId: str,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Unified converse API matching Bedrock's interface.

        Args:
            modelId: Model identifier (used for validation)
            system: System messages list
            messages: Conversation messages
            toolConfig: Tool configuration with tools list
            inferenceConfig: Inference parameters (temperature, maxTokens)

        Returns:
            ConverseResponse with typed structure
        """
        pass

    def _convert_bedrock_tools_to_langchain(
        self, bedrock_tools: Sequence[Any]
    ) -> List[Dict[str, Any]]:
        """
        Convert Bedrock tool format to langchain/OpenAI function calling format.

        This is a key format transformation that allows OpenAI and Gemini to understand
        tools defined in Bedrock's format.

        Bedrock toolSpec format:
        {
            "toolSpec": {
                "name": "tool_name",
                "description": "Search for entities",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"]
                    }
                }
            }
        }

        Langchain/OpenAI function format:
        {
            "type": "function",
            "function": {
                "name": "tool_name",
                "description": "Search for entities",
                "parameters": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"]
                }
            }
        }

        Key transformations:
        - Unwrap "toolSpec" -> add "type": "function" wrapper
        - Rename "inputSchema"."json" -> "parameters"
        - Keep name and description as-is
        """
        lc_tools = []
        for tool in bedrock_tools:
            if "toolSpec" in tool:
                spec = tool["toolSpec"]
                # Transform to langchain/OpenAI format
                lc_tool = {
                    "type": "function",  # Langchain requires this wrapper
                    "function": {
                        "name": spec["name"],
                        "description": spec.get("description", ""),
                        # Extract JSON schema from nested structure
                        "parameters": spec.get("inputSchema", {}).get("json", {}),
                    },
                }
                lc_tools.append(lc_tool)
            else:
                # Unexpected tool format - not a toolSpec
                # This shouldn't happen since we only pass tools from ToolWrapper.to_bedrock_spec()
                # Log warning to help debug if malformed tools are passed
                logger.warning(
                    f"Unexpected tool format in Bedrock tools list (expected 'toolSpec'): {list(tool.keys())}"
                )
        return lc_tools

    def _convert_bedrock_tool_results_to_langchain(
        self, content: Sequence[Any]
    ) -> List[ToolMessage]:
        """
        Convert Bedrock tool result blocks to langchain ToolMessage objects.

        This is critical for multi-turn tool calling - after the chatbot executes tools,
        it sends the results back to the LLM. This method converts those results.

        Bedrock toolResult format:
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

        Langchain ToolMessage format:
        [
            ToolMessage(
                content='{"key": "value"}\\nAdditional context',
                tool_call_id="call_123"
            )
        ]

        Key transformations:
        - Extract toolResult blocks from content array
        - toolUseId → tool_call_id
        - content array → single string (JSON objects serialized, text joined)
        - Each toolResult becomes one ToolMessage

        Args:
            content: List of content blocks that may contain toolResult blocks

        Returns:
            List of ToolMessage objects (one per toolResult found)
        """
        tool_messages = []

        # Find all toolResult blocks in the content
        for block in content:
            if isinstance(block, dict) and "toolResult" in block:
                tool_result = block["toolResult"]
                tool_use_id = tool_result.get("toolUseId", "")
                result_content = tool_result.get("content", [])

                # Extract text/json from result content blocks
                result_parts = []
                for result_block in result_content:
                    if isinstance(result_block, dict):
                        if "json" in result_block:
                            # Serialize JSON to string
                            result_parts.append(json.dumps(result_block["json"]))
                        elif "text" in result_block:
                            # Use text as-is
                            result_parts.append(result_block["text"])

                # Combine all parts into one string for the ToolMessage
                combined_result = "\n".join(result_parts) if result_parts else ""

                # Create ToolMessage with the tool call ID mapping
                tool_messages.append(
                    ToolMessage(content=combined_result, tool_call_id=tool_use_id)
                )

        return tool_messages

    def _convert_langchain_response_to_bedrock(
        self, response: AIMessage
    ) -> ConverseResponse:
        """
        Convert langchain AIMessage response back to Bedrock converse format.

        This is the reverse transformation of the message conversion - we take langchain's
        response format and convert it to match what Bedrock's converse API returns.

        Langchain AIMessage format:
        AIMessage(
            content="Here's what I found...",
            tool_calls=[
                {"name": "search", "args": {"query": "test"}, "id": "call_123"}
            ],
            usage_metadata={"input_tokens": 100, "output_tokens": 50}
        )

        Bedrock converse response format:
        {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"text": "Here's what I found..."},
                        {"toolUse": {
                            "toolUseId": "call_123",
                            "name": "search",
                            "input": {"query": "test"}
                        }}
                    ]
                }
            },
            "stopReason": "tool_use",  # or "end_turn"
            "usage": {
                "inputTokens": 100,
                "outputTokens": 50
            }
        }

        Key transformations:
        - Single content string -> content blocks array
        - tool_calls array -> toolUse content blocks
        - usage_metadata dict -> Bedrock usage format
        - Infer stopReason from presence of tool calls

        Args:
            response: AIMessage from langchain

        Returns:
            ConverseResponse in Bedrock format
        """
        # Build content blocks array from langchain response
        content_blocks: List[Dict[str, Any]] = []

        # Add text content block if the model generated text
        if response.content:
            content_blocks.append({"text": response.content})

        # Add tool call blocks if the model wants to call tools
        # Langchain tool_calls: [{"name": "...", "args": {...}, "id": "..."}]
        # Bedrock toolUse: {"toolUse": {"toolUseId": "...", "name": "...", "input": {...}}}
        if hasattr(response, "tool_calls") and response.tool_calls:
            for tool_call in response.tool_calls:
                # Transform langchain tool_call to Bedrock toolUse format
                content_blocks.append(
                    {
                        "toolUse": {
                            "toolUseId": tool_call.get("id", ""),
                            "name": tool_call.get("name", ""),
                            "input": tool_call.get("args", {}),  # args -> input
                        }
                    }
                )

        # Determine stop reason by mapping provider-specific values to Bedrock format
        #
        # Bedrock stopReason values:
        #   "end_turn" - Normal completion
        #   "tool_use" - Model wants to call tools
        #   "max_tokens" - Hit token limit
        #
        # OpenAI finish_reason values (in response_metadata):
        #   "stop" -> "end_turn"
        #   "length" -> "max_tokens"
        #   "tool_calls" or "function_call" -> "tool_use"
        #   "content_filter" -> "end_turn" (content was filtered, treat as completion)
        #
        # Gemini finish_reason values (in response_metadata):
        #   "STOP" -> "end_turn"
        #   "MAX_TOKENS" -> "max_tokens"
        #   "SAFETY" -> "end_turn" (safety filter triggered, treat as completion)
        #   "RECITATION" -> "end_turn" (recitation filter, treat as completion)

        # First, check if model made tool calls (highest priority)
        if hasattr(response, "tool_calls") and response.tool_calls:
            stop_reason = "tool_use"
        else:
            # Extract finish_reason from response metadata and map to Bedrock format
            response_metadata = getattr(response, "response_metadata", {})
            finish_reason = response_metadata.get("finish_reason", "")

            # Map provider-specific finish reasons to Bedrock stopReason
            if finish_reason in ("stop", "STOP"):
                # OpenAI "stop" or Gemini "STOP" -> normal completion
                stop_reason = "end_turn"
            elif finish_reason in ("length", "MAX_TOKENS"):
                # OpenAI "length" or Gemini "MAX_TOKENS" -> hit token limit
                stop_reason = "max_tokens"
            elif finish_reason in ("tool_calls", "function_call"):
                # OpenAI tool calling (backup - should be caught by tool_calls check above)
                stop_reason = "tool_use"
            elif finish_reason in ("content_filter", "SAFETY", "RECITATION"):
                # Content/safety filters - treat as normal completion
                stop_reason = "end_turn"
            else:
                # Default to end_turn for unknown finish reasons
                # Log warning to track unexpected values
                if finish_reason:
                    logger.warning(
                        f"Unknown finish_reason '{finish_reason}' in response, defaulting to 'end_turn'"
                    )
                stop_reason = "end_turn"

        # Extract token usage information
        # Langchain: response.usage_metadata = {"input_tokens": 100, "output_tokens": 50}
        # Bedrock: {"inputTokens": 100, "outputTokens": 50, "cacheReadInputTokens": 0, ...}
        usage_metadata = getattr(response, "usage_metadata", None)
        if usage_metadata:
            token_usage: TokenUsage = {
                "inputTokens": usage_metadata.get("input_tokens", 0),
                "outputTokens": usage_metadata.get("output_tokens", 0),
            }
            # Note: OpenAI doesn't provide cache read/write tokens in the same way
            # Could add them here if OpenAI starts reporting cache usage
        else:
            # Fallback if usage metadata not available (shouldn't happen with OpenAI)
            token_usage = {
                "inputTokens": 0,
                "outputTokens": 0,
            }

        # Construct final response in Bedrock's expected format
        return {
            "output": {"message": {"role": "assistant", "content": content_blocks}},
            "stopReason": stop_reason,
            "usage": token_usage,
        }

    def _invoke_with_langchain(
        self,
        lc_messages: List[Any],
        toolConfig: Optional[Dict[str, Any]],
        inferenceConfig: Optional[Dict[str, Any]],
    ) -> AIMessage:
        """
        Invoke langchain client with optional tool calling support.

        This is a shared helper method for langchain-based providers (OpenAI, Gemini, etc.)
        that handles the common pattern of:
        1. Mapping Bedrock inference config to langchain invoke kwargs
        2. Filtering out Bedrock-specific cachePoint markers from tools
        3. Converting Bedrock tools to langchain format
        4. Binding tools and invoking with the appropriate configuration

        Note: Both ChatOpenAI and ChatVertexAI accept 'max_tokens' parameter.
        If a future provider needs a different parameter name (e.g., 'max_output_tokens'),
        we could add a _get_max_tokens_param_name() method that subclasses override.

        Args:
            lc_messages: Langchain messages to send to the LLM
            toolConfig: Optional Bedrock tool configuration with tools list
            inferenceConfig: Optional Bedrock inference config (temperature, maxTokens)

        Returns:
            AIMessage response from the LLM
        """
        # Map Bedrock inference config to langchain invoke kwargs
        # Bedrock uses: {"temperature": 0.5, "maxTokens": 4096}
        # Langchain uses: invoke(messages, temperature=0.5, max_tokens=4096)
        invoke_kwargs = {}
        if inferenceConfig:
            if temp := inferenceConfig.get("temperature"):
                invoke_kwargs["temperature"] = temp
            if max_tokens := inferenceConfig.get("maxTokens"):
                invoke_kwargs["max_tokens"] = max_tokens
        if toolConfig and "tools" in toolConfig:
            tools_list = toolConfig["tools"]
            # Filter out Bedrock-specific cachePoint markers
            # Bedrock uses explicit cache markers: [tool1, tool2, {"cachePoint": {...}}]
            # Langchain providers handle caching automatically/differently
            actual_tools = [t for t in tools_list if "cachePoint" not in t]

            if actual_tools:
                # Convert Bedrock toolSpec format to langchain function format
                lc_tools = self._convert_bedrock_tools_to_langchain(actual_tools)
                # Bind tools to enable tool calling and invoke
                llm_with_tools = self._client.bind_tools(lc_tools, strict=True)
                return llm_with_tools.invoke(lc_messages, **invoke_kwargs)
            else:
                # No tools after filtering out cachePoint markers
                return self._client.invoke(lc_messages, **invoke_kwargs)
        else:
            # No tools configured - regular conversation
            return self._client.invoke(lc_messages, **invoke_kwargs)
