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
from datahub_integrations.gen_ai.model_config import CustomModelProvider

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


def _get_tool_call_error_message(
    response: AIMessage, response_metadata: Dict[str, Any]
) -> Optional[str]:
    """Get error message for invalid or malformed tool calls, or None if neither exists.

    Checks for both invalid_tool_calls (from Langchain parsing errors) and
    malformed function calls (from provider finish_reason like MALFORMED_FUNCTION_CALL).
    """
    # Check for invalid tool calls first (more specific)
    if response.invalid_tool_calls:
        error_messages = [
            f"Invalid tool call to '{call.get('name', 'unknown')}': {call.get('error', 'Unknown error')}. "
            "Please retry with corrected parameters."
            for call in response.invalid_tool_calls
        ]
        return "\n".join(error_messages)

    # Check for malformed function calls (from provider)
    if "MALFORMED_FUNCTION_CALL" in (response_metadata.get("finish_reason") or ""):
        finish_message = response_metadata.get("finish_message")
        if finish_message:
            return f"Tool call error: {finish_message}. Please retry with corrected parameters."
        return "Tool call error: Malformed function call detected. Please retry with corrected parameters."

    return None


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
        custom_model_provider: CustomModelProvider | None = None,
    ):
        """
        Initialize LLM wrapper.

        Args:
            model_name: Model name (without provider prefix)
            read_timeout: Read timeout in seconds
            connect_timeout: Connection timeout in seconds
            max_attempts: Maximum retry attempts
            custom_model_provider: Custom proxy model configuration
        """
        self.model_name = model_name
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_attempts = max_attempts
        self.custom_model_provider = custom_model_provider
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
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Unified converse API matching Bedrock's interface.

        Args:
            system: System messages list
            messages: Conversation messages
            toolConfig: Tool configuration with tools list
            inferenceConfig: Inference parameters (temperature, maxTokens)

        Returns:
            ConverseResponse with typed structure
        """
        pass

    def _convert_bedrock_messages_to_langchain(
        self,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
    ) -> List[Any]:
        """
        Convert Bedrock message format to langchain message format.

        This is a shared helper for OpenAI and Gemini that handles:
        1. System messages conversion
        2. Tool result conversion (user messages with toolResult blocks)
        3. Text content extraction and combining
        4. Tool use extraction from assistant messages

        Bedrock message structure:
        - One message can have MULTIPLE content blocks (text, toolUse, toolResult, cachePoint)
        - System: [{"text": "You are helpful"}, {"text": "Follow rules"}]
        - User: {"role": "user", "content": [{"text": "..."}, {"cachePoint": {...}}]}
        - Assistant: {"role": "assistant", "content": [{"text": "..."}, {"toolUse": {...}}]}

        Langchain message structure:
        - One message has ONE content string
        - System: [SystemMessage(content="You are helpful"), SystemMessage(content="Follow rules")]
        - User: HumanMessage(content="...")
        - Assistant: AIMessage(content="...", tool_calls=[...])

        Args:
            system: Bedrock system messages
            messages: Bedrock conversation messages

        Returns:
            List of langchain messages (SystemMessage, HumanMessage, AIMessage, ToolMessage)
        """
        lc_messages: List[Any] = []

        # Convert system messages
        for sys_msg in system:
            if isinstance(sys_msg, dict) and "text" in sys_msg:
                from langchain_core.messages import SystemMessage

                lc_messages.append(SystemMessage(content=sys_msg["text"]))

        # Convert conversation messages
        for msg in messages:
            role = msg.get("role")  # "user" or "assistant"
            content = msg.get("content", [])  # List of content blocks

            # Check if this message contains tool results
            # Tool results become ToolMessage objects, not HumanMessage
            tool_messages = self._convert_bedrock_tool_results_to_langchain(content)

            if tool_messages:
                # This message contains tool results - add all ToolMessages
                lc_messages.extend(tool_messages)
                continue

            # Not a tool result - process as regular message
            # Extract all text blocks from this message and combine them
            text_parts = []
            for block in content:
                if isinstance(block, dict):
                    if "text" in block:
                        text_parts.append(block["text"])
                    elif "cachePoint" in block:
                        # Bedrock-specific caching marker - skip it
                        # OpenAI/Gemini handle caching differently
                        pass
                    elif "toolResult" in block:
                        # Tool results already handled above
                        pass
                    elif "toolUse" in block and role == "assistant":
                        # Tool use in assistant message - handled below after text extraction
                        pass
                    else:
                        # Unexpected block type
                        logger.warning(
                            f"Unexpected content block type in {role} message during langchain conversion: {list(block.keys())}"
                        )

            # Combine all text blocks with newlines
            combined_text = "\n".join(text_parts)

            if role == "user":
                from langchain_core.messages import HumanMessage

                lc_messages.append(HumanMessage(content=combined_text))
            elif role == "assistant":
                from langchain_core.messages import AIMessage

                # Extract any tool calls from this assistant message
                # Bedrock toolUse format: {"toolUse": {"toolUseId": "abc", "name": "search", "input": {...}}}
                # Langchain format: {"name": "search", "args": {...}, "id": "abc"}
                tool_calls_in_msg = []
                for block in content:
                    if isinstance(block, dict) and "toolUse" in block:
                        tool_use = block["toolUse"]
                        tool_calls_in_msg.append(
                            {
                                "name": tool_use.get("name"),
                                "args": tool_use.get("input", {}),
                                "id": tool_use.get("toolUseId"),
                            }
                        )

                # Create AIMessage with text content and optional tool calls
                if tool_calls_in_msg:
                    lc_messages.append(
                        AIMessage(content=combined_text, tool_calls=tool_calls_in_msg)
                    )
                else:
                    lc_messages.append(AIMessage(content=combined_text))

        return lc_messages

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

        # Convert tool call errors to text content so the agent can retry.
        # This ensures malformed/invalid tool calls don't terminate the agent loop.
        response_metadata = response.response_metadata or {}
        llm_server_tool_error_message = _get_tool_call_error_message(
            response, response_metadata
        )

        if llm_server_tool_error_message:
            content_blocks.append(
                {
                    "text": f"{response.content}\n\n{llm_server_tool_error_message}"
                    if response.content
                    else llm_server_tool_error_message
                }
            )
        elif response.content:
            content_blocks.append({"text": response.content})

        # Add tool call blocks if the model wants to call tools (only valid ones)
        # Langchain tool_calls: [{"name": "...", "args": {...}, "id": "..."}]
        # Bedrock toolUse: {"toolUse": {"toolUseId": "...", "name": "...", "input": {...}}}
        if response.tool_calls:
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
        #   "MALFORMED_FUNCTION_CALL" -> "tool_use" (malformed tool call, treat as error)

        # Determine stop_reason: check valid tool calls first, then tool call errors,
        # then finish_reason (which may indicate normal completion)
        if response.tool_calls:
            stop_reason = "tool_use"
        elif llm_server_tool_error_message:
            # Invalid or malformed tool calls -> set tool_use so agent can retry
            stop_reason = "tool_use"
        else:
            # Map provider-specific finish reasons to Bedrock stopReason
            finish_reason = response_metadata.get("finish_reason", "")
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
        Invoke langchain client using streaming internally (same interface as request/response).

        This is a shared helper method for langchain-based providers (OpenAI, Gemini, etc.)
        that uses streaming under the hood to avoid read timeouts on large outputs, but
        accumulates all chunks and returns a complete AIMessage.

        Handles:
        1. Mapping Bedrock inference config to langchain stream kwargs
        2. Filtering out Bedrock-specific cachePoint markers from tools
        3. Converting Bedrock tools to langchain format
        4. Binding tools and streaming with the appropriate configuration
        5. Accumulating chunks using langchain's built-in combining

        Note: Both ChatOpenAI and ChatVertexAI accept 'max_tokens' parameter.
        If a future provider needs a different parameter name (e.g., 'max_output_tokens'),
        we could add a _get_max_tokens_param_name() method that subclasses override.

        Args:
            lc_messages: Langchain messages to send to the LLM
            toolConfig: Optional Bedrock tool configuration with tools list
            inferenceConfig: Optional Bedrock inference config (temperature, maxTokens)

        Returns:
            Complete AIMessage response from the LLM (accumulated from streaming chunks)
        """
        # Map Bedrock inference config to langchain stream kwargs
        # Bedrock uses: {"temperature": 0.5, "maxTokens": 4096}
        # Langchain uses: stream(messages, temperature=0.5, max_tokens=4096)
        stream_kwargs = {}
        if inferenceConfig:
            if temp := inferenceConfig.get("temperature"):
                stream_kwargs["temperature"] = temp
            if max_tokens := inferenceConfig.get("maxTokens"):
                stream_kwargs["max_tokens"] = max_tokens

        # Prepare the LLM client with optional tools
        llm = self._client
        if toolConfig and "tools" in toolConfig:
            tools_list = toolConfig["tools"]
            # Filter out Bedrock-specific cachePoint markers
            # Bedrock uses explicit cache markers: [tool1, tool2, {"cachePoint": {...}}]
            # Langchain providers handle caching automatically/differently
            actual_tools = [t for t in tools_list if "cachePoint" not in t]

            if actual_tools:
                # Convert Bedrock toolSpec format to langchain function format
                lc_tools = self._convert_bedrock_tools_to_langchain(actual_tools)
                # Bind tools to enable tool calling
                llm = self._client.bind_tools(lc_tools, strict=True)

        # Stream and accumulate chunks using langchain's built-in combining
        # The + operator on AIMessageChunk properly merges:
        # - Text content
        # - Tool calls (including partial JSON arguments)
        # - Response metadata
        # - Usage metadata
        full_message: Optional[AIMessage] = None
        last_finish_reason: Optional[str] = None
        last_finish_message: Optional[str] = None

        for chunk in llm.stream(lc_messages, **stream_kwargs):
            if full_message is None:
                full_message = chunk
            else:
                # Langchain's AIMessageChunk supports + operator for proper merging
                full_message = full_message + chunk

            # Track finish_reason and finish_message from each chunk (will use last one)
            chunk_metadata = getattr(chunk, "response_metadata", {}) or {}
            if "finish_reason" in chunk_metadata:
                last_finish_reason = chunk_metadata["finish_reason"]
            if "finish_message" in chunk_metadata:
                last_finish_message = chunk_metadata["finish_message"]

        # Fix finish_reason and finish_message: merge_dicts concatenates string values,
        # but these should use only the last chunk's value (the final state)
        if full_message is not None:
            if last_finish_reason is not None:
                if full_message.response_metadata is None:
                    full_message.response_metadata = {}
                full_message.response_metadata["finish_reason"] = last_finish_reason
            if last_finish_message is not None:
                if full_message.response_metadata is None:
                    full_message.response_metadata = {}
                full_message.response_metadata["finish_message"] = last_finish_message

        # Return the combined message (or empty message if stream was empty)
        return full_message if full_message is not None else AIMessage(content="")
