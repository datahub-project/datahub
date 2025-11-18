"""OpenAI LLM wrapper implementation."""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import openai
from datahub.utilities.perf_timer import PerfTimer
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import SecretStr

from datahub_integrations.gen_ai.llm.base import LLMWrapper
from datahub_integrations.gen_ai.llm.exceptions import (
    LlmAuthenticationException,
    LlmInputTooLongException,
    LlmRateLimitException,
    LlmValidationException,
)
from datahub_integrations.gen_ai.llm.types import ConverseResponse

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


class OpenAILLMWrapper(LLMWrapper):
    """OpenAI LLM wrapper using langchain."""

    def _initialize_client(self) -> Any:
        """Initialize OpenAI client via langchain."""
        # Get API key from environment
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable is required for OpenAI provider"
            )

        return ChatOpenAI(
            model=self.model_name,
            api_key=SecretStr(api_key),  # Wrap in SecretStr for type safety
            temperature=0.5,  # Default, can be overridden per-request via invoke kwargs
            timeout=self.read_timeout,
            max_retries=self.max_attempts,
            # Note: max_tokens is not set here - it's passed to invoke() per-request
            # use_responses_api=True,
        )

    @property
    def exceptions(self) -> Any:
        """Get OpenAI-specific exception classes."""
        try:
            import openai

            return openai
        except ImportError:
            return Exception

    def converse(
        self,
        modelId: str,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Call OpenAI via langchain, translating from Bedrock format to langchain format.

        High-level transformation flow:
        1. Convert Bedrock messages -> langchain messages
        2. Convert Bedrock tools -> langchain tools
        3. Call OpenAI via langchain
        4. Convert langchain response -> Bedrock format

        Handles:
        - Message format conversion (Bedrock multi-block -> langchain single content)
        - Tool calling (Bedrock toolSpec -> langchain function format)
        - Prompt caching (Bedrock cachePoint markers -> OpenAI automatic caching)
        - Response format conversion (langchain AIMessage -> Bedrock output structure)
        """
        # STEP 1: Convert Bedrock system messages to langchain SystemMessage format
        # Bedrock: [{"text": "You are helpful"}, {"text": "Follow rules"}]
        # Langchain: [SystemMessage(content="You are helpful"), SystemMessage(content="Follow rules")]
        lc_messages: List[Any] = []
        for sys_msg in system:
            if isinstance(sys_msg, dict) and "text" in sys_msg:
                lc_messages.append(SystemMessage(content=sys_msg["text"]))

        # STEP 2: Convert Bedrock conversation messages to langchain message format
        #
        # Key difference in message structure:
        # Bedrock: One message can have MULTIPLE content blocks (text, toolUse, cachePoint)
        #   Example: {"role": "user", "content": [
        #       {"text": "What is DataHub?"},
        #       {"text": "Please be concise."},
        #       {"cachePoint": {"type": "default"}}
        #   ]}
        #
        # Langchain: One message has ONE content string
        #   Example: HumanMessage(content="What is DataHub?\nPlease be concise.")
        #
        # We combine all text blocks from one Bedrock message into one langchain message.
        # This preserves message boundaries (each Bedrock message = one turn in conversation).
        #
        # Note on caching:
        # - Bedrock: Explicit cachePoint markers tell where to cache
        # - OpenAI: Automatically caches prompt prefixes ≥1024 tokens on supported models
        #   (GPT-4, GPT-4-turbo, GPT-4o) without special configuration
        # We filter out Bedrock's cachePoint markers and rely on OpenAI's automatic caching
        for msg in messages:
            role = msg.get("role")  # "user" or "assistant"
            content = msg.get("content", [])  # List of content blocks

            # Check if this message contains tool results
            # Tool results need special handling - they become ToolMessage, not HumanMessage
            # Bedrock: {"role": "user", "content": [{"toolResult": {"toolUseId": "123", "content": [...]}}]}
            # Langchain: ToolMessage(content="...", tool_call_id="123")
            tool_messages = self._convert_bedrock_tool_results_to_langchain(content)

            if tool_messages:
                # This message contains tool results - add all ToolMessages
                lc_messages.extend(tool_messages)
                # Skip the rest of the loop - tool results are fully handled
                continue

            # Not a tool result - process as regular message
            # Extract all text blocks from this message and combine them
            text_parts = []
            for block in content:
                if isinstance(block, dict):
                    if "text" in block:
                        # This is a text content block - extract the text
                        text_parts.append(block["text"])
                    elif "cachePoint" in block:
                        # This is a Bedrock-specific caching marker - intentionally skipped
                        # OpenAI handles caching automatically, doesn't use explicit markers
                        pass
                    elif "toolResult" in block:
                        # Tool results already handled above - shouldn't reach here
                        pass
                    else:
                        # Unexpected block type - may be toolUse (handled separately for assistant)
                        # or future block types like image, document, etc.
                        # Log warning to track if we're missing important content
                        logger.warning(
                            f"Unexpected content block type in {role} message during OpenAI conversion: {list(block.keys())}"
                        )

            # Bedrock supports multiple content "parts" (text blocks) in a single message,
            # but Langchain's message format expects a single string for content.
            # Combine all text blocks with newlines to preserve the multi-part structure.
            combined_text = "\n".join(text_parts)

            if role == "user":
                # User message: Convert to langchain HumanMessage
                lc_messages.append(HumanMessage(content=combined_text))
            elif role == "assistant":
                # Assistant message: May contain text and/or tool calls
                # Need to extract tool calls separately from text content
                #
                # Bedrock toolUse format:
                #   {"toolUse": {"toolUseId": "abc", "name": "search", "input": {...}}}
                # Langchain tool_calls format:
                #   {"name": "search", "args": {...}, "id": "abc"}
                #
                # Extract any tool calls from this assistant message
                tool_calls_in_msg = []
                for block in content:
                    if isinstance(block, dict) and "toolUse" in block:
                        tool_use = block["toolUse"]
                        # Transform Bedrock toolUse to langchain tool_call format
                        tool_calls_in_msg.append(
                            {
                                "name": tool_use.get("name"),
                                "args": tool_use.get("input", {}),
                                "id": tool_use.get("toolUseId"),
                            }
                        )

                # Create AIMessage with text content and optional tool calls
                if tool_calls_in_msg:
                    # Assistant made tool calls - include both text and tool_calls
                    lc_messages.append(
                        AIMessage(content=combined_text, tool_calls=tool_calls_in_msg)
                    )
                else:
                    # Regular text response without tool calls
                    lc_messages.append(AIMessage(content=combined_text))

        # STEP 3: Log before API call with structured fields
        logger.info(
            "Calling OpenAI LLM",
            extra={
                "provider": "openai",
                "model": modelId,
                "temperature": inferenceConfig.get("temperature")
                if inferenceConfig
                else None,
                "max_tokens": inferenceConfig.get("maxTokens")
                if inferenceConfig
                else None,
            },
        )

        try:
            # Time the entire API call regardless of tool configuration
            with PerfTimer() as timer:
                # Use shared langchain invocation helper
                # Handles: inference config mapping, tool conversion, cachePoint filtering
                response = self._invoke_with_langchain(
                    lc_messages, toolConfig, inferenceConfig
                )

            # Extract token usage for logging (may be None)
            usage_metadata = getattr(response, "usage_metadata", None)
            if usage_metadata:
                input_tokens = usage_metadata.get("input_tokens", 0)
                output_tokens = usage_metadata.get("output_tokens", 0)
                total_tokens = usage_metadata.get("total_tokens", 0)
            else:
                input_tokens = output_tokens = total_tokens = 0

            # Log after API call with structured fields
            logger.info(
                "OpenAI LLM call completed",
                extra={
                    "provider": "openai",
                    "model": modelId,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "has_content": bool(response.content),
                    "content_length": len(response.content) if response.content else 0,
                    "finish_reason": response.response_metadata.get(
                        "finish_reason", "N/A"
                    ),
                },
            )

        except Exception as e:
            # Translate provider-specific exceptions to standardized LLM exceptions
            #
            # Note: Langchain doesn't wrap provider exceptions - they bubble through directly.
            # We catch openai.* exceptions directly (not wrapped in langchain exceptions).
            # Verified: openai.AuthenticationError, openai.RateLimitError, etc. are raised as-is.
            if isinstance(e, openai.AuthenticationError):
                # API key invalid or missing
                raise LlmAuthenticationException(str(e)) from e
            elif isinstance(e, openai.RateLimitError):
                # Rate limit exceeded (too many requests or tokens)
                raise LlmRateLimitException(str(e)) from e
            elif isinstance(e, openai.BadRequestError):
                # Invalid request - check if it's a context length error
                error_msg = str(e)
                if (
                    "context_length_exceeded" in error_msg
                    or "maximum context length" in error_msg
                ):
                    raise LlmInputTooLongException(error_msg) from e
                else:
                    # Other bad request errors (invalid parameters, etc.)
                    raise LlmValidationException(error_msg) from e
            else:
                # Unknown error type - re-raise as-is for debugging
                # This could be network errors, timeouts, or new OpenAI exception types
                raise

        # STEP 5: Convert langchain response back to Bedrock format
        # Langchain returns: AIMessage(content="...", tool_calls=[...], usage_metadata={...})
        # Bedrock expects: {"output": {"message": {...}}, "stopReason": "...", "usage": {...}}
        # This conversion is done in _convert_langchain_response_to_bedrock() (see base.py)
        return self._convert_langchain_response_to_bedrock(response)
