"""OpenAI LLM wrapper implementation."""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import httpx
import openai
from datahub.utilities.perf_timer import PerfTimer
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


class CustomOpenAIProxyLLMWrapper(LLMWrapper):
    """Custom OpenAI Proxy LLM wrapper using langchain."""

    def _initialize_client(self) -> Any:
        """Initialize Custom OpenAI Proxy client via langchain."""

        openai_api_key = None
        custom_client = None
        custom_base_url = None
        if not self.custom_model_provider:
            raise ValueError(
                "custom_model_provider provider is required for CustomOpenAIProxyLLMWrapper provider"
            )

        # create a custom http client if cert file and key file exist
        if self.custom_model_provider.cert_file and self.custom_model_provider.key_file:
            custom_client = httpx.Client(
                cert=(
                    self.custom_model_provider.cert_file,
                    self.custom_model_provider.key_file,
                )
            )

        # Intentionally creating has_custom_openai_api_key to avoid accidentally logging the api key
        has_custom_openai_api_key = False
        if self.custom_model_provider.api_key:
            openai_api_key = str(self.custom_model_provider.api_key)
            has_custom_openai_api_key = True

        if self.custom_model_provider.base_url:
            custom_base_url = self.custom_model_provider.base_url

        # Get API key from environment if not already present
        if not openai_api_key:
            openai_api_key = os.environ.get("OPENAI_API_KEY")

        if not openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable or custom model provider API key is required for OpenAI provider"
            )

        logger.info(
            f"Initializing CustomOpenAIProxyLLMWrapper with model={self.model_name}, \
            custom_base_url={custom_base_url}, \
            has_custom_openai_api_key={has_custom_openai_api_key}, \
            has_custom_http_client={custom_client is not None}, \
            cert_file={self.custom_model_provider.cert_file}, \
            key_file={self.custom_model_provider.key_file}"
        )

        return ChatOpenAI(
            model=self.model_name,
            api_key=SecretStr(openai_api_key),  # Wrap in SecretStr for type safety
            http_client=custom_client,
            http_async_client=custom_client,  # Langchain requires a separate custom http client for async calls
            base_url=custom_base_url,
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
        """  # STEP 1 & 2: Convert Bedrock messages to langchain format
        # Use shared helper from base class
        lc_messages = self._convert_bedrock_messages_to_langchain(system, messages)

        # STEP 3: Log before API call with structured fields
        logger.info(
            "Calling OpenAI LLM (streaming)",
            extra={
                "provider": "openai",
                "model": self.model_name,
                "temperature": inferenceConfig.get("temperature")
                if inferenceConfig
                else None,
                "max_tokens": inferenceConfig.get("maxTokens")
                if inferenceConfig
                else None,
            },
        )

        try:
            # Time the entire API call
            with PerfTimer() as timer:
                # Use shared langchain streaming helper from base class
                # This handles: inference config mapping, tool conversion, cachePoint filtering,
                # and streaming with langchain's built-in chunk combining
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

            # Extract response metadata
            response_metadata = getattr(response, "response_metadata", {})

            # Log after API call with structured fields
            logger.info(
                "OpenAI LLM call completed (streaming)",
                extra={
                    "provider": "openai",
                    "model": self.model_name,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "has_content": bool(response.content),
                    "content_length": len(response.content) if response.content else 0,
                    "finish_reason": response_metadata.get("finish_reason", "N/A"),
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
