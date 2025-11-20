"""Google Gemini/Vertex AI LLM wrapper implementation."""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datahub.utilities.perf_timer import PerfTimer
from google.api_core import exceptions as gcp_exceptions
from langchain_google_vertexai import ChatVertexAI
from loguru import logger

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


class GeminiLLMWrapper(LLMWrapper):
    """Google Gemini/Vertex AI LLM wrapper using langchain."""

    def _initialize_client(self) -> Any:
        """Initialize Google Gemini/Vertex AI client via langchain."""
        # For Vertex AI, we need project and location
        project = os.environ.get("VERTEXAI_PROJECT")
        location = os.environ.get("VERTEXAI_LOCATION")

        if not project:
            raise ValueError(
                "VERTEXAI_PROJECT environment variable is required for Gemini provider"
            )
        if not location:
            raise ValueError(
                "VERTEXAI_LOCATION environment variable is required for Gemini provider"
            )

        return ChatVertexAI(
            model=self.model_name,
            project=project,
            location=location,
            temperature=0.5,  # Default, can be overridden per-request via invoke kwargs
            thinking_budget=0,  # Disable extended reasoning mode by default
            timeout=self.read_timeout,
            max_retries=self.max_attempts,
            # Note: max_tokens is not set here - it's passed to invoke() per-request
        )

    @property
    def exceptions(self) -> Any:
        """Get Gemini/Vertex AI-specific exception classes."""
        try:
            from google.api_core import exceptions

            return exceptions
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
        Call Gemini/Vertex AI via langchain, translating from Bedrock format.

        Follows the same transformation pattern as OpenAI:
        1. Convert Bedrock messages -> langchain messages
        2. Convert Bedrock tools -> langchain tools
        3. Call Gemini via langchain
        4. Convert langchain response -> Bedrock format

        Key differences from OpenAI:
        - Uses max_output_tokens instead of max_tokens
        - No prompt caching support (cachePoint markers are skipped)
        - Tool calling format may have subtle differences
        """
        # STEP 1 & 2: Convert Bedrock messages to langchain format
        # Use shared helper from base class
        lc_messages = self._convert_bedrock_messages_to_langchain(system, messages)

        # STEP 3: Log before API call with structured fields
        logger.info(
            "Calling Gemini LLM (streaming)",
            extra={
                "provider": "gemini",
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

            # Log after API call with structured fields
            logger.info(
                "Gemini LLM call completed (streaming)",
                extra={
                    "provider": "gemini",
                    "model": modelId,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                },
            )

        except Exception as e:
            # Translate Gemini/Vertex AI-specific exceptions to standardized LLM exceptions
            if isinstance(e, gcp_exceptions.PermissionDenied):
                raise LlmAuthenticationException(str(e)) from e
            elif isinstance(e, gcp_exceptions.ResourceExhausted):
                raise LlmRateLimitException(str(e)) from e
            elif isinstance(e, gcp_exceptions.InvalidArgument):
                error_msg = str(e)
                # Check for context length errors (Gemini-specific error messages)
                if (
                    "context length" in error_msg.lower()
                    or "too long" in error_msg.lower()
                ):
                    raise LlmInputTooLongException(error_msg) from e
                else:
                    raise LlmValidationException(error_msg) from e
            else:
                # Unknown Gemini error - re-raise as-is for debugging
                raise

        # STEP 5: Convert langchain response to Bedrock format
        # Uses the same conversion method as OpenAI (see base.py)
        return self._convert_langchain_response_to_bedrock(response)
