"""Google Gemini/Vertex AI LLM wrapper implementation."""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

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


# See https://docs.cloud.google.com/vertex-ai/generative-ai/docs/thinking
# Gemini 3+ models: Use thinking_level="LOW" (thinking cannot be turned off)
# Gemini 2.5 models: Use thinking_budget (2.5 Pro: min 128, Flash/Flash-Lite: can use 0)
_GEMINI_THINKING_CONFIG: Dict[str, Dict[str, Any]] = {
    "gemini-3-pro-preview": {"thinking_level": "LOW"},
    "gemini-2.5-pro": {"thinking_budget": 128},  # min 128, cannot turn off
    "gemini-2.5-flash": {"thinking_budget": 0},  # can turn off (min 1 if enabled)
    "gemini-2.5-flash-lite": {
        "thinking_budget": 0
    },  # can turn off (min 512 if enabled)
}


class GeminiLLMWrapper(LLMWrapper):
    """Google Gemini/Vertex AI LLM wrapper using langchain."""

    def _get_thinking_config(self) -> Dict[str, Any]:
        name = self.model_name.lower().strip()

        if name in _GEMINI_THINKING_CONFIG:
            return _GEMINI_THINKING_CONFIG[name].copy()

        # Match by base name (e.g., "gemini-2.5-pro-001" -> "gemini-2.5-pro")
        for key in _GEMINI_THINKING_CONFIG:
            if name.startswith(key):
                return _GEMINI_THINKING_CONFIG[key].copy()

        if name.startswith("gemini-3"):
            logger.debug(
                f"Unknown Gemini 3 model '{self.model_name}', using default LOW thinking level"
            )
            return {"thinking_level": "LOW"}
        logger.debug(
            f"Unknown Gemini model '{self.model_name}', using default thinking_budget=0"
        )
        return {"thinking_budget": 0}

    def _preprocess_schema_for_vertex_ai(
        self, schema: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Preprocess JSON schema to remove null types from anyOf arrays for Vertex AI protobuf compatibility."""
        if not isinstance(schema, dict):
            return schema

        if "$ref" in schema:
            return schema

        converted_schema: Dict[str, Any] = {}

        for key, value in schema.items():
            if key == "$defs":
                converted_schema["$defs"] = {
                    def_key: self._preprocess_schema_for_vertex_ai(def_value)
                    for def_key, def_value in value.items()
                }
            elif key == "items":
                converted_schema["items"] = self._preprocess_schema_for_vertex_ai(value)
            elif key == "properties":
                processed_properties = {}
                for pkey, pvalue in value.items():
                    processed = self._preprocess_schema_for_vertex_ai(pvalue)
                    if processed is not None:
                        processed_properties[pkey] = processed
                converted_schema["properties"] = processed_properties
                if "required" in schema:
                    converted_schema["required"] = schema["required"]
            elif key == "anyOf":
                if not isinstance(value, list):
                    logger.warning(
                        f"anyOf value is not a list: {type(value)}, preserving as-is"
                    )
                    converted_schema["anyOf"] = value
                else:
                    non_null_items = [
                        item
                        for item in value
                        if not (isinstance(item, dict) and item.get("type") == "null")
                    ]
                    if not non_null_items:
                        return None
                    elif len(non_null_items) == 1:
                        return self._preprocess_schema_for_vertex_ai(non_null_items[0])
                    else:
                        converted_schema["anyOf"] = [
                            self._preprocess_schema_for_vertex_ai(item)
                            for item in non_null_items
                        ]
            elif key == "allOf":
                if not isinstance(value, list):
                    logger.warning(
                        f"allOf value is not a list: {type(value)}, preserving as-is"
                    )
                    return {"allOf": value}
                if not value:
                    logger.warning("allOf array is empty, returning empty schema")
                    return {}
                if len(value) > 1:
                    logger.warning(
                        f"Only first value for 'allOf' key is supported. Got {len(value)}, ignoring other than first value!"
                    )
                return self._preprocess_schema_for_vertex_ai(value[0])
            elif key == "required":
                converted_schema[key] = value
            else:
                converted_schema[key] = value

        return converted_schema

    def _initialize_client(self) -> Any:
        # Schema preprocessing now happens in _convert_bedrock_tools_to_langchain()
        # No need to patch langchain internals anymore

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

        thinking_config = self._get_thinking_config()

        return ChatVertexAI(
            model=self.model_name,
            project=project,
            location=location,
            temperature=0.5,
            timeout=self.read_timeout,
            max_retries=self.max_attempts,
            **thinking_config,
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

    def _convert_bedrock_tools_to_langchain(
        self, bedrock_tools: Sequence[Any]
    ) -> List[Dict[str, Any]]:
        """
        Convert Bedrock tool format to langchain format, with Vertex AI schema preprocessing.

        Overrides base class method to preprocess schemas before langchain conversion.
        This ensures tool configs are protobuf-compatible for Vertex AI by removing
        null types from anyOf arrays.

        Args:
            bedrock_tools: List of Bedrock toolSpec format tools

        Returns:
            List of langchain function format tools with preprocessed schemas
        """
        lc_tools = []
        for tool in bedrock_tools:
            if "toolSpec" in tool:
                spec = tool["toolSpec"]
                # Extract raw schema
                raw_schema = spec.get("inputSchema", {}).get("json", {})

                # Preprocess schema for Vertex AI protobuf compatibility
                # This removes null types from anyOf arrays before langchain processes it
                preprocessed_schema = self._preprocess_schema_for_vertex_ai(raw_schema)

                lc_tool = {
                    "type": "function",
                    "function": {
                        "name": spec["name"],
                        "description": spec.get("description", ""),
                        "parameters": preprocessed_schema,  # Use preprocessed schema
                    },
                }
                lc_tools.append(lc_tool)
            else:
                logger.warning(
                    f"Unexpected tool format in Bedrock tools list (expected 'toolSpec'): {list(tool.keys())}"
                )
        return lc_tools
