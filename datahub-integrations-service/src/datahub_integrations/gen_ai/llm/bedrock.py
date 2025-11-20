"""AWS Bedrock LLM wrapper implementation."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.gen_ai.bedrock import get_bedrock_client
from datahub_integrations.gen_ai.llm.base import LLMWrapper
from datahub_integrations.gen_ai.llm.bedrock_stream_aggregator import (
    aggregate_converse_stream,
)
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


class BedrockLLMWrapper(LLMWrapper):
    """AWS Bedrock LLM wrapper - passes through to native Bedrock API."""

    def _initialize_client(self) -> Any:
        """Initialize AWS Bedrock client using existing infrastructure."""
        return get_bedrock_client(
            read_timeout=self.read_timeout, connect_timeout=self.connect_timeout
        )

    @property
    def exceptions(self) -> Any:
        """Get Bedrock-specific exception classes."""
        return self._client.exceptions

    def converse(
        self,
        modelId: str,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Call Bedrock using streaming API internally, returning complete response.

        This uses converse_stream() under the hood to avoid read timeouts on large outputs,
        but accumulates all chunks and returns the same format as the request/response API.

        Catches Bedrock-specific exceptions and translates them to standardized LlmException types.
        """
        kwargs: Dict[str, Any] = {
            "modelId": modelId,
            "system": system,
            "messages": messages,
        }

        if toolConfig:
            kwargs["toolConfig"] = toolConfig

        if inferenceConfig:
            kwargs["inferenceConfig"] = inferenceConfig

        # Log before API call with structured fields
        logger.info(
            "Calling Bedrock LLM (streaming)",
            extra={
                "provider": "bedrock",
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
            with PerfTimer() as timer:
                # Use streaming API and accumulate results
                stream_response = self._client.converse_stream(**kwargs)

                # Aggregate the stream into a complete response
                aggregated = aggregate_converse_stream(stream_response["stream"])

            # Extract components from aggregated response
            usage = aggregated.get("usage", {})
            stop_reason = aggregated.get("stopReason", "end_turn")

            # Build the complete response in the same format as converse()
            response: ConverseResponse = {
                "output": aggregated["output"],
                "stopReason": stop_reason,
                "usage": {
                    "inputTokens": usage.get("inputTokens", 0),
                    "outputTokens": usage.get("outputTokens", 0),
                },
            }

            # Add optional cache token fields if present
            if "cacheReadInputTokens" in usage:
                response["usage"]["cacheReadInputTokens"] = usage[
                    "cacheReadInputTokens"
                ]
            if "cacheWriteInputTokens" in usage:
                response["usage"]["cacheWriteInputTokens"] = usage[
                    "cacheWriteInputTokens"
                ]

            # Extract token usage for logging
            input_tokens = usage.get("inputTokens", 0)
            output_tokens = usage.get("outputTokens", 0)
            total_tokens = usage.get("totalTokens", 0)

            # Log after API call with structured fields
            logger.info(
                "Bedrock LLM call completed (streaming)",
                extra={
                    "provider": "bedrock",
                    "model": modelId,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "stop_reason": stop_reason,
                },
            )

            # Note: stopReason == "max_tokens" is a valid response, not an error
            # The calling code will check stopReason and handle it appropriately
            return response

        except self._client.exceptions.ValidationException as e:
            error_msg = str(e)
            # Check for input too long errors (Bedrock-specific error messages)
            if "Input is too long" in error_msg or "exceed context limit" in error_msg:
                raise LlmInputTooLongException(error_msg) from e
            else:
                # Other validation errors (invalid model, invalid config, etc.)
                raise LlmValidationException(error_msg) from e
        except self._client.exceptions.ThrottlingException as e:
            # Rate limit exceeded
            raise LlmRateLimitException(str(e)) from e
        except self._client.exceptions.AccessDeniedException as e:
            # Authentication/authorization failure
            raise LlmAuthenticationException(str(e)) from e
