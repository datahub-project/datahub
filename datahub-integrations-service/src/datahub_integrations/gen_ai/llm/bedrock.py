"""AWS Bedrock LLM wrapper implementation."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.gen_ai.bedrock import get_bedrock_client
from datahub_integrations.gen_ai.llm.base import LLMWrapper
from datahub_integrations.gen_ai.llm.bedrock_stream_aggregator import (
    aggregate_converse_stream,
)
from datahub_integrations.gen_ai.llm.daily_limiter import get_daily_token_limiter
from datahub_integrations.gen_ai.llm.exceptions import (
    LlmAuthenticationException,
    LlmInputTooLongException,
    LlmRateLimitException,
    LlmValidationException,
)
from datahub_integrations.gen_ai.llm.types import ConverseResponse
from datahub_integrations.observability.cost import TokenUsage, get_cost_tracker
from datahub_integrations.observability.decorators import otel_llm_call
from datahub_integrations.observability.metrics_constants import AIModule

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


class BedrockLLMWrapper(LLMWrapper):
    """AWS Bedrock LLM wrapper - passes through to native Bedrock API."""

    provider_name = "bedrock"

    def _initialize_client(self) -> Any:
        """Initialize AWS Bedrock client using existing infrastructure."""
        return get_bedrock_client(
            read_timeout=self.read_timeout, connect_timeout=self.connect_timeout
        )

    @property
    def exceptions(self) -> Any:
        """Get Bedrock-specific exception classes."""
        return self._client.exceptions

    @otel_llm_call(ai_module_param="ai_module")
    def converse(
        self,
        *,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        ai_module: AIModule,
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Call Bedrock using streaming API internally, returning complete response.

        This uses converse_stream() under the hood to avoid read timeouts on large outputs,
        but accumulates all chunks and returns the same format as the request/response API.

        Catches Bedrock-specific exceptions and translates them to standardized LlmException types.
        """
        # Fail fast if daily token limit already exceeded
        get_daily_token_limiter().check()

        kwargs: Dict[str, Any] = {
            "modelId": self.model_name,
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
                    "model": self.model_name,
                    "ai_module": ai_module.value,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "stop_reason": stop_reason,
                },
            )

            # Track cost for observability
            # Bedrock returns ALL THREE as SEPARATE, non-overlapping values:
            # - inputTokens: non-cached input tokens (charged at prompt rate)
            # - cacheReadInputTokens: tokens served from cache (charged at ~10% rate)
            # - cacheWriteInputTokens: tokens written to cache (charged at ~125% rate)
            # Total input = inputTokens + cacheReadInputTokens + cacheWriteInputTokens
            cache_read_tokens = usage.get("cacheReadInputTokens", 0)
            cache_write_tokens = usage.get("cacheWriteInputTokens", 0)
            # prompt_tokens = inputTokens (these are the non-cached, non-cache-write tokens)
            prompt_tokens = input_tokens

            # Debug: Log raw usage values from Bedrock to verify token counting
            logger.debug(
                f"Bedrock raw usage: inputTokens={input_tokens}, outputTokens={output_tokens}, "
                f"cacheRead={cache_read_tokens}, cacheWrite={cache_write_tokens}"
            )

            get_cost_tracker().record_llm_call(
                provider="bedrock",
                model=self.model_name,
                usage=TokenUsage(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=output_tokens,
                    total_tokens=input_tokens
                    + output_tokens
                    + cache_read_tokens
                    + cache_write_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_write_tokens=cache_write_tokens,
                ),
                ai_module=ai_module,
                success=True,
            )

            # Record usage for daily token limit tracking
            get_daily_token_limiter().record_usage(
                input_tokens + output_tokens + cache_read_tokens + cache_write_tokens
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
