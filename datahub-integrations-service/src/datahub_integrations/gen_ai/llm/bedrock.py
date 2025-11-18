"""AWS Bedrock LLM wrapper implementation."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.gen_ai.bedrock import get_bedrock_client
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
        Call Bedrock's native converse API with exception translation.

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
            "Calling Bedrock LLM",
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
                response = self._client.converse(**kwargs)

            # Extract token usage for logging
            usage = response.get("usage", {})
            input_tokens = usage.get("inputTokens", 0)
            output_tokens = usage.get("outputTokens", 0)
            total_tokens = usage.get("totalTokens", 0)
            stop_reason = response.get("stopReason", "unknown")

            # Log after API call with structured fields
            logger.info(
                "Bedrock LLM call completed",
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
            return response  # type: ignore[return-value]

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
