import logging
import os
import pprint
import time
import typing
from typing import Any, List, Optional, Union

import litellm
import pydantic
from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger

from datahub_integrations.gen_ai.model_config import LiteLLMModel

_LLM_TRACE = get_boolean_env_variable("DATAHUB_LLM_TRACE")

# Enable boto3 debug logging for troubleshooting timeouts
logging.getLogger("boto3").setLevel(logging.DEBUG)
logging.getLogger("botocore").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

_ENABLE_LITELLM_OPTIMIZED_LATENCY = get_boolean_env_variable(
    "_ENABLE_LITELLM_OPTIMIZED_LATENCY", False
)

_ENABLE_LITELLM_PROMPT_CACHING = get_boolean_env_variable(
    "ENABLE_LITELLM_PROMPT_CACHING", False
)

_MAX_ATTEMPTS = int(os.getenv("LITELLM_MAX_ATTEMPTS", "3"))


# Generic return type for Bedrock inference responses.
class LiteLLMResponseBody(pydantic.BaseModel):
    model: LiteLLMModel | str
    text: str
    stop_reason: str

    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None

    retry_attempts: Optional[int] = None

    first_token_latency: float
    total_latency: float


class LiteLLMPromptMessage(pydantic.BaseModel):
    text: str
    cache: bool = False


class LiteLLM:
    """A minimal sdk for using LiteLLM.

    Accepts OpenAI-style messages: [{"role": "user"|"system"|"assistant", "content": str}].
    Returns a normalized `LiteLLMResponseBody` instance.
    """

    def __init__(
        self,
        model: LiteLLMModel | str,
        max_tokens: int,
        temperature: float,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature

    def prepare_body_for_prompt(
        self,
        prompt: Union[str, List[LiteLLMPromptMessage]],
    ) -> list[dict]:
        if isinstance(prompt, str):
            messages = [
                {
                    "role": "user",
                    "content": prompt,
                }
            ]
        else:
            messages = []
            for item in prompt:
                message: dict[str, typing.Any] = {"role": "user", "content": item.text}

                if item.cache and _ENABLE_LITELLM_PROMPT_CACHING:
                    message["cache_control"] = {"type": "ephemeral"}

                messages.append(message)

        return messages

    def call_lite_llm(self, prompt: Union[str, List[LiteLLMPromptMessage]]) -> str:
        return self.call_lite_llm_inner(
            prompt=prompt,
            max_tokens=self.max_tokens,
            model=self.model,
            temperature=self.temperature,
        ).text

    def call_lite_llm_inner(
        self,
        prompt: Union[str, List[LiteLLMPromptMessage]],
        max_tokens: int,
        model: LiteLLMModel | str,
        temperature: float,
    ) -> LiteLLMResponseBody:
        if _LLM_TRACE:
            litellm._turn_on_debug()

        model_id = model.value if isinstance(model, LiteLLMModel) else model

        messages = self.prepare_body_for_prompt(prompt)
        performanceConfig = None

        if _ENABLE_LITELLM_OPTIMIZED_LATENCY:
            performanceConfig = {"latency": "optimized"}

        start_time = time.time()
        model_response_stream = litellm.completion(
            model=model_id,
            messages=messages,
            stream=True,
            timeout=60,
            stream_timeout=10,
            num_retries=_MAX_ATTEMPTS,
            temperature=temperature,
            max_tokens=max_tokens,
            stream_options={"include_usage": True},
            performanceConfig=performanceConfig,
        )

        parsed_llm_response = self.handle_streaming_response(
            start_time, model_response_stream
        )

        logger.bind(
            total_time=parsed_llm_response.total_latency,
            first_token_elapsed=parsed_llm_response.first_token_latency,
        ).info(
            f"LLM call took {parsed_llm_response.total_latency} seconds, first token in {parsed_llm_response.first_token_latency} seconds"
        )

        if _LLM_TRACE:
            logger.info(
                f"LLM response body: {pprint.pformat(parsed_llm_response.text, sort_dicts=False, width=120)}"
            )

        # If the generation ran out of tokens, log a warning.
        if parsed_llm_response.stop_reason not in {"end_turn", "tool_use", "stop"}:
            logger.warning(f"LLM call stopped early: {parsed_llm_response.stop_reason}")

        return parsed_llm_response

    def handle_streaming_response(
        self, start_time: float, model_response_stream: Any
    ) -> LiteLLMResponseBody:
        complete_response_content = ""
        finish_reason = ""
        first_token_time = 0.0

        input_tokens = 0
        output_tokens = 0

        for chunk in model_response_stream:
            if first_token_time == 0.0:
                first_token_time = time.time()
            # Each chunk is a dictionary containing response data
            # The actual content is typically in chunk["choices"][0]["delta"]["content"]
            # if chunk and choices exist, the full payload including `delta`` and `content` will always exist.
            if chunk and chunk["choices"] and chunk["choices"][0]:
                first_chunk = chunk["choices"][0]
                if "content" in first_chunk["delta"]:
                    content_part = first_chunk["delta"]["content"]
                    if content_part is not None:
                        complete_response_content += content_part

                if first_chunk["finish_reason"] is not None:
                    finish_reason = chunk["choices"][0]["finish_reason"]

            # usage data will be included at the very end of the response.
            if chunk and "usage" in chunk:
                output_tokens = chunk["usage"]["completion_tokens"]
                input_tokens = chunk["usage"]["prompt_tokens"]

            # metadata is last message
        total_time_elapsed = time.time() - start_time
        first_token_elapsed = first_token_time - start_time

        return LiteLLMResponseBody(
            model=model_response_stream.model,
            text=complete_response_content,
            stop_reason=finish_reason,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            first_token_latency=first_token_elapsed,
            total_latency=total_time_elapsed,
        )
