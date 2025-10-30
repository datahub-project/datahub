import functools
import json
import logging
import os
import pprint
import time
from typing import TYPE_CHECKING, List, Optional, Union

import boto3
import botocore.config
import pydantic
from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger

from datahub_integrations.gen_ai.model_config import BedrockModel
from datahub_integrations.util.serialized import serialized

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient

_LLM_TRACE = get_boolean_env_variable("DATAHUB_LLM_TRACE")

# Enable boto3 debug logging for troubleshooting timeouts
logging.getLogger("boto3").setLevel(logging.DEBUG)
logging.getLogger("botocore").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

_ENABLE_BEDROCK_OPTIMIZED_LATENCY = get_boolean_env_variable(
    "ENABLE_BEDROCK_OPTIMIZED_LATENCY", False
)

_ENABLE_BEDROCK_PROMPT_CACHING = get_boolean_env_variable(
    "ENABLE_BEDROCK_PROMPT_CACHING", False
)

_MAX_ATTEMPTS = int(os.getenv("BEDROCK_MAX_ATTEMPTS", "10"))


# Generic return type for Bedrock inference responses.
class BedrockResponseBody(pydantic.BaseModel):
    model: BedrockModel | str
    text: str
    stop_reason: str

    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None

    retry_attempts: Optional[int] = None


class BedrockPromptMessage(pydantic.BaseModel):
    text: str
    cache: bool = False


@serialized
@functools.cache
def get_bedrock_client(
    read_timeout: int = 60, connect_timeout: int = 60
) -> "BedrockRuntimeClient":
    """
    Get a singleton Bedrock client with appropriate authentication.

    Authentication is determined in the following order:
    1. Explicit credentials via BEDROCK_AWS_ACCESS_KEY_ID and BEDROCK_AWS_SECRET_ACCESS_KEY
    2. AWS Profile via AWS_PROFILE (supports SSO profiles)
    3. Default AWS credential chain (~/.aws/credentials, instance profile, etc.)

    For local development with SSO:
    - Run: aws sso login --profile your-profile-name
    - Set: AWS_PROFILE=your-profile-name
    - Optionally set: BEDROCK_AWS_REGION=us-west-2 (or your preferred region)

    The cache decorator ensures that this is a singleton, and the serialized
    decorator ensures that it is only initialized once even if called from multiple threads.
    """
    config = botocore.config.Config(
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
        max_pool_connections=100,
        retries={"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
    )

    if "BEDROCK_AWS_ROLE" in os.environ:
        logger.warning(
            "Using BEDROCK_AWS_ROLE is to assume a role is no longer supported. "
            "Use instance profiles or explicit credentials."
        )

    if "BEDROCK_AWS_ACCESS_KEY_ID" in os.environ:
        # Option 1: Explicit credentials for local development
        logger.info("Initializing Bedrock client from explicit env vars")
        boto3_session = boto3.Session(
            aws_access_key_id=os.environ["BEDROCK_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["BEDROCK_AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("BEDROCK_AWS_REGION", "us-west-2"),
        )
    elif "AWS_PROFILE" in os.environ:
        # Option 2: AWS Profile (great for local development with SSO)
        profile_name = os.environ["AWS_PROFILE"]
        logger.info(f"Initializing Bedrock client from AWS profile: {profile_name}")
        boto3_session = boto3.Session(
            profile_name=profile_name,
            region_name=os.environ.get("BEDROCK_AWS_REGION", "us-west-2"),
        )
    else:
        # Option 3: Default - use instance profile or standard AWS credential chain
        # This will check ~/.aws/credentials, EC2 instance profile, etc.
        # Region is determined from AWS config, AWS_REGION env var, or instance metadata
        logger.info(
            "Initializing Bedrock client from default credential chain (instance profile or AWS CLI)"
        )
        boto3_session = boto3.Session()

    return boto3_session.client("bedrock-runtime", config=config)  # type: ignore


def call_bedrock_llm(
    prompt: Union[str, List[BedrockPromptMessage]],
    max_tokens: int,
    model: BedrockModel | str,
    temperature: float = 0.3,
    system_messages: Optional[List[BedrockPromptMessage]] = None,
) -> str:
    boto3_bedrock = get_bedrock_client()
    response = call_bedrock_llm_inner(
        boto3_bedrock, prompt, max_tokens, model, temperature, system_messages
    )
    return response.text


def call_bedrock_llm_inner(
    boto3_bedrock: "BedrockRuntimeClient",
    prompt: Union[str, List[BedrockPromptMessage]],
    max_tokens: int,
    model: BedrockModel | str,
    temperature: float,
    system_messages: Optional[List[BedrockPromptMessage]] = None,
) -> BedrockResponseBody:
    body = prepare_body_for_prompt(prompt, max_tokens, temperature, system_messages)
    accept = "application/json"
    contentType = "application/json"
    modelId = model.value if isinstance(model, BedrockModel) else model
    if _LLM_TRACE:
        logger.info(f"Calling Bedrock LLM with model {modelId} and prompt:\n{prompt}")

    start_time = time.time()
    response = boto3_bedrock.invoke_model(
        body=json.dumps(body),
        modelId=modelId,
        accept=accept,
        contentType=contentType,
        performanceConfigLatency="optimized"
        if _ENABLE_BEDROCK_OPTIMIZED_LATENCY
        else "standard",
    )
    response_body = json.loads(response["body"].read())
    logger.info(f"LLM call took {time.time() - start_time} seconds")
    if _LLM_TRACE:
        logger.info(
            f"LLM response body: {pprint.pformat(response_body, sort_dicts=False, width=120)}"
        )

    # If the generation ran out of tokens, log a warning.
    stop_reason = response_body["stop_reason"]
    if stop_reason not in {"end_turn", "tool_use"}:
        logger.warning(f"LLM call stopped early: {stop_reason}")

    return BedrockResponseBody(
        model=model,
        text=response_body["content"][0]["text"],
        stop_reason=stop_reason,
        input_tokens=response_body["usage"]["input_tokens"],
        output_tokens=response_body["usage"]["output_tokens"],
        retry_attempts=response["ResponseMetadata"]["RetryAttempts"],
    )


def prepare_body_for_prompt(
    prompt: Union[str, List[BedrockPromptMessage]],
    max_tokens: int,
    temperature: float,
    system_messages: Optional[List[BedrockPromptMessage]] = None,
) -> dict:
    if isinstance(prompt, str):
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                }
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
    else:
        # depending on cache, we need to format the messages differently
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": message.text,
                            "cache_control": {
                                "type": "ephemeral",
                            },
                        }
                        if message.cache and _ENABLE_BEDROCK_PROMPT_CACHING
                        else {"type": "text", "text": message.text}
                        for message in prompt
                    ],
                }
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

    # Add system messages if provided
    if system_messages:
        system_content = [
            {
                "type": "text",
                "text": message.text,
                "cache_control": {
                    "type": "ephemeral",
                },
            }
            if message.cache and _ENABLE_BEDROCK_PROMPT_CACHING
            else {"type": "text", "text": message.text}
            for message in system_messages
        ]
        body["system"] = system_content

    return body


if __name__ == "__main__":
    # Simple testing code.
    import sys

    text_prompt = sys.argv[1]
    logger.info(call_bedrock_llm(text_prompt, 100, BedrockModel.CLAUDE_37_SONNET))
