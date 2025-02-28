import enum
import functools
import json
import os
import pprint
import time
from typing import TYPE_CHECKING

import boto3
import botocore.config
from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger

from datahub_integrations.util.serialized import serialized

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient

_LLM_TRACE = get_boolean_env_variable("DATAHUB_LLM_TRACE")


class BedrockModel(enum.Enum):
    CLAUDE_3_HAIKU = "anthropic.claude-3-haiku-20240307-v1:0"
    CLAUDE_35_SONNET = "anthropic.claude-3-5-sonnet-20240620-v1:0"

    CLAUDE_35_HAIKU = "anthropic.claude-3-5-haiku-20241022-v1:0"
    CLAUDE_35_SONNET_V2 = "anthropic.claude-3-5-sonnet-20241022-v2:0"


@serialized
@functools.cache
def get_bedrock_client() -> "BedrockRuntimeClient":
    # Set up Bedrock client. The cache decorator ensures that this is a singleton,
    # and the serialized decorator ensures that it is only initialized once
    # even if called from multiple threads.

    # Increase the read and connect timeouts, since Bedrock can be slow.
    config = botocore.config.Config(read_timeout=300, connect_timeout=60)

    if "BEDROCK_AWS_ROLE" in os.environ:
        logger.warning(
            "Using BEDROCK_AWS_ROLE is to assume a role is no longer supported. "
            "Use instance profiles or explicit credentials."
        )

    if "BEDROCK_AWS_ACCESS_KEY_ID" in os.environ:
        # For local development - if Bedrock-specific env vars are set, use them.
        logger.info("Initializing Bedrock client from explicit env vars")
        boto3_session = boto3.Session(
            aws_access_key_id=os.environ["BEDROCK_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["BEDROCK_AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("BEDROCK_AWS_REGION", "us-west-2"),
        )
    else:
        # By default, use the pod's instance profile.
        logger.info("Initializing Bedrock client from instance profile")
        boto3_session = boto3.Session()

    return boto3_session.client("bedrock-runtime", config=config)  # type: ignore


def call_bedrock_llm(
    prompt: str, max_tokens: int, model: BedrockModel, temperature: float = 0.3
) -> str:
    start_time = time.time()
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
    accept = "application/json"
    contentType = "application/json"

    boto3_bedrock = get_bedrock_client()

    if _LLM_TRACE:
        logger.info(
            f"Calling Bedrock LLM with model {model.value} and prompt:\n{prompt}"
        )
    response = boto3_bedrock.invoke_model(
        body=json.dumps(body),
        modelId=model.value,
        accept=accept,
        contentType=contentType,
    )
    response_body = json.loads(response["body"].read())
    if _LLM_TRACE:
        logger.info(
            f"LLM response body: {pprint.pformat(response_body, sort_dicts=False, width=120)}"
        )

    # If the generation ran out of tokens, log a warning.
    stop_reason = response_body["stop_reason"]
    if stop_reason not in {"end_turn", "tool_use"}:
        logger.warning(f"LLM call stopped early: {stop_reason}")

    outputText = response_body["content"][0]["text"]

    logger.info(f"LLM call took {time.time() - start_time} seconds")
    return outputText


if __name__ == "__main__":
    # Simple testing code.
    import sys

    prompt = sys.argv[1]
    logger.info(call_bedrock_llm(prompt, 100, BedrockModel.CLAUDE_35_HAIKU))
