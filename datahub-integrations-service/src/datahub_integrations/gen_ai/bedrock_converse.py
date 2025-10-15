import time
from typing import TYPE_CHECKING, Sequence

from loguru import logger

from datahub_integrations.gen_ai.bedrock import get_bedrock_client
from datahub_integrations.gen_ai.model_config import BedrockModel

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


def converse_with_bedrock(
    system_message: "SystemContentBlockTypeDef",
    bedrock_messages: Sequence["MessageUnionTypeDef"],
    model_id: BedrockModel | str,
    temperature: float,
    max_tokens: int,
) -> str:
    try:
        bedrock_client = get_bedrock_client()
        start_time = time.time()
        response = bedrock_client.converse(
            modelId=model_id.value if isinstance(model_id, BedrockModel) else model_id,
            system=[system_message],
            messages=bedrock_messages,
            inferenceConfig={
                "temperature": temperature,
                "maxTokens": max_tokens,
            },
        )
        logger.info(f"LLM call took {time.time() - start_time} seconds")

        stop_reason = response["stopReason"]
        if stop_reason != "end_turn":
            logger.warning(f"LLM call stopped early: {stop_reason}")
    except bedrock_client.exceptions.ValidationException as e:
        logger.error(f"Validation exception: {e}")
        raise e
    return response["output"]["message"]["content"][0]["text"]
