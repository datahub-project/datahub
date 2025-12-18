import functools
import json
import logging
import os
import pprint
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import boto3
import botocore.config
import pydantic
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger

from datahub_integrations.gen_ai.model_config import BedrockModel
from datahub_integrations.util.serialized import serialized

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient
from datahub_integrations import is_dev_mode

if not is_dev_mode():
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("s3transfer").setLevel(logging.INFO)

_LLM_TRACE = get_boolean_env_variable("DATAHUB_LLM_TRACE")

# Enable boto3 debug logging for troubleshooting timeouts

_ENABLE_BEDROCK_OPTIMIZED_LATENCY = get_boolean_env_variable(
    "ENABLE_BEDROCK_OPTIMIZED_LATENCY", False
)

_ENABLE_BEDROCK_PROMPT_CACHING = get_boolean_env_variable(
    "ENABLE_BEDROCK_PROMPT_CACHING", False
)

_MAX_ATTEMPTS = int(os.getenv("BEDROCK_MAX_ATTEMPTS", "10"))

# Cache for refreshable sessions used in cross-account role assumption
_refreshable_sessions: Dict[str, boto3.Session] = {}


def _get_bedrock_region() -> Optional[str]:
    """Get the Bedrock region from environment variables.

    Returns:
        Region string if BEDROCK_AWS_REGION or AWS_REGION is set, None otherwise.
    """
    return os.environ.get("BEDROCK_AWS_REGION") or os.environ.get("AWS_REGION")


def _create_refreshable_bedrock_session(
    role_arn: str, region: Optional[str]
) -> boto3.Session:
    """Create a boto3 session with RefreshableCredentials for cross-account access.

    This function creates a session that automatically refreshes credentials when
    they approach expiration. The credentials are obtained by assuming the specified
    IAM role via STS.

    Args:
        role_arn: The ARN of the IAM role to assume in the target account.
        region: AWS region for the STS client. If None, uses boto3 default resolution.

    Returns:
        A boto3.Session configured with refreshable credentials.

    Raises:
        RuntimeError: If role assumption fails.

    Credential refresh behavior:
        RefreshableCredentials handles refresh failures gracefully:
        - During "advisory" refresh (credentials still valid but nearing expiration):
          exceptions are caught and logged as warnings, existing credentials continue to work
        - During "mandatory" refresh (credentials already expired):
          exceptions propagate to the caller, but RefreshableCredentials is NOT permanently
          broken - subsequent API calls will retry the refresh

        Potential causes of refresh failures:
        - IAM role deleted or trust policy changed → requires admin intervention
        - Transient network issues → handled by botocore's built-in retries

        This means the system is resilient: temporary failures don't break the session,
        and persistent failures (IAM misconfiguration) will keep failing until fixed.
    """

    def _refresh_credentials() -> Dict[str, str]:
        """Refresh the assumed role credentials."""
        try:
            # Get STS client using default credentials (the source account's credentials)
            if region:
                sts_client = boto3.client("sts", region_name=region)
            else:
                sts_client = boto3.client("sts")

            # Assume the cross-account role
            session_name = (
                f"datahub-bedrock-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
            )
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=3600,  # 1 hour
            )

            credentials = assumed_role["Credentials"]
            return {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": credentials["Expiration"].isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to refresh credentials for role {role_arn}: {e}")
            raise RuntimeError(
                f"Failed to refresh credentials for Bedrock cross-account role: {role_arn}"
            ) from e

    # Create a botocore session with RefreshableCredentials
    botocore_session = get_session()
    refreshable = RefreshableCredentials.create_from_metadata(
        metadata=_refresh_credentials(),
        refresh_using=_refresh_credentials,
        method="sts-assume-role",
    )
    botocore_session._credentials = refreshable  # type: ignore[attr-defined]
    if region:
        botocore_session.set_config_variable("region", region)

    # Create and return a boto3 session using the botocore session
    return boto3.Session(botocore_session=botocore_session)


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


def _create_bedrock_client(
    read_timeout: int = 60,
    connect_timeout: int = 60,
    cross_account_role_arn: Optional[str] = None,
) -> "BedrockRuntimeClient":
    """Create a Bedrock client with the specified configuration.

    This is the internal implementation that does the actual client creation.
    For production use, call get_bedrock_client() which adds caching and reads
    the BEDROCK_CROSS_ACCOUNT_ROLE_ARN env var.

    Args:
        read_timeout: Read timeout in seconds (default: 60)
        connect_timeout: Connect timeout in seconds (default: 60)
        cross_account_role_arn: Optional role ARN for cross-account access.
            If provided, uses STS assume_role with auto-refresh credentials.

    Returns:
        BedrockRuntimeClient instance

    Authentication priority (when cross_account_role_arn is None):
    1. Explicit credentials via BEDROCK_AWS_ACCESS_KEY_ID and BEDROCK_AWS_SECRET_ACCESS_KEY
    2. AWS Profile via AWS_PROFILE (supports SSO profiles)
    3. Default AWS credential chain (~/.aws/credentials, instance profile, etc.)
    """
    config = botocore.config.Config(
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
        max_pool_connections=100,
        retries={"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
    )

    if "BEDROCK_AWS_ROLE" in os.environ:
        logger.warning(
            "Using BEDROCK_AWS_ROLE to assume a role is no longer supported. "
            "Use BEDROCK_CROSS_ACCOUNT_ROLE_ARN instead."
        )

    region = _get_bedrock_region()

    # Option 1: Cross-account role assumption (for invoking Bedrock in another AWS account)
    if cross_account_role_arn:
        logger.info(
            f"Initializing Bedrock client with cross-account role: {cross_account_role_arn}"
        )
        # Create or reuse a refreshable session for this role
        session_key = f"bedrock-{cross_account_role_arn}-{region}"
        if session_key not in _refreshable_sessions:
            _refreshable_sessions[session_key] = _create_refreshable_bedrock_session(
                cross_account_role_arn, region
            )
        boto3_session = _refreshable_sessions[session_key]
        return boto3_session.client("bedrock-runtime", config=config)  # type: ignore

    # Option 2: Explicit credentials for local development
    if "BEDROCK_AWS_ACCESS_KEY_ID" in os.environ:
        logger.info("Initializing Bedrock client from explicit env vars")
        boto3_session = boto3.Session(
            aws_access_key_id=os.environ["BEDROCK_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["BEDROCK_AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("BEDROCK_AWS_REGION", "us-west-2"),
        )
    # Option 3: AWS Profile (great for local development with SSO)
    elif "AWS_PROFILE" in os.environ:
        profile_name = os.environ["AWS_PROFILE"]
        logger.info(f"Initializing Bedrock client from AWS profile: {profile_name}")
        boto3_session = boto3.Session(
            profile_name=profile_name,
            region_name=os.environ.get("BEDROCK_AWS_REGION", "us-west-2"),
        )
    # Option 4: Default - use instance profile or standard AWS credential chain
    else:
        logger.info(
            "Initializing Bedrock client from default credential chain (instance profile or AWS CLI)"
        )
        if region:
            logger.info(f"Using explicit region from environment: {region}")
            boto3_session = boto3.Session(region_name=region)
        else:
            logger.info(
                "No explicit region found in BEDROCK_AWS_REGION or AWS_REGION, "
                "boto3 will resolve from AWS config or instance metadata"
            )
            boto3_session = boto3.Session()

    return boto3_session.client("bedrock-runtime", config=config)  # type: ignore


@serialized
@functools.cache
def get_bedrock_client(
    read_timeout: int = 60, connect_timeout: int = 60
) -> "BedrockRuntimeClient":
    """Get a singleton Bedrock client with appropriate authentication.

    This is a cached wrapper around _create_bedrock_client(). The cache ensures
    that only one client is created per (read_timeout, connect_timeout) combination,
    and the @serialized decorator ensures thread-safe initialization.

    For cross-account access:
    - Set: BEDROCK_CROSS_ACCOUNT_ROLE_ARN=arn:aws:iam::123456789012:role/BedrockRole
    - The role will be assumed using STS with automatic credential refresh

    For local development with SSO:
    - Run: aws sso login --profile your-profile-name
    - Set: AWS_PROFILE=your-profile-name
    - Optionally set: BEDROCK_AWS_REGION=us-west-2 (or your preferred region)
    """
    cross_account_role_arn = os.environ.get("BEDROCK_CROSS_ACCOUNT_ROLE_ARN")
    return _create_bedrock_client(
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
        cross_account_role_arn=cross_account_role_arn,
    )


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
