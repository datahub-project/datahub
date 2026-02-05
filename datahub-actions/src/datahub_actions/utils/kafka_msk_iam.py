"""Module for AWS MSK IAM authentication."""

import logging
import os
from typing import Any, Optional

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger(__name__)

# Config keys that can be passed in oauth_config (via consumer_config)
AWS_ROLE_ARN_KEY = "aws_role_arn"
AWS_ROLE_SESSION_NAME_KEY = "aws_role_session_name"
AWS_REGION_KEY = "aws_region"

DEFAULT_SESSION_NAME = "datahub-msk-session"


def _get_region(oauth_config: Optional[dict[str, Any]]) -> str:
    """Get AWS region from oauth_config or environment variables."""
    if oauth_config and oauth_config.get(AWS_REGION_KEY):
        return oauth_config[AWS_REGION_KEY]
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"


def oauth_cb(oauth_config: Optional[dict[str, Any]]) -> tuple[str, float]:
    """
    OAuth callback function for AWS MSK IAM authentication.

    This function is called by the Kafka client to generate the SASL/OAUTHBEARER token
    for authentication with AWS MSK using IAM.

    Supports IAM role assumption by passing configuration in consumer_config:
        - aws_role_arn: The ARN of the IAM role to assume
        - aws_role_session_name: Optional session name (default: "datahub-msk-session")
        - aws_region: Optional AWS region (falls back to AWS_REGION env var)

    Example consumer_config for role assumption:
        consumer_config:
            security.protocol: "SASL_SSL"
            sasl.mechanism: "OAUTHBEARER"
            oauth_cb: "datahub_actions.utils.kafka_msk_iam:oauth_cb"
            aws_role_arn: "arn:aws:iam::123456789012:role/MyMSKRole"
            aws_role_session_name: "my-session"
            aws_region: "us-west-2"

    Returns:
        tuple[str, float]: (auth_token, expiry_time_seconds)
    """
    try:
        region = _get_region(oauth_config)
        role_arn = oauth_config.get(AWS_ROLE_ARN_KEY) if oauth_config else None

        if role_arn:
            # oauth_config is guaranteed non-None here since role_arn came from it
            session_name = oauth_config.get(
                AWS_ROLE_SESSION_NAME_KEY, DEFAULT_SESSION_NAME
            )
            logger.debug(f"Assuming IAM role {role_arn} with session {session_name}")
            auth_token, expiry_ms = (
                MSKAuthTokenProvider.generate_auth_token_from_role_arn(
                    region=region,
                    role_arn=role_arn,
                    sts_session_name=session_name,
                )
            )
        else:
            auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
                region=region
            )

        # Convert expiry from milliseconds to seconds as required by Kafka client
        return auth_token, float(expiry_ms) / 1000
    except Exception as e:
        logger.error(
            f"Error generating AWS MSK IAM authentication token: {e}", exc_info=True
        )
        raise
