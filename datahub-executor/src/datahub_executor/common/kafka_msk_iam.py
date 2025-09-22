"""
NOTE:
This file duplicates the AWS MSK IAM Kafka OAuth callback originally available at
`datahub_actions.utils.kafka_msk_iam`. It is intentionally copied into the
executor package to avoid dependency and packaging issues that previously
prevented the runtime from reliably importing the callback.

Keeping this implementation within the executor ensures that the configuration
value `oauth_cb: datahub_executor.kafka_msk_iam:oauth_cb` is always resolvable
without requiring the `acryl-datahub-actions` package at runtime.

If you consider changing or removing this file, verify that all environments
which reference the callback continue to function and that the signer dependency
(`aws-msk-iam-sasl-signer-python`) remains available.
"""

import logging
import os

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger(__name__)


def oauth_cb(oauth_config: dict) -> tuple[str, float]:
    """
    OAuth callback function for AWS MSK IAM authentication.

    This function is invoked by the Kafka client to generate the SASL/OAUTHBEARER token
    for authentication with AWS MSK using IAM. It must return a tuple of
    (auth_token, expiry_time_seconds).
    """
    try:
        region = (
            os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region=region)
        # Convert expiry from milliseconds to seconds as required by Kafka client
        return auth_token, float(expiry_ms) / 1000
    except Exception:
        logger.exception("Error generating AWS MSK IAM authentication token")
        raise
