"""Module for AWS MSK IAM authentication."""

import logging

from aws_msk_iam_sasl_signer_python.msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger(__name__)


def oauth_cb(oauth_config):
    """
    OAuth callback function for AWS MSK IAM authentication.

    This function is called by the Kafka client to generate the SASL/OAUTHBEARER token
    for authentication with AWS MSK using IAM.

    Returns:
        tuple: (auth_token, expiry_time_seconds)
    """
    try:
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token()
        # Convert expiry from milliseconds to seconds as required by Kafka client
        return auth_token, expiry_ms / 1000
    except Exception as e:
        logger.error(f"Error generating AWS MSK IAM authentication token: {e}")
        raise
