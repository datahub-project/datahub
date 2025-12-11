# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

"""Module for AWS MSK IAM authentication."""

import logging
import os

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger(__name__)


def oauth_cb(oauth_config: dict) -> tuple[str, float]:
    """
    OAuth callback function for AWS MSK IAM authentication.

    This function is called by the Kafka client to generate the SASL/OAUTHBEARER token
    for authentication with AWS MSK using IAM.

    Returns:
        tuple[str, float]: (auth_token, expiry_time_seconds)
    """
    try:
        region = (
            os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region=region)
        # Convert expiry from milliseconds to seconds as required by Kafka client
        return auth_token, float(expiry_ms) / 1000
    except Exception as e:
        logger.error(
            f"Error generating AWS MSK IAM authentication token: {e}", exc_info=True
        )
        raise
