"""
NOTE:
This file duplicates the Azure Event Hubs OAuth callback originally available at
`datahub_actions.utils.kafka_eventhubs_auth`. It is intentionally copied into the
executor package to avoid dependency and packaging issues that previously
prevented the runtime from reliably importing the callback.

Keeping this implementation within the executor ensures that the configuration
value `oauth_cb: datahub_executor.common.kafka_eventhubs_auth:oauth_cb` is always resolvable
without requiring the `acryl-datahub-actions` package at runtime.

If you consider changing or removing this file, verify that all environments
which reference the callback continue to function and that the Azure identity dependency
(`azure-identity`) remains available.
"""

import logging
import os

from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)


def oauth_cb(oauth_config: dict) -> tuple[str, float]:
    """
    OAuth callback function for Azure Event Hubs authentication.

    This function is invoked by the Kafka client to generate the SASL/OAUTHBEARER token
    for authentication with Azure Event Hubs using Azure Active Directory. It must return
    a tuple of (auth_token, expiry_time_seconds).

    The callback uses Azure DefaultAzureCredential which attempts authentication through:
    1. Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    2. Managed Identity (when running in Azure)
    3. Azure CLI credentials
    4. Other credential sources in the Azure credential chain

    Environment variables:
        AZURE_TENANT_ID: Azure AD tenant ID (optional, can be inferred)
        AZURE_CLIENT_ID: Service principal client ID (for service principal auth)
        AZURE_CLIENT_SECRET: Service principal secret (for service principal auth)
        AZURE_EVENT_HUBS_NAMESPACE: Event Hubs namespace (e.g., "mynamespace.servicebus.windows.net")

    Returns:
        tuple[str, float]: (auth_token, expiry_time_seconds)
    """
    try:
        # Get the Event Hubs namespace from environment
        # This is used to construct the resource scope
        namespace = os.getenv("AZURE_EVENT_HUBS_NAMESPACE", "")
        if not namespace:
            logger.warning(
                "AZURE_EVENT_HUBS_NAMESPACE not set. Using default Event Hubs scope."
            )
            # Default scope for Event Hubs
            scope = "https://eventhubs.azure.net/.default"
        else:
            # Construct the scope for the specific namespace
            # Note: Event Hubs uses a standard scope, not namespace-specific
            scope = "https://eventhubs.azure.net/.default"

        # Use DefaultAzureCredential for flexible authentication
        credential = DefaultAzureCredential()

        # Get the access token
        token = credential.get_token(scope)

        # token.expires_on is a Unix timestamp (seconds since epoch)
        # The Kafka client expects expiry time in seconds since epoch
        expiry_time = float(token.expires_on)

        logger.debug(
            f"Successfully obtained Azure Event Hubs token, expires at {expiry_time}"
        )

        return token.token, expiry_time

    except Exception as e:
        logger.error(
            f"Error generating Azure Event Hubs authentication token: {e}",
            exc_info=True,
        )
        raise
