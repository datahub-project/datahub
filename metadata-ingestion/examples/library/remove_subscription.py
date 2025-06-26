import logging

from acryl_datahub_cloud.sdk.subscription_client import SubscriptionClient

from datahub.sdk import DataHubClient

log = logging.getLogger(__name__)

# Initialize the clients
client = DataHubClient(
    server="https://your-datahub-cloud-instance.com", token="your-token"
)
subscription_client = SubscriptionClient(client)

# Unsubscribe from all changes for a dataset
subscription_client.unsubscribe(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)",
    subscriber_urn="urn:li:corpuser:john.doe",
    # entity_change_types defaults to all existing change types
)
log.info("Successfully unsubscribed from all dataset notifications")

# Unsubscribe from specific assertion change types
subscription_client.unsubscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpuser:john.doe",
    entity_change_types=[
        "ASSERTION_PASSED"
    ],  # Keep ASSERTION_FAILED and ASSERTION_ERROR
)
log.info("Successfully unsubscribed from specific assertion change types")

# Unsubscribe a group from assertion changes
subscription_client.unsubscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpGroup:data-team",
    entity_change_types=["ASSERTION_FAILED", "ASSERTION_ERROR"],
)
log.info("Successfully unsubscribed group from assertion notifications")
