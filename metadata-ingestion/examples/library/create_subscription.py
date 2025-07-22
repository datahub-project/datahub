import logging

from datahub.sdk import DataHubClient

log = logging.getLogger(__name__)

# Initialize the client
client = DataHubClient(
    server="https://your-datahub-cloud-instance.com", token="your-token"
)

# Subscribe to all assertion changes for a dataset
client.subscriptions.subscribe(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)",
    subscriber_urn="urn:li:corpuser:john.doe",
    # entity_change_types defaults to all available change types for datasets
)
log.info("Successfully subscribed to dataset notifications")

# Subscribe to specific assertion changes
client.subscriptions.subscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpuser:john.doe",
    entity_change_types=["ASSERTION_PASSED", "ASSERTION_FAILED"],
)
log.info("Successfully subscribed to specific assertion changes")

# Subscribe a group to assertion changes
client.subscriptions.subscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpGroup:data-team",
    entity_change_types=["ASSERTION_FAILED", "ASSERTION_ERROR"],
)
log.info("Successfully subscribed group to assertion failures and errors")
