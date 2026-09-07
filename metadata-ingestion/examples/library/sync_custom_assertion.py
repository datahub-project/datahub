import logging
import time

from datahub.sdk import DataHubClient

log = logging.getLogger(__name__)

client = DataHubClient.from_env()

assertion_urn = "urn:li:assertion:my-unique-assertion-id"
entity_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,example.table,PROD)"

# Sync a CUSTOM assertion with structured display fields (external / self-reported).
res = client.assertions.sync_custom_assertion(
    urn=assertion_urn,
    entity_urn=entity_urn,
    type="My Custom Category",
    description="Column profileId must not be null",
    platform_urn="urn:li:dataPlatform:great-expectations",
    field_paths=["profileId"],
    scope="DATASET_COLUMN",
    aggregation="IDENTITY",
    operator="NOT_NULL",
    native_type="expect_column_values_to_not_be_null",
    external_url="https://my-monitoring-tool.com/result-for-this-assertion",
)

# Report a run result so the assertion appears as passing/failing in the UI.
client.assertions.report_assertion_result(
    urn=assertion_urn,
    timestamp_millis=int(time.time() * 1000),
    type="SUCCESS",
)

log.info("Synced custom assertion %s: %s", assertion_urn, res)
