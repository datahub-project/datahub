import logging
import time

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

existing_assertion_urn = "urn:li:assertion:my-unique-assertion-id"

# Report result for assertion
res = graph.report_assertion_result(
    urn="urn:li:assertion:<your-new-assertion-id>",  # Replace with your actual assertion URN
    timestampMillis=int(time.time() * 1000),  # Current Unix timestamp in milliseconds
    type="SUCCESS",  # Can be 'SUCCESS', 'FAILURE', 'ERROR', or 'INIT'
    properties=[
        {
            "key": "my_custom_key",
            "value": "my_custom_value"
        }
    ],
    externalUrl="https://my-great-expectations.com/results/1234",  # Optional: URL to the results in the external tool
    # Uncomment the following section and use if type is 'ERROR'
    # errorType="UNKNOWN_ERROR",
    # errorMessage="The assertion failed due to an unknown error"
)

if res:
    log.info(f'Successfully reported Assertion Result!')
