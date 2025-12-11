# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
    timestamp_millis=int(time.time() * 1000),  # Current Unix timestamp in milliseconds
    type="SUCCESS",  # Can be 'SUCCESS', 'FAILURE', 'ERROR', or 'INIT'
    properties=[{"key": "my_custom_key", "value": "my_custom_value"}],
    external_url="https://my-great-expectations.com/results/1234",  # Optional: URL to the results in the external tool
    # Uncomment the following section and use if type is 'ERROR'
    # error_type="UNKNOWN_ERROR",
    # error_message="The assertion failed due to an unknown error"
)

if res:
    log.info("Successfully reported Assertion Result!")
