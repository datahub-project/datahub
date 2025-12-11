# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.sdk import DataHubClient

log = logging.getLogger(__name__)

# Initialize the client
client = DataHubClient(
    server="https://your-datahub-cloud-instance.com", token="your-token"
)

# Unsubscribe from all changes for a dataset
client.subscriptions.unsubscribe(
    urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)",
    subscriber_urn="urn:li:corpuser:john.doe",
    # entity_change_types defaults to all existing change types
)
log.info("Successfully unsubscribed from all dataset notifications")

# Unsubscribe from specific assertion change types
client.subscriptions.unsubscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpuser:john.doe",
    entity_change_types=[
        "ASSERTION_PASSED"
    ],  # Keep ASSERTION_FAILED and ASSERTION_ERROR
)
log.info("Successfully unsubscribed from specific assertion change types")

# Unsubscribe a group from assertion changes
client.subscriptions.unsubscribe(
    urn="urn:li:assertion:your-assertion-id",
    subscriber_urn="urn:li:corpGroup:data-team",
    entity_change_types=["ASSERTION_FAILED", "ASSERTION_ERROR"],
)
log.info("Successfully unsubscribed group from assertion notifications")
