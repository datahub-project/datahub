# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

# Get column lineage for the entire flow
results = client.lineage.get_lineage(
    source_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD),id)",
    direction="downstream",
)

print(list(results))
