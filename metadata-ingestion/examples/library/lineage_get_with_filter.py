# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient.from_env()

# get upstream snowflake production datasets.
results = client.lineage.get_lineage(
    source_urn="urn:li:dataset:(platform,sales_agg,PROD)",
    direction="upstream",
    filter=F.and_(F.platform("snowflake"), F.entity_type("dataset"), F.env("PROD")),
)

print(results)
