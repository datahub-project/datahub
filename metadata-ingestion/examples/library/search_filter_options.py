# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient, FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for entities that are in PROD environment
prod_results = client.search.get_urns(filter=F.env("PROD"))

# Search for entities that are dashboards
dashboards = client.search.get_urns(filter=F.entity_type("dashboard"))

# Search for datasets that are on snowflake platform
snowflake_datasets = client.search.get_urns(
    filter=F.and_(F.entity_type("dataset"), F.platform("snowflake"))
)


print(list(prod_results))
print(list(dashboards))
print(list(snowflake_datasets))
