# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient

client = DataHubClient(server="<your_server>", token="<your_token>")

# Search for entities with "sales" in the metadata
results = client.search.get_urns(query="sales")

print(list(results))
