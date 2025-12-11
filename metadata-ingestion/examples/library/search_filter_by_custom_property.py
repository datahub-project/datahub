# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient(server="<your_server>", token="<your_token>")
# search for all assets with a custom property "my_custom_property" set to "my_value"
results = client.search.get_urns(
    filter=F.has_custom_property("my_custom_property", "my_value")
)
