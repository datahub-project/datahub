# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import ChartUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use ChartUrn.from_string(...)
chart_urn = ChartUrn("looker", "example_chart_id")

chart_entity = client.entities.get(chart_urn)
print("Chart name:", chart_entity.name)
print("Chart platform:", chart_entity.platform)
print("Chart description:", chart_entity.description)
