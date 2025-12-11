# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DashboardUrn.from_string(...)
dashboard_urn = DashboardUrn("looker", "example_dashboard_id")

dashboard_entity = client.entities.get(dashboard_urn)
print("Dashboard name:", dashboard_entity.name)
print("Dashboard platform:", dashboard_entity.platform)
print("Dashboard description:", dashboard_entity.description)
