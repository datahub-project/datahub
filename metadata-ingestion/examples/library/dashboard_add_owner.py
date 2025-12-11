# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import CorpUserUrn, DashboardUrn, DataHubClient

client = DataHubClient.from_env()

dashboard = client.entities.get(DashboardUrn("looker", "dashboards.999999"))

dashboard.add_owner(CorpUserUrn("jdoe"))

client.entities.update(dashboard)

print(f"Added owner {CorpUserUrn('jdoe')} to dashboard {dashboard.urn}")
