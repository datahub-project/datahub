# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import ChartUrn, CorpUserUrn, DataHubClient

client = DataHubClient.from_env()

chart = client.entities.get(ChartUrn("looker", "sales_dashboard"))

chart.add_owner(CorpUserUrn("jdoe"))

client.entities.update(chart)

print(f"Added owner {CorpUserUrn('jdoe')} to chart {chart.urn}")
