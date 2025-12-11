# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient

client = DataHubClient.from_env()

chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(chart)
