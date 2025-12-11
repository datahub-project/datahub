# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()


upstream_urn = DatasetUrn(platform="snowflake", name="sales_raw")
downstream_urn = DatasetUrn(platform="snowflake", name="sales_cleaned")
client.lineage.add_lineage(upstream=upstream_urn, downstream=downstream_urn)
