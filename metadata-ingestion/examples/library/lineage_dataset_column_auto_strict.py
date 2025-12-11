# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

client.lineage.add_lineage(
    upstream=DatasetUrn(platform="snowflake", name="sales_raw"),
    downstream=DatasetUrn(platform="snowflake", name="sales_cleaned"),
    column_lineage="auto_strict",
)
