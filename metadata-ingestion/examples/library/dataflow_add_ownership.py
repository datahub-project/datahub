# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/dataflow_add_ownership.py
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, DataFlowUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Get the existing DataFlow
dataflow_urn = DataFlowUrn("airflow", "daily_sales_pipeline", "prod")
dataflow = client.entities.get(dataflow_urn)

# Add individual owners
dataflow.add_owner((CorpUserUrn("alice"), "DATAOWNER"))
dataflow.add_owner((CorpUserUrn("bob"), "DEVELOPER"))

# Add group owner
dataflow.add_owner((CorpGroupUrn("analytics-team"), "DATAOWNER"))

# Save changes
client.entities.upsert(dataflow)

print(f"Updated DataFlow: {dataflow.urn}")
print(f"Owners: {dataflow.owners}")
