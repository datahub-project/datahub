# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/dataflow_update_description.py
from datahub.metadata.urns import DataFlowUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Get the existing DataFlow
dataflow_urn = DataFlowUrn("airflow", "daily_sales_pipeline", "prod")
dataflow = client.entities.get(dataflow_urn)

# Update the description
dataflow.set_description(
    "This pipeline processes daily sales data from multiple regional databases, "
    "performs aggregation and validation, and updates the central reporting warehouse. "
    "Runs daily at 2 AM UTC with a 4-hour SLA."
)

# Save changes
client.entities.upsert(dataflow)

print(f"Updated description for DataFlow: {dataflow.urn}")
print(f"New description: {dataflow.description}")
