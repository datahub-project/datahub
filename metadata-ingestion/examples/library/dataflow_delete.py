# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/dataflow_delete.py
from datahub.metadata.urns import DataFlowUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Create the URN for the DataFlow to delete
dataflow_urn = DataFlowUrn("airflow", "old_pipeline", "dev")

# Soft delete the DataFlow (marks as removed but retains metadata)
client.entities.delete(dataflow_urn, hard=False)

print(f"Soft deleted DataFlow: {dataflow_urn}")

# To hard delete (permanently remove):
# client.entities.delete(dataflow_urn, hard=True)
