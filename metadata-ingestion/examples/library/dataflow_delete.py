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
