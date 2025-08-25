from datahub.sdk import DataFlowUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DataFlowUrn.from_string(...)
dataflow_urn = DataFlowUrn("airflow", "example_dataflow_id")

dataflow_entity = client.entities.get(dataflow_urn)
print("DataFlow name:", dataflow_entity.name)
print("DataFlow platform:", dataflow_entity.platform)
print("DataFlow description:", dataflow_entity.description)
