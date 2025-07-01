from datahub.metadata.urns import TagUrn
from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="example_dataflow",
    platform="airflow",
    description="airflow pipeline for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(dataflow)

dataflow_entity = client.entities.get(dataflow.urn)
print("DataFlow name:", dataflow_entity.name)
print("DataFlow platform:", dataflow_entity.platform)
print("DataFlow description:", dataflow_entity.description)
