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
