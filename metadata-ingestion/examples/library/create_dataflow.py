from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="example_dataflow",
    platform="airflow",
)

client.entities.upsert(dataflow)
