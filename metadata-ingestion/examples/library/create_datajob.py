from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

dataflow = DataFlow(
    platform="airflow",
    name="example_dag",
    platform_instance="PROD",
)

datajob = DataJob(
    name="example_datajob",
    flow=dataflow,
)

client.entities.upsert(datajob)
