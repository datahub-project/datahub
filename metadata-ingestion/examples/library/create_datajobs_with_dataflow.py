from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="example_dataflow",
    platform="airflow",
)
# datajob will inherit the platform from the flow_urn
datajobs = dataflow.create_jobs(["datajob1", "datajob2"])

client.entities.upsert(dataflow)
for datajob in datajobs:
    client.entities.create(datajob)
