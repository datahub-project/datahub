from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

dataflow = DataFlow(
    platform="airflow",
    name="example_dag",
    platform_instance="PROD",
)

# datajob will inherit the platform and platform instance from the flow
datajob = DataJob(
    name="example_datajob",
    description="example datajob",
    flow=dataflow,
)

client.entities.upsert(dataflow)
client.entities.upsert(datajob)

datajob_entity = client.entities.get(datajob.urn)

print("DataJob name:", datajob_entity.name)
print("DataJob Flow URN:", datajob_entity.flow_urn)
print("DataJob description:", datajob_entity.description)
