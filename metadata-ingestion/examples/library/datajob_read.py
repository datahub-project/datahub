from datahub.sdk import DataHubClient, DataJobUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DataJobUrn.from_string(...)
datajob_urn = DataJobUrn("airflow", "example_dag", "example_datajob_id")

datajob_entity = client.entities.get(datajob_urn)
print("DataJob name:", datajob_entity.name)
print("DataJob Flow URN:", datajob_entity.flow_urn)
print("DataJob description:", datajob_entity.description)
