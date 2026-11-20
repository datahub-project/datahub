from datahub.sdk import DataFlowUrn, DataHubClient, DataJobUrn

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DataJobUrn.from_string(...)
# The flow_id carries the flow's platform instance as a prefix ("PROD."), which is
# how DataFlow(platform_instance=...) builds its urn.
datajob_urn = DataJobUrn(
    flow=DataFlowUrn(
        orchestrator="airflow", flow_id="PROD.example_dag", cluster="PROD"
    ),
    job_id="example_datajob",
)

datajob_entity = client.entities.get(datajob_urn)
print("DataJob name:", datajob_entity.name)
print("DataJob Flow URN:", datajob_entity.flow_urn)
print("DataJob description:", datajob_entity.description)
