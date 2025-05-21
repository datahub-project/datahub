from datahub.metadata.urns import DataFlowUrn
from datahub.sdk import DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform from the flow_urn
datajob = DataJob(
    name="example_datajob",
    flow_urn=DataFlowUrn(orchestrator="airflow", flow_id="example_dag", cluster="PROD"),
)

client.entities.upsert(datajob)
