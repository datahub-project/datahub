from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataflow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="data_pipeline", cluster="PROD"
)

client.lineage.add_lineage(
    upstream=DataJobUrn(flow=dataflow_urn, job_id="data_job_1"),
    downstream=DataJobUrn(flow=dataflow_urn, job_id="data_job_2"),
)
