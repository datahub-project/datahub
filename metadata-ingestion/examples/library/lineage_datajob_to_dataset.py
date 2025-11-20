from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataflow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="data_pipeline", cluster="PROD"
)

client.lineage.add_lineage(
    upstream=DataJobUrn(flow=dataflow_urn, job_id="data_job_1"),
    downstream=DatasetUrn(platform="postgres", name="raw_data"),
)
