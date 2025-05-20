from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

flow_urn = DataFlowUrn(orchestrator="airflow", flow_id="data_pipeline", cluster="PROD")

client.lineage.add_datajob_lineage(
    datajob=DataJobUrn(flow=flow_urn, job_id="data_pipeline"),
    upstreams=[DataJobUrn(flow=flow_urn, job_id="extract_job")],
)
